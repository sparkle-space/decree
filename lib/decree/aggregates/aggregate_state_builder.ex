defmodule Decree.Aggregates.AggregateStateBuilder do
  use TelemetryRegistry
  alias Decree.Aggregates.Aggregate
  alias Decree.EventStore
  alias Decree.EventStore.RecordedEvent
  alias Decree.EventStore.SnapshotData
  alias Decree.PersonalFields
  alias Decree.PersonalFields.RedactedError
  alias Decree.Snapshotting
  alias Decree.Telemetry

  telemetry_event(%{
    event: [:decree, :aggregate, :populate, :start],
    description: "Emitted when an aggregate begins loading from the event store",
    measurements: "%{system_time: integer()}",
    metadata: """
    %{application: Decree.Application.t(),
      aggregate_module: module(),
      aggregate_uuid: String.t(),
      aggregate_state: struct(),
      aggregate_version: non_neg_integer()}
    """
  })

  telemetry_event(%{
    event: [:decree, :aggregate, :populate, :stop],
    description: "Emitted when an aggregate completes loading from the event store",
    measurements: "%{duration: non_neg_integer(), count: non_neg_integer()}",
    metadata: """
    %{application: Decree.Application.t(),
      aggregate_module: module(),
      aggregate_uuid: String.t(),
      aggregate_state: struct(),
      aggregate_version: non_neg_integer()}
    """
  })

  @moduledoc """
  Builds an aggregate's state by loading its snapshot and/or events from the
  event store.

  ## Telemetry

  #{telemetry_docs()}

  """

  @read_event_batch_size 1_000

  @doc """
  Populate the aggregate's state from a snapshot, if present, and its events.

  Attempt to fetch a snapshot for the aggregate to use as its initial state.
  If the snapshot exists, fetch any subsequent events to rebuild its state.
  Otherwise start with the aggregate struct and stream all existing events for
  the aggregate from the event store to rebuild its state from those events.

  Answers the `%Aggregate{}`, or `{:error, :key_unknown}` when a personal value in its
  snapshot or in an event its fold reads can't be read. It never raises for that.
  """
  def populate(%Aggregate{} = state) do
    %Aggregate{snapshotting: snapshotting} = state

    case Snapshotting.read_snapshot(snapshotting) do
      {:ok, %SnapshotData{} = snapshot} ->
        populate_from_snapshot(state, snapshot)

      {:error, _error} ->
        # No snapshot present, or exists but for outdated state, so use initial empty state
        rebuild_from_events(initial_state(state), %{})
    end
  end

  defp populate_from_snapshot(%Aggregate{} = state, %SnapshotData{} = snapshot) do
    %Aggregate{
      application: application,
      aggregate_module: aggregate_module,
      aggregate_uuid: aggregate_uuid
    } = state

    %SnapshotData{source_version: source_version, data: data} = snapshot

    case PersonalFields.decrypt_state(application, aggregate_uuid, aggregate_module, data) do
      {:ok, data, answers} ->
        state = %Aggregate{state | aggregate_version: source_version, aggregate_state: data}

        rebuild_from_events(state, answers)

      {:erased, answers} ->
        # The events' erased fields fold as absent, which a snapshot can't give.
        rebuild_from_events(initial_state(state), answers)

      {:error, :key_unknown} = error ->
        error
    end
  end

  defp initial_state(%Aggregate{aggregate_module: aggregate_module} = state) do
    %Aggregate{state | aggregate_version: 0, aggregate_state: struct(aggregate_module)}
  end

  @doc """
  Load events from the event store, in batches, to rebuild the aggregate state

  Answers the `%Aggregate{}`, or `{:error, :key_unknown}` as `populate/1` does.
  """
  def rebuild_from_events(%Aggregate{} = state), do: rebuild_from_events(state, %{})

  defp rebuild_from_events(%Aggregate{} = state, answers) do
    %Aggregate{
      application: application,
      aggregate_uuid: aggregate_uuid,
      aggregate_version: aggregate_version
    } = state

    case EventStore.stream_forward(
           application,
           aggregate_uuid,
           aggregate_version + 1,
           @read_event_batch_size
         ) do
      {:error, :stream_not_found} ->
        # aggregate does not exist, return initial state
        state

      event_stream ->
        rebuild_from_event_stream(event_stream, state, answers)
    end
  end

  @doc false
  # Applies one recorded event: the fold's one step, for every path that folds. `answers` is the
  # fold's own lookups, which end with it. `fold` is `PersonalFields.fold_events/1`'s answer, read
  # once per fold: `nil` for an aggregate that doesn't use `Decree.PersonalFields.Aggregate`,
  # whose `apply/2` runs bare, and otherwise the events to decrypt first. On such an aggregate
  # any `apply/2` can hold decrypted state, from an earlier event or from a snapshot, listed
  # event or not, so a raise, an exit or a throw leaves here redacted.
  @spec apply_event(RecordedEvent.t(), Aggregate.t(), [module] | nil, PersonalFields.answers()) ::
          {:ok, Aggregate.t(), PersonalFields.answers()} | {:error, :key_unknown}
  def apply_event(%RecordedEvent{} = event, %Aggregate{} = state, nil, answers) do
    %RecordedEvent{data: data, stream_version: stream_version} = event
    %Aggregate{aggregate_module: aggregate_module, aggregate_state: aggregate_state} = state

    aggregate_state = aggregate_module.apply(aggregate_state, data)

    {:ok, advance(state, stream_version, aggregate_state), answers}
  end

  def apply_event(%RecordedEvent{} = event, %Aggregate{} = state, fold, answers)
      when is_list(fold) do
    %RecordedEvent{stream_version: stream_version} = event
    %Aggregate{aggregate_module: aggregate_module, aggregate_state: aggregate_state} = state

    with {:ok, data, answers} <-
           PersonalFields.readable_event(state.application, event, fold, answers) do
      aggregate_state = apply_redacted(aggregate_module, aggregate_state, data)

      {:ok, advance(state, stream_version, aggregate_state), answers}
    end
  end

  defp apply_redacted(aggregate_module, aggregate_state, data) do
    aggregate_module.apply(aggregate_state, data)
  rescue
    error ->
      reraise RedactedError.redact(error), RedactedError.scrub_stacktrace(__STACKTRACE__)
  catch
    :exit, reason -> exit(RedactedError.reduce(reason))
    :throw, value -> throw(RedactedError.reduce(value))
  end

  defp advance(%Aggregate{} = state, stream_version, aggregate_state) do
    %{state | aggregate_version: stream_version, aggregate_state: aggregate_state}
  end

  # Rebuild aggregate state from a `Stream` of its events.
  defp rebuild_from_event_stream(event_stream, %Aggregate{} = state, answers) do
    telemetry_prefix = [:decree, :aggregate, :populate]
    start_time = Telemetry.start(telemetry_prefix, telemetry_metadata(state))
    fold = PersonalFields.fold_events(state.aggregate_module)

    result =
      Enum.reduce_while(event_stream, {state, 0, answers}, fn event, {state, count, answers} ->
        case apply_event(event, state, fold, answers) do
          {:ok, state, answers} -> {:cont, {state, count + 1, answers}}
          {:error, :key_unknown} = error -> {:halt, error}
        end
      end)

    case result do
      {:error, :key_unknown} = error ->
        metadata = Map.put(telemetry_metadata(state), :error, :key_unknown)
        Telemetry.stop(telemetry_prefix, start_time, metadata, %{count: 0})

        error

      {state, count, _answers} ->
        Telemetry.stop(telemetry_prefix, start_time, telemetry_metadata(state), %{count: count})

        state
    end
  end

  defp telemetry_metadata(%Aggregate{} = state) do
    %Aggregate{
      application: application,
      aggregate_module: aggregate_module,
      aggregate_uuid: aggregate_uuid,
      aggregate_state: aggregate_state,
      aggregate_version: aggregate_version
    } = state

    %{
      application: application,
      aggregate_module: aggregate_module,
      aggregate_uuid: aggregate_uuid,
      aggregate_state: aggregate_state,
      aggregate_version: aggregate_version
    }
  end
end
