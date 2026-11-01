defmodule Decree.Decision.Dispatch do
  @moduledoc false

  # One dispatch of a decision, in the dispatcher's task. The steps are those of
  # `Decree.Decision`'s moduledoc, and each is named below by its number there.

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Commands.Router
  alias Decree.Decision.Read
  alias Decree.Decision.Read.Merged
  alias Decree.Event.{Append, Mapper, TagContext}
  alias Decree.EventStore
  alias Decree.Store.{AppendResult, ReadResult}
  alias Decree.Telemetry
  alias Decree.UUID

  require Logger

  @execute [:decree, :decision, :execute]
  @read [:decree, :decision, :read]
  @retry [:decree, :decision, :retry]
  @refused [:decree, :decision, :refused]

  defstruct [
    :application,
    :decision,
    :stream_uuid,
    :identity_prefix,
    :context,
    :tag_context,
    :command_tag,
    :bounds,
    attempt: 1
  ]

  @doc """
  Runs the dispatch. Answers `{:decided, outcome, reply}`, `reply` being a
  `ExecutionContext.format_reply/3` tuple, or `{:error, reason}`, or
  `{:error, error, stacktrace}` for a raise.
  """
  def execute(application, decision, stream_uuid, identity_prefix, %ExecutionContext{} = context) do
    metadata = %{
      application: application,
      decision: decision,
      stream_uuid: stream_uuid,
      execution_context: context
    }

    start_time = Telemetry.start(@execute, metadata)

    dispatch = %__MODULE__{
      application: application,
      decision: decision,
      stream_uuid: stream_uuid,
      identity_prefix: identity_prefix,
      context: context
    }

    {result, attempts} = run(dispatch)

    telemetry_stop(result, start_time, Map.put(metadata, :attempts, attempts))
    answer(result, context)
  end

  @doc """
  What a `decide/2` answer means, with no store and no route in it.
  """
  def shape(:duplicate), do: {:error, :duplicate_command}
  def shape({:error, _reason} = error), do: error
  def shape(none) when none in [[], :ok, nil, {:streams, []}], do: no_events()
  def shape({:streams, [_ | _] = members}), do: members(members)

  def shape(events) when is_struct(events) or is_list(events) do
    with {:ok, events} <- events(events), do: {:ok, {:route, events}}
  end

  def shape(_invalid), do: {:error, :invalid_decision_return}

  @doc """
  The events of a `shape/1`, in the order they append.
  """
  def events_of({:route, events}), do: events

  def events_of({:streams, members}) do
    Enum.flat_map(members, fn {_kind, _identity, events} -> events end)
  end

  defp no_events, do: {:error, :decision_returned_no_events}

  defp events(events) do
    case List.wrap(events) do
      [] ->
        no_events()

      [_event | _] = events ->
        if not List.improper?(events) and Enum.all?(events, &is_struct/1) do
          {:ok, events}
        else
          {:error, :invalid_decision_return}
        end
    end
  end

  defp members(members) do
    members
    |> Enum.reduce_while([], fn member, members ->
      case member(member) do
        {:ok, member} -> {:cont, [member | members]}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      members -> {:ok, {:streams, Enum.reverse(members)}}
    end
  end

  defp member({kind, identity, events}) do
    cond do
      not Router.stream_tag_kind?(kind) ->
        {:error, :invalid_decision_stream}

      not UUID.canonical?(identity) ->
        {:error, :invalid_decision_identity}

      true ->
        with {:ok, events} <- events(events), do: {:ok, {kind, identity, events}}
    end
  end

  defp member(_member), do: {:error, :invalid_decision_stream}

  # Everything a raise can come from runs under this, as in an aggregate's `execute_command`.
  defp run(%__MODULE__{} = dispatch) do
    with {:ok, dispatch} <- ready(dispatch) do
      attempt(dispatch)
    else
      {:error, _reason} = error -> {error, 0}
    end
  rescue
    error ->
      stacktrace = __STACKTRACE__
      Logger.error(Exception.format(:error, error, stacktrace))

      {{:error, error, stacktrace}, nil}
  end

  # Step 0.
  defp ready(%__MODULE__{context: context} = dispatch) do
    %ExecutionContext{command: command, aggregate_identity: identity, metadata: metadata} =
      context

    case {Append.command_tag(command), UUID.canonical?(identity)} do
      {nil, _canonical?} ->
        {:error, :missing_command_id}

      {_command_tag, false} ->
        {:error, :invalid_decision_identity}

      {command_tag, true} ->
        {:ok,
         %__MODULE__{
           dispatch
           | tag_context: %TagContext{metadata: metadata, command: command},
             command_tag: command_tag,
             bounds: dispatch.decision.__decision__()
         }}
    end
  end

  defp attempt(%__MODULE__{attempt: attempt} = dispatch) do
    case cycle(dispatch) do
      {:retry, reason} -> retry(dispatch, reason)
      result -> {result, attempt}
    end
  end

  # From step 1, on what the store holds now.
  defp retry(%__MODULE__{context: context, attempt: attempt} = dispatch, reason) do
    case ExecutionContext.retry(context) do
      {:ok, context} ->
        :telemetry.execute(@retry, %{attempt: attempt}, %{
          application: dispatch.application,
          decision: dispatch.decision,
          reason: reason
        })

        attempt(%__MODULE__{dispatch | context: context, attempt: attempt + 1})

      {:error, :too_many_attempts} = error ->
        {error, attempt}
    end
  end

  # Steps 1 to 8, once.
  defp cycle(%__MODULE__{} = dispatch) do
    started = System.monotonic_time()

    with {:ok, read} <- first_read(dispatch),
         {:ok, result} <- read_query(dispatch, read),
         merged = Read.first(read, result),
         :ok <- within_events(dispatch, merged, [read], started),
         {:ok, merged, reads} <- second_read(dispatch, merged, read, started),
         {:ok, tags} <- fetch_tags(dispatch, merged),
         :ok <- check_tag_index(tags),
         read_time = System.monotonic_time() - started,
         state = fold(dispatch, merged, reads, read_time),
         :ok <- admit(dispatch, state),
         {:ok, mode} <- classify(merged, tags) do
      decide(mode, dispatch, state, merged, reads)
    end
  end

  # Step 1, up to the read.
  defp first_read(%__MODULE__{decision: decision, context: context} = dispatch) do
    case decision.query(context.command, dispatch.tag_context) do
      {:error, _reason} = error ->
        error

      items ->
        with {:ok, parsed} <- Read.parse(items) do
          read = Read.layout(parsed, framework_items(dispatch))

          with :ok <- within_items(dispatch, [read]), do: {:ok, read}
        end
    end
  end

  # Step 2.
  defp second_read(%__MODULE__{bounds: %{followup_query?: false}}, merged, read, _started) do
    {:ok, merged, [read]}
  end

  defp second_read(%__MODULE__{} = dispatch, %Merged{} = merged, first, started) do
    %__MODULE__{decision: decision, context: context, tag_context: tag_context} = dispatch

    business_events = merged |> Read.business_events() |> Enum.map(& &1.data)

    case decision.followup_query(context.command, tag_context, business_events) do
      [] ->
        {:ok, merged, [first]}

      {:error, _reason} = error ->
        error

      items ->
        with {:ok, parsed} <- Read.parse(items) do
          read = Read.layout(parsed, framework_items(dispatch))
          reads = [first, read]

          with :ok <- within_items(dispatch, reads),
               {:ok, result} <- read_query(dispatch, read),
               :ok <- no_new_command_match(merged, read, result) do
            merged = Read.merge(merged, read, result)

            with :ok <- within_events(dispatch, merged, reads, started) do
              {:ok, merged, reads}
            end
          end
        end
    end
  end

  defp no_new_command_match(%Merged{} = merged, %Read{} = read, %ReadResult{} = result) do
    if Read.new_command_match?(merged, read, result) do
      {:retry, :new_command_match}
    else
      :ok
    end
  end

  defp framework_items(%__MODULE__{command_tag: command_tag}), do: [[tags: [command_tag]]]

  defp read_query(%__MODULE__{} = dispatch, %Read{items: items}) do
    %__MODULE__{application: application, bounds: bounds, command_tag: command_tag} = dispatch

    EventStore.read_query(application, items,
      count: bounds.max_events + 1,
      command_tag: command_tag
    )
  end

  # The one tag fetch, for every position a framework item matched. No positions send no
  # statement.
  defp fetch_tags(%__MODULE__{application: application}, %Merged{} = merged) do
    EventStore.read_tags(application, Read.framework_positions(merged))
  end

  # Step 3: the place a tag index check reads the fetched tags.
  defp check_tag_index(_tags), do: :ok

  # Steps 4 and 5.
  defp fold(%__MODULE__{decision: decision} = dispatch, %Merged{} = merged, reads, read_time) do
    started = System.monotonic_time()

    state =
      merged
      |> Read.business_events()
      |> Enum.reduce(decision.initial_state(), &decision.evolve(&2, &1.data))

    telemetry_read(dispatch, merged, reads, read_time, System.monotonic_time() - started)

    state
  end

  # Step 6.
  defp admit(%__MODULE__{decision: decision, context: context}, state) do
    case decision.admit(state, context.command) do
      :ok -> :ok
      {:error, _reason} = error -> error
    end
  end

  # Step 7: any command match refuses, and the mode is what step 8 takes.
  defp classify(%Merged{} = merged, _tags) do
    case Read.command_positions(merged) do
      [] -> {:ok, :open}
      [_position | _] -> {:error, :unclassified_command_match}
    end
  end

  # Step 8.
  defp decide(:open, %__MODULE__{decision: decision} = dispatch, state, merged, reads) do
    with {:ok, shape} <- shape(decision.decide(state, dispatch.context.command)),
         events = events_of(shape),
         :ok <- within_batch(dispatch, events),
         # Before anything commits: a raise here has to leave the store as it was.
         state = Enum.reduce(events, state, &decision.evolve(&2, &1)),
         {:ok, appends} <- prepare_members(dispatch, shape) do
      append(dispatch, appends, merged, reads, events, state)
    end
  end

  defp prepare_members(%__MODULE__{application: application} = dispatch, shape) do
    dispatch
    |> members_of(shape)
    |> Enum.reduce_while([], fn {stream_uuid, context, events}, appends ->
      case prepare(application, stream_uuid, event_data(events, context), context) do
        {:ok, prepared} -> {:cont, [{stream_uuid, :any_version, prepared} | appends]}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      appends -> {:ok, Enum.reverse(appends)}
    end
  end

  defp members_of(%__MODULE__{stream_uuid: stream_uuid, context: context}, {:route, events}) do
    [{stream_uuid, context, events}]
  end

  defp members_of(%__MODULE__{identity_prefix: prefix} = dispatch, {:streams, members}) do
    %__MODULE__{context: %ExecutionContext{} = context} = dispatch

    for {kind, identity, events} <- members do
      member_context = %ExecutionContext{context | aggregate_identity: identity, stream_tag: kind}

      {prefix <> identity, member_context, events}
    end
  end

  defp event_data(events, %ExecutionContext{} = context) do
    Mapper.map_to_event_data(events,
      causation_id: context.causation_id,
      correlation_id: context.correlation_id,
      metadata: context.metadata
    )
  end

  # The one prepare step, per member. Its error is answered here, ahead of `append/6`'s arms:
  # those read the store's answer, and a `Decree.Event.Tags` implementation's is not one.
  defp prepare(_application, _stream_uuid, event_data, %ExecutionContext{} = context) do
    Append.prepare(event_data, context)
  end

  defp append(%__MODULE__{} = dispatch, appends, %Merged{} = merged, reads, events, state) do
    condition = [items: Read.condition_items(reads), after: merged.after]
    opts = [condition: condition, returning: :append_result]

    case EventStore.append_to_streams(dispatch.application, appends, opts) do
      {:ok, %AppendResult{} = result} ->
        decided(dispatch, appends, result, events, state)

      {:error, :append_condition_failed} ->
        {:retry, :append_condition_failed}

      {:error, _reason} = error ->
        error
    end
  end

  defp decided(%__MODULE__{}, [{stream_uuid, _, _} | _], result, events, state) do
    %AppendResult{position: position, stream_versions: stream_versions} = result

    {:ok,
     %{
       aggregate_uuid: stream_uuid,
       aggregate_state: state,
       aggregate_version: Map.fetch!(stream_versions, stream_uuid),
       position: position,
       stream_versions: stream_versions
     }, events}
  end

  defp answer({:ok, outcome, events}, %ExecutionContext{} = context) do
    {:decided, outcome, ExecutionContext.format_reply({:ok, events}, context, outcome)}
  end

  defp answer({:error, _reason} = error, _context), do: error
  defp answer({:error, _error, _stacktrace} = error, _context), do: error

  defp within_items(%__MODULE__{bounds: %{max_items: limit}} = dispatch, reads) do
    within(dispatch, :items, Read.condition_count(reads), limit)
  end

  defp within_events(%__MODULE__{bounds: %{max_events: limit}} = dispatch, merged, reads, started) do
    matched = Read.matched_count(merged)

    if matched > limit do
      telemetry_read(dispatch, merged, reads, System.monotonic_time() - started, 0)
    end

    within(dispatch, :events, matched, limit)
  end

  defp within_batch(%__MODULE__{bounds: %{max_batch_events: limit}} = dispatch, events) do
    within(dispatch, :batch_events, length(events), limit)
  end

  defp within(_dispatch, _bound, value, limit) when value <= limit, do: :ok

  defp within(%__MODULE__{application: application, decision: decision}, bound, value, limit) do
    Logger.error(fn ->
      "#{inspect(decision)} refused on the #{bound} bound: #{value} over #{limit}"
    end)

    :telemetry.execute(@refused, %{value: value, limit: limit}, %{
      application: application,
      decision: decision,
      bound: bound
    })

    {:error, refusal(bound)}
  end

  defp refusal(:batch_events), do: :decision_batch_too_large
  defp refusal(_bound), do: :decision_query_too_large

  defp telemetry_read(%__MODULE__{} = dispatch, %Merged{} = merged, reads, read_time, fold_time) do
    measurements = %{
      items: Read.condition_count(reads),
      matched_events: Read.matched_count(merged),
      read_time: read_time,
      fold_time: fold_time
    }

    :telemetry.execute(@read, measurements, %{
      application: dispatch.application,
      decision: dispatch.decision,
      attempt: dispatch.attempt
    })
  end

  defp telemetry_stop({:ok, _outcome, events}, start_time, metadata) do
    Telemetry.stop(@execute, start_time, Map.put(metadata, :events, events))
  end

  defp telemetry_stop({:error, error}, start_time, metadata) do
    Telemetry.stop(@execute, start_time, Map.put(metadata, :error, error))
  end

  defp telemetry_stop({:error, error, stacktrace}, start_time, metadata) do
    metadata = Map.delete(metadata, :attempts)

    Telemetry.exception(@execute, start_time, :error, error, stacktrace, metadata)
  end
end
