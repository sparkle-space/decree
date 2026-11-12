defmodule Decree.EventStore do
  @moduledoc """
  Use the event store configured for a Decree application: the `Decree.Store` module named by
  `event_store: [event_store: MyApp.EventStore]` in its config, called with the store process's
  `name:`. The framework's `Decree.EventStore.EventData`, `RecordedEvent` and `SnapshotData` are
  mapped to the store's structs and back here.

  ### Telemetry Events

  Adds telemetry events for the following functions. Events are emitted in the form

  `[:decree, :event_store, event]` with their spannable postfixes (`start`, `stop`, `exception`)

    * ack_event/3
    * append_to_stream/4
    * append_to_streams/3
    * delete_snapshot/2
    * delete_subscription/3
    * prune_snapshots/3
    * read_query/3
    * read_snapshot/2
    * read_snapshot/3
    * read_tags/3
    * record_snapshot/2
    * stream_forward/2
    * stream_forward/3
    * stream_forward/4
    * subscribe/2
    * subscribe_to/5
    * subscribe_to/6
    * unsubscribe/2

  `read_query/3`'s span never fires `exception`, and a failure stops it with `error`: its doc
  says why.
  """
  alias Decree.Application
  alias Decree.Event.Upcast
  alias Decree.EventStore.{EventData, RecordedEvent, SnapshotData}
  alias Decree.Store.Config, as: StoreConfig
  alias Decree.Store.ReadResult

  @type application :: Application.t()
  @type config :: Keyword.t()
  @type stream_uuid :: String.t()
  @type start_from :: :origin | :current | integer
  @type expected_version :: :any_version | :no_stream | :stream_exists | non_neg_integer
  @type subscription_name :: String.t()
  @type subscription :: any
  @type subscriber :: pid
  @type source_uuid :: String.t()
  @type error :: term

  @all_stream "$all"

  @doc """
  Append one or more events to a stream atomically.

  The store's answers come back unchanged (see `c:Decree.Store.append_to_stream/4`).
  `{:error, :wrong_expected_version}` makes an aggregate rebuild its state and retry the
  command while it has attempts left. A failure of the append statement other than its domain
  answers is `{:error, %Decree.Store.AppendError{}}`, classified `:unavailable`, `:retryable`
  or `:rejected`, and the aggregate returns it as the command's error. After an
  `:unavailable` one the aggregate stops, so the next command rebuilds its state from the
  stream (see `Decree.Store.AppendError`).

  With `returning: :append_result` in `opts`, success answers
  `{:ok, %Decree.Store.AppendResult{}}` instead of `:ok`.
  """
  def append_to_stream(application, stream_uuid, expected_version, events, opts \\ []) do
    meta = %{
      application: application,
      stream_uuid: stream_uuid,
      expected_version: expected_version
    }

    span(:append_to_stream, meta, fn ->
      store_append(application, stream_uuid, expected_version, events, opts)
    end)
  end

  @doc """
  Appends one or more events to a stream atomically, as `append_to_stream/5` does, with no
  telemetry event.

  It answers what `append_to_stream/5` answers, and a raise, an exit or a throw reaches the
  caller as it would there.
  """
  # No `span/3`: a span's exception event holds the raised term and its stacktrace, and a
  # stacktrace's first frame can hold a call's arguments.
  def append_without_span(application, stream_uuid, expected_version, events) do
    store_append(application, stream_uuid, expected_version, events, [])
  end

  defp store_append(application, stream_uuid, expected_version, events, opts) do
    {event_store, name} = store(application)

    events = Enum.map(events, &to_event_data/1)

    event_store.append_to_stream(stream_uuid, expected_version, events, with_name(opts, name))
  end

  @doc """
  Append events to several streams atomically, in one statement.

  `appends` is a list of `{stream_uuid, expected_version, events}`, one per stream. The
  options and answers are those of `c:Decree.Store.append_to_streams/2`: with `condition:` an
  event past its `after` matching its `items` answers `{:error, :append_condition_failed}`, and
  with `returning: :append_result` success answers `{:ok, %Decree.Store.AppendResult{}}`.
  """
  def append_to_streams(application, appends, opts \\ []) do
    # Names the streams only: every telemetry handler sees this, and the events are plaintext.
    meta = %{application: application, stream_uuids: Enum.map(appends, &elem(&1, 0))}

    span(:append_to_streams, meta, fn ->
      {event_store, name} = store(application)

      appends =
        Enum.map(appends, fn {stream_uuid, expected_version, events} ->
          {stream_uuid, expected_version, Enum.map(events, &to_event_data/1)}
        end)

      event_store.append_to_streams(appends, with_name(opts, name))
    end)
  end

  @doc """
  Reads the events matching a query across every stream, in one statement, each upcast as
  `stream_forward/4` upcasts it.

  See `c:Decree.Store.read_query/2` for the items, options and answers. The result's
  events are `Decree.EventStore.RecordedEvent`s.

  Its span covers the store's answer, and the upcast runs after it stops. A raise, an exit or
  a throw from either, the store's deserializing and the mapping included, reaches
  the caller as it was raised, and no telemetry event holds it. The span never fires its
  exception event. A failure inside it stops it with `error` in the stop metadata, the kind
  alone: `:error`, `:exit` or `:throw`. A read that answered stops with no `error`.
  """
  def read_query(application, items, opts \\ []) do
    meta = %{application: application, items: items, opts: opts}

    read =
      contained_span(:read_query, meta, fn ->
        {event_store, name} = store(application)

        with {:ok, %ReadResult{events: events} = result} <-
               event_store.read_query(items, with_name(opts, name)) do
          {:ok, %ReadResult{result | events: Enum.map(events, &from_recorded_event/1)}}
        end
      end)

    # Not in the span: an upcaster's failure can hold the stored event it failed on, and it
    # reaches the caller as it was raised.
    with {:ok, %ReadResult{events: events} = result} <- read do
      upcast_opts = [additional_metadata: %{application: application}]

      {:ok, %ReadResult{result | events: Upcast.upcast_event_stream(events, upcast_opts)}}
    end
  end

  @doc """
  Reads the tags of the events at the given `$all` positions, in one statement.

  Answers as `c:Decree.Store.read_tags/2` does: a position with no row is absent, and so are
  tags of a kind the store declares deletable and every tag of an event of a type it declares.
  """
  def read_tags(application, positions, opts \\ []) do
    meta = %{application: application, positions: positions}

    span(:read_tags, meta, fn ->
      {event_store, name} = store(application)

      event_store.read_tags(positions, with_name(opts, name))
    end)
  end

  @doc """
  Streams events from the given stream, in the order in which they were originally written.
  """
  def stream_forward(application, stream_uuid, start_version \\ 0, read_batch_size \\ 1_000) do
    meta = %{
      application: application,
      stream_uuid: stream_uuid,
      start_version: start_version,
      read_batch_size: read_batch_size
    }

    span(:stream_forward, meta, fn ->
      {event_store, name} = store(application)

      opts = [name: name, read_batch_size: read_batch_size]

      case event_store.stream_forward(stream_uuid, start_version, opts) do
        {:error, _error} = error ->
          error

        stream ->
          stream
          |> Stream.map(&from_recorded_event/1)
          |> Upcast.upcast_event_stream(additional_metadata: %{application: application})
      end
    end)
  end

  @doc """
  Reads the last event of the given stream, upcast as `stream_forward/4` upcasts.

  Answers `{:ok, recorded_event}`, or `{:error, :stream_not_found}` for a stream that never
  existed and for one hard-deleted, which nothing tells apart, or `{:error, reason}` for a read
  that failed. A failed read is never `:stream_not_found`.

  Its cost does not grow with the stream.
  """
  # No `span/3`: a span's exception event holds the raised term, and `Jason.DecodeError` holds
  # the row it failed on.
  def read_stream_head(application, stream_uuid) do
    {event_store, name} = store(application)

    case event_store.read_stream_backward(stream_uuid, -1, 1, name: name) do
      {:ok, [event]} ->
        event = from_recorded_event(event)

        {:ok, Upcast.upcast_event(event, additional_metadata: %{application: application})}

      # The store reads the stream's row, then its events: a hard delete can land between them.
      {:ok, []} ->
        {:error, :stream_not_found}

      {:error, _error} = error ->
        error
    end
  end

  @doc """
  The stream prefixes the application's event store declares as deletable.

  A store without `enable_hard_deletes` answers `[]`, whatever it declares: no delete can reach
  a stream there.
  """
  def deletable_stream_prefixes(application) do
    {_event_store, name} = store(application)

    # `delete_stream/4` refuses a hard delete without this setting, so what a store declares
    # could never be deleted. The config's parse makes it a boolean.
    if StoreConfig.lookup(name, :enable_hard_deletes) do
      case StoreConfig.lookup(name, :deletable) do
        nil -> []
        deletable -> Keyword.get(deletable, :stream_prefixes, [])
      end
    else
      []
    end
  end

  @doc """
  The targets the application's running event store's `conditional_appends` declares, or `[]`
  if it declares none.

  It answers from the config the store parsed as it started, so it agrees with what the store
  refuses. An application with a rule route or a windowed aggregate calls this as it starts,
  right after its store has, and an application with neither never calls it. See "Two
  declarations" in `Decree.Decision.Rule`.
  """
  def conditional_targets(application) do
    {_event_store, name} = store(application)

    case StoreConfig.lookup(name, :conditional_appends) do
      nil -> []
      conditional_appends -> Keyword.get(conditional_appends, :targets, [])
    end
  end

  @doc """
  Create a transient subscription to a single event stream.

  The event store will publish any events appended to the given stream to the
  `subscriber` process as an `{:events, events}` message.

  The subscriber does not need to acknowledge receipt of the events.
  """
  def subscribe(application, stream_uuid) do
    span(:subscribe, %{application: application, stream_uuid: stream_uuid}, fn ->
      {event_store, name} = store(application)

      event_store.subscribe(stream(stream_uuid), name: name, mapper: &from_recorded_event/1)
    end)
  end

  @doc """
  Create a persistent subscription to an event stream.

  To subscribe to all events appended to any stream use `:all` as the stream
  when subscribing.

  The event store will remember the subscribers last acknowledged event.
  Restarting the named subscription will resume from the next event following
  the last seen.

  Once subscribed, the subscriber process should be sent a
  `{:subscribed, subscription}` message to allow it to defer initialisation
  until the subscription has started.

  The subscriber process will be sent all events persisted to the stream. It
  will receive a `{:events, events}` message for each batch of events persisted
  for a single aggregate.

  The subscriber must ack each received, and successfully processed event, using
  `Decree.EventStore.ack_event/3`.

  ## Examples

  Subscribe to all streams:

      {:ok, subscription} =
        Decree.EventStore.subscribe_to(MyApp, :all, "Example", self(), :current)

  Subscribe to a single stream:

      {:ok, subscription} =
        Decree.EventStore.subscribe_to(MyApp, "stream1", "Example", self(), :origin)

  """
  def subscribe_to(
        application,
        stream_uuid,
        subscription_name,
        subscriber,
        start_from,
        opts \\ []
      ) do
    meta = %{
      application: application,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscriber: subscriber,
      start_from: start_from
    }

    span(:subscribe_to, meta, fn ->
      {event_store, name} = store(application)

      opts = subscription_options(name, start_from, opts)

      case stream_uuid do
        :all ->
          event_store.subscribe_to_all_streams(subscription_name, subscriber, opts)

        stream_uuid ->
          event_store.subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts)
      end
    end)
  end

  @doc """
  Acknowledge receipt and successful processing of the given event received from
  a subscription to an event stream. If the event is part of a batch, every event that
  precedes it in the batch is acknowledged as well.
  """
  def ack_event(application, subscription, %RecordedEvent{} = event) do
    meta = %{application: application, subscription: subscription, event: event}

    span(:ack_event, meta, fn ->
      {event_store, _name} = store(application)

      %RecordedEvent{event_number: event_number} = event

      event_store.ack(subscription, event_number)
    end)
  end

  @doc """
  Unsubscribe an existing subscriber from event notifications.

  This will not delete the subscription.

  ## Example

      :ok = Decree.EventStore.unsubscribe(MyApp, subscription)

  """
  def unsubscribe(application, subscription) do
    span(:unsubscribe, %{application: application, subscription: subscription}, fn ->
      Decree.Store.Subscriptions.Subscription.unsubscribe(subscription)
    end)
  end

  @doc """
  Delete an existing subscription.

  ## Example

      :ok = Decree.EventStore.delete_subscription(MyApp, :all, "Example")

  """
  def delete_subscription(application, subscribe_to, handler_name) do
    meta = %{application: application, subscribe_to: subscribe_to, handler_name: handler_name}

    span(:delete_subscription, meta, fn ->
      {event_store, name} = store(application)

      event_store.delete_subscription(stream(subscribe_to), handler_name, name: name)
    end)
  end

  @doc """
  Read a snapshot, if available, for a given source.

  The read from before snapshot versions, unchanged: it reads the store's legacy one-row
  table, never a versioned row. `read_snapshot/3` reads a snapshot version's row.
  """
  def read_snapshot(application, source_uuid) do
    {event_store, name} = store(application)

    span(:read_snapshot, %{application: application, source_uuid: source_uuid}, fn ->
      with {:ok, snapshot} <- event_store.read_snapshot(source_uuid, name: name) do
        {:ok, from_snapshot_data(snapshot)}
      end
    end)
  end

  @doc """
  Read a snapshot, if available, for a given source at a snapshot version.

  A source has one row per snapshot version, so code at another version never reads or
  overwrites this one's. A nil `snapshot_version` reads the store's legacy one-row table.
  """
  def read_snapshot(application, source_uuid, snapshot_version) do
    {event_store, name} = store(application)

    meta = %{
      application: application,
      source_uuid: source_uuid,
      snapshot_version: snapshot_version
    }

    span(:read_snapshot, meta, fn ->
      opts = [name: name, snapshot_version: snapshot_version]

      with {:ok, snapshot} <- event_store.read_snapshot(source_uuid, opts) do
        {:ok, from_snapshot_data(snapshot)}
      end
    end)
  end

  @doc """
  Record a snapshot of the data and metadata for a given source
  """
  def record_snapshot(application, snapshot) do
    {event_store, name} = store(application)

    span(:record_snapshot, %{application: application, snapshot: snapshot}, fn ->
      snapshot
      |> to_snapshot_data()
      |> event_store.record_snapshot(name: name)
    end)
  end

  @doc """
  Delete a previously recorded snapshot for a given source, at every snapshot version.
  """
  def delete_snapshot(application, source_uuid) do
    {event_store, name} = store(application)

    span(:delete_snapshot, %{application: application, source_uuid: source_uuid}, fn ->
      event_store.delete_snapshot(source_uuid, name: name)
    end)
  end

  @doc """
  Delete the snapshots of a source type whose snapshot version is below the given one.
  See `Decree.Aggregate.prune_snapshots/2`.
  """
  def prune_snapshots(application, source_type, snapshot_version) do
    {event_store, name} = store(application)

    meta = %{
      application: application,
      source_type: source_type,
      snapshot_version: snapshot_version
    }

    span(:prune_snapshots, meta, fn ->
      event_store.prune_snapshots(source_type, snapshot_version, name: name)
    end)
  end

  # The application's `Decree.Store` module and the name its store process runs under.
  defp store(application) do
    %{event_store: event_store, name: name} = Application.event_store(application)

    {event_store, name}
  end

  defp with_name(opts, name), do: Keyword.put(opts, :name, name)

  defp stream(:all), do: @all_stream
  defp stream(stream_uuid), do: stream_uuid

  defp subscription_options(name, start_from, opts) do
    opts
    |> Keyword.merge(name: name, start_from: start_from, mapper: &from_recorded_event/1)
    |> Keyword.update(:partition_by, nil, fn
      nil ->
        nil

      partition_by when is_function(partition_by, 1) ->
        fn %Decree.Store.RecordedEvent{} = event ->
          event |> from_recorded_event() |> partition_by.()
        end
    end)
  end

  # The mapping between the framework's structs and the store's.

  defp to_event_data(%EventData{} = event_data) do
    %EventData{
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      tags: tags,
      required_tag_kinds: required_tag_kinds
    } = event_data

    %Decree.Store.EventData{
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      tags: tags,
      required_tag_kinds: required_tag_kinds
    }
  end

  defp from_recorded_event(%Decree.Store.RecordedEvent{} = event) do
    %Decree.Store.RecordedEvent{
      event_id: event_id,
      event_number: event_number,
      stream_uuid: stream_uuid,
      stream_version: stream_version,
      correlation_id: correlation_id,
      causation_id: causation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      created_at: created_at
    } = event

    %RecordedEvent{
      event_id: event_id,
      event_number: event_number,
      stream_id: stream_uuid,
      stream_version: stream_version,
      correlation_id: correlation_id,
      causation_id: causation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      created_at: created_at
    }
  end

  defp to_snapshot_data(%SnapshotData{} = snapshot) do
    %SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at,
      snapshot_version: snapshot_version
    } = snapshot

    %Decree.Store.Snapshots.SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at,
      snapshot_version: snapshot_version
    }
  end

  defp from_snapshot_data(%Decree.Store.Snapshots.SnapshotData{} = snapshot) do
    %Decree.Store.Snapshots.SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at,
      snapshot_version: snapshot_version
    } = snapshot

    %SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at,
      snapshot_version: snapshot_version
    }
  end

  # TODO convert to macro
  defp span(event, meta, func) do
    :telemetry.span([:decree, :event_store, event], meta, fn ->
      {func.(), meta}
    end)
  end

  # A span whose exception event never fires. That event holds the raised term and its
  # stacktrace, and a read's failure can hold the stored bytes it failed on, as
  # `Jason.DecodeError` does. So a raise, an exit or a throw in `func` is answered as a value,
  # the span stops with `error` set to its kind and nothing else of it, and it is raised again,
  # as it was, for the caller's own boundary to decide.
  defp contained_span(event, meta, func) do
    contained =
      :telemetry.span([:decree, :event_store, event], meta, fn ->
        try do
          {{:answered, func.()}, meta}
        catch
          kind, value -> {{:left, kind, value, __STACKTRACE__}, Map.put(meta, :error, kind)}
        end
      end)

    case contained do
      {:answered, answer} -> answer
      {:left, kind, value, stacktrace} -> :erlang.raise(kind, value, stacktrace)
    end
  end
end
