defmodule Decree.EventStore.Adapter do
  @moduledoc """
  Defines the behaviour to be implemented by an event store adapter to be used by Decree.
  """

  alias Decree.EventStore.{EventData, RecordedEvent, SnapshotData}

  @type adapter_meta :: map
  @type application :: Decree.Application.t()
  @type config :: Keyword.t()
  @type stream_uuid :: String.t()
  @type start_from :: :origin | :current | integer
  @type expected_version :: :any_version | :no_stream | :stream_exists | non_neg_integer
  @type subscription_name :: String.t()
  @type subscription :: any
  @type subscriber :: pid
  @type source_uuid :: String.t()
  @type error :: term

  @doc """
  Return a child spec defining all processes required by the event store.
  """
  @callback child_spec(application, config) ::
              {:ok, [:supervisor.child_spec() | {module, term} | module], adapter_meta}

  @doc """
  Append one or more events to a stream atomically.

  `{:error, :wrong_expected_version}` makes an aggregate rebuild its state and
  retry the command while it has attempts left.

  The `Decree.EventStore.Adapters.EventStore` adapter returns the store's errors
  unchanged (see `c:Decree.Store.append_to_stream/4`). A failure of the append
  statement other than its domain answers is
  `{:error, %Decree.Store.AppendError{}}`, classified `:unavailable`,
  `:retryable` or `:rejected`, and the aggregate returns it as the command's
  error. After an `:unavailable` one the aggregate stops, so the next command
  rebuilds its state from the stream (see `Decree.Store.AppendError`).

  With `returning: :append_result` in `opts`, success answers
  `{:ok, %Decree.Store.AppendResult{}}` instead of `:ok`.
  """
  @callback append_to_stream(
              adapter_meta,
              stream_uuid,
              expected_version,
              events :: list(EventData.t()),
              opts :: Keyword.t()
            ) ::
              :ok
              | {:ok, Decree.Store.AppendResult.t()}
              | {:error, :wrong_expected_version}
              | {:error, Decree.Store.AppendError.t()}
              | {:error, error}

  @doc """
  Append events to several streams atomically, in one statement.

  `appends` is a list of `{stream_uuid, expected_version, events}`, one per stream. The
  options and answers are those of `c:Decree.Store.append_to_streams/2`: with `condition:`
  an event past its `after` matching its `items` answers
  `{:error, :append_condition_failed}`, and with `returning: :append_result` success
  answers `{:ok, %Decree.Store.AppendResult{}}`.
  """
  @callback append_to_streams(
              adapter_meta,
              appends :: [{stream_uuid, expected_version, list(EventData.t())}],
              opts :: Keyword.t()
            ) ::
              :ok
              | {:ok, Decree.Store.AppendResult.t()}
              | {:error, Decree.Store.AppendError.t()}
              | {:error, error}

  @doc """
  Reads the events matching a query across every stream, in one statement.

  The options and answers are those of `c:Decree.Store.read_query/2`, except that the
  `Decree.Store.ReadResult`'s events are `Decree.EventStore.RecordedEvent`s.
  """
  @callback read_query(adapter_meta, items :: [Decree.Store.Query.item()], opts :: Keyword.t()) ::
              {:ok, Decree.Store.ReadResult.t()} | {:error, error}

  @doc """
  Reads the tags of the events at the given `$all` positions, in one statement.

  Answers as `c:Decree.Store.read_tags/2` does: a position with no row is absent, and so
  are tags of a kind the store declares deletable and every tag of an event of a type it
  declares.
  """
  @callback read_tags(adapter_meta, positions :: [non_neg_integer()], opts :: Keyword.t()) ::
              {:ok, %{non_neg_integer() => [String.t()]}} | {:error, error}

  @doc """
  Streams events from the given stream, in the order in which they were
  originally written.
  """
  @callback stream_forward(
              adapter_meta,
              stream_uuid,
              start_version :: non_neg_integer,
              read_batch_size :: non_neg_integer
            ) ::
              Enumerable.t()
              | {:error, :stream_not_found}
              | {:error, error}

  @doc """
  Reads the last event of the given stream.

  Answers `{:error, :stream_not_found}` for a stream with no events, which is also what a
  hard delete landing during the read gives, and `{:error, reason}` for a read that failed.
  A failed read never answers `:stream_not_found`.
  """
  @callback read_stream_head(adapter_meta, stream_uuid) ::
              {:ok, RecordedEvent.t()}
              | {:error, :stream_not_found}
              | {:error, :stream_deleted}
              | {:error, error}

  @doc """
  The stream prefixes the event store declares as deletable, or `[]` if it declares none.

  A store without `enable_hard_deletes` answers `[]`, whatever it declares: no delete can reach
  a stream there.
  """
  @callback deletable_stream_prefixes(adapter_meta) :: [String.t()]

  @doc """
  The targets the event store's `conditional_appends` declares, or `[]` if it declares none.

  An application calls this as it starts, before the store has, so it answers from what
  `c:child_spec/2` was given and needs no running store. See "Two declarations" in
  `Decree.Decision.Rule`.
  """
  @callback conditional_targets(adapter_meta) :: [term()]

  @doc """
  Create a transient subscription to a single event stream.

  The event store will publish any events appended to the given stream to the
  `subscriber` process as an `{:events, events}` message.

  The subscriber does not need to acknowledge receipt of the events.
  """
  @callback subscribe(adapter_meta, stream_uuid | :all) ::
              :ok | {:error, error}

  @doc """
  Create a persistent subscription to an event stream.
  """
  @callback subscribe_to(
              adapter_meta,
              stream_uuid | :all,
              subscription_name,
              subscriber,
              start_from,
              opts :: Keyword.t()
            ) ::
              {:ok, subscription}
              | {:error, :subscription_already_exists}
              | {:error, error}

  @doc """
  Acknowledge receipt and successful processing of the given event received from
  a subscription to an event stream. Note that if the event is part of a batch,
  all events that precede this event in the batch are considered to be acknowledged
  as well.
  """
  @callback ack_event(adapter_meta, pid, RecordedEvent.t()) :: :ok

  @doc """
  Unsubscribe an existing subscriber from event notifications.

  This should not delete the subscription.
  """
  @callback unsubscribe(adapter_meta, subscription) :: :ok

  @doc """
  Delete an existing subscription.
  """
  @callback delete_subscription(
              adapter_meta,
              stream_uuid | :all,
              subscription_name
            ) ::
              :ok | {:error, :subscription_not_found} | {:error, error}

  @doc """
  Read a snapshot, if available, for a given source.
  """
  @callback read_snapshot(adapter_meta, source_uuid) ::
              {:ok, SnapshotData.t()} | {:error, :snapshot_not_found}

  @doc """
  Record a snapshot of the data and metadata for a given source
  """
  @callback record_snapshot(adapter_meta, SnapshotData.t()) ::
              :ok | {:error, error}

  @doc """
  Delete a previously recorded snapshot for a given source
  """
  @callback delete_snapshot(adapter_meta, source_uuid) ::
              :ok | {:error, error}
end
