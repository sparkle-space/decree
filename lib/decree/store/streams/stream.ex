defmodule Decree.Store.Streams.Stream do
  @moduledoc false

  alias Decree.Store.{
    AppendCondition,
    AppendError,
    EventData,
    Query,
    ReadResult,
    RecordedEvent,
    Storage,
    UUID
  }

  alias Decree.Store.Streams.StreamInfo

  @all_stream "$all"
  @max_bigint 9_223_372_036_854_775_807

  defguardp is_bigint(value) when is_integer(value) and abs(value) <= @max_bigint

  def append_to_stream(conn, stream_uuid, expected_version, events, opts)
      when length(events) < 1000 do
    {serializer, new_opts} = Keyword.pop(opts, :serializer)

    with :ok <- validate_tags(events),
         {:ok, new_opts} <- compile_condition([stream_uuid], new_opts),
         {:ok, stream} <- stream_info(conn, stream_uuid, expected_version, new_opts),
         :ok <- do_append_to_storage(conn, stream, events, expected_version, serializer, new_opts) do
      :ok
    end
    |> maybe_retry_once(conn, opts, retry_append(conn, stream_uuid, expected_version, events))
  end

  def append_to_stream(conn, stream_uuid, expected_version, events, opts) do
    {serializer, new_opts} = Keyword.pop(opts, :serializer)

    with :ok <- validate_tags(events),
         {:ok, new_opts} <- compile_condition([stream_uuid], new_opts) do
      transaction(
        conn,
        fn transaction ->
          with {:ok, stream} <- stream_info(transaction, stream_uuid, expected_version, new_opts),
               :ok <-
                 do_append_to_storage(
                   transaction,
                   stream,
                   events,
                   expected_version,
                   serializer,
                   new_opts
                 ) do
            :ok
          else
            {:error, error} -> Postgrex.rollback(transaction, error)
          end
        end,
        new_opts
      )
      |> maybe_retry_once(conn, opts, retry_append(conn, stream_uuid, expected_version, events))
    end
  end

  # One statement at any size, so no transaction of its own: see `Appender.append_streams/3`.
  def append_to_streams(conn, appends, opts) do
    {serializer, new_opts} = Keyword.pop(opts, :serializer)

    with :ok <- validate_appends(appends),
         :ok <-
           validate_tags(Enum.flat_map(appends, fn {_stream, _version, events} -> events end)),
         {:ok, new_opts} <- compile_condition(stream_uuids(appends), new_opts),
         {:ok, streams} <- streams_info(conn, appends, new_opts) do
      appends
      |> Enum.zip(streams)
      |> Enum.map(&prepare_member(&1, serializer, new_opts))
      |> then(&Storage.append_to_streams(conn, &1, new_opts))
    end
    |> maybe_retry_once(conn, opts, &append_to_streams(conn, appends, &1))
  end

  def link_to_stream(conn, stream_uuid, expected_version, events_or_event_ids, opts) do
    transaction(
      conn,
      fn transaction ->
        with {:ok, stream} <- stream_info(transaction, stream_uuid, expected_version, opts),
             {:ok, stream} <- prepare_stream(transaction, stream, opts),
             :ok <- do_link_to_storage(transaction, stream, events_or_event_ids, opts) do
          :ok
        else
          {:error, error} -> Postgrex.rollback(transaction, error)
        end
      end,
      opts
    )
  end

  def paginate_streams(conn, opts), do: Storage.paginate_streams(conn, opts)

  def read_stream_forward(conn, stream_uuid, start_version, count, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      read_storage_forward(conn, stream, start_version, count, opts)
    end
  end

  def read_stream_backward(conn, stream_uuid, start_version, count, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      read_storage_backward(conn, stream, start_version, count, opts)
    end
  end

  def stream_forward(conn, stream_uuid, start_version, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      stream_storage_forward(conn, stream, start_version, opts)
    end
  end

  def stream_backward(conn, stream_uuid, start_version, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      stream_storage_backward(conn, stream, start_version, opts)
    end
  end

  def read_query(conn, items, opts) do
    {command_tag, opts} = Keyword.pop(opts, :command_tag)
    {direction, bound, opts} = pop_direction_and_bound(opts)
    {count, opts} = Keyword.pop(opts, :count)

    count = validate_count(count)

    with {:ok, query} <- Query.compile(items),
         :ok <- validate_command_tag(query, command_tag) do
      read_query_storage(conn, query, direction, bound, count, command_tag, opts)
    end
  end

  def stream_query(conn, items, opts) do
    {direction, bound, opts} = pop_direction_and_bound(opts)

    with {:ok, query} <- Query.compile(items) do
      stream_query_storage(conn, query, direction, bound, opts)
    end
  end

  def start_from(_conn, _stream_uuid, :origin, _opts), do: {:ok, 0}

  def start_from(conn, stream_uuid, :current, opts),
    do: stream_version(conn, stream_uuid, opts)

  def start_from(_conn, _stream_uuid, start_from, _opts) when is_integer(start_from),
    do: {:ok, start_from}

  def start_from(_conn, _stream_uuid, _start_from, _opts),
    do: {:error, :invalid_start_from}

  def stream_version(conn, stream_uuid, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :any_version, opts) do
      %StreamInfo{stream_version: stream_version} = stream

      {:ok, stream_version}
    end
  end

  def delete(conn, stream_uuid, expected_version, :soft, opts) do
    with {:ok, %StreamInfo{} = stream} <- stream_info(conn, stream_uuid, expected_version, opts) do
      soft_delete_stream(conn, stream, opts)
    end
  end

  def delete(conn, stream_uuid, expected_version, :hard, opts) do
    with {:ok, %StreamInfo{} = stream} <- stream_info(conn, stream_uuid, expected_version, opts) do
      hard_delete_stream(conn, stream, opts)
    end
  end

  def stream_info(conn, stream_uuid, expected_version, opts) do
    opts = query_opts(opts)

    with {:ok, stream_info} <- Storage.stream_info(conn, stream_uuid, opts),
         :ok <- StreamInfo.validate_expected_version(stream_info, expected_version) do
      {:ok, stream_info}
    end
  end

  # Create stream when it doesn't yet exist.
  defp prepare_stream(conn, %StreamInfo{stream_id: nil} = stream, opts) do
    %StreamInfo{stream_uuid: stream_uuid} = stream

    opts = query_opts(opts)

    with {:ok, stream_id} <- Storage.create_stream(conn, stream_uuid, opts) do
      {:ok, %StreamInfo{stream | stream_id: stream_id}}
    end
  end

  # Stream already exists, nothing to do.
  defp prepare_stream(_conn, %StreamInfo{} = stream, _opts), do: {:ok, stream}

  defp do_append_to_storage(
         conn,
         %StreamInfo{} = stream,
         events,
         expected_version,
         serializer,
         opts
       ) do
    prepared_events = prepare_events(events, stream, serializer, opts)

    write_to_stream(conn, prepared_events, stream, expected_version, unique_tags(events), opts)
  end

  # One list per event. The `uniq` is load-bearing: the appender reads any unique
  # violation it does not name, `event_tags_pkey` included, as a stale version.
  defp unique_tags(events), do: Enum.map(events, &Enum.uniq(&1.tags))

  defp prepare_member({{_stream_uuid, expected_version, events}, stream}, serializer, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    %{
      stream_id: stream_id,
      expected_version: expected_version,
      events: prepare_events(events, stream, serializer, opts),
      tags: unique_tags(events)
    }
  end

  # Every member's version check, so a stale member answers before the function runs.
  defp streams_info(conn, appends, opts) do
    appends
    |> Enum.reduce_while([], fn {stream_uuid, expected_version, _events}, streams ->
      case stream_info(conn, stream_uuid, expected_version, opts) do
        {:ok, stream} -> {:cont, [stream | streams]}
        {:error, _error} = reply -> {:halt, reply}
      end
    end)
    |> case do
      {:error, _error} = reply -> reply
      streams -> {:ok, Enum.reverse(streams)}
    end
  end

  defp stream_uuids(appends), do: Enum.map(appends, &elem(&1, 0))

  # Runs first, as `validate_tags/1` does and for the same reason. A repeated stream is
  # refused, whatever its expected versions: a repeat cannot name one base, and its
  # positions could not both be right.
  defp validate_appends(appends) do
    if valid_appends?(appends) do
      stream_uuids = stream_uuids(appends)

      cond do
        @all_stream in stream_uuids -> {:error, :cannot_append_to_all_stream}
        stream_uuids != Enum.uniq(stream_uuids) -> {:error, :repeated_stream}
        true -> :ok
      end
    else
      {:error, :invalid_append}
    end
  end

  defp valid_appends?([_append | _appends] = appends), do: well_formed_appends?(appends)
  defp valid_appends?(_appends), do: false

  defp well_formed_appends?([]), do: true

  defp well_formed_appends?([{stream_uuid, expected_version, [_event | _events]} | appends])
       when is_binary(stream_uuid) and
              (expected_version in [:any_version, :no_stream, :stream_exists] or
                 (is_integer(expected_version) and expected_version >= 0)),
       do: well_formed_appends?(appends)

  defp well_formed_appends?(_appends), do: false

  # Runs first in both `append_to_stream/5` clauses, so a refusal sends no statement at
  # all, not even a BEGIN, and answers before `stream_info/4` can. A caller's transaction
  # stays alive: the table's CHECK, or a rollback here, would abort it.
  defp validate_tags(events) do
    too_long? = fn tag -> byte_size(tag) > EventData.max_tag_bytes() end

    if Enum.any?(events, fn event -> Enum.any?(event.tags, too_long?) end) do
      {:error, :tag_too_long}
    else
      :ok
    end
  end

  # Runs where `validate_tags/1` does, for the same reason: a refusal sends no statement.
  # `opts` come back with the compiled condition, and without one a plain append's are
  # untouched. `Keyword.get_values/2` because `condition: nil` is a refused condition, not
  # a plain append, and because a repeated option must not leave one condition unchecked.
  defp compile_condition(stream_uuids, opts) do
    case Keyword.get_values(opts, :condition) do
      [] ->
        {:ok, opts}

      [condition] ->
        with {:ok, %AppendCondition{} = compiled} <- AppendCondition.compile(condition),
             :ok <- validate_declared(stream_uuids, compiled, opts) do
          {:ok, Keyword.put(opts, :condition, compiled)}
        end

      [_condition, _repeat | _repeats] ->
        {:error, :invalid_condition}
    end
  end

  # Declared by the store's config, which `opts` carry. `Decree.Store` passes no caller
  # option of this name through, so a caller cannot declare its own append. Every member
  # must start with a declared prefix, or the condition's `target` must be declared.
  defp validate_declared(stream_uuids, %AppendCondition{target: target}, opts) do
    declared = Keyword.get(opts, :conditional_appends, [])
    prefixes = Keyword.get(declared, :stream_prefixes, [])
    targets = Keyword.get(declared, :targets, [])

    prefixed? = Enum.all?(stream_uuids, &String.starts_with?(&1, prefixes))

    if prefixed? or (not is_nil(target) and target in targets) do
      :ok
    else
      {:error, :undeclared_conditional_target}
    end
  end

  defp prepare_events(events, %StreamInfo{} = stream, serializer, opts) do
    %StreamInfo{stream_uuid: stream_uuid, stream_version: stream_version} = stream

    events
    |> Enum.map(&map_to_recorded_event(&1, opts[:created_at_override] || utc_now(), serializer))
    |> Enum.with_index(1)
    |> Enum.map(fn {%RecordedEvent{} = recorded_event, index} ->
      %RecordedEvent{
        recorded_event
        | stream_uuid: stream_uuid,
          stream_version: stream_version + index
      }
    end)
  end

  defp map_to_recorded_event(
         %EventData{data: %{__struct__: event_type}, event_type: nil} = event,
         created_at,
         serializer
       ) do
    %{event | event_type: Atom.to_string(event_type)}
    |> map_to_recorded_event(created_at, serializer)
  end

  defp map_to_recorded_event(%EventData{} = event_data, created_at, serializer) do
    %EventData{
      event_id: event_id,
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata
    } = event_data

    %RecordedEvent{
      event_id: event_id || UUID.uuid4(),
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: serializer.serialize(data),
      metadata: serializer.serialize(metadata),
      created_at: created_at
    }
  end

  defp do_link_to_storage(conn, %StreamInfo{} = stream, events_or_event_ids, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    event_ids = Enum.map(events_or_event_ids, &extract_event_id/1)

    Storage.link_to_stream(conn, stream_id, event_ids, opts)
  end

  defp extract_event_id(%RecordedEvent{event_id: event_id}), do: event_id
  defp extract_event_id(event_id) when is_binary(event_id), do: event_id

  defp extract_event_id(invalid) do
    raise ArgumentError, message: "Invalid event id, expected a UUID but got: #{inspect(invalid)}"
  end

  defp write_to_stream(
         conn,
         prepared_events,
         %StreamInfo{} = stream,
         expected_version,
         tags,
         opts
       ) do
    %StreamInfo{stream_id: stream_id} = stream

    opts = Keyword.merge(opts, expected_version: expected_version, tags: tags)

    Storage.append_to_stream(conn, stream_id, prepared_events, opts)
  end

  defp read_storage_forward(conn, %StreamInfo{} = stream, start_version, count, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    {serializer, opts} = Keyword.pop(opts, :serializer)

    with {:ok, recorded_events} <-
           Storage.read_stream_forward(conn, stream_id, start_version, count, opts) do
      deserialized_events = deserialize_recorded_events(recorded_events, serializer)

      {:ok, deserialized_events}
    end
  end

  defp read_storage_backward(conn, %StreamInfo{} = stream, start_version, count, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    {serializer, opts} = Keyword.pop(opts, :serializer)

    with {:ok, recorded_events} <-
           Storage.read_stream_backward(conn, stream_id, start_version, count, opts) do
      deserialized_events = deserialize_recorded_events(recorded_events, serializer)

      {:ok, deserialized_events}
    end
  end

  # Stream forwards from the first event in the stream.
  defp stream_storage_forward(conn, stream, 0, opts),
    do: stream_storage_forward(conn, stream, 1, opts)

  defp stream_storage_forward(conn, stream, start_version, opts) do
    read_batch_size = Keyword.fetch!(opts, :read_batch_size)

    Elixir.Stream.resource(
      fn -> start_version end,
      fn next_version ->
        case read_storage_forward(conn, stream, next_version, read_batch_size, opts) do
          {:ok, []} -> {:halt, next_version}
          {:ok, events} -> {events, List.last(events).event_number + 1}
        end
      end,
      fn _next_version -> :ok end
    )
  end

  # Stream backwards from the last event in the stream.
  defp stream_storage_backward(conn, stream, -1, opts) do
    %StreamInfo{stream_version: stream_version} = stream

    stream_storage_backward(conn, stream, stream_version, opts)
  end

  defp stream_storage_backward(conn, stream, start_version, opts) do
    read_batch_size = Keyword.fetch!(opts, :read_batch_size)

    Elixir.Stream.resource(
      fn -> start_version end,
      fn
        next_version when next_version <= 0 ->
          {:halt, 0}

        next_version ->
          case read_storage_backward(conn, stream, next_version, read_batch_size, opts) do
            {:ok, []} -> {:halt, next_version}
            {:ok, events} -> {events, List.last(events).event_number - 1}
          end
      end,
      fn _next_version -> :ok end
    )
  end

  defp read_query_storage(conn, query, direction, bound, count, command_tag, opts) do
    {serializer, opts} = Keyword.pop(opts, :serializer)

    with {:ok, %ReadResult{events: events} = result} <-
           Storage.read_query(conn, query, direction, bound, count, command_tag, opts) do
      {:ok, %ReadResult{result | events: deserialize_recorded_events(events, serializer)}}
    end
  end

  defp stream_query_storage(conn, query, direction, bound, opts) do
    read_batch_size = validate_read_batch_size(Keyword.fetch!(opts, :read_batch_size))

    Elixir.Stream.resource(
      fn -> bound end,
      fn
        next_bound when direction == :backward and next_bound <= 0 ->
          {:halt, next_bound}

        next_bound ->
          case read_query_storage(conn, query, direction, next_bound, read_batch_size, nil, opts) do
            {:ok, %ReadResult{events: []}} -> {:halt, next_bound}
            {:ok, %ReadResult{events: events}} -> {events, next_query_bound(direction, events)}
          end
      end,
      fn _next_bound -> :ok end
    )
  end

  defp next_query_bound(:forward, events), do: List.last(events).event_number + 1
  defp next_query_bound(:backward, events), do: List.last(events).event_number - 1

  defp pop_direction_and_bound(opts) do
    {direction, opts} = Keyword.pop(opts, :direction, :forward)
    direction = validate_direction(direction)

    {from, opts} = Keyword.pop(opts, :from, default_from(direction))

    {direction, query_bound(direction, from), opts}
  end

  defp default_from(:forward), do: 0
  defp default_from(:backward), do: -1

  # The head has no text of its own, as `OR $n = -1` would defeat the index.
  defp query_bound(:backward, -1), do: @max_bigint
  defp query_bound(_direction, from) when is_bigint(from), do: from

  defp query_bound(_direction, invalid) do
    raise ArgumentError, message: "expected `:from` to be an integer, got: " <> inspect(invalid)
  end

  defp validate_direction(direction) when direction in [:forward, :backward], do: direction

  defp validate_direction(invalid) do
    raise ArgumentError,
      message: "expected `:direction` to be `:forward` or `:backward`, got: " <> inspect(invalid)
  end

  defp validate_count(count) when is_bigint(count) and count >= 0, do: count

  defp validate_count(invalid) do
    raise ArgumentError,
      message: "expected `:count` to be a non-negative integer, got: " <> inspect(invalid)
  end

  # An empty page ends a stream, so a batch of nothing would end it at once.
  defp validate_read_batch_size(size) when is_bigint(size) and size > 0, do: size

  defp validate_read_batch_size(invalid) do
    raise ArgumentError,
      message: "expected `:read_batch_size` to be a positive integer, got: " <> inspect(invalid)
  end

  # A tag no item names reads as "no prior command" for every event, so a typo must not
  # reach the database.
  defp validate_command_tag(_query, nil), do: :ok

  defp validate_command_tag(query, command_tag) do
    if is_binary(command_tag) and Query.names_tag?(query, command_tag) do
      :ok
    else
      {:error, :invalid_query}
    end
  end

  defp deserialize_recorded_events(recorded_events, serializer),
    do: Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer))

  defp soft_delete_stream(conn, stream, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    opts = query_opts(opts)

    Storage.soft_delete_stream(conn, stream_id, opts)
  end

  defp hard_delete_stream(conn, stream, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    if Keyword.fetch!(opts, :enable_hard_deletes) do
      opts = query_opts(opts)

      transaction(
        conn,
        fn transaction ->
          with :ok <- set_enable_hard_deletes(transaction),
               :ok <- Storage.hard_delete_stream(transaction, stream_id, opts) do
            :ok
          else
            {:error, error} -> Postgrex.rollback(transaction, error)
          end
        end,
        opts
      )
    else
      {:error, :not_supported}
    end
  end

  defp set_enable_hard_deletes(conn) do
    query = "SET SESSION eventstore.enable_hard_deletes TO 'on';"

    with {:ok, %Postgrex.Result{}} <- Postgrex.query(conn, query, []) do
      :ok
    end
  end

  defp retry_append(conn, stream_uuid, expected_version, events) do
    &append_to_stream(conn, stream_uuid, expected_version, events, &1)
  end

  # `retry` runs the whole append again with the given opts, from its stream-info reads.
  # Not inside a caller's transaction: the unique violation aborted it, so the read would
  # answer `in_failed_sql_transaction`, and the caller decides. A `DBConnection.run/3`
  # checkout is not one, and retries. `Decree.Store` takes no `mode: :savepoint`, which would
  # keep the transaction alive, so this does not ask: `DBConnection.status/1` disconnects a
  # connection marked aborted, as the path for 1,000 or more events leaves a caller's.
  defp maybe_retry_once(
         {:error, :duplicate_stream_uuid} = error,
         %DBConnection{conn_mode: :transaction},
         _opts,
         _retry
       ),
       do: error

  defp maybe_retry_once({:error, :duplicate_stream_uuid}, _conn, opts, retry) do
    unless Keyword.has_key?(opts, :retried_once) do
      retry.(Keyword.put(opts, :retried_once, true))
    else
      {:error, {:already_retried_once, :duplicate_stream_uuid}}
    end
  end

  # A deadlock victim's transaction rolled back, so nothing was written and the append
  # can run again, from its stream-info read. Not inside a caller's transaction, a
  # `%DBConnection{}` that `conn_mode: :transaction` marks: that transaction is aborted
  # and the caller decides. A `DBConnection.run/3` checkout is not one, and retries.
  # The path for 1,000 or more events retries around its own whole transaction.
  defp maybe_retry_once(
         {:error, %AppendError{code: :deadlock_detected}} = error,
         %DBConnection{conn_mode: :transaction},
         _opts,
         _retry
       ),
       do: error

  defp maybe_retry_once(
         {:error, %AppendError{code: :deadlock_detected}} = error,
         _conn,
         opts,
         retry
       ) do
    if Keyword.has_key?(opts, :retried_deadlock_once) do
      error
    else
      retry.(Keyword.put(opts, :retried_deadlock_once, true))
    end
  end

  defp maybe_retry_once(reply, _conn, _opts, _retry), do: reply

  defp transaction(conn, transaction_fun, opts) do
    case Postgrex.transaction(conn, transaction_fun, opts) do
      {:ok, :ok} -> :ok
      {:error, _error} = reply -> reply
    end
  end

  defp query_opts(opts), do: Keyword.take(opts, [:schema, :timeout])

  # Returns the current date time in UTC.
  defp utc_now, do: DateTime.utc_now()
end
