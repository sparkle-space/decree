defmodule Decree.Store.Streams.Stream do
  @moduledoc false

  alias Decree.Store.{
    AppendCondition,
    AppendError,
    AppendResult,
    EventData,
    Query,
    ReadResult,
    RecordedEvent,
    Storage,
    TagIndex,
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
         :ok <- validate_tag_index_facts(events),
         :ok <- validate_required_tags(events),
         :ok <- validate_deletable_types([{stream_uuid, expected_version, events}], new_opts),
         :ok <- validate_returning(new_opts),
         {:ok, new_opts} <- compile_condition([stream_uuid], new_opts),
         {:ok, stream} <- stream_info(conn, stream_uuid, expected_version, new_opts) do
      do_append_to_storage(conn, stream, events, expected_version, serializer, new_opts)
    end
    |> maybe_retry_once(conn, opts, retry_append(conn, stream_uuid, expected_version, events))
  end

  def append_to_stream(conn, stream_uuid, expected_version, events, opts) do
    {serializer, new_opts} = Keyword.pop(opts, :serializer)

    with :ok <- validate_tags(events),
         :ok <- validate_tag_index_facts(events),
         :ok <- validate_required_tags(events),
         :ok <- validate_deletable_types([{stream_uuid, expected_version, events}], new_opts),
         :ok <- validate_returning(new_opts),
         {:ok, new_opts} <- compile_condition([stream_uuid], new_opts) do
      transaction(
        conn,
        fn transaction ->
          with {:ok, stream} <- stream_info(transaction, stream_uuid, expected_version, new_opts) do
            do_append_to_storage(
              transaction,
              stream,
              events,
              expected_version,
              serializer,
              new_opts
            )
          end
          |> case do
            {:error, error} -> Postgrex.rollback(transaction, error)
            reply -> reply
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
         events = Enum.flat_map(appends, fn {_stream, _version, events} -> events end),
         :ok <- validate_tags(events),
         :ok <- validate_tag_index_facts(events),
         :ok <- validate_required_tags(events),
         :ok <- validate_deletable_types(appends, new_opts),
         :ok <- validate_returning(new_opts),
         {:ok, new_opts} <- compile_condition(stream_uuids(appends), new_opts),
         {:ok, streams} <- streams_info(conn, appends, new_opts) do
      appends
      |> Enum.zip(streams)
      |> Enum.map(&prepare_member(&1, serializer, new_opts))
      |> then(&Storage.append_to_streams(conn, &1, new_opts))
    end
    |> maybe_retry_once(conn, opts, &append_to_streams(conn, appends, &1))
  end

  def read_stream_forward(conn, stream_uuid, start_version, count, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      read_storage_forward(conn, stream, start_version, count, opts)
    end
  end

  # A subscription's catch-up read: answers `{:ok, events, nil, stream_id}`, or, when it
  # meets an event it must hold before, `{:ok, prefix, {event_number, event_type, cause},
  # stream_id}` with the events before it, the cause `:unknown_type` for a type the
  # serializer does not know or `{:undecodable, class}` for a known type that would not
  # deserialize (`RecordedEvent.deserialize_until_hold/2`). Never raises for either.
  # `stream_id` is the id of the stream the events were read from, by which a subscription
  # tells a stream created again after a hard delete from the one it was reading
  # (sparkle-space/sparklespace#8386).
  def read_stream_forward_known(conn, stream_uuid, start_version, count, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      %StreamInfo{stream_id: stream_id} = stream

      {serializer, opts} = Keyword.pop(opts, :serializer)

      with {:ok, recorded_events} <-
             Storage.read_stream_forward(conn, stream_id, start_version, count, opts) do
        {events, pending} = RecordedEvent.deserialize_until_hold(recorded_events, serializer)

        {:ok, events, pending, stream_id}
      end
    end
  end

  # The type of the event at `event_number` in the stream, read without deserializing it:
  # `{:ok, type}`, `{:ok, nil}` when there is no such event, or `{:error, reason}`.
  def read_event_type(conn, stream_uuid, event_number, opts) do
    with {:ok, stream} <- stream_info(conn, stream_uuid, :stream_exists, opts) do
      %StreamInfo{stream_id: stream_id} = stream

      {_serializer, opts} = Keyword.pop(opts, :serializer)

      case Storage.read_stream_forward(conn, stream_id, event_number, 1, opts) do
        {:ok, [%RecordedEvent{event_number: ^event_number, event_type: type}]} -> {:ok, type}
        {:ok, _events} -> {:ok, nil}
        {:error, _error} = reply -> reply
      end
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
    {tag_items, opts} = Keyword.pop(opts, :tag_items, [])
    {position_items, opts} = Keyword.pop(opts, :position_items, [])
    {direction, bound, opts} = pop_direction_and_bound(opts)
    {count, opts} = Keyword.pop(opts, :count)

    count = validate_count(count)

    with {:ok, query} <- Query.compile(items),
         :ok <- validate_deletable(query, opts),
         :ok <- validate_command_tag(query, command_tag),
         :ok <- validate_item_indexes(tag_items, length(items)),
         :ok <- validate_item_indexes(position_items, length(items)) do
      opts = Keyword.put(opts, :position_items, position_items)

      read_query_storage(conn, query, direction, bound, count, command_tag, tag_items, opts)
    end
  end

  def read_tags(_conn, [], _opts), do: {:ok, %{}}

  def read_tags(conn, positions, opts) do
    with :ok <- validate_positions(positions) do
      deletable = Keyword.get(opts, :deletable, [])
      tag_kinds = Keyword.get(deletable, :tag_kinds, [])
      event_types = Keyword.get(deletable, :event_types, [])

      Storage.read_tags(conn, positions, tag_kinds, event_types, opts)
    end
  end

  def stream_query(conn, items, opts) do
    {direction, bound, opts} = pop_direction_and_bound(opts)

    with {:ok, query} <- Query.compile(items),
         :ok <- validate_deletable(query, opts) do
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
    with :ok <- validate_hard_delete_target(stream_uuid, opts),
         {:ok, %StreamInfo{} = stream} <- stream_info(conn, stream_uuid, expected_version, opts) do
      hard_delete_stream(conn, stream, expected_version, opts)
    end
  end

  # Read through `Storage.stream_info/3` and not `stream_info/4`, because erasure must still
  # reach a soft-deleted stream. A stream with no row sends `nil` down, whose lock finds
  # nothing, and that answers `:stream_not_found` as a stream gone since this read does.
  def delete_tags(conn, stream_uuid, tag_kinds, opts) do
    with :ok <- validate_tag_kinds(tag_kinds),
         :ok <- validate_hard_deletes(opts),
         opts = query_opts(opts),
         {:ok, %StreamInfo{stream_id: stream_id}} <- Storage.stream_info(conn, stream_uuid, opts) do
      Postgrex.transaction(
        conn,
        fn transaction ->
          with :ok <- set_enable_hard_deletes(transaction),
               {:ok, _count} = reply <-
                 Storage.delete_stream_tags(transaction, stream_id, tag_kinds, opts),
               :ok <- close_hard_deletes(transaction) do
            reply
          else
            {:error, error} -> Postgrex.rollback(transaction, error)
          end
        end,
        opts
      )
      |> case do
        {:ok, {:ok, count}} -> {:ok, count}
        {:error, _error} = reply -> reply
      end
    end
  end

  def stream_info(conn, stream_uuid, expected_version, opts) do
    opts = query_opts(opts)

    with {:ok, stream_info} <- Storage.stream_info(conn, stream_uuid, opts),
         :ok <- StreamInfo.validate_expected_version(stream_info, expected_version) do
      {:ok, stream_info}
    end
  end

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
    cond do
      any_tag?(events, &(not EventData.valid_tag?(&1))) -> {:error, :invalid_tag}
      any_tag?(events, &(byte_size(&1) > EventData.max_tag_bytes())) -> {:error, :tag_too_long}
      true -> :ok
    end
  end

  defp any_tag?(events, fun), do: Enum.any?(events, &Enum.any?(&1.tags, fun))

  # Runs straight after `validate_tags/1`, for the same reason. A disable whose state tag lacks
  # the fact tag is invisible to the read that finds the facts, so an enable before it would
  # stand: see `Decree.Store.TagIndex`.
  defp validate_tag_index_facts(events) do
    if Enum.all?(events, &TagIndex.valid_tags?(&1.tags)) do
      :ok
    else
      {:error, :invalid_tag_index_fact}
    end
  end

  # Runs straight after `validate_tags/1`, for the same reason, and on what that let through:
  # every tag is a valid binary. The kinds are checked in this function because a condition
  # is only as good as the tags: nothing past here could tell an absent tag from no match.
  defp validate_required_tags(events) do
    cond do
      not Enum.all?(events, &Query.tag_kinds?(&1.required_tag_kinds)) ->
        {:error, :invalid_required_tag_kinds}

      Enum.all?(events, &carries_required?/1) ->
        :ok

      true ->
        {:error, :missing_required_tag}
    end
  end

  defp carries_required?(%EventData{tags: tags, required_tag_kinds: kinds}) do
    Enum.all?(kinds, fn kind -> Enum.any?(tags, &carries?(&1, kind)) end)
  end

  # This runs per event, so it matches on the bytes rather than split the tag. A tag with no
  # colon is its own kind, and `<kind>:` with no value carries nothing.
  defp carries?(tag, kind) do
    size = byte_size(kind)

    case tag do
      ^kind -> true
      <<^kind::binary-size(^size), ":", _value, _rest::binary>> -> true
      _other -> false
    end
  end

  # Runs where `validate_tags/1` does, for the same reason: a refusal sends no statement.
  # `Keyword.get_values/2` for what `compile_condition/2` says of it.
  defp validate_returning(opts) do
    case Keyword.get_values(opts, :returning) do
      [] -> :ok
      [:append_result] -> :ok
      _invalid -> {:error, :invalid_returning}
    end
  end

  # Runs where `validate_tags/1` does, for the same reason: a refusal sends no statement.
  # `opts` come back with the compiled condition, and without one a plain append's are
  # untouched. `Keyword.get_values/2` because `condition: nil` is a refused condition, not
  # a plain append, and because a repeated option must not leave one condition unchecked.
  defp compile_condition(stream_uuids, opts) do
    case Keyword.get_values(opts, :condition) do
      [] ->
        with :ok <- validate_plain_append(stream_uuids, opts), do: {:ok, opts}

      [condition] ->
        with {:ok, %AppendCondition{} = compiled} <- AppendCondition.compile(condition),
             :ok <- validate_deletable(compiled.query, opts),
             :ok <- validate_declared(stream_uuids, compiled, opts) do
          {:ok, Keyword.put(opts, :condition, compiled)}
        end

      [_condition, _repeat | _repeats] ->
        {:error, :invalid_condition}
    end
  end

  # A declared prefix is a decision's: everything on such a stream was decided against a
  # condition, and an append without one would write what no decision saw. One such member
  # refuses the batch. `compile_condition/2` calls this from its one arm for no condition,
  # which every append crosses, so the refusal lives once.
  defp validate_plain_append(stream_uuids, opts) do
    prefixes = conditional_prefixes(opts)

    if Enum.any?(stream_uuids, &String.starts_with?(&1, prefixes)) do
      {:error, :plain_append_to_conditional_prefix}
    else
      :ok
    end
  end

  defp conditional_prefixes(opts) do
    opts |> Keyword.get(:conditional_appends, []) |> Keyword.get(:stream_prefixes, [])
  end

  # Declared by the store's config, which `opts` carry. `Decree.Store` passes no caller
  # option of this name through, so a caller cannot declare its own append. The condition's
  # shape picks the one declaration it needs: a `target` must be declared, whatever its
  # streams are named, and with none every member must start with a declared prefix. A
  # prefix never stands in for a target, or a rule's aggregate would write into a decision's
  # prefix undeclared.
  defp validate_declared(stream_uuids, %AppendCondition{target: target}, opts) do
    prefixes = conditional_prefixes(opts)
    targets = opts |> Keyword.get(:conditional_appends, []) |> Keyword.get(:targets, [])

    declared? =
      case target do
        nil -> Enum.all?(stream_uuids, &String.starts_with?(&1, prefixes))
        _target -> target in targets
      end

    if declared? do
      :ok
    else
      {:error, :undeclared_conditional_target}
    end
  end

  # The one check a read and a condition share, so that nothing can decide on what a hard
  # delete may remove. Declared by the store's config, as `validate_declared/3` is, and it
  # sends no statement.
  defp validate_deletable(%Query{} = query, opts) do
    if Query.names_deletable?(query, Keyword.get(opts, :deletable, [])) do
      {:error, :query_names_deletable}
    else
      :ok
    end
  end

  # What gives a declared prefix its meaning: without this a forgotten event type stays
  # queryable, and vanishes under a decision. Runs where `validate_tags/1` does, for the
  # same reason: a refusal sends no statement.
  defp validate_deletable_types(appends, opts) do
    case Keyword.get(opts, :deletable) do
      nil ->
        :ok

      deletable ->
        prefixes = Keyword.get(deletable, :stream_prefixes, [])
        types = Keyword.get(deletable, :event_types, [])

        undeclared? = fn {stream_uuid, _expected_version, events} ->
          String.starts_with?(stream_uuid, prefixes) and
            Enum.any?(events, &(event_type(&1) not in types))
        end

        if Enum.any?(appends, undeclared?) do
          {:error, :undeclared_deletable_event_type}
        else
          :ok
        end
    end
  end

  # A stream under a declared prefix is what later decisions rest on, its `command:` tags
  # included. One a `deletable` prefix also covers has been declared as both, and "Deletable
  # streams" states that cost. Runs where `validate_tags/1` does, for the same reason: a
  # refusal sends no statement. A store with no hard deletes is passed on, to answer as it
  # does in `hard_delete_stream/4`.
  defp validate_hard_delete_target(stream_uuid, opts) do
    if validate_hard_deletes(opts) == :ok and
         String.starts_with?(stream_uuid, conditional_prefixes(opts)) and
         not String.starts_with?(stream_uuid, deletable_prefixes(opts)) do
      {:error, :hard_delete_of_conditional_prefix}
    else
      :ok
    end
  end

  defp deletable_prefixes(opts) do
    opts |> Keyword.get(:deletable, []) |> Keyword.get(:stream_prefixes, [])
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

  # The type an event is stored under.
  defp event_type(%EventData{data: %{__struct__: event_type}, event_type: nil}),
    do: Atom.to_string(event_type)

  defp event_type(%EventData{event_type: event_type}), do: event_type

  defp map_to_recorded_event(%EventData{} = event_data, created_at, serializer) do
    %EventData{
      event_id: event_id,
      causation_id: causation_id,
      correlation_id: correlation_id,
      data: data,
      metadata: metadata
    } = event_data

    %RecordedEvent{
      event_id: event_id || UUID.uuid4(),
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type(event_data),
      data: serializer.serialize(data),
      metadata: serializer.serialize(metadata),
      created_at: created_at
    }
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

  defp read_query_storage(conn, query, direction, bound, count, command_tag, tag_items, opts) do
    {serializer, opts} = Keyword.pop(opts, :serializer)

    with {:ok, %ReadResult{events: events} = result} <-
           Storage.read_query(conn, query, direction, bound, count, command_tag, tag_items, opts) do
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
          case read_query_storage(
                 conn,
                 query,
                 direction,
                 next_bound,
                 read_batch_size,
                 nil,
                 [],
                 opts
               ) do
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

  # An index no item has reads no tag and answers no match by position, so, as with
  # `command_tag`, a typo must not reach the database. Walked by hand because an improper list
  # must answer, not raise.
  defp validate_item_indexes([], _count), do: :ok

  defp validate_item_indexes([index | indexes], count)
       when is_integer(index) and index >= 0 and index < count,
       do: validate_item_indexes(indexes, count)

  defp validate_item_indexes(_indexes, _count), do: {:error, :invalid_query}

  # Walked by hand because an improper list must answer, not raise. `$1` is a `bigint[]`, so
  # a position past a bigint could not be sent, and a refusal here leaves a caller's
  # transaction alive.
  defp validate_positions([]), do: :ok

  defp validate_positions([position | positions]) when is_bigint(position) and position >= 0,
    do: validate_positions(positions)

  defp validate_positions(_positions), do: {:error, :invalid_positions}

  # `command` rows are what a decision reads to refuse a resend, so erasing them would let one
  # `command_id` commit twice.
  defp validate_tag_kinds([_kind | _kinds] = tag_kinds) do
    if Query.tag_kinds?(tag_kinds) and "command" not in tag_kinds and
         not Enum.any?(tag_kinds, &(&1 in TagIndex.reserved_kinds())) do
      :ok
    else
      {:error, :invalid_tag_kinds}
    end
  end

  defp validate_tag_kinds(_tag_kinds), do: {:error, :invalid_tag_kinds}

  defp validate_hard_deletes(opts) do
    if Keyword.fetch!(opts, :enable_hard_deletes), do: :ok, else: {:error, :not_supported}
  end

  defp deserialize_recorded_events(recorded_events, serializer),
    do: Enum.map(recorded_events, &RecordedEvent.deserialize(&1, serializer))

  defp soft_delete_stream(conn, stream, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    opts = query_opts(opts)

    Storage.soft_delete_stream(conn, stream_id, opts)
  end

  defp hard_delete_stream(conn, stream, expected_version, opts) do
    %StreamInfo{stream_id: stream_id} = stream

    with :ok <- validate_hard_deletes(opts) do
      opts = query_opts(opts)
      delete_opts = Keyword.put(opts, :expected_version, expected_version)

      transaction(
        conn,
        fn transaction ->
          with :ok <- set_enable_hard_deletes(transaction),
               :ok <- Storage.hard_delete_stream(transaction, stream_id, delete_opts),
               :ok <- close_hard_deletes(transaction) do
            :ok
          else
            {:error, error} -> Postgrex.rollback(transaction, error)
          end
        end,
        opts
      )
    end
  end

  # LOCAL, so the setting cannot outlive the transaction: a SET SESSION would outlive the
  # commit and leave this pooled connection able to delete anything. `close_hard_deletes/1`
  # closes it again before the call returns, so a caller's transaction does not keep it.
  defp set_enable_hard_deletes(conn) do
    query = "SET LOCAL eventstore.enable_hard_deletes TO 'on';"

    with {:ok, %Postgrex.Result{}} <- Postgrex.query(conn, query, []) do
      :ok
    end
  end

  # A nested `Postgrex.transaction/3` is the caller's transaction, with no savepoint, and a
  # released savepoint would keep a SET LOCAL anyway. So this is the transaction function's
  # last statement on success. It closes the fence and does not restore a prior value: a
  # caller wanting its own fence sets it again. A failure needs no close, because an SQL error
  # aborts the transaction and `Postgrex.rollback/2` marks it rolled back, so nothing further
  # runs in it.
  defp close_hard_deletes(conn) do
    query = "SET LOCAL eventstore.enable_hard_deletes TO 'off';"

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

  # A deadlock victim's transaction rolled back, so nothing was written and the append can
  # run again, from its stream-info read. Not inside a caller's transaction, a
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
      {:ok, {:ok, %AppendResult{}} = reply} -> reply
      {:error, _error} = reply -> reply
    end
  end

  defp query_opts(opts), do: Keyword.take(opts, [:schema, :timeout])

  # Returns the current date time in UTC.
  defp utc_now, do: DateTime.utc_now()
end
