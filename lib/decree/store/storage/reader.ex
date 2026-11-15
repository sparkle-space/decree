defmodule Decree.Store.Storage.Reader do
  @moduledoc false

  require Logger

  alias Decree.Store.{ReadResult, RecordedEvent, UUID}
  alias Decree.Store.Sql.Statements
  alias Decree.Store.Storage.Reader

  @all_stream_id 0

  @doc """
  Read events from a single stream forwards from the given starting version.
  """
  def read_forward(conn, stream_id, start_version, count, opts) do
    {correlation_id_type, opts} = Keyword.pop(opts, :correlation_id_type, "uuid")
    {causation_id_type, opts} = Keyword.pop(opts, :causation_id_type, "uuid")

    case Reader.Query.read_events_forward(conn, stream_id, start_version, count, opts) do
      {:ok, []} = reply -> reply
      {:ok, rows} -> map_rows_to_event_data(rows, correlation_id_type, causation_id_type)
      {:error, reason} -> failed_to_read(stream_id, reason)
    end
  end

  @doc """
  Read events from a single stream backwards from the given starting version.
  """
  def read_backward(conn, stream_id, start_version, count, opts) do
    {correlation_id_type, opts} = Keyword.pop(opts, :correlation_id_type, "uuid")
    {causation_id_type, opts} = Keyword.pop(opts, :causation_id_type, "uuid")

    case Reader.Query.read_events_backward(conn, stream_id, start_version, count, opts) do
      {:ok, []} = reply -> reply
      {:ok, rows} -> map_rows_to_event_data(rows, correlation_id_type, causation_id_type)
      {:error, reason} -> failed_to_read(stream_id, reason)
    end
  end

  @doc """
  Read the events matching a compiled query, with the `$all` head of the same snapshot, and
  the tags of the matches any of `tag_items` matched. A match only `opts`' `position_items`
  matched comes with no event.
  """
  def read_query(conn, query, direction, bound, count, command_tag, tag_items, opts) do
    {correlation_id_type, opts} = Keyword.pop(opts, :correlation_id_type, "uuid")
    {causation_id_type, opts} = Keyword.pop(opts, :causation_id_type, "uuid")

    case Reader.Query.read_query(
           conn,
           query,
           direction,
           bound,
           count,
           command_tag,
           tag_items,
           opts
         ) do
      # The head comes from `$all`'s row, which every store has, so a read answers a row.
      {:ok, []} -> failed_to_read(@all_stream_id, :stream_not_found)
      {:ok, rows} -> {:ok, map_rows_to_read_result(rows, correlation_id_type, causation_id_type)}
      {:error, reason} -> failed_to_read(@all_stream_id, reason)
    end
  end

  @doc """
  Read the tags at the given positions, leaving out the given kinds and every tag of an event
  of the given types, as `%{position => [tag]}`. A position with no such row is absent.
  """
  def read_tags(conn, positions, tag_kinds, event_types, opts) do
    case Reader.Query.read_tags(conn, positions, tag_kinds, event_types, opts) do
      {:ok, rows} -> {:ok, Map.new(rows, fn [position, tags] -> {position, tags} end)}
      {:error, reason} -> failed_to_read(@all_stream_id, reason)
    end
  end

  defp map_rows_to_event_data(rows, correlation_id_type, causation_id_type) do
    {:ok, Reader.EventAdapter.to_event_data(rows, correlation_id_type, causation_id_type)}
  end

  # Rows are `[head, read_at, items, command_row_found, stamp, tags | event columns]`. An
  # empty match is one row with `nil` in every column after `read_at`, and `tags` is `nil` for
  # a match with no tag to answer. A bare match, one only `position_items` matched, has its
  # position and `nil` in every other event column, so it is a match with no event.
  defp map_rows_to_read_result([[head, read_at | _row] | _rows] = rows, correlation, causation) do
    matches = Enum.filter(rows, fn [_head, _read_at, items | _row] -> items end)

    event_rows =
      for [_head, _read_at, _items, _found, _stamp, _tags | [_position, event_id | _] = event] <-
            matches,
          event_id,
          do: event

    %ReadResult{
      events: Reader.EventAdapter.to_event_data(event_rows, correlation, causation),
      after: head,
      read_at: read_at,
      command_stamps:
        for [_head, _read_at, _items, true, stamp, _tags, position | _row] <- matches,
            into: %{} do
          {position, stamp}
        end,
      matched_items:
        for [_head, _read_at, items, _found, _stamp, _tags, position | _row] <- matches,
            into: %{} do
          {position, Enum.sort(items)}
        end,
      tags:
        for [_head, _read_at, _items, _found, _stamp, tags, position | _row] <- matches,
            is_list(tags),
            into: %{} do
          {position, tags}
        end
    }
  end

  defp failed_to_read(stream_id, reason) do
    Logger.warning("Failed to read events from stream id #{stream_id} due to: #{inspect(reason)}")

    {:error, reason}
  end

  defmodule EventAdapter do
    @moduledoc false

    @doc """
    Map event data from the database to `RecordedEvent` struct
    """
    def to_event_data(rows, correlation_id_type, causation_id_type),
      do: Enum.map(rows, &to_event_data_from_row(&1, correlation_id_type, causation_id_type))

    def to_event_data_from_row(
          [
            event_number,
            event_id,
            stream_uuid,
            stream_version,
            event_type,
            correlation_id,
            causation_id,
            data,
            metadata,
            created_at
          ],
          correlation_id_type,
          causation_id_type
        ) do
      %RecordedEvent{
        event_number: event_number,
        event_id: from_uuid(event_id),
        stream_uuid: stream_uuid,
        stream_version: stream_version,
        event_type: event_type,
        correlation_id: decode_id(correlation_id, correlation_id_type),
        causation_id: decode_id(causation_id, causation_id_type),
        data: data,
        metadata: metadata,
        created_at: from_timestamp(created_at)
      }
    end

    defp from_uuid(nil), do: nil
    defp from_uuid(uuid), do: UUID.binary_to_string!(uuid)

    defp decode_id(nil, _type), do: nil
    defp decode_id(value, "uuid"), do: UUID.binary_to_string!(value)
    defp decode_id(value, "text"), do: value

    defp from_timestamp(%DateTime{} = timestamp), do: timestamp

    defp from_timestamp(%NaiveDateTime{} = timestamp) do
      DateTime.from_naive(timestamp, "Etc/UTC")
    end

    if Code.ensure_loaded?(Postgrex.Timestamp) do
      defp from_timestamp(%Postgrex.Timestamp{} = timestamp) do
        %Postgrex.Timestamp{
          year: year,
          month: month,
          day: day,
          hour: hour,
          min: minute,
          sec: second,
          usec: microsecond
        } = timestamp

        with {:ok, naive} <-
               NaiveDateTime.new(year, month, day, hour, minute, second, {microsecond, 6}),
             {:ok, datetime} <- DateTime.from_naive(naive, "Etc/UTC") do
          datetime
        end
      end
    end
  end

  defmodule Query do
    @moduledoc false

    def read_events_forward(conn, stream_id, start_version, count, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.query_stream_events_forward(schema)

      do_query(conn, query, [stream_id, start_version, count], opts)
    end

    def read_events_backward(conn, stream_id, start_version, count, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)

      query = Statements.query_stream_events_backward(schema)

      do_query(conn, query, [stream_id, start_version, count], opts)
    end

    def read_query(conn, query, direction, bound, count, command_tag, tag_items, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)
      {position_items, opts} = Keyword.pop(opts, :position_items, [])

      deletable = Keyword.get(opts, :deletable, [])
      tag_kinds = Keyword.get(deletable, :tag_kinds, [])
      event_types = Keyword.get(deletable, :event_types, [])

      %Decree.Store.Query{
        probe_items: probe_items,
        probe_tags: probe_tags,
        probe_types: probe_types,
        rest_items: rest_items,
        rest_tags: rest_tags
      } = query

      params = [
        probe_items,
        probe_tags,
        probe_types,
        rest_items,
        rest_tags,
        bound,
        count,
        command_tag,
        tag_items,
        tag_kinds,
        event_types
      ]

      case position_items do
        [] ->
          do_query(conn, read_query_statement(schema, direction), params, opts)

        _items ->
          statement = read_query_statement(schema, direction, true)

          do_query(conn, statement, params ++ [position_items], opts)
      end
    end

    # `positioned` renders the `position_items` form, with them as `$12`. With none the text is
    # the plain read's, so a read naming none plans as it always did.
    def read_query_statement(schema, direction, positioned \\ false) do
      {op, order} = bound_and_order(direction)

      positions =
        Statements.query_positions(
          ~s("#{schema}".),
          "$1::bigint[]",
          "$2::text[]",
          "$3::text[]",
          "$4::bigint[]",
          "$5::text[]",
          "$6::bigint",
          probe_limit(positioned),
          op,
          order
        )

      Statements.read_query(schema, positions, order, positioned)
    end

    # `LIMIT NULL` is `LIMIT ALL`: a position item's probe is never cut.
    defp probe_limit(false), do: "$7::bigint"

    defp probe_limit(true),
      do: "CASE WHEN probe.item = ANY($12::bigint[]) THEN NULL ELSE $7::bigint END"

    def read_tags(conn, positions, tag_kinds, event_types, opts) do
      {schema, opts} = Keyword.pop(opts, :schema)

      do_query(conn, Statements.read_tags(schema), [positions, tag_kinds, event_types], opts)
    end

    defp bound_and_order(:forward), do: {">=", "ASC"}
    defp bound_and_order(:backward), do: {"<=", "DESC"}

    defp do_query(conn, query, params, opts) do
      case Postgrex.query(conn, query, params, opts) do
        {:ok, %Postgrex.Result{num_rows: 0}} ->
          {:ok, []}

        {:ok, %Postgrex.Result{rows: rows}} ->
          {:ok, rows}

        {:error, %Postgrex.Error{postgres: %{message: message}}} ->
          Logger.warning("Failed to read events from stream due to: " <> inspect(message))

          {:error, message}

        {:error, %DBConnection.ConnectionError{message: message}} ->
          Logger.warning("Failed to read events from stream due to: " <> inspect(message))

          {:error, message}

        {:error, error} = reply ->
          Logger.warning("Failed to read events from stream due to: " <> inspect(error))

          reply
      end
    end
  end
end
