defmodule Decree.Store.Storage.Appender do
  @moduledoc false
  alias Decree.Store.{AppendCondition, AppendError, Query, RecordedEvent, UUID}
  alias Decree.Store.Sql.Statements

  require Logger

  # The SQLSTATE `append_events_v3` raises for a failed condition: the `ZD` class is no
  # Postgres one, so nothing else raises it, and Postgrex has no name for it.
  @append_condition_failed "ZD001"

  @doc """
  Append the given list of events to storage.

  Events are inserted in batches of 1,000 within a single transaction, one call
  of the `append_events_v3` function per batch. The batch keeps one call's arrays
  and its hold of the `$all` row bounded.

  The `:tags` option is one list of tags per event, in event order; an event past
  its end has none. `Decree.Store.Streams.Stream` refuses a tag over 512 bytes and
  removes repeats, so a direct caller is answered by the table: an over-long tag is
  a `:rejected` `Decree.Store.AppendError`, and a tag repeated on one event hits
  `event_tags_pkey`, which reads as `:wrong_expected_version`.

  The `:condition` option is a compiled `Decree.Store.AppendCondition`.

  Returns `:ok` on success, or one of `{:error, :not_found}`,
  `{:error, :duplicate_event}`, `{:error, :duplicate_stream_uuid}`,
  `{:error, :wrong_expected_version}`, `{:error, :append_condition_failed}` and
  `{:error, %Decree.Store.AppendError{}}`.
  """
  def append(conn, stream_id, events, opts) do
    [%RecordedEvent{stream_uuid: stream_uuid} | _] = events
    {correlation_id_type, opts} = Keyword.pop(opts, :correlation_id_type, "uuid")
    {causation_id_type, opts} = Keyword.pop(opts, :causation_id_type, "uuid")
    {tags, opts} = Keyword.pop(opts, :tags, [])
    {condition, opts} = Keyword.pop(opts, :condition)

    try do
      events
      |> Stream.map(&encode_uuids(&1, correlation_id_type, causation_id_type))
      |> Stream.zip(Stream.concat(tags, Stream.cycle([[]])))
      |> Stream.chunk_every(1_000)
      |> Enum.reduce({stream_id, condition}, fn batch, {stream_id, condition} ->
        {events, tags} = Enum.unzip(batch)
        event_count = length(events)

        with {:ok, new_stream_id} <-
               insert_event_batch(conn, stream_id, stream_uuid, events, tags, condition, opts) do
          Logger.debug("Appended #{event_count} event(s) to stream #{inspect(stream_uuid)}")

          # Only the first batch carries the condition. A later batch's would see the
          # earlier batches' own rows and fail on them, and `$all` is held from the first
          # batch to the commit, so nothing can interleave.
          {new_stream_id, nil}
        else
          {:error, error} -> throw({:error, error})
        end
      end)

      :ok
    catch
      {:error, error} = reply ->
        Logger.warning(
          "Failed to append events to stream #{inspect(stream_uuid)} due to: " <> inspect(error)
        )

        reply
    end
  end

  @doc """
  Link the given list of existing event ids to another stream in storage.

  Returns `:ok` on success, or one of `{:error, :not_found}`,
  `{:error, :duplicate_event}`, `{:error, :wrong_expected_version}` and
  `{:error, %Decree.Store.AppendError{}}`.
  """
  def link(conn, stream_id, event_ids, opts \\ [])

  def link(conn, stream_id, event_ids, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    try do
      event_ids
      |> Stream.map(&encode_uuid/1)
      |> Stream.chunk_every(1_000)
      |> Enum.each(fn batch ->
        event_count = length(batch)

        parameters =
          batch
          |> Stream.with_index(1)
          |> Enum.flat_map(fn {event_id, index} -> [index, event_id] end)

        params = [stream_id, event_count] ++ parameters

        with :ok <- insert_link_events(conn, params, event_count, schema, opts) do
          Logger.debug("Linked #{event_count} event(s) to stream")

          :ok
        else
          {:error, error} -> throw({:error, error})
        end
      end)
    catch
      {:error, error} = reply ->
        Logger.warning("Failed to link events to stream due to: #{inspect(error)}")

        reply
    end
  end

  defp encode_uuids(%RecordedEvent{} = event, correlation_id_type, causation_id_type) do
    %RecordedEvent{event_id: event_id, causation_id: causation_id, correlation_id: correlation_id} =
      event

    %RecordedEvent{
      event
      | event_id: encode_uuid(event_id),
        causation_id: encode_id(causation_id, causation_id_type),
        correlation_id: encode_id(correlation_id, correlation_id_type)
    }
  end

  defp encode_id(nil, _type), do: nil
  defp encode_id(value, "uuid"), do: UUID.string_to_binary!(value)
  defp encode_id(value, "text"), do: value

  defp encode_uuid(nil), do: nil
  defp encode_uuid(value), do: UUID.string_to_binary!(value)

  defp insert_event_batch(conn, stream_id, stream_uuid, events, tags, condition, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)
    {expected_version, opts} = Keyword.pop(opts, :expected_version)
    {created_at, opts} = Keyword.pop(opts, :created_at_override)

    params =
      [
        schema,
        stream_id,
        stream_uuid,
        batch_expected_version(expected_version, events),
        created_at
      ] ++
        build_insert_parameters(events) ++
        build_tag_parameters(tags) ++ build_condition_parameters(condition)

    case Postgrex.query(conn, Statements.append_events(schema), params, opts) do
      {:ok, %Postgrex.Result{rows: [[nil]]}} ->
        {:error, :not_found}

      {:ok, %Postgrex.Result{rows: [[stream_id]]}} ->
        {:ok, stream_id}

      {:error, error} ->
        handle_error(error)
    end
  end

  # Each batch checks its own base, so the second batch of a new stream finds the
  # version the first one wrote.
  defp batch_expected_version(:any_version, _events), do: nil

  defp batch_expected_version(_expected_version, [%RecordedEvent{} = event | _events]) do
    %RecordedEvent{stream_version: stream_version} = event

    stream_version - 1
  end

  # One array per column of `append_events_v3`, in the function's order.
  defp build_insert_parameters(events) do
    [
      Enum.map(events, & &1.event_id),
      Enum.map(events, & &1.event_type),
      Enum.map(events, & &1.causation_id),
      Enum.map(events, & &1.correlation_id),
      Enum.map(events, &array_element(&1.data)),
      Enum.map(events, &array_element(&1.metadata)),
      Enum.map(events, & &1.created_at)
    ]
  end

  # Two parallel flat arrays: each tag, and the 1-based ordinal in this batch of its
  # event. An event with no tags contributes nothing, so no tags are two empty arrays.
  defp build_tag_parameters(tags) do
    {indexes, tags} =
      tags
      |> Enum.with_index(1)
      |> Enum.flat_map(fn {event_tags, index} -> Enum.map(event_tags, &{index, &1}) end)
      |> Enum.unzip()

    [indexes, tags]
  end

  # A NULL `after` is how the function means a plain append.
  defp build_condition_parameters(nil), do: [nil, nil, nil, nil, nil, nil]

  defp build_condition_parameters(%AppendCondition{} = condition) do
    %AppendCondition{query: %Query{} = query, after: position} = condition

    [
      query.probe_items,
      query.probe_tags,
      query.probe_types,
      query.rest_items,
      query.rest_tags,
      position
    ]
  end

  # Postgrex reads a list that heads an array parameter as another dimension of the
  # array, so a `jsonb` store's serialized value that is a top-level JSON list would
  # lose its brackets, or shift the batch's rows. As a fragment it is one element.
  defp array_element(value) when is_list(value) do
    value |> Jason.encode_to_iodata!() |> Jason.Fragment.new()
  end

  defp array_element(value), do: value

  defp insert_link_events(conn, params, event_count, schema, opts) do
    statement = Statements.insert_link_events(schema, event_count)

    case Postgrex.query(conn, statement, params, opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} -> {:error, :not_found}
      {:ok, %Postgrex.Result{}} -> :ok
      {:error, error} -> handle_error(error)
    end
  end

  # The five domain answers stay plain atoms; every other failure is classified
  # as a `Decree.Store.AppendError` (see its moduledoc for the classes).
  defp handle_error(%Postgrex.Error{postgres: %{code: :foreign_key_violation}}),
    do: {:error, :not_found}

  defp handle_error(%Postgrex.Error{postgres: %{code: :unique_violation} = postgres}) do
    case postgres do
      %{constraint: "events_pkey"} ->
        {:error, :duplicate_event}

      %{constraint: "stream_events_pkey"} ->
        {:error, :duplicate_event}

      %{constraint: "ix_streams_stream_uuid"} ->
        # Decree.Store.Streams.Stream will retry when it gets this error code. That will always work
        # because on the second time around, the stream will have been made, so the race to create
        # the stream will have been resolved.
        {:error, :duplicate_stream_uuid}

      _postgres ->
        {:error, :wrong_expected_version}
    end
  end

  defp handle_error(%Postgrex.Error{postgres: %{pg_code: @append_condition_failed}}),
    do: {:error, :append_condition_failed}

  defp handle_error(error), do: {:error, AppendError.from_error(error)}
end
