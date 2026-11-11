defmodule Decree.Store.StoreRows do
  @moduledoc false

  # What a store holds, read from Postgres through `observer`, a connection of the test's
  # own. Each function takes a context with `:observer` and `:schema`.

  import ExUnit.Assertions

  def stream_id(context, stream_uuid) do
    context
    |> rows(~s(SELECT stream_id FROM "#{context.schema}".streams WHERE stream_uuid = $1), [
      stream_uuid
    ])
    |> List.first([nil])
    |> hd()
  end

  def stream_version(context, stream_uuid) do
    [[version]] =
      rows(
        context,
        ~s(SELECT stream_version FROM "#{context.schema}".streams WHERE stream_uuid = $1),
        [stream_uuid]
      )

    version
  end

  def stream_versions(context, stream_uuid) do
    context
    |> rows(
      ~s(SELECT se.stream_version FROM "#{context.schema}".stream_events se) <>
        ~s[ JOIN "#{context.schema}".streams s USING (stream_id)] <>
        " WHERE s.stream_uuid = $1 ORDER BY se.stream_version",
      [stream_uuid]
    )
    |> Enum.map(&hd/1)
  end

  def all_version(context) do
    [[version]] =
      rows(
        context,
        ~s(SELECT stream_version FROM "#{context.schema}".streams WHERE stream_id = 0),
        []
      )

    version
  end

  def count(context, table) do
    [[count]] = rows(context, ~s[SELECT count(*) FROM "#{context.schema}".#{table}], [])

    count
  end

  # The stream each `$all` position came from, and the event's version there, in position
  # order. Every position is a row, so a gap shows in `all_positions/1`.
  def all_events(context) do
    rows(
      context,
      ~s(SELECT s.stream_uuid, se.original_stream_version) <>
        ~s( FROM "#{context.schema}".stream_events se) <>
        ~s[ JOIN "#{context.schema}".streams s ON s.stream_id = se.original_stream_id] <>
        " WHERE se.stream_id = 0 ORDER BY se.stream_version",
      []
    )
    |> Enum.map(&List.to_tuple/1)
  end

  def all_positions(context) do
    context
    |> rows(
      ~s(SELECT stream_version FROM "#{context.schema}".stream_events) <>
        " WHERE stream_id = 0 ORDER BY stream_version",
      []
    )
    |> Enum.map(&hd/1)
  end

  # A bare `streams` row for `stream_uuid`, with no event, inserted as the store's own
  # create-stream statement does. Answers `{:ok, stream_id}`. `conn` can be a transaction.
  def insert_stream(conn, schema, stream_uuid) do
    %Postgrex.Result{rows: [[stream_id]]} =
      Postgrex.query!(
        conn,
        ~s[INSERT INTO "#{schema}".streams (stream_uuid) VALUES ($1) RETURNING stream_id],
        [stream_uuid]
      )

    {:ok, stream_id}
  end

  # Removes a stream and leaves its tag and type rows behind, with the statement a pod from
  # before store migration 1.8.0 still sends during a rollout. These rows have no `$all` row,
  # which is what the query template's live-event check is for.
  def orphan_stream(conn, schema, stream_uuid) do
    prefix = ~s("#{schema}".)

    {:ok, :ok} =
      Postgrex.transaction(conn, fn tx ->
        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

        Postgrex.query!(
          tx,
          """
          WITH stream AS (
            SELECT stream_id FROM #{prefix}streams WHERE stream_uuid = $1
          ),
          deleted_stream_events AS (
            DELETE FROM #{prefix}stream_events
            WHERE stream_id = (SELECT stream_id FROM stream)
            RETURNING event_id
          ),
          linked_events AS (
            DELETE FROM #{prefix}stream_events
            WHERE event_id IN (SELECT event_id FROM deleted_stream_events)
          ),
          events AS (
            DELETE FROM #{prefix}events
            WHERE event_id IN (SELECT event_id FROM deleted_stream_events)
          )
          DELETE FROM #{prefix}streams
          WHERE stream_id = (SELECT stream_id FROM stream)
          """,
          [stream_uuid]
        )

        :ok
      end)

    :ok
  end

  # Stands in for a store that holds link rows from before #8075 removed links: the API can no
  # longer write one, and the committed seed dumps hold some. The rows are what the deleted
  # `insert_link_events` statement wrote. `stream_uuid` is created if absent, and gets one
  # `stream_events` row per event at its next versions, carrying the event's own stream and
  # version as `original_stream_id` and `original_stream_version`. Its `streams.stream_version`
  # moves with them. `$all` is untouched: a link never took a position.
  def seed_legacy_link(context, stream_uuid, recorded_events) do
    prefix = ~s("#{context.schema}".)
    event_ids = Enum.map(recorded_events, &Decree.Store.UUID.string_to_binary!(&1.event_id))
    event_count = length(event_ids)
    existing_stream_id = stream_id(context, stream_uuid)

    {:ok, :ok} =
      Postgrex.transaction(context.observer, fn tx ->
        stream_id =
          case existing_stream_id do
            nil ->
              %Postgrex.Result{rows: [[stream_id]]} =
                Postgrex.query!(
                  tx,
                  "INSERT INTO #{prefix}streams (stream_uuid) VALUES ($1) RETURNING stream_id",
                  [stream_uuid]
                )

              stream_id

            stream_id ->
              stream_id
          end

        %Postgrex.Result{num_rows: ^event_count} =
          Postgrex.query!(
            tx,
            """
            WITH
              stream AS (
                UPDATE #{prefix}streams
                SET stream_version = stream_version + $2::bigint
                WHERE stream_id = $1::bigint
                RETURNING stream_version - $2 AS initial_stream_version
              ),
              linked_events (index, event_id) AS (
                SELECT ordinality, event_id
                FROM unnest($3::uuid[]) WITH ORDINALITY AS linked (event_id, ordinality)
              )
            INSERT INTO #{prefix}stream_events
              (stream_id, stream_version, event_id, original_stream_id, original_stream_version)
            SELECT
              $1,
              stream.initial_stream_version + linked_events.index,
              linked_events.event_id,
              original_stream_events.original_stream_id,
              original_stream_events.stream_version
            FROM linked_events
            CROSS JOIN stream
            INNER JOIN #{prefix}stream_events AS original_stream_events
              ON original_stream_events.event_id = linked_events.event_id
                AND original_stream_events.stream_id = original_stream_events.original_stream_id
            """,
            [stream_id, event_count, event_ids]
          )

        :ok
      end)

    :ok
  end

  def assert_row_locked(context, stream_id) do
    assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
             try_row_lock(context, stream_id)
  end

  def assert_row_free(context, stream_id) do
    assert {:ok, %Postgrex.Result{num_rows: 1}} = try_row_lock(context, stream_id)
  end

  defp try_row_lock(context, stream_id) do
    Postgrex.query(
      context.observer,
      ~s(SELECT 1 FROM "#{context.schema}".streams WHERE stream_id = $1) <>
        " FOR NO KEY UPDATE NOWAIT",
      [stream_id]
    )
  end

  def rows(context, sql, params) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(context.observer, sql, params)

    rows
  end
end
