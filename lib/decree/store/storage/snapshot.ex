defmodule Decree.Store.Storage.Snapshot do
  @moduledoc false

  require Logger

  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Sql.Statements

  # A snapshot with a `snapshot_version` is a row of `versioned_snapshots`, one per source and
  # version. One without is the one-row `snapshots` table that a pod from before store
  # migration 1.10.0 still reads and writes: the legacy path, kept until a later release.

  def read_snapshot(conn, source_uuid, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)
    {snapshot_version, opts} = Keyword.pop(opts, :snapshot_version)

    with {:ok, query, params} <- read_query(schema, source_uuid, snapshot_version) do
      case Postgrex.query(conn, query, params, opts) do
        {:ok, %Postgrex.Result{num_rows: 0}} ->
          {:error, :snapshot_not_found}

        {:ok, %Postgrex.Result{rows: [row]}} ->
          {:ok, to_snapshot_from_row(row)}

        {:error, error} = reply ->
          Logger.warning(
            "Failed to read snapshot for source \"#{source_uuid}\" due to: #{inspect(error)}"
          )

          reply
      end
    end
  end

  defp read_query(schema, source_uuid, nil),
    do: {:ok, Statements.query_snapshot(schema), [source_uuid]}

  defp read_query(schema, source_uuid, snapshot_version)
       when is_integer(snapshot_version) and snapshot_version >= 0,
       do: {:ok, Statements.query_versioned_snapshot(schema), [source_uuid, snapshot_version]}

  defp read_query(_schema, _source_uuid, _snapshot_version),
    do: {:error, :invalid_snapshot_version}

  def record_snapshot(conn, %SnapshotData{snapshot_version: nil} = snapshot, opts) do
    %SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata
    } = snapshot

    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.insert_snapshot(schema)
    params = [source_uuid, source_version, source_type, data, metadata]

    case Postgrex.query(conn, query, params, opts) do
      {:ok, _result} ->
        :ok

      {:error, error} = reply ->
        log_failed_record(snapshot, error)

        reply
    end
  end

  # One transaction: the stream's row FOR KEY SHARE, then the upsert. No row answers
  # `:stream_not_found` and writes nothing.
  #
  # A hard delete deletes the stream's row before it deletes the stream's snapshot rows
  # (`DeleteStream.hard_delete/3`), and that DELETE conflicts with KEY SHARE. So a snapshot
  # that took the row first commits before the DELETE goes on, and the hard delete's later
  # statement, with a snapshot of its own, sees its row and deletes it; one that comes to the
  # row after the DELETE waits for the hard delete to commit, and then finds no row. Either
  # way no snapshot outlives the stream it was taken of. KEY SHARE does not conflict with the
  # NO KEY UPDATE an append takes, so an append never waits on a snapshot write.
  def record_snapshot(conn, %SnapshotData{snapshot_version: snapshot_version} = snapshot, opts)
      when is_integer(snapshot_version) and snapshot_version >= 0 do
    %SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata
    } = snapshot

    {schema, opts} = Keyword.pop(opts, :schema)

    lock = Statements.lock_snapshot_stream(schema)
    insert = Statements.insert_versioned_snapshot(schema)
    params = [source_uuid, snapshot_version, source_version, source_type, data, metadata]

    transaction =
      Postgrex.transaction(
        conn,
        fn transaction ->
          with {:ok, %Postgrex.Result{num_rows: 1}} <-
                 Postgrex.query(transaction, lock, [source_uuid], opts),
               {:ok, %Postgrex.Result{}} <- Postgrex.query(transaction, insert, params, opts) do
            :ok
          else
            # Nothing written, and no rollback: inside a caller's transaction a rollback here
            # would abort the caller's, for an answer that is not an error of the database.
            {:ok, %Postgrex.Result{num_rows: 0}} -> {:error, :stream_not_found}
            {:error, error} -> Postgrex.rollback(transaction, error)
          end
        end,
        Keyword.take(opts, [:timeout])
      )

    case transaction do
      {:ok, :ok} ->
        :ok

      {:ok, {:error, :stream_not_found} = reply} ->
        reply

      {:error, error} = reply ->
        log_failed_record(snapshot, error)

        reply
    end
  end

  def record_snapshot(_conn, %SnapshotData{}, _opts), do: {:error, :invalid_snapshot_version}

  # Every row of the source: the legacy row and every version's.
  def delete_snapshot(conn, source_uuid, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.delete_snapshot(schema)

    case Postgrex.query(conn, query, [source_uuid], opts) do
      {:ok, _result} ->
        :ok

      {:error, error} = reply ->
        Logger.warning(
          "Failed to delete snapshot for source \"#{source_uuid}\" due to: " <> inspect(error)
        )

        reply
    end
  end

  def prune_snapshots(conn, source_type, snapshot_version, opts)
      when is_binary(source_type) and is_integer(snapshot_version) and snapshot_version >= 0 do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.prune_snapshots(schema)

    case Postgrex.query(conn, query, [source_type, snapshot_version], opts) do
      {:ok, %Postgrex.Result{num_rows: count}} ->
        {:ok, count}

      {:error, error} = reply ->
        Logger.warning(
          "Failed to prune snapshots of type \"#{source_type}\" below version " <>
            "#{snapshot_version} due to: " <> inspect(error)
        )

        reply
    end
  end

  def prune_snapshots(_conn, _source_type, _snapshot_version, _opts),
    do: {:error, :invalid_snapshot_version}

  defp log_failed_record(%SnapshotData{} = snapshot, error) do
    %SnapshotData{source_uuid: source_uuid, source_version: source_version} = snapshot

    Logger.warning(
      "Failed to record snapshot for source \"#{source_uuid}\" at version \"#{source_version}\" due to: " <>
        inspect(error)
    )
  end

  defp to_snapshot_from_row([
         source_uuid,
         source_version,
         source_type,
         data,
         metadata,
         created_at
       ]) do
    %SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at
    }
  end

  defp to_snapshot_from_row([
         source_uuid,
         source_version,
         source_type,
         data,
         metadata,
         created_at,
         snapshot_version
       ]) do
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
end
