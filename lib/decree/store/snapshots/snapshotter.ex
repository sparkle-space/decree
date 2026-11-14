defmodule Decree.Store.Snapshots.Snapshotter do
  @moduledoc false

  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Storage.Snapshot

  @doc """
  Read a snapshot, if available, for a given source.
  """
  def read_snapshot(conn, source_uuid, opts) do
    {serializer, opts} = Keyword.pop(opts, :serializer)

    with {:ok, snapshot} <- Snapshot.read_snapshot(conn, source_uuid, opts) do
      deserialized = SnapshotData.deserialize(snapshot, serializer)

      {:ok, deserialized}
    end
  end

  @doc """
  Record a snapshot containing data and metadata for a given source.

  Returns `:ok` on success.
  """
  def record_snapshot(conn, %SnapshotData{} = snapshot, opts) do
    {serializer, opts} = Keyword.pop(opts, :serializer)

    serialized = SnapshotData.serialize(snapshot, serializer)

    Snapshot.record_snapshot(conn, serialized, opts)
  end

  @doc """
  Delete a previously recorded snapshot for a given source: every version's row.

  Returns `:ok` on success.
  """
  def delete_snapshot(conn, source_uuid, opts \\ []) do
    Snapshot.delete_snapshot(conn, source_uuid, opts)
  end

  @doc """
  Delete the versioned snapshots of a source type whose version is below `snapshot_version`.

  Returns `{:ok, count}` on success.
  """
  def prune_snapshots(conn, source_type, snapshot_version, opts \\ []) do
    Snapshot.prune_snapshots(conn, source_type, snapshot_version, opts)
  end
end
