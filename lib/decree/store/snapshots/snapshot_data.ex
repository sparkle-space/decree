defmodule Decree.Store.Snapshots.SnapshotData do
  @moduledoc """
  Snapshot data.

  `snapshot_version` names the row: an integer is the version of the snapshot's shape, one
  row per source and version in `versioned_snapshots`; nil is the one-row `snapshots` table
  that a pod from before store migration 1.10.0 reads and writes, kept until a later
  release.
  """

  alias Decree.Store.Snapshots.SnapshotData

  defstruct [
    :source_uuid,
    :source_version,
    :source_type,
    :data,
    :metadata,
    :created_at,
    snapshot_version: nil
  ]

  @type t :: %SnapshotData{
          source_uuid: String.t(),
          source_version: non_neg_integer,
          source_type: String.t(),
          data: binary,
          metadata: binary,
          created_at: DateTime.t(),
          snapshot_version: non_neg_integer | nil
        }

  def serialize(%SnapshotData{} = snapshot, serializer) do
    %SnapshotData{data: data, metadata: metadata} = snapshot

    %SnapshotData{
      snapshot
      | data: serializer.serialize(data),
        metadata: serializer.serialize(metadata)
    }
  end

  def deserialize(%SnapshotData{} = snapshot, serializer) do
    %SnapshotData{source_type: source_type, data: data, metadata: metadata} = snapshot

    %SnapshotData{
      snapshot
      | data: serializer.deserialize(data, type: source_type),
        metadata: serializer.deserialize(metadata, [])
    }
  end
end
