defmodule Decree.EventStore.SnapshotData do
  @moduledoc """
  Snapshot data

  `snapshot_version` is the version of the snapshot's shape, and with the source names its
  row: code at two versions keeps two rows. A nil `snapshot_version` is refused with
  `{:error, :invalid_snapshot_version}`.
  """

  @type t :: %Decree.EventStore.SnapshotData{
          source_uuid: String.t(),
          source_version: non_neg_integer,
          source_type: String.t(),
          data: binary,
          metadata: binary,
          created_at: DateTime.t(),
          snapshot_version: non_neg_integer | nil
        }

  defstruct [
    :source_uuid,
    :source_version,
    :source_type,
    :data,
    :metadata,
    :created_at,
    snapshot_version: nil
  ]
end
