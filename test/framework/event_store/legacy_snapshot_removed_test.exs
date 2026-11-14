defmodule Decree.EventStore.LegacySnapshotRemovedTest do
  use Decree.PostgresCase

  alias Decree.DefaultApp
  alias Decree.EventStore
  alias Decree.FrameworkStorage
  alias Decree.Serialization.JsonSerializer
  alias Decree.Store.Config
  alias Decree.UUID

  # The facade reads a snapshot at a version only: `read_snapshot/2`, the read of the
  # one-row `snapshots` table from before store migration 1.10.0, is gone, and a nil version
  # is refused rather than read from that table.

  defmodule Snap do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:at]
  end

  setup do
    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})
    start_supervised!(DefaultApp)

    [conn: conn]
  end

  test "a read with a nil version is refused, even with a legacy row for the source",
       %{conn: conn} do
    source_uuid = UUID.uuid4()
    insert_legacy_row(conn, source_uuid)

    assert EventStore.read_snapshot(DefaultApp, source_uuid, nil) ==
             {:error, :invalid_snapshot_version}
  end

  test "`read_snapshot/2` is not exported" do
    assert Code.ensure_loaded?(EventStore)

    refute function_exported?(EventStore, :read_snapshot, 2)
    assert function_exported?(EventStore, :read_snapshot, 3)
  end

  # The row a pod from before store migration 1.10.0 writes.
  defp insert_legacy_row(conn, source_uuid) do
    Postgrex.query!(
      conn,
      ~s|INSERT INTO "public".snapshots | <>
        "(source_uuid, source_version, source_type, data, metadata) " <>
        "VALUES ($1, $2, $3, $4, $5)",
      [
        source_uuid,
        1,
        Atom.to_string(Snap),
        JsonSerializer.serialize(%Snap{at: "legacy"}),
        JsonSerializer.serialize(%{})
      ]
    )

    :ok
  end
end
