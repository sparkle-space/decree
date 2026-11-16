defmodule Decree.EventStore.LegacySnapshotRemovedTest do
  use Decree.PostgresCase

  alias Decree.DefaultApp
  alias Decree.EventStore
  alias Decree.FrameworkStorage
  alias Decree.Store.Config
  alias Decree.UUID

  # The facade reads a snapshot at a version only: `read_snapshot/2`, the read of the
  # one-row `snapshots` table from before store migration 1.10.0, is gone, and a nil version
  # is refused. The table is gone too: Init no longer creates it, and store migration 1.11.0
  # drops it (sparkle-space/sparklespace#8200).

  setup do
    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})
    start_supervised!(DefaultApp)

    [conn: conn]
  end

  test "a read with a nil version is refused, and the store has no legacy table",
       %{conn: conn} do
    source_uuid = UUID.uuid4()

    assert EventStore.read_snapshot(DefaultApp, source_uuid, nil) ==
             {:error, :invalid_snapshot_version}

    assert %Postgrex.Result{rows: [[nil]]} =
             Postgrex.query!(conn, ~s|SELECT to_regclass('"public".snapshots')::text|, [])
  end

  test "`read_snapshot/2` is not exported" do
    assert Code.ensure_loaded?(EventStore)

    refute function_exported?(EventStore, :read_snapshot, 2)
    assert function_exported?(EventStore, :read_snapshot, 3)
  end
end
