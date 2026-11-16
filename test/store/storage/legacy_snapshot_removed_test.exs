defmodule Decree.Store.Storage.LegacySnapshotRemovedTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{Config, EventFactory, UUID}
  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Storage.Initializer

  # A snapshot without a `snapshot_version` is refused: no code path reads or writes the
  # one-row `snapshots` table from before store migration 1.10.0, which Init no longer creates
  # and store migration 1.11.0 drops (sparkle-space/sparklespace#8200). Every snapshot path
  # runs on a store without it.

  defmodule Snap do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:at]
  end

  setup_all do
    config = SchemaEventStore.config()
    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(config)}, id: :schema_conn)

    [schema_conn: conn, schema_config: config]
  end

  setup %{schema_conn: conn, schema_config: config} do
    Initializer.reset!(conn, config)
    start_supervised!(SchemaEventStore)

    :ok
  end

  for {name, store, schema} <- [
        {"public", TestEventStore, "public"},
        {"a named schema", SchemaEventStore, "example"}
      ] do
    describe "a snapshot with no version under #{name}" do
      setup do
        [store: unquote(store), schema: unquote(schema)]
      end

      test "the store has no legacy table", context do
        assert legacy_table(context) == nil
      end

      test "a read is refused", context do
        %{store: store} = context
        source_uuid = stream(store)
        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))

        assert store.read_snapshot(source_uuid) == {:error, :invalid_snapshot_version}

        assert store.read_snapshot(source_uuid, snapshot_version: nil) ==
                 {:error, :invalid_snapshot_version}
      end

      test "a record is refused, and writes no row", context do
        %{store: store} = context
        source_uuid = stream(store)

        assert store.record_snapshot(snapshot(source_uuid, nil, %Snap{at: "legacy"})) ==
                 {:error, :invalid_snapshot_version}

        assert count(context, "versioned_snapshots", source_uuid) == 0
      end

      test "delete_snapshot removes every version's row, with no legacy table", context do
        %{store: store} = context
        source_uuid = stream(store)

        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
        :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))

        assert :ok = store.delete_snapshot(source_uuid)

        assert count(context, "versioned_snapshots", source_uuid) == 0
        assert legacy_table(context) == nil
      end
    end

    describe "a hard delete under #{name}" do
      setup do
        store = unquote(store)

        stop_supervised!(store)
        start_supervised!({store, enable_hard_deletes: true})

        [store: store, schema: unquote(schema)]
      end

      test "removes its stream's versioned rows, with no legacy table", context do
        %{store: store} = context
        source_uuid = stream(store)

        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
        :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))

        :ok = store.delete_stream(source_uuid, :any_version, :hard)

        assert count(context, "versioned_snapshots", source_uuid) == 0
        assert legacy_table(context) == nil
      end
    end
  end

  # A stream with one event: a versioned write needs its row.
  defp stream(store) do
    stream_uuid = UUID.uuid4()

    :ok = store.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

    stream_uuid
  end

  # The one-row `snapshots` table in the store's schema, or nil.
  defp legacy_table(context) do
    %{conn: conn, schema: schema} = context

    %Postgrex.Result{rows: [[table]]} =
      Postgrex.query!(conn, "SELECT to_regclass(format('%I.snapshots', $1::text))::text", [
        schema
      ])

    table
  end

  defp snapshot(source_uuid, snapshot_version, data) do
    %SnapshotData{
      source_uuid: source_uuid,
      source_version: 1,
      source_type: Atom.to_string(Snap),
      snapshot_version: snapshot_version,
      data: data,
      metadata: %{}
    }
  end

  defp count(context, table, source_uuid) do
    %{conn: conn, schema: schema} = context

    %Postgrex.Result{rows: [[count]]} =
      Postgrex.query!(
        conn,
        ~s|SELECT count(*) FROM "#{schema}".#{table} WHERE source_uuid = $1|,
        [source_uuid]
      )

    count
  end
end
