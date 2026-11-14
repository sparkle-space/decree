defmodule Decree.Store.Storage.LegacySnapshotRemovedTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{Config, EventFactory, UUID}
  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Storage.Initializer

  # No code path reads or writes the one-row `snapshots` table from before store migration
  # 1.10.0 any longer: a snapshot without a `snapshot_version` is refused. The table stays for
  # a pod of the previous release, whose hard delete still deletes from it, so a row an old
  # pod wrote there outlives every delete this release makes. A later release drops the table.

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

      test "a read is refused, even with a legacy row for the source", context do
        %{store: store} = context
        source_uuid = stream(store)
        insert_legacy_row(context, source_uuid)

        assert store.read_snapshot(source_uuid) == {:error, :invalid_snapshot_version}

        assert store.read_snapshot(source_uuid, snapshot_version: nil) ==
                 {:error, :invalid_snapshot_version}
      end

      test "a record is refused, and writes no legacy row", context do
        %{store: store} = context
        source_uuid = stream(store)

        assert store.record_snapshot(snapshot(source_uuid, nil, %Snap{at: "legacy"})) ==
                 {:error, :invalid_snapshot_version}

        assert count(context, "snapshots", source_uuid) == 0
        assert count(context, "versioned_snapshots", source_uuid) == 0
      end

      test "delete_snapshot removes every version's row and leaves the legacy row", context do
        %{store: store} = context
        source_uuid = stream(store)
        insert_legacy_row(context, source_uuid)

        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
        :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))

        assert :ok = store.delete_snapshot(source_uuid)

        assert count(context, "versioned_snapshots", source_uuid) == 0
        assert count(context, "snapshots", source_uuid) == 1
      end
    end

    describe "a hard delete under #{name}" do
      setup do
        store = unquote(store)

        stop_supervised!(store)
        start_supervised!({store, enable_hard_deletes: true})

        [store: store, schema: unquote(schema)]
      end

      test "removes its stream's versioned rows and leaves the legacy row", context do
        %{store: store} = context
        source_uuid = stream(store)
        insert_legacy_row(context, source_uuid)

        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
        :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))

        :ok = store.delete_stream(source_uuid, :any_version, :hard)

        assert count(context, "versioned_snapshots", source_uuid) == 0
        assert count(context, "snapshots", source_uuid) == 1
      end
    end
  end

  # A stream with one event: a versioned write needs its row.
  defp stream(store) do
    stream_uuid = UUID.uuid4()

    :ok = store.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

    stream_uuid
  end

  # The row a pod from before store migration 1.10.0 writes: the statement its legacy
  # `record_snapshot` runs.
  defp insert_legacy_row(context, source_uuid) do
    %{conn: conn, schema: schema, serializer: serializer} = context

    Postgrex.query!(
      conn,
      ~s|INSERT INTO "#{schema}".snapshots | <>
        "(source_uuid, source_version, source_type, data, metadata) " <>
        "VALUES ($1, $2, $3, $4, $5)",
      [
        source_uuid,
        1,
        Atom.to_string(Snap),
        serializer.serialize(%Snap{at: "legacy"}),
        serializer.serialize(%{})
      ]
    )

    :ok
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
