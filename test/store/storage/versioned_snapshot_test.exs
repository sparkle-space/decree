defmodule Decree.Store.Storage.VersionedSnapshotTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{Config, EventFactory, LockSession, StoreRows, UUID}
  alias Decree.Store.Snapshots.{SnapshotData, Snapshotter}
  alias Decree.Store.Storage.{DeleteStream, Initializer}

  # One row per source and snapshot version in `versioned_snapshots`, beside the one-row
  # `snapshots` table that a pod from before store migration 1.10.0 still reads and writes.

  defmodule Snap do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:at]
  end

  defmodule OtherSnap do
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
    describe "per-version rows under #{name}" do
      setup do
        [store: unquote(store), schema: unquote(schema)]
      end

      test "versions of one source coexist, and each read answers its own", context do
        %{store: store} = context
        source_uuid = stream(store)

        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
        :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))

        assert {:ok, %SnapshotData{snapshot_version: 1, data: %Snap{at: 1}}} =
                 store.read_snapshot(source_uuid, snapshot_version: 1)

        assert {:ok, %SnapshotData{snapshot_version: 2, data: %Snap{at: 2}}} =
                 store.read_snapshot(source_uuid, snapshot_version: 2)

        assert {:error, :snapshot_not_found} =
                 store.read_snapshot(source_uuid, snapshot_version: 3)

        assert count(context, "versioned_snapshots", source_uuid) == 2
        assert count(context, "snapshots", source_uuid) == 0
      end

      test "a write of the same version replaces only that version's row", context do
        %{store: store} = context
        source_uuid = stream(store)

        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
        :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))
        :ok = store.record_snapshot(%{snapshot(source_uuid, 2, %Snap{at: 3}) | source_version: 3})

        assert {:ok, %SnapshotData{source_version: 3, data: %Snap{at: 3}}} =
                 store.read_snapshot(source_uuid, snapshot_version: 2)

        assert {:ok, %SnapshotData{source_version: 1, data: %Snap{at: 1}}} =
                 store.read_snapshot(source_uuid, snapshot_version: 1)
      end

      test "a legacy row is independent of every version", context do
        %{store: store} = context
        source_uuid = stream(store)

        :ok = store.record_snapshot(snapshot(source_uuid, nil, %Snap{at: "legacy"}))
        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
        :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))

        assert {:ok, %SnapshotData{snapshot_version: nil, data: %Snap{at: "legacy"}}} =
                 store.read_snapshot(source_uuid)

        assert {:ok, %SnapshotData{data: %Snap{at: 1}}} =
                 store.read_snapshot(source_uuid, snapshot_version: 1)

        assert count(context, "snapshots", source_uuid) == 1
        assert count(context, "versioned_snapshots", source_uuid) == 2
      end

      test "a legacy write needs no stream, as before", context do
        %{store: store} = context
        source_uuid = UUID.uuid4()

        :ok = store.record_snapshot(snapshot(source_uuid, nil, %Snap{at: "legacy"}))

        assert {:ok, %SnapshotData{data: %Snap{at: "legacy"}}} = store.read_snapshot(source_uuid)
      end

      test "delete_snapshot removes the legacy row and every version's", context do
        %{store: store} = context
        source_uuid = stream(store)
        other_uuid = stream(store)

        :ok = store.record_snapshot(snapshot(source_uuid, nil, %Snap{at: "legacy"}))
        :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
        :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))
        :ok = store.record_snapshot(snapshot(other_uuid, nil, %Snap{at: "legacy"}))
        :ok = store.record_snapshot(snapshot(other_uuid, 1, %Snap{at: 1}))

        assert :ok = store.delete_snapshot(source_uuid)

        assert {:error, :snapshot_not_found} = store.read_snapshot(source_uuid)

        for version <- [1, 2] do
          assert {:error, :snapshot_not_found} =
                   store.read_snapshot(source_uuid, snapshot_version: version)
        end

        assert count(context, "snapshots", source_uuid) == 0
        assert count(context, "versioned_snapshots", source_uuid) == 0
        assert count(context, "snapshots", other_uuid) == 1
        assert count(context, "versioned_snapshots", other_uuid) == 1
      end

      test "a versioned write for a stream with no row writes nothing", context do
        %{store: store} = context
        source_uuid = UUID.uuid4()

        assert store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1})) ==
                 {:error, :stream_not_found}

        assert count(context, "versioned_snapshots", source_uuid) == 0
        assert count(context, "snapshots", source_uuid) == 0
      end

      test "a versioned write waiting on an erase of its stream finds none, and writes nothing",
           context do
        %{store: store, schema: schema, conn: observer, postgrex_config: postgrex_config} =
          context

        # A bare stream row, with no events referring to it, so it can be deleted on its own,
        # as the hard delete's last delete does.
        source_uuid = UUID.uuid4()

        %Postgrex.Result{rows: [[stream_id]]} =
          Postgrex.query!(
            observer,
            ~s|INSERT INTO "#{schema}".streams (stream_uuid) VALUES ($1) RETURNING stream_id|,
            [source_uuid]
          )

        eraser = LockSession.start(postgrex_config)

        LockSession.run(eraser, fn tx ->
          Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

          Postgrex.query!(tx, ~s(DELETE FROM "#{schema}".streams WHERE stream_id = $1), [
            stream_id
          ])
        end)

        write =
          Task.async(fn -> store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1})) end)

        LockSession.wait_until_waiting(observer, eraser.backend, 1)
        {:ok, :ok} = LockSession.commit(eraser)

        assert Task.await(write) == {:error, :stream_not_found}
        assert count(context, "versioned_snapshots", source_uuid) == 0
      end

      test "a versioned write does not wait on an append to its stream", context do
        %{store: store, schema: schema, conn: observer, postgrex_config: postgrex_config} =
          context

        source_uuid = stream(store)
        stream_id = StoreRows.stream_id(%{observer: observer, schema: schema}, source_uuid)

        # The lock an append takes on its stream's row.
        appender = LockSession.start(postgrex_config)
        LockSession.lock_row(appender, schema, stream_id)

        write =
          Task.async(fn -> store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1})) end)

        assert Task.yield(write, 5_000) == {:ok, :ok}

        LockSession.rollback(appender)
      end

      test "prune deletes one type's rows below a version, and no legacy row", context do
        %{store: store} = context
        t = stream(store)
        u = stream(store)

        for version <- [1, 2, 3] do
          :ok = store.record_snapshot(snapshot(t, version, %Snap{}))
        end

        :ok = store.record_snapshot(other_type(snapshot(u, 1, %OtherSnap{})))
        :ok = store.record_snapshot(snapshot(t, nil, %Snap{}))

        assert store.prune_snapshots(Atom.to_string(Snap), 3) == {:ok, 2}

        assert {:ok, %SnapshotData{}} = store.read_snapshot(t, snapshot_version: 3)
        assert {:error, :snapshot_not_found} = store.read_snapshot(t, snapshot_version: 2)
        assert {:error, :snapshot_not_found} = store.read_snapshot(t, snapshot_version: 1)
        assert {:ok, %SnapshotData{}} = store.read_snapshot(u, snapshot_version: 1)
        assert {:ok, %SnapshotData{}} = store.read_snapshot(t)

        assert store.prune_snapshots(Atom.to_string(Snap), 3) == {:ok, 0}
      end
    end

    describe "a hard delete under #{name}" do
      setup do
        store = unquote(store)

        stop_supervised!(store)
        start_supervised!({store, enable_hard_deletes: true})

        [store: store, schema: unquote(schema)]
      end

      test "removes its stream's rows in both tables, at every version, and no other's",
           context do
        %{store: store} = context
        deleted = stream(store)
        kept = stream(store)

        for source_uuid <- [deleted, kept] do
          :ok = store.record_snapshot(snapshot(source_uuid, nil, %Snap{at: "legacy"}))
          :ok = store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1}))
          :ok = store.record_snapshot(snapshot(source_uuid, 2, %Snap{at: 2}))
        end

        :ok = store.delete_stream(deleted, :any_version, :hard)

        assert count(context, "snapshots", deleted) == 0
        assert count(context, "versioned_snapshots", deleted) == 0
        assert count(context, "snapshots", kept) == 1
        assert count(context, "versioned_snapshots", kept) == 2
      end

      test "waits on a versioned write that holds its stream, and then removes its row",
           context do
        %{store: store, schema: schema, conn: observer, postgrex_config: postgrex_config} =
          context

        source_uuid = stream(store)
        :ok = store.record_snapshot(snapshot(source_uuid, nil, %Snap{at: "legacy"}))

        # The store's own versioned write, its stream's row held FOR KEY SHARE and its row
        # upserted, held open before commit.
        writer = LockSession.start(postgrex_config)

        :ok =
          LockSession.run(writer, fn tx ->
            Snapshotter.record_snapshot(tx, snapshot(source_uuid, 1, %Snap{at: 1}),
              schema: schema,
              serializer: context.serializer
            )
          end)

        erase = Task.async(fn -> store.delete_stream(source_uuid, :any_version, :hard) end)

        LockSession.wait_until_waiting(observer, writer.backend, 1)

        {:ok, :ok} = LockSession.commit(writer)

        assert Task.await(erase) == :ok
        assert StoreRows.stream_id(%{observer: observer, schema: schema}, source_uuid) == nil
        assert count(context, "versioned_snapshots", source_uuid) == 0
        assert count(context, "snapshots", source_uuid) == 0
      end

      test "held before commit, makes a versioned write wait, which then finds no stream",
           context do
        %{store: store, schema: schema, conn: observer, postgrex_config: postgrex_config} =
          context

        source_uuid = stream(store)
        stream_id = StoreRows.stream_id(%{observer: observer, schema: schema}, source_uuid)

        # The store's own hard delete, all five statements, held open before commit.
        eraser = LockSession.start(postgrex_config)

        :ok =
          LockSession.run(eraser, fn tx ->
            Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])
            DeleteStream.hard_delete(tx, stream_id, schema: schema)
          end)

        write =
          Task.async(fn -> store.record_snapshot(snapshot(source_uuid, 1, %Snap{at: 1})) end)

        LockSession.wait_until_waiting(observer, eraser.backend, 1)
        {:ok, :ok} = LockSession.commit(eraser)

        assert Task.await(write) == {:error, :stream_not_found}
        assert count(context, "versioned_snapshots", source_uuid) == 0
      end
    end
  end

  # A stream with one event: a versioned write needs its row.
  defp stream(store) do
    stream_uuid = UUID.uuid4()

    :ok = store.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

    stream_uuid
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

  defp other_type(%SnapshotData{} = snapshot),
    do: %SnapshotData{snapshot | source_type: Atom.to_string(OtherSnap)}

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
