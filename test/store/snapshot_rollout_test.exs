defmodule Decree.Store.SnapshotRolloutTest do
  use ExUnit.Case

  alias Decree.Store.{Config, EventData, StorageInitializer, UUID}
  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Storage.{Database, Snapshot}
  alias Decree.Store.Streams.Stream
  alias Decree.Store.Tasks.{Create, Init, Migrate}

  @moduletag :migration

  # A rolling deploy from release 1 of sparkle-space/sparklespace#8200, decree 60077090, to
  # this release, which drops the one-row `snapshots` table in store migration 1.11.0: an "old
  # pod" (60077090's snapshot and hard delete SQL, copied byte for byte under
  # `test/store/fixture/snapshot_sql_60077090/` and never edited) runs beside a "new pod" (this
  # release's store API) on the same rows of a store at 1.11.0, a migrated store and one this
  # release's Init created, under `public` and a named schema. Release 1 reads, writes and
  # deletes `versioned_snapshots` only, and its hard delete is four statements that name no
  # legacy table, so every old-pod path works with the table gone.

  @pinned "test/store/fixture/snapshot_sql_60077090"

  # The tag the old pod's hard delete refuses a stream for, its `TagIndex.fact_tag/0`.
  @fact_tag "tag_index:fact"

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  setup_all do
    config = TestEventStore.config()

    on_exit(&StorageInitializer.recreate_test_event_store_database!/0)

    [config: config]
  end

  for {store, store_name} <- [{:migrated, "a migrated store"}, {:new, "a new store"}],
      {schema_name, schema} <- [{"public", "public"}, {"a named schema", "renamed_store"}] do
    describe "#{store_name} under #{schema_name}" do
      setup %{config: config} do
        schema = unquote(schema)
        config = Keyword.put(config, :schema, schema)

        prepare_store!(unquote(store), config)

        {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

        # The premise: the store is at 1.11.0, and its legacy table is gone.
        assert store_version(conn, schema) == [1, 11, 0]
        assert legacy_table(conn, schema) == nil

        [conn: conn, schema: schema, source_uuid: stream!(conn, schema)]
      end

      test "an old pod and a new pod read each other's rows at a version", context do
        old_write(context, 1, 1, "old at version 1")

        assert new_read(context, 1) == {:ok, 1, "old at version 1"}

        new_write(context, 1, 2, "new at version 1")
        new_write(context, 2, 2, "new at version 2")

        assert old_read(context, 1) == {:ok, 2, "new at version 1"}
        assert old_read(context, 2) == {:ok, 2, "new at version 2"}

        # Rolled back, the old pod's upsert moves only its own version's row.
        old_write(context, 1, 3, "old again")

        assert new_read(context, 1) == {:ok, 3, "old again"}
        assert new_read(context, 2) == {:ok, 2, "new at version 2"}
        assert old_read(context, 2) == {:ok, 2, "new at version 2"}
      end

      test "a new pod's unversioned read and write are refused, and move no row", context do
        old_write(context, 1, 1, "old at version 1")

        assert new_unversioned_read(context) == {:error, :invalid_snapshot_version}
        assert new_unversioned_write(context) == {:error, :invalid_snapshot_version}

        assert old_read(context, 1) == {:ok, 1, "old at version 1"}
        assert versioned_count(context) == 1
      end

      test "a new pod's delete removes every version's row, the old pod's too", context do
        old_write(context, 1, 2, "old at version 1")
        new_write(context, 2, 2, "new at version 2")

        assert :ok = Snapshot.delete_snapshot(context.conn, context.source_uuid, opts(context))

        assert old_read(context, 1) == :none
        assert new_read(context, 2) == {:error, :snapshot_not_found}
        assert versioned_count(context) == 0
      end

      test "an old pod's delete removes every version's row, the new pod's too", context do
        old_write(context, 1, 2, "old at version 1")
        new_write(context, 2, 2, "new at version 2")

        old_delete(context)

        assert new_read(context, 1) == {:error, :snapshot_not_found}
        assert new_read(context, 2) == {:error, :snapshot_not_found}
        assert versioned_count(context) == 0
      end

      test "an old pod's prune deletes a type's rows below a version, the new pod's too",
           context do
        new_write(context, 1, 2, "new at version 1")
        old_write(context, 2, 2, "old at version 2")
        new_write(context, 3, 2, "new at version 3")

        assert old_prune(context, "snapshot", 3) == 2

        assert new_read(context, 1) == {:error, :snapshot_not_found}
        assert old_read(context, 2) == :none
        assert new_read(context, 3) == {:ok, 2, "new at version 3"}
        assert old_prune(context, "snapshot", 3) == 0
      end

      test "a new pod's prune deletes a type's rows below a version, the old pod's too",
           context do
        old_write(context, 1, 2, "old at version 1")
        new_write(context, 2, 2, "new at version 2")
        old_write(context, 3, 2, "old at version 3")

        assert Snapshot.prune_snapshots(context.conn, "snapshot", 3, opts(context)) == {:ok, 2}

        assert old_read(context, 1) == :none
        assert new_read(context, 2) == {:error, :snapshot_not_found}
        assert old_read(context, 3) == {:ok, 2, "old at version 3"}
      end

      test "an old pod's hard delete removes every version's row, the new pod's too",
           context do
        old_write(context, 1, 2, "old at version 1")
        new_write(context, 2, 2, "new at version 2")
        assert versioned_count(context) == 2

        assert old_hard_delete(context) == :ok

        assert stream_id(context) == nil
        assert new_read(context, 1) == {:error, :snapshot_not_found}
        assert new_read(context, 2) == {:error, :snapshot_not_found}
        assert versioned_count(context) == 0
      end

      test "a new pod's hard delete removes every version's row, the old pod's too",
           context do
        old_write(context, 1, 2, "old at version 1")
        new_write(context, 2, 2, "new at version 2")

        assert :ok =
                 Stream.delete(context.conn, context.source_uuid, :any_version, :hard,
                   schema: context.schema,
                   enable_hard_deletes: true
                 )

        assert stream_id(context) == nil
        assert old_read(context, 1) == :none
        assert old_read(context, 2) == :none
        assert versioned_count(context) == 0
      end
    end
  end

  defp prepare_store!(:migrated, config) do
    StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
    if Keyword.fetch!(config, :schema) != "public", do: move_public_schema_to(config)
    Migrate.exec(config, quiet: true, is_mix: false)
  end

  defp prepare_store!(:new, config) do
    StorageInitializer.drop_database!(config)
    Create.exec(config, quiet: true)
    :ok = Init.exec(config, quiet: true)
  end

  # The old pod: the pinned SQL, rendered for the store's schema, sent as 60077090's
  # `Decree.Store.Storage.Snapshot` and hard delete send it.

  # A versioned write: the stream's row FOR KEY SHARE, then the upsert, in one transaction.
  defp old_write(context, snapshot_version, source_version, data) do
    %{conn: conn, source_uuid: source_uuid} = context

    params = [
      source_uuid,
      snapshot_version,
      source_version,
      "snapshot",
      data,
      ~s({"snapshot_module_version":#{snapshot_version}})
    ]

    {:ok, %Postgrex.Result{}} =
      Postgrex.transaction(conn, fn tx ->
        %Postgrex.Result{num_rows: 1} =
          Postgrex.query!(tx, pinned("lock_snapshot_stream", context), [source_uuid])

        Postgrex.query!(tx, pinned("insert_versioned_snapshot", context), params)
      end)
  end

  defp old_read(context, snapshot_version) do
    params = [context.source_uuid, snapshot_version]

    case Postgrex.query!(context.conn, pinned("query_versioned_snapshot", context), params) do
      %Postgrex.Result{rows: []} ->
        :none

      %Postgrex.Result{rows: [[_uuid, version, _type, data, _meta, _at, ^snapshot_version]]} ->
        {:ok, version, data}
    end
  end

  defp old_delete(context) do
    Postgrex.query!(context.conn, pinned("delete_snapshot", context), [context.source_uuid])
  end

  defp old_prune(context, source_type, snapshot_version) do
    %Postgrex.Result{num_rows: count} =
      Postgrex.query!(context.conn, pinned("prune_snapshots", context), [
        source_type,
        snapshot_version
      ])

    count
  end

  # The pinned hard delete at `:any_version`: the fence on, its four statements in its order,
  # each answer matched as 60077090 matches it, and the fence closed as the last statement, in
  # one transaction. `delete_stream_row` cascades to the stream's versioned rows.
  defp old_hard_delete(context) do
    %{conn: conn} = context
    stream_id = stream_id(context)

    {:ok, :ok} =
      Postgrex.transaction(conn, fn tx ->
        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on';", [])

        %Postgrex.Result{rows: [[_stream_version, nil]]} =
          Postgrex.query!(tx, pinned("lock_stream", context), [stream_id])

        %Postgrex.Result{num_rows: 0} =
          Postgrex.query!(tx, pinned("query_index_facts", context), [stream_id, @fact_tag])

        Postgrex.query!(tx, pinned("hard_delete_stream", context), [stream_id])

        %Postgrex.Result{num_rows: 1, rows: [[^stream_id]]} =
          Postgrex.query!(tx, pinned("delete_stream_row", context), [stream_id])

        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'off';", [])

        :ok
      end)

    :ok
  end

  defp pinned(statement, context) do
    @pinned
    |> Path.join("#{statement}.sql.eex")
    |> EEx.eval_file(schema: context.schema)
  end

  # The new pod: this release's store API.

  defp new_write(context, snapshot_version, source_version, data) do
    snapshot = %SnapshotData{
      source_uuid: context.source_uuid,
      source_version: source_version,
      source_type: "snapshot",
      snapshot_version: snapshot_version,
      data: data,
      metadata: ~s({"snapshot_module_version":#{snapshot_version}})
    }

    :ok = Snapshot.record_snapshot(context.conn, snapshot, opts(context))
  end

  defp new_read(context, snapshot_version) do
    opts = Keyword.put(opts(context), :snapshot_version, snapshot_version)

    case Snapshot.read_snapshot(context.conn, context.source_uuid, opts) do
      {:ok, %SnapshotData{snapshot_version: ^snapshot_version} = snapshot} ->
        {:ok, snapshot.source_version, snapshot.data}

      {:error, _reason} = error ->
        error
    end
  end

  defp new_unversioned_read(context),
    do: Snapshot.read_snapshot(context.conn, context.source_uuid, opts(context))

  defp new_unversioned_write(context) do
    snapshot = %SnapshotData{
      source_uuid: context.source_uuid,
      source_version: 4,
      source_type: "snapshot",
      data: "new unversioned",
      metadata: "{}"
    }

    Snapshot.record_snapshot(context.conn, snapshot, opts(context))
  end

  defp opts(context), do: [schema: context.schema]

  defp stream_id(context) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        context.conn,
        ~s|SELECT stream_id FROM "#{context.schema}".streams WHERE stream_uuid = $1|,
        [context.source_uuid]
      )

    case rows do
      [] -> nil
      [[stream_id]] -> stream_id
    end
  end

  defp store_version(conn, schema) do
    %Postgrex.Result{rows: [version]} =
      Postgrex.query!(
        conn,
        ~s|SELECT major_version, minor_version, patch_version FROM "#{schema}".schema_migrations| <>
          " ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 1",
        []
      )

    version
  end

  defp legacy_table(conn, schema) do
    %Postgrex.Result{rows: [[table]]} =
      Postgrex.query!(conn, "SELECT to_regclass(format('%I.snapshots', $1::text))::text", [
        schema
      ])

    table
  end

  defp versioned_count(context) do
    %Postgrex.Result{rows: [[count]]} =
      Postgrex.query!(
        context.conn,
        ~s|SELECT count(*) FROM "#{context.schema}".versioned_snapshots WHERE source_uuid = $1|,
        [context.source_uuid]
      )

    count
  end

  # A versioned write takes its stream's row, so the source is a stream with an event.
  defp stream!(conn, schema) do
    stream_uuid = UUID.uuid4()
    event = %EventData{event_type: "typed", data: "{}", metadata: "{}"}

    :ok =
      Stream.append_to_stream(conn, stream_uuid, 0, [event],
        schema: schema,
        serializer: PassThroughSerializer
      )

    stream_uuid
  end

  # As `Decree.Store.MigrateEventStoreTest` moves it: the restored `public` becomes the
  # named store, and a fresh `public` stays behind.
  defp move_public_schema_to(config) do
    schema = Keyword.fetch!(config, :schema)
    username = Keyword.fetch!(config, :username)

    Enum.each(
      [
        ~s(ALTER SCHEMA public RENAME TO "#{schema}"),
        ~s(ALTER SCHEMA "#{schema}" OWNER TO "#{username}"),
        ~s(COMMENT ON SCHEMA "#{schema}" IS NULL),
        "CREATE SCHEMA public"
      ],
      fn statement -> :ok = Database.execute(config, statement) end
    )
  end
end
