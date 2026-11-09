defmodule Decree.Store.SnapshotRolloutTest do
  use ExUnit.Case

  alias Decree.Store.{Config, EventData, StorageInitializer, UUID}
  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Storage.{Database, Snapshot}
  alias Decree.Store.Streams.Stream
  alias Decree.Store.Tasks.{Create, Init, Migrate}

  @moduletag :migration

  # A rolling deploy across store migration 1.10.0: the store is migrated, and an "old pod"
  # (the snapshot and hard delete SQL at the production pin, rendered from the copies under
  # `test/store/fixture/snapshot_sql_f46d1d039/`) runs beside a "new pod" (this release's
  # store API) on the same rows. The old pod knows only the one-row `snapshots` table.

  @pinned "test/store/fixture/snapshot_sql_f46d1d039"

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

  for {name, schema} <- [{"public", "public"}, {"a named schema", "renamed_store"}] do
    describe "a migrated store under #{name}" do
      setup %{config: config} do
        schema = unquote(schema)
        config = Keyword.put(config, :schema, schema)

        StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
        if schema != "public", do: move_public_schema_to(config)
        Migrate.exec(config, quiet: true, is_mix: false)

        {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

        [conn: conn, schema: schema, source_uuid: stream!(conn, schema)]
      end

      test "an old pod and a new pod each keep their own rows", context do
        old_write(context, 1, "old")
        assert old_read(context) == {:ok, 1, "old"}

        new_write(context, 2, 2, "new at version 2")
        new_write(context, 1, 2, "new at version 1")

        assert new_read(context, 2) == {:ok, 2, "new at version 2"}
        assert new_read(context, 1) == {:ok, 2, "new at version 1"}

        assert old_read(context) == {:ok, 1, "old"}
        assert legacy_read(context) == {:ok, 1, "old"}

        # The old pod's upsert still runs, and moves only its own row.
        old_write(context, 3, "old again")

        assert old_read(context) == {:ok, 3, "old again"}
        assert new_read(context, 1) == {:ok, 2, "new at version 1"}
        assert new_read(context, 2) == {:ok, 2, "new at version 2"}
      end

      test "rolled back, an old pod reads its own row after a new pod wrote", context do
        old_write(context, 1, "old")

        new_write(context, 1, 2, "new at version 1")
        new_write(context, 2, 3, "new at version 2")

        assert old_read(context) == {:ok, 1, "old"}

        old_write(context, 4, "old after rollback")

        assert old_read(context) == {:ok, 4, "old after rollback"}
      end

      test "a new pod's delete removes the old pod's row too", context do
        old_write(context, 1, "old")
        new_write(context, 1, 2, "new at version 1")
        new_write(context, 2, 2, "new at version 2")

        assert :ok = Snapshot.delete_snapshot(context.conn, context.source_uuid, opts(context))

        assert old_read(context) == :none
        assert new_read(context, 1) == {:error, :snapshot_not_found}
        assert new_read(context, 2) == {:error, :snapshot_not_found}
      end

      test "an old pod's delete removes only its own row", context do
        old_write(context, 1, "old")
        new_write(context, 1, 2, "new at version 1")

        old_delete(context)

        assert old_read(context) == :none
        assert new_read(context, 1) == {:ok, 2, "new at version 1"}
      end

      test "an old pod's hard delete leaves no new pod's row, at any version", context do
        assert_old_hard_delete_leaves_no_versioned_row(context)
      end
    end

    # A store created at this release, with an old pod still running beside it.
    describe "a new store under #{name}" do
      setup %{config: config} do
        schema = unquote(schema)
        config = Keyword.put(config, :schema, schema)

        StorageInitializer.drop_database!(config)
        Create.exec(config, quiet: true)
        :ok = Init.exec(config, quiet: true)

        {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

        [conn: conn, schema: schema, source_uuid: stream!(conn, schema)]
      end

      test "an old pod's hard delete leaves no new pod's row, at any version", context do
        assert_old_hard_delete_leaves_no_versioned_row(context)
      end
    end
  end

  # An old pod's hard delete knows nothing of `versioned_snapshots`, so it is the stream
  # row's delete that has to take a new pod's rows with it.
  defp assert_old_hard_delete_leaves_no_versioned_row(context) do
    new_write(context, 1, 2, "new at version 1")
    new_write(context, 2, 2, "new at version 2")
    assert versioned_count(context) == 2

    old_hard_delete(context)

    assert stream_id(context) == nil
    assert new_read(context, 1) == {:error, :snapshot_not_found}
    assert new_read(context, 2) == {:error, :snapshot_not_found}
    assert versioned_count(context) == 0
  end

  # The old pod: the pinned SQL, rendered for the store's schema.

  defp old_write(context, source_version, data) do
    params = [
      context.source_uuid,
      source_version,
      "snapshot",
      data,
      ~s({"snapshot_module_version":1})
    ]

    Postgrex.query!(context.conn, pinned("insert_snapshot", context), params)
  end

  defp old_read(context) do
    case Postgrex.query!(context.conn, pinned("query_snapshot", context), [context.source_uuid]) do
      %Postgrex.Result{rows: []} -> :none
      %Postgrex.Result{rows: [[_uuid, version, _type, data, _meta, _at]]} -> {:ok, version, data}
    end
  end

  defp old_delete(context) do
    Postgrex.query!(context.conn, pinned("delete_snapshot", context), [context.source_uuid])
  end

  # The pinned hard delete's statements that delete rows, in its order, in one transaction:
  # the stream's events and their links, then the stream's row. The pin sends nothing after
  # them.
  defp old_hard_delete(context) do
    stream_id = stream_id(context)

    {:ok, %Postgrex.Result{num_rows: 1, rows: [[^stream_id]]}} =
      Postgrex.transaction(context.conn, fn tx ->
        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])
        Postgrex.query!(tx, pinned("hard_delete_stream", context), [stream_id])
        Postgrex.query!(tx, pinned("delete_stream_row", context), [stream_id])
      end)
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

  defp legacy_read(context) do
    case Snapshot.read_snapshot(context.conn, context.source_uuid, opts(context)) do
      {:ok, %SnapshotData{snapshot_version: nil} = snapshot} ->
        {:ok, snapshot.source_version, snapshot.data}

      {:error, _reason} = error ->
        error
    end
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
