defmodule Decree.Store.SnapshotRolloutTest do
  use ExUnit.Case

  alias Decree.Store.{Config, EventData, UUID}
  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Storage.{Database, Snapshot}
  alias Decree.Store.Streams.Stream
  alias Decree.Store.Tasks.Migrate

  @moduletag :migration

  # A rolling deploy across store migration 1.10.0: the store is migrated, and an "old pod"
  # (the snapshot SQL at the production pin, rendered from the copies under
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

    on_exit(&Decree.Store.StorageInitializer.recreate_test_event_store_database!/0)

    [config: config]
  end

  for {name, schema} <- [{"public", "public"}, {"a named schema", "renamed_store"}] do
    describe "a migrated store under #{name}" do
      setup %{config: config} do
        schema = unquote(schema)
        config = Keyword.put(config, :schema, schema)

        restore_pre_migration_event_store(config)
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
    end
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

  defp restore_pre_migration_event_store(config) do
    Database.drop(config)
    :ok = Database.create(config)

    {_, 0} = Database.restore(config, "test/store/fixture/eventstore.dump")
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
