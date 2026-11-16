defmodule Decree.Store.MigrateEventStoreTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  alias Decree.Store.Storage.Database
  alias Decree.Store.Streams.Stream
  alias Decree.Store.Tasks.{Create, Init, Migrate, Migrations}

  alias Decree.Store.{
    AppendError,
    AppendResult,
    Config,
    EventData,
    ReadResult,
    RecordedEvent,
    StorageInitializer,
    TcpProxy,
    UUID
  }

  @moduletag :migration

  # The latest `append_events_v<n>`. A later version adds a function and leaves this
  # one in place, so a migrated store carries them all and a fresh one only the latest.
  @latest_append_function "append_events_v5"

  # pg_dump picks the quote per body: `$_$` for one that holds a `$`, as `$all` does.
  @dollar_quote_end ~r/^\$\w*\$;$/

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

  describe "migrate event store" do
    test "should prevent migrations from running concurrently", %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")

      log =
        [
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end),
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end),
          Task.async(fn ->
            capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end)
          end)
        ]
        |> Enum.map(&Task.await/1)
        |> Enum.sort()

      # One migration should succeed, two should be prevented due to already running
      assert Enum.at(log, 0) =~ "Decree.Store database migration already in progress."
      assert Enum.at(log, 1) =~ "Decree.Store database migration already in progress."
      assert Enum.at(log, 2) =~ "The Decree.Store database has been migrated."

      assert length(log) == 3
    end
  end

  describe "migrated event store schema" do
    test "should be identical to new event store schema", %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
      migrate_eventstore(config, quiet: true, is_mix: false)

      # Dump migrated database
      {migrated_schema, 0} = Database.dump(config, [])
      migrated_function = append_function(config)

      create_new_database(config)

      # Dump newly created database
      {new_schema, 0} = Database.dump(config, [])

      assert_same_schema(migrated_schema, new_schema)
      assert migrated_function == append_function(config)
    end

    test "should be identical under a named schema", %{config: config} do
      config = Keyword.put(config, :schema, "renamed_store")

      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
      move_public_schema_to(config)
      migrate_eventstore(config, quiet: true, is_mix: false)

      migrated_schema = dump_schema(config)
      migrated_function = append_function(config)

      create_new_named_database(config)

      assert_same_schema(migrated_schema, dump_schema(config))
      assert migrated_function == append_function(config)
    end

    test "should be identical for a jsonb store under a named schema", %{config: config} do
      config =
        Keyword.merge(config,
          schema: "jsonb_store",
          column_data_type: "jsonb",
          correlation_id_type: "text",
          causation_id_type: "text"
        )

      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
      move_public_schema_to(config)

      execute_each(config, [
        """
        ALTER TABLE jsonb_store.events
          ALTER COLUMN data TYPE jsonb USING convert_from(data, 'UTF8')::jsonb,
          ALTER COLUMN metadata TYPE jsonb USING convert_from(metadata, 'UTF8')::jsonb,
          ALTER COLUMN causation_id TYPE text,
          ALTER COLUMN correlation_id TYPE text
        """,
        """
        ALTER TABLE jsonb_store.snapshots
          ALTER COLUMN data TYPE jsonb USING convert_from(data, 'UTF8')::jsonb,
          ALTER COLUMN metadata TYPE jsonb USING convert_from(metadata, 'UTF8')::jsonb
        """
      ])

      migrate_eventstore(config, quiet: true, is_mix: false)

      migrated_schema = dump_schema(config)
      migrated_function = append_function(config)

      assert migrated_function =~ "p_data jsonb[]"
      assert migrated_function =~ "p_causation_ids text[]"
      assert migrated_function =~ "p_correlation_ids text[]"
      assert migrated_function =~ "p_tags text[]"
      assert migrated_function =~ "p_tag_event_types text[]"
      assert migrated_function =~ "p_after bigint"
      assert migrated_function =~ "OUT o_stream_ids bigint[]"
      assert migrated_function =~ "OUT o_stream_versions bigint[]"
      assert migrated_function =~ "OUT o_position bigint"
      assert migrated_function =~ "RETURNS record"
      assert migrated_function =~ "SET enable_seqscan TO 'off'"

      assert_append_round_trips(config)

      create_new_named_database(config)

      assert_append_round_trips(config)

      # The round trip wrote rows on both sides.
      assert_same_schema(migrated_schema, dump_schema(config), :schema_only)
      assert migrated_function == append_function(config)
    end
  end

  # A migration runs from a release Job, so its failure message reaches persistent pod logs.
  # A `Postgrex.Error` carries the row values it failed on in its detail; the message names
  # the failure by its class only (sparkle-space/sparklespace#8352).
  describe "a failed migration's message" do
    @detail_marker "migrate-detail-marker"

    test "names a failed migration step by its class only", %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")

      # Every migration records its version last: the insert fails, with the marker in the
      # error's detail.
      execute_each(config, [
        """
        CREATE FUNCTION public.refuse_schema_migration() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'migration refused' USING DETAIL = '#{@detail_marker}';
        END;
        $$
        """,
        """
        CREATE TRIGGER refuse_schema_migration BEFORE INSERT ON public.schema_migrations
        FOR EACH ROW EXECUTE FUNCTION public.refuse_schema_migration()
        """
      ])

      message = failed_migration_message(config)

      assert message =~ "The Decree.Store database couldn't be migrated, reason given: "
      assert message =~ "{Postgrex.Error, :raise_exception}"
      refute message =~ @detail_marker
    end

    test "names a failed migration lock by its class only", %{config: config} do
      create_new_database(config)

      # The lock query calls `pg_try_advisory_lock/2` unqualified: a function of the same
      # name in a schema searched before `pg_catalog` answers it, and raises with the marker
      # in its detail.
      execute_each(config, [
        "CREATE SCHEMA lock_refusal",
        """
        CREATE FUNCTION lock_refusal.pg_try_advisory_lock(integer, integer) RETURNS boolean
        LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'lock refused' USING DETAIL = '#{@detail_marker}';
        END;
        $$
        """
      ])

      config = Keyword.put(config, :parameters, search_path: "lock_refusal, pg_catalog, public")

      message = failed_migration_message(config)

      assert message =~
               "The Decree.Store database could not be migrated. Could not acquire migration lock due to: "

      assert message =~ "{Postgrex.Error, :raise_exception}"
      refute message =~ @detail_marker
    end

    # A lock query that fails with an error other than a `Postgrex.Error` is named by its
    # class too: through a proxy that refuses every connection, the lock connection never
    # connects and the query is dropped from its queue.
    @tag capture_log: true
    test "names a failed migration lock's connection error by its class only", %{
      config: config
    } do
      proxy =
        start_supervised!(
          {TcpProxy, upstream: {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
        )

      :ok = TcpProxy.refuse(proxy)
      port = TcpProxy.port(proxy)

      config =
        Keyword.merge(config,
          hostname: "127.0.0.1",
          port: port,
          queue_target: 10,
          queue_interval: 100
        )

      message = failed_migration_message(config)

      assert message =~
               "The Decree.Store database could not be migrated. Could not acquire migration lock due to: " <>
                 "{DBConnection.ConnectionError, :queue_timeout}"

      refute message =~ "127.0.0.1"
      refute message =~ Integer.to_string(port)
    end
  end

  describe "version bookkeeping" do
    test "the store knows migration 1.11.0" do
      assert "1.10.0" in Migrations.available_migrations()
      assert List.last(Migrations.available_migrations()) == "1.11.0"
    end

    test "a new event store records 1.11.0 and has nothing pending", %{config: config} do
      create_new_database(config)

      assert schema_versions(config) == [[1, 11, 0]]

      assert capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end) =~
               "already migrated"

      assert schema_versions(config) == [[1, 11, 0]]
    end

    test "the migration records 1.10.0 and then 1.11.0", %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
      migrate_eventstore(config, quiet: true, is_mix: false)

      assert Enum.take(schema_versions(config), -2) == [[1, 10, 0], [1, 11, 0]]

      assert capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end) =~
               "already migrated"
    end

    test "Init creates versioned_snapshots and no snapshots", %{config: config} do
      create_new_database(config)

      assert snapshot_tables(config) == ["versioned_snapshots"]
    end

    test "Init creates only the latest append function", %{config: config} do
      create_new_database(config)

      assert append_function_names(config) == [@latest_append_function]
    end
  end

  # Store migration 1.11.0 drops the one-row `snapshots` table, which no pod of the previous
  # release reads or writes (sparkle-space/sparklespace#8200). It drops the table whatever it
  # holds: a store whose table still has rows migrates, and loses them with the table.
  describe "the legacy snapshots table" do
    test "is dropped with its rows, and versioned_snapshots stays", %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
      insert_legacy_rows(config, 3)

      assert legacy_row_count(config, "public") >= 3

      migrate_eventstore(config, quiet: true, is_mix: false)

      assert snapshot_tables(config) == ["versioned_snapshots"]
      assert List.last(schema_versions(config)) == [1, 11, 0]
    end

    test "is dropped from a named store only, and another schema's table stays",
         %{config: config} do
      config = Keyword.put(config, :schema, "renamed_store")

      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
      move_public_schema_to(config)
      insert_legacy_rows(config, 3)

      # A same-named table in another schema, which the migration must not reach.
      execute_each(config, [
        "CREATE TABLE public.snapshots (source_uuid text PRIMARY KEY)",
        "INSERT INTO public.snapshots (source_uuid) VALUES ('elsewhere')"
      ])

      migrate_eventstore(config, quiet: true, is_mix: false)

      assert snapshot_tables(config) == ["versioned_snapshots"]
      assert List.last(schema_versions(config)) == [1, 11, 0]
      assert legacy_row_count(config, "public") == 1
    end
  end

  # The rolling-deploy case: the migration has run and an old pod still appends.
  describe "append_events_v1 on a migrated store" do
    setup %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
      migrate_eventstore(config, quiet: true, is_mix: false)

      {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

      [conn: conn]
    end

    test "still works, and its events simply have no rows", %{config: config, conn: conn} do
      assert append_function_names(config) ==
               [
                 "append_events_v1",
                 "append_events_v2",
                 "append_events_v3",
                 "append_events_v4",
                 @latest_append_function
               ]

      all_version = all_version(conn)

      assert %Postgrex.Result{rows: [[stream_id]]} = append_v1(conn, UUID.uuid4())
      assert is_integer(stream_id)

      assert all_version(conn) == all_version + 1
      assert tag_rows(conn) == []
      assert type_rows(conn) == []
    end

    test "leaves the next append the next position", %{conn: conn} do
      all_version = all_version(conn)

      append_v1(conn, UUID.uuid4())

      event = %EventData{
        event_type: "typed",
        data: "{}",
        metadata: "{}",
        tags: ["tenant:a"]
      }

      assert :ok =
               Stream.append_to_stream(conn, UUID.uuid4(), 0, [event],
                 schema: "public",
                 serializer: PassThroughSerializer
               )

      assert tag_rows(conn) == [[all_version + 2, "tenant:a", "typed"]]
      assert type_rows(conn) == [[all_version + 2, "typed"]]
    end

    # The coverage gap `Decree.Store` documents: `after` counts what no query can see.
    test "is not matched by a query, while `after` counts it", %{conn: conn} do
      all_version = all_version(conn)

      opts = [
        schema: "public",
        serializer: PassThroughSerializer,
        correlation_id_type: "text",
        causation_id_type: "text",
        count: 10
      ]

      append_v1(conn, UUID.uuid4())

      assert {:ok, %ReadResult{events: [], after: after_v1}} =
               Stream.read_query(conn, [[types: ["typed"]]], opts)

      assert after_v1 == all_version + 1

      event = %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: ["tenant:a"]}

      assert :ok =
               Stream.append_to_stream(conn, UUID.uuid4(), 0, [event],
                 schema: "public",
                 serializer: PassThroughSerializer
               )

      assert {:ok, %ReadResult{events: [%RecordedEvent{event_number: position}], after: head}} =
               Stream.read_query(conn, [[types: ["typed"]]], opts)

      assert position == all_version + 2
      assert head == position
    end
  end

  # The other side of a rolling deploy: the store is at 1.9.0, new pods call v5, and old
  # pods still call v1, v2, v3 or v4, on the same positions.
  describe "append_events_v5 beside the older versions on a migrated store" do
    setup %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")
      migrate_eventstore(config, quiet: true, is_mix: false)

      {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

      [conn: conn]
    end

    test "every version is there", %{config: config} do
      assert append_function_names(config) ==
               [
                 "append_events_v1",
                 "append_events_v2",
                 "append_events_v3",
                 "append_events_v4",
                 "append_events_v5"
               ]
    end

    test "v2, v3, v4 and v5 calls mix on one run of positions", %{conn: conn} do
      head = all_version(conn)

      assert %Postgrex.Result{rows: [[_stream_id]]} = append_v2(conn, UUID.uuid4(), ["tenant:a"])
      assert :ok = append_v5(conn, ["tenant:b"])
      assert %Postgrex.Result{rows: [[_stream_id]]} = append_v3(conn, UUID.uuid4(), ["tenant:c"])
      assert %Postgrex.Result{rows: [[[_stream_id]]]} = append_v4(conn, ["tenant:d"])
      assert :ok = append_v5(conn, ["tenant:e"], [[tags: ["tenant:f"]]], head)

      assert all_version(conn) == head + 5

      assert tag_rows(conn) == [
               [head + 1, "tenant:a", "typed"],
               [head + 2, "tenant:b", "typed"],
               [head + 3, "tenant:c", "typed"],
               [head + 4, "tenant:d", "typed"],
               [head + 5, "tenant:e", "typed"]
             ]

      assert length(type_rows(conn)) == 5
      assert all_positions(conn) == Enum.to_list(1..(head + 5))
    end

    test "a v4 call still answers its stream ids, and a stale one raises", %{conn: conn} do
      head = all_version(conn)

      assert %Postgrex.Result{rows: [[[stream_id]]]} = append_v4(conn, ["tenant:a"])
      assert is_integer(stream_id)
      assert all_version(conn) == head + 1

      assert {:error, %Postgrex.Error{postgres: %{code: :unique_violation}}} =
               append_v4(conn, [], stream_id: stream_id, expected: 0, query: &Postgrex.query/3)

      assert all_version(conn) == head + 1
    end

    test "a v5 call answers what it wrote, reading back what v4 wrote", %{conn: conn} do
      head = all_version(conn)
      stream_uuid = UUID.uuid4()

      assert %Postgrex.Result{rows: [[[_stream_id]]]} =
               append_v4(conn, [], stream_uuid: stream_uuid)

      assert {:ok, %AppendResult{} = result} =
               Stream.append_to_stream(conn, stream_uuid, 1, [typed_event([])],
                 schema: "public",
                 serializer: PassThroughSerializer,
                 returning: :append_result
               )

      assert result == %AppendResult{position: head + 2, stream_versions: %{stream_uuid => 2}}
    end

    # The order a rollout must keep: a pod ahead of its migration fails closed, and a
    # retry cannot help it.
    test "a pod ahead of the migration is refused, and writes nothing", %{conn: conn} do
      head = all_version(conn)

      Postgrex.query!(conn, "DROP ROUTINE public.append_events_v5", [])

      assert {:error, %AppendError{class: :rejected, code: :undefined_function}} =
               append_v5(conn, ["tenant:a"])

      assert all_version(conn) == head
    end

    test "a v3 call still answers its stream id, and a gone row NULL", %{conn: conn} do
      head = all_version(conn)
      stream_uuid = UUID.uuid4()

      assert %Postgrex.Result{rows: [[stream_id]]} = append_v3(conn, stream_uuid, ["tenant:a"])
      assert is_integer(stream_id)
      assert all_version(conn) == head + 1

      assert %Postgrex.Result{rows: [[nil]]} = append_v3(conn, stream_uuid, [], 123_456)
      assert all_version(conn) == head + 1
    end

    test "a multi-stream append and an old pod's calls mix on one run of positions",
         %{conn: conn} do
      head = all_version(conn)
      first = UUID.uuid4()
      second = UUID.uuid4()
      event = %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: ["tenant:b"]}

      assert %Postgrex.Result{rows: [[_stream_id]]} = append_v3(conn, first, ["tenant:a"])

      assert :ok =
               Stream.append_to_streams(
                 conn,
                 [{first, 1, [event]}, {second, 0, [event, %{event | tags: []}]}],
                 schema: "public",
                 serializer: PassThroughSerializer
               )

      assert %Postgrex.Result{rows: [[_stream_id]]} = append_v2(conn, UUID.uuid4(), ["tenant:c"])

      assert all_positions(conn) == Enum.to_list(1..(head + 5))

      assert Enum.map(tag_rows(conn), &tl/1) == [
               ["tenant:a", "typed"],
               ["tenant:b", "typed"],
               ["tenant:b", "typed"],
               ["tenant:c", "typed"]
             ]
    end

    test "a v2 writer's tagged event trips a v5 condition", %{conn: conn} do
      head = all_version(conn)

      append_v2(conn, UUID.uuid4(), ["tenant:a"])

      assert append_v5(conn, [], [[tags: ["tenant:a"]]], head) ==
               {:error, :append_condition_failed}

      assert all_version(conn) == head + 1
    end

    test "a v3 writer's tagged event trips a v5 condition", %{conn: conn} do
      head = all_version(conn)

      append_v3(conn, UUID.uuid4(), ["tenant:a"])

      assert append_v5(conn, [], [[tags: ["tenant:a"]]], head) ==
               {:error, :append_condition_failed}

      assert all_version(conn) == head + 1
    end

    test "a v4 writer's tagged event trips a v5 condition", %{conn: conn} do
      head = all_version(conn)

      append_v4(conn, ["tenant:a"])

      assert append_v5(conn, [], [[tags: ["tenant:a"]]], head) ==
               {:error, :append_condition_failed}

      assert all_version(conn) == head + 1
    end

    # The coverage gap `Decree.Store` documents, on a write path: an event from v1 has
    # no rows, so a condition passes that a read of the stream would call a conflict.
    test "a v1 writer's event does not trip a v5 condition", %{conn: conn} do
      head = all_version(conn)

      append_v1(conn, UUID.uuid4())

      assert append_v5(conn, [], [[types: ["typed"]]], head) == :ok
      assert all_version(conn) == head + 2
    end
  end

  describe "comparing dumps" do
    test "drops an older append function and keeps the latest" do
      dump = """
      CREATE FUNCTION example.append_events_v1(p_schema text) RETURNS bigint
      AS $$
      older body
      $$;
      ALTER FUNCTION example.append_events_v1(p_schema text) OWNER TO postgres;
      CREATE FUNCTION example.append_events_v2(p_schema text) RETURNS bigint
      AS $$
      latest body
      $$;
      ALTER FUNCTION example.append_events_v2(p_schema text) OWNER TO postgres;
      CREATE FUNCTION example.notify_events() RETURNS trigger
      AS $$
      another body
      $$;
      """

      assert split_and_normalize([dump]) == [
               "CREATE FUNCTION example.append_events_v2(p_schema text) RETURNS bigint",
               "AS $$",
               "latest body",
               "$$;",
               "ALTER FUNCTION example.append_events_v2(p_schema text) OWNER TO postgres;",
               "CREATE FUNCTION example.notify_events() RETURNS trigger",
               "AS $$",
               "another body",
               "$$;",
               ""
             ]
    end

    test "ends an older append function at pg_dump's `$_$` quote" do
      dump = """
      CREATE FUNCTION example.append_events_v1(p_schema text) RETURNS bigint
      AS $_$
      older body with $all
      $_$;
      CREATE FUNCTION example.append_events_v2(p_schema text) RETURNS bigint
      AS $_$
      latest body with $all
      $_$;
      """

      assert split_and_normalize([dump]) == [
               "CREATE FUNCTION example.append_events_v2(p_schema text) RETURNS bigint",
               "AS $_$",
               "latest body with $all",
               "$_$;",
               ""
             ]
    end

    test "keeps the only append function" do
      dump = """
      CREATE FUNCTION example.append_events_v1(p_schema text) RETURNS bigint
      AS $$
      the body
      $$;
      """

      assert split_and_normalize([dump]) == [
               "CREATE FUNCTION example.append_events_v1(p_schema text) RETURNS bigint",
               "AS $$",
               "the body",
               "$$;",
               ""
             ]
    end
  end

  defp assert_same_schema(migrated_schema, new_schema, mode \\ :with_data) do
    migrated_normalized = comparable(migrated_schema, mode)
    new_normalized = comparable(new_schema, mode)

    [migrated_normalized, new_normalized]
    |> Enum.zip()
    |> Enum.each(fn {migrated, new} ->
      assert migrated == new
    end)

    assert length(migrated_normalized) == length(new_normalized)
  end

  # pg_dump's blank lines around a dropped function stay behind, so they do not count.
  # `pg_get_functiondef` compares the function's own text, blank lines included.
  defp comparable(dump, :with_data) do
    dump |> split_and_normalize() |> reject_blank_lines()
  end

  defp comparable(dump, :schema_only) do
    dump |> split_and_normalize() |> reject_data() |> reject_blank_lines()
  end

  defp reject_blank_lines(lines), do: Enum.reject(lines, &(&1 == ""))

  # A round trip's rows differ per side: `COPY` blocks are dropped from the comparison.
  defp reject_data(lines) do
    {kept, _in_copy} =
      Enum.flat_map_reduce(lines, false, fn
        "COPY " <> _rest, false -> {[], true}
        "\\.", true -> {[], false}
        _line, true -> {[], true}
        "SELECT pg_catalog.setval(" <> _rest, false -> {[], false}
        line, false -> {[line], false}
      end)

    kept
  end

  defp split_and_normalize(enumerable) do
    enumerable
    |> Enum.join()
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.map(
      &Regex.replace(
        ~r/[0-9]{4}\-[0-9]{2}\-[0-9]{2}\s[0-9]{2}\:[0-9]{2}\:[0-9]{2}\.[0-9]{0,6}\+[0-9]{2}/,
        &1,
        "NOW"
      )
    )
    # Ignore comments and pg_dump version-specific directives with random tokens
    |> Enum.reject(&String.starts_with?(&1, "--"))
    |> Enum.reject(&String.starts_with?(&1, "\\restrict "))
    |> Enum.reject(&String.starts_with?(&1, "\\unrestrict "))
    |> Enum.reject(fn
      "0\t17\t0\tNOW" -> true
      "1\t1\t0\tNOW" -> true
      "1\t2\t0\tNOW" -> true
      "1\t3\t0\tNOW" -> true
      "1\t3\t2\tNOW" -> true
      "1\t4\t0\tNOW" -> true
      "1\t5\t0\tNOW" -> true
      "1\t6\t0\tNOW" -> true
      "1\t7\t0\tNOW" -> true
      "1\t8\t0\tNOW" -> true
      "1\t9\t0\tNOW" -> true
      "1\t10\t0\tNOW" -> true
      _line -> false
    end)
    |> reject_superseded_append_functions()
  end

  # A migrated store keeps every `append_events_v<n>`, so that an old pod's call still
  # works, and a new one carries only the latest. These are the one allowed difference.
  defp reject_superseded_append_functions(lines) do
    case append_function_versions(lines) do
      [] ->
        lines

      versions ->
        latest = Enum.max(versions)

        {kept, _in_function} =
          Enum.flat_map_reduce(lines, false, fn line, in_function ->
            case {in_function, append_function_line(line)} do
              {true, _match} -> {[], not Regex.match?(@dollar_quote_end, line)}
              {false, {"CREATE", version}} when version < latest -> {[], true}
              {false, {"ALTER", version}} when version < latest -> {[], false}
              {false, _match} -> {[line], false}
            end
          end)

        kept
    end
  end

  defp append_function_versions(lines) do
    for line <- lines, {_verb, version} <- [append_function_line(line)], do: version
  end

  defp append_function_line(line) do
    case Regex.run(~r/^(CREATE|ALTER) FUNCTION \S*append_events_v(\d+)\(/, line) do
      [_line, verb, version] -> {verb, String.to_integer(version)}
      nil -> nil
    end
  end

  # `Database.dump/2` dumps the whole database. A named store is compared by its own
  # schema, without privileges: moving the `public` of a restored dump leaves a `public`
  # that is not the one a new database has, and takes `public`'s grants along.
  defp dump_schema(config) do
    args = [
      config[:database],
      "-U",
      config[:username],
      "-p",
      to_string(config[:port]),
      "--host",
      config[:hostname],
      "--no-password",
      "--no-privileges",
      "--schema",
      config[:schema]
    ]

    {dump, 0} =
      System.cmd("pg_dump", args,
        env: [{"PGPASSWORD", config[:password]}, {"PGCONNECT_TIMEOUT", "10"}],
        into: []
      )

    dump
  end

  # Clears what a schema that was `public` carries from the database's template and a
  # schema made by `CREATE SCHEMA` does not: its owner and its comment. Its grants stay,
  # and `dump_schema/1` leaves them out.
  defp move_public_schema_to(config) do
    schema = Keyword.fetch!(config, :schema)
    username = Keyword.fetch!(config, :username)

    execute_each(config, [
      ~s(ALTER SCHEMA public RENAME TO "#{schema}"),
      ~s(ALTER SCHEMA "#{schema}" OWNER TO "#{username}"),
      ~s(COMMENT ON SCHEMA "#{schema}" IS NULL),
      "CREATE SCHEMA public"
    ])
  end

  defp execute_each(config, statements) do
    Enum.each(statements, fn statement -> :ok = Database.execute(config, statement) end)
  end

  defp append_function(config) do
    query_rows(
      config,
      """
      SELECT pg_get_functiondef(p.oid)
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = $1 AND p.proname = $2
      """,
      [config[:schema], @latest_append_function]
    )
    |> List.flatten()
    |> Enum.join()
  end

  defp append_function_names(config) do
    query_rows(
      config,
      """
      SELECT p.proname
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = $1 AND p.proname LIKE 'append\\_events%'
      ORDER BY p.proname
      """,
      [config[:schema]]
    )
    |> List.flatten()
  end

  defp snapshot_tables(config) do
    query_rows(
      config,
      "SELECT tablename FROM pg_tables WHERE schemaname = $1 AND tablename LIKE '%snapshots'" <>
        " ORDER BY tablename",
      [config[:schema]]
    )
    |> List.flatten()
  end

  # Rows of the one-row `snapshots` table, as a pod from before store migration 1.10.0 wrote
  # them, in the restored dump's `bytea` columns.
  defp insert_legacy_rows(config, count) do
    execute_each(config, [
      """
      INSERT INTO "#{config[:schema]}".snapshots
        (source_uuid, source_version, source_type, data, metadata)
      SELECT 'legacy-' || i, 1, 'Elixir.Snapshot', convert_to('{}', 'UTF8'), convert_to('{}', 'UTF8')
      FROM generate_series(1, #{count}) i
      """
    ])
  end

  defp legacy_row_count(config, schema) do
    [[count]] = query_rows(config, ~s|SELECT count(*) FROM "#{schema}".snapshots|, [])

    count
  end

  defp schema_versions(config) do
    query_rows(
      config,
      ~s(SELECT major_version, minor_version, patch_version FROM "#{config[:schema]}".schema_migrations ORDER BY 1, 2, 3),
      []
    )
  end

  defp query_rows(config, sql, params) do
    {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

    %Postgrex.Result{rows: rows} = Postgrex.query!(conn, sql, params)

    :ok = GenServer.stop(conn)

    rows
  end

  # The call an old pod's `append_events.sql.eex` makes.
  defp append_v1(conn, stream_uuid) do
    Postgrex.query!(
      conn,
      "SELECT public.append_events_v1($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
      [
        "public",
        nil,
        stream_uuid,
        nil,
        nil,
        [UUID.string_to_binary!(UUID.uuid4())],
        ["typed"],
        [nil],
        [nil],
        ["{}"],
        ["{}"],
        [DateTime.utc_now()]
      ]
    )
  end

  # The call an old pod's `append_events.sql.eex` makes.
  defp append_v2(conn, stream_uuid, tags) do
    Postgrex.query!(
      conn,
      "SELECT public.append_events_v2(" <>
        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
      [
        "public",
        nil,
        stream_uuid,
        nil,
        nil,
        [UUID.string_to_binary!(UUID.uuid4())],
        ["typed"],
        [nil],
        [nil],
        ["{}"],
        ["{}"],
        [DateTime.utc_now()],
        Enum.map(tags, fn _tag -> 1 end),
        tags
      ]
    )
  end

  # The call an old pod's `append_events.sql.eex` makes.
  defp append_v3(conn, stream_uuid, tags, stream_id \\ nil) do
    Postgrex.query!(
      conn,
      "SELECT public.append_events_v3(" <>
        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)",
      [
        "public",
        stream_id,
        stream_uuid,
        nil,
        nil,
        [UUID.string_to_binary!(UUID.uuid4())],
        ["typed"],
        [nil],
        [nil],
        ["{}"],
        ["{}"],
        [DateTime.utc_now()],
        Enum.map(tags, fn _tag -> 1 end),
        tags,
        nil,
        nil,
        nil,
        nil,
        nil,
        nil
      ]
    )
  end

  # The call an old pod's `append_events.sql.eex` makes.
  defp append_v4(conn, tags, opts \\ []) do
    query = Keyword.get(opts, :query, &Postgrex.query!/3)

    query.(
      conn,
      "SELECT public.append_events_v4(" <>
        "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, " <>
        "$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)",
      [
        "public",
        [opts[:stream_id]],
        [Keyword.get_lazy(opts, :stream_uuid, &UUID.uuid4/0)],
        [opts[:expected]],
        [1],
        nil,
        [1],
        [1],
        [UUID.string_to_binary!(UUID.uuid4())],
        ["typed"],
        [nil],
        [nil],
        ["{}"],
        ["{}"],
        [DateTime.utc_now()],
        Enum.map(tags, fn _tag -> 1 end),
        tags,
        Enum.map(tags, fn _tag -> "typed" end),
        nil,
        nil,
        nil,
        nil,
        nil,
        nil
      ]
    )
  end

  defp typed_event(tags) do
    %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: tags}
  end

  defp append_v5(conn, tags, items \\ nil, position \\ nil) do
    event = typed_event(tags)

    condition =
      if items, do: [condition: [items: items, after: position, target: :rolling]], else: []

    Stream.append_to_stream(
      conn,
      UUID.uuid4(),
      0,
      [event],
      [
        schema: "public",
        serializer: PassThroughSerializer,
        conditional_appends: [targets: [:rolling]]
      ] ++ condition
    )
  end

  defp all_positions(conn) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT stream_version FROM public.stream_events" <>
          " WHERE stream_id = 0 ORDER BY stream_version",
        []
      )

    Enum.map(rows, &hd/1)
  end

  defp all_version(conn) do
    %Postgrex.Result{rows: [[version]]} =
      Postgrex.query!(conn, "SELECT stream_version FROM public.streams WHERE stream_id = 0", [])

    version
  end

  defp tag_rows(conn) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(conn, "SELECT position, tag, event_type FROM public.event_tags", [])

    rows
  end

  defp type_rows(conn) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(conn, "SELECT position, event_type FROM public.event_types", [])

    rows
  end

  defp assert_append_round_trips(config) do
    {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

    opts = [
      schema: config[:schema],
      serializer: PassThroughSerializer,
      correlation_id_type: "text",
      causation_id_type: "text"
    ]

    event = %EventData{
      event_type: "typed",
      correlation_id: "correlation",
      causation_id: "causation",
      data: %{"key" => "value"},
      metadata: %{"user" => "someone"},
      tags: ["tenant:a"]
    }

    stream_uuid = UUID.uuid4()

    assert :ok = Stream.append_to_stream(conn, stream_uuid, 0, [event], opts)
    assert {:ok, [recorded]} = Stream.read_stream_forward(conn, stream_uuid, 0, 10, opts)
    assert recorded.correlation_id == "correlation"
    assert recorded.causation_id == "causation"
    assert recorded.data == %{"key" => "value"}

    :ok = GenServer.stop(conn)
  end

  defp create_new_database(config) do
    StorageInitializer.recreate_database!(config)

    {:ok, conn} = Postgrex.start_link(config)

    Decree.Store.Storage.Initializer.run!(conn, config)

    :ok = GenServer.stop(conn)
  end

  defp create_new_named_database(config) do
    StorageInitializer.drop_database!(config)

    Create.exec(config, quiet: true)
    :ok = Init.exec(config, quiet: true)
  end

  defp migrate_eventstore(config, opts) do
    Migrate.exec(config, opts)
  end

  # The message of whatever the failed migration raises, with its printed output, so a
  # marker printed rather than raised is caught too.
  defp failed_migration_message(config) do
    output =
      capture_io(fn ->
        exception =
          try do
            migrate_eventstore(config, quiet: false, is_mix: false)
            flunk("the migration did not fail")
          rescue
            exception in ExUnit.AssertionError -> reraise exception, __STACKTRACE__
            exception -> exception
          end

        send(self(), {:raised, exception})
      end)

    assert_received {:raised, exception}

    inspect(exception.__struct__) <> ": " <> Exception.message(exception) <> "\n" <> output
  end
end
