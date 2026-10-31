defmodule Decree.Store.MigrateEventStoreTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  alias Decree.Store.Storage.Database
  alias Decree.Store.Streams.Stream
  alias Decree.Store.Tasks.{Create, Init, Migrate, Migrations}
  alias Decree.Store.{Config, EventData, ReadResult, RecordedEvent, UUID}

  @moduletag :migration

  # The latest `append_events_v<n>`. A later version adds a function and leaves this
  # one in place, so a migrated store carries them all and a fresh one only the latest.
  @latest_append_function "append_events_v3"

  # pg_dump picks the quote per body: `$_$` for one that holds a `$`, as `$all` does.
  @dollar_quote_end ~r/^\$\w*\$;$/

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

  describe "migrate event store" do
    test "should prevent migrations from running concurrently", %{config: config} do
      restore_pre_migration_event_store(config)

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
      restore_pre_migration_event_store(config)
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

      restore_pre_migration_event_store(config)
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

      restore_pre_migration_event_store(config)
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
      assert migrated_function =~ "p_after bigint"
      assert migrated_function =~ "SET enable_seqscan TO 'off'"

      assert_append_round_trips(config)

      create_new_named_database(config)

      assert_append_round_trips(config)

      # The round trip wrote rows on both sides.
      assert_same_schema(migrated_schema, dump_schema(config), :schema_only)
      assert migrated_function == append_function(config)
    end
  end

  describe "version bookkeeping" do
    test "the store knows migration 1.6.0" do
      assert "1.6.0" in Migrations.available_migrations()
      assert List.last(Migrations.available_migrations()) == "1.6.0"
    end

    test "a new event store records 1.6.0 and has nothing pending", %{config: config} do
      create_new_database(config)

      assert schema_versions(config) == [[1, 6, 0]]

      assert capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end) =~
               "already migrated"

      assert schema_versions(config) == [[1, 6, 0]]
    end

    test "the migration records 1.6.0", %{config: config} do
      restore_pre_migration_event_store(config)
      migrate_eventstore(config, quiet: true, is_mix: false)

      assert List.last(schema_versions(config)) == [1, 6, 0]

      assert capture_io(fn -> migrate_eventstore(config, quiet: false, is_mix: false) end) =~
               "already migrated"
    end

    test "Init creates only the latest append function", %{config: config} do
      create_new_database(config)

      assert append_function_names(config) == [@latest_append_function]
    end
  end

  # The rolling-deploy case: the migration has run and an old pod still appends.
  describe "append_events_v1 on a migrated store" do
    setup %{config: config} do
      restore_pre_migration_event_store(config)
      migrate_eventstore(config, quiet: true, is_mix: false)

      {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

      [conn: conn]
    end

    test "still works, and its events simply have no rows", %{config: config, conn: conn} do
      assert append_function_names(config) ==
               ["append_events_v1", "append_events_v2", @latest_append_function]

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

  # The other side of a rolling deploy: the store is at 1.6.0, new pods call v3, and old
  # pods still call v1 or v2, on the same positions.
  describe "append_events_v3 beside the older versions on a migrated store" do
    setup %{config: config} do
      restore_pre_migration_event_store(config)
      migrate_eventstore(config, quiet: true, is_mix: false)

      {:ok, conn} = config |> Config.default_postgrex_opts() |> Postgrex.start_link()

      [conn: conn]
    end

    test "every version is there", %{config: config} do
      assert append_function_names(config) ==
               ["append_events_v1", "append_events_v2", "append_events_v3"]
    end

    test "v2 and v3 calls mix on one run of positions", %{conn: conn} do
      head = all_version(conn)

      assert %Postgrex.Result{rows: [[_stream_id]]} = append_v2(conn, UUID.uuid4(), ["tenant:a"])
      assert :ok = append_v3(conn, ["tenant:b"])
      assert %Postgrex.Result{rows: [[_stream_id]]} = append_v2(conn, UUID.uuid4(), ["tenant:c"])
      assert :ok = append_v3(conn, ["tenant:d"], [[tags: ["tenant:e"]]], head)

      assert all_version(conn) == head + 4

      assert tag_rows(conn) == [
               [head + 1, "tenant:a", "typed"],
               [head + 2, "tenant:b", "typed"],
               [head + 3, "tenant:c", "typed"],
               [head + 4, "tenant:d", "typed"]
             ]

      assert length(type_rows(conn)) == 4
      assert all_positions(conn) == Enum.to_list(1..(head + 4))
    end

    test "a v2 writer's tagged event trips a v3 condition", %{conn: conn} do
      head = all_version(conn)

      append_v2(conn, UUID.uuid4(), ["tenant:a"])

      assert append_v3(conn, [], [[tags: ["tenant:a"]]], head) ==
               {:error, :append_condition_failed}

      assert all_version(conn) == head + 1
    end

    # The coverage gap `Decree.Store` documents, on a write path: an event from v1 has
    # no rows, so a condition passes that a read of the stream would call a conflict.
    test "a v1 writer's event does not trip a v3 condition", %{conn: conn} do
      head = all_version(conn)

      append_v1(conn, UUID.uuid4())

      assert append_v3(conn, [], [[types: ["typed"]]], head) == :ok
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

  defp append_v3(conn, tags, items \\ nil, position \\ nil) do
    event = %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: tags}

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
    :ok = Decree.Store.Storage.Database.drop(config)
    :ok = Decree.Store.Storage.Database.create(config)

    {:ok, conn} = Postgrex.start_link(config)

    Decree.Store.Storage.Initializer.run!(conn, config)

    :ok = GenServer.stop(conn)
  end

  defp create_new_named_database(config) do
    :ok = Database.drop(config)

    Create.exec(config, quiet: true)
    :ok = Init.exec(config, quiet: true)
  end

  defp restore_pre_migration_event_store(config) do
    Decree.Store.Storage.Database.drop(config)
    :ok = Decree.Store.Storage.Database.create(config)

    {_, 0} = Decree.Store.Storage.Database.restore(config, "test/store/fixture/eventstore.dump")
  end

  defp migrate_eventstore(config, opts) do
    Migrate.exec(config, opts)
  end
end
