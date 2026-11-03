defmodule Decree.Store.Storage.AppendConditionPlanTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{Config, EventData, LockSession, Query, UUID}
  alias Decree.Store.Sql.Statements
  alias Decree.Store.Storage.{CreateStream, Initializer, Reader, Schema}
  alias Decree.Store.Streams.Stream

  @tables ["streams", "events", "stream_events", "event_tags", "event_types"]
  @growth 20_000
  @warming_calls 8

  # What this proves is the plan inside the function, at real parameters, after the case
  # that made the pin necessary: PL/pgSQL caches a generic plan per session, and a plan
  # cached while a store is tiny outlives its growth, because plain inserts invalidate
  # nothing. Reading it takes `auto_explain`, whose NOTICE for a nested statement lands in
  # the result's `messages`. An `EXPLAIN` from outside would plan afresh.
  #
  # Each step keeps that case alive, so none of them may move:
  #
  #   - The store is private, because switching autovacuum off for its tables is DDL.
  #     That is done before the first call: an ANALYZE, or any DDL, would invalidate the
  #     cached plan and hide the finding.
  #   - No session here creates a temporary table: the first one creates a namespace,
  #     which resets every plan cache in the database.
  #   - The tables are analysed while tiny, or the planner assumes a default size and
  #     picks the index without the pin.
  #
  # Without `SET enable_seqscan TO off` on the function, these fail.
  describe "the condition's plan" do
    test "stays index-bound at one probe, after the store grew", context do
      assert_index_bound_after_growth(context, [[tags: ["absent"]]])
    end

    test "stays index-bound at 16 items and more probes than items", context do
      types = for index <- 1..4, do: "absent-type-#{index}"

      items =
        for index <- 1..16 do
          case rem(index, 3) do
            0 -> [types: types]
            1 -> [types: types, tags: ["absent-#{index}", "absent-rest-#{index}"]]
            2 -> [tags: ["absent-#{index}"]]
          end
        end

      {:ok, %Query{probe_items: probes}} = Query.compile(items)
      assert length(probes) > 16

      assert_index_bound_after_growth(context, items)
    end

    # The same case, in a call of two members, one of them created after the warming.
    test "stays index-bound in a multi-stream call, after the store grew", context do
      assert_index_bound_after_growth(context, [[tags: ["absent"]]], ["second"])
    end
  end

  # A read has none of the function's cached plan: `Postgrex.query/4` without
  # `cache_statement` sends an unnamed statement, which is planned on every call at its
  # real parameters. So nothing outlives the growth, which is what this pins, with the
  # same tiny, analysed store and the same growth as above.
  describe "the read's plan" do
    test "is custom, at literal values, and stays index-bound after the store grew", context do
      %{postgrex_config: postgrex_config} = context

      schema = unique_schema("read_plan")
      init_schema(context, schema)

      writer = LockSession.connect(postgrex_config)
      session = LockSession.connect(postgrex_config)

      run = &Postgrex.query!(writer, &1, [], timeout: 60_000)

      run.(~s(SET search_path = "#{schema}", pg_temp))

      for table <- @tables do
        run.("ALTER TABLE #{table} SET (autovacuum_enabled = off)")
      end

      seed_tiny_store(writer, schema)

      for table <- @tables, do: run.("ANALYZE #{table}")

      explain_nested_statements(session)

      {:ok, stream_id} = CreateStream.execute(writer, "target", schema: schema)
      {:ok, %Query{} = query} = Query.compile([[tags: ["seed-1"]]])

      for _call <- 1..@warming_calls do
        read(session, schema, query)
      end

      grow(run, stream_id)

      assert %Postgrex.Result{rows: [_row]} = read(session, schema, query)

      # `auto_explain` reports a top-level statement when its portal closes, so the NOTICE
      # arrives with the next statement's result, where a nested one's arrives in its own.
      %Postgrex.Result{messages: messages} = Postgrex.query!(session, "SELECT 1", [])

      assert [%{message: message}] = Enum.filter(messages, &(&1.message =~ "WITH head AS"))

      # The message opens with the statement's own text, which names `$1` either way. A
      # generic plan would show it in the plan too.
      [_statement, plan] = String.split(message, "Query Parameters:", parts: 2)

      assert plan =~ "unnest('{"
      refute plan =~ "unnest($"

      assert Regex.scan(~r/Seq Scan on \S*(event_tags|event_types|stream_events)\b/, plan) == [],
             plan

      assert plan =~ "ix_stream_events"

      assert %Postgrex.Result{rows: []} =
               Postgrex.query!(session, "SELECT name, statement FROM pg_prepared_statements", [])
    end
  end

  defp read(session, schema, %Query{} = query) do
    params = [
      query.probe_items,
      query.probe_tags,
      query.probe_types,
      query.rest_items,
      query.rest_tags,
      0,
      10,
      nil,
      [],
      [],
      []
    ]

    Postgrex.query!(session, Reader.Query.read_query_statement(schema, :forward), params)
  end

  defp assert_index_bound_after_growth(context, items, others \\ []) do
    %{postgrex_config: postgrex_config} = context

    schema = unique_schema("condition_plan")
    init_schema(context, schema)

    writer = LockSession.connect(postgrex_config)
    session = LockSession.connect(postgrex_config)

    run = &Postgrex.query!(writer, &1, [], timeout: 60_000)

    run.(~s(SET search_path = "#{schema}", pg_temp))

    for table <- @tables do
      run.("ALTER TABLE #{table} SET (autovacuum_enabled = off)")
    end

    seed_tiny_store(writer, schema)

    for table <- @tables, do: run.("ANALYZE #{table}")

    explain_nested_statements(session)

    members =
      for stream_uuid <- ["target" | others] do
        {:ok, stream_id} = CreateStream.execute(writer, stream_uuid, schema: schema)

        {stream_id, stream_uuid}
      end

    {:ok, %Query{} = query} = Query.compile(items)

    for _call <- 1..@warming_calls do
      call(session, schema, members, query)
    end

    grow(run, members |> hd() |> elem(0))

    plan = session |> call(schema, members, query) |> condition_plan()

    # A custom plan would show the arrays as values. It would also be planned against
    # the grown tables, which is not the case under test.
    assert plan =~ "unnest($"

    assert Regex.scan(~r/Seq Scan on \S*(event_tags|event_types|stream_events)\b/, plan) == [],
           plan

    assert plan =~ "ix_stream_events"
    assert plan =~ "event_types_pkey"
    assert plan =~ "event_tags"
  end

  defp seed_tiny_store(conn, schema) do
    opts = [schema: schema, serializer: Decree.Store.JsonSerializer]

    for index <- 1..3 do
      event = %EventData{
        event_type: "seed-#{index}",
        data: %{"key" => "value"},
        metadata: %{},
        tags: ["seed-#{index}"]
      }

      :ok = Stream.append_to_stream(conn, "seed-#{index}", 0, [event], opts)
    end
  end

  defp explain_nested_statements(session) do
    for statement <- [
          "LOAD 'auto_explain'",
          "SET auto_explain.log_min_duration = 0",
          "SET auto_explain.log_nested_statements = on",
          "SET auto_explain.log_level = notice",
          "SET auto_explain.log_verbose = on",
          "SET client_min_messages = notice"
        ] do
      Postgrex.query!(session, statement, [])
    end
  end

  # Plain inserts on another session: no DDL and no ANALYZE, so the session's cached plan
  # stands. The rows carry none of the query's tags or types, so the appends here pass.
  defp grow(run, stream_id) do
    run.("""
    INSERT INTO events (event_id, event_type, data, metadata)
    SELECT md5('grown-' || n)::uuid, 'grown-' || (n % 20), '{}'::text::bytea, '{}'::text::bytea
    FROM generate_series(1, #{@growth}) AS n
    """)

    run.("""
    INSERT INTO stream_events
      (event_id, stream_id, stream_version, original_stream_id, original_stream_version)
    SELECT md5('grown-' || n)::uuid, 0, head.version + n, #{stream_id}, n
    FROM generate_series(1, #{@growth}) AS n
    CROSS JOIN (SELECT stream_version AS version FROM streams WHERE stream_id = 0) AS head
    """)

    run.("""
    INSERT INTO event_types (event_type, position)
    SELECT 'grown-' || (n % 20), head.version + n
    FROM generate_series(1, #{@growth}) AS n
    CROSS JOIN (SELECT stream_version AS version FROM streams WHERE stream_id = 0) AS head
    """)

    run.("""
    INSERT INTO event_tags (position, tag, event_type)
    SELECT head.version + n, 'grown-' || (n % 400), 'grown-' || (n % 20)
    FROM generate_series(1, #{@growth}) AS n
    CROSS JOIN (SELECT stream_version AS version FROM streams WHERE stream_id = 0) AS head
    """)

    run.("UPDATE streams SET stream_version = stream_version + #{@growth} WHERE stream_id = 0")
  end

  # The function's own call, so the result keeps the NOTICEs an `Appender` call would drop.
  defp call(session, schema, members, %Query{} = query) do
    {stream_ids, stream_uuids} = Enum.unzip(members)
    ordinals = Enum.to_list(1..length(members))
    each_event = fn value -> Enum.map(members, fn _member -> value end) end

    params = [
      schema,
      stream_ids,
      stream_uuids,
      each_event.(nil),
      each_event.(1),
      nil,
      ordinals,
      each_event.(1),
      Enum.map(members, fn _member -> UUID.string_to_binary!(UUID.uuid4()) end),
      each_event.("written"),
      each_event.(nil),
      each_event.(nil),
      each_event.("{}"),
      each_event.("{}"),
      each_event.(DateTime.utc_now()),
      [],
      [],
      [],
      query.probe_items,
      query.probe_tags,
      query.probe_types,
      query.rest_items,
      query.rest_tags,
      0
    ]

    Postgrex.query!(session, Statements.append_events(schema), params)
  end

  defp condition_plan(%Postgrex.Result{messages: messages}) do
    assert [%{message: plan}] = Enum.filter(messages, &(&1.message =~ "SELECT EXISTS"))

    plan
  end

  defp unique_schema(prefix) do
    "#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp init_schema(context, schema) do
    %{config: config} = context

    config = Keyword.put(config, :schema, schema)

    :ok = Schema.create(config)
    on_exit(fn -> Schema.drop(config) end)

    conn = config |> Config.default_postgrex_opts() |> LockSession.connect()

    {:ok, _} = Initializer.run!(conn, config)
    :ok = GenServer.stop(conn)
  end
end
