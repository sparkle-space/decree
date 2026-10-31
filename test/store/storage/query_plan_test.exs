defmodule Decree.Store.Storage.QueryPlanTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{EventData, LockSession, Query, ReadResult, Storage}
  alias Decree.Store.Sql.Statements
  alias Decree.Store.Storage.Reader
  alias TestEventStore, as: EventStore

  @positions 40_000
  @tags 4_000
  @types 20
  @streams 2_000
  @tags_per_event 3
  @count 1_001

  # A plan is the planner's choice, so this asserts what matters: no table a probe touches
  # is read whole, each probe reaches its table through an index condition on the tag or
  # the type, and the live-event check runs per row. CI's default collation is `en_US.utf8`,
  # so this is also what proves the `COLLATE "C"`: without it the comparison cannot use a
  # "C" index at all.
  #
  # `@count` is the size of a real decision read, and large against this seed, so this
  # proves the fragment stays index-bound where the planner would most like to join whole
  # tables. `streams` may still be scanned: it is joined in the fetch stage, after the
  # match is decided, so it is no probe, and the planner picks that on cost.
  describe "the read" do
    setup :seed

    test "is index-bound for each shape of item, in both directions", context do
      tag_and_type = {"ix_event_tags_tag_event_type_position", "tag"}
      tag_and_type_type = {"ix_event_tags_tag_event_type_position", "event_type"}
      tag_only = {"event_tags_pkey", "tag"}
      type_only = {"event_types_pkey", "event_type"}

      for direction <- [:forward, :backward],
          {items, indexes} <- [
            {[[types: ["type-1"], tags: ["tag-1"]]], [tag_and_type, tag_and_type_type]},
            {[[types: ["type-1", "type-2"], tags: ["tag-1"]]], [tag_and_type, tag_and_type_type]},
            {[[tags: ["tag-1"]]], [tag_only]},
            {[[tags: ["tag-1", "tag-2"]]], [tag_only]},
            {[[types: ["type-1"]]], [type_only]},
            {[
               [types: ["type-1", "type-2"], tags: ["tag-1", "tag-2"]],
               [tags: ["tag-3"]],
               [types: ["type-3"]]
             ], [tag_and_type, tag_and_type_type, tag_only, type_only]},
            {[[tags: ["broad", "tag-1"]]], [tag_only]},
            {[[types: ["type-1"], tags: ["broad", "tag-1"]]], [tag_and_type, tag_and_type_type]}
          ] do
        plan = explain(context, items, direction)

        assert_no_seq_scan(plan, ["event_tags", "event_types", "stream_events"])
        assert_live_event_check(plan)

        for {index, column} <- indexes do
          assert_index_on(plan, index, column)
        end
      end
    end

    test "reads the rows it planned", %{plan_conn: conn, schema: schema} do
      query = compile([[tags: ["tag-1"]]])

      assert {:ok, %ReadResult{events: events, after: @positions}} =
               Storage.read_query(conn, query, :forward, 0, 5, nil, schema: schema)

      assert length(events) == 5
    end
  end

  # What the conditional append does: the same text, in a PL/pgSQL body, with a limit of 1.
  describe "the fragment as a condition" do
    setup context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.connect(postgrex_config)

      Postgrex.query!(session, condition_function(), [])

      [session: session]
    end

    test "fails on a match past `after`, and passes on one at `after`", context do
      %{session: session} = context

      for _ <- 1..3 do
        :ok = EventStore.append_to_stream("other", :any_version, [event("other", ["other"])])
      end

      :ok = EventStore.append_to_stream("stream", 0, [event("typed", ["tag"])])

      query = compile([[types: ["typed"], tags: ["tag"]]])

      assert conflicts?(session, query, 0, context)
      assert conflicts?(session, query, 3, context)
      refute conflicts?(session, query, 4, context)
      refute conflicts?(session, query, 5, context)
    end

    test "answers for each shape of item", context do
      %{session: session} = context

      :ok = EventStore.append_to_stream("stream", 0, [event("typed", ["first", "second"])])

      for items <- [
            [[types: ["typed"], tags: ["first"]]],
            [[tags: ["first"]]],
            [[tags: ["first", "second"]]],
            [[types: ["typed"]]]
          ] do
        query = compile(items)

        assert conflicts?(session, query, 0, context)
        refute conflicts?(session, query, 1, context)
      end

      refute conflicts?(session, compile([[tags: ["first", "absent"]]]), 0, context)
    end

    test "is not tripped by a hard-deleted event's rows", context do
      %{postgrex_config: postgrex_config, schema: schema, session: session} = context

      :ok = EventStore.append_to_stream("gone", 0, [event("typed", ["tag"])])

      assert :ok =
               Decree.Store.Streams.Stream.delete(
                 LockSession.connect(postgrex_config),
                 "gone",
                 :any_version,
                 :hard,
                 schema: schema,
                 enable_hard_deletes: true
               )

      refute conflicts?(session, compile([[tags: ["tag"]]]), 0, context)
      refute conflicts?(session, compile([[types: ["typed"]]]), 0, context)
    end
  end

  describe "the fragment on a generic plan" do
    setup :seed

    test "is index-bound", context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      session = LockSession.connect(postgrex_config)

      Postgrex.query!(session, "SET plan_cache_mode = force_generic_plan", [])
      Postgrex.query!(session, ~s(SET search_path = "#{schema}", pg_temp), [])

      Postgrex.query!(
        session,
        """
        PREPARE condition (bigint[], text[], text[], bigint[], text[], bigint) AS
        SELECT EXISTS (#{fragment("$1", "$2", "$3", "$4", "$5", "$6")})
        """,
        []
      )

      for items <- [
            [[types: ["type-1"], tags: ["tag-1"]]],
            [[tags: ["tag-1", "tag-2"]]],
            [[types: ["type-1"]]],
            [[tags: ["broad", "tag-1"]]],
            [[types: ["type-1"], tags: ["tag-1"]], [tags: ["tag-2"]], [types: ["type-2"]]]
          ] do
        %Query{} = query = compile(items)

        execute =
          "EXPLAIN (FORMAT JSON) EXECUTE condition(" <>
            Enum.map_join(
              [
                array(query.probe_items, "bigint"),
                array(query.probe_tags, "text"),
                array(query.probe_types, "text"),
                array(query.rest_items, "bigint"),
                array(query.rest_tags, "text"),
                "0"
              ],
              ", ",
              & &1
            ) <> ")"

        %Postgrex.Result{rows: [[[%{"Plan" => plan}]]]} = Postgrex.query!(session, execute, [])

        # A custom plan would show the bound as a value.
        assert inspect(plan, limit: :infinity) =~ "$6"

        assert_no_seq_scan(plan)
        assert_live_event_check(plan)
        assert_index_on(plan, "ix_event_tags_tag_event_type_position", "tag")
        assert_index_on(plan, "event_tags_pkey", "tag")
        assert_index_on(plan, "event_types_pkey", "event_type")
      end
    end
  end

  defp seed(%{postgrex_config: postgrex_config, schema: schema}) do
    # A running store would read every seeded `$all` bump back through its notifier.
    stop_supervised!(TestEventStore)

    conn = LockSession.connect(postgrex_config)

    run = &Postgrex.query!(conn, &1, [], timeout: 60_000)

    run.(~s(SET search_path = "#{schema}", pg_temp))

    run.("""
    INSERT INTO streams (stream_uuid, stream_version)
    SELECT 'stream-' || n, #{div(@positions, @streams)}
    FROM generate_series(1, #{@streams}) AS n
    """)

    run.("""
    INSERT INTO events (event_id, event_type, data, metadata)
    SELECT
      md5('event-' || n)::uuid,
      'type-' || (n % #{@types}),
      '{}'::text::bytea,
      '{}'::text::bytea
    FROM generate_series(1, #{@positions}) AS n
    """)

    run.("""
    INSERT INTO stream_events
      (event_id, stream_id, stream_version, original_stream_id, original_stream_version)
    SELECT
      md5('event-' || n)::uuid,
      target.stream_id,
      target.stream_version,
      s.stream_id,
      (n - 1) / #{@streams} + 1
    FROM generate_series(1, #{@positions}) AS n
    JOIN streams s ON s.stream_uuid = 'stream-' || (n % #{@streams} + 1)
    CROSS JOIN LATERAL (
      VALUES (s.stream_id, (n - 1) / #{@streams} + 1), (0::bigint, n::bigint)
    ) AS target (stream_id, stream_version)
    """)

    run.("UPDATE streams SET stream_version = #{@positions} WHERE stream_id = 0")

    run.("""
    INSERT INTO event_types (event_type, position)
    SELECT 'type-' || (n % #{@types}), n
    FROM generate_series(1, #{@positions}) AS n
    """)

    run.("""
    INSERT INTO event_tags (position, tag, event_type)
    SELECT n, 'tag-' || ((n * 7 + k * 131) % #{@tags}), 'type-' || (n % #{@types})
    FROM generate_series(1, #{@positions}) AS n
    CROSS JOIN generate_series(1, #{@tags_per_event}) AS k
    """)

    # A tag on half the events, for a query that puts it first.
    run.("""
    INSERT INTO event_tags (position, tag, event_type)
    SELECT n, 'broad', 'type-' || (n % #{@types})
    FROM generate_series(2, #{@positions}, 2) AS n
    """)

    for table <- ["streams", "events", "stream_events", "event_tags", "event_types"] do
      run.("ANALYZE #{table}")
    end

    start_supervised!(TestEventStore)

    [plan_conn: conn]
  end

  defp explain(context, items, direction) do
    %{plan_conn: conn, schema: schema} = context

    %Query{} = query = compile(items)

    statement =
      schema
      |> Reader.Query.read_query_statement(direction)
      |> IO.iodata_to_binary()

    bound = if direction == :forward, do: 0, else: 9_223_372_036_854_775_807

    params = [
      query.probe_items,
      query.probe_tags,
      query.probe_types,
      query.rest_items,
      query.rest_tags,
      bound,
      @count,
      "tag-1"
    ]

    %Postgrex.Result{rows: [[[%{"Plan" => plan}]]]} =
      Postgrex.query!(conn, "EXPLAIN (FORMAT JSON) " <> statement, params)

    plan
  end

  defp compile(items) do
    {:ok, %Query{} = query} = Query.compile(items)

    query
  end

  defp nodes(%{"Plans" => plans} = node), do: [node | Enum.flat_map(plans, &nodes/1)]
  defp nodes(node), do: [node]

  # `tables` nil means every table.
  defp assert_no_seq_scan(plan, tables \\ nil) do
    scans = for %{"Node Type" => "Seq Scan"} = node <- nodes(plan), do: node["Relation Name"]
    scans = if tables, do: Enum.filter(scans, &(&1 in tables)), else: scans

    assert scans == [], "sequential scans of: #{inspect(scans)}\n#{summary(plan)}"
  end

  # An index scan of `index`, with a condition on `column`, so an index walked end to end
  # for its order does not count.
  defp assert_index_on(plan, index, column) do
    conditions =
      for %{"Index Name" => ^index} = node <- nodes(plan), do: node["Index Cond"] || ""

    assert Enum.any?(conditions, &(&1 =~ "#{column} =")),
           "no condition on #{column} in #{index}: #{inspect(conditions)}\n#{summary(plan)}"
  end

  # The fetch stage probes `ix_stream_events` too, by `hits.position`. This is the fragment's
  # own check, a subplan probing per row, by the position of the row being tested.
  defp assert_live_event_check(plan) do
    checks =
      for %{"Parent Relationship" => "SubPlan"} = subplan <- nodes(plan),
          %{"Index Name" => "ix_stream_events"} = node <- nodes(subplan),
          do: node["Index Cond"] || ""

    assert Enum.any?(checks, &(&1 =~ ~r/stream_version = [te]1(_\d+)?\."position"/)),
           "no per-row live-event check: #{inspect(checks)}\n#{summary(plan)}"
  end

  defp summary(plan) do
    plan
    |> nodes()
    |> Enum.map_join("\n", fn node ->
      inspect(
        Map.take(node, [
          "Node Type",
          "Parent Relationship",
          "Relation Name",
          "Index Name",
          "Index Cond",
          "Filter"
        ])
      )
    end)
  end

  defp array(values, type) do
    literals = Enum.map_join(values, ", ", &literal/1)

    "ARRAY[#{literals}]::#{type}[]"
  end

  defp literal(nil), do: "NULL"
  defp literal(value) when is_integer(value), do: Integer.to_string(value)
  defp literal(value) when is_binary(value), do: "'#{value}'"

  defp fragment(items, tags, types, rest_items, rest_tags, after_position) do
    IO.iodata_to_binary(
      Statements.query_positions(
        "",
        items,
        tags,
        types,
        rest_items,
        rest_tags,
        "#{after_position} + 1",
        "1",
        ">=",
        "ASC"
      )
    )
  end

  # Built like `append_events_v3`: no schema in its text, and the `search_path` set on entry.
  defp condition_function do
    """
    CREATE FUNCTION pg_temp.conflicts(
      p_schema text,
      p_probe_items bigint[],
      p_probe_tags text[],
      p_probe_types text[],
      p_rest_items bigint[],
      p_rest_tags text[],
      p_after bigint
    ) RETURNS boolean LANGUAGE plpgsql SET search_path TO pg_catalog, pg_temp AS $fn$
    DECLARE
      v_conflict boolean;
    BEGIN
      PERFORM set_config('search_path', quote_ident(p_schema) || ', pg_temp', true);

      SELECT EXISTS (
        #{fragment("p_probe_items", "p_probe_tags", "p_probe_types", "p_rest_items", "p_rest_tags", "p_after")}
      ) INTO v_conflict;

      RETURN v_conflict;
    END;
    $fn$
    """
  end

  defp conflicts?(session, %Query{} = query, after_position, %{schema: schema}) do
    %Postgrex.Result{rows: [[conflict]]} =
      Postgrex.query!(
        session,
        """
        SELECT pg_temp.conflicts(
          $1, $2::bigint[], $3::text[], $4::text[], $5::bigint[], $6::text[], $7
        )
        """,
        [
          schema,
          query.probe_items,
          query.probe_tags,
          query.probe_types,
          query.rest_items,
          query.rest_tags,
          after_position
        ]
      )

    conflict
  end

  defp event(event_type, tags) do
    %EventData{event_type: event_type, data: %{"key" => "value"}, metadata: %{}, tags: tags}
  end
end
