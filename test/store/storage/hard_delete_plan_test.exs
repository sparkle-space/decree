defmodule Decree.Store.Storage.HardDeletePlanTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{Config, EventData, LockSession}
  alias Decree.Store.Sql.Statements
  alias Decree.Store.Storage.{Initializer, Schema}
  alias Decree.Store.Streams.Stream

  @growth 40_000
  @stream_events 50
  @tables ["streams", "events", "stream_events", "event_tags", "event_types"]

  # The tag rows carry no key but the tag, so a stream's rows are found by position: without
  # `ix_event_tags_position` these are a read of the whole table. A statement's plan is its
  # own: both are sent unnamed, so each is planned at its real parameters, as `EXPLAIN` does
  # here. A store of this size is what makes a sequential scan show.
  describe "on a store big enough for a sequential scan to show" do
    setup context do
      schema = "hard_delete_plan_#{System.unique_integer([:positive])}"
      init_schema(context, schema)

      conn = LockSession.connect(context.postgrex_config)
      opts = [schema: schema, serializer: Decree.Store.JsonSerializer]

      events = for index <- 1..@stream_events, do: event(["email:#{index}", "user:#{index}"])

      :ok = Stream.append_to_stream(conn, "subject", 0, events, opts)

      grow(conn, schema)

      %Postgrex.Result{rows: [[stream_id]]} =
        Postgrex.query!(
          conn,
          ~s(SELECT stream_id FROM "#{schema}".streams WHERE stream_uuid = 'subject'),
          []
        )

      [conn: conn, plan_schema: schema, stream_id: stream_id]
    end

    test "a hard delete reaches its tag and type rows through their indexes", context do
      %{conn: conn, plan_schema: schema, stream_id: stream_id} = context

      plan = explain(conn, Statements.hard_delete_stream(schema), [stream_id])

      assert plan =~ "ix_event_tags_position", plan
      assert plan =~ "event_types_pkey", plan
      assert_tag_rows_locked_in_order(plan)
      refute plan =~ ~r/Seq Scan on \S*event_tags\b/, plan
      refute plan =~ ~r/Seq Scan on \S*event_types\b/, plan
    end

    test "`delete_stream_tags` reaches its tag rows through the position", context do
      %{conn: conn, plan_schema: schema, stream_id: stream_id} = context

      plan = explain(conn, Statements.delete_stream_tags(schema), [stream_id, ["email"]])

      assert plan =~ "ix_event_tags_position", plan
      assert_tag_rows_locked_in_order(plan)
      refute plan =~ ~r/Seq Scan on \S*(event_tags|stream_events)\b/, plan
    end
  end

  # `LockRows` over the sort, or the locks follow the scan and the two statements can cycle.
  defp assert_tag_rows_locked_in_order(plan) do
    assert plan =~
             ~r/LockRows[^\n]*\n\s*->  (?:Incremental )?Sort [^\n]*\n\s*Sort Key: \S+"position", \S+\.tag /,
           plan
  end

  defp explain(conn, statement, params) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(conn, ["EXPLAIN ", statement] |> IO.iodata_to_binary(), params)

    Enum.map_join(rows, "\n", &hd/1)
  end

  defp grow(conn, schema) do
    run = &Postgrex.query!(conn, &1, [], timeout: 60_000)
    head = "(SELECT stream_version FROM streams WHERE stream_id = 0)"

    run.(~s(SET search_path = "#{schema}", pg_temp))

    run.("""
    INSERT INTO streams (stream_uuid, stream_version) VALUES ('grown', #{@growth})
    """)

    run.("""
    INSERT INTO events (event_id, event_type, data, metadata)
    SELECT md5('grown-' || n)::uuid, 'grown-' || (n % 20), '{}'::text::bytea, '{}'::text::bytea
    FROM generate_series(1, #{@growth}) AS n
    """)

    run.("""
    INSERT INTO stream_events
      (event_id, stream_id, stream_version, original_stream_id, original_stream_version)
    SELECT md5('grown-' || n)::uuid, sides.stream_id, sides.version + n, grown.stream_id, n
    FROM generate_series(1, #{@growth}) AS n
    CROSS JOIN (SELECT stream_id FROM streams WHERE stream_uuid = 'grown') AS grown
    CROSS JOIN LATERAL (
      VALUES (0::bigint, #{head}), (grown.stream_id, 0::bigint)
    ) AS sides (stream_id, version)
    """)

    run.("""
    INSERT INTO event_types (event_type, position)
    SELECT 'grown-' || (n % 20), #{head} + n
    FROM generate_series(1, #{@growth}) AS n
    """)

    run.("""
    INSERT INTO event_tags (position, tag, event_type)
    SELECT #{head} + n, 'grown-' || (n % 400), 'grown-' || (n % 20)
    FROM generate_series(1, #{@growth}) AS n
    """)

    run.("UPDATE streams SET stream_version = stream_version + #{@growth} WHERE stream_id = 0")

    for table <- @tables, do: run.("ANALYZE #{table}")

    run.("RESET search_path")
  end

  defp event(tags) do
    %EventData{
      event_type: "Elixir.Decree.Store.EventFactory.Event",
      data: %Decree.Store.EventFactory.Event{event: 1},
      metadata: %{},
      tags: tags
    }
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
