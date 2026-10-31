defmodule Decree.Store.Storage.AppendFunctionTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{AppendError, Config, EventData, EventFactory, LockSession, UUID}
  alias Decree.Store.Storage.{Appender, CreateStream, Initializer, Schema}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  setup %{postgrex_config: postgrex_config} do
    [observer: LockSession.connect(postgrex_config)]
  end

  describe "the function's answers" do
    test "an integer expected version that a concurrent writer moved", context do
      assert_concurrent_move_answers(context, 1, {:error, :wrong_expected_version})
    end

    test "`:stream_exists` when a concurrent writer moved the stream", context do
      assert_concurrent_move_answers(context, :stream_exists, {:error, :wrong_expected_version})
    end

    test "`:any_version` never conflicts and numbers from the locked row", context do
      assert_concurrent_move_answers(context, :any_version, :ok)

      assert stream_versions(context, "stream") == [1, 2, 3, 4]
    end

    test "`:not_found` for a stream row that is gone writes nothing", context do
      %{conn: conn, schema: schema} = context

      :ok = EventStore.append_to_stream("stream", 0, EventFactory.create_events(1))

      events = EventFactory.create_recorded_events(2, UUID.uuid4())

      assert {:error, :not_found} = Appender.append(conn, 123_456, events, schema: schema)

      assert count(context, ~s[SELECT count(*) FROM "#{schema}".events]) == 1
      assert all_version(context) == 1
    end

    test "`:duplicate_event` for an event id already stored, at the right version", context do
      %{conn: conn, schema: schema} = context

      {:ok, stream_id} = CreateStream.execute(conn, "stream", schema: schema)
      [event] = EventFactory.create_recorded_events(1, "stream")

      :ok = Appender.append(conn, stream_id, [event], schema: schema)

      assert {:error, :duplicate_event} =
               Appender.append(conn, stream_id, [%{event | stream_version: 2}], schema: schema)
    end

    test "`:duplicate_event` before a stale expected version", context do
      %{conn: conn, schema: schema} = context

      {:ok, stream_id} = CreateStream.execute(conn, "stream", schema: schema)
      [event] = EventFactory.create_recorded_events(1, "stream")

      :ok = Appender.append(conn, stream_id, [event], schema: schema)

      assert {:error, :duplicate_event} =
               Appender.append(conn, stream_id, [event], schema: schema)
    end

    test "`:duplicate_stream_uuid` for a new stream that exists", context do
      %{conn: conn, schema: schema} = context

      {:ok, _stream_id} = CreateStream.execute(conn, "stream", schema: schema)
      events = EventFactory.create_recorded_events(1, "stream")

      assert {:error, :duplicate_stream_uuid} = Appender.append(conn, nil, events, schema: schema)
    end

    test "`created_at_override` dates a new stream and its events", context do
      %{schema: schema} = context

      created_at = ~U[2021-03-04 05:06:07.000000Z]
      events = EventFactory.create_events(2)

      :ok = EventStore.append_to_stream("stream", 0, events, created_at_override: created_at)

      assert [%{created_at: ^created_at}, %{created_at: ^created_at}] =
               EventStore.stream_forward("stream") |> Enum.to_list()

      %Postgrex.Result{rows: [[^created_at]]} =
        Postgrex.query!(
          context.observer,
          ~s(SELECT created_at FROM "#{schema}".streams WHERE stream_uuid = 'stream'),
          []
        )
    end

    test "a new stream without an override is dated by the database", context do
      %{schema: schema} = context

      :ok = EventStore.append_to_stream("stream", 0, EventFactory.create_events(1))

      assert count(
               context,
               ~s[SELECT count(*) FROM "#{schema}".streams] <>
                 " WHERE stream_uuid = 'stream' AND created_at > now() - interval '1 minute'"
             ) == 1
    end

    test "the stream's notification fires before the one for `$all`", context do
      %{schema: schema, postgrex_config: postgrex_config} = context

      {:ok, listener} = Postgrex.Notifications.start_link(postgrex_config)
      {:ok, _reference} = Postgrex.Notifications.listen(listener, "#{schema}.events")

      :ok = EventStore.append_to_stream("stream", 0, EventFactory.create_events(3))

      assert_receive {:notification, ^listener, _ref, _channel, first}
      assert_receive {:notification, ^listener, _ref, _channel, second}

      assert [stream_id, "1", "3"] = first |> String.split(",") |> tl()
      assert String.starts_with?(first, "stream,")
      assert stream_id != "0"
      assert second == "$all,0,1,3"
    end

    test "`$all` stays gapless and in order across concurrent appends", context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      1..8
      |> Enum.map(fn writer ->
        conn = LockSession.connect(postgrex_config)

        Task.async(fn ->
          for number <- 1..10 do
            :ok =
              EventStore.append_to_stream(
                "stream-#{writer}",
                :any_version,
                EventFactory.create_events(number),
                conn: conn
              )
          end
        end)
      end)
      |> Task.await_many(30_000)

      assert all_version(context) == 8 * 55

      %Postgrex.Result{rows: rows} =
        Postgrex.query!(
          context.observer,
          ~s(SELECT stream_version, original_stream_id, original_stream_version) <>
            ~s( FROM "#{schema}".stream_events WHERE stream_id = 0 ORDER BY stream_version),
          []
        )

      assert Enum.map(rows, &hd/1) == Enum.to_list(1..(8 * 55))

      # Each stream's own events keep their order inside `$all`.
      rows
      |> Enum.group_by(&Enum.at(&1, 1), &Enum.at(&1, 2))
      |> Enum.each(fn {_stream_id, versions} ->
        assert versions == Enum.to_list(1..55)
      end)
    end
  end

  describe "a batch of 1,000 or more events" do
    test "creates a stream across the split", context do
      assert :ok = EventStore.append_to_stream("stream", 0, EventFactory.create_events(2_500))

      assert stream_versions(context, "stream") == Enum.to_list(1..2_500)
      assert all_version(context) == 2_500
    end

    test "numbers each batch from the version the one before wrote", context do
      :ok = EventStore.append_to_stream("stream", 0, EventFactory.create_events(10))

      assert :ok = EventStore.append_to_stream("stream", 10, EventFactory.create_events(1_500))
      assert :ok = EventStore.append_to_stream("stream", :any_version, create_batch(1_200))

      assert stream_versions(context, "stream") == Enum.to_list(1..2_710)
    end

    test "a failure in a later batch leaves nothing behind", context do
      %{schema: schema} = context

      :ok = EventStore.append_to_stream("existing", 0, EventFactory.create_events(1))

      events = create_batch(1_199) ++ [untyped_event()] ++ create_batch(100)

      assert {:error, %AppendError{class: :rejected, code: :not_null_violation}} =
               EventStore.append_to_stream("stream", 0, events)

      assert {:error, %AppendError{class: :rejected, code: :not_null_violation}} =
               EventStore.append_to_stream("existing", 1, events)

      assert {:error, :stream_not_found} = EventStore.read_stream_forward("stream")
      assert stream_versions(context, "existing") == [1]
      assert count(context, ~s[SELECT count(*) FROM "#{schema}".events]) == 1
      assert all_version(context) == 1
    end
  end

  describe "the isolation level" do
    test "an append in a REPEATABLE READ transaction is rejected", context do
      assert_rejected_isolation(context, "REPEATABLE READ")
    end

    test "an append in a SERIALIZABLE transaction is rejected", context do
      assert_rejected_isolation(context, "SERIALIZABLE")
    end
  end

  describe "name resolution" do
    test "survives renaming the schema", context do
      %{observer: observer} = context

      schema = unique_schema("before_rename")
      renamed = unique_schema("after_rename")
      init_schema(context, schema)
      on_exit_drop(context, renamed)

      start_supervised!({TestEventStore, name: :before_rename, schema: schema})
      :ok = append_named(:before_rename, 0, 2)
      stop_supervised!(:before_rename)

      Postgrex.query!(observer, ~s(ALTER SCHEMA "#{schema}" RENAME TO "#{renamed}"), [])

      start_supervised!({TestEventStore, name: :after_rename, schema: renamed})
      assert :ok = append_named(:after_rename, 2, 3)

      assert {:ok, events} =
               EventStore.read_stream_forward("stream", 0, 10, name: :after_rename)

      assert Enum.map(events, & &1.stream_version) == [1, 2, 3, 4, 5]
    end

    test "an append finds the configured schema, not the session's search_path", context do
      %{postgrex_config: postgrex_config} = context

      schema = unique_schema("configured")
      decoy = unique_schema("decoy")
      init_schema(context, schema)
      init_schema(context, decoy)

      conn = LockSession.connect(Keyword.put(postgrex_config, :parameters, search_path: decoy))
      {:ok, stream_id} = CreateStream.execute(conn, "stream", schema: schema)

      # Temporary tables come first in a session's search_path, unless it names them.
      for table <- ["streams", "events", "stream_events"] do
        Postgrex.query!(conn, ~s[CREATE TEMP TABLE #{table} (LIKE "#{decoy}".#{table})], [])
      end

      events = EventFactory.create_recorded_events(2, "stream")

      assert :ok = Appender.append(conn, stream_id, events, schema: schema)

      assert search_path(conn) == decoy

      assert count(context, ~s[SELECT count(*) FROM "#{schema}".events]) == 2
      assert count(context, ~s[SELECT count(*) FROM "#{decoy}".events]) == 0
      assert count(context, ~s[SELECT count(*) FROM "#{decoy}".stream_events]) == 0

      assert %Postgrex.Result{rows: [[0]]} =
               Postgrex.query!(conn, "SELECT count(*) FROM pg_temp.events", [])

      assert version_of_all(context, schema) == 2
      assert version_of_all(context, decoy) == 0
    end

    test "leaves the session's search_path alone when the function raises", context do
      %{postgrex_config: postgrex_config} = context

      schema = unique_schema("raises")
      init_schema(context, schema)

      conn = LockSession.connect(Keyword.put(postgrex_config, :parameters, search_path: "public"))
      {:ok, stream_id} = CreateStream.execute(conn, "stream", schema: schema)

      stale = EventFactory.create_recorded_events(1, "stream", 1, 5)

      assert {:error, :wrong_expected_version} =
               Appender.append(conn, stream_id, stale, schema: schema)

      assert search_path(conn) == "public"
    end

    test "leaves the search_path of a caller's transaction alone", context do
      %{postgrex_config: postgrex_config} = context

      schema = unique_schema("in_transaction")
      init_schema(context, schema)

      session = LockSession.start(postgrex_config)

      LockSession.run(session, fn tx ->
        Postgrex.query!(tx, "SET LOCAL search_path TO pg_catalog", [])

        {:ok, stream_id} = CreateStream.execute(tx, "stream", schema: schema)
        events = EventFactory.create_recorded_events(1, "stream")

        assert :ok = Appender.append(tx, stream_id, events, schema: schema)
        assert search_path(tx) == "pg_catalog"

        # The raise rolls back to a savepoint, so the transaction lives to be read.
        stale = EventFactory.create_recorded_events(1, "stream", 1, 5)

        assert {:error, :wrong_expected_version} =
                 Appender.append(tx, stream_id, stale, schema: schema, mode: :savepoint)

        assert search_path(tx) == "pg_catalog"
      end)

      assert {:ok, :ok} = LockSession.commit(session)
    end
  end

  describe "a non-default store" do
    setup context do
      schema = unique_schema("jsonb_store")

      init_schema(context, schema,
        column_data_type: "jsonb",
        correlation_id_type: "text",
        causation_id_type: "text"
      )

      [
        jsonb_opts: [
          schema: schema,
          serializer: PassThroughSerializer,
          correlation_id_type: "text",
          causation_id_type: "text"
        ]
      ]
    end

    test "round-trips an append and its ids", context do
      %{conn: conn, jsonb_opts: opts} = context

      event = %EventData{
        event_type: "typed",
        correlation_id: "a correlation that is no UUID",
        causation_id: "a causation that is no UUID",
        data: %{"key" => "value"},
        metadata: %{"user" => "someone"}
      }

      assert :ok = Stream.append_to_stream(conn, "stream", 0, [event], opts)

      assert {:ok, [recorded]} = Stream.read_stream_forward(conn, "stream", 0, 10, opts)

      assert recorded.correlation_id == event.correlation_id
      assert recorded.causation_id == event.causation_id
      assert recorded.data == event.data
      assert recorded.metadata == event.metadata
    end

    test "keeps a top-level JSON list one value", context do
      %{conn: conn, jsonb_opts: opts} = context

      batches = [
        [[1, 2]],
        [[%{"a" => 1}]],
        [[]],
        [[[1, 2]]],
        [[1], [2]],
        [[1, 2], [3, 4]],
        [[1, 2], [3]],
        [[1, 2], %{"a" => 1}],
        [%{"a" => 1}, [1, 2]]
      ]

      for {values, index} <- Enum.with_index(batches) do
        events =
          Enum.map(values, fn value ->
            %EventData{event_type: "typed", data: value, metadata: value}
          end)

        stream_uuid = "stream-#{index}"

        assert :ok = Stream.append_to_stream(conn, stream_uuid, 0, events, opts)

        assert {:ok, recorded} = Stream.read_stream_forward(conn, stream_uuid, 0, 10, opts)

        assert Enum.map(recorded, & &1.data) == values
        assert Enum.map(recorded, & &1.metadata) == values
      end
    end
  end

  defp assert_concurrent_move_answers(context, expected_version, answer) do
    %{postgrex_config: postgrex_config, observer: observer} = context

    :ok = EventStore.append_to_stream("stream", 0, EventFactory.create_events(1))

    # The writer holds the stream's row, so the append read the version before the
    # move and reaches the function only to wait on the row.
    writer = LockSession.start(postgrex_config)

    LockSession.run(writer, fn tx ->
      assert :ok =
               EventStore.append_to_stream("stream", 1, EventFactory.create_events(2), conn: tx)
    end)

    appender = LockSession.connect(postgrex_config)
    backend = LockSession.backend_pid(appender)
    events = EventFactory.create_events(1)

    append =
      Task.async(fn ->
        EventStore.append_to_stream("stream", expected_version, events, conn: appender)
      end)

    LockSession.wait_until_blocked(observer, backend, writer.backend)

    assert {:ok, :ok} = LockSession.commit(writer)
    assert Task.await(append) == answer
  end

  defp assert_rejected_isolation(context, level) do
    %{postgrex_config: postgrex_config} = context

    conn = LockSession.connect(postgrex_config)
    events = EventFactory.create_events(1)

    assert {:error, {:error, %AppendError{class: :rejected, code: :invalid_transaction_state}}} =
             Postgrex.transaction(conn, fn tx ->
               Postgrex.query!(tx, "SET TRANSACTION ISOLATION LEVEL #{level}", [])

               reply = EventStore.append_to_stream("stream", 0, events, conn: tx)

               Postgrex.rollback(tx, reply)
             end)

    assert {:error, :stream_not_found} = EventStore.read_stream_forward("stream")
  end

  defp append_named(name, expected_version, count) do
    EventStore.append_to_stream("stream", expected_version, EventFactory.create_events(count),
      name: name
    )
  end

  defp create_batch(count) do
    EventFactory.create_events(count)
  end

  defp untyped_event do
    %EventData{event_type: nil, data: %{"untyped" => true}, metadata: %{}}
  end

  defp unique_schema(prefix) do
    "#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp init_schema(context, schema, overrides \\ []) do
    %{config: config} = context

    config = config |> Keyword.merge(overrides) |> Keyword.put(:schema, schema)

    :ok = Schema.create(config)
    on_exit_drop(context, schema)

    conn = config |> Config.default_postgrex_opts() |> LockSession.connect()

    {:ok, _} = Initializer.run!(conn, config)
    :ok = GenServer.stop(conn)

    config
  end

  defp on_exit_drop(%{config: config}, schema) do
    on_exit(fn -> Schema.drop(Keyword.put(config, :schema, schema)) end)
  end

  defp search_path(conn) do
    %Postgrex.Result{rows: [[search_path]]} = Postgrex.query!(conn, "SHOW search_path", [])

    search_path
  end

  defp count(context, sql) do
    %Postgrex.Result{rows: [[count]]} = Postgrex.query!(context.observer, sql, [])

    count
  end

  defp all_version(%{schema: schema} = context), do: version_of_all(context, schema)

  defp version_of_all(context, schema) do
    count(context, ~s(SELECT stream_version FROM "#{schema}".streams WHERE stream_id = 0))
  end

  defp stream_versions(context, stream_uuid) do
    %{schema: schema} = context

    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        context.observer,
        ~s(SELECT stream_events.stream_version FROM "#{schema}".stream_events) <>
          ~s[ JOIN "#{schema}".streams USING (stream_id)] <>
          " WHERE stream_uuid = $1 ORDER BY stream_events.stream_version",
        [stream_uuid]
      )

    Enum.map(rows, &hd/1)
  end
end
