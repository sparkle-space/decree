defmodule Decree.Store.Storage.AppendStreamsTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{
    AppendError,
    EventData,
    EventFactory,
    LockSession,
    ReadResult,
    RecordedEvent,
    UUID
  }

  alias Decree.Store.Storage.Appender
  alias Decree.Store.StoreRows
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  @declared [stream_prefixes: ["decision-"], targets: [:declared_target]]
  @no_conn :a_connection_that_does_not_exist

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore, conditional_appends: @declared, serializer: PassThroughSerializer}
    )

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "expected versions" do
    test "every form, on new and existing members, each from its own base", context do
      for {stream_uuid, count} <- [{"integer", 2}, {"any", 1}, {"exists", 3}] do
        :ok = EventStore.append_to_stream(stream_uuid, 0, events(count))
      end

      {:ok, empty} = StoreRows.insert_stream(context.conn, context.schema, "empty")

      assert :ok =
               EventStore.append_to_streams([
                 {"integer", 2, events(1)},
                 {"new-zero", 0, events(2)},
                 {"any", :any_version, events(2)},
                 {"exists", :stream_exists, events(1)},
                 {"new-none", :no_stream, events(1)},
                 {"new-any", :any_version, events(1)},
                 {"empty", :no_stream, events(1)}
               ])

      assert stream_versions(context, "integer") == [1, 2, 3]
      assert stream_versions(context, "any") == [1, 2, 3]
      assert stream_versions(context, "exists") == [1, 2, 3, 4]
      assert stream_versions(context, "new-zero") == [1, 2]
      assert stream_versions(context, "new-none") == [1]
      assert stream_versions(context, "new-any") == [1]
      assert stream_versions(context, "empty") == [1]

      assert stream_id(context, "empty") == empty

      for stream_uuid <- ["integer", "any", "exists", "new-zero", "new-none", "new-any", "empty"] do
        assert stream_version(context, stream_uuid) ==
                 length(stream_versions(context, stream_uuid))
      end
    end

    test "a stale member answers for the batch, in any position", context do
      :ok = EventStore.append_to_stream("existing", 0, events(2))
      :ok = EventStore.append_to_stream("other", 0, events(1))
      :ok = EventStore.delete_stream("other", 1, :soft)
      all_version = all_version(context)

      for {stale, answer} <- [
            {{"existing", 1, events(1)}, :wrong_expected_version},
            {{"existing", :no_stream, events(1)}, :stream_exists},
            {{"missing", :stream_exists, events(1)}, :stream_not_found},
            {{"missing", 3, events(1)}, :wrong_expected_version},
            {{"other", :any_version, events(1)}, :stream_deleted}
          ],
          appends <- [[{"new", 0, events(1)}, stale], [stale, {"new", 0, events(1)}]] do
        assert EventStore.append_to_streams(appends) == {:error, answer}
      end

      assert stream_id(context, "new") == nil
      assert all_version(context) == all_version
    end
  end

  describe "positions" do
    test "follow the input order, not the sort", context do
      :ok = EventStore.append_to_stream("m", 0, events(1))

      assert :ok =
               EventStore.append_to_streams([
                 {"z", 0, events(2)},
                 {"a", 0, events(1)},
                 {"m", 1, events(2)}
               ])

      assert all_events(context) == [
               {"m", 1},
               {"z", 1},
               {"z", 2},
               {"a", 1},
               {"m", 2},
               {"m", 3}
             ]

      assert all_positions(context) == [1, 2, 3, 4, 5, 6]

      assert {:ok, read} = EventStore.read_stream_forward("m")
      assert Enum.map(read, & &1.stream_version) == [1, 2, 3]
      assert Enum.map(read, & &1.event_number) == [1, 2, 3]
    end
  end

  describe "all or none" do
    setup context do
      for stream_uuid <- ["a", "m", "z"] do
        :ok = EventStore.append_to_stream(stream_uuid, 0, events(1))
      end

      members =
        for stream_uuid <- ["a", "m", "z"] do
          member(stream_id(context, stream_uuid), stream_uuid, 1)
        end

      [members: [member(nil, "n-new", 0) | members]]
    end

    test "a stale member, in any position of the caller's order and of the sort", context do
      %{conn: conn, members: members} = context

      all_version = all_version(context)
      event_count = count(context, "events")

      for stale <- ["a", "m", "z"], order <- [& &1, &Enum.reverse/1, &rotate/1] do
        members = members |> stale_member(stale) |> order.()

        assert Appender.append_streams(conn, members, schema: context.schema) ==
                 {:error, :wrong_expected_version},
               inspect({stale, Enum.map(members, & &1.stream_id)})
      end

      assert_nothing_written(context, all_version, event_count)
      assert stream_id(context, "n-new") == nil
    end

    test "a stale member fires no notification", context do
      %{conn: conn, members: members} = context

      listener = listen(context)

      assert {:error, :wrong_expected_version} =
               Appender.append_streams(conn, stale_member(members, "z"), schema: context.schema)

      assert_next_notification_is_the_sentinel(listener)
    end

    test "a failed condition with two new members and one existing", context do
      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])

      all_version = all_version(context)
      event_count = count(context, "events")
      streams = count(context, "streams")

      listener = listen(context)

      assert EventStore.append_to_streams(
               [
                 {"decision-new-z", 0, [event("typed", [])]},
                 {"a", 1, [event("typed", [])]},
                 {"decision-new-b", 0, [event("typed", [])]}
               ],
               condition: [items: [[tags: ["tag"]]], after: 0, target: :declared_target]
             ) == {:error, :append_condition_failed}

      assert_nothing_written(context, all_version, event_count)
      assert count(context, "streams") == streams
      assert stream_version(context, "a") == 1
      assert_next_notification_is_the_sentinel(listener)
    end

    # In autocommit, the case a `RETURN NULL` would leak: the new member's row is written
    # first, and a successful statement would commit it.
    test "a member's row gone after a new member sorted before it was inserted", context do
      %{conn: conn} = context

      all_version = all_version(context)
      event_count = count(context, "events")
      streams = count(context, "streams")
      listener = listen(context)

      assert Appender.append_streams(
               conn,
               [member(123_456, "z-gone", 1), member(nil, "a-new", 0)],
               schema: context.schema
             ) == {:error, :not_found}

      assert stream_id(context, "a-new") == nil
      assert count(context, "streams") == streams
      assert_nothing_written(context, all_version, event_count)
      assert_next_notification_is_the_sentinel(listener)
    end

    test "a row gone aborts a caller's transaction, as a stale append does", context do
      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        assert Appender.append_streams(
                 tx,
                 [member(123_456, "z-gone", 1), member(nil, "a-new", 0)],
                 schema: context.schema
               ) == {:error, :not_found}

        assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
                 Postgrex.query(tx, "SELECT 1", [])
      end)

      assert {:error, :rollback} = LockSession.commit(session)
      assert stream_id(context, "a-new") == nil
    end

    test "a one-member append to a gone row still answers `:not_found`", context do
      %{conn: conn} = context

      assert Appender.append(conn, 123_456, EventFactory.create_recorded_events(1, "gone"),
               schema: context.schema
             ) == {:error, :not_found}
    end

    test "a one-member append to a gone row aborts a caller's transaction", context do
      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        assert Appender.append(tx, 123_456, EventFactory.create_recorded_events(1, "gone"),
                 schema: context.schema
               ) == {:error, :not_found}

        assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
                 Postgrex.query(tx, "SELECT 1", [])
      end)

      assert {:error, :rollback} = LockSession.commit(session)
    end

    defp rotate([first | rest]), do: rest ++ [first]

    defp stale_member(members, stream_uuid) do
      Enum.map(members, fn
        %{events: [%{stream_uuid: ^stream_uuid}]} = member ->
          %{member | events: EventFactory.create_recorded_events(1, stream_uuid, 1, 7)}

        member ->
          member
      end)
    end
  end

  describe "tags, types and stamps" do
    test "land per stream and per event, and a query finds them", context do
      assert :ok =
               EventStore.append_to_streams([
                 {"z", 0, [event("first", ["t:1", "t:1", "shared"]), event("second", [])]},
                 {"a", 0, [event("third", ["t:3", "shared"])]}
               ])

      assert rows(
               context,
               ~s(SELECT position, tag, event_type FROM "#{context.schema}".event_tags) <>
                 " ORDER BY position, tag",
               []
             ) == [
               [1, "shared", "first"],
               [1, "t:1", "first"],
               [3, "shared", "third"],
               [3, "t:3", "third"]
             ]

      assert rows(
               context,
               ~s(SELECT position, event_type FROM "#{context.schema}".event_types) <>
                 " ORDER BY position",
               []
             ) == [[1, "first"], [2, "second"], [3, "third"]]

      assert {:ok, %ReadResult{events: events, after: 3}} =
               EventStore.read_query([[tags: ["shared"]]])

      assert Enum.map(events, &{&1.stream_uuid, &1.stream_version, &1.event_type}) ==
               [{"z", 1, "first"}, {"a", 1, "third"}]
    end

    test "`command:` stamps share the call's one clock read", context do
      assert :ok =
               EventStore.append_to_streams([
                 {"z", 0, [event("first", ["command:1"]), event("second", ["command:2"])]},
                 {"a", 0, [event("third", ["command:3", "plain"])]}
               ])

      assert [[stamp, 3], [nil, 1]] =
               rows(
                 context,
                 ~s[SELECT stamped_at, count(*) FROM "#{context.schema}".event_tags] <>
                   " GROUP BY stamped_at ORDER BY stamped_at NULLS LAST",
                 []
               )

      assert %DateTime{} = stamp
    end

    test "a later condition sees them", context do
      assert :ok =
               EventStore.append_to_streams([
                 {"z", 0, [event("first", [])]},
                 {"a", 0, [event("second", ["tag"])]}
               ])

      assert EventStore.append_to_stream("decision-x", 0, [event("typed", [])],
               condition: [items: [[tags: ["tag"]]], after: 1]
             ) == {:error, :append_condition_failed}

      assert EventStore.append_to_stream("decision-x", 0, [event("typed", [])],
               condition: [items: [[tags: ["tag"]]], after: 2]
             ) == :ok

      assert all_version(context) == 3
    end

    test "a condition reads a member's earlier events, and not its own", context do
      assert :ok =
               EventStore.append_to_streams(
                 [
                   {"decision-a", 0, [event("typed", ["tag"])]},
                   {"decision-b", 0, [event("typed", ["tag"])]}
                 ],
                 condition: [items: [[tags: ["tag"]]], after: 0]
               )

      assert EventStore.append_to_streams(
               [{"decision-a", 1, [event("typed", [])]}, {"decision-c", 0, [event("typed", [])]}],
               condition: [items: [[tags: ["tag"]]], after: 1]
             ) == {:error, :append_condition_failed}

      assert all_version(context) == 2
    end
  end

  describe "notifications" do
    test "one per member, sorted by name, then one for `$all`", context do
      :ok = EventStore.append_to_stream("m", 0, events(2))

      listener = listen(context)

      assert :ok =
               EventStore.append_to_streams([
                 {"z", 0, events(2)},
                 {"m", 2, events(1)},
                 {"a", 0, events(3)}
               ])

      payloads = for _index <- 1..4, do: next_notification(listener)

      assert Enum.map(payloads, &Enum.drop(String.split(&1, ","), 2)) ==
               [["1", "3"], ["3", "3"], ["1", "2"], ["3", "8"]]

      assert Enum.map(payloads, &hd(String.split(&1, ","))) == ["a", "m", "z", "$all"]

      for payload <- payloads do
        [_stream, _id, from, to] = String.split(payload, ",")

        assert String.to_integer(from) <= String.to_integer(to)
      end

      refute_receive {:notification, ^listener, _ref, _channel, _payload}
    end

    test "a stream's subscriber gets its slice, a `$all` subscriber the batch in order" do
      :ok = EventStore.subscribe("a")
      :ok = EventStore.subscribe("z")
      :ok = EventStore.subscribe("$all")

      assert :ok =
               EventStore.append_to_streams([
                 {"z", 0, [event("z1", []), event("z2", [])]},
                 {"a", 0, [event("a1", [])]}
               ])

      batches =
        for _batch <- 1..3 do
          assert_receive {:events, events}

          Enum.map(events, &{&1.stream_uuid, &1.event_number, &1.event_type})
        end

      assert Enum.sort(batches) ==
               Enum.sort([
                 [{"a", 1, "a1"}],
                 [{"z", 1, "z1"}, {"z", 2, "z2"}],
                 [{"z", 1, "z1"}, {"z", 2, "z2"}, {"a", 3, "a1"}]
               ])

      refute_receive {:events, _events}
    end

    test "a persistent subscription's stream and `$all` deliver the same batch" do
      {:ok, _subscription} = EventStore.subscribe_to_stream("z", "stream-sub", self())
      {:ok, _subscription} = EventStore.subscribe_to_all_streams("all-sub", self())

      assert_receive {:subscribed, _subscription}
      assert_receive {:subscribed, _subscription}

      assert :ok =
               EventStore.append_to_streams([
                 {"z", 0, [event("z1", [])]},
                 {"a", 0, [event("a1", [])]}
               ])

      assert_receive {:events, [%RecordedEvent{stream_uuid: "z", event_number: 1}]}
      assert_receive {:events, [%RecordedEvent{event_number: 1} | _rest]}
    end
  end

  describe "a batch past 1,000 events" do
    @batch_tag "batch"
    @first 1_500
    @second 1_000
    @total @first + @second

    setup context do
      :ok = EventStore.append_to_stream("before", 0, [event("before", [])])

      first =
        for index <- 1..@first, do: event("first-#{index}", [@batch_tag, "first:#{index}"])

      second =
        for index <- 1..@second, do: event("second-#{index}", ["second:#{index}", @batch_tag])

      [first: first, second: second, head: all_version(context)]
    end

    test "is one statement over two streams, and all of it lands", context do
      %{first: first, second: second, head: head} = context

      test_pid = self()

      log = fn %DBConnection.LogEntry{query: query} ->
        send(test_pid, {:statement, IO.iodata_to_binary(query.statement)})
      end

      # A condition on the batch's own tag: one evaluated on a later call, or after this
      # call's own rows, would fail on them.
      condition = [items: [[tags: [@batch_tag]]], after: head, target: :declared_target]

      assert :ok =
               Stream.append_to_streams(
                 context.conn,
                 [{"z-first", 0, first}, {"a-second", 0, second}],
                 schema: context.schema,
                 serializer: PassThroughSerializer,
                 conditional_appends: @declared,
                 condition: condition,
                 log: log
               )

      assert_received {:statement, statement}
      assert statement =~ "append_events_v5"
      refute_received {:statement, _statement}

      assert stream_versions(context, "z-first") == Enum.to_list(1..@first)
      assert stream_versions(context, "a-second") == Enum.to_list(1..@second)
      assert all_positions(context) == Enum.to_list(1..(head + @total))

      tag_rows =
        rows(
          context,
          ~s(SELECT position, tag, event_type FROM "#{context.schema}".event_tags) <>
            " WHERE tag <> $1 ORDER BY position",
          [@batch_tag]
        )

      assert length(tag_rows) == @total

      # Past index 1,000 is where a subscript that lost its place would first show.
      assert Enum.at(tag_rows, 1_001) ==
               [head + 1_002, "first:1002", "first-1002"]

      assert Enum.at(tag_rows, @first) ==
               [head + @first + 1, "second:1", "second-1"]

      assert List.last(tag_rows) ==
               [head + @total, "second:#{@second}", "second-#{@second}"]

      assert_tag_rows_carry_their_events_type(context)
    end

    test "a failed condition leaves nothing, at this size too", context do
      %{first: first, second: second} = context

      assert EventStore.append_to_streams(
               [{"z-first", 0, first}, {"a-second", 0, second}],
               condition: [items: [[types: ["before"]]], after: 0, target: :declared_target]
             ) == {:error, :append_condition_failed}

      assert stream_id(context, "z-first") == nil
      assert stream_id(context, "a-second") == nil
      assert all_version(context) == context.head
      assert count(context, "events") == 1
    end

    # The one case that still splits. A condition on any later batch would see the first
    # batch's rows, and these all carry the tag it reads.
    test "one stream keeps the split, with the condition on its first batch only", context do
      %{first: first, second: second, head: head} = context

      assert :ok =
               EventStore.append_to_stream("decision-big", 0, first ++ second,
                 condition: [items: [[tags: [@batch_tag]]], after: head]
               )

      assert stream_versions(context, "decision-big") == Enum.to_list(1..@total)
      assert all_positions(context) == Enum.to_list(1..(head + @total))
      assert_tag_rows_carry_their_events_type(context)
    end

    test "a one-element list is one call, not the split", context do
      %{first: first, second: second} = context

      test_pid = self()

      log = fn %DBConnection.LogEntry{query: query} ->
        send(test_pid, {:statement, IO.iodata_to_binary(query.statement)})
      end

      assert :ok =
               Stream.append_to_streams(context.conn, [{"big", 0, first ++ second}],
                 schema: context.schema,
                 serializer: PassThroughSerializer,
                 log: log
               )

      assert_received {:statement, _statement}
      refute_received {:statement, _statement}

      assert stream_versions(context, "big") == Enum.to_list(1..@total)
    end

    defp assert_tag_rows_carry_their_events_type(context) do
      %{schema: schema} = context

      assert rows(
               context,
               """
               SELECT count(*)
               FROM "#{schema}".event_tags t
               JOIN "#{schema}".stream_events se ON se.stream_id = 0 AND se.stream_version = t.position
               JOIN "#{schema}".events e ON e.event_id = se.event_id
               WHERE t.event_type IS DISTINCT FROM e.event_type
               """,
               []
             ) == [[0]]
    end
  end

  describe "duplicates" do
    test "the same event id across two members answers `:duplicate_event`", context do
      event_id = UUID.uuid4()

      assert EventStore.append_to_streams([
               {"a", 0, [%EventData{event("typed", []) | event_id: event_id}]},
               {"z", 0, [%EventData{event("typed", []) | event_id: event_id}]}
             ]) == {:error, :duplicate_event}

      assert stream_id(context, "a") == nil
      assert stream_id(context, "z") == nil
      assert count(context, "events") == 0
      assert all_version(context) == 0
    end

    test "an event id already stored answers `:duplicate_event` before a stale member",
         context do
      [event] = events(1)
      event = %EventData{event | event_id: UUID.uuid4()}

      :ok = EventStore.append_to_stream("stored", 0, [event])

      stored = stream_id(context, "stored")
      [%RecordedEvent{} = recorded] = EventFactory.create_recorded_events(1, "stored", 1, 7)
      recorded = %RecordedEvent{recorded | event_id: event.event_id}

      assert Appender.append_streams(
               context.conn,
               [
                 %{stream_id: stored, expected_version: 7, events: [recorded], tags: [[]]},
                 member(nil, "new", 0)
               ],
               schema: context.schema
             ) == {:error, :duplicate_event}

      assert all_version(context) == 1
    end
  end

  # A connection that does not exist: any statement, a BEGIN included, would exit.
  describe "a refusal sends no statement at all" do
    test "for what is not a non-empty list of appends" do
      for appends <- [
            [],
            nil,
            :stream,
            %{},
            [{"stream", 0}],
            [{"stream", 0, []}],
            [{:stream, 0, events(1)}],
            [{"stream", -1, events(1)}],
            [{"stream", :some_version, events(1)}],
            [{"stream", 0, :events}],
            [{"stream", 0, events(1)} | :improper],
            [{"a", 0, events(1)}, {"b", 0, []}]
          ] do
        assert EventStore.append_to_streams(appends, conn: @no_conn) ==
                 {:error, :invalid_append},
               inspect(appends)
      end
    end

    test "for a stream repeated, whatever its expected versions" do
      for appends <- [
            [{"a", 0, events(1)}, {"a", 0, events(1)}],
            [{"a", 0, events(1)}, {"b", 0, events(1)}, {"a", 3, events(1)}],
            [{"a", :any_version, events(1)}, {"a", :no_stream, events(1)}]
          ] do
        assert EventStore.append_to_streams(appends, conn: @no_conn) ==
                 {:error, :repeated_stream}
      end
    end

    test "for `$all`, in any position" do
      for appends <- [
            [{"$all", 0, events(1)}],
            [{"a", 0, events(1)}, {"$all", :any_version, events(1)}]
          ] do
        assert EventStore.append_to_streams(appends, conn: @no_conn) ==
                 {:error, :cannot_append_to_all_stream}
      end
    end

    test "for a tag too long on any member" do
      long = event("typed", [String.duplicate("a", 513)])

      assert EventStore.append_to_streams([{"a", 0, events(1)}, {"b", 0, [long]}],
               conn: @no_conn
             ) == {:error, :tag_too_long}
    end

    test "for a condition that cannot compile, or that is not declared" do
      appends = [{"decision-a", 0, events(1)}, {"decision-b", 0, events(1)}]

      for {condition, answer} <- [
            {[items: [[tags: ["tag"]]], after: nil], :invalid_condition},
            {[items: [], after: 0], :invalid_query},
            {[items: [[tags: [String.duplicate("a", 513)]]], after: 0], :tag_too_long}
          ] do
        assert EventStore.append_to_streams(appends, conn: @no_conn, condition: condition) ==
                 {:error, answer}
      end

      assert EventStore.append_to_streams([{"a", 0, events(1)}],
               conn: @no_conn,
               condition: [items: [[tags: ["tag"]]], after: 0]
             ) == {:error, :undeclared_conditional_target}
    end

    test "and a caller's transaction stays alive", context do
      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        for {appends, answer} <- [
              {[], :invalid_append},
              {[{"a", 0, events(1)}, {"a", 0, events(1)}], :repeated_stream},
              {[{"$all", 0, events(1)}], :cannot_append_to_all_stream}
            ] do
          assert EventStore.append_to_streams(appends, conn: tx) == {:error, answer}
        end

        assert :ok = EventStore.append_to_stream("written", 0, events(1), conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)
      assert stream_versions(context, "written") == [1]
    end
  end

  describe "the declared rule" do
    @condition [items: [[tags: ["tag"]]], after: 0]

    test "one undeclared member refuses the batch", context do
      for appends <- [
            [{"decision-a", 0, events(1)}, {"undeclared", 0, events(1)}],
            [{"undeclared", 0, events(1)}, {"decision-a", 0, events(1)}]
          ] do
        assert EventStore.append_to_streams(appends, condition: @condition) ==
                 {:error, :undeclared_conditional_target}
      end

      assert all_version(context) == 0
    end

    test "every member declared passes" do
      assert :ok =
               EventStore.append_to_streams(
                 [{"decision-a", 0, events(1)}, {"decision-b", 0, events(1)}],
                 condition: @condition
               )
    end

    test "a declared target passes any member" do
      assert :ok =
               EventStore.append_to_streams(
                 [{"undeclared", 0, events(1)}, {"decision-a", 0, events(1)}],
                 condition: Keyword.put(@condition, :target, :declared_target)
               )
    end

    test "an undeclared target and an undeclared member refuse" do
      assert EventStore.append_to_streams(
               [{"undeclared", 0, events(1)}, {"decision-a", 0, events(1)}],
               condition: Keyword.put(@condition, :target, :another_target)
             ) == {:error, :undeclared_conditional_target}
    end
  end

  describe "`created_at_override`" do
    test "dates new streams and every event", context do
      created_at = ~U[2021-03-04 05:06:07.000000Z]

      :ok = EventStore.append_to_stream("existing", 0, events(1))

      assert :ok =
               EventStore.append_to_streams(
                 [{"new", 0, events(1)}, {"existing", 1, events(1)}],
                 created_at_override: created_at
               )

      assert [[^created_at]] =
               rows(
                 context,
                 ~s(SELECT created_at FROM "#{context.schema}".streams) <>
                   " WHERE stream_uuid = 'new'",
                 []
               )

      assert {:ok, [%RecordedEvent{created_at: ^created_at}]} =
               EventStore.read_stream_forward("new")

      assert {:ok, [_first, %RecordedEvent{created_at: ^created_at}]} =
               EventStore.read_stream_forward("existing")
    end
  end

  describe "the error of a failed statement" do
    test "is an `AppendError`, and nothing is written", context do
      untyped = %EventData{event_type: nil, data: "{}", metadata: "{}"}

      assert {:error, %AppendError{class: :rejected, code: :not_null_violation}} =
               EventStore.append_to_streams([{"a", 0, events(1)}, {"z", 0, [untyped]}])

      assert stream_id(context, "a") == nil
      assert all_version(context) == 0
    end
  end

  defp assert_nothing_written(context, all_version, event_count) do
    assert all_version(context) == all_version
    assert count(context, "events") == event_count
    assert all_positions(context) == Enum.to_list(1..all_version//1)
  end

  defp listen(%{postgrex_config: postgrex_config, schema: schema}) do
    {:ok, listener} = Postgrex.Notifications.start_link(postgrex_config)
    {:ok, _reference} = Postgrex.Notifications.listen(listener, "#{schema}.events")

    listener
  end

  defp next_notification(listener) do
    assert_receive {:notification, ^listener, _ref, _channel, payload}

    payload
  end

  # Notifications arrive in commit order, so the sentinel's being the first two proves
  # that nothing came before them, on a member's row or on `$all`.
  defp assert_next_notification_is_the_sentinel(listener) do
    :ok = EventStore.append_to_stream("sentinel", 0, events(1))

    assert String.starts_with?(next_notification(listener), "sentinel,")
    assert String.starts_with?(next_notification(listener), "$all,")
  end

  defp member(stream_id, stream_uuid, version) do
    %{
      stream_id: stream_id,
      expected_version: version,
      events: EventFactory.create_recorded_events(1, stream_uuid, 1, version + 1),
      tags: [[]]
    }
  end

  defp event(event_type, tags) do
    %EventData{event_type: event_type, data: "{}", metadata: "{}", tags: tags}
  end

  defp events(count) do
    for _index <- 1..count, do: event("typed", [])
  end
end
