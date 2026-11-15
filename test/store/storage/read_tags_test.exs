defmodule Decree.Store.Storage.ReadTagsTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{EventData, LockSession, ReadResult, RecordedEvent}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  @deletable [
    stream_prefixes: ["XPost-"],
    event_types: ["captured"],
    tag_kinds: ["content_ref"]
  ]
  @no_conn :a_connection_that_does_not_exist
  @max_bigint 9_223_372_036_854_775_807

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  setup do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore,
       deletable: @deletable, enable_hard_deletes: true, serializer: PassThroughSerializer}
    )

    :ok
  end

  describe "tags at positions" do
    test "are answered per position, sorted" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event(["user:7", "command:c1", "Zed:1", "email:a"]),
          event(["user:8"])
        ])

      assert EventStore.read_tags([2, 1]) ==
               {:ok,
                %{
                  1 => ["Zed:1", "command:c1", "email:a", "user:7"],
                  2 => ["user:8"]
                }}
    end

    test "sort a kind and its longer sibling bytewise" do
      :ok = EventStore.append_to_stream("stream", 0, [event(["a:b", "a", "a:B", "é", "z"])])

      assert EventStore.read_tags([1]) == {:ok, %{1 => ["a", "a:B", "a:b", "z", "é"]}}
    end

    test "leave out a position with no row" do
      :ok = EventStore.append_to_stream("stream", 0, [event(["user:7"]), event([])])

      assert EventStore.read_tags([0, 1, 2, 3, @max_bigint]) == {:ok, %{1 => ["user:7"]}}
    end

    test "answer a repeated position once" do
      :ok = EventStore.append_to_stream("stream", 0, [event(["user:7"])])

      assert EventStore.read_tags([1, 1, 1]) == {:ok, %{1 => ["user:7"]}}
    end

    test "cover every stream of a multi-stream append" do
      :ok =
        EventStore.append_to_streams([
          {"b", 0, [event(["user:1"])]},
          {"a", 0, [event(["user:2"])]}
        ])

      assert EventStore.read_tags([1, 2]) == {:ok, %{1 => ["user:1"], 2 => ["user:2"]}}
    end

    test "are gone once a stream's tags are erased" do
      :ok = EventStore.append_to_stream("stream", 0, [event(["email:a", "user:7"])])

      assert {:ok, 1} = EventStore.delete_stream_tags("stream", ["email"])
      assert EventStore.read_tags([1]) == {:ok, %{1 => ["user:7"]}}
    end
  end

  describe "a deletable kind" do
    test "is left out, and the rest of the event's tags stay" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event(["content_ref:abc", "command:c1", "user:7"])
        ])

      assert EventStore.read_tags([1]) == {:ok, %{1 => ["command:c1", "user:7"]}}
    end

    test "matches by kind as `delete_stream_tags` splits it" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event([
            "content_ref",
            "content_ref:",
            "content_ref:a:b",
            "content_refs:1",
            "not:content_ref:1"
          ])
        ])

      assert EventStore.read_tags([1]) ==
               {:ok, %{1 => ["content_refs:1", "not:content_ref:1"]}}
    end

    test "leaves out a position with nothing else" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event(["content_ref:abc"]),
          event(["user:7"])
        ])

      assert EventStore.read_tags([1, 2]) == {:ok, %{2 => ["user:7"]}}
    end

    test "cannot be asked for: no option turns the filter off" do
      :ok = EventStore.append_to_stream("stream", 0, [event(["content_ref:abc"])])

      assert EventStore.read_tags([1], deletable: []) == {:ok, %{}}
    end

    test "is answered on a store that declares no kind" do
      :ok = EventStore.append_to_stream("stream", 0, [event(["content_ref:abc"])])

      start_supervised!(
        {TestEventStore, name: :undeclared_store, serializer: PassThroughSerializer}
      )

      assert EventStore.read_tags([1], name: :undeclared_store) ==
               {:ok, %{1 => ["content_ref:abc"]}}
    end
  end

  describe "a deletable event type" do
    @tags ["command:c1", "tenant:t1"]

    test "is left out whole, so its position is absent" do
      :ok = EventStore.append_to_stream("stream", 0, [event("captured", @tags)])

      assert EventStore.read_tags([1]) == {:ok, %{}}
    end

    test "is left out of a position a `command:` item reached" do
      :ok = EventStore.append_to_stream("XPost-1", 0, [event("captured", @tags)])

      assert {:ok, %ReadResult{events: [%RecordedEvent{event_number: position}]}} =
               EventStore.read_query([[tags: ["command:c1"]]], command_tag: "command:c1")

      assert EventStore.read_tags([position]) == {:ok, %{}}
    end

    test "leaves another type with the same tags" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event("captured", @tags),
          event("typed", @tags)
        ])

      assert EventStore.read_tags([1, 2]) == {:ok, %{2 => @tags}}
    end

    test "leaves out a deletable kind on another type beside it" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event("captured", @tags),
          event("typed", ["content_ref:abc", "tenant:t1"])
        ])

      assert EventStore.read_tags([1, 2]) == {:ok, %{2 => ["tenant:t1"]}}
    end

    test "is answered on a store that declares no type and no kind" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event("captured", ["content_ref:abc" | @tags])
        ])

      start_supervised!(
        {TestEventStore, name: :undeclared_store, serializer: PassThroughSerializer}
      )

      assert EventStore.read_tags([1], name: :undeclared_store) ==
               {:ok, %{1 => ["command:c1", "content_ref:abc", "tenant:t1"]}}
    end
  end

  # A read's `tags` leave out what this read leaves out, in the read's own statement.
  describe "a read's `tags`" do
    test "leave out a deletable kind, as `read_tags` does" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event(["content_ref:abc", "command:c1", "user:7"]),
          event(["content_ref:def"]),
          event(["user:7"])
        ])

      assert {:ok, %ReadResult{events: [_, _], tags: tags}} =
               EventStore.read_query([[tags: ["user:7"]]], tag_items: [0])

      assert tags == %{1 => ["command:c1", "user:7"], 3 => ["user:7"]}
      assert EventStore.read_tags([1, 3]) == {:ok, tags}
    end

    test "leave out a position whose every tag is of a deletable kind" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event("typed", ["content_ref:abc"]),
          event("typed", ["user:7"])
        ])

      assert {:ok, %ReadResult{events: [_, _], tags: tags}} =
               EventStore.read_query([[types: ["typed"]]], tag_items: [0])

      assert tags == %{2 => ["user:7"]}
      assert EventStore.read_tags([1, 2]) == {:ok, tags}
    end

    test "leave out every tag of an event of a deletable type, at a `command:` match" do
      :ok =
        EventStore.append_to_stream("XPost-1", 0, [event("captured", @tags)])

      :ok = EventStore.append_to_stream("stream", 0, [event("typed", @tags)])

      assert {:ok, %ReadResult{events: [%{event_number: 1}, %{event_number: 2}], tags: tags}} =
               EventStore.read_query([[tags: ["command:c1"]]],
                 command_tag: "command:c1",
                 tag_items: [0]
               )

      assert tags == %{2 => @tags}
      assert EventStore.read_tags([1, 2]) == {:ok, tags}
    end

    test "leave out the tags of a match only `position_items` matched, and keep the match" do
      :ok = EventStore.append_to_stream("XPost-1", 0, [event("captured", ["user:7"])])
      :ok = EventStore.append_to_stream("stream", 0, [event("typed", ["content_ref:abc"])])
      :ok = EventStore.append_to_stream("other", 0, [event("typed", ["user:7"])])

      items = [[tags: ["user:7"]], [types: ["typed"]]]

      assert {:ok, %ReadResult{events: [], tags: tags, matched_items: matched}} =
               EventStore.read_query(items, tag_items: [0, 1], position_items: [0, 1])

      # Positions 1 and 2 have no tag left to answer, and they are matches all the same.
      assert matched == %{1 => [0], 2 => [1], 3 => [0, 1]}
      assert tags == %{3 => ["user:7"]}
      assert EventStore.read_tags([1, 2, 3]) == {:ok, tags}
    end

    test "are answered on a store that declares no type and no kind" do
      :ok =
        EventStore.append_to_stream("stream", 0, [
          event("captured", ["content_ref:abc" | @tags])
        ])

      start_supervised!(
        {TestEventStore, name: :undeclared_store, serializer: PassThroughSerializer}
      )

      assert {:ok, %ReadResult{tags: tags}} =
               EventStore.read_query([[tags: ["command:c1"]]],
                 tag_items: [0],
                 name: :undeclared_store
               )

      assert tags == %{1 => ["command:c1", "content_ref:abc", "tenant:t1"]}
    end
  end

  describe "what a read sends" do
    test "`[]` sends no statement" do
      assert EventStore.read_tags([], conn: @no_conn) == {:ok, %{}}
    end

    test "invalid positions send no statement" do
      for positions <- invalid_positions() do
        assert EventStore.read_tags(positions, conn: @no_conn) == {:error, :invalid_positions},
               inspect(positions)
      end
    end

    test "a read is one statement", %{conn: conn, schema: schema} do
      :ok = EventStore.append_to_stream("stream", 0, [event(["user:7"]), event(["user:8"])])

      test_pid = self()

      log = fn %DBConnection.LogEntry{query: query} ->
        send(test_pid, {:statement, IO.iodata_to_binary(query.statement)})
      end

      assert Stream.read_tags(conn, [1, 2], schema: schema, log: log) ==
               {:ok, %{1 => ["user:7"], 2 => ["user:8"]}}

      assert_received {:statement, statement}
      assert statement =~ "event_tags"
      refute_received {:statement, _statement}
    end

    test "a refusal leaves a caller's transaction alive", context do
      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        for positions <- invalid_positions() do
          assert EventStore.read_tags(positions, conn: tx) == {:error, :invalid_positions}
        end

        assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])
      end)

      assert {:ok, :ok} = LockSession.commit(session)
    end

    test "`conn:` reads a caller's transaction", context do
      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        :ok = EventStore.append_to_stream("stream", 0, [event(["user:7"])], conn: tx)

        assert EventStore.read_tags([1], conn: tx) == {:ok, %{1 => ["user:7"]}}
        assert EventStore.read_tags([1]) == {:ok, %{}}
      end)

      assert {:error, :rolled_back} = LockSession.rollback(session)
    end
  end

  defp invalid_positions do
    [
      nil,
      :positions,
      "1",
      1,
      %{},
      [nil],
      [-1],
      [1, -1],
      [1.0],
      ["1"],
      [:a],
      [[1]],
      [1 | 2],
      [a: 1],
      [@max_bigint + 1]
    ]
  end

  defp event(type \\ "typed", tags) do
    %EventData{event_type: type, data: "{}", metadata: "{}", tags: tags}
  end
end
