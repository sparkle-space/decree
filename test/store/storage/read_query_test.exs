defmodule Decree.Store.Storage.ReadQueryTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{Config, EventData, LockSession, ReadResult, RecordedEvent, StoreRows, UUID}
  alias Decree.Store.Storage.{Initializer, Schema}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  # `Decree.Store.JsonSerializer` builds a struct from an event's type, and the types
  # here are plain strings.
  defmodule UntypedSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: Jason.encode!(term)
    def deserialize(binary, _config), do: Jason.decode!(binary)
  end

  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)
    start_supervised!({TestEventStore, serializer: UntypedSerializer})

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "what matches" do
    setup do
      append("one", [
        event("placed", ["order:1", "tenant:a"]),
        event("placed", ["order:2", "tenant:a"]),
        event("paid", ["order:1"])
      ])

      append("two", [event("shipped", []), event("paid", ["order:2", "tenant:b"])])

      :ok
    end

    test "an item with tags only, whatever the type" do
      assert positions([[tags: ["order:1"]]]) == [1, 3]
    end

    test "an item with types only, tagged or not" do
      assert positions([[types: ["shipped"]]]) == [4]
      assert positions([[types: ["placed", "shipped"]]]) == [1, 2, 4]
    end

    test "an item with a tag and a type" do
      assert positions([[types: ["paid"], tags: ["order:1"]]]) == [3]
    end

    test "an item with a tag and two types" do
      assert positions([[types: ["placed", "paid"], tags: ["order:2"]]]) == [2, 5]
    end

    test "all of an item's tags, whichever comes first" do
      assert positions([[tags: ["order:2", "tenant:a"]]]) == [2]
      assert positions([[tags: ["tenant:a", "order:2"]]]) == [2]
      assert positions([[tags: ["order:1", "tenant:b"]]]) == []
    end

    test "all of an item's tags, with a type" do
      assert positions([[types: ["paid"], tags: ["tenant:b", "order:2"]]]) == [5]
      assert positions([[types: ["placed"], tags: ["tenant:b", "order:2"]]]) == []
    end

    test "any of the items, in a mixed query" do
      assert {:ok, result} =
               EventStore.read_query([
                 [types: ["shipped"]],
                 %{tags: ["order:1", "tenant:a"]},
                 [types: ["paid"], tags: ["tenant:b"]]
               ])

      assert numbers(result) == [1, 4, 5]
      assert result.matched_items == %{1 => [1], 4 => [0], 5 => [2]}
    end

    test "an event matching two items comes once, and lists both" do
      assert {:ok, result} =
               EventStore.read_query([
                 [tags: ["order:1"]],
                 [types: ["placed"]],
                 [tags: ["tenant:a", "order:1"]]
               ])

      assert numbers(result) == [1, 2, 3]
      assert result.matched_items == %{1 => [0, 1, 2], 2 => [1], 3 => [0]}
    end

    test "an event matching one item through its type lists it once" do
      assert {:ok, result} = EventStore.read_query([[types: ["placed", "paid"]]])

      assert result.matched_items == %{1 => [0], 2 => [0], 3 => [0], 5 => [0]}
    end

    test "nothing, for a tag or type no event has" do
      assert positions([[tags: ["order:9"]]]) == []
      assert positions([[types: ["unknown"]]]) == []
    end

    test "a tag as data, whatever its text" do
      tag = "it's a tag\"; DROP TABLE events; --"

      append("quoted", [event("quoted", [tag])])

      assert positions([[tags: [tag]]]) == [6]
    end

    test "a type as data, whatever its text" do
      type = "it's a type"

      append("quoted", [event(type, [])])

      assert positions([[types: [type]]]) == [6]
    end
  end

  describe "the events" do
    test "are `$all` rows, deserialized", context do
      %{event_store: event_store} = context

      correlation_id = UUID.uuid4()
      causation_id = UUID.uuid4()

      append("noise", [event("noise", [])])

      append("stream", [
        event("noise", []),
        %{
          event("wanted", ["tenant:a"])
          | correlation_id: correlation_id,
            causation_id: causation_id,
            data: %{"key" => "value"},
            metadata: %{"meta" => "data"}
        }
      ])

      assert {:ok, %ReadResult{events: [%RecordedEvent{} = read]}} =
               event_store.read_query([[tags: ["tenant:a"]]])

      assert {:ok, rows} = event_store.read_all_streams_forward()

      assert read == List.last(rows)

      assert %RecordedEvent{
               event_number: 3,
               stream_uuid: "stream",
               stream_version: 2,
               event_type: "wanted",
               correlation_id: ^correlation_id,
               causation_id: ^causation_id,
               data: %{"key" => "value"},
               metadata: %{"meta" => "data"}
             } = read
    end
  end

  describe "direction" do
    setup do
      append("stream", for(_ <- 1..5, do: event("typed", ["tag"])))

      :ok
    end

    test "defaults to forward" do
      assert positions([[tags: ["tag"]]]) == [1, 2, 3, 4, 5]
    end

    test "backward starts at the head" do
      assert positions([[tags: ["tag"]]], direction: :backward) == [5, 4, 3, 2, 1]
    end

    test "backward with `from: -1` starts at the head" do
      assert positions([[tags: ["tag"]]], direction: :backward, from: -1) == [5, 4, 3, 2, 1]
    end

    test "forward with `from: -1` starts at the beginning" do
      assert positions([[tags: ["tag"]]], from: -1) == [1, 2, 3, 4, 5]
    end

    test "invalid raises" do
      assert_raise ArgumentError, ~r/`:direction`/, fn ->
        EventStore.read_query([[tags: ["tag"]]], direction: :sideways)
      end

      assert_raise ArgumentError, ~r/`:direction`/, fn ->
        EventStore.stream_query([[tags: ["tag"]]], direction: "forward")
      end
    end
  end

  describe "`from`" do
    setup do
      append("stream", for(_ <- 1..5, do: event("typed", ["tag"])))

      :ok
    end

    for {name, item} <- [
          {"a tag and type item", [types: ["typed"], tags: ["tag"]]},
          {"a tag item", [tags: ["tag"]]},
          {"a type item", [types: ["typed"]]}
        ] do
      test "is inclusive forward, for #{name}" do
        item = unquote(item)

        assert positions([item], from: 1) == [1, 2, 3, 4, 5]
        assert positions([item], from: 5) == [5]
        assert positions([item], from: 6) == []
      end

      test "is inclusive backward, for #{name}" do
        item = unquote(item)

        assert positions([item], direction: :backward, from: 5) == [5, 4, 3, 2, 1]
        assert positions([item], direction: :backward, from: 1) == [1]
        assert positions([item], direction: :backward, from: 0) == []
      end
    end

    test "past the head reads nothing forward, and from the head backward" do
      assert positions([[tags: ["tag"]]], from: 99) == []
      assert positions([[tags: ["tag"]]], direction: :backward, from: 99) == [5, 4, 3, 2, 1]
    end

    test "that is not an integer raises" do
      assert_raise ArgumentError, ~r/`:from`/, fn ->
        EventStore.read_query([[tags: ["tag"]]], from: "1")
      end

      assert_raise ArgumentError, ~r/`:from`/, fn ->
        EventStore.stream_query([[tags: ["tag"]]], from: nil)
      end
    end
  end

  describe "`count`" do
    setup do
      append("stream", for(_ <- 1..5, do: event("typed", ["tag"])))

      :ok
    end

    test "cuts the read in either direction" do
      assert positions([[tags: ["tag"]]], count: 2) == [1, 2]
      assert positions([[tags: ["tag"]]], count: 2, direction: :backward) == [5, 4]
    end

    test "cuts a read for each probe shape" do
      assert positions([[types: ["typed"], tags: ["tag"]]], count: 2) == [1, 2]
      assert positions([[types: ["typed"]]], count: 2) == [1, 2]
    end

    test "counts an event matching two items once, and keeps both" do
      items = [[tags: ["tag"]], [types: ["typed"]]]

      assert {:ok, result} = EventStore.read_query(items, count: 3)

      assert numbers(result) == [1, 2, 3]

      assert result.matched_items == %{1 => [0, 1], 2 => [0, 1], 3 => [0, 1]}
    end

    test "keeps the top of the union, not of one item" do
      append("other", [event("late", []), event("typed", ["late"]), event("late", [])])

      items = [[tags: ["late"]], [types: ["late"]]]

      assert positions(items, count: 2) == [6, 7]
      assert positions(items, count: 2, direction: :backward) == [8, 7]
    end

    test "defaults to 1,000" do
      append("large", for(_ <- 1..1_001, do: event("large", ["large"])))

      assert length(elem(EventStore.read_query([[tags: ["large"]]]), 1).events) == 1_000
    end

    test "of zero answers the head and no events" do
      assert {:ok, %ReadResult{events: [], after: 5}} =
               EventStore.read_query([[tags: ["tag"]]], count: 0)
    end

    test "that is invalid raises" do
      for invalid <- [-1, "1", nil, 1.5] do
        assert_raise ArgumentError, ~r/`:count`/, fn ->
          EventStore.read_query([[tags: ["tag"]]], count: invalid)
        end
      end
    end
  end

  describe "`after`" do
    test "is zero on an empty store" do
      assert {:ok, result} = EventStore.read_query([[tags: ["tag"]]])

      assert %ReadResult{
               events: [],
               after: 0,
               command_stamps: stamps,
               matched_items: items,
               read_at: %DateTime{}
             } = result

      assert stamps == %{}
      assert items == %{}
    end

    test "is the head on an empty match of a non-empty store" do
      append("stream", [event("typed", ["other"]), event("typed", [])])

      assert {:ok, %ReadResult{events: [], after: 2, read_at: %DateTime{}}} =
               EventStore.read_query([[tags: ["tag"]]])
    end

    test "is the head, not the last match" do
      append("stream", [event("typed", ["tag"]), event("typed", []), event("typed", ["other"])])

      assert {:ok, %ReadResult{events: [%RecordedEvent{event_number: 1}], after: 3}} =
               EventStore.read_query([[tags: ["tag"]]])
    end

    test "is the head of a read cut by `count`, `from` or direction" do
      append("stream", for(_ <- 1..4, do: event("typed", ["tag"])))

      assert {:ok, %ReadResult{after: 4}} = EventStore.read_query([[tags: ["tag"]]], count: 1)
      assert {:ok, %ReadResult{after: 4}} = EventStore.read_query([[tags: ["tag"]]], from: 3)

      assert {:ok, %ReadResult{after: 4}} =
               EventStore.read_query([[tags: ["tag"]]], direction: :backward, count: 1)
    end
  end

  describe "`read_at`" do
    test "is at or past every stamp it can see" do
      append("stream", [event("typed", ["command:a"]), event("typed", ["command:b"])])

      items = [[tags: ["command:a"]], [tags: ["command:b"]]]

      assert {:ok, %ReadResult{read_at: read_at} = result} =
               EventStore.read_query(items, command_tag: "command:b")

      assert %{2 => %DateTime{} = stamp} = result.command_stamps

      assert DateTime.compare(read_at, stamp) != :lt
    end

    test "is the clock when the read runs, not when a caller's transaction began", context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.start(postgrex_config)

      # The transaction's first statement fixes its `now()`.
      LockSession.run(session, fn tx -> Postgrex.query!(tx, "SELECT now()", []) end)

      append("stream", [event("typed", ["command:a"])])

      assert {:ok, %ReadResult{read_at: read_at, command_stamps: %{1 => stamp}}} =
               LockSession.run(session, fn tx ->
                 EventStore.read_query([[tags: ["command:a"]]],
                   command_tag: "command:a",
                   conn: tx
                 )
               end)

      assert DateTime.compare(read_at, stamp) != :lt

      LockSession.commit(session)
    end
  end

  describe "`command_tag`" do
    setup do
      append("stream", [
        event("typed", ["command:a", "tenant:a"]),
        event("typed", ["tenant:a"]),
        event("typed", ["command:b", "tenant:a"])
      ])

      :ok
    end

    test "gives each match carrying it its stamp, and no other an entry" do
      items = [[tags: ["tenant:a"]], [tags: ["command:a"]]]

      assert {:ok, %ReadResult{events: events, command_stamps: stamps}} =
               EventStore.read_query(items, command_tag: "command:a")

      assert length(events) == 3
      assert %{1 => %DateTime{}} = stamps
      assert map_size(stamps) == 1
    end

    test "is honoured when it is not an item's first tag" do
      assert {:ok, %ReadResult{command_stamps: stamps}} =
               EventStore.read_query([[tags: ["tenant:a", "command:b"]]],
                 command_tag: "command:b"
               )

      assert %{3 => %DateTime{}} = stamps
    end

    test "tells a row with no stamp from no row", %{observer: observer, schema: schema} do
      # Only a `command:` row is stamped, and a type-only match has no tag row of its
      # own, so this is what a NULL stamp looks like beside a match without the tag.
      Postgrex.query!(
        observer,
        ~s[INSERT INTO "#{schema}".event_tags (position, tag, event_type) VALUES (2, 'command:raw', 'typed')],
        []
      )

      items = [[tags: ["tenant:a"]], [tags: ["command:raw"]]]

      assert {:ok, %ReadResult{events: events, command_stamps: stamps}} =
               EventStore.read_query(items, command_tag: "command:raw")

      assert length(events) == 3
      assert stamps == %{2 => nil}
    end

    test "an ordinary tag's row has no stamp" do
      assert {:ok, %ReadResult{command_stamps: stamps}} =
               EventStore.read_query([[tags: ["tenant:a"]]], command_tag: "tenant:a")

      assert stamps == %{1 => nil, 2 => nil, 3 => nil}
    end

    test "absent leaves the stamps empty" do
      assert {:ok, %ReadResult{events: [_, _], command_stamps: stamps}} =
               EventStore.read_query([[tags: ["command:a"]], [tags: ["command:b"]]])

      assert stamps == %{}
    end

    test "that no item names is refused" do
      assert {:error, :invalid_query} =
               EventStore.read_query([[tags: ["tenant:a"]]], command_tag: "command:a")

      assert {:error, :invalid_query} =
               EventStore.read_query([[types: ["command:a"]]], command_tag: "command:a")
    end

    test "that is not a string is refused" do
      assert {:error, :invalid_query} =
               EventStore.read_query([[tags: ["tenant:a"]]], command_tag: :"tenant:a")
    end

    test "refused leaves a caller's transaction usable", %{postgrex_config: postgrex_config} do
      session = LockSession.start(postgrex_config)

      LockSession.run(session, fn tx ->
        assert {:error, :invalid_query} =
                 EventStore.read_query([[tags: ["tenant:a"]]], command_tag: "typo", conn: tx)

        assert {:ok, %ReadResult{after: 3}} =
                 EventStore.read_query([[tags: ["tenant:a"]]], conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)
    end
  end

  describe "`tag_items`" do
    setup do
      append("stream", [
        event("typed", ["tenant:a", "command:a", "Zed:1"]),
        event("typed", ["tenant:a", "user:7"]),
        event("typed", ["command:b", "tenant:a", "a:B", "a:b"]),
        event("other", ["tenant:b"])
      ])

      :ok
    end

    test "answers the tags of exactly the matches those items matched, sorted bytewise" do
      items = [[tags: ["tenant:a"]], [tags: ["command:a"]], [tags: ["command:b"]]]

      assert {:ok, %ReadResult{events: events, tags: tags}} =
               EventStore.read_query(items, tag_items: [1, 2])

      assert length(events) == 3

      assert tags == %{
               1 => ["Zed:1", "command:a", "tenant:a"],
               3 => ["a:B", "a:b", "command:b", "tenant:a"]
             }
    end

    test "answers the tags of a match another item also matched, and of no other" do
      items = [[types: ["other"]], [tags: ["tenant:a"]]]

      assert {:ok, %ReadResult{events: [_, _, _, _], tags: tags}} =
               EventStore.read_query(items, tag_items: [0])

      assert tags == %{4 => ["tenant:b"]}
    end

    test "answers what `read_tags` answers for the same positions" do
      items = [[tags: ["tenant:a"]], [types: ["other"]]]

      assert {:ok, %ReadResult{tags: tags}} = EventStore.read_query(items, tag_items: [0, 1])
      assert EventStore.read_tags([1, 2, 3, 4]) == {:ok, tags}
    end

    test "empty, or absent, answers no tags" do
      items = [[tags: ["tenant:a"]]]

      # A `%{}` pattern matches any map, so each is compared.
      assert {:ok, %ReadResult{events: [_, _, _], tags: tags}} =
               EventStore.read_query(items, tag_items: [])

      assert tags == %{}

      assert {:ok, %ReadResult{events: [_, _, _], tags: tags}} = EventStore.read_query(items)
      assert tags == %{}
    end

    test "holds no entry for a match the read did not return" do
      assert {:ok, %ReadResult{events: [%RecordedEvent{event_number: 1}], tags: tags}} =
               EventStore.read_query([[tags: ["tenant:a"]]], tag_items: [0], count: 1)

      assert tags == %{1 => ["Zed:1", "command:a", "tenant:a"]}
    end

    test "that is not a list of the query's item indexes is refused" do
      for tag_items <- [nil, 0, [-1], [1], [0, 1], [0.0], ["0"], [0 | 1], %{}] do
        assert {:error, :invalid_query} =
                 EventStore.read_query([[tags: ["tenant:a"]]], tag_items: tag_items),
               inspect(tag_items)
      end
    end
  end

  # A decision's index item matches every fact a store holds, and needs only their positions
  # and tags (sparkle-space/sparklespace#8313).
  describe "`position_items`" do
    # Facts at 1 to 3, business events at 4 and 5.
    setup do
      append(
        "facts",
        for(state <- ["a", "b", "c"], do: event("fact", ["fact", "state:" <> state]))
      )

      append("business", [event("typed", ["order:1"]), event("typed", ["order:1"])])

      :ok
    end

    @items [[tags: ["order:1"]], [tags: ["fact"]]]

    test "answers a match only they matched by position and tags, with no event" do
      assert {:ok, %ReadResult{after: 5} = result} =
               EventStore.read_query(@items, position_items: [1], tag_items: [1])

      assert numbers(result) == [4, 5]
      assert result.matched_items == %{1 => [1], 2 => [1], 3 => [1], 4 => [0], 5 => [0]}

      assert result.tags == %{
               1 => ["fact", "state:a"],
               2 => ["fact", "state:b"],
               3 => ["fact", "state:c"]
             }
    end

    test "absent, or empty, answers every match with its event, as before" do
      assert {:ok, %ReadResult{} = result} = EventStore.read_query(@items, tag_items: [1])
      assert numbers(result) == [1, 2, 3, 4, 5]

      assert {:ok, %ReadResult{} = empty} =
               EventStore.read_query(@items, tag_items: [1], position_items: [])

      assert %ReadResult{empty | read_at: nil} == %ReadResult{result | read_at: nil}

      assert {:ok, %ReadResult{} = positioned} =
               EventStore.read_query(@items, position_items: [1], tag_items: [1])

      # The same matches, items and tags: only the events differ.
      assert positioned.matched_items == result.matched_items
      assert positioned.tags == result.tags
      assert positioned.after == result.after
      assert positioned.events == Enum.filter(result.events, &(&1.event_number in [4, 5]))
    end

    test "answers a match another item matched too with its event, and counts it" do
      append("both", [event("fact", ["fact", "order:1"])])

      assert {:ok, result} = EventStore.read_query(@items, position_items: [1], count: 3)

      assert numbers(result) == [4, 5, 6]
      assert %RecordedEvent{event_type: "fact"} = List.last(result.events)
      assert result.matched_items[6] == [0, 1]

      assert {:ok, result} = EventStore.read_query(@items, position_items: [1], count: 2)

      assert numbers(result) == [4, 5]
    end

    test "are neither cut nor counted by `count`" do
      append("business", [event("typed", ["order:1"])])

      for count <- [0, 1, 3] do
        assert {:ok, result} = EventStore.read_query(@items, position_items: [1], count: count)

        assert numbers(result) == Enum.take([4, 5, 6], count)

        assert Map.take(result.matched_items, [1, 2, 3]) == %{1 => [1], 2 => [1], 3 => [1]},
               "count: #{count}"
      end
    end

    test "answer every match of a bare item, cut or not, however far past the cut" do
      append("late-facts", for(_ <- 1..3, do: event("fact", ["fact"])))

      assert {:ok, result} = EventStore.read_query(@items, position_items: [1], count: 1)

      assert numbers(result) == [4]
      assert Enum.sort(Map.keys(result.matched_items)) == [1, 2, 3, 4, 6, 7, 8]
    end

    test "a read cut by `count` answers a match past the cut by the items that reached it" do
      # 6 matches item 0 as well, but item 0's probe stops at the cut, so the read cannot see
      # that: it answers 6 as a bare match. The read answered `count` events, so it is partial.
      append("both", [event("fact", ["fact", "order:1"])])

      assert {:ok, result} = EventStore.read_query(@items, position_items: [1], count: 2)

      assert numbers(result) == [4, 5]
      assert result.matched_items[6] == [1]
    end

    test "read backward" do
      assert {:ok, result} =
               EventStore.read_query(@items, position_items: [1], direction: :backward)

      assert numbers(result) == [5, 4]
      assert Enum.sort(Map.keys(result.matched_items)) == [1, 2, 3, 4, 5]

      assert {:ok, result} =
               EventStore.read_query(@items,
                 position_items: [1],
                 direction: :backward,
                 from: 2,
                 count: 0
               )

      assert result.events == []
      assert result.matched_items == %{1 => [1], 2 => [1]}
    end

    test "give a match only they matched its command stamp entry" do
      assert {:ok, result} =
               EventStore.read_query(@items, position_items: [1], command_tag: "fact")

      # An ordinary tag's row has no stamp, so each entry is `nil`: the entry is the point.
      assert result.command_stamps == %{1 => nil, 2 => nil, 3 => nil}
    end

    test "a match of a type item, or of a tag and type item" do
      items = [[tags: ["order:1"]], [types: ["fact"], tags: ["state:a"]], [types: ["fact"]]]

      assert {:ok, result} = EventStore.read_query(items, position_items: [1, 2])

      assert numbers(result) == [4, 5]
      assert result.matched_items == %{1 => [1, 2], 2 => [2], 3 => [2], 4 => [0], 5 => [0]}
    end

    test "leave out a match whose `$all` row has no original stream, as the event's fetch does",
         context do
      %{observer: observer, schema: schema} = context

      Postgrex.transaction(observer, fn tx ->
        # Replica mode fires no trigger, so the update is let through.
        Postgrex.query!(tx, "SET LOCAL session_replication_role = replica", [])

        Postgrex.query!(
          tx,
          ~s[UPDATE "#{schema}".stream_events SET original_stream_id = NULL ] <>
            "WHERE stream_id = 0 AND stream_version IN (2, 4)",
          []
        )
      end)

      assert {:ok, plain} = EventStore.read_query(@items)
      assert {:ok, positioned} = EventStore.read_query(@items, position_items: [1])

      assert numbers(plain) == [1, 3, 5]
      assert numbers(positioned) == [5]
      assert Map.keys(positioned.matched_items) == Map.keys(plain.matched_items)
      assert Enum.sort(Map.keys(positioned.matched_items)) == [1, 3, 5]
    end

    test "that is not a list of the query's item indexes is refused" do
      for position_items <- [nil, 0, [-1], [2], [0, 2], [0.0], ["0"], [0 | 1], %{}] do
        assert {:error, :invalid_query} =
                 EventStore.read_query(@items, position_items: position_items),
               inspect(position_items)
      end
    end
  end

  # A store's life of facts, against a decision's read: `count` is `max_events + 1` at the
  # default bound.
  describe "`position_items` over 2,000 matches" do
    setup do
      for _batch <- 1..20 do
        append("facts", for(_ <- 1..100, do: event("fact", ["fact", "state:a"])))
      end

      append("business", for(_ <- 1..5, do: event("typed", ["order:1"])))

      :ok
    end

    test "answers every one of them, past `count`, and every business event" do
      items = [[tags: ["order:1"]], [tags: ["fact"]]]

      assert {:ok, result} =
               EventStore.read_query(items, position_items: [1], tag_items: [1], count: 1_001)

      assert numbers(result) == Enum.to_list(2_001..2_005)
      assert Map.keys(result.matched_items) |> Enum.sort() == Enum.to_list(1..2_005)
      assert Enum.all?(1..2_000, &(result.matched_items[&1] == [1]))
      assert Map.keys(result.tags) |> Enum.sort() == Enum.to_list(1..2_000)

      # Without the option the facts are events, and `count` cuts them.
      assert {:ok, plain} = EventStore.read_query(items, tag_items: [1], count: 1_001)

      assert numbers(plain) == Enum.to_list(1..1_001)
    end
  end

  describe "a refused query" do
    test "answers its error from `read_query`" do
      assert {:error, :invalid_query} = EventStore.read_query([])
      assert {:error, :invalid_query} = EventStore.read_query([[]])
      assert {:error, :invalid_query} = EventStore.read_query([[tags: [:atom]]])

      assert {:error, :tag_too_long} =
               EventStore.read_query([[tags: [String.duplicate("a", 513)]]])
    end

    test "answers its error from `stream_query`, before any stream exists" do
      assert {:error, :invalid_query} = EventStore.stream_query([])

      assert {:error, :tag_too_long} =
               EventStore.stream_query([[tags: [String.duplicate("a", 513)]]])
    end

    test "leaves a caller's transaction usable", %{postgrex_config: postgrex_config} do
      session = LockSession.start(postgrex_config)

      LockSession.run(session, fn tx ->
        assert {:error, :invalid_query} = EventStore.read_query([[tags: [<<0>>]]], conn: tx)
        assert {:ok, %ReadResult{after: 0}} = EventStore.read_query([[tags: ["a"]]], conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)
    end
  end

  describe "a read failing" do
    test "answers the database's message", %{postgrex_config: postgrex_config} do
      {:ok, conn} = Postgrex.start_link(postgrex_config)

      assert {:error, message} =
               Stream.read_query(conn, [[tags: ["tag"]]],
                 schema: "no_such_schema",
                 serializer: nil,
                 count: 1
               )

      assert message =~ "no_such_schema"
    end

    test "answers `:stream_not_found` for a store with no `$all` row", context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      session = LockSession.start(postgrex_config)

      assert {:error, :stream_not_found} =
               LockSession.run(session, fn tx ->
                 Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])
                 Postgrex.query!(tx, ~s[DELETE FROM "#{schema}".streams WHERE stream_id = 0], [])

                 EventStore.read_query([[tags: ["tag"]]], conn: tx)
               end)

      assert {:error, :rolled_back} = LockSession.rollback(session)
    end
  end

  describe "inside a caller's transaction" do
    test "a read through `conn:` sees its own uncommitted rows", context do
      %{postgrex_config: postgrex_config} = context

      append("committed", [event("typed", ["tag"])])

      session = LockSession.start(postgrex_config)

      assert {:ok, result} =
               LockSession.run(session, fn tx ->
                 :ok =
                   EventStore.append_to_stream("uncommitted", 0, [event("typed", ["tag"])],
                     conn: tx
                   )

                 EventStore.read_query([[tags: ["tag"]]], conn: tx)
               end)

      assert %ReadResult{after: 2} = result
      assert numbers(result) == [1, 2]

      assert {:ok, %ReadResult{after: 1}} = EventStore.read_query([[tags: ["tag"]]])

      assert {:error, :rolled_back} = LockSession.rollback(session)
    end
  end

  describe "a hard-deleted stream" do
    setup context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      deleter = LockSession.connect(postgrex_config)

      delete = fn stream_uuid ->
        :ok =
          Stream.delete(deleter, stream_uuid, :any_version, :hard,
            schema: schema,
            enable_hard_deletes: true
          )
      end

      [
        delete: delete,
        orphan: &StoreRows.orphan_stream(deleter, schema, &1)
      ]
    end

    test "leaves holes the read skips, and `after` does not fall", %{delete: delete} do
      append("keep-1", [event("typed", ["tag"])])
      append("gone", [event("typed", ["tag"]), event("typed", ["tag"])])
      append("keep-2", [event("typed", ["tag"])])

      delete.("gone")

      assert {:ok, %ReadResult{after: 4} = result} = EventStore.read_query([[tags: ["tag"]]])

      assert numbers(result) == [1, 4]
      assert Map.keys(result.matched_items) == [1, 4]

      assert {:ok, %ReadResult{after: 4} = result} =
               EventStore.read_query([[types: ["typed"]]], direction: :backward)

      assert numbers(result) == [4, 1]
    end

    for {name, item} <- [
          {"a tag and type item", [types: ["typed"], tags: ["tag"]]},
          {"a tag item", [tags: ["tag"]]},
          {"a type item", [types: ["typed"]]}
        ] do
      test "rows with no `$all` row use up no `count`, for #{name}", %{orphan: orphan} do
        append("gone", for(_ <- 1..3, do: event("typed", ["tag"])))
        append("keep", for(_ <- 1..2, do: event("typed", ["tag"])))
        append("gone-too", for(_ <- 1..3, do: event("typed", ["tag"])))

        orphan.("gone")
        orphan.("gone-too")

        item = unquote(item)

        assert positions([item], count: 2) == [4, 5]
        assert positions([item], count: 2, direction: :backward) == [5, 4]
      end
    end

    test "rows with no `$all` row use up no `count` when a later tag matches", %{orphan: orphan} do
      append("gone", for(_ <- 1..3, do: event("typed", ["first", "second"])))
      append("keep", [event("typed", ["first", "second"])])

      orphan.("gone")

      assert positions([[tags: ["first", "second"]]], count: 1) == [4]
    end

    test "rows with no `$all` row are no match of `position_items` either", %{orphan: orphan} do
      append("keep-1", [event("typed", ["tag"])])
      append("gone", for(_ <- 1..3, do: event("typed", ["tag"])))
      append("keep-2", [event("typed", ["tag"])])

      orphan.("gone")

      assert {:ok, result} = EventStore.read_query([[tags: ["tag"]]], position_items: [0])

      assert result.events == []
      assert result.matched_items == %{1 => [0], 5 => [0]}
    end

    test "a whole page of orphans does not end `stream_query`", %{orphan: orphan} do
      append("keep-1", [event("typed", ["tag"])])
      append("gone", for(_ <- 1..4, do: event("typed", ["tag"])))
      append("keep-2", [event("typed", ["tag"])])

      orphan.("gone")

      read =
        &(EventStore.stream_query([[tags: ["tag"]]], &1) |> Enum.map(fn e -> e.event_number end))

      assert read.(read_batch_size: 2) == [1, 6]
      assert read.(read_batch_size: 1) == [1, 6]
      assert read.(read_batch_size: 2, direction: :backward) == [6, 1]
    end
  end

  describe "a soft-deleted stream" do
    test "is still returned" do
      append("deleted", [event("typed", ["tag"])])

      :ok = EventStore.delete_stream("deleted", :any_version, :soft)

      assert {:error, :stream_deleted} = EventStore.read_stream_forward("deleted")

      assert {:ok, %ReadResult{events: [%RecordedEvent{stream_uuid: "deleted"}]}} =
               EventStore.read_query([[tags: ["tag"]]])
    end
  end

  describe "`stream_query`" do
    setup do
      append("stream", for(n <- 1..7, do: event("typed", ["tag:#{rem(n, 2)}"])))

      :ok
    end

    test "walks every match forward, across pages" do
      assert numbers_of(EventStore.stream_query([[tags: ["tag:1"]]], read_batch_size: 2)) ==
               [1, 3, 5, 7]
    end

    test "walks every match backward, across pages" do
      opts = [read_batch_size: 2, direction: :backward]

      assert numbers_of(EventStore.stream_query([[tags: ["tag:1"]]], opts)) == [7, 5, 3, 1]
    end

    test "starts at `from`, inclusive" do
      opts = [read_batch_size: 1, from: 3]

      assert numbers_of(EventStore.stream_query([[tags: ["tag:1"]]], opts)) == [3, 5, 7]

      opts = [read_batch_size: 1, from: 5, direction: :backward]

      assert numbers_of(EventStore.stream_query([[tags: ["tag:1"]]], opts)) == [5, 3, 1]
    end

    test "with a page size matching the last page exactly" do
      assert numbers_of(EventStore.stream_query([[tags: ["tag:1"]]], read_batch_size: 4)) ==
               [1, 3, 5, 7]
    end

    test "defaults to a page of 1,000" do
      append("large", for(_ <- 1..1_001, do: event("large", ["large"])))

      assert length(numbers_of(EventStore.stream_query([[tags: ["large"]]]))) == 1_001
    end

    test "answers an empty stream for no match" do
      assert numbers_of(EventStore.stream_query([[tags: ["nothing"]]])) == []
    end

    test "dedupes an event matching two items" do
      items = [[tags: ["tag:1"]], [types: ["typed"]]]

      assert numbers_of(EventStore.stream_query(items, read_batch_size: 3)) ==
               [1, 2, 3, 4, 5, 6, 7]
    end

    test "reads lazily" do
      assert [%RecordedEvent{event_number: 1}] =
               [[tags: ["tag:1"]]]
               |> EventStore.stream_query(read_batch_size: 1)
               |> Enum.take(1)
    end

    test "refuses a page size that could never move" do
      for invalid <- [0, -1, "1"] do
        assert_raise ArgumentError, ~r/`:read_batch_size`/, fn ->
          EventStore.stream_query([[tags: ["tag:1"]]], read_batch_size: invalid)
        end
      end
    end

    test "raises when a page fails to read", %{postgrex_config: postgrex_config} do
      {:ok, conn} = Postgrex.start_link(postgrex_config)

      stream =
        Stream.stream_query(conn, [[tags: ["tag:1"]]],
          schema: "no_such_schema",
          serializer: nil,
          read_batch_size: 1
        )

      assert_raise CaseClauseError, fn -> Enum.to_list(stream) end
    end
  end

  describe "a named schema" do
    test "reads its own store", context do
      schema = "read_query_#{System.unique_integer([:positive])}"
      init_schema(context, schema)

      start_supervised!(
        {TestEventStore, name: :query_store, schema: schema, serializer: UntypedSerializer}
      )

      append("public", [event("typed", ["tag"])])

      assert :ok =
               EventStore.append_to_stream(
                 "named",
                 0,
                 [event("typed", ["tag"]), event("typed", ["tag"])],
                 name: :query_store
               )

      assert {:ok, %ReadResult{after: 2, events: [first, second]}} =
               EventStore.read_query([[tags: ["tag"]]], name: :query_store)

      assert %RecordedEvent{event_number: 1, stream_uuid: "named"} = first
      assert %RecordedEvent{event_number: 2, stream_uuid: "named"} = second

      assert {:ok, %ReadResult{after: 1}} = EventStore.read_query([[tags: ["tag"]]])
    end
  end

  defp append(stream_uuid, events) do
    :ok = EventStore.append_to_stream(stream_uuid, :any_version, events)
  end

  defp event(event_type, tags) do
    %EventData{event_type: event_type, data: %{"key" => "value"}, metadata: %{}, tags: tags}
  end

  defp positions(items, opts \\ []) do
    {:ok, result} = EventStore.read_query(items, opts)

    numbers(result)
  end

  defp numbers(%ReadResult{events: events}), do: Enum.map(events, & &1.event_number)

  defp numbers_of(stream), do: Enum.map(stream, & &1.event_number)

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
