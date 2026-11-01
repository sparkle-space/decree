defmodule Decree.Decision.ReadTest do
  use ExUnit.Case, async: true

  alias Decree.Decision.Read
  alias Decree.Decision.Read.Merged
  alias Decree.Store.ReadResult

  @command [tags: ["command:1"]]
  @kind "command_kind:Kind"
  @route "claim:1"
  @read_at ~U[2026-11-01 10:00:00.000000Z]
  @window_ms 60_000

  defp layout(items, read_only \\ []) do
    {:ok, parsed} = Read.parse(items)

    Read.layout(parsed, [@command], read_only)
  end

  defp item(tag), do: [types: ["T"], tags: [tag]]

  defp replay_item(tag), do: [types: ["T"], tags: [tag, @kind], replay: true]

  defp event(position), do: %{event_number: position, data: {:event, position}}

  defp result(positions_and_items, opts \\ []) do
    positions = Enum.map(positions_and_items, &elem(&1, 0))

    %ReadResult{
      events: Enum.map(positions, &event/1),
      after: Keyword.get(opts, :after, 100),
      read_at: Keyword.get(opts, :read_at, ~U[2026-11-01 10:00:00Z]),
      command_stamps: Keyword.get(opts, :command_stamps, %{}),
      matched_items: Map.new(positions_and_items)
    }
  end

  describe "parse/1" do
    test "takes a keyword list or a map, and leaves `:replay` out of the store's item" do
      assert {:ok, [%{item: [types: ["A"]], replay: false}]} = Read.parse([[types: ["A"]]])

      assert {:ok, [%{item: [types: ["A"], tags: ["t", @kind]], replay: true}]} =
               Read.parse([[types: ["A"], tags: ["t", @kind], replay: true]])

      assert {:ok, [%{item: %{types: ["A"], tags: ["t", @kind]}, replay: true}]} =
               Read.parse([%{types: ["A"], tags: ["t", @kind], replay: true}])

      assert {:ok, [%{item: %{types: ["A"]}, replay: false}]} = Read.parse([%{types: ["A"]}])
    end

    test "refuses an item naming no types, and anything but a list of items" do
      for items <- [
            [],
            nil,
            :items,
            [[tags: ["t"]]],
            [%{tags: ["t"]}],
            [[types: []]],
            [[types: "A"]],
            [[]],
            [%{}],
            [:item],
            [~D[2026-11-01]],
            [[types: ["A"], typos: ["B"]]],
            [%{types: ["A"], typos: ["B"]}],
            [[types: ["A"], replay: :yes]],
            [[types: ["A"], replay: true, replay: false]],
            [[types: ["A"]] | :improper]
          ] do
        assert Read.parse(items) == {:error, :invalid_query}, inspect(items)
      end
    end

    test "leaves a repeated key in the item, for the store to refuse" do
      assert {:ok, [%{item: [types: ["A"], types: ["B"]]}]} =
               Read.parse([[types: ["A"], types: ["B"]]])
    end
  end

  describe "parse/1, a replay item" do
    test "needs a `command_kind:` tag and another" do
      for tags <- [["t", @kind], [@kind, "t"], ["a", "b", @kind]] do
        assert {:ok, [_item]} = Read.parse([[types: ["A"], tags: tags, replay: true]])
        assert {:ok, [_item]} = Read.parse([%{types: ["A"], tags: tags, replay: true}])
      end
    end

    test "Q-7, with no `command_kind:` tag, with only that tag, or with no tags, refuses" do
      for tags <- [["t"], ["a", "b"], [@kind], [@kind, "command_kind:Other"], []] do
        assert Read.parse([[types: ["A"], tags: tags, replay: true]]) ==
                 {:error, :invalid_query},
               inspect(tags)

        assert Read.parse([%{types: ["A"], tags: tags, replay: true}]) ==
                 {:error, :invalid_query},
               inspect(tags)
      end

      assert Read.parse([[types: ["A"], replay: true]]) == {:error, :invalid_query}
      assert Read.parse([%{types: ["A"], replay: true}]) == {:error, :invalid_query}
    end

    test "refuses malformed tags as `:invalid_query`, not a raise" do
      for tags <- [:tag, "t", [@kind | :x]] do
        assert Read.parse([[types: ["A"], tags: tags, replay: true]]) == {:error, :invalid_query}
      end
    end

    test "`replay: false` asks nothing of the tags" do
      assert {:ok, [%{replay: false}]} = Read.parse([[types: ["A"], replay: false]])
    end

    test "`replays_tag?/2` reads a replay item's tags and no other's" do
      {:ok, parsed} =
        Read.parse([
          [types: ["A"], tags: ["a"]],
          [types: ["B"], tags: ["b", @kind], replay: true]
        ])

      assert Read.replays_tag?(parsed, "b")
      refute Read.replays_tag?(parsed, "a")
    end
  end

  describe "layout/3" do
    test "puts a decision's items first, then the framework's, then read-only ones" do
      read = layout([item("a"), item("b")], [item("narrow")])

      assert read.items == [item("a"), item("b"), @command, item("narrow")]
      assert read.decision_indexes == [0, 1]
      assert read.framework_indexes == [2]
      assert read.command_index == 2
    end

    test "keeps which of a read's items are replay items" do
      assert layout([item("a"), replay_item("b")]).replay_indexes == [1]
    end
  end

  describe "the condition" do
    test "holds every read's own items and the command item once" do
      reads = [layout([item("a")]), layout([item("b"), item("c")])]

      assert Read.condition_items(reads) == [item("a"), item("b"), item("c"), @command]
      assert Read.condition_count(reads) == 4
    end

    test "never holds a read-only item, and they do not count" do
      reads = [layout([item("a")], [item("narrow")]), layout([item("b")], [item("narrower")])]

      assert Read.condition_items(reads) == [item("a"), item("b"), @command]
      assert Read.condition_count(reads) == 3
    end

    test "counts one probe per item and type, the command item once" do
      reads = [
        layout([[types: ["T", "U", "T"], tags: ["a", "b"]]]),
        layout([[types: ["V", "W"]]], [[types: ["narrow"]]])
      ]

      assert Read.condition_count(reads) == 3
      assert Read.count_probes(reads) == 5
    end

    test "counts nothing for items the store will refuse" do
      assert Read.count_probes([layout([[types: [:not_a_string]]])]) == nil
    end

    test "holds its count on the last read" do
      reads = [layout([item("a")]), %Read{layout([item("b")]) | probes: 3}]

      assert Read.condition_probes(reads) == 3
    end
  end

  describe "merge/3" do
    setup do
      first = layout([item("a"), item("b")])
      second = layout([item("c")])

      [first: first, second: second, offset: 3]
    end

    test "offsets the second read's indexes by the first read's item count", context do
      merged =
        context.first
        |> Read.first(result([{1, [0]}]))
        |> Read.merge(context.second, result([{2, [0]}, {3, [1]}]))

      assert merged.matched_items == %{1 => [0], 2 => [3], 3 => [4]}
      assert merged.decision == MapSet.new([0, 1, 3])
      assert merged.command == MapSet.new([2, 4])
      assert merged.framework == MapSet.new([2, 4])
    end

    test "offsets by read-only items too" do
      first = layout([item("a")], [item("narrow")])

      merged =
        first
        |> Read.first(result([]))
        |> Read.merge(layout([item("c")]), result([{2, [0]}]))

      assert merged.matched_items == %{2 => [3]}
      assert merged.decision == MapSet.new([0, 3])
      assert merged.framework == MapSet.new([1, 4])
    end

    test "merges by position, an event in both once", context do
      merged =
        context.first
        |> Read.first(result([{1, [0]}, {5, [1]}]))
        |> Read.merge(context.second, result([{3, [0]}, {5, [0, 1]}]))

      assert Enum.map(merged.events, & &1.event_number) == [1, 3, 5]
      assert merged.matched_items[5] == [1, 3, 4]
      assert Read.matched_count(merged) == 3
    end

    test "keeps the first read's `after`, `read_at` and stamps", context do
      merged =
        context.first
        |> Read.first(
          result([{1, [2]}],
            after: 10,
            read_at: ~U[2026-11-01 10:00:00Z],
            command_stamps: %{1 => ~U[2026-01-01 00:00:00Z]}
          )
        )
        |> Read.merge(
          context.second,
          result([{1, [1]}],
            after: 20,
            read_at: ~U[2026-11-01 11:00:00Z],
            command_stamps: %{1 => ~U[2026-02-02 00:00:00Z]}
          )
        )

      assert %Merged{after: 10, read_at: ~U[2026-11-01 10:00:00Z]} = merged
      assert merged.command_stamps == %{1 => ~U[2026-01-01 00:00:00Z]}
    end

    test "keeps replay flags per merged index" do
      first = layout([item("a"), replay_item("b")])
      second = layout([replay_item("c")])

      merged =
        first
        |> Read.first(result([{1, [0]}, {2, [1]}]))
        |> Read.merge(second, result([{3, [0]}]))

      assert merged.replay == MapSet.new([1, 3])
      refute Read.replay?(merged, 1)
      assert Read.replay?(merged, 2)
      assert Read.replay?(merged, 3)
    end
  end

  describe "the filter" do
    test "keeps what a decision's own item matched, in either read" do
      merged =
        [item("a")]
        |> layout()
        |> Read.first(result([{1, [0]}, {2, [1]}, {3, [0, 1]}]))
        |> Read.merge(layout([item("b")]), result([{4, [0]}, {5, [1]}]))

      assert merged |> Read.business_events() |> Enum.map(& &1.event_number) == [1, 3, 4]
    end
  end

  describe "command matches" do
    test "are the positions the command item matched, in either read" do
      merged =
        [item("a")]
        |> layout()
        |> Read.first(result([{1, [0]}, {2, [1]}]))
        |> Read.merge(layout([item("b")]), result([{3, [0]}, {2, [1]}]))

      assert Read.command_positions(merged) == [2]
      assert Read.framework_positions(merged) == [2]
    end

    test "a second read's match the first read did not have is new" do
      first = layout([item("a")])
      merged = Read.first(first, result([{1, [1]}]))
      second = layout([item("b")])

      refute Read.new_command_match?(merged, second, result([{1, [1]}]))
      refute Read.new_command_match?(merged, second, result([{2, [0]}]))
      assert Read.new_command_match?(merged, second, result([{1, [1]}, {2, [1]}]))
    end
  end

  describe "within_window?/3" do
    test "is under the window" do
      assert Read.within_window?(@read_at, before(59_999_999), @window_ms)
    end

    test "past the window is outside" do
      refute Read.within_window?(@read_at, before(60_000_001), @window_ms)
    end

    test "exactly at the edge is outside" do
      refute Read.within_window?(@read_at, before(60_000_000), @window_ms)
    end

    test "a negative age is inside" do
      assert Read.within_window?(@read_at, before(-5_000_000), @window_ms)
    end

    test "a nil stamp is outside" do
      refute Read.within_window?(@read_at, nil, @window_ms)
    end
  end

  describe "classify/5" do
    # One command match per entry, position => {stamp, its tags}. `opts` names the positions a
    # replay item selected.
    defp classified(matches, opts \\ []) do
      replayed = Keyword.get(opts, :replayed, [])
      scope = Keyword.get(opts, :scope, [@route])

      read = layout([replay_item("gate")])

      rows =
        for {position, _match} <- matches do
          {position, if(position in replayed, do: [0, 1], else: [1])}
        end

      stamps = Map.new(matches, fn {position, {stamp, _tags}} -> {position, stamp} end)

      tags =
        for {position, {_stamp, tags}} <- matches, tags != nil, into: %{}, do: {position, tags}

      read
      |> Read.first(result(rows, read_at: @read_at, command_stamps: stamps))
      |> Read.classify(tags, @kind, scope, @window_ms)
    end

    defp inside, do: before(1_000_000)
    defp outside, do: before(60_000_000)

    test "no match is `:open`" do
      assert classified([]) == :open
    end

    test "a match outside the window is `:open`, whatever it carries" do
      assert classified([{1, {outside(), ["other"]}}]) == :open
      assert classified([{1, {nil, ["other"]}}]) == :open
    end

    test "a match inside, of this kind and scope, is `:duplicate`" do
      assert classified([{1, {inside(), [@kind, @route]}}]) == :duplicate
    end

    test "a negative age classifies as inside" do
      assert classified([{1, {before(-1_000_000), ["other"]}}]) == :reused
    end

    test "only the matches inside count, so one outside cannot make a match reused" do
      matches = [{1, {outside(), ["other"]}}, {2, {inside(), [@kind, @route]}}]

      assert classified(matches) == :duplicate
    end

    test "kind missing on any event inside is `:reused`" do
      assert classified([{1, {inside(), [@route]}}]) == :reused

      assert classified([{1, {inside(), [@kind, @route]}}, {2, {inside(), [@route]}}]) == :reused
    end

    test "a scope tag missing is `:reused`" do
      assert classified([{1, {inside(), [@kind]}}]) == :reused

      matches = [{1, {inside(), [@kind, @route]}}]

      assert classified(matches, scope: [@route, "order:2"]) == :reused
    end

    test "CC-11, a scope split across the events inside passes" do
      matches = [{1, {inside(), [@kind, @route]}}, {2, {inside(), [@kind, "order:2"]}}]

      assert classified(matches, scope: [@route, "order:2"]) == :duplicate
    end

    test "a position with no fetched tags reads as `:reused`" do
      assert classified([{1, {inside(), nil}}]) == :reused
      assert classified([{1, {inside(), []}}]) == :reused
    end

    test "a replay item selecting an event inside is `:replay`, not `:duplicate`" do
      matches = [{1, {inside(), [@kind, @route]}}, {2, {inside(), [@kind, @route]}}]

      assert classified(matches, replayed: [2]) == :replay
      assert classified(matches) == :duplicate
    end

    test "a replay item selecting only an event outside is not a replay" do
      matches = [{1, {outside(), [@kind, @route]}}, {2, {inside(), [@kind, @route]}}]

      assert classified(matches, replayed: [1]) == :duplicate
    end

    test "kind and scope come before replay" do
      assert classified([{1, {inside(), [@route]}}], replayed: [1]) == :reused
      assert classified([{1, {inside(), [@kind]}}], replayed: [1]) == :reused
    end

    test "a command position with no stamp entry raises" do
      merged =
        [item("a")]
        |> layout()
        |> Read.first(result([{1, [1]}], read_at: @read_at, command_stamps: %{}))

      assert_raise KeyError, fn -> Read.classify(merged, %{}, @kind, [@route], @window_ms) end
    end

    test "a window one microsecond past a stamp at the edge is what decides" do
      assert classified([{1, {before(60_000_000), ["other"]}}]) == :open
      assert classified([{1, {before(59_999_999), ["other"]}}]) == :reused
    end
  end

  defp before(microseconds), do: DateTime.add(@read_at, -microseconds, :microsecond)
end
