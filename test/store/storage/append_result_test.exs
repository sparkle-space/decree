defmodule Decree.Store.Storage.AppendResultTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{AppendResult, EventData, LockSession, RecordedEvent}
  alias TestEventStore, as: EventStore

  @declared [stream_prefixes: ["decision-"], targets: [:declared_target]]
  @no_conn :a_connection_that_does_not_exist
  @returning [returning: :append_result]

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

    # So a position can't pass for a stream version.
    :ok = EventStore.append_to_stream("noise", 0, events(7))

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "on one stream" do
    test "a new stream" do
      assert EventStore.append_to_stream("new", 0, events(3), @returning) ==
               {:ok, %AppendResult{position: 10, stream_versions: %{"new" => 3}}}
    end

    test "an existing stream, at its version" do
      :ok = EventStore.append_to_stream("old", 0, events(2))

      assert EventStore.append_to_stream("old", 2, events(3), @returning) ==
               {:ok, %AppendResult{position: 12, stream_versions: %{"old" => 5}}}
    end

    test "`:any_version`, on a new and on an existing stream" do
      assert EventStore.append_to_stream("any", :any_version, events(2), @returning) ==
               {:ok, %AppendResult{position: 9, stream_versions: %{"any" => 2}}}

      assert EventStore.append_to_stream("any", :any_version, events(1), @returning) ==
               {:ok, %AppendResult{position: 10, stream_versions: %{"any" => 3}}}
    end

    test "`:no_stream` and `:stream_exists`" do
      assert EventStore.append_to_stream("named", :no_stream, events(1), @returning) ==
               {:ok, %AppendResult{position: 8, stream_versions: %{"named" => 1}}}

      assert EventStore.append_to_stream("named", :stream_exists, events(1), @returning) ==
               {:ok, %AppendResult{position: 9, stream_versions: %{"named" => 2}}}
    end

    test "a conditional append" do
      {:ok, %{after: head}} = EventStore.read_query([[tags: ["contested"]]])

      assert EventStore.append_to_stream(
               "decision-1",
               0,
               [event(["contested"])],
               @returning ++ [condition: [items: [[tags: ["contested"]]], after: head]]
             ) == {:ok, %AppendResult{position: 8, stream_versions: %{"decision-1" => 1}}}

      assert EventStore.append_to_stream(
               "decision-1",
               1,
               events(2),
               @returning ++ [condition: [items: [[tags: ["other"]]], after: head]]
             ) == {:ok, %AppendResult{position: 10, stream_versions: %{"decision-1" => 3}}}
    end

    test "the position is the last event's in `$all`, read back", context do
      {:ok, %AppendResult{position: position}} =
        EventStore.append_to_stream("read-back", 0, events(4), @returning)

      {:ok, events} = EventStore.read_stream_forward("read-back")

      assert %RecordedEvent{event_number: 4, stream_version: 4} = List.last(events)

      {:ok, all} = EventStore.read_all_streams_forward(position, 1)

      assert [%RecordedEvent{event_number: ^position, stream_uuid: "read-back"}] = all
      assert context |> all_positions() |> List.last() == position
    end

    test "without the option an append answers `:ok`" do
      assert :ok = EventStore.append_to_stream("plain", 0, events(1))
      assert :ok = EventStore.append_to_stream("plain", 1, events(1_001))
      assert :ok = EventStore.append_to_streams([{"plain-a", 0, events(1)}])
    end

    test "a failure answers as it does without the option" do
      :ok = EventStore.append_to_stream("old", 0, events(1))

      assert EventStore.append_to_stream("old", 0, events(1), @returning) ==
               {:error, :wrong_expected_version}

      assert EventStore.append_to_stream(
               "decision-1",
               0,
               events(1),
               @returning ++ [condition: [items: [[types: ["typed"]]], after: 0]]
             ) == {:error, :append_condition_failed}
    end
  end

  # The batches of a split share one transaction, so its last batch's answer is the
  # whole append's. The first batch's would be a thousand short.
  describe "on the split path" do
    test "a new stream, at each side of a batch's edge" do
      for {count, head} <- [{1_000, 7}, {1_001, 1_007}, {2_000, 2_008}, {2_500, 4_008}] do
        stream_uuid = "new-#{count}"

        assert EventStore.append_to_stream(stream_uuid, 0, events(count), @returning) ==
                 {:ok,
                  %AppendResult{position: head + count, stream_versions: %{stream_uuid => count}}}
      end
    end

    test "an existing stream" do
      :ok = EventStore.append_to_stream("old", 0, events(2))

      assert EventStore.append_to_stream("old", 2, events(1_001), @returning) ==
               {:ok, %AppendResult{position: 1_010, stream_versions: %{"old" => 1_003}}}
    end

    test "`:any_version`" do
      assert EventStore.append_to_stream("any", :any_version, events(1_500), @returning) ==
               {:ok, %AppendResult{position: 1_507, stream_versions: %{"any" => 1_500}}}
    end

    test "a conditional append" do
      {:ok, %{after: head}} = EventStore.read_query([[tags: ["contested"]]])

      assert EventStore.append_to_stream(
               "decision-big",
               0,
               events(1_200, ["contested"]),
               @returning ++ [condition: [items: [[tags: ["contested"]]], after: head]]
             ) ==
               {:ok, %AppendResult{position: 1_207, stream_versions: %{"decision-big" => 1_200}}}
    end

    test "in a caller's transaction", context do
      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        assert EventStore.append_to_stream("big", 0, events(1_001), [conn: tx] ++ @returning) ==
                 {:ok, %AppendResult{position: 1_008, stream_versions: %{"big" => 1_001}}}
      end)

      assert {:ok, :ok} = LockSession.commit(session)
      assert stream_versions(context, "big") == Enum.to_list(1..1_001)
    end
  end

  describe "on several streams" do
    # The counts differ, and the list is not in the order the function locks in, so a version
    # read in that order lands on the wrong stream.
    test "a version per member, for members given out of order", context do
      :ok = EventStore.append_to_stream("m-existing", 0, events(5))

      assert EventStore.append_to_streams(
               [
                 {"z-new", 0, events(3)},
                 {"m-existing", 5, events(2)},
                 {"a-new", :any_version, events(1)}
               ],
               @returning
             ) ==
               {:ok,
                %AppendResult{
                  position: 18,
                  stream_versions: %{"z-new" => 3, "m-existing" => 7, "a-new" => 1}
                }}

      assert stream_versions(context, "z-new") == [1, 2, 3]
      assert stream_versions(context, "m-existing") == Enum.to_list(1..7)
      assert stream_versions(context, "a-new") == [1]
    end

    test "positions run contiguous in list order, ending at the answer", context do
      {:ok, %AppendResult{position: position}} =
        EventStore.append_to_streams(
          [{"z", 0, events(2)}, {"a", 0, events(1)}, {"m", 0, events(2)}],
          @returning
        )

      assert all_positions(context) == Enum.to_list(1..12)
      assert position == 12

      assert context |> all_events() |> Enum.drop(7) == [
               {"z", 1},
               {"z", 2},
               {"a", 1},
               {"m", 1},
               {"m", 2}
             ]
    end

    test "one member" do
      assert EventStore.append_to_streams([{"only", 0, events(2)}], @returning) ==
               {:ok, %AppendResult{position: 9, stream_versions: %{"only" => 2}}}
    end

    test "over 1,000 events, in one call" do
      assert EventStore.append_to_streams(
               [{"z-big", 0, events(1_100)}, {"a-big", 0, events(1)}],
               @returning
             ) ==
               {:ok,
                %AppendResult{
                  position: 1_108,
                  stream_versions: %{"z-big" => 1_100, "a-big" => 1}
                }}
    end

    test "a conditional append" do
      {:ok, %{after: head}} = EventStore.read_query([[tags: ["contested"]]])

      assert EventStore.append_to_streams(
               [{"decision-b", 0, events(2)}, {"decision-a", 0, events(1)}],
               @returning ++ [condition: [items: [[tags: ["contested"]]], after: head]]
             ) ==
               {:ok,
                %AppendResult{
                  position: 10,
                  stream_versions: %{"decision-b" => 2, "decision-a" => 1}
                }}
    end
  end

  describe "any other `returning:`" do
    test "is refused before any statement, in every path" do
      for returning <- invalid_returnings() do
        opts = [conn: @no_conn] ++ Enum.map(returning, &{:returning, &1})

        for count <- [1, 1_001] do
          assert EventStore.append_to_stream("stream", 0, events(count), opts) ==
                   {:error, :invalid_returning},
                 inspect({returning, count})
        end

        assert EventStore.append_to_streams([{"stream", 0, events(1)}], opts) ==
                 {:error, :invalid_returning},
               inspect(returning)
      end
    end

    test "leaves a caller's transaction alive, and writes nothing", context do
      session = LockSession.start(context.postgrex_config)
      head = all_version(context)

      LockSession.run(session, fn tx ->
        for returning <- invalid_returnings() do
          opts = [conn: tx] ++ Enum.map(returning, &{:returning, &1})

          assert EventStore.append_to_stream("stream", 0, events(1), opts) ==
                   {:error, :invalid_returning}

          assert EventStore.append_to_streams([{"stream", 0, events(1)}], opts) ==
                   {:error, :invalid_returning}
        end

        assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])
      end)

      assert {:ok, :ok} = LockSession.commit(session)
      assert all_version(context) == head
    end
  end

  defp invalid_returnings do
    [
      [nil],
      [true],
      [:ok],
      [:position],
      ["append_result"],
      [[:append_result]],
      [:append_result, :append_result],
      [:append_result, :position]
    ]
  end

  defp event(tags) do
    %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: tags}
  end

  defp events(count, tags \\ []) do
    for _index <- 1..count, do: event(tags)
  end
end
