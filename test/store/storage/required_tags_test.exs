defmodule Decree.Store.Storage.RequiredTagsTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{EventData, LockSession}
  alias TestEventStore, as: EventStore

  @no_conn :a_connection_that_does_not_exist

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)
    start_supervised!({TestEventStore, serializer: PassThroughSerializer})

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "an event lacking a required kind" do
    test "refuses `append_to_stream` with no statement, under and over the 1,000-event clause" do
      for count <- [1, 999, 1_000, 1_001], expected_version <- [:any_version, :no_stream, 0, 3] do
        events = events(count - 1) ++ [event([], ["email"])]

        assert EventStore.append_to_stream("stream", expected_version, events, conn: @no_conn) ==
                 {:error, :missing_required_tag},
               inspect({count, expected_version})
      end
    end

    test "refuses `append_to_streams` with no statement, one member refusing the batch" do
      refusing = [event([], ["email"])]

      for appends <- [
            [{"a", 0, refusing}],
            [{"a", 0, refusing}, {"b", 0, events(1)}],
            [{"a", 0, events(1)}, {"b", :any_version, refusing}]
          ] do
        assert EventStore.append_to_streams(appends, conn: @no_conn) ==
                 {:error, :missing_required_tag}
      end
    end

    test "writes nothing", context do
      assert EventStore.append_to_stream("a", 0, [event([], ["email"])]) ==
               {:error, :missing_required_tag}

      assert EventStore.append_to_streams([
               {"a", 0, events(1)},
               {"b", 0, [event([], ["email"])]}
             ]) == {:error, :missing_required_tag}

      assert all_version(context) == 0
      assert stream_id(context, "a") == nil
    end

    test "is refused on a conditional append too" do
      stop_supervised!(TestEventStore)

      start_supervised!(
        {TestEventStore,
         conditional_appends: [stream_prefixes: ["decision-"]], serializer: PassThroughSerializer}
      )

      assert EventStore.append_to_stream("decision-1", 0, [event([], ["email"])],
               conn: @no_conn,
               condition: [items: [[tags: ["never"]]], after: 0]
             ) == {:error, :missing_required_tag}
    end

    test "leaves a caller's transaction usable", context do
      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        for events <- [[event([], ["email"])], events(1_000) ++ [event([], ["email"])]] do
          assert EventStore.append_to_stream("a", 0, events, conn: tx) ==
                   {:error, :missing_required_tag}
        end

        assert EventStore.append_to_streams([{"a", 0, [event([], ["email"])]}], conn: tx) ==
                 {:error, :missing_required_tag}

        assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])

        assert :ok = EventStore.append_to_stream("written", 0, events(1), conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)
      assert stream_versions(context, "written") == [1]
    end
  end

  describe "what carries a kind" do
    test "an empty list and a satisfied list both pass", context do
      assert :ok = EventStore.append_to_stream("a", 0, [event([], [])])
      assert :ok = EventStore.append_to_stream("b", 0, [event(["email:x"], ["email"])])

      assert :ok =
               EventStore.append_to_streams([
                 {"c", 0, [event(["x:1", "email:x"], ["x", "email"])]},
                 {"d", 0, [event([], [])]}
               ])

      assert all_version(context) == 4
    end

    test "a tag with no colon carries its own kind" do
      assert :ok = EventStore.append_to_stream("a", 0, [event(["email"], ["email"])])
    end

    test "a value may hold a colon" do
      assert :ok = EventStore.append_to_stream("a", 0, [event(["email:a:b"], ["email"])])
    end

    test "`email:` with no value does not" do
      assert EventStore.append_to_stream("a", 0, [event(["email:"], ["email"])], conn: @no_conn) ==
               {:error, :missing_required_tag}
    end

    test "a kind that only starts the same does not" do
      for tag <- ["emailed:x", "emailed", "e", "mail:email", "email_x:y"] do
        assert EventStore.append_to_stream("a", 0, [event([tag], ["email"])], conn: @no_conn) ==
                 {:error, :missing_required_tag},
               tag
      end
    end

    test "every kind on the list has to be carried" do
      assert EventStore.append_to_stream("a", 0, [event(["a:1"], ["a", "b"])], conn: @no_conn) ==
               {:error, :missing_required_tag}
    end

    test "each event answers for itself" do
      events = [event(["a:1"], ["a"]), event([], ["a"])]

      assert EventStore.append_to_stream("a", 0, events, conn: @no_conn) ==
               {:error, :missing_required_tag}
    end
  end

  describe "a list that is not kinds" do
    test "refuses with no statement, on every entry" do
      for kinds <- [
            [:email],
            ["ok", :email],
            nil,
            "email",
            [""],
            ["a:b"],
            [1],
            [<<0>>],
            ["a" | :b]
          ] do
        events = [event(["email:x"], kinds)]

        assert EventStore.append_to_stream("a", 0, events, conn: @no_conn) ==
                 {:error, :invalid_required_tag_kinds},
               inspect(kinds)

        assert EventStore.append_to_stream("a", 0, events(1_000) ++ events, conn: @no_conn) ==
                 {:error, :invalid_required_tag_kinds},
               inspect(kinds)

        assert EventStore.append_to_streams([{"a", 0, events}], conn: @no_conn) ==
                 {:error, :invalid_required_tag_kinds},
               inspect(kinds)
      end
    end

    test "answers before a missing kind, whichever event holds it" do
      events = [event([], ["email"]), event([], [:email])]

      assert EventStore.append_to_stream("a", 0, events, conn: @no_conn) ==
               {:error, :invalid_required_tag_kinds}
    end
  end

  describe "the order of refusals" do
    test "`:invalid_tag` wins over both" do
      for kinds <- [["email"], [:email]] do
        assert EventStore.append_to_stream("a", 0, [event([<<0>>], kinds)], conn: @no_conn) ==
                 {:error, :invalid_tag}

        assert EventStore.append_to_streams([{"a", 0, [event([<<0>>], kinds)]}], conn: @no_conn) ==
                 {:error, :invalid_tag}
      end
    end

    test "`:tag_too_long` wins over both" do
      too_long = String.duplicate("a", 513)

      for kinds <- [["email"], [:email]] do
        assert EventStore.append_to_stream("a", 0, [event([too_long], kinds)], conn: @no_conn) ==
                 {:error, :tag_too_long}
      end
    end

    test "they win over what reads the stream, and over a plain append's refusal" do
      stop_supervised!(TestEventStore)

      start_supervised!(
        {TestEventStore,
         conditional_appends: [stream_prefixes: ["decision-"]], serializer: PassThroughSerializer}
      )

      assert EventStore.append_to_stream("decision-1", 0, [event([], ["email"])], conn: @no_conn) ==
               {:error, :missing_required_tag}
    end
  end

  test "the kinds are not stored", context do
    assert :ok = EventStore.append_to_stream("a", 0, [event(["email:x"], ["email"])])

    assert %Postgrex.Result{rows: [["email:x"]]} =
             Postgrex.query!(context.conn, "SELECT tag FROM event_tags", [])
  end

  defp event(tags, kinds) do
    %EventData{
      event_type: "typed",
      data: "{}",
      metadata: "{}",
      tags: tags,
      required_tag_kinds: kinds
    }
  end

  defp events(count) do
    for _index <- 1..count//1, do: event([], [])
  end
end
