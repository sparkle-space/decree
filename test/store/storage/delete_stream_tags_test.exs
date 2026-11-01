defmodule Decree.Store.Storage.DeleteStreamTagsTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{EventData, LockSession, ReadResult}
  alias TestEventStore, as: EventStore

  @all_stream 0
  # A connection that does not exist: any statement, a BEGIN included, would exit.
  @no_conn :a_connection_that_does_not_exist

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore,
       enable_hard_deletes: true,
       conditional_appends: [stream_prefixes: ["decision-"]],
       serializer: PassThroughSerializer}
    )

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "what goes" do
    test "only that subject's rows of that kind", context do
      append("a", ["email:x", "user:1"])
      append("b", ["email:x", "user:2"])

      assert {:ok, 1} = EventStore.delete_stream_tags("a", ["email"])

      assert tags(context, "a") == ["user:1"]
      assert tags(context, "b") == ["email:x", "user:2"]
    end

    test "every kind named, on every event of the stream", context do
      append("a", ["email:x", "phone:1", "user:1"])
      append("a", ["email:y", "phone:2"])

      assert {:ok, 4} = EventStore.delete_stream_tags("a", ["email", "phone"])

      assert tags(context, "a") == ["user:1"]
    end

    test "a later holder keeps theirs, and a re-run deletes nothing", context do
      append("a", ["email:x"])

      assert {:ok, 1} = EventStore.delete_stream_tags("a", ["email"])

      append("later-holder", ["email:x"])

      assert {:ok, 0} = EventStore.delete_stream_tags("a", ["email"])
      assert tags(context, "later-holder") == ["email:x"]
    end

    test "a kind matches no longer kind, and a value may hold a colon", context do
      append("a", ["email:x", "email:y:z", "email", "email_verified:x", "emai:x"])

      assert {:ok, 3} = EventStore.delete_stream_tags("a", ["email"])

      assert tags(context, "a") == ["emai:x", "email_verified:x"]
    end

    test "a row of an event the stream only links to stays", context do
      append("b", ["email:b"])
      append("a", ["email:a"])

      {:ok, linked} = EventStore.read_stream_forward("b")
      :ok = EventStore.link_to_stream("a", :any_version, linked)

      assert {:ok, 1} = EventStore.delete_stream_tags("a", ["email"])

      assert tags(context, "a") == []
      assert tags(context, "b") == ["email:b"]
    end

    test "the events stay, and so do their type rows", context do
      append("a", ["email:x"])

      assert {:ok, 1} = EventStore.delete_stream_tags("a", ["email"])

      assert count(context, "events") == 1
      assert count(context, "event_types") == 1
      assert stream_versions(context, "a") == [1]
    end

    test "a soft-deleted stream is still reached", context do
      append("a", ["email:x"])

      :ok = EventStore.delete_stream("a", :any_version, :soft)

      assert {:ok, 1} = EventStore.delete_stream_tags("a", ["email"])
      assert tags(context, "a") == []
    end
  end

  describe "what it answers" do
    test "`$all` is refused" do
      assert {:error, :cannot_delete_all_stream} =
               EventStore.delete_stream_tags("$all", ["email"], conn: @no_conn)
    end

    test "an unknown stream" do
      assert {:error, :stream_not_found} = EventStore.delete_stream_tags("nobody", ["email"])
    end

    test "malformed kinds, with no statement sent" do
      for tag_kinds <- [
            [],
            nil,
            "email",
            [:email],
            [""],
            ["email:x"],
            ["email", ""],
            ["email", :phone],
            ["email" | "phone"],
            [<<0>>],
            [<<255>>]
          ] do
        assert {:error, :invalid_tag_kinds} =
                 EventStore.delete_stream_tags("a", tag_kinds, conn: @no_conn)
      end
    end

    test "`enable_hard_deletes` off, with no statement sent" do
      stop_supervised!(TestEventStore)
      start_supervised!({TestEventStore, serializer: PassThroughSerializer})

      assert {:error, :not_supported} =
               EventStore.delete_stream_tags("a", ["email"], conn: @no_conn)
    end
  end

  # What a phantom `$all` notification would be: an UPDATE of its row fires
  # `event_notification`. The marker is the proof nothing came before it, since notifications
  # arrive in commit order.
  describe "no notification" do
    test "reaches a subscriber or the channel, and `$all` does not move", context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      {:ok, listener} = Postgrex.Notifications.start_link(postgrex_config)
      {:ok, _reference} = Postgrex.Notifications.listen(listener, "#{schema}.events")

      :ok = EventStore.subscribe("$all")

      append("a", ["email:x"])

      assert_receive {:events, [_appended]}
      assert_receive {:notification, ^listener, _reference, _channel, "a," <> _rest}
      assert_receive {:notification, ^listener, _reference, _channel, "$all," <> _rest}

      version = all_version(context)

      assert {:ok, 1} = EventStore.delete_stream_tags("a", ["email"])

      assert all_version(context) == version

      append("marker", [])

      assert_receive {:notification, ^listener, _reference, _channel, payload}
      assert String.starts_with?(payload, "marker,")

      assert_receive {:events, [%{stream_uuid: "marker"}]}
      refute_received {:events, _events}
    end
  end

  describe "under the lock" do
    test "it waits for `$all`, holding the stream's row", context do
      %{observer: observer, postgrex_config: postgrex_config, schema: schema} = context

      append("a", ["email:x"])

      deleter = LockSession.connect(postgrex_config)
      backend = LockSession.backend_pid(deleter)

      holder = LockSession.start(postgrex_config)
      LockSession.lock_row(holder, schema, @all_stream)

      delete = Task.async(fn -> EventStore.delete_stream_tags("a", ["email"], conn: deleter) end)

      LockSession.wait_until_blocked(observer, backend, holder.backend)

      assert_row_locked(context, stream_id(context, "a"))
      assert tags(context, "a") == ["email:x"]

      assert {:ok, :ok} = LockSession.commit(holder)
      assert {:ok, 1} = Task.await(delete)

      # Started after the commit, so its snapshot has the row gone.
      assert :ok = conditional([[tags: ["email:x"]]])
    end
  end

  # A read and a condition share `query_positions`, so they cannot fork on an erased row.
  describe "a read and a condition after an erasure" do
    setup do
      append("a", ["user:1", "email:x"])

      assert {:ok, 1} = EventStore.delete_stream_tags("a", ["email"])

      :ok
    end

    test "agree that the erased tag matches nothing" do
      items = [[tags: ["email:x"]]]

      assert matches(items) == []
      assert :ok = conditional(items)
    end

    test "agree that the event still matches by its type" do
      items = [[types: ["typed"]]]

      assert matches(items) == [1]
      assert {:error, :append_condition_failed} = conditional(items)
    end

    test "agree that an item with the erased tag as a rest tag matches nothing" do
      items = [[tags: ["user:1", "email:x"]]]

      assert matches(items) == []
      assert :ok = conditional(items)
    end
  end

  defp append(stream_uuid, tags) do
    :ok = EventStore.append_to_stream(stream_uuid, :any_version, [event("typed", tags)])
  end

  defp conditional(items) do
    stream_uuid = "decision-#{System.unique_integer([:positive])}"

    EventStore.append_to_stream(stream_uuid, 0, [event("written", [])],
      condition: [items: items, after: 0]
    )
  end

  defp matches(items) do
    assert {:ok, %ReadResult{events: events}} = EventStore.read_query(items)

    Enum.map(events, & &1.event_number)
  end

  # The tags of the events appended to this stream, found through `$all`.
  defp tags(%{schema: schema} = context, stream_uuid) do
    context
    |> rows(
      ~s(SELECT t.tag FROM "#{schema}".event_tags t) <>
        ~s( JOIN "#{schema}".stream_events se) <>
        " ON se.stream_id = 0 AND se.stream_version = t.position" <>
        ~s( JOIN "#{schema}".streams s ON s.stream_id = se.original_stream_id) <>
        " WHERE s.stream_uuid = $1 ORDER BY 1",
      [stream_uuid]
    )
    |> Enum.map(&hd/1)
  end

  defp event(event_type, tags) do
    %EventData{event_type: event_type, data: "{}", metadata: "{}", tags: tags}
  end
end
