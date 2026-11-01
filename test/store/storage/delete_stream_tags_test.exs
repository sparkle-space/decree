defmodule Decree.Store.Storage.DeleteStreamTagsTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{EventData, LockSession, ReadResult}
  alias TestEventStore, as: EventStore

  @all_stream 0
  @subject_events 8
  @subject_tag_rows @subject_events * 2
  @race_rounds 50
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

  # The subject's row is no common lock here: the hard delete holds the link stream's. So the
  # two meet on tag rows, and what keeps them from a cycle is the one order they take them in.
  describe "against a hard delete of a link stream holding the subject's events" do
    setup %{postgrex_config: postgrex_config} do
      tags_conn = LockSession.connect(postgrex_config)
      hard_conn = LockSession.connect(postgrex_config)

      [
        tags_conn: tags_conn,
        tags_backend: LockSession.backend_pid(tags_conn),
        hard_conn: hard_conn,
        hard_backend: LockSession.backend_pid(hard_conn)
      ]
    end

    test "each waits for the lowest row holding no other, and both finish", context do
      %{observer: observer, postgrex_config: postgrex_config} = context
      %{hard_backend: hard_backend, tags_backend: tags_backend} = context

      subject_held_by_link("subject", "link")

      [lowest | others] = tag_rows(context)

      holder = LockSession.start(postgrex_config)
      hold_tag_row(holder, context, lowest)

      delete_tags = async_delete_tags("subject", context.tags_conn)

      LockSession.wait_until_blocked(observer, tags_backend, holder.backend)

      assert_tag_rows_free(context, others)

      delete_link = async_delete_link("link", context.hard_conn)

      # It queues behind the first waiter of that row, not behind the row's holder.
      LockSession.wait_until_blocked(observer, hard_backend, tags_backend)

      assert_tag_rows_free(context, others)

      assert {:ok, :ok} = LockSession.commit(holder)

      assert {:ok, @subject_tag_rows} = Task.await(delete_tags)
      assert :ok = Task.await(delete_link)

      assert tag_rows(context) == []
      assert stream_versions(context, "subject") == []
    end

    # Whoever takes the lowest row holds every row the other wants, because the subject's tag
    # rows are all of the kind deleted and all held by the link stream. So the erasure takes
    # them all, or a hard delete that committed first leaves it none. It never answers a part
    # of them, and the hard delete always goes through. A deadlock stays in the difference
    # the assertion prints.
    test "concurrently, the two never deadlock", context do
      %{hard_conn: hard_conn, tags_conn: tags_conn} = context

      answers =
        for round <- 1..@race_rounds do
          subject = "subject-#{round}"
          link = "link-#{round}"

          subject_held_by_link(subject, link)

          delete_tags = async_delete_tags(subject, tags_conn)
          delete_link = async_delete_link(link, hard_conn)

          {Task.await(delete_tags), Task.await(delete_link)}
        end

      assert answers -- legitimate_answers(answers) == []

      assert count(context, "event_tags") == 0
      assert count(context, "events") == 0
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

  # Each event's tags out of their lock order, so that a scan's order is not that order.
  defp subject_held_by_link(subject, link) do
    for index <- 1..@subject_events do
      append(subject, ["email:b#{index}", "email:a#{index}"])
    end

    {:ok, events} = EventStore.read_stream_forward(subject)

    :ok = EventStore.link_to_stream(link, :any_version, events)
  end

  defp async_delete_tags(subject, conn) do
    Task.async(fn -> EventStore.delete_stream_tags(subject, ["email"], conn: conn) end)
  end

  defp async_delete_link(link, conn) do
    Task.async(fn -> EventStore.delete_stream(link, :any_version, :hard, conn: conn) end)
  end

  defp legitimate_answers(answers) do
    Enum.filter(answers, fn
      {{:ok, @subject_tag_rows}, :ok} -> true
      {{:ok, 0}, :ok} -> true
      _other -> false
    end)
  end

  defp tag_rows(%{schema: schema} = context) do
    rows(context, ~s(SELECT position, tag FROM "#{schema}".event_tags ORDER BY position, tag), [])
  end

  defp hold_tag_row(session, %{schema: schema}, [position, tag]) do
    assert %Postgrex.Result{num_rows: 1} =
             LockSession.run(session, fn tx ->
               Postgrex.query!(
                 tx,
                 ~s(SELECT 1 FROM "#{schema}".event_tags WHERE position = $1 AND tag = $2) <>
                   " FOR UPDATE",
                 [position, tag]
               )
             end)
  end

  # NOWAIT from the observer, in autocommit, so a free row is taken and let go at once.
  defp assert_tag_rows_free(%{observer: observer, schema: schema}, tag_rows) do
    for [position, tag] <- tag_rows do
      assert %Postgrex.Result{num_rows: 1} =
               Postgrex.query!(
                 observer,
                 ~s(SELECT 1 FROM "#{schema}".event_tags WHERE position = $1 AND tag = $2) <>
                   " FOR UPDATE NOWAIT",
                 [position, tag]
               )
    end
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
