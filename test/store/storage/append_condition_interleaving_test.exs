defmodule Decree.Store.Storage.AppendConditionInterleavingTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{EventData, LockSession}
  alias TestEventStore, as: EventStore

  @contested [[tags: ["contested"]]]

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  # The pool of `TestEventStore` has one connection, which would serialise the two
  # sessions and prove nothing, so each session here has its own.
  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore,
       conditional_appends: [stream_prefixes: ["decision-"]], serializer: PassThroughSerializer}
    )

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "an append that waits on `$all`" do
    test "fails on a matching event committed while it waited", context do
      %{observer: observer, postgrex_config: postgrex_config} = context

      :ok = EventStore.append_to_stream("before", 0, [event(["other"])])

      after_position = head(context)

      # Session A holds `$all`: its rows and its bump are both uncommitted.
      session_a = LockSession.start(postgrex_config)

      LockSession.run(session_a, fn tx ->
        :ok = EventStore.append_to_stream("session-a", 0, [event(["contested"])], conn: tx)
      end)

      {session_b, backend_b} = open_session(context)
      append = conditional_append(session_b, "decision-b", @contested, after_position)

      # Without this the test could pass on timing alone: B must have reached `$all`,
      # after a read of its own that cannot see A's event, before A commits.
      LockSession.wait_until_blocked(observer, backend_b, session_a.backend)

      assert head(context) == after_position

      assert {:ok, :ok} = LockSession.commit(session_a)

      assert Task.await(append) == {:error, :append_condition_failed}

      assert head(context) == after_position + 1
      assert {:ok, [_event]} = EventStore.read_stream_forward("session-a")
      assert {:error, :stream_not_found} = EventStore.read_stream_forward("decision-b")
      assert all_positions(context) == [1, 2]
    end

    test "passes on an event that does not match, and takes the next position", context do
      %{observer: observer, postgrex_config: postgrex_config} = context

      :ok = EventStore.append_to_stream("before", 0, [event(["other"])])

      after_position = head(context)

      session_a = LockSession.start(postgrex_config)

      LockSession.run(session_a, fn tx ->
        :ok = EventStore.append_to_stream("session-a", 0, [event(["uncontested"])], conn: tx)
      end)

      {session_b, backend_b} = open_session(context)
      append = conditional_append(session_b, "decision-b", @contested, after_position)

      LockSession.wait_until_blocked(observer, backend_b, session_a.backend)

      assert {:ok, :ok} = LockSession.commit(session_a)

      assert Task.await(append) == :ok

      assert head(context) == after_position + 2

      assert {:ok, events} = EventStore.read_all_streams_forward()

      assert %{stream_uuid: "decision-b", event_number: position} = List.last(events)
      assert position == after_position + 2
      assert all_positions(context) == [1, 2, 3]
    end

    test "passes on a matching event whose transaction rolled back", context do
      %{observer: observer, postgrex_config: postgrex_config} = context

      session_a = LockSession.start(postgrex_config)

      LockSession.run(session_a, fn tx ->
        :ok = EventStore.append_to_stream("session-a", 0, [event(["contested"])], conn: tx)
      end)

      {session_b, backend_b} = open_session(context)
      append = conditional_append(session_b, "decision-b", @contested, 0)

      LockSession.wait_until_blocked(observer, backend_b, session_a.backend)

      assert {:error, :rolled_back} = LockSession.rollback(session_a)

      assert Task.await(append) == :ok
      assert all_positions(context) == [1]
    end

    test "on an existing stream fails the same way", context do
      %{observer: observer, postgrex_config: postgrex_config} = context

      :ok = EventStore.append_to_stream("decision-b", 0, [event([])])

      after_position = head(context)

      session_a = LockSession.start(postgrex_config)

      LockSession.run(session_a, fn tx ->
        :ok = EventStore.append_to_stream("session-a", 0, [event(["contested"])], conn: tx)
      end)

      {session_b, backend_b} = open_session(context)
      append = conditional_append(session_b, "decision-b", @contested, after_position, 1)

      LockSession.wait_until_blocked(observer, backend_b, session_a.backend)

      assert {:ok, :ok} = LockSession.commit(session_a)

      assert Task.await(append) == {:error, :append_condition_failed}

      assert {:ok, [_event]} = EventStore.read_stream_forward("decision-b")
      assert head(context) == after_position + 1
    end
  end

  describe "a multi-stream append that waits on `$all`" do
    test "fails on a matching event committed while it waited", context do
      %{observer: observer, postgrex_config: postgrex_config} = context

      :ok = EventStore.append_to_stream("before", 0, [event(["other"])])
      :ok = EventStore.append_to_stream("decision-existing", 0, [event([])])

      after_position = head(context)

      session_a = LockSession.start(postgrex_config)

      LockSession.run(session_a, fn tx ->
        :ok = EventStore.append_to_stream("session-a", 0, [event(["contested"])], conn: tx)
      end)

      {session_b, backend_b} = open_session(context)
      append = conditional_multi_append(session_b, after_position)

      LockSession.wait_until_blocked(observer, backend_b, session_a.backend)

      assert head(context) == after_position

      assert {:ok, :ok} = LockSession.commit(session_a)

      assert Task.await(append) == {:error, :append_condition_failed}

      assert head(context) == after_position + 1
      assert {:ok, [_event]} = EventStore.read_stream_forward("decision-existing")
      assert {:error, :stream_not_found} = EventStore.read_stream_forward("decision-new-a")
      assert {:error, :stream_not_found} = EventStore.read_stream_forward("decision-new-z")
      assert all_positions(context) == [1, 2, 3]
    end

    test "passes on an event that does not match, and takes the next positions", context do
      %{observer: observer, postgrex_config: postgrex_config} = context

      :ok = EventStore.append_to_stream("decision-existing", 0, [event([])])

      after_position = head(context)

      session_a = LockSession.start(postgrex_config)

      LockSession.run(session_a, fn tx ->
        :ok = EventStore.append_to_stream("session-a", 0, [event(["uncontested"])], conn: tx)
      end)

      {session_b, backend_b} = open_session(context)
      append = conditional_multi_append(session_b, after_position)

      LockSession.wait_until_blocked(observer, backend_b, session_a.backend)

      assert {:ok, :ok} = LockSession.commit(session_a)

      assert Task.await(append) == :ok

      assert head(context) == after_position + 4
      assert all_positions(context) == [1, 2, 3, 4, 5]
    end
  end

  describe "a burst of creators" do
    test "over one tag with the same `after`: exactly one wins", context do
      creators = 8
      sessions = for _creator <- 1..creators, do: open_session(context)

      for round <- 1..5 do
        after_position = head(context)
        tag = "contested-#{round}"

        answers =
          sessions
          |> Enum.with_index()
          |> Enum.map(fn {{session, _backend}, creator} ->
            Task.async(fn ->
              EventStore.append_to_stream("decision-#{round}-#{creator}", 0, [event([tag])],
                conn: session,
                condition: [items: [[tags: [tag]]], after: after_position]
              )
            end)
          end)
          |> Task.await_many(15_000)

        assert Enum.frequencies(answers) ==
                 %{:ok => 1, {:error, :append_condition_failed} => creators - 1}

        assert head(context) == after_position + 1
      end

      assert all_positions(context) == Enum.to_list(1..5)
    end
  end

  defp open_session(%{postgrex_config: postgrex_config}) do
    conn = LockSession.connect(postgrex_config)

    {conn, LockSession.backend_pid(conn)}
  end

  defp conditional_append(conn, stream_uuid, items, after_position, expected_version \\ 0) do
    Task.async(fn ->
      EventStore.append_to_stream(stream_uuid, expected_version, [event(["contested"])],
        conn: conn,
        condition: [items: items, after: after_position]
      )
    end)
  end

  defp conditional_multi_append(conn, after_position) do
    Task.async(fn ->
      EventStore.append_to_streams(
        [
          {"decision-new-z", 0, [event(["contested"])]},
          {"decision-existing", 1, [event(["contested"])]},
          {"decision-new-a", 0, [event(["contested"])]}
        ],
        conn: conn,
        condition: [items: @contested, after: after_position]
      )
    end)
  end

  defp event(tags) do
    %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: tags}
  end

  defp head(%{observer: observer, schema: schema}) do
    %Postgrex.Result{rows: [[version]]} =
      Postgrex.query!(
        observer,
        ~s(SELECT stream_version FROM "#{schema}".streams WHERE stream_id = 0),
        []
      )

    version
  end

  defp all_positions(%{observer: observer, schema: schema}) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        observer,
        ~s(SELECT stream_version FROM "#{schema}".stream_events) <>
          " WHERE stream_id = 0 ORDER BY stream_version",
        []
      )

    Enum.map(rows, &hd/1)
  end
end
