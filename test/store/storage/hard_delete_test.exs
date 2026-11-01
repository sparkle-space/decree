defmodule Decree.Store.Storage.HardDeleteTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{EventData, LockSession, UUID}
  alias Decree.Store.Storage
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  @multi_link_events 8

  # A hard delete takes the stream's row, takes the events' rows, deletes the events with their
  # tag and type rows, and then deletes the stream's row. Each of these holds a session open
  # across a step, so what each side holds shows in Postgres.
  setup %{postgrex_config: postgrex_config} do
    observer = LockSession.connect(postgrex_config)
    deleter = LockSession.connect(postgrex_config)

    [
      observer: observer,
      deleter: deleter,
      deleter_backend: LockSession.backend_pid(deleter),
      appender: LockSession.connect(postgrex_config)
    ]
  end

  describe "against a plain append" do
    test "delete first: the append blocks, answers `:not_found`, and nothing is left", context do
      %{appender: appender, observer: observer} = context

      :ok = EventStore.append_to_stream("stream", 0, [event(["tenant:a"])])

      deleter = LockSession.start(context.postgrex_config)

      LockSession.run(deleter, fn tx ->
        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

        assert :ok =
                 Storage.hard_delete_stream(tx, stream_id(context, "stream"),
                   schema: context.schema
                 )
      end)

      events = [event(["tenant:b"])]

      append =
        Task.async(fn ->
          EventStore.append_to_stream("stream", :any_version, events, conn: appender)
        end)

      LockSession.wait_until_blocked(observer, LockSession.backend_pid(appender), deleter.backend)

      assert {:ok, :ok} = LockSession.commit(deleter)
      assert {:error, :not_found} = Task.await(append)

      assert_nothing_left(context)
    end

    test "append first: the delete blocks, then removes what the append wrote", context do
      %{deleter_backend: deleter_backend, observer: observer} = context

      :ok = EventStore.append_to_stream("stream", 0, [event(["tenant:a"])])

      appender = LockSession.start(context.postgrex_config)

      LockSession.run(appender, fn tx ->
        assert :ok =
                 EventStore.append_to_stream("stream", :any_version, [event(["tenant:b"])],
                   conn: tx
                 )
      end)

      delete = async_delete(context, "stream")
      LockSession.wait_until_blocked(observer, deleter_backend, appender.backend)

      assert {:ok, :ok} = LockSession.commit(appender)
      assert :ok = Task.await(delete)

      assert_nothing_left(context)
    end
  end

  describe "a stream holding a link" do
    test "takes the linked event from everywhere, and its tags with it", context do
      :ok = EventStore.append_to_stream("source", 0, [event(["tenant:a"])])
      :ok = EventStore.append_to_stream("other", 0, [event(["tenant:b"])])

      {:ok, linked} = EventStore.read_stream_forward("source")
      :ok = EventStore.link_to_stream("link-stream", :any_version, linked)

      assert :ok = delete(context, "link-stream")

      assert stream_id(context, "link-stream") == nil
      assert stream_versions(context, "source") == []
      assert all_positions(context) == [2]
      assert tag_rows(context) == [[2, "tenant:b"]]
      assert rows(context, ~s(SELECT position FROM "#{context.schema}".event_types), []) == [[2]]
    end
  end

  # A link takes KEY SHARE on the event's row, and then on the original stream's. The delete
  # holds that stream's row in NO KEY UPDATE, which KEY SHARE does not conflict with, while
  # it waits on the event's row. A delete of the stream's row before that wait would be a cycle.
  describe "against a link" do
    setup context do
      :ok = EventStore.append_to_stream("stream", 0, [event(["tenant:a"])])

      {:ok, [recorded]} = EventStore.read_stream_forward("stream")

      [recorded: recorded, stream_id: stream_id(context, "stream")]
    end

    test "with no link row written, the delete waits and goes through", context do
      %{recorded: recorded, stream_id: stream_id} = context

      linker = hold_event_key_share(context, recorded)
      delete = delete_blocked_on(context, linker)

      assert %Postgrex.Result{num_rows: 1} =
               LockSession.run(linker, fn tx ->
                 Postgrex.query!(
                   tx,
                   ~s(SELECT 1 FROM "#{context.schema}".streams WHERE stream_id = $1) <>
                     " FOR KEY SHARE NOWAIT",
                   [stream_id]
                 )
               end)

      assert {:ok, :ok} = LockSession.commit(linker)
      assert :ok = Task.await(delete)

      assert_nothing_left(context)
    end

    test "with a real link: no cycle, and the link's row goes with the event", context do
      %{recorded: recorded} = context

      linker = hold_event_key_share(context, recorded)
      delete = delete_blocked_on(context, linker)

      # Its foreign key checks take the original stream's row while the delete holds it, so
      # this answering at all is the proof.
      assert :ok =
               LockSession.run(linker, fn tx ->
                 EventStore.link_to_stream("link-stream", :any_version, [recorded], conn: tx)
               end)

      assert {:ok, :ok} = LockSession.commit(linker)
      assert :ok = Task.await(delete)

      assert stream_versions(context, "link-stream") == []
      assert_nothing_left(context)
    end

    test "a link after the delete commits answers `:not_found`", context do
      %{recorded: recorded} = context

      assert :ok = delete(context, "stream")

      # The link's lock finds no event row, so it writes nothing.
      assert {:error, :not_found} =
               EventStore.link_to_stream("link-stream", :any_version, [recorded])

      assert stream_versions(context, "link-stream") == []
      assert_nothing_left_of(context, "stream")
    end

    test "concurrently, a link and a delete of one event never deadlock", context do
      answers =
        for round <- 1..50 do
          stream_uuid = "race-#{round}"

          :ok = EventStore.append_to_stream(stream_uuid, 0, [event(["tenant:a"])])

          {:ok, linked} = EventStore.read_stream_forward(stream_uuid)

          link =
            Task.async(fn ->
              EventStore.link_to_stream("race-link-#{round}", :any_version, linked,
                conn: context.appender
              )
            end)

          delete = async_delete(context, stream_uuid)

          {answer(Task.await(link)), answer(Task.await(delete))}
        end

      assert answers -- legitimate_answers(answers) == []
    end
  end

  # The event ids fall as the versions rise, so the link's list and the delete's scan both run
  # against the sorted order, which is the one they share. The delete waits on the lowest id,
  # holding no other event, and the link then takes every row from the session it waits on. So
  # this stages the delete's order alone: the delete holds nothing a link's order could meet,
  # and `Decree.Store.Storage.LinkLockOrderTest` stages the link's.
  describe "against a link of several of the stream's events" do
    setup context do
      events =
        for rank <- @multi_link_events..1//-1 do
          event(["tenant:#{rank}"], "00000000-0000-4000-8000-#{pad(rank)}")
        end

      :ok = EventStore.append_to_stream("stream", 0, events)

      {:ok, recorded} = EventStore.read_stream_forward("stream")

      [recorded: recorded, stream_id: stream_id(context, "stream")]
    end

    test "no cycle: the delete waits, and takes the link's rows with the events", context do
      %{recorded: recorded} = context

      linker = hold_event_key_share(context, Enum.min_by(recorded, & &1.event_id))
      delete = delete_blocked_on(context, linker)

      assert :ok =
               LockSession.run(linker, fn tx ->
                 EventStore.link_to_stream("link-stream", :any_version, recorded, conn: tx)
               end)

      assert {:ok, :ok} = LockSession.commit(linker)
      assert :ok = Task.await(delete)

      assert stream_versions(context, "link-stream") == []
      assert_nothing_left(context)
    end
  end

  describe "the fence" do
    setup %{deleter: deleter} do
      :ok = EventStore.append_to_stream("stream", 0, [event(["tenant:a"])])
      :ok = EventStore.append_to_stream("subject", 0, [event(["email:x"])])

      [raw_delete: &Postgrex.query(deleter, ~s(DELETE FROM "public".#{&1}), [])]
    end

    test "is closed again on the connection after a hard delete", context do
      assert :ok = delete(context, "stream")

      assert_fenced(context)
    end

    test "is closed again on the connection after `delete_stream_tags`", context do
      assert {:ok, 1} =
               Stream.delete_tags(context.deleter, "subject", ["email"],
                 schema: context.schema,
                 enable_hard_deletes: true
               )

      assert_fenced(context)
    end

    # A nested transaction is the caller's, so only the close can shut the fence here. The
    # savepoint keeps that transaction alive past each refusal, and the commit shows the close
    # took nothing with it.
    test "is closed again inside a caller's transaction after a hard delete", context do
      assert {:ok, :ok} =
               Postgrex.transaction(context.deleter, fn tx ->
                 assert :ok = Stream.delete(tx, "stream", :any_version, :hard, opts(context))

                 assert_fenced_in(context, tx)
               end)

      assert stream_versions(context, "stream") == []
      assert tag_rows(context) == [[2, "email:x"]]
    end

    test "is closed again inside a caller's transaction after `delete_stream_tags`", context do
      assert {:ok, :ok} =
               Postgrex.transaction(context.deleter, fn tx ->
                 assert {:ok, 1} = Stream.delete_tags(tx, "subject", ["email"], opts(context))

                 assert_fenced_in(context, tx)
               end)

      assert tag_rows(context) == [[1, "tenant:a"]]
    end

    # The stream's version moves between the delete's read and its lock, so this is the error
    # arm with the fence open, and there is no close on it. The rollback is what stands in.
    test "a failed delete leaves a caller's transaction rolled back, with nothing run after",
         context do
      %{deleter: deleter, deleter_backend: deleter_backend, observer: observer} = context

      appender = LockSession.start(context.postgrex_config)

      LockSession.run(appender, fn tx ->
        assert :ok =
                 EventStore.append_to_stream("stream", :any_version, [event(["tenant:b"])],
                   conn: tx
                 )
      end)

      delete =
        Task.async(fn ->
          Postgrex.transaction(deleter, fn tx ->
            assert {:error, :wrong_expected_version} =
                     Stream.delete(tx, "stream", 1, :hard, opts(context))

            assert {:error, %DBConnection.ConnectionError{message: "transaction rolling back"}} =
                     Postgrex.query(tx, raw_delete_sql(context, "event_tags"), [])
          end)
        end)

      LockSession.wait_until_blocked(observer, deleter_backend, appender.backend)

      assert {:ok, :ok} = LockSession.commit(appender)
      assert {:error, :rollback} = Task.await(delete)

      assert stream_versions(context, "stream") == [1, 2]
      assert tag_rows(context) == [[1, "tenant:a"], [2, "email:x"], [3, "tenant:b"]]
    end

    defp assert_fenced(%{raw_delete: raw_delete}) do
      for table <- ["event_tags", "event_types"] do
        assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
                 raw_delete.(table)
      end
    end

    defp assert_fenced_in(context, tx) do
      for table <- ["event_tags", "event_types"] do
        assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
                 Postgrex.query(tx, raw_delete_sql(context, table), [], mode: :savepoint)
      end

      :ok
    end

    defp raw_delete_sql(context, table), do: ~s(DELETE FROM "#{context.schema}".#{table})

    defp opts(context), do: [schema: context.schema, enable_hard_deletes: true]

    test "answers `:not_supported` with `enable_hard_deletes` off" do
      assert {:error, :not_supported} = EventStore.delete_stream("stream", :any_version, :hard)
      assert {:error, :not_supported} = EventStore.delete_stream_tags("subject", ["email"])
    end
  end

  describe "with the row gone or changed under the lock" do
    test "`:stream_not_found`, from any expected version", context do
      assert {:error, :stream_not_found} =
               LockSession.run(LockSession.start(context.postgrex_config), fn tx ->
                 Storage.hard_delete_stream(tx, 12_345,
                   schema: context.schema,
                   expected_version: 0
                 )
               end)
    end

    test "`:stream_deleted` for a soft delete since the read", context do
      :ok = EventStore.append_to_stream("stream", 0, [event([])])
      stream_id = stream_id(context, "stream")
      :ok = EventStore.delete_stream("stream", :any_version, :soft)

      session = LockSession.start(context.postgrex_config)

      assert {:error, :stream_deleted} =
               LockSession.run(session, fn tx ->
                 Storage.hard_delete_stream(tx, stream_id, schema: context.schema)
               end)

      assert {:ok, :ok} = LockSession.commit(session)
    end
  end

  defp hold_event_key_share(context, recorded) do
    linker = LockSession.start(context.postgrex_config)

    assert %Postgrex.Result{num_rows: 1} =
             LockSession.run(linker, fn tx ->
               Postgrex.query!(
                 tx,
                 ~s(SELECT 1 FROM "#{context.schema}".events WHERE event_id = $1) <>
                   " FOR KEY SHARE",
                 [UUID.string_to_binary!(recorded.event_id)]
               )
             end)

    linker
  end

  # Blocked on the event's row, so it holds the stream's row, shown here with NOWAIT.
  defp delete_blocked_on(context, linker) do
    %{deleter_backend: deleter_backend, observer: observer} = context

    delete = async_delete(context, "stream")
    LockSession.wait_until_blocked(observer, deleter_backend, linker.backend)

    assert_row_locked(context, context.stream_id)

    delete
  end

  defp delete(context, stream_uuid) do
    Stream.delete(context.deleter, stream_uuid, :any_version, :hard,
      schema: context.schema,
      enable_hard_deletes: true
    )
  end

  defp async_delete(context, stream_uuid), do: Task.async(fn -> delete(context, stream_uuid) end)

  # What the concurrent test may see. Anything else, a deadlock above all, stays in the
  # difference its assertion prints.
  defp legitimate_answers(answers) do
    Enum.filter(answers, fn
      {:ok, :ok} -> true
      {:not_found, :ok} -> true
      _other -> false
    end)
  end

  defp answer(%Postgrex.Error{postgres: %{code: code}}), do: code
  defp answer({:error, %Postgrex.Error{postgres: %{code: code}}}), do: code
  defp answer({:error, reason}), do: reason
  defp answer(:ok), do: :ok

  defp assert_nothing_left(context) do
    assert stream_id(context, "stream") == nil
    assert_nothing_left_of(context, "stream")

    assert all_positions(context) == []
    assert count(context, "events") == 0
    assert count(context, "event_tags") == 0
    assert count(context, "event_types") == 0
  end

  defp assert_nothing_left_of(context, stream_uuid) do
    assert stream_versions(context, stream_uuid) == []
    assert tag_rows(context) == []
  end

  defp tag_rows(context) do
    rows(context, ~s(SELECT position, tag FROM "#{context.schema}".event_tags ORDER BY 1, 2), [])
  end

  defp pad(rank), do: rank |> Integer.to_string() |> String.pad_leading(12, "0")

  defp event(tags, event_id \\ nil) do
    %EventData{
      event_id: event_id,
      event_type: "Elixir.Decree.Store.EventFactory.Event",
      data: %Decree.Store.EventFactory.Event{event: 1},
      metadata: %{},
      tags: tags
    }
  end
end
