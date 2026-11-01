defmodule Decree.Store.Storage.HardDeleteTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{AppendError, EventData, LockSession, UUID}
  alias Decree.Store.Storage
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  # Enough that the delete almost always holds another event's row when it blocks.
  @multi_link_events 8

  # A hard delete takes the stream's row, deletes the events with their tag and type rows,
  # and then deletes the stream's row. Each of these holds a session open across a step, so
  # what each side holds shows in Postgres.
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
  # it waits on the event's row. Its old shape deleted the stream's row first: this cycle.
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

    test "with a real link: no cycle, and the delete fails the foreign key", context do
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

      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation}}} =
               Task.await(delete)

      assert stream_versions(context, "stream") == [1]
      assert stream_versions(context, "link-stream") == [1]
      assert tag_rows(context) == [[1, "tenant:a"]]
    end

    test "a link after the delete commits answers `:not_found`", context do
      %{recorded: recorded} = context

      assert :ok = delete(context, "stream")

      # The link's join finds no original row, so it writes none.
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

  # A measured residual, not a promise. A link of several of the stream's events locks event
  # rows in its own row order and the delete in its scan's, so each can hold what the other
  # wants. The delete's order changes run to run, so the setup can miss: the delete can reach
  # the held row first and hold no other. Each arm is therefore what must be true of its
  # case, and nothing here says that a deadlock happens.
  describe "against a link of several of the stream's events" do
    setup context do
      events = for index <- 1..@multi_link_events, do: event(["tenant:#{index}"])

      :ok = EventStore.append_to_stream("stream", 0, events)

      {:ok, recorded} = EventStore.read_stream_forward("stream")

      [recorded: recorded, stream_id: stream_id(context, "stream")]
    end

    test "a deadlock takes a delete back whole, and a retry goes through", context do
      %{recorded: [first | rest]} = context

      before = store_state(context)

      linker = hold_event_key_share(context, first)
      delete = delete_blocked_on(context, linker)

      case Enum.find(rest, &event_held?(context, &1)) do
        nil ->
          assert {:ok, :ok} = LockSession.commit(linker)
          assert :ok = Task.await(delete)

          assert_nothing_left(context)

        other ->
          link =
            LockSession.start_run(linker, fn tx ->
              EventStore.link_to_stream("link-stream", :any_version, [first, other], conn: tx)
            end)

          # The link first: a delete that survives waits for the link's transaction to end.
          # `deadlock_timeout` is what bounds this, and it fires on the session that has
          # waited longest, so either side can be the one that answers.
          case LockSession.await_run(link) do
            :ok ->
              assert {:error, %Postgrex.Error{postgres: %{code: :deadlock_detected}}} =
                       Task.await(delete)

              assert store_state(context) == before

              assert {:ok, :ok} = LockSession.commit(linker)
              assert stream_versions(context, "link-stream") == [1, 2]

              assert :ok = delete(context, "stream")

              assert stream_versions(context, "link-stream") == []
              assert_nothing_left(context)

            {:error, %AppendError{code: :deadlock_detected}} ->
              assert {:error, _rolled_back} = LockSession.commit(linker)
              assert :ok = Task.await(delete)

              assert_nothing_left(context)
          end
      end
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

    defp assert_fenced(%{raw_delete: raw_delete}) do
      for table <- ["event_tags", "event_types"] do
        assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
                 raw_delete.(table)
      end
    end

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

  # NOWAIT from the observer, in autocommit, so a free row is taken and let go at once.
  defp event_held?(context, recorded) do
    match?(
      {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}},
      Postgrex.query(
        context.observer,
        ~s(SELECT 1 FROM "#{context.schema}".events WHERE event_id = $1) <>
          " FOR KEY SHARE NOWAIT",
        [UUID.string_to_binary!(recorded.event_id)]
      )
    )
  end

  defp store_state(context) do
    %{
      version: stream_version(context, "stream"),
      versions: stream_versions(context, "stream"),
      positions: all_positions(context),
      events: count(context, "events"),
      tags: tag_rows(context),
      types: count(context, "event_types")
    }
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
      {:ok, :foreign_key_violation} -> true
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

  defp event(tags) do
    %EventData{
      event_type: "Elixir.Decree.Store.EventFactory.Event",
      data: %Decree.Store.EventFactory.Event{event: 1},
      metadata: %{},
      tags: tags
    }
  end
end
