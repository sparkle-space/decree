defmodule Decree.Store.Storage.LinkLockOrderTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows
  import ExUnit.CaptureLog

  alias Decree.Store.{AppendError, EventFactory, LockSession, Storage, UUID}
  alias Decree.Store.Sql.Statements
  alias Decree.Store.Storage.CreateStream
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  # A link takes its stream's row, then its events' rows in `event_id` order, then inserts. A
  # hard delete takes its stream's row, then its events' rows in that same order, then deletes.
  # These tests hold each lock from another session, so what each side has and has not taken
  # shows in Postgres, never in timing.
  #
  # Every event id here is the test's own, and falls as the stream's version rises, so that a
  # stream's scan order, a caller's order and the sorted order differ.
  setup %{postgrex_config: postgrex_config} do
    observer = LockSession.connect(postgrex_config)
    linker = LockSession.connect(postgrex_config)
    deleter = LockSession.connect(postgrex_config)
    second_deleter = LockSession.connect(postgrex_config)

    # A btree scan sorts the keys of `= ANY` itself, and a merge join sorts its rows too, so on
    # tables this small either hands back `event_id` order with or without a lock's `ORDER BY`,
    # and a missing one would pass by plan. These sessions read in physical order instead, which
    # runs against `event_id` here. `second_deleter` follows its stream's rows, not `events`':
    # two sessions walking one table in one order could not cycle, sorted or not.
    # `HardDeletePlanTest` pins the statements' own plans.
    read_in_physical_order(linker)
    read_in_physical_order(deleter, ["mergejoin"])
    read_in_physical_order(second_deleter, ["mergejoin", "hashjoin", "material"])

    [
      observer: observer,
      linker: linker,
      linker_backend: LockSession.backend_pid(linker),
      deleter: deleter,
      deleter_backend: LockSession.backend_pid(deleter),
      second_deleter: second_deleter,
      second_deleter_backend: LockSession.backend_pid(second_deleter)
    ]
  end

  describe "losing a race to create the target" do
    test "to an uncommitted row: the link retries, and links", context do
      ids = source(context, "source", 1, 2)

      holder = hold_new_stream(context, "target")

      link = async_link(context, "target", :any_version, ids)
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(link)

      assert linked_ids(context, "target") == ids
      assert stream_versions(context, "target") == [1, 2]
      assert stream_version(context, "target") == 2
    end

    test "to an uncommitted link: the retry numbers after it", context do
      [first, second] = source(context, "source", 1, 2)

      holder = LockSession.start(context.postgrex_config)

      LockSession.run(holder, fn tx ->
        assert :ok = EventStore.link_to_stream("target", :any_version, [first], conn: tx)
      end)

      link = async_link(context, "target", :any_version, [second])
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(link)

      assert linked_ids(context, "target") == [first, second]
    end

    test "with `:no_stream`: `:stream_exists` from its retry", context do
      [first, second] = source(context, "source", 1, 2)

      holder = LockSession.start(context.postgrex_config)

      LockSession.run(holder, fn tx ->
        assert :ok = EventStore.link_to_stream("target", :no_stream, [first], conn: tx)
      end)

      link = async_link(context, "target", :no_stream, [second])
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert {:error, :stream_exists} = Task.await(link)

      assert linked_ids(context, "target") == [first]
    end

    test "in a caller's transaction: answered, with no retry", context do
      ids = source(context, "source", 1, 1)

      holder = hold_new_stream(context, "target")
      caller = LockSession.start(context.postgrex_config)

      reply =
        LockSession.start_run(caller, fn tx ->
          assert %DBConnection{conn_mode: :transaction} = tx

          EventStore.link_to_stream("target", :any_version, ids, conn: tx)
        end)

      LockSession.wait_until_blocked(context.observer, caller.backend, holder.backend)

      assert {:ok, :ok} = LockSession.commit(holder)

      # Answered as itself: a retry's read in this aborted transaction would answer
      # `in_failed_sql_transaction`.
      assert {:error, :duplicate_stream_uuid} = LockSession.await_run(reply)

      assert {:error, _error} = LockSession.run(caller, &Postgrex.query(&1, "SELECT 1", []))
      assert {:error, :rollback} = LockSession.commit(caller)

      assert stream_versions(context, "target") == []
    end

    test "on its one retry: `:already_retried_once`", context do
      ids = source(context, "source", 1, 1)
      opts = [schema: context.schema, serializer: context.serializer, retried_once: true]

      holder = hold_new_stream(context, "target")

      link =
        Task.async(fn ->
          Stream.link_to_stream(context.linker, "target", :any_version, ids, opts)
        end)

      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)

      assert {:error, {:already_retried_once, :duplicate_stream_uuid}} = Task.await(link)
    end

    test "a `DBConnection.run/3` checkout is no transaction, and retries", context do
      %{linker: linker} = context

      ids = source(context, "source", 1, 1)

      holder = hold_new_stream(context, "target")

      link =
        Task.async(fn ->
          DBConnection.run(linker, fn checkout ->
            assert %DBConnection{conn_mode: nil} = checkout

            EventStore.link_to_stream("target", :any_version, ids, conn: checkout)
          end)
        end)

      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(link)

      assert linked_ids(context, "target") == ids
    end

    test "`:no_stream` against a stream that exists answers `:stream_exists`", context do
      [first, second] = source(context, "source", 1, 2)

      :ok = EventStore.link_to_stream("target", :no_stream, [first])

      assert {:error, :stream_exists} = EventStore.link_to_stream("target", :no_stream, [second])

      assert linked_ids(context, "target") == [first]
    end
  end

  # A real `deadlock_detected`. Session X holds an event, the link takes the target's row and
  # waits on the event, then X waits on the target's row. X runs with a long `deadlock_timeout`,
  # so the link's check finds the cycle and the link is the victim.
  describe "losing a deadlock" do
    setup context do
      target_ids = source(context, "target", 2, 1)
      [id] = source(context, "source", 1, 1)

      session_x = hold_event(context, id, "FOR UPDATE")

      LockSession.run(session_x, fn tx ->
        Postgrex.query!(tx, "SET LOCAL deadlock_timeout = '5s'", [])
      end)

      [id: id, session_x: session_x, target_ids: target_ids]
    end

    test "the link is retried and writes once", context do
      %{id: id, session_x: session_x, target_ids: target_ids} = context

      log =
        capture_log(fn ->
          link = async_link(context, "target", :any_version, [id])

          force_deadlock(context, context.linker_backend)

          assert {:ok, :ok} = LockSession.commit(session_x)
          assert :ok = Task.await(link, 15_000)
        end)

      assert log =~ "deadlock_detected"
      assert linked_ids(context, "target") == target_ids ++ [id]
    end

    test "inside a caller's transaction it is answered, not retried", context do
      %{id: id, session_x: session_x, target_ids: target_ids} = context

      caller = LockSession.start(context.postgrex_config)

      link =
        LockSession.start_run(caller, fn tx ->
          EventStore.link_to_stream("target", :any_version, [id], conn: tx)
        end)

      force_deadlock(context, caller.backend)

      assert {:error, %AppendError{class: :retryable, code: :deadlock_detected}} =
               LockSession.await_run(link)

      assert {:error, :rollback} = LockSession.commit(caller)
      assert {:ok, :ok} = LockSession.commit(session_x)

      assert linked_ids(context, "target") == target_ids
    end

    defp force_deadlock(context, victim_backend) do
      %{observer: observer, session_x: session_x, schema: schema} = context

      LockSession.wait_until_blocked(observer, victim_backend, session_x.backend)

      LockSession.lock_row(session_x, schema, stream_id(context, "target"))
    end
  end

  describe "blocked on the target's row" do
    setup context do
      source(context, "target", 2, 1)

      [ids: source(context, "source", 1, 2)]
    end

    test "by an append that moves it: `:wrong_expected_version`", context do
      %{ids: ids} = context

      holder = LockSession.start(context.postgrex_config)

      LockSession.run(holder, fn tx ->
        assert :ok =
                 EventStore.append_to_stream(
                   "target",
                   1,
                   EventFactory.create_events(1),
                   conn: tx
                 )
      end)

      link = async_link(context, "target", 1, ids)
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert {:error, :wrong_expected_version} = Task.await(link)

      assert stream_versions(context, "target") == [1, 2]
      assert stream_version(context, "target") == 2
    end

    test "by an append, at `:any_version`: it numbers after the append", context do
      %{ids: ids} = context

      holder = LockSession.start(context.postgrex_config)

      LockSession.run(holder, fn tx ->
        assert :ok =
                 EventStore.append_to_stream(
                   "target",
                   1,
                   EventFactory.create_events(1),
                   conn: tx
                 )
      end)

      link = async_link(context, "target", :any_version, ids)
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(link)

      assert stream_versions(context, "target") == [1, 2, 3, 4]
    end

    test "by a soft delete: `:stream_deleted`, and nothing lands in it", context do
      %{ids: ids} = context

      target_id = stream_id(context, "target")
      holder = LockSession.start(context.postgrex_config)

      LockSession.run(holder, fn tx ->
        assert :ok = Storage.soft_delete_stream(tx, target_id, schema: context.schema)
      end)

      link = async_link(context, "target", :any_version, ids)
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert {:error, :stream_deleted} = Task.await(link)

      assert stream_version(context, "target") == 1
      assert stream_versions(context, "target") == [1]
    end

    test "by a hard delete: `:not_found`", context do
      %{ids: ids} = context

      holder = hold_hard_delete(context, "target")

      link = async_link(context, "target", :any_version, ids)
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert {:error, :not_found} = Task.await(link)

      assert stream_id(context, "target") == nil
      assert stream_versions(context, "source") == [1, 2]
    end
  end

  describe "the events" do
    test "with one missing: `:not_found`, and a new stream is not left behind", context do
      [first, second] = source(context, "source", 1, 2)

      assert {:error, :not_found} =
               EventStore.link_to_stream("target", :any_version, [first, UUID.uuid4(), second])

      assert stream_id(context, "target") == nil
    end

    test "with one missing: `:not_found`, and an existing stream does not move", context do
      [first, second] = source(context, "source", 1, 2)
      target_ids = source(context, "target", 2, 1)

      assert {:error, :not_found} =
               EventStore.link_to_stream("target", :any_version, [first, UUID.uuid4(), second])

      assert stream_version(context, "target") == 1
      assert linked_ids(context, "target") == target_ids
    end

    test "with a repeated id: `:duplicate_event`, and nothing written", context do
      [first, second] = source(context, "source", 1, 2)

      for ids <- [[first, first], [first, second, first]] do
        assert {:error, :duplicate_event} = EventStore.link_to_stream("target", :any_version, ids)
      end

      assert stream_id(context, "target") == nil
    end

    test "with an id in another case: one event, so `:duplicate_event`", context do
      id = event_id(10, 255)

      :ok = EventStore.append_to_stream("source", 0, [EventFactory.create_event(id)])

      assert {:error, :duplicate_event} =
               EventStore.link_to_stream("target", :any_version, [id, String.upcase(id)])
    end

    test "none: `:ok`, and a stream at version 0", context do
      assert :ok = EventStore.link_to_stream("target", :any_version, [])

      assert stream_version(context, "target") == 0
      assert stream_versions(context, "target") == []
    end

    test "blocked by a hard delete of their stream: `:not_found`", context do
      ids = source(context, "source", 1, 2)

      holder = hold_hard_delete(context, "source")

      link = async_link(context, "target", :any_version, ids)
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert {:error, :not_found} = Task.await(link)

      assert stream_id(context, "target") == nil
      assert count(context, "events") == 0
    end

    test "blocked by a hard delete of some of them: `:not_found`, and nothing written", context do
      [doomed] = source(context, "source", 1, 1)
      [kept] = source(context, "other", 2, 1)
      target_ids = source(context, "target", 3, 1)

      holder = hold_hard_delete(context, "source")

      link = async_link(context, "target", :any_version, [kept, doomed])
      wait_for_link(context, holder)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert {:error, :not_found} = Task.await(link)

      assert stream_version(context, "target") == 1
      assert linked_ids(context, "target") == target_ids
    end
  end

  describe "a link's own lock order" do
    test "is left to the statement: without it the linker's rows come back unsorted", context do
      ids = source(context, "source", 1, 5)

      plan =
        plan_without_order(
          context.linker,
          Statements.lock_link_events(context.schema),
          "ORDER BY event_id",
          [Enum.map(ids, &UUID.string_to_binary!/1)]
        )

      assert_unsorted(plan)
    end

    for {name, order} <- [{"the sorted order", :asc}, {"the reverse of it", :desc}] do
      @order order

      test "is `event_id` order, from a caller's list in #{name}", context do
        ids = source(context, "source", 1, 5)
        [lowest, low, median, high, highest] = sorted = Enum.sort(ids)

        holder = hold_event(context, median, "FOR UPDATE")

        link = async_link(context, "target", :any_version, Enum.sort(sorted, @order))
        wait_for_link(context, holder)

        assert held?(context, lowest, "FOR UPDATE")
        assert held?(context, low, "FOR UPDATE")
        refute held?(context, high, "FOR UPDATE")
        refute held?(context, highest, "FOR UPDATE")

        assert {:ok, :ok} = LockSession.commit(holder)
        assert :ok = Task.await(link)

        # Sorted to lock, and still numbered as the caller listed them.
        assert linked_ids(context, "target") == Enum.sort(sorted, @order)
      end
    end

    test "is one statement over every batch, and numbering stays the caller's", context do
      ids = source(context, "source", 1, 1_500)

      sorted = Enum.sort(ids)
      {_below, [held | above]} = Enum.split(sorted, 1_200)
      shuffled = Enum.shuffle(ids)

      holder = hold_event(context, held, "FOR UPDATE")

      link = async_link(context, "target", :any_version, shuffled)
      wait_for_link(context, holder)

      # Only an order across batches can hold exactly these: the caller's order scatters them.
      assert free_events(context, sorted) == above

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(link)

      assert linked_ids(context, "target") == shuffled
      assert stream_versions(context, "target") == Enum.to_list(1..1_500)
      assert stream_version(context, "target") == 1_500
    end

    test "blocked on the target's row, it holds no event row", context do
      ids = source(context, "source", 1, 2)
      source(context, "target", 2, 1)

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, stream_id(context, "target"))

      link = async_link(context, "target", :any_version, ids)
      wait_for_link(context, holder)

      for id <- ids, do: refute(held?(context, id, "FOR UPDATE"))

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(link)
    end
  end

  describe "a hard delete's lock order" do
    test "is left to the statement: without it `deleter` follows `events`", context do
      assert plan_of_delete(context, :deleter) =~
               ~r/Seq Scan on events e.*Seq Scan on stream_events/s
    end

    test "is left to the statement: without it `second_deleter` follows its stream", context do
      assert plan_of_delete(context, :second_deleter) =~
               ~r/Seq Scan on stream_events.*Seq Scan on events e/s
    end

    test "is `event_id` order, with the stream's scan the reverse of it", context do
      ids = source(context, "stream", 1, 5)
      [lowest, low, median, high, highest] = Enum.sort(ids)

      holder = hold_event(context, median, "FOR KEY SHARE")

      delete = async_delete(context, :deleter, "stream")
      LockSession.wait_until_blocked(context.observer, context.deleter_backend, holder.backend)

      assert held?(context, lowest, "FOR KEY SHARE")
      assert held?(context, low, "FOR KEY SHARE")
      refute held?(context, high, "FOR KEY SHARE")
      refute held?(context, highest, "FOR KEY SHARE")

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(delete)

      assert_all_gone(context)
    end

    test "is the same for a link stream's events, listed the other way", context do
      ids = source(context, "source", 1, 5)
      [lowest, low, median, high, highest] = sorted = Enum.sort(ids)

      :ok = EventStore.link_to_stream("link-stream", :any_version, sorted)

      holder = hold_event(context, median, "FOR KEY SHARE")

      delete = async_delete(context, :deleter, "link-stream")
      LockSession.wait_until_blocked(context.observer, context.deleter_backend, holder.backend)

      assert held?(context, lowest, "FOR KEY SHARE")
      assert held?(context, low, "FOR KEY SHARE")
      refute held?(context, high, "FOR KEY SHARE")
      refute held?(context, highest, "FOR KEY SHARE")

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(delete)

      assert stream_versions(context, "source") == []
      assert count(context, "events") == 0
    end
  end

  # Each is staged on a third session's lock of the median event, so that each side holds all
  # it can before either moves. Either side taking its rows in any other order is a cycle.
  describe "a link and a hard delete of the same events" do
    test "queue on the rows, with no deadlock, and the link's rows go too", context do
      ids = source(context, "stream", 1, 5)
      [_lowest, _low, median, _high, _highest] = Enum.sort(ids)

      holder = hold_event(context, median, "FOR UPDATE")

      # The stream's own order, which is the reverse of the sorted one.
      link = async_link(context, "link-stream", :any_version, ids)
      wait_for_link(context, holder)

      delete = async_delete(context, :deleter, "stream")

      LockSession.wait_until_blocked(
        context.observer,
        context.deleter_backend,
        context.linker_backend
      )

      assert {:ok, :ok} = LockSession.commit(holder)

      assert :ok = Task.await(link)
      assert :ok = Task.await(delete)

      assert stream_versions(context, "link-stream") == []
      assert_all_gone(context)
    end

    test "with the delete ahead: the link answers `:not_found`, with no deadlock", context do
      ids = source(context, "stream", 1, 5)
      [_lowest, _low, median, _high, _highest] = Enum.sort(ids)

      holder = hold_event(context, median, "FOR KEY SHARE")

      delete = async_delete(context, :deleter, "stream")
      LockSession.wait_until_blocked(context.observer, context.deleter_backend, holder.backend)

      # A link retries its one deadlock, and its retry answers this same `:not_found`, so only
      # the log can show that there was none.
      log =
        capture_log(fn ->
          link = async_link(context, "link-stream", :any_version, ids)

          LockSession.wait_until_blocked(
            context.observer,
            context.linker_backend,
            context.deleter_backend
          )

          assert {:ok, :ok} = LockSession.commit(holder)

          assert :ok = Task.await(delete)
          assert {:error, :not_found} = Task.await(link)
        end)

      refute log =~ "deadlock_detected"

      assert stream_id(context, "link-stream") == nil
      assert_all_gone(context)
    end
  end

  # The target's row comes first on both sides. Taking the event rows first here would be a
  # cycle: the link holding the event and wanting the stream, the delete the other way round.
  describe "a link of an event already in its target, against a hard delete of the target" do
    setup context do
      ids = source(context, "stream", 1, 1)
      [id] = ids

      [id: id, holder: hold_event(context, id, "FOR UPDATE")]
    end

    test "the delete first: the link waits on the row, and answers `:not_found`", context do
      %{holder: holder, id: id} = context

      delete = async_delete(context, :deleter, "stream")
      LockSession.wait_until_blocked(context.observer, context.deleter_backend, holder.backend)

      link = async_link(context, "stream", :any_version, [id])

      LockSession.wait_until_blocked(
        context.observer,
        context.linker_backend,
        context.deleter_backend
      )

      assert {:ok, :ok} = LockSession.commit(holder)

      assert :ok = Task.await(delete)
      assert {:error, :not_found} = Task.await(link)

      assert_all_gone(context)
    end

    test "the link first: it answers `:duplicate_event`, and the delete goes through", context do
      %{holder: holder, id: id} = context

      link = async_link(context, "stream", :any_version, [id])
      wait_for_link(context, holder)

      delete = async_delete(context, :deleter, "stream")

      LockSession.wait_until_blocked(
        context.observer,
        context.deleter_backend,
        context.linker_backend
      )

      assert {:ok, :ok} = LockSession.commit(holder)

      assert {:error, :duplicate_event} = Task.await(link)
      assert :ok = Task.await(delete)

      assert_all_gone(context)
    end
  end

  describe "two hard deletes meeting through a link stream" do
    test "queue on the rows, with no deadlock", context do
      ids = source(context, "stream", 1, 5)
      [_lowest, _low, median, _high, _highest] = sorted = Enum.sort(ids)

      # One at a time, as one link's insert does not write its rows in version order, so that
      # this stream's rows run against the events' and each delete's walk against the other's.
      for id <- sorted, do: :ok = EventStore.link_to_stream("link-stream", :any_version, [id])

      holder = hold_event(context, median, "FOR KEY SHARE")

      first = async_delete(context, :deleter, "stream")
      LockSession.wait_until_blocked(context.observer, context.deleter_backend, holder.backend)

      second = async_delete(context, :second_deleter, "link-stream")

      LockSession.wait_until_blocked(
        context.observer,
        context.second_deleter_backend,
        context.deleter_backend
      )

      assert {:ok, :ok} = LockSession.commit(holder)

      assert :ok = Task.await(first)
      assert :ok = Task.await(second)

      assert stream_id(context, "stream") == nil
      assert stream_id(context, "link-stream") == nil
      assert_all_gone(context)
    end

    test "each holding a link of the other's, with no deadlock", context do
      # `right` first, so `left`'s own rows are the last `deleter` would come to, and the first
      # `second_deleter` would.
      [right_low, right_high] = Enum.sort(source(context, "right", 2, 2))
      [left_low, left_high] = Enum.sort(source(context, "left", 1, 2))

      :ok = EventStore.link_to_stream("left", :any_version, [right_low, right_high])
      :ok = EventStore.link_to_stream("right", :any_version, [left_low, left_high])

      holder = hold_event(context, right_low, "FOR KEY SHARE")

      first = async_delete(context, :deleter, "right")
      LockSession.wait_until_blocked(context.observer, context.deleter_backend, holder.backend)

      second = async_delete(context, :second_deleter, "left")

      LockSession.wait_until_blocked(
        context.observer,
        context.second_deleter_backend,
        context.deleter_backend
      )

      assert {:ok, :ok} = LockSession.commit(holder)

      assert :ok = Task.await(first)
      assert :ok = Task.await(second)

      assert_all_gone(context)
    end
  end

  defp read_in_physical_order(conn, more \\ []) do
    for setting <- ["indexscan", "indexonlyscan", "bitmapscan" | more] do
      Postgrex.query!(conn, "SET enable_#{setting} TO off", [])
    end
  end

  # The plan a mutation that drops the `ORDER BY` would get, in this session.
  defp plan_without_order(conn, statement, order_by, params) do
    statement = IO.iodata_to_binary(statement)

    %Postgrex.Result{rows: rows} =
      Postgrex.query!(conn, "EXPLAIN " <> String.replace(statement, order_by, ""), params)

    Enum.map_join(rows, "\n", &hd/1)
  end

  # A hash join builds on the side the planner estimates smaller, and no `enable_*` setting picks
  # that side. The estimates come from `pg_statistic`, which autoanalyze writes in its own time
  # and a TRUNCATE leaves behind, so a sample from when one stream held half of `stream_events`
  # turns `deleter`'s walk around. The guard analyzes what it plans over instead: `padding` puts
  # `stream`'s 5 rows well below `events`' 25, so `stream_events` is the hashed side.
  defp plan_of_delete(context, session) do
    source(context, "stream", 1, 5)
    source(context, "padding", 2, 20)

    for table <- ["events", "stream_events"] do
      Postgrex.query!(context.observer, ~s(ANALYZE "#{context.schema}".#{table}), [])
    end

    plan =
      plan_without_order(
        Map.fetch!(context, session),
        Statements.lock_deleted_events(context.schema),
        "ORDER BY e.event_id",
        [stream_id(context, "stream")]
      )

    assert_unsorted(plan)

    plan
  end

  defp assert_unsorted(plan) do
    assert plan =~ ~r/Seq Scan on \S*events\b/, plan
    refute plan =~ ~r/Index|Bitmap|Sort|Merge|Unique/, plan
  end

  # A stream of `count` events, committed, whose ids fall as its versions rise. Answers the
  # ids in stream order. `prefix` keeps one stream's ids apart from another's.
  defp source(context, stream_uuid, prefix, count) do
    ids = for rank <- count..1//-1, do: event_id(prefix, rank)

    events =
      for {id, number} <- Enum.with_index(ids, 1), do: EventFactory.create_event(id, number)

    :ok = EventStore.append_to_stream(stream_uuid, :any_version, events, conn: context.observer)

    ids
  end

  defp event_id(prefix, rank) do
    [prefix, rank]
    |> then(&:io_lib.format("~8.16.0b-0000-4000-8000-~12.16.0b", &1))
    |> IO.iodata_to_binary()
  end

  defp async_link(context, stream_uuid, expected_version, ids) do
    %{linker: linker} = context

    Task.async(fn ->
      EventStore.link_to_stream(stream_uuid, expected_version, ids, conn: linker)
    end)
  end

  defp wait_for_link(context, %LockSession{backend: holder}) do
    LockSession.wait_until_blocked(context.observer, context.linker_backend, holder)
  end

  defp async_delete(context, conn, stream_uuid) do
    conn = Map.fetch!(context, conn)

    Task.async(fn ->
      Stream.delete(conn, stream_uuid, :any_version, :hard,
        schema: context.schema,
        enable_hard_deletes: true
      )
    end)
  end

  # A session that has inserted the stream's row and not committed, so a second creator waits
  # on the unique index.
  defp hold_new_stream(context, stream_uuid) do
    holder = LockSession.start(context.postgrex_config)

    LockSession.run(holder, fn tx ->
      assert {:ok, _stream_id} = CreateStream.execute(tx, stream_uuid, schema: context.schema)
    end)

    holder
  end

  defp hold_hard_delete(context, stream_uuid) do
    holder = LockSession.start(context.postgrex_config)
    stream_id = stream_id(context, stream_uuid)

    LockSession.run(holder, fn tx ->
      Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

      assert :ok = Storage.hard_delete_stream(tx, stream_id, schema: context.schema)
    end)

    holder
  end

  defp hold_event(context, event_id, lock) do
    holder = LockSession.start(context.postgrex_config)

    assert %Postgrex.Result{num_rows: 1} =
             LockSession.run(holder, fn tx ->
               Postgrex.query!(
                 tx,
                 ~s(SELECT 1 FROM "#{context.schema}".events WHERE event_id = $1 #{lock}),
                 [UUID.string_to_binary!(event_id)]
               )
             end)

    holder
  end

  # NOWAIT from the observer, in autocommit, so a free row is taken and let go at once.
  defp held?(context, event_id, lock) do
    case Postgrex.query(
           context.observer,
           ~s(SELECT 1 FROM "#{context.schema}".events WHERE event_id = $1 #{lock} NOWAIT),
           [UUID.string_to_binary!(event_id)]
         ) do
      {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} -> true
      {:ok, %Postgrex.Result{num_rows: 1}} -> false
    end
  end

  # The ids of `event_ids` that no one holds, in `event_id` order.
  defp free_events(context, event_ids) do
    context
    |> rows(
      ~s|SELECT event_id FROM "#{context.schema}".events WHERE event_id = ANY($1::uuid[])| <>
        " ORDER BY event_id FOR UPDATE SKIP LOCKED",
      [Enum.map(event_ids, &UUID.string_to_binary!/1)]
    )
    |> Enum.map(fn [event_id] -> UUID.binary_to_string!(event_id) end)
  end

  # The ids at the stream's versions, in version order, read straight from `stream_events`.
  defp linked_ids(context, stream_uuid) do
    context
    |> rows(
      ~s(SELECT se.event_id FROM "#{context.schema}".stream_events se) <>
        ~s[ JOIN "#{context.schema}".streams s USING (stream_id)] <>
        " WHERE s.stream_uuid = $1 ORDER BY se.stream_version",
      [stream_uuid]
    )
    |> Enum.map(fn [event_id] -> UUID.binary_to_string!(event_id) end)
  end

  defp assert_all_gone(context) do
    assert all_positions(context) == []
    assert count(context, "events") == 0
    assert count(context, "event_tags") == 0
    assert count(context, "event_types") == 0
    assert count(context, "stream_events") == 0
  end
end
