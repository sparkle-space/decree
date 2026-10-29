defmodule Decree.Store.AdvisoryLockSessionTest do
  # Regression tests for advisory locks lost with their database session
  # (sparkle-space/sparklespace#7543; upstream commanded/eventstore#330 and #331).
  #
  # They use only the store's subscription messages, `AdvisoryLocks` and SQL, so
  # they run unchanged against the code before the fix.
  use Decree.Store.StorageCase

  alias Decree.Store.{AdvisoryLocks, EventFactory, TcpProxy, UUID}

  describe "lock session ends and the lock connection reconnects (#330)" do
    setup do
      start_supervised!({TestEventStore, name: :lock_node_a, schema: "public"}, id: :lock_node_a)
      start_supervised!({TestEventStore, name: :lock_node_b, schema: "public"}, id: :lock_node_b)

      :ok
    end

    test "the instance that lost its lock stops delivering, and the other takes over", %{
      conn: conn
    } do
      subscription_name = UUID.uuid4()

      # Node A subscribes first and takes the lock. It would retry its lock only
      # after a minute, so after the loss node B (retrying every 200ms) takes it.
      subscriber_a = start_subscriber(:a)

      {:ok, subscription_a} =
        TestEventStore.subscribe_to_all_streams(subscription_name, subscriber_a,
          name: :lock_node_a,
          retry_interval: 60_000
        )

      assert_receive {:a, {:subscribed, ^subscription_a}}

      subscriber_b = start_subscriber(:b)

      {:ok, subscription_b} =
        TestEventStore.subscribe_to_all_streams(subscription_name, subscriber_b,
          name: :lock_node_b,
          retry_interval: 200
        )

      refute_receive {:b, {:subscribed, ^subscription_b}}, 500

      append_events(:lock_node_a, 1)

      assert_receive {:a, {:events, [%{event_number: 1}]}}
      refute_receive {:b, {:events, _events}}

      # End node A's lock session. Its lock pool reconnects on a new session and
      # lives on.
      subscription_id = subscription_id(conn, subscription_name)
      backend_pid = lock_backend_pid(conn, subscription_id)

      terminate_backend(conn, backend_pid)

      assert_receive {:a, {:subscription_lock_lost, ^subscription_a}}, 5_000
      assert_receive {:b, {:subscribed, ^subscription_b}}, 5_000

      refute lock_backend_pid(conn, subscription_id) == backend_pid

      append_events(:lock_node_a, 3)

      # Every event is delivered once in total, by node B.
      delivered = receive_events(3)

      assert Enum.sort(delivered) == [{:b, 2}, {:b, 3}, {:b, 4}]
      refute_receive {_node, {:events, _events}}, 1_000
    end
  end

  describe "lock pool dies while the database refuses new connections (#331)" do
    setup %{config: config} do
      proxy =
        start_supervised!(
          {TcpProxy, upstream: {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
        )

      # The lock (and notification) connections go through the proxy; the main
      # pool connects directly, so the subscription row is still created while the
      # proxy refuses.
      start_supervised!(
        {TestEventStore,
         name: :lock_proxied,
         schema: "public",
         session_mode_pool: [hostname: "127.0.0.1", port: TcpProxy.port(proxy)],
         advisory_locks_restart_delay: 1_500},
        id: :lock_proxied
      )

      [proxy: proxy, locks: Module.concat([:lock_proxied, AdvisoryLocks])]
    end

    test "lock calls answer while refused, and the subscription takes its lock back", %{
      conn: conn,
      proxy: proxy,
      locks: locks
    } do
      subscription_name = UUID.uuid4()

      {:ok, subscription} =
        TestEventStore.subscribe_to_all_streams(subscription_name, self(), name: :lock_proxied)

      assert_receive {:subscribed, ^subscription}

      subscription_ref = Process.monitor(subscription)

      :ok = TcpProxy.refuse(proxy)

      terminate_backend(conn, lock_backend_pid(conn, subscription_id(conn, subscription_name)))

      # The lock pool's reconnects are refused, so it stops, and is started again
      # every 1.5s and stops again. Meanwhile the subscription retries its lock
      # every second. Every lock call must answer, with an error.
      assert_lock_calls_fail(locks, 424_242, System.monotonic_time(:millisecond) + 5_000)

      :ok = TcpProxy.forward(proxy)

      # Once the pool is back, lock calls succeed and the subscription takes its
      # lock again.
      assert_lock_call_succeeds(locks, 424_243, 10_000)
      assert_receive {:subscribed, ^subscription}, 10_000

      refute_received {:DOWN, ^subscription_ref, :process, _pid, _reason}
    end
  end

  # A lock call made from a task, given `bound` milliseconds to answer.
  defp bounded_lock_call(locks, key, bound) do
    task = Task.async(fn -> AdvisoryLocks.try_advisory_lock(locks, key, bound + 1_000) end)

    case Task.yield(task, bound) || Task.shutdown(task, :brutal_kill) do
      {:ok, reply} -> reply
      {:exit, reason} -> flunk("lock call exited: " <> inspect(reason))
      nil -> flunk("lock call did not answer within #{bound}ms")
    end
  end

  # Until `deadline`, every lock call answers within three seconds, with an error.
  defp assert_lock_calls_fail(locks, key, deadline) do
    if System.monotonic_time(:millisecond) < deadline do
      assert {:error, _reason} = bounded_lock_call(locks, key, 3_000)

      Process.sleep(100)
      assert_lock_calls_fail(locks, key, deadline)
    end
  end

  defp assert_lock_call_succeeds(locks, key, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    case bounded_lock_call(locks, key, 3_000) do
      {:ok, lock} ->
        lock

      {:error, _reason} = reply ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(100)
          assert_lock_call_succeeds(locks, key, deadline - System.monotonic_time(:millisecond))
        else
          flunk("lock call still failing: " <> inspect(reply))
        end
    end
  end

  # A subscriber that acknowledges every batch of events and forwards every message
  # to the test process, tagged.
  defp start_subscriber(tag) do
    test = self()

    spawn_link(fn -> forward_messages(tag, test, nil) end)
  end

  defp forward_messages(tag, test, subscription) do
    receive do
      {:subscribed, subscription} = message ->
        send(test, {tag, message})
        forward_messages(tag, test, subscription)

      {:events, events} = message ->
        :ok = TestEventStore.ack(subscription, events)
        send(test, {tag, message})
        forward_messages(tag, test, subscription)

      message ->
        send(test, {tag, message})
        forward_messages(tag, test, subscription)
    end
  end

  defp receive_events(count, delivered \\ [])

  defp receive_events(count, delivered) when length(delivered) >= count, do: delivered

  defp receive_events(count, delivered) do
    assert_receive {node, {:events, events}}

    receive_events(count, delivered ++ Enum.map(events, &{node, &1.event_number}))
  end

  defp append_events(event_store_name, count) do
    :ok =
      TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(count),
        name: event_store_name
      )
  end

  defp subscription_id(conn, subscription_name) do
    %Postgrex.Result{rows: [[subscription_id]]} =
      Postgrex.query!(
        conn,
        "SELECT subscription_id FROM subscriptions WHERE subscription_name = $1",
        [subscription_name]
      )

    subscription_id
  end

  # The backend process id of the session holding the subscription's advisory lock.
  defp lock_backend_pid(conn, subscription_id) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT pid FROM pg_locks
        WHERE locktype = 'advisory'
          AND granted
          AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND classid = 'public.subscriptions'::regclass::oid
          AND objid::bigint = $1::bigint
          AND objsubid = 2
        """,
        [subscription_id]
      )

    case rows do
      [[pid]] -> pid
      [] -> nil
    end
  end

  defp terminate_backend(conn, backend_pid) when is_integer(backend_pid) do
    %Postgrex.Result{rows: [[true]]} =
      Postgrex.query!(conn, "SELECT pg_terminate_backend($1)", [backend_pid])

    :ok
  end
end
