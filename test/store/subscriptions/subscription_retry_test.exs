defmodule Decree.Store.Subscriptions.SubscriptionRetryTest do
  # A failed subscribe attempt is retried after a wait drawn between half of and the whole
  # of a backoff from a second, doubling up to the retry interval, while a lock that another
  # instance holds keeps the full interval, silently (sparkle-space/sparklespace#7591,
  # the jitter sparkle-space/sparklespace#8154).
  #
  # The lock connection goes through a TCP proxy, and the main pool connects directly, so
  # the subscription row is created whatever the proxy does. A store does not finish
  # starting until its notification connection, on the same proxy, is up, so most tests
  # take the lock connection down after that: the pool is killed with the proxy in
  # `:hold`, and the pool started in its place stays in its connect until the test moves
  # on. That is the state of a boot race, a live pool with no connection, set by the test
  # rather than won in a race. `subscribe_to_all_streams` returns after the first
  # attempt, which makes that the point to move on from.
  #
  # Attempts are seen by tracing the `{:subscribe_to_stream, token}` messages the
  # subscription receives, with their timestamps. `:sys.get_state/1` after one is the
  # barrier that the attempt has run. A test that needs an attempt sooner than the wait in
  # front of it sends the pending timer's message itself. The test of a message that must
  # not attempt traces the attempt's call instead, since that message is received too.
  use Decree.Store.StorageCase

  import Decree.Store.SubscriptionHelpers, only: [attempt_subscribe: 1]
  import ExUnit.CaptureLog

  alias Decree.Store.{AdvisoryLocks, MonitoredServer, RecordedEvent, TcpProxy, UUID, Wait}
  alias Decree.Store.Subscriptions.{Subscription, SubscriptionFsm, SubscriptionState}

  # The full wait, in the tests that prove a retry does not take it.
  @full_interval 60_000

  # The bound of a wait that must end soon: well short of the full wait, and far above
  # the seconds it takes, so a loaded host does not reach it.
  @soon 15_000

  # A wait measured between two attempts may read this much short of the timer's value
  # (timer and clock granularity).
  @early_tolerance 50

  @proxied :retry_proxied

  # An event store with no lock pool running.
  @no_lock_pool __MODULE__.NoLockPool

  describe "a lock pool that connects late at boot" do
    # The lock pool is the first of the store's two connections through the proxy, and
    # its delay outlasts the rest of the start. Should the order ever flip, the
    # notification connection takes the delay and this test proves less; the next
    # describe does not depend on the order.
    setup %{config: config} do
      proxy = start_proxy(config)

      :ok = TcpProxy.delay_next(proxy, 500)

      start_proxied_store(proxy)

      :ok
    end

    test "the subscription is subscribed within seconds, not after its full interval" do
      {:ok, subscription} = subscribe(@proxied, UUID.uuid4(), @full_interval)

      assert_receive {:subscribed, ^subscription}, @soon
    end
  end

  describe "a lock connection that is not up" do
    setup [:start_proxied_store_and_await_lock, :take_lock_connection_down]

    test "the subscription is subscribed within seconds of it coming up", %{proxy: proxy} do
      {:ok, subscription} = subscribe(@proxied, UUID.uuid4(), @full_interval)

      refute_received {:subscribed, ^subscription}

      :ok = TcpProxy.forward(proxy)

      assert_receive {:subscribed, ^subscription}, @soon
    end

    test "a run of refused attempts logs one warning, which names the reason" do
      log =
        capture_log([level: :warning], fn ->
          {:ok, subscription} = subscribe(@proxied, UUID.uuid4(), @full_interval)

          trace(subscription)

          first = assert_attempt(subscription)
          second = assert_attempt(subscription)

          # The wait after the second failure is drawn over a two-second backoff, so it is
          # at least half of it.
          assert to_ms(second - first) >= 1_000 - @early_tolerance

          assert %SubscriptionState{subscribe_failures: failures} = data(subscription)
          assert failures >= 3
        end)

      assert [_warning] = Regex.scan(~r/failed to subscribe/, log)
      assert log =~ "connection not available and queuing is disabled"
    end

    test "a success resets the backoff, so the next failure waits the shortest", %{proxy: proxy} do
      {:ok, subscription} = subscribe(@proxied, UUID.uuid4(), @full_interval)

      # The first attempt was refused; this is a second. Its own timer can have fired as
      # well on a host that stalled for half a second.
      attempt_subscribe(subscription)

      assert %SubscriptionState{subscribe_failures: failures} = failed = data(subscription)
      assert failures >= 2
      # Drawn over a backoff of at least two seconds, so at least half of it.
      assert SubscriptionFsm.resubscribe_wait(failed) >= 1_000

      :ok = TcpProxy.forward(proxy)

      assert_receive {:subscribed, ^subscription}, @soon
      assert %SubscriptionState{subscribe_failures: 0} = data(subscription)

      :ok = TcpProxy.refuse(proxy)
      kill_lock_pool()

      assert_receive {:subscription_lock_lost, ^subscription}, @soon

      attempt_subscribe(subscription)

      assert %SubscriptionState{subscribe_failures: 1} = failed = data(subscription)
      # Drawn over the shortest backoff, a second, so between half of it and all of it.
      assert SubscriptionFsm.resubscribe_wait(failed) in 500..1_000
    end
  end

  describe "a lost lock" do
    setup [:start_proxied_store_and_await_lock]

    test "while the lock pool is refused, a failed attempt retries soon and takes the lock", %{
      proxy: proxy
    } do
      {:ok, subscription} = subscribe(@proxied, UUID.uuid4(), @full_interval)

      assert_receive {:subscribed, ^subscription}, @soon

      :ok = TcpProxy.refuse(proxy)
      kill_lock_pool()

      assert_receive {:subscription_lock_lost, ^subscription}, @soon

      # Only an attempt that failed retries soon: the first wait is the full interval.
      assert SubscriptionFsm.resubscribe_wait(data(subscription)) == @full_interval

      trace(subscription)

      # The attempt that wait leads to, a minute sooner.
      attempt_subscribe(subscription)

      assert_attempt(subscription)
      assert %SubscriptionState{subscribe_failures: 1} = data(subscription)

      # The timer armed by the refusal, which replaced the one a minute away.
      assert_attempt(subscription)

      :ok = TcpProxy.forward(proxy)

      assert_receive {:subscribed, ^subscription}, @soon
      assert %SubscriptionState{subscribe_failures: 0} = data(subscription)
    end

    test "a refused attempt by a connecting subscriber replaces the full wait", %{proxy: proxy} do
      subscription_name = UUID.uuid4()

      {:ok, subscription} =
        subscribe(@proxied, subscription_name, @full_interval, concurrency_limit: 2)

      assert_receive {:subscribed, ^subscription}, @soon

      :ok = TcpProxy.refuse(proxy)
      kill_lock_pool()

      assert_receive {:subscription_lock_lost, ^subscription}, @soon

      trace(subscription)

      # The timer pending here is a minute away. Connecting attempts at once.
      subscriber = spawn_link(fn -> Process.sleep(:infinity) end)

      {:ok, ^subscription} =
        TestEventStore.subscribe_to_all_streams(subscription_name, subscriber,
          name: @proxied,
          retry_interval: @full_interval,
          concurrency_limit: 2
        )

      assert %SubscriptionState{subscribe_failures: 1} = data(subscription)

      assert_attempt(subscription)
    end
  end

  describe "a lock another instance holds" do
    @standby_interval 3_000

    setup do
      start_supervised!({TestEventStore, name: :retry_holder, schema: "public"},
        id: :retry_holder
      )

      start_supervised!({TestEventStore, name: :retry_standby, schema: "public"},
        id: :retry_standby
      )

      subscription_name = UUID.uuid4()

      {:ok, holder} = subscribe(:retry_holder, subscription_name, @full_interval)

      assert_receive {:subscribed, ^holder}, @soon

      await_lock_connection(:retry_standby)

      [subscription_name: subscription_name]
    end

    test "the standby waits its full interval between attempts, and logs nothing", %{
      subscription_name: subscription_name
    } do
      log =
        capture_log([level: :warning], fn ->
          connected_at = System.monotonic_time()

          {:ok, standby} = subscribe(:retry_standby, subscription_name, @standby_interval)

          trace(standby)

          first = assert_attempt(standby)
          second = assert_attempt(standby)

          assert to_ms(first - connected_at) >= @standby_interval
          assert to_ms(second - first) >= @standby_interval - @early_tolerance

          assert %SubscriptionState{subscribe_failures: 0} = data(standby)
        end)

      refute log =~ "failed to subscribe"
    end

    test "passes through a parked state leave one timer pending", %{
      subscription_name: subscription_name
    } do
      {:ok, standby} =
        subscribe(:retry_standby, subscription_name, @standby_interval, concurrency_limit: 3)

      timer = subscribe_timer(standby)

      trace(standby)

      for _pass <- 1..3 do
        send(standby, {:events, [%RecordedEvent{event_number: 1}]})
      end

      for _subscriber <- 1..2 do
        subscriber = spawn_link(fn -> Process.sleep(:infinity) end)

        {:ok, ^standby} =
          TestEventStore.subscribe_to_all_streams(subscription_name, subscriber,
            name: :retry_standby,
            retry_interval: @standby_interval,
            concurrency_limit: 3
          )
      end

      # The wait of each pass ends later than the pending timer, which stays as it is.
      assert subscribe_timer(standby) == timer

      assert_attempt(standby)

      # A timer per pass would fire here, all at about the same time.
      refute_receive {:trace_ts, ^standby, :receive, {:subscribe_to_stream, _token}, _at},
                     @standby_interval - 1_000
    end
  end

  describe "a timer pending when the subscription stops being parked" do
    @pending_interval 3_000

    setup do
      start_supervised!({TestEventStore, name: :retry_holder, schema: "public"},
        id: :retry_holder
      )

      start_supervised!({TestEventStore, name: :retry_standby, schema: "public"},
        id: :retry_standby
      )

      await_lock_connection(:retry_standby)

      :ok
    end

    test "is cancelled, so a lock lost later still waits the full interval", %{conn: conn} do
      subscription_name = UUID.uuid4()

      {:ok, holder} = subscribe(:retry_holder, subscription_name, @full_interval)

      assert_receive {:subscribed, ^holder}, @soon

      {:ok, standby} =
        subscribe(:retry_standby, subscription_name, @pending_interval, concurrency_limit: 2)

      stop_supervised!(:retry_holder)

      Wait.until(@soon, fn -> assert advisory_locks(conn) == 0 end)

      # A second subscriber connects, and its attempt at once takes the lock.
      subscriber = spawn_link(fn -> Process.sleep(:infinity) end)

      {:ok, ^standby} =
        TestEventStore.subscribe_to_all_streams(subscription_name, subscriber,
          name: :retry_standby,
          retry_interval: @pending_interval,
          concurrency_limit: 2
        )

      assert_receive {:subscribed, ^standby}, @soon

      trace(standby)

      %SubscriptionState{lock_ref: lock_ref} = data(standby)

      lost_at = System.monotonic_time()

      send(standby, {AdvisoryLocks, :lock_released, lock_ref, :shutdown})

      assert to_ms(assert_attempt(standby) - lost_at) >= @pending_interval
    end
  end

  describe "a retry message from a timer no longer pending" do
    test "leaves the pending timer as it is and makes no attempt" do
      {:ok, subscription} = subscribe(TestEventStore, UUID.uuid4(), @full_interval)

      assert_receive {:subscribed, ^subscription}, @soon

      %SubscriptionState{lock_ref: lock_ref} = data(subscription)

      send(subscription, {AdvisoryLocks, :lock_released, lock_ref, :shutdown})

      assert_receive {:subscription_lock_lost, ^subscription}, @soon

      {ref, _due_at, token} = timer = subscribe_timer(subscription)

      trace_attempts(subscription)

      send(subscription, {:subscribe_to_stream, make_ref()})

      assert subscribe_timer(subscription) == timer
      assert is_integer(Process.read_timer(ref))
      refute_attempted(subscription)

      # The pending timer's message does attempt, so the trace above would have seen one.
      send(subscription, {:subscribe_to_stream, token})

      assert_receive {:trace, ^subscription, :call, {SubscriptionFsm, :subscribe, _args}}, @soon
      assert_receive {:subscribed, ^subscription}, @soon
    end
  end

  describe "a failed subscription row query" do
    test "takes the short retry too", %{postgrex_config: postgrex_config} do
      lock = lock_subscriptions_table(postgrex_config)

      # The row query times out on the table lock, ahead of any lock attempt.
      {:ok, subscription} =
        TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self(),
          timeout: 100,
          retry_interval: @full_interval
        )

      assert %SubscriptionState{subscribe_failures: failures} = data(subscription)
      assert failures >= 1

      release(lock)

      assert_receive {:subscribed, ^subscription}, @soon
    end
  end

  describe "resubscribe_wait/1" do
    # Every attempt here fails for real: its row is created, and its lock call finds no lock
    # pool running for its event store (`failing_fsm/2`), a failed attempt like any other.
    test "is the full interval until an attempt failed", context do
      for retry_interval <- [@full_interval, 200] do
        data = failing_fsm(context, retry_interval).data

        assert SubscriptionFsm.resubscribe_wait(data) == retry_interval
      end
    end

    test "after the nth failed attempt, is drawn between half of and the whole capped backoff",
         context do
      # A sub-second interval caps the backoff at itself from the first failure, and the
      # wait is drawn over it all the same.
      for retry_interval <- [1, 200, 1_000, @full_interval] do
        runs = for _run <- 1..30, do: failed_waits(context, retry_interval, 8)

        for {waits, attempt} <- Enum.with_index(Enum.zip_with(runs, & &1), 1) do
          base = min(min(1_000, retry_interval) * Integer.pow(2, attempt - 1), retry_interval)

          for wait <- waits do
            assert wait >= max(1, div(base, 2)),
                   "a #{wait}ms wait after failure #{attempt} is under half of #{base}ms"

            assert wait <= max(1, base),
                   "a #{wait}ms wait after failure #{attempt} is over #{base}ms"
          end

          # Only a one-millisecond backoff has a single wait; any other gives at least two,
          # and 30 draws of two values are all equal about once in 2^29 runs.
          if base > 1 do
            assert length(Enum.uniq(waits)) > 1,
                   "every wait after failure #{attempt} of #{retry_interval}ms was " <>
                     "#{hd(waits)}ms: the wait is not jittered"
          end
        end
      end
    end

    test "is the wait the warning names, on every pass until the next attempt", context do
      fsm = failing_fsm(context, @full_interval)

      {fsm, log} = with_log([level: :warning], fn -> SubscriptionFsm.subscribe(fsm) end)

      assert %SubscriptionFsm{data: %SubscriptionState{subscribe_failures: 1} = data} = fsm
      assert [_line, logged] = Regex.run(~r/retrying in (\d+)ms/, log)

      wait = SubscriptionFsm.resubscribe_wait(data)

      assert String.to_integer(logged) == wait
      assert SubscriptionFsm.resubscribe_wait(data) == wait
    end
  end

  # A new subscription FSM for `$all` whose every subscribe attempt fails.
  defp failing_fsm(context, retry_interval) do
    %{conn: conn, schema: schema, serializer: serializer} = context

    SubscriptionFsm.new("$all", UUID.uuid4(),
      conn: conn,
      event_store: @no_lock_pool,
      serializer: serializer,
      schema: schema,
      retry_interval: retry_interval
    )
  end

  # The wait after each of `attempts` consecutive failed attempts of one subscription.
  defp failed_waits(context, retry_interval, attempts) do
    fsm = failing_fsm(context, retry_interval)

    {{waits, _fsm}, _log} =
      with_log([level: :warning], fn ->
        Enum.map_reduce(1..attempts, fsm, fn _attempt, fsm ->
          %SubscriptionFsm{state: :initial, data: data} = fsm = SubscriptionFsm.subscribe(fsm)

          {SubscriptionFsm.resubscribe_wait(data), fsm}
        end)
      end)

    waits
  end

  defp start_proxy(config) do
    start_supervised!(
      {TcpProxy, upstream: {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)}}
    )
  end

  defp start_proxied_store(proxy) do
    start_supervised!(
      {TestEventStore,
       name: @proxied,
       schema: "public",
       session_mode_pool: [hostname: "127.0.0.1", port: TcpProxy.port(proxy)],
       advisory_locks_restart_delay: 50},
      id: @proxied
    )
  end

  defp start_proxied_store_and_await_lock(%{config: config}) do
    proxy = start_proxy(config)

    start_proxied_store(proxy)
    await_lock_connection(@proxied)

    [proxy: proxy]
  end

  # Until a new lock pool runs, with its connection held in its connect.
  defp take_lock_connection_down(%{proxy: proxy}) do
    :ok = TcpProxy.hold(proxy)

    pool = kill_lock_pool()

    Wait.until(@soon, fn -> assert lock_pool() not in [nil, pool] end)

    :ok
  end

  defp subscribe(event_store, subscription_name, retry_interval, opts \\ []) do
    TestEventStore.subscribe_to_all_streams(
      subscription_name,
      self(),
      [name: event_store, retry_interval: retry_interval] ++ opts
    )
  end

  # Until the instance's lock pool has its connection.
  defp await_lock_connection(event_store) do
    locks = Module.concat([event_store, AdvisoryLocks])

    Wait.until(@soon, fn ->
      assert {:ok, _locks} = AdvisoryLocks.subscription_locks(locks, [0])
    end)
  end

  # Ends the lock session, as a lost connection does. The pool is started again after
  # its restart delay; while the proxy refuses, it stops again.
  defp kill_lock_pool do
    pool = lock_pool()

    Process.exit(pool, :kill)

    pool
  end

  defp lock_pool do
    [@proxied, AdvisoryLocks, Postgrex]
    |> Module.concat()
    |> MonitoredServer.whereis()
  end

  defp data(subscription) do
    %Subscription{subscription: %SubscriptionFsm{data: %SubscriptionState{} = data}} =
      :sys.get_state(subscription)

    data
  end

  defp subscribe_timer(subscription), do: :sys.get_state(subscription).subscribe_timer

  defp advisory_locks(conn) do
    %Postgrex.Result{rows: [[count]]} =
      Postgrex.query!(conn, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'", [])

    count
  end

  defp trace(subscription) do
    :erlang.trace(subscription, true, [:receive, :monotonic_timestamp])
  end

  defp assert_attempt(subscription) do
    assert_receive {:trace_ts, ^subscription, :receive, {:subscribe_to_stream, _token}, at},
                   @soon

    at
  end

  defp trace_attempts(subscription) do
    attempt = {SubscriptionFsm, :subscribe, 1}

    :erlang.trace_pattern(attempt, true, [:global])
    on_exit(fn -> :erlang.trace_pattern(attempt, false, [:global]) end)

    :erlang.trace(subscription, true, [:call])
  end

  # `:erlang.trace_delivered/1` is the barrier that any call traced so far has arrived.
  defp refute_attempted(subscription) do
    delivered = :erlang.trace_delivered(subscription)

    assert_receive {:trace_delivered, ^subscription, ^delivered}

    refute_received {:trace, ^subscription, :call, {SubscriptionFsm, :subscribe, _args}}
  end

  defp to_ms(native), do: System.convert_time_unit(native, :native, :millisecond)

  # Hold an ACCESS EXCLUSIVE lock on `subscriptions` from a second connection until
  # `release/1`. The holder is linked to the test, so the lock is also released when the
  # test exits.
  defp lock_subscriptions_table(postgrex_config) do
    test = self()

    holder =
      spawn_link(fn ->
        {:ok, conn} = Postgrex.start_link(postgrex_config)

        Postgrex.transaction(
          conn,
          fn transaction ->
            Postgrex.query!(
              transaction,
              ~s(LOCK TABLE "public".subscriptions IN ACCESS EXCLUSIVE MODE),
              []
            )

            send(test, {:locked, self()})

            receive do
              :release -> :ok
            end
          end,
          timeout: :infinity
        )

        send(test, {:released, self()})
      end)

    assert_receive {:locked, ^holder}

    holder
  end

  defp release(holder) do
    send(holder, :release)

    assert_receive {:released, ^holder}
  end
end
