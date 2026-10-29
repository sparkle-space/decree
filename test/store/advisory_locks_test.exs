defmodule Decree.Store.AdvisoryLocksTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{AdvisoryLocks, Config, MonitoredServer, ProcessHelper, Wait}
  alias Decree.Store.AdvisoryLocks.Lock
  alias Decree.Store.Storage

  @locks TestEventStore.Decree.Store.AdvisoryLocks
  @conn TestEventStore.Decree.Store.AdvisoryLocks.Postgrex

  setup do
    postgrex_config = Config.parsed(TestEventStore, :decree) |> Config.default_postgrex_opts()

    conn = start_supervised!({Postgrex, postgrex_config})

    [conn: conn]
  end

  describe "acquire lock" do
    test "should acquire lock when available" do
      assert {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert is_reference(lock)
    end

    test "should acquire lock when same process already has lock" do
      assert {:ok, lock1} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert {:ok, lock2} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert {:ok, lock3} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      assert is_reference(lock1)
      assert is_reference(lock2)
      assert is_reference(lock3)
    end

    test "should fail to acquire lock when already taken", %{conn: conn, schema: schema} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      assert {:error, :lock_already_taken} = AdvisoryLocks.try_advisory_lock(@locks, 1)
    end
  end

  describe "release lock" do
    test "should release lock when process terminates", %{conn: conn, schema: schema} do
      reply_to = self()

      pid =
        spawn_link(fn ->
          {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

          send(reply_to, :lock_acquired)

          # Wait until shutdown
          receive do
            :shutdown -> :ok
          end
        end)

      assert_receive :lock_acquired

      assert {:error, :lock_already_taken} =
               Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)

      send(pid, :shutdown)

      # Wait for lock to be released after process terminates
      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 1, schema: schema)
      end)
    end
  end

  describe "disconnect" do
    test "should send `lock_released` message" do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)

      shutdown_database_connection()

      assert_receive({AdvisoryLocks, :lock_released, ^lock, :shutdown})
    end
  end

  defmodule ScriptedMonitoredServer do
    # Stands in for the lock pool's MonitoredServer: `whereis` answers the scripted
    # pids in turn, repeating the last one.
    use GenServer

    alias Decree.Store.MonitoredServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, [], opts)

    def script(server, pids), do: GenServer.call(server, {:script, pids})

    def init(pids), do: {:ok, pids}

    def handle_call({:script, pids}, _from, _pids), do: {:reply, :ok, pids}

    def handle_call({MonitoredServer, :monitor, _pid}, _from, pids),
      do: {:reply, {:ok, make_ref()}, pids}

    def handle_call({MonitoredServer, :whereis}, _from, [pid]), do: {:reply, pid, [pid]}
    def handle_call({MonitoredServer, :whereis}, _from, [pid | pids]), do: {:reply, pid, pids}
  end

  describe "lock session" do
    test "a disconnect releases every lock, and unlocks it on the current session", %{
      conn: conn
    } do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert is_integer(lock_backend_pid(conn, 1))

      # As if the lock connection's listener reported a disconnect while the pool
      # is still on the session the lock was taken on.
      send(Process.whereis(@locks), {:disconnected, self()})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_lost}
      assert tracked_locks() == []

      # The released lock is not left held, so the key is free.
      assert lock_backend_pid(conn, 1) == nil
    end

    test "a connect keeps the locks the current session holds", %{conn: conn} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      backend_pid = lock_backend_pid(conn, 1)

      send(Process.whereis(@locks), {:connected, self()})

      refute_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 200

      assert lock_backend_pid(conn, 1) == backend_pid
      assert [%Lock{key: 1, owner: owner, backend_pid: ^backend_pid}] = tracked_locks()
      assert owner == self()
    end

    test "a connect releases the locks the session no longer holds, though its pid is the same",
         %{conn: conn} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      backend_pid = lock_backend_pid(conn, 1)

      # The session keeps its backend pid but loses its locks, as after a pooler's
      # server reset or a backend pid reused on a new primary.
      pool = MonitoredServer.whereis(@conn)
      {:ok, _result} = Postgrex.query(pool, "SELECT pg_advisory_unlock_all()", [])
      assert lock_backend_pid(conn, 1) == nil

      send(Process.whereis(@locks), {:connected, self()})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_lost}
      assert tracked_locks() == []

      # The pool is still on the same session.
      assert {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      assert lock_backend_pid(conn, 1) == backend_pid
    end

    test "should send `lock_released` when the session ends and the pool reconnects", %{
      conn: conn
    } do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      pool = Process.whereis(@conn) |> :sys.get_state() |> Map.fetch!(:pid)

      terminate_backend(conn, lock_backend_pid(conn, 1))

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_lost}, 5_000

      # The pool lived on, on a new session, and the key is free to take again.
      assert Process.alive?(pool)
      assert tracked_locks() == []
      assert {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
    end

    test "a lock taken on the new session while the disconnect is queued is released and unlocked",
         %{conn: conn} do
      {:ok, lock1} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      old_backend_pid = lock_backend_pid(conn, 1)

      locks = Process.whereis(@locks)
      :ok = :sys.suspend(locks)
      on_exit(fn -> resume(locks) end)

      # A lock call queued before the session ends...
      owner = start_lock_owner(2)

      wait_for_message(
        locks,
        &match?({:"$gen_call", _from, {:try_advisory_lock, 2, _owner, _timeout, _deadline}}, &1)
      )

      # ...and the connection's disconnect and reconnect queued after it.
      terminate_backend(conn, old_backend_pid)
      wait_for_message(locks, &match?({:connected, _pid}, &1))

      :ok = :sys.resume(locks)

      # The call is served by the new session.
      assert_receive {:lock_owner, ^owner, {:ok, lock2}}

      # Both locks are released: the disconnect ends the locks of the session.
      assert_receive {AdvisoryLocks, :lock_released, ^lock1, :session_lost}
      assert_receive {:lock_owner, ^owner, {AdvisoryLocks, :lock_released, ^lock2, :session_lost}}

      assert tracked_locks() == []

      # The lock the new session took was unlocked once: no session holds the key,
      # so no lock count is left behind.
      assert lock_backend_pid(conn, 2) == nil
      assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 2, schema: "public")
    end

    test "a `:DOWN` of an earlier pool keeps the locks taken on the current pool" do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      %AdvisoryLocks.State{ref: ref} = :sys.get_state(@locks)

      earlier_pool = spawn(fn -> :ok end)
      send(Process.whereis(@locks), {:DOWN, ref, :process, earlier_pool, :shutdown})

      refute_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 200
      assert [%Lock{key: 1}] = tracked_locks()

      # The current pool going down releases it.
      pool = MonitoredServer.whereis(@conn)
      send(Process.whereis(@locks), {:DOWN, ref, :process, pool, :shutdown})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :shutdown}
      assert tracked_locks() == []
    end

    test "a lock request whose caller stopped waiting takes no lock", %{conn: conn} do
      locks = Process.whereis(@locks)
      :ok = :sys.suspend(locks)
      on_exit(fn -> resume(locks) end)

      test = self()

      caller =
        spawn(fn ->
          reply =
            try do
              AdvisoryLocks.try_advisory_lock(@locks, 3, 100)
            catch
              :exit, reason -> {:exit, reason}
            end

          send(test, {:caller, reply})

          receive do
            :stop -> :ok
          end
        end)

      assert_receive {:caller, {:exit, {:timeout, _call}}}, 1_000

      :ok = :sys.resume(locks)

      # The request is answered without a lock: none is tracked, none is held.
      assert tracked_locks() == []
      assert lock_backend_pid(conn, 3) == nil

      send(caller, :stop)
    end

    test "should answer `{:error, :not_connected}` while the lock pool is down" do
      shutdown_database_connection()

      assert {:error, :not_connected} = AdvisoryLocks.try_advisory_lock(@locks, 1, 1_000)
    end
  end

  describe "cleanup that cannot be confirmed ends the lock session" do
    # Moving the `subscriptions` table away makes the lock statements fail (they
    # name it), while the locks already taken stay held: their key derives from
    # the table's oid, which a rename keeps.
    setup %{postgrex_config: postgrex_config} do
      on_exit(fn -> restore_subscriptions_table(postgrex_config) end)
    end

    test "after a disconnect, when the held-locks query fails", %{conn: conn} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      pool = MonitoredServer.whereis(@conn)

      move_subscriptions_table(conn)
      send(Process.whereis(@locks), {:disconnected, self()})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_lost}
      assert_session_ended(conn, pool, 1)
    end

    test "after a connect, when the held-locks query fails", %{conn: conn} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      pool = MonitoredServer.whereis(@conn)

      move_subscriptions_table(conn)
      send(Process.whereis(@locks), {:connected, self()})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_lost}
      assert_session_ended(conn, pool, 1)
    end

    test "when the unlock after an owner terminated fails", %{conn: conn} do
      owner = start_lock_owner(1)
      assert_receive {:lock_owner, ^owner, {:ok, _lock}}
      pool = MonitoredServer.whereis(@conn)

      move_subscriptions_table(conn)
      ProcessHelper.shutdown(owner)

      assert_session_ended(conn, pool, 1)
    end
  end

  describe "cleanup ends the exact pool" do
    setup %{postgrex_config: postgrex_config} do
      on_exit(fn -> restore_subscriptions_table(postgrex_config) end)
    end

    test "when the pool cannot be looked up after an owner terminated, the lock's own pool ends",
         %{conn: conn} do
      owner = start_lock_owner(1)
      assert_receive {:lock_owner, ^owner, {:ok, _lock}}
      pool = MonitoredServer.whereis(@conn)

      # MonitoredServer stalls, so AdvisoryLocks cannot tell which pool is current.
      monitored = Process.whereis(@conn)
      :ok = :sys.suspend(monitored)
      on_exit(fn -> resume(monitored) end)

      ProcessHelper.shutdown(owner)

      # The lookup gives up after five seconds; the lock's own pool is ended.
      Wait.until(10_000, fn -> refute Process.alive?(pool) end)
      Wait.until(5_000, fn -> assert lock_backend_pid(conn, 1) == nil end)
    end

    test "a failed held-locks query ends the pool it ran on, not a pool started since",
         %{conn: conn, postgrex_config: postgrex_config} do
      # Answers AdvisoryLocks's MonitoredServer calls with scripted pools.
      scripted = start_supervised!({ScriptedMonitoredServer, name: :scripted_lock_pool})
      pool = start_supervised!({Postgrex, postgrex_config}, id: :scripted_pool)
      replacement = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(replacement, :kill) end)

      :ok = ScriptedMonitoredServer.script(scripted, [pool])

      locks =
        start_supervised!(
          {AdvisoryLocks, conn: :scripted_lock_pool, query_timeout: 5_000, schema: "public"},
          id: :scripted_locks
        )

      {:ok, lock} = AdvisoryLocks.try_advisory_lock(locks, 1)

      # The held-locks query runs on `pool` and fails; by the time the session is
      # ended, `replacement` is the current pool.
      move_subscriptions_table(conn)
      :ok = ScriptedMonitoredServer.script(scripted, [pool, replacement])

      send(locks, {:connected, self()})

      assert_receive {AdvisoryLocks, :lock_released, ^lock, :session_lost}
      _state = :sys.get_state(locks)

      assert Process.alive?(replacement)
      Wait.until(5_000, fn -> refute Process.alive?(pool) end)

      restore_subscriptions_table(conn)
      Wait.until(5_000, fn -> assert lock_backend_pid(conn, 1) == nil end)
    end
  end

  describe "lock pool dies holding a checkout" do
    defmodule SilentPool do
      # Stands in for a connection pool that takes a checkout request and never
      # answers it, then dies.
      use GenServer

      def start_link(test), do: GenServer.start_link(__MODULE__, test)

      def init(test), do: {:ok, test}

      def handle_info({:db_connection, _from, {:checkout, _callers, _start, _queue?}}, test) do
        send(test, {:checkout_received, self()})

        {:noreply, test}
      end
    end

    test "a lock call answers with an error instead of waiting forever" do
      monitored =
        start_supervised!(
          {MonitoredServer,
           mfa: {SilentPool, :start_link, [self()]}, name: :silent_pool, backoff_min: 60_000}
        )

      locks =
        start_supervised!(
          {AdvisoryLocks, conn: :silent_pool, query_timeout: 15_000, schema: "public"},
          id: :silent_pool_locks
        )

      task = Task.async(fn -> AdvisoryLocks.try_advisory_lock(locks, 1, 10_000) end)

      assert_receive {:checkout_received, pool}
      assert MonitoredServer.whereis(monitored) == pool

      Process.exit(pool, :kill)

      assert {:ok, {:error, _reason}} = Task.yield(task, 2_000) || Task.shutdown(task)
    end
  end

  describe "subscription locks" do
    test "should report the row and whether this session holds the lock", %{
      conn: conn,
      schema: schema
    } do
      {:ok, %Storage.Subscription{subscription_id: held}} =
        Storage.Subscription.subscribe_to_stream(conn, "$all", "held", 0, schema: schema)

      {:ok, %Storage.Subscription{subscription_id: free}} =
        Storage.Subscription.subscribe_to_stream(conn, "$all", "free", 0, schema: schema)

      {:ok, %Storage.Subscription{subscription_id: elsewhere}} =
        Storage.Subscription.subscribe_to_stream(conn, "$all", "elsewhere", 0, schema: schema)

      {:ok, _lock} = AdvisoryLocks.try_advisory_lock(@locks, held)
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn, elsewhere, schema: schema)

      missing = held + free + elsewhere

      assert {:ok, locks} =
               AdvisoryLocks.subscription_locks(@locks, [held, free, elsewhere, missing])

      assert locks == %{
               held => %{row_exists: true, lock_held: true},
               free => %{row_exists: true, lock_held: false},
               elsewhere => %{row_exists: true, lock_held: false},
               missing => %{row_exists: false, lock_held: false}
             }

      assert {:ok, %{}} = AdvisoryLocks.subscription_locks(@locks, [])
    end
  end

  describe "acquire same lock on different schemas" do
    setup do
      postgrex_config = Config.parsed(TestEventStore, :decree)

      public_schema =
        postgrex_config
        |> Keyword.put(:schema, "public")
        |> Config.default_postgrex_opts()

      conn1 = start_supervised!({Postgrex, public_schema}, id: :conn1)

      example_schema =
        postgrex_config
        |> Keyword.put(:schema, "example")
        |> Config.default_postgrex_opts()

      conn2 = start_supervised!({Postgrex, example_schema}, id: :conn2)

      [conn1: conn1, conn2: conn2, schema1: "public", schema2: "example"]
    end

    test "should acquire lock", %{conn1: conn1, conn2: conn2, schema1: schema1, schema2: schema2} do
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn1, 1, schema: schema1)
      :ok = Storage.Lock.try_acquire_exclusive_lock(conn2, 1, schema: schema2)
    end
  end

  defp tracked_locks do
    %AdvisoryLocks.State{locks: locks} = :sys.get_state(@locks)

    Map.values(locks)
  end

  # A process that takes the lock on `key`, reports the reply and then forwards
  # every message it receives to the test process.
  defp start_lock_owner(key) do
    test = self()

    spawn(fn ->
      reply = AdvisoryLocks.try_advisory_lock(@locks, key, 15_000)
      send(test, {:lock_owner, self(), reply})

      forward = fn forward ->
        receive do
          message ->
            send(test, {:lock_owner, self(), message})
            forward.(forward)
        end
      end

      forward.(forward)
    end)
  end

  # The lock pool was killed, and with it its session: once the table is back the
  # key is free from another connection.
  defp assert_session_ended(conn, pool, key) do
    Wait.until(5_000, fn -> refute Process.alive?(pool) end)

    restore_subscriptions_table(conn)

    Wait.until(5_000, fn -> assert lock_backend_pid(conn, key) == nil end)
    assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, key, schema: "public")
  end

  defp move_subscriptions_table(conn) do
    Postgrex.query!(conn, "ALTER TABLE public.subscriptions RENAME TO subscriptions_moved", [])
  end

  defp restore_subscriptions_table(conn) when is_pid(conn) do
    Postgrex.query!(
      conn,
      "ALTER TABLE IF EXISTS public.subscriptions_moved RENAME TO subscriptions",
      []
    )
  end

  defp restore_subscriptions_table(postgrex_config) when is_list(postgrex_config) do
    {:ok, conn} = Postgrex.start_link(postgrex_config)

    try do
      restore_subscriptions_table(conn)
    after
      GenServer.stop(conn)
    end
  end

  defp resume(pid) do
    :sys.resume(pid)
  catch
    :exit, _reason -> :ok
  end

  defp wait_for_message(pid, match?) do
    Wait.until(5_000, fn ->
      {:messages, messages} = Process.info(pid, :messages)

      assert Enum.any?(messages, match?)
    end)
  end

  defp lock_backend_pid(conn, key) do
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
        [key]
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

  defp shutdown_database_connection do
    %{pid: pid} = Process.whereis(@conn) |> :sys.get_state()

    ProcessHelper.shutdown(pid)
  end
end
