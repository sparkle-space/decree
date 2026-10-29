defmodule Decree.Store.AdvisoryLocksTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{AdvisoryLocks, Config, ProcessHelper, Wait}
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

  describe "lock session" do
    test "a lock on the live session stays held when the connection reports a disconnect",
         %{conn: conn} do
      {:ok, lock} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      backend_pid = lock_backend_pid(conn, 1)

      # As if the lock connection's listener reported a disconnect, while the pool
      # is still on the session the lock was taken on.
      send(Process.whereis(@locks), {:disconnected, self()})
      send(Process.whereis(@locks), {:connected, self()})

      refute_receive {AdvisoryLocks, :lock_released, ^lock, _reason}, 200

      assert lock_backend_pid(conn, 1) == backend_pid
      assert [%Lock{key: 1, owner: owner, backend_pid: ^backend_pid}] = tracked_locks()
      assert owner == self()
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

    test "a lock taken on the new session while the disconnect is queued stays held", %{
      conn: conn
    } do
      {:ok, lock1} = AdvisoryLocks.try_advisory_lock(@locks, 1)
      old_backend_pid = lock_backend_pid(conn, 1)

      locks = Process.whereis(@locks)
      :ok = :sys.suspend(locks)

      # A lock call queued before the session ends...
      owner = start_lock_owner(2)
      wait_for_message(locks, &match?({:"$gen_call", _from, {:try_advisory_lock, 2, _, _}}, &1))

      # ...and the connection's disconnect and reconnect queued after it.
      terminate_backend(conn, old_backend_pid)
      wait_for_message(locks, &match?({:connected, _pid}, &1))

      :ok = :sys.resume(locks)

      # The call is served by the new session.
      assert_receive {:lock_owner, ^owner, {:ok, lock2}}

      # The lock on the old session is released; the one on the new session is not.
      assert_receive {AdvisoryLocks, :lock_released, ^lock1, :session_lost}
      refute_receive {:lock_owner, ^owner, {AdvisoryLocks, :lock_released, ^lock2, _reason}}, 300

      new_backend_pid = lock_backend_pid(conn, 2)
      assert is_integer(new_backend_pid) and new_backend_pid != old_backend_pid
      assert [%Lock{key: 2, owner: ^owner, backend_pid: ^new_backend_pid}] = tracked_locks()

      # The owner's lock count on that session is one: when it stops, the single
      # unlock frees the key.
      ProcessHelper.shutdown(owner)

      Wait.until(fn ->
        assert :ok = Storage.Lock.try_acquire_exclusive_lock(conn, 2, schema: "public")
      end)
    end

    test "should answer `{:error, :not_connected}` while the lock pool is down" do
      shutdown_database_connection()

      assert {:error, :not_connected} = AdvisoryLocks.try_advisory_lock(@locks, 1, 1_000)
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
      reply = AdvisoryLocks.try_advisory_lock(@locks, key)
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

    Decree.Store.ProcessHelper.shutdown(pid)
  end
end
