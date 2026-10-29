defmodule Decree.Store.AdvisoryLocks do
  @moduledoc false

  # PostgreSQL provides a means for creating locks that have application-defined
  # meanings. Advisory locks are faster, avoid table bloat, and are
  # automatically cleaned up by the server at the end of the session.
  #
  # Every lock is taken on the instance's lock connection pool (pool size 1), so
  # all of an instance's locks live in one database session. A lock is lost when
  # that session ends, which happens in two ways:
  #
  #   - the pool process dies: `MonitoredServer` sends a `:DOWN` naming the pool,
  #     and every lock taken on that pool is released;
  #   - the connection inside the pool disconnects and the pool reconnects on a
  #     new session, while the pool itself lives on: no `:DOWN` arrives
  #     (sparkle-space/sparklespace#7543, upstream commanded/eventstore#330).
  #
  # For the second, the pool's connection listener sends `{:connected, pid}` and
  # `{:disconnected, pid}` here, and what the session holds is read from
  # `pg_locks`, not inferred:
  #
  #   - `:disconnected`: the session that held the locks has ended, so every
  #     tracked lock is released at once, before any query. A lock call already
  #     in flight may have been served by the NEW session, so its lock is live
  #     and was just released from tracking; then `pg_locks` is asked which of
  #     the released keys the current session holds, and each released lock on
  #     such a key is unlocked once. As `pg_try_advisory_lock` is reentrant
  #     within a session, this leaves no lock count behind; an extra unlock only
  #     returns false. If that query fails, nothing more is done.
  #   - `:connected`, while locks are tracked: `pg_locks` says which tracked keys
  #     the current session holds. Locks on those keys are kept, the others are
  #     released; if the query fails, every lock is released. Reading the locks
  #     themselves, rather than comparing backend process ids, also catches a
  #     session whose backend pid is the same as before but whose locks are gone
  #     (a pooler's server reset, or a pid reused after a failover).
  #
  # Each lock is also tagged with the backend pid of the session that took it,
  # and a lock query that reports a different backend releases the locks of the
  # earlier one, an early signal in addition to the listener.
  #
  # Queries run against the pool's pid, never through `MonitoredServer`'s name: a
  # checkout through the name is forwarded (or queued and forwarded) to a pool,
  # and if that pool dies holding it, nothing replies and the checkout's monitor,
  # which watches `MonitoredServer`, never fires, so this process would wait
  # forever (upstream commanded/eventstore#331). Against the pid, a pool that
  # dies mid-checkout exits the query, which is returned as `{:error, reason}`.
  # A pool that stays alive with no connection (rare for the lock pool: with
  # `backoff_type: :stop` a failed reconnect usually ends the pool first) drops
  # the checkout from its queue after its `queue_target`/`queue_interval`, four
  # to six seconds with the lock pool's defaults, measured.

  use GenServer

  require Logger

  alias Decree.Store.MonitoredServer

  defmodule State do
    @moduledoc false
    defstruct [:conn, :query_timeout, :ref, :schema, :backend_pid, locks: %{}]

    def new(opts) do
      conn = Keyword.fetch!(opts, :conn)
      query_timeout = Keyword.fetch!(opts, :query_timeout)
      schema = Keyword.fetch!(opts, :schema)

      %State{conn: conn, query_timeout: query_timeout, schema: schema}
    end
  end

  defmodule Lock do
    @moduledoc false

    @type t :: %Lock{
            key: non_neg_integer(),
            owner: pid(),
            ref: reference(),
            backend_pid: pos_integer() | nil,
            pool: pid() | nil
          }
    defstruct [:key, :owner, :ref, :backend_pid, :pool]

    def new(key, owner, backend_pid \\ nil, pool \\ nil) do
      %Lock{key: key, owner: owner, ref: make_ref(), backend_pid: backend_pid, pool: pool}
    end

    def ref(%Lock{ref: ref}), do: ref
  end

  alias Decree.Store.AdvisoryLocks.{Lock, State}
  alias Decree.Store.Storage

  def start_link(opts) do
    {start_opts, advisory_locks_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    state = State.new(advisory_locks_opts)

    GenServer.start_link(__MODULE__, state, start_opts)
  end

  def init(%State{} = state) do
    %State{conn: conn} = state

    {:ok, ref} = MonitoredServer.monitor(conn)

    {:ok, %State{state | ref: ref}}
  end

  @doc """
  Attempt to obtain an advisory lock.

     - `key` - an application specific integer to acquire a lock on.
     - `timeout` - an integer greater than zero which specifies how many
        milliseconds to wait for a reply, or the atom `:infinity` to wait
        indefinitely.

  Returns `{:ok, lock}` when lock successfully acquired, or
  `{:error, :lock_already_taken}` if the lock cannot be acquired immediately.
  Returns `{:error, :not_connected}` while the lock connection pool is down,
  `{:error, :timeout}` when the request could not be served within `timeout`
  (no lock is then held for the caller), or `{:error, reason}` when the lock
  query fails.

  ## Lock released

  An `{Decree.Store.AdvisoryLocks, :lock_released, lock, reason}` message will be
  sent to the lock owner when the lock is released, usually due to the database
  connection terminating (`reason` is then the pool's exit reason) or the lock
  connection disconnecting or reconnecting on a new database session (`reason`
  is `:session_lost`). It is up to the lock owner to attempt to reacquire the
  lost lock.

  """
  @spec try_advisory_lock(server :: GenServer.server(), key :: non_neg_integer(), timeout()) ::
          {:ok, reference()} | {:error, :lock_already_taken} | {:error, term}
  def try_advisory_lock(server, key, timeout \\ 5_000) when is_integer(key) do
    deadline = deadline(timeout)

    GenServer.call(server, {:try_advisory_lock, key, self(), timeout, deadline}, timeout)
  end

  @doc """
  For each subscription id, whether its `subscriptions` row exists and whether this
  instance's lock session holds its advisory lock.

  Returns `{:ok, %{subscription_id => %{row_exists: boolean(), lock_held:
  boolean()}}}` or `{:error, reason}`.
  """
  @spec subscription_locks(GenServer.server(), [pos_integer()], timeout()) ::
          {:ok, %{pos_integer() => %{row_exists: boolean(), lock_held: boolean()}}}
          | {:error, term}
  def subscription_locks(server, subscription_ids, timeout \\ 5_000)
      when is_list(subscription_ids) do
    GenServer.call(server, {:subscription_locks, subscription_ids, timeout}, timeout)
  catch
    :exit, reason -> {:error, reason}
  end

  def handle_call({:try_advisory_lock, key, owner, timeout, deadline}, _from, %State{} = state) do
    if expired?(deadline) do
      # The caller has stopped waiting: take nothing for it.
      {:reply, {:error, :timeout}, state}
    else
      try_advisory_lock(key, owner, timeout, deadline, state)
    end
  end

  def handle_call({:subscription_locks, [], _timeout}, _from, %State{} = state) do
    {:reply, {:ok, %{}}, state}
  end

  def handle_call({:subscription_locks, subscription_ids, timeout}, _from, %State{} = state) do
    %State{schema: schema} = state

    reply =
      with_pool(state, fn pool ->
        Storage.Lock.subscription_locks(pool, subscription_ids, schema: schema, timeout: timeout)
      end)

    case reply do
      {:ok, backend_pid, locks} -> {:reply, {:ok, locks}, session_seen(state, backend_pid)}
      {:error, _error} = reply -> {:reply, reply, state}
    end
  end

  # Lost the locks taken on the pool that went down. A `:DOWN` handled after a
  # restarted pool has served a new lock leaves that lock alone.
  def handle_info({:DOWN, ref, :process, pool, reason}, %State{ref: ref} = state) do
    %State{locks: locks} = state

    {lost, kept} =
      Enum.split_with(locks, fn {_ref, %Lock{pool: lock_pool}} -> lock_pool == pool end)

    release_from_tracking(lost, reason)

    {:noreply, %State{state | locks: Map.new(kept)}}
  end

  # Release lock when the lock owner process terminates.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    %State{locks: locks} = state

    state =
      case Map.get(locks, ref) do
        nil ->
          state

        %Lock{} = lock ->
          release_lock(lock, state)

          %State{state | locks: Map.delete(locks, ref)}
      end

    {:noreply, state}
  end

  # The lock pool's connection listener: the connection inside the pool connected
  # or disconnected. Without a tag the message is `{event, pid}`, with one (when
  # the configured `:connection_listeners` carries one) `{event, pid, tag}`.
  def handle_info({:disconnected, _conn_pid}, %State{} = state),
    do: {:noreply, session_ended(state)}

  def handle_info({:disconnected, _conn_pid, _tag}, %State{} = state),
    do: {:noreply, session_ended(state)}

  def handle_info({:connected, _conn_pid}, %State{} = state),
    do: {:noreply, verify_held_locks(state)}

  def handle_info({:connected, _conn_pid, _tag}, %State{} = state),
    do: {:noreply, verify_held_locks(state)}

  defp try_advisory_lock(key, owner, timeout, deadline, %State{} = state) do
    case try_acquire_exclusive_lock(key, timeout, state) do
      {:ok, :acquired, backend_pid, pool} ->
        state = session_seen(state, backend_pid)

        if expired?(deadline) do
          # Acquired after the caller stopped waiting: no one would own it.
          unlock(pool, key, state)

          {:reply, {:error, :timeout}, state}
        else
          lock = Lock.new(key, owner, backend_pid, pool)

          {:reply, {:ok, Lock.ref(lock)}, monitor_acquired_lock(state, lock)}
        end

      {:ok, :lock_already_taken, backend_pid, _pool} ->
        {:reply, {:error, :lock_already_taken}, session_seen(state, backend_pid)}

      {:error, _error} = reply ->
        {:reply, reply, state}
    end
  end

  defp try_acquire_exclusive_lock(key, timeout, %State{} = state) do
    %State{schema: schema} = state

    with_pool(state, fn pool ->
      case Storage.Lock.try_acquire_exclusive_session_lock(pool, key,
             schema: schema,
             timeout: timeout
           ) do
        {:ok, acquired, backend_pid} -> {:ok, acquired, backend_pid, pool}
        {:error, _error} = reply -> reply
      end
    end)
  end

  defp monitor_acquired_lock(%State{locks: locks} = state, %Lock{} = lock) do
    %Lock{owner: owner} = lock

    owner_ref = Process.monitor(owner)

    %State{state | locks: Map.put(locks, owner_ref, lock)}
  end

  # The session that held the locks has ended: release them all at once, then
  # undo any of them a lock call in flight took on the new session (see the
  # module comment).
  defp session_ended(%State{locks: locks} = state) when map_size(locks) == 0, do: state

  defp session_ended(%State{} = state) do
    %State{locks: locks} = state

    release_from_tracking(locks, :session_lost)

    unlock_released_on_current_session(Map.values(locks), state)

    %State{state | locks: %{}}
  end

  defp unlock_released_on_current_session(released, %State{} = state) do
    %State{query_timeout: query_timeout, schema: schema} = state

    keys = released |> Enum.map(fn %Lock{key: key} -> key end) |> Enum.uniq()

    with_pool(state, fn pool ->
      with {:ok, held} <-
             Storage.Lock.held_keys(pool, keys, schema: schema, timeout: query_timeout) do
        for %Lock{key: key} <- released, MapSet.member?(held, key) do
          Storage.Lock.unlock(pool, key, schema: schema, timeout: query_timeout)
        end

        :ok
      end
    end)
  end

  # Keep the tracked locks the current session holds, release the others.
  defp verify_held_locks(%State{locks: locks} = state) when map_size(locks) == 0, do: state

  defp verify_held_locks(%State{} = state) do
    %State{locks: locks, query_timeout: query_timeout, schema: schema} = state

    keys = locks |> Enum.map(fn {_ref, %Lock{key: key}} -> key end) |> Enum.uniq()

    case with_pool(
           state,
           &Storage.Lock.held_keys(&1, keys, schema: schema, timeout: query_timeout)
         ) do
      {:ok, held} ->
        {kept, lost} =
          Enum.split_with(locks, fn {_ref, %Lock{key: key}} -> MapSet.member?(held, key) end)

        release_from_tracking(lost, :session_lost)

        %State{state | locks: Map.new(kept)}

      {:error, error} ->
        Logger.warning(
          "Advisory lock session could not be verified, releasing all locks due to: " <>
            inspect(error)
        )

        release_from_tracking(locks, :session_lost)

        %State{state | locks: %{}}
    end
  end

  # A query ran on the session `backend_pid`: every lock taken on another session
  # is gone, so release it.
  defp session_seen(%State{} = state, backend_pid) do
    %State{locks: locks} = state

    {lost, kept} =
      Enum.split_with(locks, fn {_ref, %Lock{backend_pid: lock_backend_pid}} ->
        lock_backend_pid != backend_pid
      end)

    release_from_tracking(lost, :session_lost)

    %State{state | locks: Map.new(kept), backend_pid: backend_pid}
  end

  # Stop watching the owners of locks that are no longer held, and tell them.
  defp release_from_tracking(locks, reason) do
    for {owner_ref, _lock} <- locks, do: Process.demonitor(owner_ref, [:flush])

    notify_lost_locks(locks, reason)
  end

  defp notify_lost_locks(locks, reason) do
    for {_ref, %Lock{} = lock} <- locks do
      %Lock{owner: owner, ref: ref} = lock

      :ok = Process.send(owner, {__MODULE__, :lock_released, ref, reason}, [])
    end

    :ok
  end

  # A lock taken on an earlier session or an earlier pool is no longer held, and
  # unlocking its key now would only make Postgres warn that it is not held.
  defp release_lock(%Lock{backend_pid: backend_pid}, %State{backend_pid: current})
       when backend_pid != current,
       do: :ok

  defp release_lock(%Lock{key: key, pool: lock_pool}, %State{} = state) do
    with_pool(state, fn
      ^lock_pool = pool -> unlock(pool, key, state)
      _other_pool -> :ok
    end)
  end

  defp unlock(pool, key, %State{} = state) do
    %State{query_timeout: query_timeout, schema: schema} = state

    Storage.Lock.unlock(pool, key, schema: schema, timeout: query_timeout)
  catch
    :exit, reason -> {:error, reason}
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp expired?(:infinity), do: false
  defp expired?(deadline), do: System.monotonic_time(:millisecond) >= deadline

  # Run `fun` against the lock pool's pid (see the module comment). Returns
  # `{:error, :not_connected}` while the pool is down, and the reason of an exit
  # (the pool died while the query waited for, or held, its connection) as
  # `{:error, reason}`.
  defp with_pool(%State{conn: conn}, fun) do
    case MonitoredServer.whereis(conn) do
      nil ->
        {:error, :not_connected}

      pool when is_pid(pool) ->
        try do
          fun.(pool)
        catch
          :exit, reason -> {:error, reason}
        end
    end
  end
end
