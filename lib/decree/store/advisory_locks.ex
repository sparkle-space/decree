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
  #     returns false.
  #   - `:connected`, while locks are tracked: `pg_locks` says which tracked keys
  #     the current session holds. Locks on those keys are kept, the others are
  #     released; if the query fails, every lock is released. Reading the locks
  #     themselves, rather than comparing backend process ids, also catches a
  #     session whose backend pid is the same as before but whose locks are gone
  #     (a pooler's server reset, or a pid reused after a failover).
  #
  # Whenever that cleanup cannot be confirmed (the `pg_locks` query or an unlock
  # fails, here, after an owner terminated, or after a lock acquired too late),
  # the session may still hold a lock count nothing tracks, which would keep the
  # key held by this instance for good. The lock pool is then killed, which ends
  # its session and every lock count in it, and is started again after its
  # restart delay. The pool killed is the one the failed step ran on or, when
  # `MonitoredServer` does not answer which pool is current, the pool each lock
  # was taken on; never a pool started since. An owner's lock counts as cleaned
  # up only once it is unlocked, or its pool is confirmed down or replaced.
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
  #
  # That queue wait is not bounded by a query's `:timeout`, so the queries a caller
  # drives (a lock attempt, an ownership probe) pass `queue: false`: when the
  # pool's one connection is not free at once they fail at once ("connection not
  # available and queuing is disabled"), the lock attempt is retried by its owner,
  # and nothing queued behind them here (a disconnect, a pool or owner `:DOWN`)
  # waits on the pool. The cleanup driven by the connection listener and by owner
  # `:DOWN`s does queue: it runs during reconnects, when the connection is briefly
  # unavailable, and failing it would end the session on every disconnect. The
  # listener's cleanup tells the owners of the locks it handles before it queries,
  # and an owner's `:DOWN` has no owner left to tell.

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
    deadline = deadline(timeout)

    GenServer.call(server, {:subscription_locks, subscription_ids, timeout, deadline}, timeout)
  catch
    :exit, reason -> {:error, reason}
  end

  def handle_call({:try_advisory_lock, key, owner, timeout, deadline}, _from, %State{} = state) do
    {reply, state} = try_advisory_lock(key, owner, timeout, deadline, state)

    {:reply, reply, state}
  end

  def handle_call({:subscription_locks, [], _timeout, _deadline}, _from, %State{} = state) do
    {:reply, {:ok, %{}}, state}
  end

  # A probe whose caller has stopped waiting is dropped before any work, so stale
  # probes queued here do not delay the handling of a lost lock behind them.
  def handle_call(
        {:subscription_locks, subscription_ids, timeout, deadline},
        _from,
        %State{} = state
      ) do
    %State{schema: schema} = state

    with {:ok, remaining} <- remaining(deadline),
         {:ok, pool} <- whereis_pool(state, min(5_000, remaining)),
         {:ok, remaining} <- remaining(deadline),
         {:ok, backend_pid, locks} <-
           query_subscription_locks(pool, subscription_ids,
             schema: schema,
             timeout: min(timeout, remaining),
             queue: false
           ) do
      {:reply, {:ok, locks}, session_seen(state, backend_pid)}
    else
      {:error, _error} = reply -> {:reply, reply, state}
    end
  end

  # Lost the locks taken on the pool that went down. A `:DOWN` handled after a
  # restarted pool has served a new lock leaves that lock alone.
  def handle_info({:DOWN, ref, :process, pool, reason}, %State{ref: ref} = state) do
    {:noreply, release_pool_locks(state, pool, reason)}
  end

  # Release lock when the lock owner process terminates.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %State{} = state) do
    %State{locks: locks} = state

    state =
      case Map.pop(locks, ref) do
        {nil, _locks} ->
          state

        {%Lock{} = lock, locks} ->
          release_lock(lock, %State{state | locks: locks})
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

  # Every blocking step uses what is left of the caller's `timeout`; once nothing
  # is left the caller has stopped waiting, and no lock is taken for it.
  defp try_advisory_lock(key, owner, timeout, deadline, %State{} = state) do
    with {:ok, remaining} <- remaining(deadline),
         {:ok, pool} <- whereis_pool(state, min(5_000, remaining)),
         {:ok, remaining} <- remaining(deadline) do
      case try_acquire_exclusive_lock(pool, key, min(timeout, remaining), state) do
        {:ok, :acquired, backend_pid} ->
          state = session_seen(state, backend_pid)

          acquired(key, owner, backend_pid, pool, deadline, state)

        {:ok, :lock_already_taken, backend_pid} ->
          {{:error, :lock_already_taken}, session_seen(state, backend_pid)}

        {:error, _error} = reply ->
          {reply, state}
      end
    else
      {:error, _error} = reply -> {reply, state}
    end
  end

  defp acquired(key, owner, backend_pid, pool, deadline, %State{} = state) do
    case remaining(deadline) do
      {:ok, _remaining} ->
        lock = Lock.new(key, owner, backend_pid, pool)

        {{:ok, Lock.ref(lock)}, monitor_acquired_lock(state, lock)}

      {:error, :timeout} = reply ->
        # Acquired after the caller stopped waiting: no one would own it.
        case unlock(pool, key, state) do
          :ok -> {reply, state}
          {:error, error} -> {reply, end_session(state, pool, {:unlock_failed, error})}
        end
    end
  end

  defp try_acquire_exclusive_lock(pool, key, timeout, %State{} = state) do
    %State{schema: schema} = state

    Storage.Lock.try_acquire_exclusive_session_lock(pool, key,
      schema: schema,
      timeout: timeout,
      queue: false
    )
  catch
    :exit, reason -> {:error, reason}
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

    state = %State{state | locks: %{}}

    released = Map.values(locks)

    case whereis_pool(state) do
      {:ok, pool} ->
        case unlock_held(released, pool, state) do
          :ok -> state
          {:error, error} -> end_session(state, pool, error)
        end

      # No pool, so no session to hold them.
      {:error, :not_connected} ->
        state

      # Which pool is current is unknown: end the pools the locks were taken on.
      {:error, {:pool_lookup_failed, _reason} = error} ->
        end_sessions(state, lock_pools(released), error)
    end
  end

  # Unlock, once per released lock, the keys the pool's current session holds.
  defp unlock_held(released, pool, %State{} = state) do
    %State{query_timeout: query_timeout, schema: schema} = state

    keys = released |> Enum.map(fn %Lock{key: key} -> key end) |> Enum.uniq()

    case held_keys(pool, keys, schema: schema, timeout: query_timeout) do
      {:ok, held} ->
        Enum.reduce_while(released, :ok, fn %Lock{key: key}, :ok ->
          with true <- MapSet.member?(held, key),
               {:error, error} <- unlock(pool, key, state) do
            {:halt, {:error, {:unlock_failed, error}}}
          else
            _unlocked_or_not_held -> {:cont, :ok}
          end
        end)

      {:error, error} ->
        {:error, {:held_locks_query_failed, error}}
    end
  end

  # Keep the tracked locks the current session holds, release the others.
  defp verify_held_locks(%State{locks: locks} = state) when map_size(locks) == 0, do: state

  defp verify_held_locks(%State{} = state) do
    %State{locks: locks, query_timeout: query_timeout, schema: schema} = state

    keys = locks |> Enum.map(fn {_ref, %Lock{key: key}} -> key end) |> Enum.uniq()

    case whereis_pool(state) do
      {:ok, pool} ->
        case held_keys(pool, keys, schema: schema, timeout: query_timeout) do
          {:ok, held} ->
            {kept, lost} =
              Enum.split_with(locks, fn {_ref, %Lock{key: key}} -> MapSet.member?(held, key) end)

            release_from_tracking(lost, :session_lost)

            %State{state | locks: Map.new(kept)}

          # End the session the query ran on, never a pool started since.
          {:error, error} ->
            state
            |> release_all()
            |> end_session(pool, {:held_locks_query_failed, error})
        end

      # No pool, so no session to hold them.
      {:error, :not_connected} ->
        release_all(state)

      # Which pool is current is unknown: end the pools the locks were taken on.
      {:error, {:pool_lookup_failed, _reason} = error} ->
        pools = locks |> Map.values() |> lock_pools()

        state
        |> release_all()
        |> end_sessions(pools, error)
    end
  end

  defp release_all(%State{locks: locks} = state) do
    release_from_tracking(locks, :session_lost)

    %State{state | locks: %{}}
  end

  # Cleaning up after a lost lock failed, so the pool's session may still hold a
  # lock count that nothing tracks: an owner retrying would take the key again
  # (advisory locks are reentrant within a session) and its one unlock would not
  # free it. End the session instead. Killing the pool ends its connections too:
  # `DBConnection.Watcher` terminates the dead pool's connection supervisor, the
  # connection processes exit and their sockets close, and Postgres releases
  # every lock the session held. `MonitoredServer` starts a new pool after its
  # restart delay.
  defp end_session(%State{} = state, pool, reason) do
    Logger.warning(
      "Ending the advisory lock session, so that it holds no lock left behind, due to: " <>
        inspect(reason)
    )

    Process.exit(pool, :kill)

    release_pool_locks(state, pool, :session_lost)
  end

  defp end_sessions(%State{} = state, pools, reason) do
    Enum.reduce(pools, state, &end_session(&2, &1, reason))
  end

  # The pools the given locks were taken on.
  defp lock_pools(locks) do
    for %Lock{pool: pool} <- locks, is_pid(pool), uniq: true, do: pool
  end

  defp release_pool_locks(%State{} = state, pool, reason) do
    %State{locks: locks} = state

    {lost, kept} =
      Enum.split_with(locks, fn {_ref, %Lock{pool: lock_pool}} -> lock_pool == pool end)

    release_from_tracking(lost, reason)

    %State{state | locks: Map.new(kept)}
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

  # Unlock the key of a lock whose owner terminated. A lock taken on an earlier
  # session or an earlier pool is no longer held, and unlocking its key now would
  # only make Postgres warn that it is not held.
  defp release_lock(%Lock{backend_pid: backend_pid}, %State{backend_pid: current} = state)
       when backend_pid != current,
       do: state

  #
  # The cleanup is complete only when the unlock succeeds, or when the lock's pool
  # is confirmed down or replaced; otherwise the lock's own pool is ended.
  defp release_lock(%Lock{key: key, pool: lock_pool}, %State{} = state) do
    case whereis_pool(state) do
      {:ok, ^lock_pool} ->
        case unlock(lock_pool, key, state) do
          :ok -> state
          {:error, error} -> end_session(state, lock_pool, {:unlock_failed, error})
        end

      # Its pool was replaced or is down, and its session with it.
      {:ok, _replacement} ->
        state

      {:error, :not_connected} ->
        state

      {:error, {:pool_lookup_failed, _reason} = error} ->
        end_sessions(state, lock_pools([%Lock{pool: lock_pool}]), error)
    end
  end

  # `:ok` when the key is unlocked or was not held, `{:error, reason}` when the
  # unlock failed.
  defp unlock(pool, key, %State{} = state) do
    %State{query_timeout: query_timeout, schema: schema} = state

    Storage.Lock.unlock(pool, key, schema: schema, timeout: query_timeout)
  catch
    :exit, reason -> {:error, reason}
  end

  # The queries against the pool's pid return an exit (the pool died while the
  # query waited for, or held, its connection) as `{:error, reason}`.
  defp query_subscription_locks(pool, subscription_ids, opts) do
    Storage.Lock.subscription_locks(pool, subscription_ids, opts)
  catch
    :exit, reason -> {:error, reason}
  end

  defp held_keys(pool, keys, opts) do
    Storage.Lock.held_keys(pool, keys, opts)
  catch
    :exit, reason -> {:error, reason}
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  # What is left of the caller's timeout, or `{:error, :timeout}` once nothing is.
  defp remaining(:infinity), do: {:ok, :infinity}

  defp remaining(deadline) do
    case deadline - System.monotonic_time(:millisecond) do
      remaining when remaining > 0 -> {:ok, remaining}
      _expired -> {:error, :timeout}
    end
  end

  # The lock pool's pid (see the module comment), `{:error, :not_connected}` while
  # the pool is confirmed down, or `{:error, {:pool_lookup_failed, reason}}` when
  # `MonitoredServer` did not answer, so the pool is unknown.
  defp whereis_pool(%State{conn: conn}, timeout \\ 5_000) do
    case MonitoredServer.lookup(conn, timeout) do
      {:ok, nil} -> {:error, :not_connected}
      {:ok, pool} when is_pid(pool) -> {:ok, pool}
      {:error, reason} -> {:error, {:pool_lookup_failed, reason}}
    end
  end
end
