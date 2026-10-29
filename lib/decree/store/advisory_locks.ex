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
  #   - the pool process dies: `MonitoredServer` sends a `:DOWN`, and every lock
  #     is released;
  #   - the connection inside the pool disconnects and the pool reconnects on a
  #     new session, while the pool itself lives on: no `:DOWN` arrives
  #     (sparkle-space/sparklespace#7543, upstream commanded/eventstore#330).
  #
  # For the second, each lock is tagged with the backend process id of the session
  # that took it (`pg_backend_pid()`), and the pool's connection listener sends
  # `{:connected, pid}` and `{:disconnected, pid}` here. On either, while any lock
  # is held, one `SELECT pg_backend_pid()` asks which session the pool is on now,
  # and every lock tagged with another session is released; if the query fails,
  # every lock is released. A lock attempt, which also reports its session, does
  # the same with the session it ran on.
  #
  # Releasing by session, rather than releasing every lock on `:disconnected`,
  # matters for a lock call already in flight across the disconnect: it is
  # served by the NEW connection, so the lock it takes is live. Dropping it from
  # tracking would make the owner retry, and as `pg_try_advisory_lock` is
  # reentrant within a session, the retry would succeed again, leaving a lock
  # count that the owner's single unlock never brings to zero: the key would
  # stay held by this session until the session ends.
  #
  # Queries run against the pool's pid, never through `MonitoredServer`'s name: a
  # checkout through the name is forwarded (or queued and forwarded) to a pool,
  # and if that pool dies holding it, nothing replies and the checkout's monitor,
  # which watches `MonitoredServer`, never fires, so this process would wait
  # forever (upstream commanded/eventstore#331). Against the pid, a pool that
  # dies mid-checkout exits the query, which is returned as `{:error, reason}`,
  # and a live pool with no connection drops the checkout from its queue after
  # its `queue_target`/`queue_interval` (four to six seconds with the lock pool's
  # defaults, measured).

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
            backend_pid: pos_integer() | nil
          }
    defstruct [:key, :owner, :ref, :backend_pid]

    def new(key, owner, backend_pid \\ nil) do
      %Lock{key: key, owner: owner, ref: make_ref(), backend_pid: backend_pid}
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
  Returns `{:error, :not_connected}` while the lock connection pool is down, or
  `{:error, reason}` when the lock query fails.

  ## Lock released

  An `{Decree.Store.AdvisoryLocks, :lock_released, lock, reason}` message will be
  sent to the lock owner when the lock is released, usually due to the database
  connection terminating (`reason` is then the pool's exit reason) or the lock
  connection reconnecting on a new database session (`reason` is
  `:session_lost`). It is up to the lock owner to attempt to reacquire the lost
  lock.

  """
  @spec try_advisory_lock(server :: GenServer.server(), key :: non_neg_integer(), timeout()) ::
          {:ok, reference()} | {:error, :lock_already_taken} | {:error, term}
  def try_advisory_lock(server, key, timeout \\ 5_000) when is_integer(key) do
    GenServer.call(server, {:try_advisory_lock, key, self(), timeout}, timeout)
  end

  def handle_call({:try_advisory_lock, key, owner, timeout}, _from, %State{} = state) do
    case try_acquire_exclusive_lock(key, timeout, state) do
      {:ok, :acquired, backend_pid} ->
        lock = Lock.new(key, owner, backend_pid)

        state =
          state
          |> session_seen(backend_pid)
          |> monitor_acquired_lock(lock)

        {:reply, {:ok, Lock.ref(lock)}, state}

      {:ok, :lock_already_taken, backend_pid} ->
        {:reply, {:error, :lock_already_taken}, session_seen(state, backend_pid)}

      {:error, _error} = reply ->
        {:reply, reply, state}
    end
  end

  # Lost locks when database connection goes down.
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{ref: ref} = state) do
    %State{locks: locks} = state

    notify_lost_locks(locks, reason)

    {:noreply, %State{state | locks: %{}, backend_pid: nil}}
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
  def handle_info({event, _conn_pid}, %State{} = state)
      when event in [:connected, :disconnected] do
    {:noreply, verify_session(state)}
  end

  def handle_info({event, _conn_pid, _tag}, %State{} = state)
      when event in [:connected, :disconnected] do
    {:noreply, verify_session(state)}
  end

  defp try_acquire_exclusive_lock(key, timeout, %State{} = state) do
    %State{schema: schema} = state

    with_pool(state, fn pool ->
      Storage.Lock.try_acquire_exclusive_session_lock(pool, key, schema: schema, timeout: timeout)
    end)
  end

  defp monitor_acquired_lock(%State{locks: locks} = state, %Lock{} = lock) do
    %Lock{owner: owner} = lock

    owner_ref = Process.monitor(owner)

    %State{state | locks: Map.put(locks, owner_ref, lock)}
  end

  # No lock is held, so there is nothing to verify.
  defp verify_session(%State{locks: locks} = state) when map_size(locks) == 0, do: state

  defp verify_session(%State{} = state) do
    %State{query_timeout: query_timeout} = state

    case with_pool(state, &Storage.Lock.session_backend_pid(&1, timeout: query_timeout)) do
      {:ok, backend_pid} ->
        session_seen(state, backend_pid)

      {:error, error} ->
        Logger.warning(
          "Advisory lock session could not be verified, releasing all locks due to: " <>
            inspect(error)
        )

        %State{locks: locks} = state

        release_from_tracking(locks)

        %State{state | locks: %{}, backend_pid: nil}
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

    release_from_tracking(lost)

    %State{state | locks: Map.new(kept), backend_pid: backend_pid}
  end

  # Stop watching the owners of locks that are no longer held, and tell them.
  defp release_from_tracking(locks) do
    for {owner_ref, _lock} <- locks, do: Process.demonitor(owner_ref, [:flush])

    notify_lost_locks(locks, :session_lost)
  end

  defp notify_lost_locks(locks, reason) do
    for {_ref, %Lock{} = lock} <- locks do
      %Lock{owner: owner, ref: ref} = lock

      :ok = Process.send(owner, {__MODULE__, :lock_released, ref, reason}, [])
    end

    :ok
  end

  # A lock taken on an earlier session is no longer held, and unlocking its key on
  # the current session would only make Postgres warn that it is not held.
  defp release_lock(%Lock{backend_pid: backend_pid}, %State{backend_pid: current})
       when backend_pid != current,
       do: :ok

  defp release_lock(%Lock{key: key}, %State{} = state) do
    %State{query_timeout: query_timeout, schema: schema} = state

    with_pool(state, &Storage.Lock.unlock(&1, key, schema: schema, timeout: query_timeout))
  end

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
