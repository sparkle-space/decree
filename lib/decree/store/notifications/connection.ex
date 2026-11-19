defmodule Decree.Store.Notifications.Connection do
  @moduledoc """
  **Temporary.** This module stands in for `Postgrex.Notifications` because Postgrex
  0.22.4, the newest release on Hex, drops a notification that arrives while its idle ping
  runs. The fix, elixir-ecto/postgrex#795 (commit `bd402a5310`), is in no release yet.
  Remove this module, and go back to `Postgrex.Notifications`, at the first Hex Postgrex
  release that contains `bd402a5310` (expected to be 0.23). sparkle-space/sparklespace#8532
  tracks that clean-up. Before removing it, check that the release's
  `Postgrex.Notifications` also covers what 0.22.4's lacks besides the ping:
    * it answers a failed `LISTEN` `{:ok, ref}` and never listens on that channel;
    * a `LISTEN` it issues on a reconnect that loses its connection strands it: 0.22.4's
      `SimpleConnection` drops the reconnect after a query issued from `handle_connect/1`
      disconnects;
    * a query waits for its answer with no timeout.

  The event store's `LISTEN` connection, a `Postgrex.SimpleConnection` that listens on one
  channel and sends each notification to the `Decree.Store.Notifications.Listener` as
  `{:notification, pid, nil, channel, payload}`.

  On every connect it listens by itself once the Listener is registered; until then the
  Listener's `listen/2` call issues the `LISTEN`. `listen/2` answers `:ok` once the
  connection listens, and `:eventually` while it is disconnected, or when the `LISTEN` it
  issued fails or is lost to a disconnect.

  The connection sends no idle ping. While it listens it runs `SELECT 1` every
  `:idle_interval` milliseconds (default 5000), and a notification that arrives during
  that query is delivered. Every query it issues is bounded by `:timeout` (default
  15000): a query still unanswered then kills the process. A query that fails ends the
  process with a `{:shutdown, reason}` exit. The process that started it (the store's
  `MonitoredServer`) starts it again.
  """

  @behaviour Postgrex.SimpleConnection

  require Logger

  alias Decree.Store.LogTerms
  alias Postgrex.SimpleConnection

  @keepalive_interval 5_000
  @timeout 15_000

  def start_link(opts) do
    {listener, opts} = Keyword.pop!(opts, :listener)
    {channel, opts} = Keyword.pop!(opts, :channel)
    {interval, opts} = Keyword.pop(opts, :idle_interval, @keepalive_interval)
    timeout = Keyword.get(opts, :timeout, @timeout)

    args = %{listener: listener, channel: channel, interval: interval, timeout: timeout}

    SimpleConnection.start_link(__MODULE__, args, Keyword.put(opts, :idle_interval, :infinity))
  end

  @doc """
  Asks the connection to listen. `:ok` once it listens, `:eventually` when it will listen
  after it reconnects.
  """
  @spec listen(GenServer.server(), timeout) :: :ok | :eventually
  def listen(conn, timeout), do: SimpleConnection.call(conn, :listen, timeout)

  @impl true
  def init(args) do
    {:ok, Map.merge(args, %{phase: :disconnected, ref: nil, from: nil, watchdog: nil})}
  end

  @impl true
  def handle_connect(state) do
    ref = make_ref()
    send(self(), {:listen, ref})

    {:noreply, %{state | phase: :connected, ref: ref}}
  end

  @impl true
  def handle_disconnect(state) do
    state = state |> disarm() |> reply(:eventually)

    {:noreply, %{state | phase: :disconnected}}
  end

  @impl true
  def handle_call(:listen, from, state) do
    case state.phase do
      :listening ->
        SimpleConnection.reply(from, :ok)
        {:noreply, state}

      :connected ->
        listen_query(%{state | from: from})

      :disconnected ->
        SimpleConnection.reply(from, :eventually)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:listen, ref}, %{ref: ref, phase: :connected} = state) do
    if GenServer.whereis(state.listener), do: listen_query(state), else: {:noreply, state}
  end

  def handle_info({:keepalive, ref}, %{ref: ref, phase: :listening} = state) do
    {:query, "SELECT 1", arm(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_result(results, %{phase: :connected} = state) when is_list(results) do
    state = state |> disarm() |> reply(:ok)

    {:noreply, schedule_keepalive(%{state | phase: :listening})}
  end

  def handle_result(results, %{phase: :listening} = state) when is_list(results) do
    {:noreply, state |> disarm() |> schedule_keepalive()}
  end

  def handle_result(%Postgrex.Error{} = error, state) do
    state |> disarm() |> reply(:eventually)

    {query, reason} =
      if state.phase == :connected,
        do: {"LISTEN", :listen_failed},
        else: {"keepalive", :keepalive_failed}

    Logger.warning(
      "Notifications connection #{query} query failed: " <> inspect(LogTerms.reduce(error))
    )

    exit({:shutdown, reason})
  end

  @impl true
  def notify(channel, payload, state) do
    case GenServer.whereis(state.listener) do
      nil -> :ok
      listener -> send(listener, {:notification, self(), nil, channel, payload})
    end

    :ok
  end

  defp listen_query(state) do
    {:query, ~s(LISTEN #{quote_channel(state.channel)}), arm(state)}
  end

  # As `Postgrex.Notifications` quotes a channel.
  defp quote_channel(channel), do: ~s("#{String.replace(channel, "\"", "\"\"")}")

  defp schedule_keepalive(state) do
    Process.send_after(self(), {:keepalive, state.ref}, state.interval)
    state
  end

  # A query waits for its answer with no timeout, and the process traps exits: only a kill
  # ends one that hangs.
  defp arm(state) do
    {:ok, watchdog} = :timer.kill_after(state.timeout)
    %{state | watchdog: watchdog}
  end

  defp disarm(%{watchdog: nil} = state), do: state

  defp disarm(state) do
    _ = :timer.cancel(state.watchdog)
    %{state | watchdog: nil}
  end

  defp reply(%{from: nil} = state, _reply), do: state

  defp reply(state, reply) do
    SimpleConnection.reply(state.from, reply)
    %{state | from: nil}
  end
end
