defmodule Decree.Store.TcpProxy do
  @moduledoc false

  # A TCP proxy for tests, in front of the test database. It listens on an ephemeral
  # port on 127.0.0.1 and, per accepted connection:
  #
  #   - `:forward` (the start mode): connects to the upstream and copies bytes both
  #     ways until either side closes;
  #   - `:refuse`: closes the connection at once, so a database client fails to
  #     connect;
  #   - `:hold`: leaves the connection unanswered until the mode changes, so a database
  #     client stays in its connect, for as long as a test needs;
  #   - `{:delay, delay}`: forwards this connection after `delay` milliseconds, and the
  #     mode is `:forward` again from the next one, which is not held up by it.
  #
  # With `tap: pid`, every chunk a client sends is also sent to `pid` as
  # `{:tcp_proxy_sent, data}` before it is forwarded, so a test can read the statements a
  # connection sends (a query's text is in its Parse message). With `tap_received: true`
  # as well, every chunk the server sends is sent to `pid` as
  # `{:tcp_proxy_received, data}` before it is forwarded.
  #
  # `pause_sent/1` holds what clients send: from when it returns, every forwarded
  # connection, and every one forwarded later, queues its client's chunks (still
  # tapped) instead of forwarding them, while the server's chunks still flow. It
  # returns once every live connection has paused, and each one, as it pauses, sends
  # the tap `{:tcp_proxy_paused, pump}`, after the taps of the chunks it forwarded and
  # before those of the chunks it holds. `resume_sent/1` forwards the held chunks in
  # order and forwarding goes on as before.
  #
  # Changing the mode affects new connections only; connections already forwarded
  # stay up. Connections are accepted one at a time, so a held one is handled by the
  # mode that follows `:hold`, and those behind it wait in the listen backlog.

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc "The port the proxy listens on."
  def port(proxy), do: GenServer.call(proxy, :port)

  @doc "Close new connections at once."
  def refuse(proxy), do: GenServer.call(proxy, {:mode, :refuse})

  @doc "Forward new connections to the upstream."
  def forward(proxy), do: GenServer.call(proxy, {:mode, :forward})

  @doc "Leave new connections unanswered until the mode changes."
  def hold(proxy), do: GenServer.call(proxy, {:mode, :hold})

  @doc "Forward the next connection after `delay` milliseconds."
  def delay_next(proxy, delay), do: GenServer.call(proxy, {:mode, {:delay, delay}})

  @doc "Hold what clients send, once every live connection has paused."
  def pause_sent(proxy) do
    ref = make_ref()

    for pump <- GenServer.call(proxy, {:pause_sent, self(), ref}) do
      monitor = Process.monitor(pump)

      receive do
        {^ref, ^pump} -> Process.demonitor(monitor, [:flush])
        {:DOWN, ^monitor, :process, ^pump, _reason} -> :ok
      end
    end

    :ok
  end

  @doc "Forward the held client chunks in order, and forward as before."
  def resume_sent(proxy), do: GenServer.call(proxy, :resume_sent)

  @impl true
  def init(opts) do
    upstream = Keyword.fetch!(opts, :upstream)
    tap = {Keyword.get(opts, :tap), Keyword.get(opts, :tap_received, false)}

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, port} = :inet.port(listen)

    proxy = self()
    acceptor = spawn_link(fn -> accept(proxy, listen, upstream, tap) end)

    {:ok,
     %{
       listen: listen,
       port: port,
       mode: :forward,
       acceptor: acceptor,
       held: nil,
       pumps: MapSet.new(),
       paused: false
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  # A pump registers once connected, before it copies anything, and learns whether to
  # start paused.
  @impl true
  def handle_call({:register, pump}, _from, state) do
    Process.monitor(pump)

    {:reply, state.paused, %{state | pumps: MapSet.put(state.pumps, pump)}}
  end

  @impl true
  def handle_call({:pause_sent, caller, ref}, _from, state) do
    for pump <- state.pumps, do: send(pump, {:pause_sent, caller, ref})

    {:reply, MapSet.to_list(state.pumps), %{state | paused: true}}
  end

  @impl true
  def handle_call(:resume_sent, _from, state) do
    for pump <- state.pumps, do: send(pump, :resume_sent)

    {:reply, :ok, %{state | paused: false}}
  end

  # The acceptor asks for the mode of the connection it just accepted. While held, it
  # is answered by the next mode change.
  @impl true
  def handle_call(:mode, from, %{mode: :hold} = state), do: {:noreply, %{state | held: from}}

  @impl true
  def handle_call(:mode, _from, %{mode: {:delay, _delay}} = state) do
    {:reply, state.mode, %{state | mode: :forward}}
  end

  @impl true
  def handle_call(:mode, _from, state), do: {:reply, state.mode, state}

  @impl true
  def handle_call({:mode, mode}, _from, %{held: nil} = state) do
    {:reply, :ok, %{state | mode: mode}}
  end

  @impl true
  def handle_call({:mode, :hold}, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_call({:mode, mode}, _from, %{held: held} = state) do
    GenServer.reply(held, mode)

    {:reply, :ok, %{state | mode: mode, held: nil}}
  end

  @impl true
  def handle_info({:DOWN, _monitor, :process, pump, _reason}, state) do
    {:noreply, %{state | pumps: MapSet.delete(state.pumps, pump)}}
  end

  # Runs linked to the proxy; each forwarded connection's pump is linked to it, so
  # stopping the proxy closes every connection.
  defp accept(proxy, listen, upstream, tap) do
    {:ok, client} = :gen_tcp.accept(listen)

    case GenServer.call(proxy, :mode, :infinity) do
      :refuse ->
        :gen_tcp.close(client)

      :forward ->
        start_pump(proxy, client, upstream, 0, tap)

      {:delay, delay} ->
        start_pump(proxy, client, upstream, delay, tap)
    end

    accept(proxy, listen, upstream, tap)
  end

  defp start_pump(proxy, client, upstream, delay, tap) do
    pump = spawn_link(fn -> pump(proxy, client, upstream, delay, tap) end)
    _ = :gen_tcp.controlling_process(client, pump)
    send(pump, :go)
  end

  defp pump(proxy, client, {host, port}, delay, tap) do
    receive do
      :go -> :ok
    end

    Process.sleep(delay)

    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: true]) do
      {:ok, server} ->
        held =
          if GenServer.call(proxy, {:register, self()}) do
            {sent_tap, _received_tap} = tap
            if sent_tap, do: send(sent_tap, {:tcp_proxy_paused, self()})
            []
          end

        _ = :inet.setopts(client, active: true)
        copy(client, server, tap, held)

      {:error, _reason} ->
        :gen_tcp.close(client)
    end
  end

  # `held` is `nil` while forwarding, and the client chunks held so far, newest first,
  # while paused.
  defp copy(client, server, {sent_tap, received_tap} = tap, held) do
    receive do
      {:tcp, ^client, data} ->
        if sent_tap, do: send(sent_tap, {:tcp_proxy_sent, data})

        if held do
          copy(client, server, tap, [data | held])
        else
          _ = :gen_tcp.send(server, data)
          copy(client, server, tap, held)
        end

      {:tcp, ^server, data} ->
        if sent_tap && received_tap, do: send(sent_tap, {:tcp_proxy_received, data})

        _ = :gen_tcp.send(client, data)
        copy(client, server, tap, held)

      {:pause_sent, caller, ref} ->
        if sent_tap, do: send(sent_tap, {:tcp_proxy_paused, self()})

        send(caller, {ref, self()})
        copy(client, server, tap, held || [])

      :resume_sent ->
        for data <- Enum.reverse(held || []), do: :gen_tcp.send(server, data)

        copy(client, server, tap, nil)

      {:tcp_closed, _socket} ->
        :gen_tcp.close(client)
        :gen_tcp.close(server)

      {:tcp_error, _socket, _reason} ->
        :gen_tcp.close(client)
        :gen_tcp.close(server)
    end
  end
end
