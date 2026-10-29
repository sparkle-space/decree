defmodule Decree.Store.TcpProxy do
  @moduledoc false

  # A TCP proxy for tests, in front of the test database. It listens on an ephemeral
  # port on 127.0.0.1 and, per accepted connection:
  #
  #   - `:forward` (the start mode): connects to the upstream and copies bytes both
  #     ways until either side closes;
  #   - `:refuse`: closes the connection at once, so a database client fails to
  #     connect.
  #
  # Changing the mode affects new connections only; connections already forwarded
  # stay up.

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

  def init(opts) do
    upstream = Keyword.fetch!(opts, :upstream)

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, port} = :inet.port(listen)

    proxy = self()
    acceptor = spawn_link(fn -> accept(proxy, listen, upstream) end)

    {:ok, %{listen: listen, port: port, mode: :forward, acceptor: acceptor}}
  end

  def handle_call(:port, _from, state), do: {:reply, state.port, state}
  def handle_call(:mode, _from, state), do: {:reply, state.mode, state}
  def handle_call({:mode, mode}, _from, state), do: {:reply, :ok, %{state | mode: mode}}

  # Runs linked to the proxy; each forwarded connection's pump is linked to it, so
  # stopping the proxy closes every connection.
  defp accept(proxy, listen, upstream) do
    {:ok, client} = :gen_tcp.accept(listen)

    case GenServer.call(proxy, :mode) do
      :refuse ->
        :gen_tcp.close(client)

      :forward ->
        pump = spawn_link(fn -> pump(client, upstream) end)
        _ = :gen_tcp.controlling_process(client, pump)
        send(pump, :go)
    end

    accept(proxy, listen, upstream)
  end

  defp pump(client, {host, port}) do
    receive do
      :go -> :ok
    end

    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: true]) do
      {:ok, server} ->
        _ = :inet.setopts(client, active: true)
        copy(client, server)

      {:error, _reason} ->
        :gen_tcp.close(client)
    end
  end

  defp copy(client, server) do
    receive do
      {:tcp, ^client, data} ->
        _ = :gen_tcp.send(server, data)
        copy(client, server)

      {:tcp, ^server, data} ->
        _ = :gen_tcp.send(client, data)
        copy(client, server)

      {:tcp_closed, _socket} ->
        :gen_tcp.close(client)
        :gen_tcp.close(server)

      {:tcp_error, _socket, _reason} ->
        :gen_tcp.close(client)
        :gen_tcp.close(server)
    end
  end
end
