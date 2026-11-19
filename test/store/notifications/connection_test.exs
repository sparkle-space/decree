defmodule Decree.Store.Notifications.ConnectionTest do
  # decree's own LISTEN connection, driven directly (sparkle-space/sparklespace#8487).
  #
  # The connection runs behind a `TcpProxy` that taps what it sends and receives and can
  # fail, drop or close the chunk that carries its LISTEN. `idle_interval` is the keepalive
  # interval and `timeout` bounds every query the connection issues.
  use ExUnit.Case, async: false

  alias Decree.Store.{Config, TcpProxy, Wait}
  alias Decree.Store.Notifications.Connection

  setup context do
    pg = TestEventStore |> Config.parsed(:decree) |> Config.default_postgrex_opts()
    plan = start_supervised!({Agent, fn -> Map.get(context, :plan, []) end})

    proxy =
      start_supervised!(
        {TcpProxy,
         upstream: {Keyword.fetch!(pg, :hostname), Keyword.fetch!(pg, :port)},
         tap: self(),
         tap_received: true,
         filter_sent: listen_filter(plan)}
      )

    listener = Module.concat(__MODULE__, "Listener#{System.unique_integer([:positive])}")
    if Map.get(context, :listener_registered, true), do: Process.register(self(), listener)

    channel = Map.get(context, :channel, "connection_#{System.unique_integer([:positive])}")

    opts =
      Keyword.merge(pg,
        hostname: "127.0.0.1",
        port: TcpProxy.port(proxy),
        auto_reconnect: true,
        sync_connect: true,
        idle_interval: Map.get(context, :idle_interval, 60_000),
        timeout: Map.get(context, :query_timeout, 1_000),
        listener: listener,
        channel: channel
      )

    conn =
      start_supervised!(%{
        id: Connection,
        start: {Connection, :start_link, [opts]},
        restart: :temporary
      })

    notifier = start_supervised!({Postgrex, pg})

    [proxy: proxy, conn: conn, notifier: notifier, listener: listener, channel: channel]
  end

  describe "LISTEN" do
    @describetag listener_registered: false

    test "waits for the Listener's call while no Listener is registered", ctx do
      refute_receive {:tcp_proxy_sent, <<?Q, _len::32, "LISTEN ", _rest::binary>>}, 300

      Process.register(self(), ctx.listener)

      assert Connection.listen(ctx.conn, 5_000) == :ok
      assert_receive {:tcp_proxy_sent, <<?Q, _len::32, "LISTEN ", _rest::binary>>}
      assert_delivered(ctx)
    end

    @tag listener_registered: true
    test "runs on connect by itself once the Listener is registered", ctx do
      assert_receive {:tcp_proxy_sent, <<?Q, _len::32, "LISTEN ", _rest::binary>>}, 2_000
      assert_delivered(ctx)
    end

    @tag listener_registered: true, channel: ~s(quote"d.events)
    test "quotes the channel as Postgrex.Notifications does", ctx do
      assert Connection.listen(ctx.conn, 5_000) == :ok
      assert_delivered(ctx)
    end

    @tag plan: [:error]
    test "that fails never reports ready, and ends the connection", ctx do
      ref = Process.monitor(ctx.conn)

      assert Connection.listen(ctx.conn, 5_000) == :eventually
      assert_receive {:DOWN, ^ref, :process, _pid, {:shutdown, _reason}}
    end

    @tag plan: [:close]
    test "dropped by a disconnect answers the pending caller, then listens again", ctx do
      assert Connection.listen(ctx.conn, 5_000) == :eventually

      Process.register(self(), ctx.listener)

      Wait.until(5_000, fn -> assert Connection.listen(ctx.conn, 5_000) == :ok end)
      assert_delivered(ctx)
    end

    @tag plan: [:drop], query_timeout: 300
    test "that hangs is ended by the watchdog", ctx do
      ref = Process.monitor(ctx.conn)

      catch_exit(Connection.listen(ctx.conn, 5_000))
      assert_receive {:DOWN, ^ref, :process, _pid, :killed}, 2_000
    end
  end

  describe "a store timeout of :infinity" do
    # The store's config parser accepts `timeout: :infinity`: then no query is bounded,
    # this connection's included.
    @tag query_timeout: :infinity, idle_interval: 50
    test "listens, keeps alive and delivers", ctx do
      assert Connection.listen(ctx.conn, 5_000) == :ok
      :ok = flush_taps()

      assert_receive {:tcp_proxy_sent, <<?Q, _len::32, "SELECT 1", 0>>}
      assert_delivered(ctx)
    end
  end

  describe "keepalive" do
    @tag idle_interval: 50
    test "a notification that arrives during the keepalive query is delivered", ctx do
      assert Connection.listen(ctx.conn, 5_000) == :ok
      :ok = flush_taps()

      :ok = TcpProxy.pause_sent(ctx.proxy)
      await_paused()
      assert_receive {:tcp_proxy_sent, <<?Q, _len::32, "SELECT 1", 0>>}

      notify(ctx, "during keepalive")
      assert_receive {:tcp_proxy_received, <<?A, _rest::binary>>}

      :ok = TcpProxy.resume_sent(ctx.proxy)

      channel = ctx.channel
      assert_receive {:notification, _conn, _ref, ^channel, "during keepalive"}
    end

    @tag idle_interval: 50
    test "the connection sends no idle ping", ctx do
      assert Connection.listen(ctx.conn, 5_000) == :ok
      :ok = flush_taps()

      Process.sleep(500)

      refute Enum.any?(sent_taps(), &match?(<<?S, 4::32, _rest::binary>>, &1))

      # The ping is SimpleConnection's idle timer, which a keepalive on the same interval
      # can outrun, so the taps alone may miss one: the timer itself is off, whatever
      # `idle_interval` the options carried.
      assert {_, %{idle_interval: :infinity}} = :sys.get_state(ctx.conn)
    end

    @tag idle_interval: 50, query_timeout: 300
    test "a keepalive that never answers ends the connection", ctx do
      assert Connection.listen(ctx.conn, 5_000) == :ok
      ref = Process.monitor(ctx.conn)

      :ok = TcpProxy.pause_sent(ctx.proxy)

      assert_receive {:DOWN, ^ref, :process, _pid, :killed}, 2_000
    end

    @tag idle_interval: 50, query_timeout: 300
    test "a disconnect during a keepalive leaves no armed watchdog", ctx do
      assert Connection.listen(ctx.conn, 5_000) == :ok
      ref = Process.monitor(ctx.conn)
      port = socket(ctx.conn)
      :ok = flush_taps()

      # The keepalive is in flight, its watchdog armed, when the connection drops.
      :ok = TcpProxy.pause_sent(ctx.proxy)
      await_paused()
      assert_receive {:tcp_proxy_sent, <<?Q, _len::32, "SELECT 1", 0>>}

      :ok = :gen_tcp.shutdown(port, :read_write)
      :ok = TcpProxy.resume_sent(ctx.proxy)

      # Reconnected, and still alive well past the 300 ms timeout.
      refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 1_500
      assert socket(ctx.conn) != port
    end

    test "a keepalive timer from an earlier connection is ignored", ctx do
      assert Connection.listen(ctx.conn, 5_000) == :ok
      %{ref: earlier} = connection_state(ctx.conn)

      :ok = :gen_tcp.shutdown(socket(ctx.conn), :read_write)

      Wait.until(5_000, fn ->
        assert %{phase: :listening, ref: current} = connection_state(ctx.conn)
        assert current != earlier
      end)

      %{ref: current} = connection_state(ctx.conn)
      :ok = flush_taps()

      send(ctx.conn, {:keepalive, earlier})
      refute_receive {:tcp_proxy_sent, <<?Q, _len::32, "SELECT 1", 0>>}, 300

      send(ctx.conn, {:keepalive, current})
      assert_receive {:tcp_proxy_sent, <<?Q, _len::32, "SELECT 1", 0>>}
    end
  end

  # Rewrites the next LISTEN chunks by plan: `:error` turns it into a query that fails,
  # `:drop` holds it from the server for good, `:close` closes the connection. Once the
  # plan is spent, a LISTEN goes through.
  defp listen_filter(plan) do
    fn
      <<?Q, _len::32, text::binary>> = data ->
        if String.contains?(text, "LISTEN ") do
          case Agent.get_and_update(plan, &next/1) do
            :forward -> data
            :error -> query("SELECT 1/0")
            other -> other
          end
        else
          data
        end

      data ->
        data
    end
  end

  defp next([action | rest]), do: {action, rest}
  defp next([]), do: {:forward, []}

  defp query(text), do: <<?Q, byte_size(text) + 5::32, text::binary, 0>>

  defp assert_delivered(ctx) do
    channel = ctx.channel
    payload = "delivered #{System.unique_integer([:positive])}"

    Wait.until(5_000, fn ->
      notify(ctx, payload)
      assert_receive {:notification, _conn, _ref, ^channel, ^payload}, 100
    end)
  end

  defp notify(ctx, payload) do
    Postgrex.query!(ctx.notifier, "SELECT pg_notify($1, $2)", [ctx.channel, payload])
  end

  defp connection_state(conn) do
    {_, %{state: {Connection, state}}} = :sys.get_state(conn)
    state
  end

  defp socket(conn) do
    {_, %{protocol: %{sock: {:gen_tcp, port}}}} = :sys.get_state(conn)
    port
  end

  defp sent_taps(acc \\ []) do
    receive do
      {:tcp_proxy_sent, data} -> sent_taps([data | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp await_paused do
    receive do
      {:tcp_proxy_paused, _pump} -> :ok
      {:tcp_proxy_sent, _data} -> await_paused()
      {:tcp_proxy_received, _data} -> await_paused()
    after
      5_000 -> flunk("the proxied connection did not pause")
    end
  end

  # The tapped handshake carries authentication bytes, which a failing assertion would print.
  defp flush_taps do
    receive do
      {:tcp_proxy_sent, _data} -> flush_taps()
      {:tcp_proxy_received, _data} -> flush_taps()
    after
      0 -> :ok
    end
  end
end
