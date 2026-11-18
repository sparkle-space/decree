defmodule Decree.Store.Notifications.IdlePingNotificationTest do
  # Regression test for a notification lost while `Postgrex.Notifications` pings its
  # idle connection (sparkle-space/sparklespace#7691; upstream elixir-ecto/postgrex#795).
  #
  # The idle ping sends a Sync and reads until ReadyForQuery. Before the fix it read
  # with no notify callback, so a NotificationResponse that came in meanwhile was read
  # and dropped. The proxy holds the ping's Sync, so the listener is still inside its
  # ping when the notification reaches it, and then lets the Sync through.
  #
  # It uses only Postgrex and the proxy, so it runs unchanged against the code before
  # the fix.
  use ExUnit.Case, async: false

  alias Decree.Store.{Config, TcpProxy}

  setup context do
    config = TestEventStore |> Config.parsed(:decree) |> Config.default_postgrex_opts()

    proxy =
      start_supervised!(
        {TcpProxy,
         upstream: {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)},
         tap: self(),
         tap_received: true}
      )

    listener_opts =
      Keyword.merge(config,
        hostname: "127.0.0.1",
        port: TcpProxy.port(proxy),
        idle_interval: Map.get(context, :idle_interval, 50),
        sync_connect: true
      )

    listener =
      start_supervised!(%{
        id: Postgrex.Notifications,
        start: {Postgrex.Notifications, :start_link, [listener_opts]}
      })

    conn = start_supervised!({Postgrex, config})

    channel = "idle_ping_#{System.unique_integer([:positive])}"
    {:ok, ref} = Postgrex.Notifications.listen(listener, channel)

    # The tapped handshake carries authentication bytes, which a failing assertion would print.
    :ok = flush_taps()

    [proxy: proxy, listener: listener, conn: conn, channel: channel, ref: ref]
  end

  test "a notification that arrives during the idle ping is delivered", %{
    proxy: proxy,
    listener: listener,
    conn: conn,
    channel: channel,
    ref: ref
  } do
    :ok = TcpProxy.pause_sent(proxy)
    await_paused()

    # The idle connection sends nothing but its pings, so this is the Sync of a ping,
    # held by the proxy.
    assert_receive {:tcp_proxy_sent, <<?S, 4::32>>}

    notify(conn, channel, "during ping")

    # The NotificationResponse is in the listener's socket while it waits for its
    # ReadyForQuery.
    assert_receive {:tcp_proxy_received, <<?A, _rest::binary>>}

    :ok = TcpProxy.resume_sent(proxy)

    assert_receive {:notification, ^listener, ^ref, ^channel, "during ping"}, 5_000
  end

  @tag idle_interval: 60_000
  test "a notification between idle pings is delivered", %{
    listener: listener,
    conn: conn,
    channel: channel,
    ref: ref
  } do
    notify(conn, channel, "between pings")

    assert_receive {:notification, ^listener, ^ref, ^channel, "between pings"}, 5_000
  end

  # Discards what the proxy tapped before the connection paused.
  defp await_paused do
    receive do
      {:tcp_proxy_paused, _pump} -> :ok
      {:tcp_proxy_sent, _data} -> await_paused()
      {:tcp_proxy_received, _data} -> await_paused()
    after
      5_000 -> flunk("the proxied connection did not pause")
    end
  end

  defp flush_taps do
    receive do
      {:tcp_proxy_sent, _data} -> flush_taps()
      {:tcp_proxy_received, _data} -> flush_taps()
    after
      0 -> :ok
    end
  end

  defp notify(conn, channel, payload) do
    Postgrex.query!(conn, "SELECT pg_notify($1, $2)", [channel, payload])
  end
end
