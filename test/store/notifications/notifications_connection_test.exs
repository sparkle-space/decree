defmodule Decree.Store.Notifications.NotificationsConnectionTest do
  # The store's LISTEN connection under `Notifications.Supervisor`
  # (sparkle-space/sparklespace#8487).
  #
  # The notifications connection runs behind a `TcpProxy` that taps what it sends and
  # receives and can fail, drop or close the chunk that carries a LISTEN; the publisher
  # reads through its own connection. A notification counts as delivered when the
  # store's pubsub broadcasts the events it names. `idle_interval` is what the store's
  # config sets, and `timeout` bounds the connection's queries.
  use Decree.Store.StorageCase

  alias Decree.Store.{Config, EventFactory, Notifications, PubSub, TcpProxy}

  @connection Module.concat([ES, "Postgrex", "Notifications"])
  @listener Module.concat([ES, Notifications.Listener])

  describe "liveness" do
    test "the connection sends no idle ping" do
      start_store(idle_interval: 50)
      :ok = flush_taps()

      Process.sleep(500)

      refute Enum.any?(sent_taps(), &match?(<<?S, 4::32, _rest::binary>>, &1))
    end

    test "a notification that arrives during the keepalive is delivered" do
      %{proxy: proxy} = start_store(idle_interval: 50)
      stream_uuid = subscribe()
      :ok = flush_taps()

      :ok = TcpProxy.pause_sent(proxy)
      await_paused()
      assert_receive {:tcp_proxy_sent, <<?Q, _len::32, "SELECT 1", 0>>}, 1_000

      :ok = append_events(stream_uuid, 3)
      assert_receive {:tcp_proxy_received, <<?A, _rest::binary>>}

      :ok = TcpProxy.resume_sent(proxy)

      assert_receive {:events, [_, _, _]}
    end

    test "a keepalive that never answers ends the connection, and the restarted one relistens" do
      %{proxy: proxy} = start_store(idle_interval: 50, timeout: 1_000)
      listener = Process.whereis(@listener)
      ref = Process.monitor(Process.whereis(@connection))

      :ok = TcpProxy.pause_sent(proxy)
      assert_receive {:DOWN, ^ref, :process, _pid, :killed}, 5_000
      :ok = TcpProxy.resume_sent(proxy)

      assert_delivered()
      assert Process.whereis(@listener) == listener
    end
  end

  describe "a LISTEN that fails at start" do
    for action <- [:error, :close, :drop] do
      @tag action: action
      test "by #{action} leaves the store up and delivers a later notification", %{
        action: action
      } do
        start_store(plan: [action], timeout: 1_000)
        listener = Process.whereis(@listener)

        assert_delivered()
        assert Process.whereis(@listener) == listener
      end
    end
  end

  describe "a LISTEN that keeps failing" do
    # A server in recovery, such as a hot standby, refuses every LISTEN.
    test "is retried, at most every half second" do
      start_store(plan: List.duplicate(:error, 1_000), timeout: 1_000)
      Process.sleep(100)
      _ = sent_taps()

      Process.sleep(2_000)

      listens = Enum.count(sent_taps(), &match?(<<?Q, _len::32, "LISTEN ", _rest::binary>>, &1))
      assert listens in 2..5
    end
  end

  describe "a LISTEN that fails on a reconnect" do
    for action <- [:error, :close, :drop] do
      @tag action: action
      test "by #{action} leaves the store up and delivers a later notification", %{
        action: action
      } do
        %{plan: plan} = start_store(timeout: 1_000)
        listener = Process.whereis(@listener)

        Agent.update(plan, fn _ -> [action] end)
        :ok = :gen_tcp.shutdown(socket(), :read_write)

        assert_receive {:tcp_proxy_sent, <<?Q, _len::32, text::binary>>}
                       when binary_part(text, 0, 3) == "DO " or
                              binary_part(text, 0, 7) == "LISTEN ",
                       5_000

        assert_delivered()
        assert Process.whereis(@listener) == listener
      end
    end
  end

  defp start_store(opts) do
    config = TestEventStore.config()
    plan = start_supervised!({Agent, fn -> Keyword.get(opts, :plan, []) end})

    proxy =
      start_supervised!(
        {TcpProxy,
         upstream: {Keyword.fetch!(config, :hostname), Keyword.fetch!(config, :port)},
         tap: self(),
         tap_received: true,
         filter_sent: listen_filter(plan)}
      )

    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(config)})

    store_config =
      config
      |> Keyword.merge(hostname: "127.0.0.1", port: TcpProxy.port(proxy), conn: conn)
      |> Keyword.merge(Keyword.take(opts, [:idle_interval, :timeout]))

    start_supervised!({Notifications.Supervisor, {ES, store_config}})

    for child_spec <- PubSub.child_spec(ES), do: start_supervised!(child_spec)

    %{proxy: proxy, plan: plan}
  end

  # Rewrites the next LISTEN chunks by plan: `:error` turns it into a query that fails,
  # `:drop` holds it from the server for good, `:close` closes the connection. Once the
  # plan is spent, a LISTEN goes through. `Postgrex.Notifications` relistens with a `DO`
  # block, which this matches too.
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

  defp subscribe do
    stream_uuid = "listen-connection-#{System.unique_integer([:positive])}"
    :ok = PubSub.subscribe(ES, stream_uuid)
    stream_uuid
  end

  # Appends one event at a time until the store broadcasts one: a notification sent before
  # the connection listens again is not delivered, and is not what this asserts.
  defp assert_delivered do
    stream_uuid = subscribe()
    deadline = System.monotonic_time(:millisecond) + 10_000

    assert_delivered(stream_uuid, 0, deadline)
  end

  defp assert_delivered(stream_uuid, version, deadline) do
    :ok = append_events(stream_uuid, 1, version)

    receive do
      {:events, [_ | _]} -> :ok
    after
      200 ->
        if System.monotonic_time(:millisecond) > deadline,
          do: flunk("no notification was delivered"),
          else: assert_delivered(stream_uuid, version + 1, deadline)
    end
  end

  defp socket do
    {_, %{protocol: %{sock: {:gen_tcp, port}}}} = :sys.get_state(@connection)
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

  defp append_events(stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version)

    TestEventStore.append_to_stream(stream_uuid, expected_version, events)
  end
end
