defmodule Decree.Store.Notifications.NotificationsReconnectTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{EventFactory, PubSub, ProcessHelper, Wait}
  alias Decree.Store.Notifications.Connection

  describe "notifications reconnect" do
    test "resume after disconnect" do
      stream_uuid = "example-stream"

      :ok = PubSub.subscribe(TestEventStore, stream_uuid)

      sock = shutdown_postgrex_notifications_connection(TestEventStore.Postgrex.Notifications)

      # Wait for notifications to reconnect on a new socket and listen again
      Wait.until(5_000, fn ->
        assert socket(TestEventStore.Postgrex.Notifications) not in [nil, sock]
        assert Connection.listen(TestEventStore.Postgrex.Notifications, 1_000) == :ok
      end)

      :ok = append_events(stream_uuid, 3)

      assert_receive {:events, events}
      assert length(events) == 3

      refute_receive {:events, _events}
    end

    test "publisher handle Postgrex connection down when reading events", %{conn: conn} do
      stream_uuid = "example-stream"

      :ok = PubSub.subscribe(TestEventStore, stream_uuid)

      :ok = append_events(stream_uuid, 3)

      assert_receive {:events, events}
      assert length(events) == 3

      shutdown_postgrex_connection(TestEventStore.Postgrex)

      Wait.until(fn ->
        Postgrex.query!(conn, "select pg_notify($1, $2);", [
          "public.events",
          "example-stream,1,1,3"
        ])

        assert_receive {:events, events}
        assert length(events) == 3
      end)
    end
  end

  defp shutdown_postgrex_connection(name) do
    pid = Process.whereis(name)
    assert is_pid(pid)

    ProcessHelper.shutdown(pid)
  end

  defp shutdown_postgrex_notifications_connection(name) do
    conn = Process.whereis(name)
    assert is_pid(conn)

    {_, %{protocol: %{sock: {:gen_tcp, sock}}}} = :sys.get_state(conn)

    :ok = :gen_tcp.shutdown(sock, :read_write)
    sock
  end

  defp socket(name) do
    case :sys.get_state(name) do
      {_, %{protocol: %{sock: {:gen_tcp, sock}}}} -> sock
      _ -> nil
    end
  end

  defp append_events(stream_uuid, count, expected_version \\ 0) do
    events = EventFactory.create_events(count, expected_version)

    TestEventStore.append_to_stream(stream_uuid, expected_version, events)
  end
end
