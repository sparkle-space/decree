defmodule Decree.Store.Notifications.PublisherUndecodableEventTest do
  use Decree.Store.StorageCase

  # An event whose stored bytes the store's serializer cannot decode is a raise in the
  # publisher's deserialize, not an error it is answered: the publisher stops that
  # notification's batch at the event, broadcasts the events before it and keeps running, so
  # the notifications tree (`one_for_all`) is not restarted, and a run of them does not
  # exhaust its restart intensity and restart the whole store tree under a concurrent append
  # (sparkle-space/sparklespace#8349).
  #
  # Each event is planted through `Appender.append/4` with bytes the JSON serializer cannot
  # decode, a real append, so the `streams` table's `event_notification` trigger fires and the
  # publisher reads it.

  import ExUnit.CaptureLog

  alias Decree.Store.{EventFactory, PubSub, RecordedEvent, UUID}
  alias Decree.Store.Notifications.Publisher
  alias Decree.Store.Storage.Appender

  @marker "undecodable-payload-marker"
  @publisher Module.concat([TestEventStore, Publisher])

  test "an undecodable event is skipped and the publisher publishes the next append", %{
    conn: conn,
    schema: schema
  } do
    publisher = Process.whereis(@publisher)
    ref = Process.monitor(publisher)

    bad = UUID.uuid4()
    good = UUID.uuid4()
    :ok = PubSub.subscribe(TestEventStore, good)

    log =
      capture_log(fn ->
        plant_undecodable(conn, schema, bad)

        :ok = TestEventStore.append_to_stream(good, 0, EventFactory.create_events(1))

        # The publisher takes one notification at a time, in order: the good append's
        # broadcast comes after it handled both of the planted event's notifications.
        assert_receive {:events, [%RecordedEvent{stream_uuid: ^good}]}
      end)

    refute_received {:DOWN, ^ref, :process, _pid, _reason}
    assert Process.whereis(@publisher) == publisher

    assert log =~
             "Decree.Store notifications failed to deserialize event 1 of " <>
               inspect(suffix(bad)) <> " due to: Jason.DecodeError"

    refute log =~ @marker
    refute log =~ bad
  end

  test "the events before an undecodable one in the same append are broadcast", %{
    conn: conn,
    schema: schema
  } do
    publisher = Process.whereis(@publisher)
    ref = Process.monitor(publisher)

    stream_uuid = UUID.uuid4()
    :ok = PubSub.subscribe(TestEventStore, stream_uuid, notices: true)

    [first, second, third] = EventFactory.create_recorded_events(3, stream_uuid)
    second = %RecordedEvent{second | data: <<0>> <> @marker}

    log =
      capture_log(fn ->
        # One append, so one notification for the stream, and no later append.
        :ok = Appender.append(conn, nil, [first, second, third], schema: schema)

        assert_receive {:events, [%RecordedEvent{stream_uuid: ^stream_uuid} = event]}
        assert %RecordedEvent{stream_version: 1, data: %EventFactory.Event{event: 1}} = event
      end)

    refute_receive {:events, _events}, 300
    refute_received {:events_pending, _event_number}

    refute_received {:DOWN, ^ref, :process, _pid, _reason}
    assert Process.whereis(@publisher) == publisher

    assert log =~
             "Decree.Store notifications failed to deserialize event 2 of " <>
               inspect(suffix(stream_uuid)) <> " due to: Jason.DecodeError"

    refute log =~ @marker
    refute log =~ stream_uuid
  end

  test "a run of undecodable events does not restart the store's tree", %{
    conn: conn,
    schema: schema
  } do
    # A restart of the store's tree reaches the test as an exit signal: trapped, so the
    # assertions below report what changed.
    Process.flag(:trap_exit, true)

    before = store_children()

    probe = UUID.uuid4()
    writer = UUID.uuid4()

    capture_log(fn ->
      appends =
        Task.async(fn ->
          for version <- 0..199 do
            try do
              TestEventStore.append_to_stream(writer, version, EventFactory.create_events(1))
            catch
              :exit, reason -> {:exit, reason}
            end
          end
        end)

      # Each plant lands once the notifications are delivered again (a probe append is
      # broadcast), so on code that crashes each is one restart of the notifications tree,
      # and five in well under five seconds exceed its intensity (3 in 5 s).
      for _plant <- 1..5 do
        plant_undecodable(conn, schema, UUID.uuid4())
        await_published(probe)
      end

      results = Task.await(appends, 30_000)

      assert Enum.uniq(results) == [:ok]
    end)

    assert store_children() == before

    assert :ok = TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(1))
  end

  defp suffix(stream_uuid), do: "..." <> String.slice(stream_uuid, -8, 8)

  # A real append of one event of a known type whose data is not JSON.
  defp plant_undecodable(conn, schema, stream_uuid) do
    [event] = EventFactory.create_recorded_events(1, stream_uuid)

    :ok =
      Appender.append(conn, nil, [%RecordedEvent{event | data: <<0>> <> @marker}], schema: schema)
  end

  # Append to the probe stream until an append is broadcast, subscribing again each time: a
  # restarted store tree starts a new PubSub registry. Each probe event has its own id, so a
  # late broadcast of an earlier probe does not count. Gives up after three seconds, so a tree
  # that does not come back fails on the children below, not here.
  defp await_published(probe, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 3_000
    event_id = UUID.uuid4()

    _ = PubSub.subscribe(TestEventStore, probe)

    _ =
      TestEventStore.append_to_stream(probe, :any_version, [EventFactory.create_event(event_id)])

    receive do
      {:events, [%RecordedEvent{event_id: ^event_id}]} -> :ok
    after
      200 ->
        if System.monotonic_time(:millisecond) < deadline,
          do: await_published(probe, deadline),
          else: :gave_up
    end
  end

  # The store's top supervisor's children and the notifications supervisor's, by id: a
  # restart of either tree changes a pid.
  defp store_children do
    top = Supervisor.which_children(TestEventStore)

    notifications =
      Enum.find_value(top, fn {id, pid, _type, _modules} ->
        if id == Module.concat(TestEventStore, Decree.Store.Notifications.Supervisor), do: pid
      end)

    %{
      top: Map.new(top, fn {id, pid, _type, _modules} -> {id, pid} end),
      notifications:
        notifications
        |> Supervisor.which_children()
        |> Map.new(fn {id, pid, _type, _modules} -> {id, pid} end)
    }
  end
end
