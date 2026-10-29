defmodule Decree.Event.EventHandlerConcurrencyRedeliveryTest do
  use Decree.PostgresCase

  # An event redelivered to a sibling instance of a `concurrency: n` handler must reach
  # `handle/2`, even when the sibling has already handled a later event
  # (commanded/commanded#671, sparkle-space/sparklespace#7429).
  #
  # Instance A blocks in `handle/2` on event 1. Events 2 and 3 go to instance B, which
  # handles and acks both. A is then killed: the store requeues its in-flight event 1
  # to B, the free subscriber with the lowest `last_sent`. B's `last_seen_event` is 3,
  # which is not a stream position at concurrency > 1, so the event must not be acked
  # as already seen.

  alias Decree.{DefaultApp, EventStore}
  alias Decree.Event.{ConcurrencyEvent, Handler, Mapper}
  alias Decree.Event.{PartitionedRedeliveryEventHandler, RedeliveryEventHandler}

  setup do
    true = Process.register(self(), :test)

    start_supervised!(DefaultApp)

    :ok
  end

  test "an in-flight event redelivered to a sibling is handled, without `partition_by/2`" do
    start_supervised!(RedeliveryEventHandler)

    assert_redelivered_event_is_handled()
  end

  test "an in-flight event redelivered to a sibling is handled, with `partition_by/2`" do
    start_supervised!(PartitionedRedeliveryEventHandler)

    assert_redelivered_event_is_handled()
  end

  defp assert_redelivered_event_is_handled do
    append_events("stream1", [{1, "partition1"}])

    assert_receive {:handling, 1, blocked}

    # A different partition from event 1's, so the events are not held behind it.
    append_events("stream2", [{2, "partition2"}, {3, "partition2"}])

    assert_receive {:handling, 2, sibling}
    assert_receive {:handled, 2, ^sibling}
    assert_receive {:handling, 3, ^sibling}
    assert_receive {:handled, 3, ^sibling}
    refute sibling == blocked

    # The sibling acks an event inside the `handle_info/2` that handled it, by a
    # synchronous call to the subscription, so once this sys call returns the
    # subscription has the ack for event 3.
    assert %Handler{last_seen_event: 3} = :sys.get_state(sibling)

    Process.exit(blocked, :kill)

    assert_receive {:handling, 1, redelivered_to}
    refute redelivered_to == blocked

    send(redelivered_to, :proceed)

    assert_receive {:handled, 1, ^redelivered_to}
    refute_receive {:handling, 1, _pid}
  end

  defp append_events(stream_uuid, events) do
    events =
      events
      |> Enum.map(fn {index, partition} ->
        %ConcurrencyEvent{stream_uuid: stream_uuid, index: index, partition: partition}
      end)
      |> Mapper.map_to_event_data()

    :ok = EventStore.append_to_stream(DefaultApp, stream_uuid, :any_version, events)
  end
end
