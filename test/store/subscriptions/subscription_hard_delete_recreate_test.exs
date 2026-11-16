defmodule Decree.Store.Subscriptions.SubscriptionHardDeleteRecreateTest do
  use Decree.Store.StorageCase

  # A hard delete removes a stream's events and its `streams` row. An append to the same
  # stream uuid afterwards creates the stream again, from version 1. A persistent
  # subscription to that one stream must deliver the recreated events, so a hard delete
  # resets the cursor of every subscription to the stream to 0, in place: the row keeps
  # its `subscription_id`, the key of its advisory lock (sparkle-space/sparklespace#8386).
  #
  # A subscription that is running when its stream is hard deleted follows the stream: it
  # drops its position and the events it has not sent, never acknowledges an event of the
  # deleted stream as one of the recreated stream, and delivers the recreated stream from
  # its first event. A `$all` subscription is never reset.

  import Decree.Store.SubscriptionHelpers, only: [wait_for_lock_pool: 1]

  alias Decree.Store.{
    DenyListSerializer,
    EventData,
    EventFactory,
    NewReleaseEvent,
    RecordedEvent,
    Storage,
    StoreRows,
    UUID,
    Wait
  }

  alias Decree.Store.Streams.Stream
  alias Decree.Store.Subscriptions.{Subscription, SubscriptionFsm, SubscriptionState}
  alias TestEventStore, as: EventStore

  @old :old_store
  @new_type "Elixir.Decree.Store.NewReleaseEvent"
  @head_check_interval 400
  @stream_recreated [:decree, :store, :subscription, :stream_recreated]

  setup do
    [subscription_name: UUID.uuid4(), stream_uuid: UUID.uuid4()]
  end

  describe "case 1: a subscription stopped across the hard delete" do
    test "delivers the recreated stream's events when it subscribes again", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      subscription = subscribe(stream_uuid, name)

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
      assert [1, 2, 3] = numbers(receive_and_ack(subscription, 3, 5_000))

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 3} = row(conn, schema, stream_uuid, name)
      end)

      %Storage.Subscription{subscription_id: subscription_id} =
        row(conn, schema, stream_uuid, name)

      :ok = EventStore.unsubscribe_from_stream(stream_uuid, name)
      refute Process.alive?(subscription)

      # The row stays after the subscription stops.
      assert %Storage.Subscription{subscription_id: ^subscription_id, last_seen: 3} =
               row(conn, schema, stream_uuid, name)

      recreated = hard_delete_and_recreate(context)

      subscription = subscribe(stream_uuid, name)
      received = receive_and_ack(subscription, 2, 2_000)

      assert data(received) == data(recreated),
             "case 1 (stopped, then resubscribed): expected the recreated events " <>
               "#{inspect(data(recreated))}, received #{inspect(data(received))}; " <>
               "row #{inspect(row(conn, schema, stream_uuid, name))}, " <>
               "cursor #{inspect(cursor(subscription))}"
    end
  end

  describe "case 2: a subscription held across the hard delete" do
    setup do
      DenyListSerializer.deny([@new_type])
      on_exit(fn -> DenyListSerializer.allow_all() end)

      start_supervised!({TestEventStore, name: @old, serializer: DenyListSerializer},
        id: :old_store
      )

      wait_for_lock_pool(@old)
      wait_for_lock_pool(TestEventStore)

      :ok
    end

    test "delivers the recreated stream's events once its probe resubscribes, and settles",
         context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      # The first probe comes within a second.
      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, name, self(),
          name: @old,
          buffer_size: 10,
          retry_interval: 5_000
        )

      assert_receive {:subscribed, ^subscription}

      # Appended through the instance that knows the new type; the old one holds before E3.
      :ok =
        EventStore.append_to_stream(stream_uuid, 0, [
          EventFactory.create_event(nil, 1),
          EventFactory.create_event(nil, 2),
          new_event(3)
        ])

      assert [1, 2] = numbers(receive_and_ack(subscription, 2, 5_000))

      Wait.until(5_000, fn -> assert fsm(subscription).state == :held end)

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 2} = row(conn, schema, stream_uuid, name)
      end)

      # The probe waits until the stream is recreated: both recreated versions are at or
      # below the row's position of 2.
      :ok = :sys.suspend(subscription)
      assert fsm(subscription).state == :held

      recreated = hard_delete_and_recreate(context)

      :ok = :sys.resume(subscription)

      received = receive_and_ack(subscription, 2, 5_000)

      assert data(received) == data(recreated),
             "case 2 (held, then probe resubscribes): expected the recreated events " <>
               "#{inspect(data(recreated))}, received #{inspect(data(received))}; " <>
               "state #{inspect(fsm(subscription).state)}, " <>
               "row #{inspect(row(conn, schema, stream_uuid, name))}, " <>
               "cursor #{inspect(cursor(subscription))}"

      # It settles in `subscribed` at the recreated stream's position, and stays there: no
      # catch-up read loops on a `last_received` above what the recreated stream holds.
      assert_settled(subscription, 2, "case 2 (held, then probe resubscribes)")

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 2} = row(conn, schema, stream_uuid, name)
      end)
    end
  end

  describe "case 3: a subscription subscribed throughout the hard delete" do
    test "delivers the recreated stream's events", context do
      assert_live_delivery(context, [], 2_000, "case 3a (live, default head check)")
    end

    test "delivers the recreated stream's events with a short head check", context do
      assert_live_delivery(
        context,
        [head_check_interval: @head_check_interval],
        3 * @head_check_interval + 500,
        "case 3b (live, head_check_interval #{@head_check_interval})"
      )
    end
  end

  describe "case 4: the subscriptions rows after a hard delete" do
    setup context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid} = context

      other_stream_uuid = UUID.uuid4()

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
      :ok = EventStore.append_to_stream(other_stream_uuid, 0, EventFactory.create_events(3))

      rows = %{
        deleted_a: create_row(conn, schema, stream_uuid, 3),
        deleted_b: create_row(conn, schema, stream_uuid, 1),
        other: create_row(conn, schema, other_stream_uuid, 3),
        all: create_row(conn, schema, "$all", 6)
      }

      :ok = hard_delete(conn, schema, stream_uuid)

      [rows: rows]
    end

    test "the deleted stream's rows are reset to 0 in place", context do
      %{conn: conn, schema: schema, rows: rows} = context

      for key <- [:deleted_a, :deleted_b] do
        %Storage.Subscription{subscription_id: subscription_id} = Map.fetch!(rows, key)

        assert [[_last_seen]] = last_seen(conn, schema, subscription_id),
               "case 4 (rows): the #{key} row #{subscription_id} is gone"
      end

      for key <- [:deleted_a, :deleted_b] do
        %Storage.Subscription{subscription_id: subscription_id, last_seen: before} =
          Map.fetch!(rows, key)

        assert last_seen(conn, schema, subscription_id) == [[0]],
               "case 4 (rows): the #{key} row #{subscription_id} was at #{before} and is at " <>
                 "#{inspect(last_seen(conn, schema, subscription_id))} after the hard " <>
                 "delete, expected [[0]]"
      end
    end

    test "rows of another stream and of $all keep their positions", context do
      %{conn: conn, schema: schema, rows: rows} = context

      for key <- [:other, :all] do
        %Storage.Subscription{subscription_id: subscription_id, last_seen: before} =
          Map.fetch!(rows, key)

        assert last_seen(conn, schema, subscription_id) == [[before]],
               "case 4 (rows, control): the #{key} row #{subscription_id} moved from " <>
                 "#{before} to #{inspect(last_seen(conn, schema, subscription_id))}"
      end
    end
  end

  describe "case 5: the recreated stream outgrows the old position before any head check" do
    test "delivers every recreated event, in order, from one append", context do
      assert_outgrown_delivery(context, [5], "case 5a (one append of 5)")
    end

    test "delivers every recreated event, in order, from two appends", context do
      assert_outgrown_delivery(context, [3, 2], "case 5b (appends of 3, then 2)")
    end
  end

  describe "case 6: a hard delete, a head check, and a recreate later" do
    test "delivers the recreated stream's events", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      subscription = subscribe(stream_uuid, name, head_check_interval: @head_check_interval)

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
      assert [1, 2, 3] = numbers(receive_and_ack(subscription, 3, 5_000))

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 3} = row(conn, schema, stream_uuid, name)
      end)

      :ok = hard_delete(conn, schema, stream_uuid)

      # At least two head checks run (each comes a quarter to half an interval after the
      # last) while the stream is gone. Nothing is delivered.
      refute_receive {:events, _events}, 3 * @head_check_interval
      assert fsm(subscription).state == :subscribed

      recreated = EventFactory.create_events(2, 100)
      :ok = EventStore.append_to_stream(stream_uuid, 0, recreated)

      received = receive_and_ack(subscription, 2, 2_000)

      assert data(received) == data(recreated),
             "case 6 (hard delete, head check, recreate later): expected the recreated " <>
               "events #{inspect(data(recreated))}, received #{inspect(data(received))}; " <>
               "state #{inspect(fsm(subscription).state)}, " <>
               "row #{inspect(row(conn, schema, stream_uuid, name))}, " <>
               "cursor #{inspect(cursor(subscription))}"

      assert_settled(subscription, 2, "case 6")
    end
  end

  describe "case 7: a stream recreated before the first head check after subscribe" do
    # The stream exists before the subscription starts, so the stream it follows is the one
    # its subscribe found: the recreated stream's first batch, which arrives as events at
    # or below its position, is told apart from a late notification of the deleted one.
    test "delivers the recreated stream's events", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))

      subscription = subscribe(stream_uuid, name)
      assert [1, 2, 3] = numbers(receive_and_ack(subscription, 3, 5_000))

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 3} = row(conn, schema, stream_uuid, name)
      end)

      recreated = hard_delete_and_recreate(context, 3)

      received = receive_and_ack(subscription, 3, 2_000)

      assert data(received) == data(recreated),
             "case 7 (recreated before the first head check): expected the recreated " <>
               "events #{inspect(data(recreated))}, received #{inspect(data(received))}; " <>
               "state #{inspect(fsm(subscription).state)}, " <>
               "row #{inspect(row(conn, schema, stream_uuid, name))}, " <>
               "cursor #{inspect(cursor(subscription))}"

      assert_settled(subscription, 3, "case 7")
    end
  end

  describe "case 8: an event of the deleted stream acknowledged after the hard delete" do
    test "writes no position, and the recreated events are delivered", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      subscription = in_flight_at_hard_delete(context)

      # The subscriber acknowledges the last event it was sent, of the deleted stream.
      assert :ok = Subscription.ack(subscription, 3)

      assert last_seen(conn, schema, row_id(context)) == [[0]],
             "case 8a (stale ack, live): the ack of a deleted event moved the row to " <>
               "#{inspect(last_seen(conn, schema, row_id(context)))}, expected [[0]]; " <>
               "cursor #{inspect(cursor(subscription))}"

      recreated = EventFactory.create_events(2, 100)
      :ok = EventStore.append_to_stream(stream_uuid, 0, recreated)

      received = receive_and_ack(subscription, 2, 2_000)

      assert data(received) == data(recreated),
             "case 8a (stale ack, live): expected the recreated events " <>
               "#{inspect(data(recreated))}, received #{inspect(data(received))}; " <>
               "row #{inspect(row(conn, schema, stream_uuid, name))}, " <>
               "cursor #{inspect(cursor(subscription))}"

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 2} = row(conn, schema, stream_uuid, name)
      end)
    end

    test "writes no position, so a restart delivers the recreated events", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      subscription = in_flight_at_hard_delete(context)

      assert :ok = Subscription.ack(subscription, 3)

      :ok = EventStore.unsubscribe_from_stream(stream_uuid, name)
      refute Process.alive?(subscription)

      assert last_seen(conn, schema, row_id(context)) == [[0]],
             "case 8b (stale ack, restart): the ack of a deleted event and the stop left " <>
               "the row at #{inspect(last_seen(conn, schema, row_id(context)))}, " <>
               "expected [[0]]"

      recreated = EventFactory.create_events(2, 100)
      :ok = EventStore.append_to_stream(stream_uuid, 0, recreated)

      subscription = subscribe(stream_uuid, name)
      received = receive_and_ack(subscription, 2, 2_000)

      assert data(received) == data(recreated),
             "case 8b (stale ack, restart): expected the recreated events " <>
               "#{inspect(data(recreated))}, received #{inspect(data(received))}; " <>
               "row #{inspect(row(conn, schema, stream_uuid, name))}, " <>
               "cursor #{inspect(cursor(subscription))}"
    end

    # The checkpoint write races the hard delete's commit: the delete has removed the
    # stream's row and reset the subscription rows, and has not committed, when the write
    # starts. The write waits for the commit and then writes nothing.
    test "a checkpoint write that meets the uncommitted hard delete writes nothing", context do
      %{
        conn: conn,
        schema: schema,
        stream_uuid: stream_uuid,
        postgrex_config: postgrex_config
      } = context

      subscription = in_flight_at_hard_delete(context, delete: false)

      stream_id = StoreRows.stream_id(%{observer: conn, schema: schema}, stream_uuid)
      deleter = start_supervised!({Postgrex, postgrex_config}, id: :deleter)
      test = self()

      delete =
        Task.async(fn ->
          Postgrex.transaction(deleter, fn transaction ->
            Postgrex.query!(transaction, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])
            :ok = Storage.hard_delete_stream(transaction, stream_id, schema: schema)

            send(test, :deleted)

            receive do
              :commit -> :ok
            end
          end)
        end)

      assert_receive :deleted, 5_000

      ack = Task.async(fn -> Subscription.ack(subscription, 3, test) end)

      # Commit once the ack waits on a lock, or has already answered.
      answered = await_lock_wait_or_reply(ack, conn, 2_000)

      send(delete.pid, :commit)
      assert {:ok, :ok} = Task.await(delete)

      assert :ok =
               (case answered do
                  {:ok, reply} -> reply
                  nil -> Task.await(ack)
                end)

      assert last_seen(conn, schema, row_id(context)) == [[0]],
             "case 8c (stale ack racing the commit): the row is at " <>
               "#{inspect(last_seen(conn, schema, row_id(context)))}, expected [[0]]"
    end
  end

  describe "case 9: an event of the deleted stream acknowledged after the subscription follows" do
    # The deleted stream's events 1 to 3 are in flight, unacknowledged, when the stream is
    # recreated with events 1 to 3. The late ack of the deleted event 3 must not
    # acknowledge the recreated event 3.
    test "acknowledges no recreated event of the same number", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      attach_stream_recreated(name)

      subscription = subscribe(stream_uuid, name)

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
      assert_receive {:events, events}, 5_000
      assert [1, 2, 3] = numbers(events)

      recreated = hard_delete_and_recreate(context, 3)

      assert_receive {:stream_recreated, %{subscription_name: ^name}},
                     2_000,
                     "case 9 (stale ack after the follow): the subscription did not follow " <>
                       "the recreated stream; cursor #{inspect(cursor(subscription))}"

      # Nothing of the recreated stream is sent while the deleted events are unacknowledged.
      refute_receive {:events, _events}, 200

      assert :ok = Subscription.ack(subscription, 3)

      assert %{last_ack: 0} = cursor(subscription),
             "case 9 (stale ack after the follow): the ack of the deleted event 3 " <>
               "acknowledged a recreated event; cursor #{inspect(cursor(subscription))}"

      received = receive_and_ack(subscription, 3, 2_000)

      assert data(received) == data(recreated),
             "case 9 (stale ack after the follow): expected the recreated events " <>
               "#{inspect(data(recreated))}, received #{inspect(data(received))}; " <>
               "cursor #{inspect(cursor(subscription))}"

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 3} = row(conn, schema, stream_uuid, name)
      end)
    end
  end

  describe "case 10: a subscriber that leaves holding events of the deleted stream" do
    test "its unacknowledged events are not sent to another subscriber", context do
      %{stream_uuid: stream_uuid, subscription_name: name} = context

      first = forwarder(:first)
      second = forwarder(:second)

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))

      {:ok, subscription} =
        EventStore.subscribe_to_stream(stream_uuid, name, first,
          buffer_size: 10,
          concurrency_limit: 2
        )

      assert_receive {:first, {:subscribed, ^subscription}}
      assert_receive {:first, {:events, events}}, 5_000
      assert [1, 2, 3] = numbers(events)

      {:ok, ^subscription} =
        EventStore.subscribe_to_stream(stream_uuid, name, second,
          buffer_size: 10,
          concurrency_limit: 2
        )

      assert_receive {:second, {:subscribed, ^subscription}}

      recreated = hard_delete_and_recreate(context)

      received = receive_tagged(:second, 2, 2_000)

      assert data(received) == data(recreated),
             "case 10 (subscriber leaves): expected the second subscriber to be sent the " <>
               "recreated events #{inspect(data(recreated))}, it was sent " <>
               "#{inspect(data(received))}; cursor #{inspect(cursor(subscription))}"

      Process.exit(first, :kill)

      refute_receive {:second, {:events, _events}}, 500
    end
  end

  describe "case 11: a transient subscription subscribed throughout the hard delete" do
    test "delivers the recreated stream's events", context do
      %{stream_uuid: stream_uuid, subscription_name: name} = context

      subscription = subscribe(stream_uuid, name, transient: true)

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
      assert [1, 2, 3] = numbers(receive_and_ack(subscription, 3, 5_000))

      recreated = hard_delete_and_recreate(context)

      received = receive_and_ack(subscription, 2, 2_000)

      assert data(received) == data(recreated),
             "case 11 (transient, live): expected the recreated events " <>
               "#{inspect(data(recreated))}, received #{inspect(data(received))}; " <>
               "state #{inspect(fsm(subscription).state)}, " <>
               "cursor #{inspect(cursor(subscription))}"

      assert_settled(subscription, 2, "case 11")
    end
  end

  describe "case 12: controls" do
    test "a stream subscription with no hard delete delivers each event once", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      attach_stream_recreated(name)

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))

      subscription = subscribe(stream_uuid, name, head_check_interval: @head_check_interval)
      assert [1, 2, 3] = numbers(receive_and_ack(subscription, 3, 5_000))

      :ok = EventStore.append_to_stream(stream_uuid, 3, EventFactory.create_events(2, 3))
      assert [4, 5] = numbers(receive_and_ack(subscription, 2, 5_000))

      # Several head checks pass: nothing more is delivered and nothing is reset.
      refute_receive {:events, _events}, 3 * @head_check_interval
      refute_received {:stream_recreated, _metadata}

      assert_settled(subscription, 5, "case 12a (no hard delete)")

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 5} = row(conn, schema, stream_uuid, name)
      end)
    end

    test "a stream subscription to a stream created later delivers each event once",
         context do
      %{stream_uuid: stream_uuid, subscription_name: name} = context

      attach_stream_recreated(name)

      subscription = subscribe(stream_uuid, name, head_check_interval: @head_check_interval)

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
      :ok = EventStore.append_to_stream(stream_uuid, 3, EventFactory.create_events(2, 3))

      assert [1, 2, 3, 4, 5] = numbers(receive_and_ack(subscription, 5, 5_000))

      refute_receive {:events, _events}, 3 * @head_check_interval
      refute_received {:stream_recreated, _metadata}

      assert_settled(subscription, 5, "case 12b (stream created after subscribe)")
    end

    test "a $all subscription keeps its position across a member's hard delete", context do
      %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} =
        context

      attach_stream_recreated(name)

      other_stream_uuid = UUID.uuid4()

      subscription = subscribe("$all", name, head_check_interval: @head_check_interval)

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
      :ok = EventStore.append_to_stream(other_stream_uuid, 0, EventFactory.create_events(2))
      assert [1, 2, 3, 4, 5] = numbers(receive_and_ack(subscription, 5, 5_000))

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 5} = row(conn, schema, "$all", name)
      end)

      :ok = hard_delete(conn, schema, stream_uuid)

      refute_receive {:events, _events}, 3 * @head_check_interval
      refute_received {:stream_recreated, _metadata}
      assert %Storage.Subscription{last_seen: 5} = row(conn, schema, "$all", name)
      assert %{last_ack: 5} = cursor(subscription)

      :ok = EventStore.append_to_stream(other_stream_uuid, 2, EventFactory.create_events(1))
      assert [6] = numbers(receive_and_ack(subscription, 1, 5_000))

      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2, 100))
      assert [7, 8] = numbers(receive_and_ack(subscription, 2, 5_000))

      refute_receive {:events, _events}, 200
      refute_received {:stream_recreated, _metadata}

      Wait.until(5_000, fn ->
        assert %Storage.Subscription{last_seen: 8} = row(conn, schema, "$all", name)
      end)
    end
  end

  def forward_telemetry(_event, _measurements, %{subscription_name: name} = metadata, config) do
    %{test: test, subscription_name: subscription_name} = config

    if name == subscription_name, do: send(test, {:stream_recreated, metadata})
  end

  defp attach_stream_recreated(name) do
    handler = "stream-recreated-#{name}"

    :ok =
      :telemetry.attach(handler, @stream_recreated, &__MODULE__.forward_telemetry/4, %{
        test: self(),
        subscription_name: name
      })

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp assert_live_delivery(context, opts, timeout, label) do
    %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} = context

    subscription = subscribe(stream_uuid, name, opts)

    :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
    assert [1, 2, 3] = numbers(receive_and_ack(subscription, 3, 5_000))

    Wait.until(5_000, fn ->
      assert %Storage.Subscription{last_seen: 3} = row(conn, schema, stream_uuid, name)
    end)

    assert fsm(subscription).state == :subscribed

    if interval = opts[:head_check_interval] do
      assert Map.get(fsm(subscription).data, :head_check_interval) == interval
    end

    recreated = hard_delete_and_recreate(context)

    received = receive_and_ack(subscription, 2, timeout)

    assert Process.alive?(subscription)

    assert data(received) == data(recreated),
           "#{label}: expected the recreated events #{inspect(data(recreated))} within " <>
             "#{timeout}ms, received #{inspect(data(received))}; " <>
             "state #{inspect(fsm(subscription).state)}, " <>
             "row #{inspect(row(conn, schema, stream_uuid, name))}, " <>
             "cursor #{inspect(cursor(subscription))}"
  end

  # The old stream has 3 events, delivered and acknowledged. The recreated stream gets
  # more events than that, in the given appends, before any head check.
  defp assert_outgrown_delivery(context, appends, label) do
    %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} = context

    subscription = subscribe(stream_uuid, name)

    :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
    assert [1, 2, 3] = numbers(receive_and_ack(subscription, 3, 5_000))

    Wait.until(5_000, fn ->
      assert %Storage.Subscription{last_seen: 3} = row(conn, schema, stream_uuid, name)
    end)

    :ok = hard_delete(conn, schema, stream_uuid)

    {recreated, total} =
      Enum.reduce(appends, {[], 0}, fn count, {recreated, version} ->
        events = EventFactory.create_events(count, 100 + version)
        :ok = EventStore.append_to_stream(stream_uuid, version, events)

        {recreated ++ events, version + count}
      end)

    received = receive_and_ack(subscription, total, 2_000)

    assert data(received) == data(recreated) and numbers(received) == Enum.to_list(1..total),
           "#{label}: expected the recreated events #{inspect(data(recreated))} as " <>
             "1..#{total}, received #{inspect(data(received))} as " <>
             "#{inspect(numbers(received))}; state #{inspect(fsm(subscription).state)}, " <>
             "row #{inspect(row(conn, schema, stream_uuid, name))}, " <>
             "cursor #{inspect(cursor(subscription))}"

    assert_settled(subscription, total, label)
  end

  # A subscription with the old stream's events 1 to 3 sent to its subscriber, 1 and 2
  # acknowledged (the row at 2) and 3 in flight; then the stream is hard deleted, unless
  # `delete: false`. Nothing is recreated, and no head check runs (the default interval).
  defp in_flight_at_hard_delete(context, opts \\ []) do
    %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} = context

    subscription = subscribe(stream_uuid, name)

    :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
    assert_receive {:events, events}, 5_000
    assert [1, 2, 3] = numbers(events)

    :ok = Subscription.ack(subscription, 2)

    Wait.until(5_000, fn ->
      assert %Storage.Subscription{last_seen: 2} = row(conn, schema, stream_uuid, name)
    end)

    if Keyword.get(opts, :delete, true) do
      :ok = hard_delete(conn, schema, stream_uuid)
    end

    subscription
  end

  # Settled in `subscribed` at `position`, sampled a few times: no catch-up read is
  # running or asked for, and nothing more is delivered.
  defp assert_settled(subscription, position, label) do
    Wait.until(2_000, fn ->
      assert %SubscriptionFsm{state: :subscribed} = fsm(subscription)

      assert %{last_received: ^position, last_sent: ^position, last_ack: ^position} =
               cursor(subscription)
    end)

    for _sample <- 1..5 do
      refute_receive {:events, _events}, 50

      assert %SubscriptionFsm{state: :subscribed} = fsm(subscription),
             "#{label}: left `subscribed` after settling; cursor #{inspect(cursor(subscription))}"

      assert %{last_received: ^position, last_sent: ^position, last_ack: ^position} =
               cursor(subscription),
             "#{label}: the cursor moved after settling at #{position}: " <>
               inspect(cursor(subscription))
    end
  end

  defp subscribe(stream_uuid, name, opts \\ []) do
    {:ok, subscription} =
      EventStore.subscribe_to_stream(
        stream_uuid,
        name,
        self(),
        Keyword.merge([buffer_size: 10], opts)
      )

    assert_receive {:subscribed, ^subscription}

    subscription
  end

  # Hard deletes the stream and appends `count` events to its uuid again, which recreates
  # it at versions 1 to `count`. Answers the recreated events.
  defp hard_delete_and_recreate(context, count \\ 2) do
    %{conn: conn, schema: schema, stream_uuid: stream_uuid} = context
    rows = %{observer: conn, schema: schema}

    old_stream_id = StoreRows.stream_id(rows, stream_uuid)
    assert is_integer(old_stream_id)

    :ok = hard_delete(conn, schema, stream_uuid)
    assert StoreRows.stream_id(rows, stream_uuid) == nil

    recreated = EventFactory.create_events(count, 100)
    :ok = EventStore.append_to_stream(stream_uuid, 0, recreated)

    assert StoreRows.stream_version(rows, stream_uuid) == count
    refute StoreRows.stream_id(rows, stream_uuid) == old_stream_id

    recreated
  end

  defp hard_delete(conn, schema, stream_uuid) do
    Stream.delete(conn, stream_uuid, :any_version, :hard,
      schema: schema,
      enable_hard_deletes: true
    )
  end

  # Receives events until `count` arrived or `timeout` passed without one, acking each
  # batch. Answers what arrived.
  defp receive_and_ack(subscription, count, timeout, acc \\ [])

  defp receive_and_ack(_subscription, count, _timeout, acc) when count <= 0,
    do: Enum.reverse(acc)

  defp receive_and_ack(subscription, count, timeout, acc) do
    receive do
      {:events, events} ->
        :ok = Subscription.ack(subscription, events)

        receive_and_ack(subscription, count - length(events), timeout, Enum.reverse(events, acc))
    after
      timeout -> Enum.reverse(acc)
    end
  end

  # Receives the events forwarded under `tag` until `count` arrived or `timeout` passed
  # without one. Answers what arrived.
  defp receive_tagged(tag, count, timeout, acc \\ [])

  defp receive_tagged(_tag, count, _timeout, acc) when count <= 0, do: Enum.reverse(acc)

  defp receive_tagged(tag, count, timeout, acc) do
    receive do
      {^tag, {:events, events}} ->
        receive_tagged(tag, count - length(events), timeout, Enum.reverse(events, acc))
    after
      timeout -> Enum.reverse(acc)
    end
  end

  # A subscriber process that forwards every message to the test, tagged. Not linked, so
  # the test can kill it.
  defp forwarder(tag) do
    test = self()

    pid =
      spawn(fn ->
        loop = fn loop ->
          receive do
            message -> send(test, {tag, message})
          end

          loop.(loop)
        end

        loop.(loop)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)

    pid
  end

  defp numbers(events), do: Enum.map(events, fn %RecordedEvent{event_number: n} -> n end)

  defp data(events), do: Enum.map(events, & &1.data)

  defp new_event(number) do
    %EventData{
      event_type: @new_type,
      data: %NewReleaseEvent{event: number},
      metadata: %{}
    }
  end

  defp create_row(conn, schema, stream_uuid, last_seen) do
    {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
      Storage.Subscription.subscribe_to_stream(conn, stream_uuid, UUID.uuid4(), 0, schema: schema)

    :ok =
      Storage.Subscription.ack_last_seen_event_by_id(conn, subscription_id, last_seen,
        schema: schema
      )

    assert last_seen(conn, schema, subscription_id) == [[last_seen]]

    %Storage.Subscription{subscription_id: subscription_id, last_seen: last_seen}
  end

  defp last_seen(conn, schema, subscription_id) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        ~s(SELECT last_seen FROM "#{schema}".subscriptions WHERE subscription_id = $1),
        [subscription_id]
      )

    rows
  end

  defp row_id(context) do
    %{conn: conn, schema: schema, stream_uuid: stream_uuid, subscription_name: name} = context

    %Storage.Subscription{subscription_id: subscription_id} =
      row(conn, schema, stream_uuid, name)

    subscription_id
  end

  # `{:ok, reply}` once the task has answered, or `nil` once a lock wait shows in `pg_locks`
  # (or `timeout` passed), with the task still running.
  defp await_lock_wait_or_reply(task, conn, timeout) do
    case Task.yield(task, 10) do
      {:ok, reply} ->
        {:ok, reply}

      nil ->
        if waiting_locks(conn) > 0 or timeout <= 0,
          do: nil,
          else: await_lock_wait_or_reply(task, conn, timeout - 10)
    end
  end

  defp waiting_locks(conn) do
    %Postgrex.Result{rows: [[count]]} =
      Postgrex.query!(conn, "SELECT count(*) FROM pg_locks WHERE NOT granted", [])

    count
  end

  defp row(conn, schema, stream_uuid, name) do
    {:ok, row} = Storage.Subscription.subscription(conn, stream_uuid, name, schema: schema)

    row
  end

  defp fsm(subscription) do
    %Subscription{subscription: %SubscriptionFsm{data: %SubscriptionState{}} = fsm} =
      :sys.get_state(subscription)

    fsm
  end

  defp cursor(subscription) do
    %SubscriptionFsm{data: data} = fsm(subscription)

    Map.take(data, [:last_received, :last_sent, :last_ack, :subscription_id])
  end
end
