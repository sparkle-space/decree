defmodule Decree.Store.Subscriptions.SubscriptionHeadCheckTest do
  use Decree.Store.StorageCase

  # A subscription in `subscribed` has no timer that reads its stream: its position moves
  # forward only when the publisher wakes it with `{:events, _}` or `{:events_pending, _}`.
  # A wake-up can be lost (a reconnected
  # notifications connection, a failed publisher read, a notification Postgrex dropped
  # during an idle ping), which left the subscription behind until the next append to its
  # stream (sparkle-space/sparklespace#8066).
  #
  # A subscribed subscription now reads its stream's head every `head_check_interval / 4`
  # to `head_check_interval / 2`. A head above what it has received, seen at one check and
  # still above it at the next, is a missed wake-up: the subscription catches up and emits
  # `[:decree, :store, :subscription, :missed_wake_up]`, once per head.
  #
  # The wake-up is dropped for real: the append runs in a transaction that set
  # `session_replication_role = replica`, so the `streams` table's `event_notification`
  # trigger does not fire and no NOTIFY is sent for that append. Every other append
  # notifies as usual.

  import ExUnit.CaptureLog

  alias Decree.Store.{EventFactory, RecordedEvent, UUID, Wait}
  alias Decree.Store.Subscriptions
  alias Decree.Store.Subscriptions.{Subscription, SubscriptionFsm, SubscriptionState}
  alias TestEventStore, as: EventStore

  @missed_wake_up [:decree, :store, :subscription, :missed_wake_up]
  @catch_up_error [:decree, :store, :subscription, :catch_up, :error]

  @head_check_interval 400

  # A dropped wake-up is repaired within `head_check_interval`; this covers the catch-up
  # read and a loaded host.
  @margin 500

  setup do
    subscription_name = UUID.uuid4()
    handler_id = {__MODULE__, subscription_name}

    :ok =
      :telemetry.attach_many(
        handler_id,
        [@missed_wake_up, @catch_up_error],
        &__MODULE__.handle_telemetry_event/4,
        %{test: self(), subscription_name: subscription_name}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    [subscription_name: subscription_name]
  end

  describe "a dropped wake-up" do
    test "is repaired for a subscription to all streams", %{
      postgrex_config: postgrex_config,
      subscription_name: subscription_name
    } do
      subscription = subscribe("$all", subscription_name)

      stream_uuid = UUID.uuid4()
      dropped_at = append_unnotified(postgrex_config, stream_uuid, 0, 3)

      assert_repaired(subscription, [1, 2, 3], dropped_at)

      assert_receive {:telemetry, @missed_wake_up, measurements, metadata}

      assert is_integer(measurements.system_time)

      assert metadata == %{
               event_store: TestEventStore,
               subscription_name: subscription_name,
               stream_uuid: "$all",
               head: 3,
               seen_head: 3,
               last_received: 0,
               last_seen: 0,
               head_check_interval: @head_check_interval
             }

      # Caught up: no later check repairs again.
      refute_receive {:telemetry, @missed_wake_up, _measurements, _metadata},
                     3 * @head_check_interval

      refute_received {:events, _events}
    end

    test "is repaired for a subscription to a single stream", %{
      postgrex_config: postgrex_config,
      subscription_name: subscription_name
    } do
      stream_uuid = UUID.uuid4()

      subscription = subscribe(stream_uuid, subscription_name)

      # The first event is notified as usual.
      :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

      assert_receive {:events, [%RecordedEvent{event_number: 1}]}
      :ok = Subscription.ack(subscription, 1)

      dropped_at = append_unnotified(postgrex_config, stream_uuid, 1, 2)

      assert_repaired(subscription, [2, 3], dropped_at)

      assert_receive {:telemetry, @missed_wake_up, _measurements, metadata}

      assert metadata == %{
               event_store: TestEventStore,
               subscription_name: subscription_name,
               stream_uuid: stream_uuid,
               head: 3,
               seen_head: 3,
               last_received: 1,
               last_seen: 1,
               head_check_interval: @head_check_interval
             }

      refute_receive {:telemetry, @missed_wake_up, _measurements, _metadata},
                     3 * @head_check_interval
    end
  end

  test "every wake-up is delivered, and only a late one is counted as missed", %{
    subscription_name: subscription_name
  } do
    # Checks 250 to 500 ms apart: a wake-up still on its way at one check has that long
    # to arrive before the next. One that takes longer is counted as missed: a stall on a
    # loaded host can hold the publisher's read past a whole check period
    # (sparkle-space/sparklespace#8608), and a late wake-up then looks like a lost one.
    interval = 1_000
    gap = div(interval, 4)

    subscription = subscribe("$all", subscription_name, head_check_interval: interval)

    # The publisher's wake-ups, traced as the subscription receives them.
    1 = :erlang.trace(subscription, true, [:receive])

    count = 60
    test = self()

    # One append every 50 ms, across three check periods: some checks read a head whose
    # wake-up is still on its way. Each append is the next event in `$all`, timed before
    # the call, so its commit is no earlier.
    appender =
      spawn_link(fn ->
        appended_at =
          Map.new(1..count, fn event_number ->
            appended_at = now()
            :ok = EventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(1))

            # Pacing the appends, not waiting for a condition.
            Process.sleep(50)

            {event_number, appended_at}
          end)

        send(test, {:appended, self(), appended_at})
      end)

    received = receive_and_time(subscription, count, 5_000)
    assert Enum.map(received, &elem(&1, 0)) == Enum.to_list(1..count)
    received_at = Map.new(received)

    assert_receive {:appended, ^appender, appended_at}

    # The checks ran while the events were appended.
    data = fsm(subscription).data
    assert is_integer(Map.get(data, :head_seen))
    assert Map.get(data, :head_seen) > 0

    # The last wake-up is confirmed missed, if at all, within a period of its append.
    refute_receive {:telemetry, @catch_up_error, _measurements, _metadata}, interval

    # A missed wake-up names the last event received at the check that confirmed it. The
    # next event's commit was seen at the check before, at least `gap` earlier, and it had
    # not arrived: it reached the test at least `gap` after its append. One that arrived
    # sooner was counted as missed though its wake-up came as usual.
    for %{last_received: last_received} <- collect_missed_wake_ups() do
      late = last_received + 1
      latency = received_at[late] - appended_at[late]

      assert latency >= gap,
             "event #{late} arrived #{latency}ms after its append, inside one check " <>
               "gap (#{gap}ms), yet its wake-up was counted as missed"
    end

    # The publisher's wake-up for every event reaches the subscription, a late one
    # included: a missed wake-up above is a late one, never a lost one.
    assert receive_wake_ups(subscription, count, 5_000) == MapSet.new(1..count)

    1 = :erlang.trace(subscription, false, [:receive])
  end

  # Driven end to end: `$all`'s `stream_version` is never lowered by a hard delete, so
  # once the only stream that held its tail events is hard deleted, `$all`'s head sits
  # above every event a catch-up read can find.
  test "a head with nothing readable under it is repaired once, not every period", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    stop_supervised!(TestEventStore)
    start_supervised!({TestEventStore, enable_hard_deletes: true})

    subscription = subscribe("$all", subscription_name)

    stream_uuid = UUID.uuid4()
    append_unnotified(postgrex_config, stream_uuid, 0, 2)
    :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)

    assert {:ok, %{stream_version: 2}} = EventStore.stream_info(:all)
    assert {:ok, []} = EventStore.read_all_streams_forward()

    assert_receive {:telemetry, @missed_wake_up, _measurements,
                    %{head: 2, seen_head: 2, last_received: 0}},
                   @head_check_interval + @margin

    # Several more periods: the same head is not repaired again, and the subscription
    # neither delivers nor loops on catch-up reads.
    refute_receive {:telemetry, @missed_wake_up, _measurements, _metadata},
                   4 * @head_check_interval

    refute_received {:events, _events}

    assert %SubscriptionFsm{state: :subscribed, data: data} = fsm(subscription)

    # `last_received` is never raised to the head.
    assert data.last_received == 0
    assert Map.get(data, :head_repaired) == 2
  end

  # Driven through `SubscriptionFsm` directly, so the checks run when the test says and
  # not on a timer. `$all`'s head was 1 at the previous check and is 2 at this one, and
  # nothing is readable under either (a hard delete). The repair records the head it
  # read, 2, so the next check does not take the second sighting for a second lost
  # wake-up and repair again.
  test "a head that rose between the two sightings is repaired once", %{
    conn: conn,
    schema: schema,
    serializer: serializer,
    subscription_name: subscription_name
  } do
    stop_supervised!(TestEventStore)
    start_supervised!({TestEventStore, enable_hard_deletes: true})

    stream_uuid = UUID.uuid4()
    :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(2))
    :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard)

    assert {:ok, %{stream_version: 2}} = EventStore.stream_info(:all)
    assert {:ok, []} = EventStore.read_all_streams_forward()

    %SubscriptionFsm{data: %SubscriptionState{} = data} =
      fsm =
      SubscriptionFsm.new("$all", subscription_name,
        conn: conn,
        event_store: TestEventStore,
        schema: schema,
        serializer: serializer,
        head_check_interval: @head_check_interval
      )

    # Subscribed with nothing received, and head 1 seen at the previous check.
    fsm = %SubscriptionFsm{
      fsm
      | state: :subscribed,
        data: %SubscriptionState{data | head_seen: 1}
    }

    # This check confirms head 1 and repairs it; three more checks follow. A check that
    # asks to catch up is caught up, as `Subscription` does.
    fsm =
      Enum.reduce(1..4, fsm, fn _check, fsm ->
        fsm = SubscriptionFsm.check_head(fsm)

        case fsm.state do
          :request_catch_up -> SubscriptionFsm.catch_up(fsm)
          _state -> fsm
        end
      end)

    assert %SubscriptionFsm{state: :subscribed, data: data} = fsm
    assert data.last_received == 0

    repairs = collect_missed_wake_ups()

    assert length(repairs) == 1,
           "#{length(repairs)} missed wake-ups for one lost wake-up: #{inspect(repairs)}"

    assert [%{head: 2, seen_head: 1, last_received: 0}] = repairs
  end

  describe "an invalid head_check_interval option" do
    for invalid <- [0, -1, -10, "1000", 1.5, nil] do
      @invalid invalid

      test "#{inspect(invalid)} is refused and starts no subscription", %{
        subscription_name: subscription_name
      } do
        assert EventStore.subscribe_to_stream("$all", subscription_name, self(),
                 head_check_interval: @invalid
               ) == {:error, :invalid_head_check_interval}

        refute_received {:subscribed, _subscription}
        assert local(subscription_name) == []
        assert {[], 0} = EventStore.local_subscriptions()
      end
    end

    test "a positive interval under a second is allowed", %{
      subscription_name: subscription_name
    } do
      subscription = subscribe("$all", subscription_name, head_check_interval: 400)

      assert Map.get(fsm(subscription).data, :head_check_interval) == 400
    end
  end

  # `Process.send_after/3` takes at most 4_294_967_295 ms. The wait is at most half the
  # interval, so any interval up to that maximum arms; a larger one is refused before a
  # subscription starts, rather than crashing it at its first head check.
  describe "a head_check_interval above the timer maximum" do
    for too_long <- [4_294_967_296, 10_000_000_000] do
      @too_long too_long

      test "#{too_long} is refused and starts no subscription", %{
        subscription_name: subscription_name
      } do
        assert EventStore.subscribe_to_stream("$all", subscription_name, self(),
                 head_check_interval: @too_long
               ) == {:error, :invalid_head_check_interval}

        refute_received {:subscribed, _subscription}
        assert local(subscription_name) == []
        assert {[], 0} = EventStore.local_subscriptions()
      end
    end

    test "4294967295 subscribes", %{subscription_name: subscription_name} do
      subscription = subscribe("$all", subscription_name, head_check_interval: 4_294_967_295)

      assert Map.get(fsm(subscription).data, :head_check_interval) == 4_294_967_295
      assert Process.alive?(subscription)
    end
  end

  test "a failed head read stays subscribed and the next good read repairs", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    # A short query timeout, so a head read blocked by the lock below fails quickly.
    subscription = subscribe("$all", subscription_name, timeout: 100)

    stream_uuid = UUID.uuid4()
    append_unnotified(postgrex_config, stream_uuid, 0, 2)

    log =
      capture_log([level: :debug], fn ->
        lock = lock_streams(postgrex_config)

        # Four periods of failed head reads.
        refute_receive {:telemetry, _event, _measurements, _metadata}, 4 * @head_check_interval
        refute_received {:events, _events}

        assert %SubscriptionFsm{state: :subscribed} = fsm(subscription)
        assert Process.alive?(subscription)

        release(lock)
      end)

    assert log =~ "head check failed"

    # A head seen before the lock was taken can be confirmed by the first good read, so
    # the events can arrive at once.
    assert_repaired(subscription, [1, 2], now(), early: true)

    assert_receive {:telemetry, @missed_wake_up, _measurements,
                    %{head: 2, seen_head: 2, last_received: 0}}

    refute_received {:telemetry, @catch_up_error, _measurements, _metadata}
  end

  test "each head-check wait is drawn between a quarter and a half of the interval" do
    for interval <- [1, 2, 3, 4, 5, 7, 100, 400, 1_000, 60_000, 3_600_000] do
      low = max(1, div(interval, 4))
      high = max(1, div(interval, 2))

      waits = for _sample <- 1..200, do: SubscriptionFsm.head_check_wait(interval)

      for wait <- waits do
        assert is_integer(wait)
        assert wait >= low, "a #{wait}ms wait is under #{low}ms for #{interval}ms"
        assert wait <= high, "a #{wait}ms wait is over #{high}ms for #{interval}ms"
      end

      if high > low do
        assert length(Enum.uniq(waits)) > 1,
               "every draw for #{interval}ms was #{hd(waits)}ms: the wait is not jittered"
      end
    end
  end

  test "the event store's head-check interval defaults to a minute and is at least a second" do
    assert Subscriptions.head_check_interval(TestEventStore, []) == 60_000

    assert Subscriptions.head_check_interval(TestEventStore,
             subscription_head_check_interval: 10
           ) == 1_000

    assert Subscriptions.head_check_interval(TestEventStore,
             subscription_head_check_interval: 5_000
           ) == 5_000

    for invalid <- [0, -1, "60000", 1.5, :never] do
      assert_raise ArgumentError, ~r/subscription_head_check_interval/, fn ->
        Subscriptions.head_check_interval(TestEventStore,
          subscription_head_check_interval: invalid
        )
      end
    end

    config = Decree.Store.Config.lookup(TestEventStore)

    assert Keyword.get(config, :subscription_head_check_interval) == 60_000
  end

  test "the event store's head-check interval is at most the timer maximum" do
    assert Subscriptions.head_check_interval(TestEventStore,
             subscription_head_check_interval: 4_294_967_295
           ) == 4_294_967_295

    for too_long <- [4_294_967_296, 10_000_000_000] do
      assert_raise ArgumentError, ~r/subscription_head_check_interval/, fn ->
        Subscriptions.head_check_interval(TestEventStore,
          subscription_head_check_interval: too_long
        )
      end
    end
  end

  test "a subscription without the option takes the event store's interval", %{
    subscription_name: subscription_name
  } do
    {:ok, subscription} = EventStore.subscribe_to_all_streams(subscription_name, self())
    assert_receive {:subscribed, ^subscription}

    assert Map.get(fsm(subscription).data, :head_check_interval) == 60_000
  end

  test "local_subscriptions reports the last event acknowledged", %{
    subscription_name: subscription_name
  } do
    subscription = subscribe("$all", subscription_name)

    assert [%{pid: ^subscription} = info] = local(subscription_name)
    assert Map.get(info, :last_seen) == 0

    :ok = EventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(2))

    assert receive_and_ack(subscription, 2, 5_000) == [1, 2]

    assert [%{pid: ^subscription, status: :subscribed} = info] = local(subscription_name)
    assert Map.get(info, :last_seen) == 2
  end

  def handle_telemetry_event(event, measurements, metadata, config) do
    %{test: test, subscription_name: subscription_name} = config

    if metadata.subscription_name == subscription_name do
      send(test, {:telemetry, event, measurements, metadata})
    end
  end

  defp subscribe(stream_uuid, subscription_name, opts \\ []) do
    opts = Keyword.merge([head_check_interval: @head_check_interval], opts)

    {:ok, subscription} =
      EventStore.subscribe_to_stream(stream_uuid, subscription_name, self(), opts)

    assert_receive {:subscribed, ^subscription}

    # Caught up on an empty stream: subscribed, waiting for a wake-up.
    Wait.until(5_000, fn ->
      assert %SubscriptionFsm{state: :subscribed} = fsm(subscription)
    end)

    subscription
  end

  # The events of a dropped wake-up arrive, none before the first check after the drop
  # could have confirmed the head (a quarter of the interval) unless `early: true`, and the
  # first of them within the interval and the margin.
  defp assert_repaired(subscription, expected, dropped_at, opts \\ []) do
    unless Keyword.get(opts, :early, false) do
      earliest = dropped_at + div(@head_check_interval, 4) - 20

      refute_receive {:events, _events}, max(earliest - now(), 0)
    end

    deadline = dropped_at + @head_check_interval + @margin

    assert_receive {:events, [%RecordedEvent{} | _] = events},
                   max(deadline - now(), 0),
                   "the dropped wake-up was not repaired within " <>
                     "#{@head_check_interval + @margin}ms"

    received = Enum.map(events, & &1.event_number)
    %RecordedEvent{event_number: last} = List.last(events)
    :ok = Subscription.ack(subscription, last)

    rest = receive_and_ack(subscription, length(expected) - length(received), 5_000)

    assert received ++ rest == expected
  end

  # Receive and ack events until `count` have been received; their event numbers in order.
  defp receive_and_ack(_subscription, count, _timeout) when count <= 0, do: []

  defp receive_and_ack(subscription, count, timeout) do
    assert_receive {:events, events}, timeout

    %RecordedEvent{event_number: last} = List.last(events)
    :ok = Subscription.ack(subscription, last)

    Enum.map(events, & &1.event_number) ++
      receive_and_ack(subscription, count - length(events), timeout)
  end

  # Receive and ack events until `count` have been received; in order, each event number and
  # the monotonic time, in milliseconds, it was received at.
  defp receive_and_time(_subscription, count, _timeout) when count <= 0, do: []

  defp receive_and_time(subscription, count, timeout) do
    assert_receive {:events, events}, timeout
    received_at = now()

    %RecordedEvent{event_number: last} = List.last(events)
    :ok = Subscription.ack(subscription, last)

    Enum.map(events, &{&1.event_number, received_at}) ++
      receive_and_time(subscription, count - length(events), timeout)
  end

  # The event numbers of the `{:events, _}` wake-ups the traced subscription receives, until
  # `count` have been seen.
  defp receive_wake_ups(subscription, count, timeout, seen \\ MapSet.new()) do
    if MapSet.size(seen) >= count do
      seen
    else
      assert_receive {:trace, ^subscription, :receive, {:events, events}}, timeout

      seen = Enum.into(events, seen, & &1.event_number)
      receive_wake_ups(subscription, count, timeout, seen)
    end
  end

  # Append `count` events with the wake-up dropped: in a transaction where the `streams`
  # table's notify trigger does not fire, so no NOTIFY reaches the store's listener.
  # Returns the monotonic time, in milliseconds, just after the commit.
  defp append_unnotified(postgrex_config, stream_uuid, expected_version, count) do
    {:ok, conn} = Postgrex.start_link(postgrex_config)

    events = EventFactory.create_events(count, expected_version + 1)

    {:ok, :ok} =
      Postgrex.transaction(conn, fn transaction ->
        Postgrex.query!(transaction, "SET LOCAL session_replication_role = replica", [])

        EventStore.append_to_stream(stream_uuid, expected_version, events, conn: transaction)
      end)

    committed_at = now()

    :ok = GenServer.stop(conn)

    committed_at
  end

  # Hold an ACCESS EXCLUSIVE lock on `streams` from a second connection until `release/1`,
  # so a head read blocks until its query timeout. Linked to the test.
  defp lock_streams(postgrex_config) do
    test = self()

    holder =
      spawn_link(fn ->
        {:ok, conn} = Postgrex.start_link(postgrex_config)

        Postgrex.transaction(
          conn,
          fn transaction ->
            Postgrex.query!(
              transaction,
              ~s(LOCK TABLE "public".streams IN ACCESS EXCLUSIVE MODE),
              []
            )

            send(test, {:locked, self()})

            receive do
              :release -> :ok
            end
          end,
          timeout: :infinity
        )

        send(test, {:released, self()})
      end)

    assert_receive {:locked, ^holder}

    holder
  end

  defp release(holder) do
    send(holder, :release)

    assert_receive {:released, ^holder}
  end

  defp local(subscription_name) do
    {infos, 0} = EventStore.local_subscriptions()

    Enum.filter(infos, &(&1.subscription_name == subscription_name))
  end

  defp fsm(subscription) do
    %Subscription{subscription: %SubscriptionFsm{} = fsm} = :sys.get_state(subscription)

    fsm
  end

  defp collect_missed_wake_ups do
    receive do
      {:telemetry, @missed_wake_up, _measurements, metadata} ->
        [metadata | collect_missed_wake_ups()]
    after
      0 -> []
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
