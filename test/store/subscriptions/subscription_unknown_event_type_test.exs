defmodule Decree.Store.Subscriptions.SubscriptionUnknownEventTypeTest do
  use Decree.Store.StorageCase

  # An event of a type this instance does not know (one a newer release appended) is never
  # skipped and never crashes anything: a subscription holds before it, releasing its lock,
  # and an instance that knows the type delivers it (sparkle-space/sparklespace#7804).
  #
  # Two event store instances run on the same database: `:old_store` with
  # `DenyListSerializer`, which does not know `NewReleaseEvent`, stands for an old pod, and
  # `TestEventStore` with the plain serializer for a new one.

  import Decree.Store.SubscriptionHelpers, only: [wait_for_lock_pool: 1]

  alias Decree.Store.{
    AdvisoryLocks,
    DenyListSerializer,
    EventData,
    EventFactory,
    NewReleaseEvent,
    PubSub,
    RecordedEvent,
    Storage,
    UnknownEventTypeError,
    UUID,
    Wait
  }

  alias Decree.Store.Notifications.Publisher
  alias Decree.Store.Subscriptions.{Subscription, SubscriptionFsm, SubscriptionState}

  @old :old_store
  @held [:decree, :store, :subscription, :held]
  @new_type "Elixir.Decree.Store.NewReleaseEvent"
  @known_type "Elixir.Decree.Store.EventFactory.Event"

  setup do
    DenyListSerializer.deny([@new_type])
    on_exit(fn -> DenyListSerializer.allow_all() end)

    start_supervised!({TestEventStore, name: @old, serializer: DenyListSerializer},
      id: :old_store
    )

    wait_for_lock_pool(@old)
    wait_for_lock_pool(TestEventStore)

    test = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        @held,
        fn _event, measurements, metadata, _config ->
          send(test, {:held, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    [subscription_name: UUID.uuid4()]
  end

  describe "a persistent subscription meeting an unknown type" do
    test "delivers and checkpoints the events before it, then holds without its lock", %{
      conn: conn,
      subscription_name: name
    } do
      subscriber = forwarder(:old)

      # A checkpoint threshold the one acked event never reaches: only the hold writes it.
      subscription = subscribe(@old, name, subscriber, checkpoint_threshold: 10)
      ref = Process.monitor(subscription)

      append_mixed()

      assert_receive {:old, {:events, [%RecordedEvent{event_number: 1} = e1]}}
      assert e1.event_type == @known_type
      :ok = Subscription.ack(subscription, 1, subscriber)

      assert_receive {:old, {:subscription_lock_lost, ^subscription}}

      assert_receive {:held, %{system_time: _}, metadata}

      assert %{
               event_store: @old,
               subscription_name: ^name,
               stream_uuid: "$all",
               event_type: @new_type,
               event_number: 2,
               attempt: 1,
               transient: false,
               retry_interval: wait
             } = metadata

      assert is_integer(wait)

      # E2 and E3 are never delivered here.
      refute_receive {:old, {:events, _events}}, 300

      %Storage.Subscription{subscription_id: subscription_id, last_seen: 1} = row(conn, name)

      assert {:ok, %{^subscription_id => %{row_exists: true, lock_held: false}}} =
               TestEventStore.subscription_locks([subscription_id], name: @old)

      # The key is free: another session takes it.
      assert :ok =
               Storage.Lock.try_acquire_exclusive_lock(conn, subscription_id, schema: "public")

      :ok = Storage.Lock.unlock(conn, subscription_id, schema: "public")

      assert fsm(subscription).state == :held
      assert [%{pid: ^subscription, status: :parked}] = local(@old, name)

      assert Process.alive?(subscription)
      assert Process.alive?(subscriber)
      refute_received {:DOWN, ^ref, :process, _pid, _reason}
    end

    test "holds at once when the unknown event is the first one read", %{
      conn: conn,
      subscription_name: name
    } do
      subscriber = forwarder(:old)
      subscription = subscribe(@old, name, subscriber)

      :ok = TestEventStore.append_to_stream(UUID.uuid4(), 0, [new_event(1), known_event(2)])

      assert_receive {:old, {:subscription_lock_lost, ^subscription}}
      assert_receive {:held, _measurements, %{event_number: 1, event_type: @new_type}}

      refute_received {:old, {:events, _events}}
      assert %Storage.Subscription{last_seen: last_seen} = row(conn, name)
      assert last_seen in [nil, 0]
      assert fsm(subscription).state == :held
    end

    test "holds once every event before it is filtered, with no ack to wait for", %{
      conn: conn,
      subscription_name: name
    } do
      subscriber = forwarder(:old)

      subscription =
        subscribe(@old, name, subscriber,
          selector: fn %RecordedEvent{event_number: number} -> number > 1 end
        )

      append_mixed()

      assert_receive {:old, {:subscription_lock_lost, ^subscription}}
      assert_receive {:held, _measurements, %{event_number: 2}}

      refute_received {:old, {:events, _events}}

      # The filtered event counts as delivered.
      assert %Storage.Subscription{last_seen: 1} = row(conn, name)
    end

    test "probes on a jittered backoff without taking the lock, alarming each time", %{
      conn: conn,
      subscription_name: name
    } do
      retry_interval = 400
      subscriber = forwarder(:old)
      subscription = subscribe(@old, name, subscriber, retry_interval: retry_interval)

      append_mixed()
      assert_receive {:old, {:events, [%RecordedEvent{event_number: 1}]}}
      :ok = Subscription.ack(subscription, 1, subscriber)

      alarms =
        for attempt <- 1..4 do
          assert_receive {:held, _measurements, %{attempt: ^attempt} = metadata}, 5_000
          metadata
        end

      for %{attempt: attempt, retry_interval: wait} <- alarms do
        base = min(400 * Integer.pow(2, attempt - 1), retry_interval)

        assert div(base, 2) <= wait
        assert wait <= retry_interval
      end

      %Storage.Subscription{subscription_id: subscription_id} = row(conn, name)

      assert {:ok, %{^subscription_id => %{lock_held: false}}} =
               TestEventStore.subscription_locks([subscription_id], name: @old)

      assert %AdvisoryLocks.State{locks: locks} =
               :sys.get_state(Module.concat(@old, AdvisoryLocks))

      assert locks == %{}

      assert :ok =
               Storage.Lock.try_acquire_exclusive_lock(conn, subscription_id, schema: "public")

      :ok = Storage.Lock.unlock(conn, subscription_id, schema: "public")

      # Held all along, and told its subscriber it lost the lock once.
      assert fsm(subscription).state == :held
      assert_received {:old, {:subscription_lock_lost, ^subscription}}
      refute_received {:old, {:subscription_lock_lost, ^subscription}}
      refute_received {:old, {:subscribed, ^subscription}}
    end

    test "a row past the held event ends the hold, silently", %{
      conn: conn,
      subscription_name: name
    } do
      # The first probe comes within a second; a standby then waits the full five.
      subscriber = forwarder(:old)
      subscription = subscribe(@old, name, subscriber, retry_interval: 5_000)

      append_mixed()
      assert_receive {:old, {:events, [%RecordedEvent{event_number: 1}]}}
      :ok = Subscription.ack(subscription, 1, subscriber)
      assert_receive {:held, _measurements, %{attempt: 1, retry_interval: wait}}
      assert wait <= 1_000

      # As if another instance delivered the held event.
      :ok = Storage.Subscription.ack_last_seen_event(conn, "$all", name, 2, schema: "public")

      Wait.until(3_000, fn -> assert fsm(subscription).state == :disconnected end)
      assert [%{status: :parked}] = local(@old, name)

      refute_receive {:held, _measurements, _metadata}, 500
      refute_received {:old, {:subscribed, ^subscription}}

      # Its next attempt takes the free lock and reads on from the row, past the event it
      # held before.
      :ok = Decree.Store.SubscriptionHelpers.attempt_subscribe(subscription)

      assert_receive {:old, {:subscribed, ^subscription}}
      assert_receive {:old, {:events, [%RecordedEvent{event_number: 3}]}}
      refute_received {:held, _measurements, _metadata}
    end

    test "takes its lock back and delivers once the type is known", %{
      subscription_name: name
    } do
      subscriber = forwarder(:old)
      subscription = subscribe(@old, name, subscriber, retry_interval: 300)

      append_mixed()
      assert_receive {:old, {:events, [%RecordedEvent{event_number: 1}]}}
      :ok = Subscription.ack(subscription, 1, subscriber)
      assert_receive {:held, _measurements, %{attempt: 1}}

      # This instance learns the type, as when the module is loaded after all.
      DenyListSerializer.allow_all()

      assert_receive {:old, {:subscribed, ^subscription}}, 5_000

      assert [2, 3] = receive_and_ack(:old, subscription, subscriber, 2)
      refute_receive {:old, {:events, _events}}, 300
    end
  end

  describe "mixed versions" do
    test "the old instance holds, the new one delivers each held event once", %{
      conn: conn,
      subscription_name: name
    } do
      old_subscriber = forwarder(:old)
      old = subscribe(@old, name, old_subscriber, retry_interval: 300)

      # The new instance's subscription finds the lock taken, and waits.
      new_subscriber = forwarder(:new)

      {:ok, new} =
        TestEventStore.subscribe_to_all_streams(name, new_subscriber, retry_interval: 300)

      append_mixed()

      assert_receive {:old, {:events, [%RecordedEvent{event_number: 1}]}}
      :ok = Subscription.ack(old, 1, old_subscriber)
      assert_receive {:old, {:subscription_lock_lost, ^old}}

      assert_receive {:new, {:subscribed, ^new}}, 5_000
      assert [2, 3] = receive_and_ack(:new, new, new_subscriber, 2)

      refute_receive {:new, {:events, _events}}, 300
      refute_received {:old, {:events, _events}}

      Wait.until(5_000, fn -> assert %Storage.Subscription{last_seen: 3} = row(conn, name) end)

      # The old instance's probe finds the row past the held event and parks silently.
      Wait.until(5_000, fn -> assert fsm(old).state == :disconnected end)
      assert [%{status: :parked}] = local(@old, name)

      flush_held()
      refute_receive {:held, _measurements, _metadata}, 1_000
      refute_received {:old, {:subscribed, ^old}}
      refute_received {:old, {:events, _events}}
    end
  end

  describe "a transient subscription meeting an unknown type" do
    test "delivers the events before it, alarms and stops", %{subscription_name: name} do
      subscriber = forwarder(:old)

      {:ok, subscription} =
        TestEventStore.subscribe_to_all_streams(name, subscriber, name: @old, transient: true)

      assert_receive {:old, {:subscribed, ^subscription}}
      ref = Process.monitor(subscription)

      append_mixed()

      assert_receive {:old, {:events, [%RecordedEvent{event_number: 1}]}}
      :ok = Subscription.ack(subscription, 1, subscriber)

      assert_receive {:DOWN, ^ref, :process, ^subscription,
                      {:shutdown, {:unknown_event_type, @new_type}}}

      assert_receive {:held, _measurements,
                      %{
                        transient: true,
                        event_type: @new_type,
                        event_number: 2,
                        attempt: 1,
                        retry_interval: nil
                      }}

      refute_received {:old, {:events, _events}}
      refute_received {:old, {:subscription_lock_lost, _subscription}}
    end
  end

  describe "the publisher" do
    test "broadcasts the events before an unknown type, and a notice only to subscriptions" do
      stream_uuid = UUID.uuid4()

      plain = forwarder(:plain)
      noticed = forwarder(:noticed)
      opted_in = forwarder(:opted_in)

      run_in(plain, fn -> TestEventStore.subscribe(stream_uuid, name: @old) end)
      run_in(noticed, fn -> PubSub.subscribe(@old, stream_uuid, notices: true) end)

      # The public `subscribe/2` cannot opt in.
      run_in(opted_in, fn -> TestEventStore.subscribe(stream_uuid, name: @old, notices: true) end)

      :ok =
        TestEventStore.append_to_stream(stream_uuid, 0, [
          known_event(1),
          new_event(2),
          known_event(3)
        ])

      assert_receive {:plain, {:events, [%RecordedEvent{stream_version: 1}]}}
      assert_receive {:opted_in, {:events, [%RecordedEvent{stream_version: 1}]}}
      assert_receive {:noticed, {:events, [%RecordedEvent{stream_version: 1}]}}
      assert_receive {:noticed, {:events_pending, 2}}

      refute_receive {:plain, _message}, 300
      refute_received {:opted_in, _message}
      refute_received {:noticed, _message}
    end

    test "a burst of unknown types restarts nothing" do
      supervisor = Process.whereis(@old)
      children = Supervisor.which_children(supervisor)

      notifications =
        Enum.find_value(children, fn {id, pid, _type, _modules} ->
          if id == Module.concat(@old, Decree.Store.Notifications.Supervisor), do: pid
        end)

      assert is_pid(notifications)
      notification_children = Supervisor.which_children(notifications)

      publisher = Process.whereis(Module.concat([@old, Publisher]))
      assert is_pid(publisher)

      plain = forwarder(:plain)
      run_in(plain, fn -> TestEventStore.subscribe("$all", name: @old) end)

      for _append <- 1..20 do
        :ok = TestEventStore.append_to_stream(UUID.uuid4(), 0, [new_event(1)])
      end

      # The publisher handles notifications in order: once it publishes a known event
      # appended last, it has read past the burst.
      :ok = TestEventStore.append_to_stream(UUID.uuid4(), 0, [known_event(1)])

      assert_receive {:plain, {:events, [%RecordedEvent{event_number: 21}]}}, 5_000
      refute_received {:plain, {:events, _events}}

      assert Process.alive?(publisher)
      assert Process.whereis(Module.concat([@old, Publisher])) == publisher
      assert Supervisor.which_children(supervisor) == children
      assert Supervisor.which_children(notifications) == notification_children
    end
  end

  describe "an unknown type between separate appends" do
    # E1, E2 of the new type and E3 in three appends, so three notifications: the publisher
    # cuts only E2's, and broadcasts E3 as usual.
    test "the subscription holds before it; a plain subscriber is sent the later event", %{
      conn: conn,
      subscription_name: name
    } do
      stream_uuid = UUID.uuid4()

      plain = forwarder(:plain)
      run_in(plain, fn -> TestEventStore.subscribe("$all", name: @old) end)

      subscriber = forwarder(:old)
      subscription = subscribe(@old, name, subscriber)

      :ok = TestEventStore.append_to_stream(stream_uuid, 0, [known_event(1)])

      assert_receive {:old, {:events, [%RecordedEvent{event_number: 1}]}}
      :ok = Subscription.ack(subscription, 1, subscriber)
      assert_receive {:plain, {:events, [%RecordedEvent{event_number: 1}]}}

      :ok = TestEventStore.append_to_stream(stream_uuid, 1, [new_event(2)])

      assert_receive {:old, {:subscription_lock_lost, ^subscription}}
      assert_receive {:held, _measurements, %{event_number: 2, event_type: @new_type}}

      :ok = TestEventStore.append_to_stream(stream_uuid, 2, [known_event(3)])

      # The plain subscriber is sent E3, with E2 missing before it, and no notice: it must
      # check continuity itself. The publisher works in order, so nothing else is on its way.
      assert_receive {:plain,
                      {:events, [%RecordedEvent{event_number: 3, stream_uuid: ^stream_uuid}]}}

      refute_received {:plain, _message}

      # E3 reached the subscription process too, which stays held and delivers nothing.
      Wait.until(3_000, fn -> assert fsm(subscription).data.last_received == 3 end)

      refute_receive {:old, {:events, _events}}, 300
      assert fsm(subscription).state == :held
      assert [%{pid: ^subscription, status: :parked}] = local(@old, name)
      assert %Storage.Subscription{last_seen: 1} = row(conn, name)
    end
  end

  describe "reads" do
    test "raise UnknownEventTypeError, naming the type and number, never the data" do
      stream_uuid = UUID.uuid4()

      :ok =
        TestEventStore.append_to_stream(stream_uuid, 0, [
          known_event(1),
          %EventData{
            event_type: @new_type,
            data: %NewReleaseEvent{event: "secret-payload"},
            metadata: %{}
          }
        ])

      error =
        assert_raise UnknownEventTypeError, fn ->
          TestEventStore.read_stream_forward(stream_uuid, 0, 10, name: @old)
        end

      assert %UnknownEventTypeError{type: @new_type, event_number: 2} = error

      message = Exception.message(error)
      assert message =~ @new_type
      assert message =~ "2"
      refute message =~ "secret-payload"

      assert_raise UnknownEventTypeError, fn ->
        stream_uuid |> TestEventStore.stream_forward(0, name: @old) |> Enum.to_list()
      end

      assert_raise UnknownEventTypeError, fn ->
        TestEventStore.read_all_streams_forward(0, 10, name: @old)
      end

      # The new instance knows the type.
      assert {:ok, [_e1, %RecordedEvent{data: %NewReleaseEvent{}}]} =
               TestEventStore.read_stream_forward(stream_uuid, 0, 10)
    end

    test "deserialize_known stops at the first unknown type" do
      stream_uuid = UUID.uuid4()

      :ok =
        TestEventStore.append_to_stream(stream_uuid, 0, [
          known_event(1),
          new_event(2),
          known_event(3)
        ])

      {:ok, recorded} = TestEventStore.read_stream_forward(stream_uuid, 0, 10)

      raw =
        Enum.map(recorded, fn %RecordedEvent{data: data} = event ->
          %RecordedEvent{event | data: Jason.encode!(data), metadata: "{}"}
        end)

      assert {[%RecordedEvent{event_number: 1}], {2, @new_type}} =
               RecordedEvent.deserialize_known(raw, DenyListSerializer)

      assert {[_e1, _e2, _e3], nil} =
               RecordedEvent.deserialize_known(raw, Decree.Store.JsonSerializer)
    end
  end

  describe "the held backoff" do
    test "draws each wait between half the capped backoff and the retry interval, never 0" do
      for retry_interval <- [1, 2, 3, 200, 1_000, 5_000, 60_000],
          attempt <- 1..12,
          _sample <- 1..50 do
        base = min(min(1_000, retry_interval) * Integer.pow(2, attempt - 1), retry_interval)
        wait = SubscriptionFsm.held_wait(attempt, retry_interval)

        assert wait >= 1
        assert div(base, 2) <= wait
        assert wait <= base
        assert wait <= retry_interval
      end
    end
  end

  defp subscribe(event_store, name, subscriber, opts \\ []) do
    {:ok, subscription} =
      TestEventStore.subscribe_to_all_streams(
        name,
        subscriber,
        Keyword.merge([name: event_store, buffer_size: 10], opts)
      )

    tag = if event_store == @old, do: :old, else: :new

    assert_receive {^tag, {:subscribed, ^subscription}}

    subscription
  end

  # [E1, E2 of the new type, E3] in one append, so one notification.
  defp append_mixed do
    :ok =
      TestEventStore.append_to_stream(UUID.uuid4(), 0, [
        known_event(1),
        new_event(2),
        known_event(3)
      ])
  end

  defp known_event(number), do: EventFactory.create_event(nil, number)

  defp new_event(number) do
    %EventData{
      event_type: @new_type,
      data: %NewReleaseEvent{event: number},
      metadata: %{}
    }
  end

  defp receive_and_ack(tag, subscription, subscriber, count, acc \\ [])

  defp receive_and_ack(_tag, _subscription, _subscriber, 0, acc), do: Enum.reverse(acc)

  defp receive_and_ack(tag, subscription, subscriber, count, acc) do
    assert_receive {^tag, {:events, events}}, 5_000

    for %RecordedEvent{event_number: number} <- events do
      :ok = Subscription.ack(subscription, number, subscriber)
    end

    numbers = Enum.map(events, & &1.event_number)

    receive_and_ack(
      tag,
      subscription,
      subscriber,
      count - length(numbers),
      Enum.reverse(numbers, acc)
    )
  end

  defp row(conn, name) do
    {:ok, row} = Storage.Subscription.subscription(conn, "$all", name, schema: "public")

    row
  end

  defp fsm(subscription) do
    %Subscription{subscription: %SubscriptionFsm{data: %SubscriptionState{}} = fsm} =
      :sys.get_state(subscription)

    fsm
  end

  defp local(event_store, name) do
    {infos, 0} = TestEventStore.local_subscriptions(name: event_store)

    Enum.filter(infos, &(&1.subscription_name == name))
  end

  defp flush_held do
    receive do
      {:held, _measurements, _metadata} -> flush_held()
    after
      0 -> :ok
    end
  end

  # A process that forwards every message to the test as `{tag, message}`, and runs a
  # function on request, so it can subscribe and ack as itself.
  defp forwarder(tag) do
    test = self()

    pid =
      spawn_link(fn ->
        loop = fn loop ->
          receive do
            {:run, ref, fun} ->
              send(test, {:ran, ref, fun.()})

            message ->
              send(test, {tag, message})
          end

          loop.(loop)
        end

        loop.(loop)
      end)

    on_exit(fn -> Process.exit(pid, :kill) end)

    pid
  end

  defp run_in(pid, fun) do
    ref = make_ref()
    send(pid, {:run, ref, fun})

    assert_receive {:ran, ^ref, :ok}
  end
end
