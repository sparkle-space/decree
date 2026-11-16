defmodule Decree.Store.Subscriptions.SubscriptionUndecodableEventTest do
  use Decree.Store.StorageCase

  # An event of a known type whose stored bytes the serializer cannot decode is never skipped
  # and never crashes the subscription: it holds before it, as before an unknown type
  # (sparkle-space/sparklespace#8357). A crash restarted nothing in the store, since the
  # subscription process is `:temporary`, but its subscriber saw it go down, and an event
  # handler (`:permanent`) stopped with it, restarted, read the same event and stopped again.
  #
  # Corrupt bytes are planted through `Appender.append/4`, a real append, with data that is
  # not JSON and prints as text, so a log line or crash report that carries the
  # serializer's error shows the marker.

  import ExUnit.CaptureLog
  import Decree.Store.SubscriptionHelpers, only: [wait_for_lock_pool: 1]

  alias Decree.Store.{
    EventData,
    EventFactory,
    ExitingSerializer,
    FlaggedRaisingSerializer,
    RecordedEvent,
    Storage,
    UUID
  }

  alias Decree.Store.Storage.Appender
  alias Decree.Store.Subscriptions.{Subscription, SubscriptionFsm, SubscriptionState}

  @marker "undecodable-subscription-marker"
  @held [:decree, :store, :subscription, :held]
  @held_undecodable [:decree, :store, :subscription, :held_undecodable]
  @known_type "Elixir.Decree.Store.EventFactory.Event"
  @flagged :flagged_store
  @exiting :exiting_store

  setup do
    test = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        [@held, @held_undecodable],
        fn event, measurements, metadata, _config ->
          send(test, {List.last(event), measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    wait_for_lock_pool(TestEventStore)

    [subscription_name: UUID.uuid4(), stream_uuid: UUID.uuid4()]
  end

  describe "a persistent subscription meeting an undecodable event" do
    test "delivers and checkpoints the events before it, then holds without its lock", %{
      conn: conn,
      schema: schema,
      subscription_name: name,
      stream_uuid: stream_uuid
    } do
      plant(conn, schema, stream_uuid)

      log =
        capture_log(fn ->
          # A checkpoint threshold the one acked event never reaches: only the hold writes it.
          subscription = subscribe(TestEventStore, stream_uuid, name, checkpoint_threshold: 10)
          ref = Process.monitor(subscription)

          assert_receive {:events, [%RecordedEvent{event_number: 1} = e1]}
          assert %EventFactory.Event{event: 1} = e1.data
          :ok = Subscription.ack(subscription, 1, self())

          assert_receive {:subscription_lock_lost, ^subscription}
          assert_receive {:held_undecodable, %{system_time: _}, metadata}

          assert %{
                   event_store: TestEventStore,
                   subscription_name: ^name,
                   stream_uuid: ^stream_uuid,
                   event_type: @known_type,
                   event_number: 2,
                   error: Jason.DecodeError,
                   attempt: 1,
                   transient: false,
                   retry_interval: wait
                 } = metadata

          assert is_integer(wait)

          # Neither the held event nor the one after it is delivered, and the hold is not
          # the unknown type's.
          refute_receive {:events, _events}, 300
          refute_received {:held, _measurements, _metadata}

          %Storage.Subscription{subscription_id: subscription_id, last_seen: 1} =
            row(conn, stream_uuid, name)

          assert {:ok, %{^subscription_id => %{row_exists: true, lock_held: false}}} =
                   TestEventStore.subscription_locks([subscription_id])

          assert fsm(subscription).state == :held
          assert [%{pid: ^subscription, status: :parked}] = local(TestEventStore, name)

          assert Process.alive?(subscription)
          refute_received {:DOWN, ^ref, :process, _pid, _reason}
        end)

      assert log =~
               "Subscription #{inspect(name)}@#{inspect(suffix(stream_uuid))} is held before " <>
                 "event 2: it could not be deserialized due to: Jason.DecodeError, attempt 1, " <>
                 "probing again in "

      refute log =~ @marker
      refute log =~ stream_uuid
    end

    test "an exit in the serializer's deserialize holds it too", %{
      subscription_name: name,
      stream_uuid: stream_uuid
    } do
      start_supervised!({TestEventStore, name: @exiting, serializer: ExitingSerializer},
        id: @exiting
      )

      wait_for_lock_pool(@exiting)

      :ok =
        TestEventStore.append_to_stream(stream_uuid, 0, [
          known_event(1),
          marked_event(ExitingSerializer.data_marker()),
          known_event(3)
        ])

      log =
        capture_log(fn ->
          subscription = subscribe(@exiting, stream_uuid, name)
          ref = Process.monitor(subscription)

          assert_receive {:events, [%RecordedEvent{event_number: 1}]}
          :ok = Subscription.ack(subscription, 1, self())

          assert_receive {:subscription_lock_lost, ^subscription}

          assert_receive {:held_undecodable, _measurements,
                          %{event_number: 2, error: {:noproc, :redacted}, attempt: 1}}

          assert fsm(subscription).state == :held
          refute_received {:DOWN, ^ref, :process, _pid, _reason}
        end)

      assert log =~ "it could not be deserialized due to: {:noproc, :redacted}"
      refute log =~ ExitingSerializer.data_marker()
    end

    test "probes on a backoff while still undecodable, and delivers once it decodes", %{
      subscription_name: name,
      stream_uuid: stream_uuid
    } do
      FlaggedRaisingSerializer.raising!()
      on_exit(fn -> FlaggedRaisingSerializer.decoding!() end)

      start_supervised!({TestEventStore, name: @flagged, serializer: FlaggedRaisingSerializer},
        id: @flagged
      )

      wait_for_lock_pool(@flagged)

      :ok =
        TestEventStore.append_to_stream(stream_uuid, 0, [
          known_event(1),
          marked_event(FlaggedRaisingSerializer.marker()),
          known_event(3)
        ])

      log =
        capture_log(fn ->
          subscription = subscribe(@flagged, stream_uuid, name, retry_interval: 300)
          ref = Process.monitor(subscription)

          assert_receive {:events, [%RecordedEvent{event_number: 1}]}
          :ok = Subscription.ack(subscription, 1, self())

          assert_receive {:held_undecodable, _measurements,
                          %{event_number: 2, error: ArgumentError, attempt: 1}}

          # The probe re-reads the one event, which still does not decode: held again.
          assert_receive {:held_undecodable, _measurements,
                          %{event_number: 2, error: ArgumentError, attempt: 2, retry_interval: w}},
                         5_000

          assert is_integer(w)
          assert fsm(subscription).state == :held
          refute_received {:subscribed, ^subscription}

          # The codec is fixed: the next probe resubscribes and delivers the held event and
          # the one after it.
          FlaggedRaisingSerializer.decoding!()

          assert_receive {:subscribed, ^subscription}, 5_000
          assert [2, 3] = receive_and_ack(subscription, 2)
          refute_receive {:events, _events}, 300

          refute_received {:held, _measurements, _metadata}
          refute_received {:DOWN, ^ref, :process, _pid, _reason}
        end)

      assert log =~ "it could not be deserialized due to: ArgumentError, attempt 2, probing again"
      refute log =~ FlaggedRaisingSerializer.marker()
      refute log =~ stream_uuid
    end
  end

  describe "a transient subscription meeting an undecodable event" do
    test "delivers the events before it, alarms and stops", %{
      conn: conn,
      schema: schema,
      subscription_name: name,
      stream_uuid: stream_uuid
    } do
      plant(conn, schema, stream_uuid)

      log =
        capture_log(fn ->
          {:ok, subscription} =
            TestEventStore.subscribe_to_stream(stream_uuid, name, self(),
              transient: true,
              buffer_size: 10
            )

          assert_receive {:subscribed, ^subscription}
          ref = Process.monitor(subscription)

          assert_receive {:events, [%RecordedEvent{event_number: 1}]}
          :ok = Subscription.ack(subscription, 1, self())

          assert_receive {:DOWN, ^ref, :process, ^subscription,
                          {:shutdown, {:undecodable_event, Jason.DecodeError}}}

          assert_receive {:held_undecodable, _measurements,
                          %{
                            transient: true,
                            event_type: @known_type,
                            event_number: 2,
                            error: Jason.DecodeError,
                            attempt: 1,
                            retry_interval: nil
                          }}

          refute_received {:events, _events}
          refute_received {:subscription_lock_lost, _subscription}
        end)

      assert log =~ "it could not be deserialized due to: Jason.DecodeError, stopping"
      refute log =~ @marker
      refute log =~ stream_uuid
    end
  end

  # [E1, E2 with data that is not JSON, E3] in one append to `stream_uuid`.
  defp plant(conn, schema, stream_uuid) do
    [first, second, third] = EventFactory.create_recorded_events(3, stream_uuid)

    :ok =
      Appender.append(conn, nil, [first, %RecordedEvent{second | data: @marker}, third],
        schema: schema
      )
  end

  defp subscribe(event_store, stream_uuid, name, opts \\ []) do
    {:ok, subscription} =
      TestEventStore.subscribe_to_stream(
        stream_uuid,
        name,
        self(),
        Keyword.merge([name: event_store, buffer_size: 10], opts)
      )

    assert_receive {:subscribed, ^subscription}

    subscription
  end

  defp known_event(number), do: EventFactory.create_event(nil, number)

  defp marked_event(marker) do
    %EventData{
      event_type: @known_type,
      data: %EventFactory.Event{event: marker},
      metadata: %{}
    }
  end

  defp receive_and_ack(subscription, count, acc \\ [])

  defp receive_and_ack(_subscription, 0, acc), do: Enum.reverse(acc)

  defp receive_and_ack(subscription, count, acc) do
    assert_receive {:events, events}, 5_000

    for %RecordedEvent{event_number: number} <- events do
      :ok = Subscription.ack(subscription, number, self())
    end

    numbers = Enum.map(events, & &1.event_number)

    receive_and_ack(subscription, count - length(numbers), Enum.reverse(numbers, acc))
  end

  defp row(conn, stream_uuid, name) do
    {:ok, row} = Storage.Subscription.subscription(conn, stream_uuid, name, schema: "public")

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

  defp suffix(stream_uuid), do: "..." <> String.slice(stream_uuid, -8, 8)
end
