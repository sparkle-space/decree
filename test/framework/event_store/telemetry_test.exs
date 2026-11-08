defmodule Decree.EventStore.TelemetryTest do
  use Decree.PostgresCase

  alias Decree.DefaultApp
  alias Decree.EventStore
  alias Decree.EventStore.EventData
  alias Decree.EventStore.RecordedEvent
  alias Decree.EventStore.SnapshotData
  alias Decree.Middleware.Commands.IncrementCount
  alias Decree.Middleware.Commands.RaiseError
  alias Decree.UUID

  setup do
    start_supervised!(DefaultApp)
    attach_telemetry()

    :ok
  end

  defmodule TestRouter do
    use Decree.Commands.Router

    alias Decree.Middleware.Commands.CommandHandler
    alias Decree.Middleware.Commands.CounterAggregateRoot

    dispatch IncrementCount,
      to: CommandHandler,
      aggregate: CounterAggregateRoot,
      identity: :aggregate_uuid

    dispatch RaiseError,
      to: CommandHandler,
      aggregate: CounterAggregateRoot,
      identity: :aggregate_uuid
  end

  describe "snapshotting telemetry events" do
    test "emit `[:decree, :event_store, :record_snapshot, :start | :stop]` event" do
      snapshot = %SnapshotData{
        source_uuid: UUID.uuid4(),
        source_version: 1,
        source_type: "Elixir.Decree.Aggregates.ExampleAggregate",
        data: %{},
        metadata: %{}
      }

      assert :ok = EventStore.record_snapshot(DefaultApp, snapshot)

      assert_receive {[:decree, :event_store, :record_snapshot, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :record_snapshot, :stop], 2, _meas, meta}
      assert %{application: DefaultApp, snapshot: ^snapshot} = meta
    end

    test "emit `[:decree, :event_store, :read_snapshot, :start | :stop]` event" do
      uuid = UUID.uuid4()
      assert {:error, :snapshot_not_found} = EventStore.read_snapshot(DefaultApp, uuid, 1)

      assert_receive {[:decree, :event_store, :read_snapshot, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :read_snapshot, :stop], 2, _meas, meta}
      assert %{application: DefaultApp, source_uuid: ^uuid, snapshot_version: 1} = meta
    end

    test "emit `[:decree, :event_store, :prune_snapshots, :start | :stop]` event" do
      type = "Elixir.Decree.Aggregates.ExampleAggregate"
      assert {:ok, 0} = EventStore.prune_snapshots(DefaultApp, type, 2)

      assert_receive {[:decree, :event_store, :prune_snapshots, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :prune_snapshots, :stop], 2, _meas, meta}
      assert %{application: DefaultApp, source_type: ^type, snapshot_version: 2} = meta
    end

    test "emit `[:decree, :event_store, :delete_snapshot, :start | :stop]` event" do
      uuid = UUID.uuid4()
      assert :ok = EventStore.delete_snapshot(DefaultApp, uuid)

      assert_receive {[:decree, :event_store, :delete_snapshot, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :delete_snapshot, :stop], 2, _meas, meta}
      assert %{application: DefaultApp, source_uuid: ^uuid} = meta
    end
  end

  describe "streaming telemetry events" do
    test "emit `[:decree, :event_store, :stream_forward, :start | :stop]` event" do
      uuid = UUID.uuid4()
      assert {:error, :stream_not_found} = EventStore.stream_forward(DefaultApp, uuid)

      assert_receive {[:decree, :event_store, :stream_forward, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :stream_forward, :stop], 2, _meas, meta}

      assert %{
               application: DefaultApp,
               stream_uuid: ^uuid,
               start_version: 0,
               read_batch_size: 1_000
             } = meta
    end
  end

  describe "ack_event telemetry events" do
    test "emit `[:decree, :event_store, :ack_event, :start | :stop]` event" do
      # The Postgres store acks an event on a live subscription.
      {:ok, pid} = EventStore.subscribe_to(DefaultApp, :all, "Test", self(), :origin)
      assert_receive {:subscribed, ^pid}

      event_data = %EventData{event_type: "Elixir.Decree.Event.ReplyEvent", data: %{}}
      :ok = EventStore.append_to_stream(DefaultApp, UUID.uuid4(), 0, [event_data])
      assert_receive {:events, [%RecordedEvent{} = event]}

      assert :ok = EventStore.ack_event(DefaultApp, pid, event)

      assert_receive {[:decree, :event_store, :ack_event, :start], 5, _meas, _meta}
      assert_receive {[:decree, :event_store, :ack_event, :stop], 6, _meas, meta}
      assert %{application: DefaultApp, subscription: ^pid, event: ^event} = meta
    end
  end

  describe "append_to_stream telemetry events" do
    test "emit `[:decree, :event_store, :append_to_stream, :start | :stop]` event" do
      uuid = UUID.uuid4()
      event = %EventData{event_type: "Elixir.Decree.Event.ReplyEvent", data: %{}, metadata: %{}}
      assert :ok = EventStore.append_to_stream(DefaultApp, uuid, 0, [event])

      assert_receive {[:decree, :event_store, :append_to_stream, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :append_to_stream, :stop], 2, _meas, meta}
      assert %{application: DefaultApp, expected_version: 0, stream_uuid: ^uuid} = meta
    end
  end

  describe "subscription telemetry events" do
    test "emit `[:decree, :event_store, :subscribe, :start | :stop]` event" do
      uuid = UUID.uuid4()
      assert :ok = EventStore.subscribe(DefaultApp, uuid)

      assert_receive {[:decree, :event_store, :subscribe, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :subscribe, :stop], 2, _meas, meta}
      assert %{application: DefaultApp, stream_uuid: ^uuid} = meta
    end

    test "emit `[:decree, :event_store, :subscribe_to, :start | :stop]` event" do
      subscriber = self()
      assert {:ok, pid} = EventStore.subscribe_to(DefaultApp, :all, "Test", subscriber, :current)

      assert_receive {:subscribed, ^pid}
      assert_receive {[:decree, :event_store, :subscribe_to, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :subscribe_to, :stop], 2, _meas, meta}

      assert %{
               application: DefaultApp,
               stream_uuid: :all,
               subscription_name: "Test",
               subscriber: ^subscriber,
               start_from: :current
             } = meta
    end

    test "emit `[:decree, :event_store, :unsubscribe, :start | :stop]` event" do
      assert {:ok, pid} = EventStore.subscribe_to(DefaultApp, :all, "Test", self(), :current)

      assert_receive {:subscribed, ^pid}

      assert_receive {[:decree, :event_store, :subscribe_to, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :subscribe_to, :stop], 2, _meas, _meta}

      assert :ok = EventStore.unsubscribe(DefaultApp, pid)

      assert_receive {[:decree, :event_store, :unsubscribe, :start], 3, _meas, _meta}
      assert_receive {[:decree, :event_store, :unsubscribe, :stop], 4, _meas, meta}
      assert %{application: DefaultApp, subscription: ^pid} = meta
    end

    test "emit `[:decree, :event_store, :delete_subscription, :start | :stop]` event" do
      # The Postgres store treats deleting an unknown subscription as done.
      assert :ok = EventStore.delete_subscription(DefaultApp, :all, "Test")

      assert_receive {[:decree, :event_store, :delete_subscription, :start], 1, _meas, _meta}
      assert_receive {[:decree, :event_store, :delete_subscription, :stop], 2, _meas, meta}
      assert %{application: DefaultApp, subscribe_to: :all, handler_name: "Test"} = meta
    end
  end

  defp attach_telemetry do
    agent = start_supervised!({Agent, fn -> 1 end})
    handler = :"#{__MODULE__}-handler"

    events = [
      :ack_event,
      :append_to_stream,
      :delete_snapshot,
      :delete_subscription,
      :prune_snapshots,
      :record_snapshot,
      :read_snapshot,
      :stream_forward,
      :subscribe,
      :subscribe_to,
      :unsubscribe
    ]

    :telemetry.attach_many(
      handler,
      Enum.flat_map(events, fn event ->
        [
          [:decree, :event_store, event, :start],
          [:decree, :event_store, event, :stop]
        ]
      end),
      fn event_name, measurements, metadata, reply_to ->
        num = Agent.get_and_update(agent, fn n -> {n, n + 1} end)
        send(reply_to, {event_name, num, measurements, metadata})
      end,
      self()
    )

    on_exit(fn ->
      :telemetry.detach(handler)
    end)
  end
end
