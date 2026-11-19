defmodule Decree.Event.EventHandlerBatchTelemetryTest do
  use Decree.PostgresCase

  alias Decree.Event.Handler
  alias Decree.EventStore.Subscription

  # Test support code
  alias Decree.Helpers.EventFactory
  alias Decree.Event.{BatchHandler, ErrorHandlingBatchHandler}
  alias Decree.Event.ReplyEvent

  setup do
    attach_telemetry()
    :ok
  end

  describe "batch event handler telemetry" do
    test "should emit `[:decree, :event, :batch, :stop]` telemetry from :ok" do
      events = [
        %ReplyEvent{reply_to: self(), value: 1},
        %ReplyEvent{reply_to: self(), value: 2}
      ]

      metadata = %{"key" => "value"}
      recorded_events = EventFactory.map_to_recorded_events(events, 1, metadata: metadata)
      state = setup_state(BatchHandler)

      {:noreply, _state} = Handler.handle_info({:events, recorded_events}, state)

      assert_receive {[:decree, :event, :batch, :start], _measurements, _metadata}

      assert_receive {[:decree, :event, :batch, :stop], _measurements, metadata}
      assert metadata.event_count == 2

      refute_received {[:decree, :event, :batch, :exception], _measurements, _metadata}
    end

    test "should emit `[:decree, :event, :batch, :stop]` telemetry from :error" do
      events = [
        %ReplyEvent{reply_to: self(), value: :error},
        %ReplyEvent{reply_to: self(), value: :error}
      ]

      metadata = %{"key" => "value"}
      recorded_events = EventFactory.map_to_recorded_events(events, 1, metadata: metadata)
      state = setup_state(BatchHandler)

      {:stop, :bad_value, _state} = Handler.handle_info({:events, recorded_events}, state)

      assert_receive {[:decree, :event, :batch, :start], _measurements, _metadata}
      assert_receive {[:decree, :event, :batch, :stop], _measurements, _metadata}
      refute_received {[:decree, :event, :batch, :exception], _measurements, _metadata}
    end

    test "should include count of successfully processed events when erroring on a specific event" do
      events = [
        %ReplyEvent{reply_to: self(), value: 1},
        %ReplyEvent{reply_to: self(), value: 2},
        %ReplyEvent{reply_to: self(), value: :error},
        %ReplyEvent{reply_to: self(), value: 4}
      ]

      metadata = %{"key" => "value"}
      recorded_events = EventFactory.map_to_recorded_events(events, 1, metadata: metadata)
      state = setup_state(BatchHandler)

      Handler.handle_info({:events, recorded_events}, state)

      assert_receive {[:decree, :event, :batch, :start], _measurements, metadata}
      assert metadata.event_count == 4

      assert_receive {[:decree, :event, :batch, :stop], _measurements, metadata}
      assert metadata.event_count == 4
    end

    test "should emit `[:decree, :event, :batch, :exception]` telemetry from thrown exception" do
      events = [
        %ReplyEvent{reply_to: self(), value: :raise},
        %ReplyEvent{reply_to: self(), value: :raise}
      ]

      metadata = %{"key" => "value"}
      recorded_events = EventFactory.map_to_recorded_events(events, 1, metadata: metadata)
      state = setup_state(ErrorHandlingBatchHandler)

      {:stop, _, _} = Handler.handle_info({:events, recorded_events}, state)

      assert_receive {[:decree, :event, :batch, :start], _measurements, _metadata}
      refute_received {[:decree, :event, :batch, :stop], _measurements, _metadata}
      assert_receive {[:decree, :event, :batch, :exception], _measurements, _metadata}
    end
  end

  defp setup_state(handler_module) do
    Decree.MockEventStoreCase.associate_acking_mock(__MODULE__)

    %Handler{
      subscription:
        struct(Subscription,
          application: __MODULE__,
          subscription_pid: self()
        ),
      handler_callback: :batch,
      handler_module: handler_module,
      application: __MODULE__,
      last_seen_event: 0
    }
  end

  defp attach_telemetry do
    :telemetry.attach_many(
      "test-handler",
      [
        [:decree, :event, :batch, :start],
        [:decree, :event, :batch, :stop],
        [:decree, :event, :batch, :exception]
      ],
      fn event_name, measurements, metadata, reply_to ->
        send(reply_to, {event_name, measurements, metadata})
      end,
      self()
    )

    on_exit(fn ->
      :telemetry.detach("test-handler")
    end)
  end
end
