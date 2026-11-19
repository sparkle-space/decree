defmodule Decree.Event.EventHandlerBatchTest do
  use ExUnit.Case, async: true

  alias Decree.Event.Handler
  alias Decree.EventStore.Subscription

  # Test support code
  alias Decree.Helpers.EventFactory
  alias Decree.Event.{BatchHandler, ErrorHandlingBatchHandler}
  alias Decree.Event.ErrorAggregate.Events.ErrorEvent
  alias Decree.Event.ReplyEvent

  describe "batch handling" do
    test "should receive events in batches" do
      event1 = %ReplyEvent{reply_to: self(), value: 1}
      event2 = %ReplyEvent{reply_to: self(), value: 2}
      events = [event1, event2]

      metadata = %{"key" => "value"}
      recorded_events = EventFactory.map_to_recorded_events(events, 1, metadata: metadata)
      state = setup_state(BatchHandler)

      {:noreply, _state} = Handler.handle_info({:events, recorded_events}, state)

      assert_received {:batch, _pid, _events, _metadata}
    end
  end

  describe "failure handling with no error handler" do
    # See BatchHandler for how the special values in ReplyEvents are handled

    test "should fail on error" do
      event = %ReplyEvent{reply_to: self(), value: :error}

      metadata = %{}
      recorded_events = EventFactory.map_to_recorded_events([event], 1, metadata: metadata)
      state = setup_state(BatchHandler)

      {:stop, :bad_value, _state} = Handler.handle_info({:events, recorded_events}, state)
    end
  end

  describe "failure handling with error handler" do
    test "skip acknowledges the last event" do
      event1 = %ErrorEvent{reply_to: self(), strategy: "skip"}
      event2 = %ReplyEvent{reply_to: self(), value: 2}

      events = [event1, event2]

      metadata = %{}
      recorded_events = EventFactory.map_to_recorded_events(events, 1, metadata: metadata)
      last_recorded_event = List.last(recorded_events)
      state = setup_state(ErrorHandlingBatchHandler)

      Handler.handle_info({:events, recorded_events}, state)
      assert_received {:error, :skipping}
      # The store's `ack/2` receives the acknowledged event's number.
      last_event_number = last_recorded_event.event_number
      assert_received {:acked, ^last_event_number}
    end

    test "should stop" do
      event1 = %ErrorEvent{reply_to: self(), strategy: "default"}
      event2 = %ReplyEvent{reply_to: self(), value: 2}
      events = [event1, event2]

      metadata = %{}
      recorded_events = EventFactory.map_to_recorded_events(events, 1, metadata: metadata)
      state = setup_state(ErrorHandlingBatchHandler)

      {:stop, _reason, _state} = Handler.handle_info({:events, recorded_events}, state)
    end

    test "should retry" do
      event1 = %ErrorEvent{reply_to: self(), strategy: "retry"}
      event2 = %ReplyEvent{reply_to: self(), value: 2}
      events = [event1, event2]

      metadata = %{}
      recorded_events = EventFactory.map_to_recorded_events(events, 1, metadata: metadata)
      state = setup_state(ErrorHandlingBatchHandler)

      {:stop, :too_many_failures, _state} = Handler.handle_info({:events, recorded_events}, state)
      assert_received {:error, :retry, %{failures: 1}}
      assert_received {:error, :retry, %{failures: 2}}
      assert_received {:error, :too_many_failures, %{failures: 3}}
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
end
