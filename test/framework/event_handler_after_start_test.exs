defmodule Decree.Event.HandlerAfterStartTest do
  use Decree.MockEventStoreCase

  import Mox
  import ExUnit.CaptureLog

  alias Decree.EventStore.Mock, as: MockEventStore

  setup do
    stub(MockEventStore, :subscribe_to_all_streams, fn _handler_name, handler, _opts ->
      {:ok, handler}
    end)

    :ok
  end

  describe "event handler `init/0` callback" do
    # TODO: remove these test when we remove init/0

    setup(%{test: test}) do
      # HACK: generate a module that can communicate back to our test process
      true = Process.register(self(), test)

      Code.eval_string("""
        defmodule DeprecatedHandler do
          use Decree.Event.Handler,
            application: Decree.MockedApp,
            name: __MODULE__

          @impl Decree.Event.Handler
          def init() do
            process_name = :"#{test}"
            Process.send(process_name, {:init, self()}, [])
          end
        end
      """)

      [handler: start_supervised!(DeprecatedHandler)]
    end

    test "should be called and a deprecation warning raised after handler subscribbed", %{
      handler: handler
    } do
      warning =
        capture_log([level: :warning], fn ->
          # When the handler subscribes to the eventstore
          send_subscribed(handler)

          # Then we expect init/0 to have been called for us
          assert_receive {:init, ^handler}
        end)

      # And we expect a deprecation warning to have been logged
      assert warning =~ "DeprecatedHandler.init/0 is deprecated, use after_start/1 instead"
    end
  end

  describe "event handler `after_start/1` callback" do
    defmodule AfterStartHandler do
      use Decree.Event.Handler,
        application: Decree.MockedApp,
        name: __MODULE__

      def after_start(state) do
        test_pid = Map.fetch!(state, :test)
        ref = Map.get_lazy(state, :ref, &make_ref/0)
        reply = Map.get(state, :reply, :ok)

        Process.send(test_pid, {ref, :after_start, reply}, [])
        reply
      end
    end

    test "should be called after handler subscribed" do
      ref = make_ref()
      state = %{test: self(), ref: ref}
      handler = start_supervised!({AfterStartHandler, state: state})

      refute_receive {^ref, :after_start, :ok}

      send_subscribed(handler)

      assert_receive {^ref, :after_start, :ok}
    end

    test "should not be called again when the subscription resubscribes after a lock loss" do
      ref = make_ref()
      state = %{test: self(), ref: ref}
      handler = start_supervised!({AfterStartHandler, state: state})

      send_subscribed(handler)
      assert_receive {^ref, :after_start, :ok}

      send(handler, {:subscription_lock_lost, handler})
      send_subscribed(handler)

      refute_receive {^ref, :after_start, :ok}
      assert Process.alive?(handler)
    end

    test "should be called again after a reset" do
      subscription = stub_stand_in_subscription()
      stub(MockEventStore, :delete_subscription, fn "$all", _name, _opts -> :ok end)

      ref = make_ref()
      state = %{test: self(), ref: ref}
      handler = start_supervised!({AfterStartHandler, state: state})

      send_subscribed(handler, subscription)
      assert_receive {^ref, :after_start, :ok}

      send(handler, :reset)
      send_subscribed(handler, subscription)

      assert_receive {^ref, :after_start, :ok}
    end

    test "should reply with new state" do
      ref = make_ref()
      state = %{test: self(), ref: ref, reply: {:ok, %{something: :new}}}
      handler = start_supervised!({AfterStartHandler, state: state})

      send_subscribed(handler)

      assert_receive {^ref, :after_start, {:ok, new_state}}
      assert new_state == %{something: :new}
    end
  end

  describe "subscription lock lost" do
    defmodule BatchHandler do
      use Decree.Event.Handler,
        application: Decree.MockedApp,
        name: __MODULE__,
        batch_size: 10,
        batch_timeout: 60_000,
        personal_fields: :opaque

      def handle_batch(_events), do: :ok
    end

    test "should drop the events buffered for a batch, without an error log" do
      handler = start_supervised!(BatchHandler)
      send_subscribed(handler)

      :sys.replace_state(handler, fn state ->
        timer = Process.send_after(handler, :flush_batch_timeout, 60_000)
        %{state | batch_buffer: [:buffered_event], batch_timer_ref: timer}
      end)

      log =
        capture_log(fn ->
          send(handler, {:subscription_lock_lost, handler})

          assert %{batch_buffer: [], batch_timer_ref: nil} = :sys.get_state(handler)
        end)

      assert log =~ "lost its advisory lock"
      refute log =~ "unexpected message"
    end

    test "should ignore a lock loss from a subscription it no longer uses" do
      handler = start_supervised!(BatchHandler)
      send_subscribed(handler)

      log =
        capture_log(fn ->
          send(handler, {:subscription_lock_lost, self()})
          assert %{batch_buffer: []} = :sys.get_state(handler)
        end)

      refute log =~ "unexpected message"
      refute log =~ "lost its advisory lock"
    end
  end

  describe "reset" do
    defmodule ReplayedEvent do
      @derive Jason.Encoder
      defstruct [:reply_to]
    end

    defmodule BufferingHandler do
      use Decree.Event.Handler,
        application: Decree.MockedApp,
        name: __MODULE__,
        batch_size: 10,
        batch_timeout: 60_000,
        personal_fields: :opaque

      def handle_batch([{%{reply_to: reply_to}, _metadata} | _rest] = events) do
        send(reply_to, {:batch_handled, length(events)})

        :ok
      end
    end

    test "should not apply events buffered before the reset" do
      subscription = stub_stand_in_subscription()
      stub(MockEventStore, :delete_subscription, fn "$all", _name, _opts -> :ok end)
      stub(MockEventStore, :ack, fn _subscription, _event_number -> :ok end)

      handler = start_supervised!(BufferingHandler)
      send_subscribed(handler, subscription)

      events =
        Decree.Helpers.EventFactory.map_to_recorded_events([%ReplayedEvent{reply_to: self()}])

      # Buffered, not yet handled, when the handler is reset.
      send(handler, {:events, events})
      send(handler, :reset)

      # The reset subscription delivers the event again.
      send_subscribed(handler, subscription)
      send(handler, {:events, events})
      send(handler, :flush_batch_timeout)

      assert_receive {:batch_handled, 1}
      refute_receive {:batch_handled, _count}
    end
  end

  defp send_subscribed(handler, subscription \\ nil) do
    send(handler, {:subscribed, subscription || handler})
  end

  # A reset unsubscribes the handler's subscription process, so the subscription is a
  # stand-in here rather than the handler itself, which cannot call itself.
  defp stub_stand_in_subscription do
    subscription = Decree.MockEventStoreCase.start_stand_in_subscription!()

    stub(MockEventStore, :subscribe_to_all_streams, fn _handler_name, _handler, _opts ->
      {:ok, subscription}
    end)

    subscription
  end
end
