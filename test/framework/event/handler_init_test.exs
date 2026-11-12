defmodule Decree.Event.HandlerInitTest do
  use Decree.MockEventStoreCase

  import Mox

  alias Decree.DefaultApp
  alias Decree.Event.{EchoHandler, Handler, InitHandler, ReplyEvent, RuntimeConfigHandler}
  alias Decree.EventStore.Mock, as: MockEventStore
  alias Decree.Helpers.{EventFactory, Wait}
  alias Decree.Registration
  alias Decree.Store.Subscriptions.Subscription, as: StoreSubscription

  # The bound of a wait for a replacement to settle: a restart that finds the old
  # subscription still running is refused and backs off for a second or more.
  @settle_timeout 15_000

  describe "event handler `init/1` callback" do
    setup do
      for tenant <- [:tenant1, :tenant2, :tenant3] do
        start_supervised!({DefaultApp, name: Module.concat([DefaultApp, tenant])})
      end

      :ok
    end

    test "should be called at runtime" do
      {:ok, handler1} = RuntimeConfigHandler.start_link(tenant: :tenant1, reply_to: self())
      {:ok, handler2} = RuntimeConfigHandler.start_link(tenant: :tenant2, reply_to: self())
      {:ok, handler3} = RuntimeConfigHandler.start_link(tenant: :tenant3, reply_to: self())

      assert_receive {:init, :tenant1}
      assert_receive {:init, :tenant2}
      assert_receive {:init, :tenant3}

      assert_handler_application(handler1, Module.concat([DefaultApp, :tenant1]))
      assert_handler_application(handler2, Module.concat([DefaultApp, :tenant2]))
      assert_handler_application(handler3, Module.concat([DefaultApp, :tenant3]))

      assert_handler_name(handler1, "Decree.Event.RuntimeConfigHandler.tenant1")
      assert_handler_name(handler2, "Decree.Event.RuntimeConfigHandler.tenant2")
      assert_handler_name(handler3, "Decree.Event.RuntimeConfigHandler.tenant3")
    end

    test "should be called on restart if the process crashes" do
      handler = start_supervised!({RuntimeConfigHandler, tenant: :tenant1, reply_to: self()})

      assert_receive {:init, :tenant1}

      subscription = Wait.until(@settle_timeout, fn -> assert_subscribed(handler) end)

      handler_ref = Process.monitor(handler)
      subscription_ref = Process.monitor(subscription)

      Process.exit(handler, :kill)

      assert_receive {:DOWN, ^handler_ref, :process, ^handler, :killed}
      assert_receive {:DOWN, ^subscription_ref, :process, ^subscription, _reason}

      assert_receive {:init, :tenant1}

      # The handler running now, on a subscription other than the one that stopped. A
      # restart racing that shutdown gets `:noproc` and inits once more, so the inits up
      # to here are not counted; from here nothing is left to restart.
      Wait.until(@settle_timeout, fn ->
        replacement = current_handler(:tenant1)

        assert is_pid(replacement) and replacement != handler
        assert assert_subscribed(replacement) != subscription
      end)

      flush_inits(:tenant1)

      refute_receive {:init, :tenant1}
    end
  end

  describe "event handler `init/0` callback" do
    setup do
      true = Process.register(self(), :test)

      expect(MockEventStore, :subscribe_to_all_streams, fn
        handler_name, handler, _opts ->
          assert is_binary(handler_name)

          {:ok, handler}
      end)

      handler = start_supervised!(InitHandler)

      [handler: handler]
    end

    test "should be called after subscription subscribed", %{handler: handler} do
      refute_receive {:init, ^handler}

      send_subscribed(handler)

      assert_receive {:init, ^handler}
    end
  end

  describe "event handler start options" do
    test "should be passed when starting handler via `start_link/1`" do
      {:ok, handler} = EchoHandler.start_link(hibernate_after: 1)

      Wait.until(fn -> assert_hibernated(handler) end)
    end

    test "should be passed when starting handler via child spec" do
      handler = start_supervised!({EchoHandler, hibernate_after: 1})

      Wait.until(fn -> assert_hibernated(handler) end)
    end

    test "hibernated handler should resume after receiving event message" do
      handler = start_supervised!({EchoHandler, hibernate_after: 1})

      # Handler should hibernate
      Wait.until(fn -> assert_hibernated(handler) end)

      reply_to = :erlang.pid_to_list(self())
      event = %ReplyEvent{reply_to: reply_to, value: 1}

      # Sending a message should wake the handler
      send_events(handler, [event])

      assert_receive {:event, ^handler, ^event, _metadata}

      # Handler should hibernate again
      Wait.until(fn -> assert_hibernated(handler) end)
    end
  end

  defp current_handler(tenant) do
    application = Module.concat([DefaultApp, tenant])
    handler_name = "Decree.Event.RuntimeConfigHandler.#{tenant}"

    Registration.whereis_name(application, Handler.name(application, handler_name))
  end

  # The subscription `handler` is subscribed to, seen from both ends: the handler had
  # `{:subscribed, subscription}` from it, and it has the handler as a subscriber. A
  # process that stops during the look fails the assertion, for `Wait.until/2` to retry.
  defp assert_subscribed(handler) do
    %Handler{subscription: %{subscription_pid: subscription}, started_subscription: started} =
      :sys.get_state(handler)

    assert is_pid(subscription) and started == subscription

    %StoreSubscription{subscription: %{data: %{subscribers: subscribers}}} =
      :sys.get_state(subscription)

    assert Map.has_key?(subscribers, handler)

    subscription
  catch
    :exit, reason -> flunk("stopped while read: " <> inspect(reason))
  end

  defp flush_inits(tenant) do
    receive do
      {:init, ^tenant} -> flush_inits(tenant)
    after
      0 -> :ok
    end
  end

  defp assert_handler_application(handler, expected_application) do
    %Handler{application: application} = :sys.get_state(handler)

    assert application == expected_application
  end

  defp assert_handler_name(handler, expected_handler_name) do
    %Handler{handler_name: handler_name} = :sys.get_state(handler)

    assert handler_name == expected_handler_name
  end

  defp assert_hibernated(pid) do
    pre_otp_28 = {:current_function, {:erlang, :hibernate, 3}}
    post_otp_28 = {:current_function, {:gen_server, :loop_hibernate, 4}}

    assert Process.info(pid, :current_function) in [pre_otp_28, post_otp_28]
  end

  defp send_subscribed(handler) do
    send(handler, {:subscribed, handler})
  end

  defp send_events(handler, events) do
    recorded_events = EventFactory.map_to_recorded_events(events)

    send(handler, {:events, recorded_events})
  end
end
