defmodule Decree.Store.Subscriptions.SubscriptionStoppingConnectTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{UUID, Wait}
  alias TestEventStore, as: EventStore

  @settle_timeout 15_000

  # A subscription stops with the exit reason of its last subscriber to go down, and stays
  # registered until it has. A subscriber whose `connect` was queued behind that `:DOWN`
  # reached the stopping subscription, and its call exited with the other subscriber's
  # reason. sparkle-space/sparklespace#8683.
  test "a connect queued behind the last subscriber's exit gets a new subscription" do
    name = UUID.uuid4()
    old_subscriber = spawn(fn -> receive(do: (:stop -> exit(:old_subscriber_reason))) end)

    {:ok, subscription} =
      EventStore.subscribe_to_all_streams(name, old_subscriber, concurrency_limit: 1)

    subscription_ref = Process.monitor(subscription)
    old_subscriber_ref = Process.monitor(old_subscriber)

    :ok = :sys.suspend(subscription)

    send(old_subscriber, :stop)
    assert_receive {:DOWN, ^old_subscriber_ref, :process, _pid, :old_subscriber_reason}

    Wait.until(@settle_timeout, fn ->
      assert queued?(subscription, &match?({:DOWN, _, :process, ^old_subscriber, _}, &1))
    end)

    new_subscriber = start_subscriber(name)

    Wait.until(@settle_timeout, fn ->
      assert queued?(subscription, &match?({:"$gen_call", _, {:connect, _, _}}, &1))
    end)

    :ok = :sys.resume(subscription)

    assert_receive {:DOWN, ^subscription_ref, :process, ^subscription, :old_subscriber_reason}
    assert_receive {:connected, ^new_subscriber, result}
    assert {:ok, new_subscription} = result
    assert new_subscription != subscription
    assert_receive {:subscribed, ^new_subscriber, ^new_subscription}
  end

  # Subscribes from a process of its own and reports the answer, and the `:subscribed`
  # that follows, to the test. An exit from the call is reported as `{:exit, reason}`.
  defp start_subscriber(name) do
    reply_to = self()

    spawn_link(fn ->
      result =
        try do
          EventStore.subscribe_to_all_streams(name, self(), concurrency_limit: 1)
        catch
          :exit, reason -> {:exit, reason}
        end

      send(reply_to, {:connected, self(), result})

      receive do
        {:subscribed, subscription} -> send(reply_to, {:subscribed, self(), subscription})
      end

      receive(do: (:never -> :ok))
    end)
  end

  defp queued?(pid, fun) do
    {:messages, messages} = Process.info(pid, :messages)

    Enum.any?(messages, fun)
  end
end
