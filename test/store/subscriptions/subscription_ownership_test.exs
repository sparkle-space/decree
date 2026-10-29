defmodule Decree.Store.Subscriptions.SubscriptionOwnershipTest do
  use Decree.Store.StorageCase

  alias Decree.Store.UUID

  describe "local subscriptions" do
    setup do
      start_supervised!({TestEventStore, name: :other_instance, schema: "public"},
        id: :other_instance
      )

      :ok
    end

    test "should list each subscription with its status" do
      name = UUID.uuid4()

      {:ok, subscribed} = TestEventStore.subscribe_to_all_streams(name, self())
      assert_receive {:subscribed, ^subscribed}

      # The same subscription on another instance waits for the lock.
      {:ok, parked} = TestEventStore.subscribe_to_all_streams(name, self(), name: :other_instance)

      {:ok, transient} =
        TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self(), transient: true)

      assert_receive {:subscribed, ^transient}

      assert [
               %{
                 subscription_name: ^name,
                 stream_uuid: "$all",
                 subscription_id: subscription_id,
                 pid: ^subscribed,
                 transient: false,
                 status: :subscribed
               },
               %{pid: ^transient, transient: true, status: :subscribed, subscription_id: nil}
             ] = Enum.sort_by(TestEventStore.local_subscriptions(), & &1.transient)

      assert is_integer(subscription_id)

      # It has not taken the lock yet, so it does not report the row's id.
      assert [
               %{
                 subscription_name: ^name,
                 subscription_id: nil,
                 pid: ^parked,
                 transient: false,
                 status: :parked
               }
             ] = TestEventStore.local_subscriptions(name: :other_instance)
    end

    test "should leave out a subscription that does not answer in time" do
      {:ok, slow} = TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self())
      {:ok, answering} = TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self())
      assert_receive {:subscribed, ^slow}
      assert_receive {:subscribed, ^answering}

      :ok = :sys.suspend(slow)

      try do
        assert [%{pid: ^answering}] = TestEventStore.local_subscriptions(timeout: 100)
      after
        :sys.resume(slow)
      end
    end
  end

  describe "subscription locks" do
    setup do
      start_supervised!({TestEventStore, name: :other_instance, schema: "public"},
        id: :other_instance
      )

      :ok
    end

    test "should answer on the instance's own lock session" do
      name = UUID.uuid4()

      {:ok, subscription} = TestEventStore.subscribe_to_all_streams(name, self())
      assert_receive {:subscribed, ^subscription}

      {:ok, _parked} =
        TestEventStore.subscribe_to_all_streams(name, self(), name: :other_instance)

      [%{subscription_id: subscription_id}] = TestEventStore.local_subscriptions()
      missing = subscription_id + 1_000

      assert {:ok,
              %{
                ^subscription_id => %{row_exists: true, lock_held: true},
                ^missing => %{row_exists: false, lock_held: false}
              }} = TestEventStore.subscription_locks([subscription_id, missing])

      assert {:ok, %{^subscription_id => %{row_exists: true, lock_held: false}}} =
               TestEventStore.subscription_locks([subscription_id], name: :other_instance)

      assert {:ok, %{}} = TestEventStore.subscription_locks([])
    end
  end

  describe "stop subscription" do
    test "should stop a running subscription and release its lock" do
      name = UUID.uuid4()

      {:ok, subscription} = TestEventStore.subscribe_to_all_streams(name, self())
      assert_receive {:subscribed, ^subscription}

      [%{subscription_id: subscription_id}] = TestEventStore.local_subscriptions()
      ref = Process.monitor(subscription)

      assert :ok = TestEventStore.stop_subscription(subscription)
      assert_receive {:DOWN, ^ref, :process, ^subscription, :shutdown}

      assert TestEventStore.local_subscriptions() == []
      assert {:error, :not_found} = TestEventStore.stop_subscription(subscription)

      # The subscription's lock is released once AdvisoryLocks sees it stop.
      Decree.Store.Wait.until(fn ->
        assert {:ok, %{^subscription_id => %{lock_held: false}}} =
                 TestEventStore.subscription_locks([subscription_id])
      end)
    end
  end
end
