defmodule Decree.Store.Subscriptions.SubscriptionOwnershipTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{UUID, Wait}

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

      assert {infos, 0} = TestEventStore.local_subscriptions()

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
             ] = Enum.sort_by(infos, & &1.transient)

      assert is_integer(subscription_id)

      # It has not taken the lock yet, so it does not report the row's id.
      assert {[
                %{
                  subscription_name: ^name,
                  subscription_id: nil,
                  pid: ^parked,
                  transient: false,
                  status: :parked
                }
              ], 0} = TestEventStore.local_subscriptions(name: :other_instance)
    end

    test "should leave out and count a subscription that does not answer in time" do
      {:ok, slow} = TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self())
      {:ok, answering} = TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self())
      assert_receive {:subscribed, ^slow}
      assert_receive {:subscribed, ^answering}

      :ok = :sys.suspend(slow)

      try do
        assert {[%{pid: ^answering}], 1} = TestEventStore.local_subscriptions(timeout: 500)
      after
        :sys.resume(slow)
      end
    end

    test "should count only the reads that timed out, not a subscription that stopped" do
      {:ok, slow} = TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self())
      {:ok, stopping} = TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self())
      {:ok, answering} = TestEventStore.subscribe_to_all_streams(UUID.uuid4(), self())
      assert_receive {:subscribed, ^slow}
      assert_receive {:subscribed, ^stopping}
      assert_receive {:subscribed, ^answering}

      :ok = :sys.suspend(slow)
      :ok = :sys.suspend(stopping)

      try do
        census = Task.async(fn -> TestEventStore.local_subscriptions(timeout: 1_000) end)

        # The census's read is waiting in the stopping subscription's mailbox: stop
        # the subscription under it, so the read exits instead of timing out.
        Wait.until(fn ->
          {:messages, messages} = Process.info(stopping, :messages)
          assert Enum.any?(messages, &match?({:"$gen_call", _from, :info}, &1))
        end)

        ref = Process.monitor(stopping)
        Process.exit(stopping, :kill)
        assert_receive {:DOWN, ^ref, :process, ^stopping, :killed}

        assert {[%{pid: ^answering}], 1} = Task.await(census, 5_000)
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

      {[%{subscription_id: subscription_id}], 0} = TestEventStore.local_subscriptions()
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

      {[%{subscription_id: subscription_id}], 0} = TestEventStore.local_subscriptions()
      ref = Process.monitor(subscription)

      assert :ok = TestEventStore.stop_subscription(subscription)
      assert_receive {:DOWN, ^ref, :process, ^subscription, :shutdown}

      assert TestEventStore.local_subscriptions() == {[], 0}
      assert {:error, :not_found} = TestEventStore.stop_subscription(subscription)

      # The subscription's lock is released once AdvisoryLocks sees it stop.
      Decree.Store.Wait.until(fn ->
        assert {:ok, %{^subscription_id => %{lock_held: false}}} =
                 TestEventStore.subscription_locks([subscription_id])
      end)
    end
  end
end
