defmodule Decree.Store.Subscriptions.SubscriptionCheckpointRowTest do
  use Decree.Store.StorageCase

  import ExUnit.CaptureLog

  alias Decree.Store.{EventFactory, Storage, UUID}
  alias Decree.Store.Subscriptions.Subscription

  describe "checkpoint after the subscription's row was replaced" do
    test "an ack does not write into the new row", %{conn: conn} = context do
      subscription_name = UUID.uuid4()
      subscription = start_subscription(context, subscription_name)

      append_events(3)
      assert_receive {:events, events}
      assert length(events) == 3

      old_id = subscription_id(subscription)
      new_id = replace_row(context, subscription_name)
      refute new_id == old_id
      new_last_seen = last_seen(conn, new_id)

      log =
        capture_log(fn ->
          :ok = Subscription.ack(subscription, events)

          # The checkpoint is written as part of the ack; wait for it to finish.
          _state = :sys.get_state(subscription)
        end)

      assert last_seen(conn, new_id) == new_last_seen
      assert log =~ "subscription_id #{old_id}"
      assert log =~ "its subscription row no longer exists"
      assert Process.alive?(subscription)
    end

    test "the final checkpoint on stop does not write into the new row",
         %{conn: conn} = context do
      subscription_name = UUID.uuid4()

      # Acks are checkpointed only on stop.
      subscription = start_subscription(context, subscription_name, checkpoint_threshold: 100)

      append_events(3)
      assert_receive {:events, events}
      :ok = Subscription.ack(subscription, events)

      new_id = replace_row(context, subscription_name)
      new_last_seen = last_seen(conn, new_id)

      log =
        capture_log(fn ->
          ref = Process.monitor(subscription)
          Process.unlink(subscription)
          :ok = GenServer.stop(subscription, :shutdown)
          assert_receive {:DOWN, ^ref, :process, ^subscription, :shutdown}
        end)

      assert last_seen(conn, new_id) == new_last_seen
      assert log =~ "its subscription row no longer exists"
    end
  end

  describe "checkpoint to the subscription's own row" do
    test "an ack writes the position", %{conn: conn} = context do
      subscription = start_subscription(context, UUID.uuid4())

      append_events(3)
      assert_receive {:events, events}
      :ok = Subscription.ack(subscription, events)

      assert last_seen(conn, subscription_id(subscription)) == 3
    end
  end

  describe "checkpoint by id" do
    test "never moves a row's position back", %{conn: conn, schema: schema} do
      {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
        Storage.Subscription.subscribe_to_stream(conn, "$all", UUID.uuid4(), 0, schema: schema)

      :ok =
        Storage.Subscription.ack_last_seen_event_by_id(conn, subscription_id, 5, schema: schema)

      assert last_seen(conn, subscription_id) == 5

      # A lower position, such as a stale holder's final checkpoint, is ignored.
      :ok =
        Storage.Subscription.ack_last_seen_event_by_id(conn, subscription_id, 3, schema: schema)

      assert last_seen(conn, subscription_id) == 5

      :ok =
        Storage.Subscription.ack_last_seen_event_by_id(conn, subscription_id, 7, schema: schema)

      assert last_seen(conn, subscription_id) == 7
    end
  end

  defp start_subscription(context, subscription_name, opts \\ []) do
    %{
      conn: conn,
      schema: schema,
      event_store: event_store,
      serializer: serializer
    } = context

    {:ok, subscription} =
      Subscription.start_link(
        [
          event_store: event_store,
          conn: conn,
          schema: schema,
          serializer: serializer,
          retry_interval: 1_000,
          stream_uuid: "$all",
          subscription_name: subscription_name,
          buffer_size: 3,
          start_from: 0
        ] ++ opts
      )

    {:ok, ^subscription} = Subscription.connect(subscription, self(), buffer_size: 3)

    assert_receive {:subscribed, ^subscription}

    subscription
  end

  defp append_events(count) do
    :ok = TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(count))
  end

  defp subscription_id(subscription) do
    %Subscription{subscription: %{data: %{subscription_id: subscription_id}}} =
      :sys.get_state(subscription)

    subscription_id
  end

  # Delete the subscription's row and create it again under the same name, as an
  # operator reset or another instance would, on a connection of its own.
  defp replace_row(%{postgrex_config: postgrex_config, schema: schema}, subscription_name) do
    conn = start_supervised!({Postgrex, postgrex_config}, id: :replacing_conn)

    :ok =
      Storage.Subscription.delete_subscription(conn, "$all", subscription_name, schema: schema)

    {:ok, %Storage.Subscription{subscription_id: new_id}} =
      Storage.Subscription.subscribe_to_stream(conn, "$all", subscription_name, 0, schema: schema)

    new_id
  end

  defp last_seen(conn, subscription_id) do
    %Postgrex.Result{rows: [[last_seen]]} =
      Postgrex.query!(conn, "SELECT last_seen FROM subscriptions WHERE subscription_id = $1", [
        subscription_id
      ])

    last_seen
  end
end
