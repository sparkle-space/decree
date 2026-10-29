defmodule Decree.Store.Subscriptions.SubscriptionCatchUpErrorTest do
  use Decree.Store.StorageCase

  # A catch-up read that returns `{:error, reason}` holds the subscription and retries
  # the read on a timer, keeping its cursor and its subscribers, instead of crashing the
  # subscription (sparkle-space/sparklespace#7307).
  #
  # The read error is a real one: a second connection holds an ACCESS EXCLUSIVE lock on
  # `stream_events`, and the subscription runs with a short query timeout, so its
  # catch-up read times out, Postgrex cancels the statement and the store's reader
  # returns `{:error, message}`. The `streams` and `subscriptions` tables stay
  # unlocked, so the subscription is created and the stream looked up; only the read
  # of the stream's events fails. Nothing is appended while the lock is held: an
  # append writes `stream_events` too.

  alias Decree.Store.{EventFactory, RecordedEvent, UUID}
  alias Decree.Store.Subscriptions.Subscription
  alias TestEventStore, as: EventStore

  @catch_up_error [:decree, :store, :subscription, :catch_up, :error]
  @query_timeout 100
  @retry_interval 200

  setup do
    subscription_name = UUID.uuid4()
    handler_id = {__MODULE__, subscription_name}

    :ok =
      :telemetry.attach(
        handler_id,
        @catch_up_error,
        &__MODULE__.handle_telemetry_event/4,
        %{test: self(), subscription_name: subscription_name}
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    [subscription_name: subscription_name]
  end

  test "a persistent catch-up read error is retried at a bounded rate, without a DOWN", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    lock = lock_stream_events(postgrex_config)

    {subscription, ref} = subscribe(subscription_name)

    window = 10 * @retry_interval
    deadline = System.monotonic_time(:millisecond) + window
    held_reads = collect_held_reads(subscription, ref, deadline, [])

    # One read when the subscription catches up, then at most one per retry interval.
    assert length(held_reads) >= 2
    assert length(held_reads) <= div(window, @retry_interval) + 1

    for metadata <- held_reads do
      assert %{
               event_store: TestEventStore,
               subscription_name: ^subscription_name,
               stream_uuid: "$all",
               retry_interval: @retry_interval,
               reason: reason
             } = metadata

      assert is_binary(reason)
    end

    assert Process.alive?(subscription)
    refute_received {:DOWN, ^ref, :process, ^subscription, _reason}

    release(lock)
  end

  test "a transient catch-up read error is retried, then the event is delivered once", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    stream_uuid = UUID.uuid4()

    append_event_and_await_notification(stream_uuid)

    lock = lock_stream_events(postgrex_config)

    {subscription, ref} = subscribe(subscription_name)

    assert_held_read(subscription, ref)
    assert_held_read(subscription, ref)

    release(lock)

    assert_receive {:events, [%RecordedEvent{event_number: 1, stream_uuid: ^stream_uuid}]},
                   5_000

    :ok = Subscription.ack(subscription, 1)

    refute_receive {:events, _events}, 5 * @retry_interval

    assert Process.alive?(subscription)
    refute_received {:DOWN, ^ref, :process, ^subscription, _reason}
  end

  def handle_telemetry_event(@catch_up_error, _measurements, metadata, config) do
    %{test: test, subscription_name: subscription_name} = config

    if metadata.subscription_name == subscription_name do
      send(test, {:held_read, metadata})
    end
  end

  defp subscribe(subscription_name) do
    {:ok, subscription} =
      EventStore.subscribe_to_all_streams(subscription_name, self(),
        timeout: @query_timeout,
        retry_interval: @retry_interval
      )

    assert_receive {:subscribed, ^subscription}

    {subscription, Process.monitor(subscription)}
  end

  defp collect_held_reads(subscription, ref, deadline, held_reads) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:held_read, metadata} ->
        collect_held_reads(subscription, ref, deadline, [metadata | held_reads])

      {:DOWN, ^ref, :process, ^subscription, reason} ->
        flunk("subscription DOWN: " <> inspect(reason))
    after
      timeout -> Enum.reverse(held_reads)
    end
  end

  defp assert_held_read(subscription, ref) do
    receive do
      {:held_read, metadata} ->
        metadata

      {:DOWN, ^ref, :process, ^subscription, reason} ->
        flunk("subscription DOWN: " <> inspect(reason))
    after
      5_000 -> flunk("no held catch-up read")
    end
  end

  # Append one event, and wait until the store's notification of it has been read
  # and published, so no notification read is left to block on the lock.
  defp append_event_and_await_notification(stream_uuid) do
    test = self()

    listener =
      spawn_link(fn ->
        :ok = EventStore.subscribe("$all")
        send(test, {:listening, self()})

        receive do
          {:events, events} -> send(test, {:notified, self(), events})
        end
      end)

    assert_receive {:listening, ^listener}

    :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

    assert_receive {:notified, ^listener, [%RecordedEvent{event_number: 1}]}
  end

  # Hold an ACCESS EXCLUSIVE lock on `stream_events` from a second connection until
  # `release/1`. The holder is linked to the test, so the lock is also released when
  # the test exits.
  defp lock_stream_events(postgrex_config) do
    test = self()

    holder =
      spawn_link(fn ->
        {:ok, conn} = Postgrex.start_link(postgrex_config)

        Postgrex.transaction(
          conn,
          fn transaction ->
            Postgrex.query!(
              transaction,
              ~s(LOCK TABLE "public".stream_events IN ACCESS EXCLUSIVE MODE),
              []
            )

            send(test, {:locked, self()})

            receive do
              :release -> :ok
            end
          end,
          timeout: :infinity
        )

        send(test, {:released, self()})
      end)

    assert_receive {:locked, ^holder}

    holder
  end

  defp release(holder) do
    send(holder, :release)

    assert_receive {:released, ^holder}
  end
end
