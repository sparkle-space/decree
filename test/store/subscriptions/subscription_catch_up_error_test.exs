defmodule Decree.Store.Subscriptions.SubscriptionCatchUpErrorTest do
  use Decree.Store.StorageCase

  # A catch-up read that returns `{:error, reason}` holds the subscription and retries
  # the read on a capped backoff timer, keeping its cursor and its subscribers, instead
  # of crashing the subscription (sparkle-space/sparklespace#7307). A `retry_interval`
  # below one second gives a flat wait of `retry_interval`, which the persistent,
  # transient and mid-stream tests use; the backoff tests use a cap above one second.
  #
  # The read error is a real one: a second connection holds an ACCESS EXCLUSIVE lock on
  # `stream_events`, and the subscription runs with a short query timeout, so its
  # catch-up read times out, Postgrex cancels the statement and the store's reader
  # returns `{:error, message}`. The `streams` and `subscriptions` tables stay
  # unlocked, so the subscription is created and the stream looked up; only the read
  # of the stream's events fails. Nothing is appended while the lock is held: an
  # append writes `stream_events` too.
  #
  # The telemetry handler runs in the subscription process as each held read is
  # emitted, and stamps it with the monotonic time, so the tests measure the real wait
  # between two held reads, not only the wait each one reports. The gap between two
  # held reads is the reported wait plus the duration of the next read (about the
  # query timeout).

  import Decree.Store.SubscriptionHelpers, only: [attempt_subscribe: 1, start_subscriber: 0]

  alias Decree.Store.{EventFactory, RecordedEvent, UUID, Wait}
  alias Decree.Store.Subscriptions.{Subscription, SubscriptionFsm, SubscriptionState}
  alias TestEventStore, as: EventStore

  @catch_up_error [:decree, :store, :subscription, :catch_up, :error]
  @query_timeout 100
  @retry_interval 200

  # A held read may come this much sooner than the wait reported by the one before it
  # (timer and clock granularity), or this much later (the read itself, plus load).
  @early_tolerance 50
  @late_tolerance 1_000

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

  test "a persistent catch-up read error is retried after each wait, without a DOWN", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    # A flat wait well above the read's own duration, so a retry that does not wait
    # shows up as a gap far below it.
    retry_interval = 500

    lock = lock_stream_events(postgrex_config)

    {subscription, ref} = subscribe(subscription_name, retry_interval: retry_interval)

    held_reads = for _attempt <- 1..4, do: assert_held_read(subscription, ref)

    for {metadata, attempt} <- Enum.with_index(held_reads, 1) do
      assert %{
               event_store: TestEventStore,
               subscription_name: ^subscription_name,
               stream_uuid: "$all",
               retry_interval: ^retry_interval,
               attempt: ^attempt,
               reason: reason
             } = metadata

      assert is_binary(reason)
    end

    assert_waited(held_reads)

    assert Process.alive?(subscription)
    refute_received {:DOWN, ^ref, :process, ^subscription, _reason}

    release(lock)
  end

  test "a transient catch-up read error is retried, then the event is delivered once", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    stream_uuid = UUID.uuid4()

    append_events_and_await_notification(stream_uuid, 1)

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

  test "a read held mid-stream keeps its cursor: each later event is delivered once", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    stream_uuid = UUID.uuid4()

    append_events_and_await_notification(stream_uuid, 3)

    # One event per read and one in flight per subscriber, so the cursor moves one
    # event at a time.
    {subscription, ref} = subscribe(subscription_name, max_size: 1, buffer_size: 1)

    assert_receive {:events, [%RecordedEvent{event_number: 1}]}

    # The next read fetched event 2, which waits in the queue behind the unacked
    # event 1, so the subscription reads no further until event 1 is acked.
    Wait.until(3_000, fn ->
      assert %SubscriptionFsm{
               state: :catching_up,
               data: %SubscriptionState{last_sent: 1, queue_size: 1}
             } = fsm(subscription)
    end)

    lock = lock_stream_events(postgrex_config)

    # The ack sends event 2, then reads from event 3, which fails: the read is held
    # with event 2 sent and unacked. The ack is a call, so the hold has happened once
    # it returns.
    :ok = Subscription.ack(subscription, 1)

    assert_receive {:events, [%RecordedEvent{event_number: 2}]}
    assert %{attempt: 1} = assert_held_read(subscription, ref)

    assert %SubscriptionFsm{
             state: :catch_up_retry,
             data: %SubscriptionState{last_sent: 2, last_ack: 1}
           } = fsm(subscription)

    release(lock)

    :ok = Subscription.ack(subscription, 2)

    assert_receive {:events, [%RecordedEvent{event_number: 3}]}, 5_000

    :ok = Subscription.ack(subscription, 3)

    # Neither event 1 nor event 2 is sent again, and event 3 only once.
    refute_receive {:events, _events}, 5 * @retry_interval

    assert Process.alive?(subscription)
    refute_received {:DOWN, ^ref, :process, ^subscription, _reason}
  end

  test "an event appended and a subscriber connected while held are each handled once", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    append_events_and_await_notification(UUID.uuid4(), 2)

    lock = lock_stream_events(postgrex_config)

    subscriber1 = start_subscriber()

    {:ok, subscription} =
      EventStore.subscribe_to_all_streams(subscription_name, subscriber1,
        concurrency_limit: 2,
        timeout: @query_timeout,
        retry_interval: 2_000
      )

    assert_receive {:subscribed, ^subscription, ^subscriber1}

    ref = Process.monitor(subscription)

    assert %{attempt: 1} = assert_held_read(subscription, ref)
    assert %{attempt: 2, retry_interval: 2_000} = assert_held_read(subscription, ref)

    # Release the lock but stay held: the next read is two seconds away, so the
    # append and the second subscriber both reach the subscription while it waits.
    release(lock)

    :ok = EventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(1))

    # The store's notification of event 3 is tracked, not read, while held.
    Wait.until(1_000, fn ->
      assert %SubscriptionFsm{
               state: :catch_up_retry,
               data: %SubscriptionState{last_received: 3, last_sent: 0}
             } = fsm(subscription)
    end)

    subscriber2 = start_subscriber()

    {:ok, ^subscription} =
      EventStore.subscribe_to_all_streams(subscription_name, subscriber2, concurrency_limit: 2)

    assert_receive {:subscribed, ^subscription, ^subscriber2}

    assert %SubscriptionFsm{
             state: :catch_up_retry,
             data: %SubscriptionState{subscribers: subscribers, last_sent: 0}
           } = fsm(subscription)

    assert subscribers |> Map.keys() |> Enum.sort() == Enum.sort([subscriber1, subscriber2])

    deliveries = receive_and_ack_all(subscription, [1, 2, 3])

    assert deliveries |> Enum.map(&elem(&1, 0)) |> Enum.sort() == [1, 2, 3]

    assert deliveries |> Enum.map(&elem(&1, 1)) |> Enum.uniq() |> Enum.sort() ==
             Enum.sort([subscriber1, subscriber2])

    refute_receive {:events, _events, _subscriber}, 5 * @retry_interval

    assert Process.alive?(subscription)
    refute_received {:DOWN, ^ref, :process, ^subscription, _reason}
  end

  test "the wait between held reads doubles from one second up to the retry interval", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    lock = lock_stream_events(postgrex_config)

    {subscription, ref} = subscribe(subscription_name, retry_interval: 4_000)

    # The 4th read comes about four seconds after the 3rd.
    held_reads = for _attempt <- 1..4, do: assert_held_read(subscription, ref, 10_000)

    assert Enum.map(held_reads, & &1.retry_interval) == [1_000, 2_000, 4_000, 4_000]
    assert Enum.map(held_reads, & &1.attempt) == [1, 2, 3, 4]

    assert_waited(held_reads)

    assert Process.alive?(subscription)

    release(lock)
  end

  test "a successful read resets the backoff", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    lock = lock_stream_events(postgrex_config)

    {subscription, ref} = subscribe(subscription_name, retry_interval: 2_000)

    assert %{attempt: 1, retry_interval: 1_000} = assert_held_read(subscription, ref)

    release(lock)

    # The retried read succeeds: the stream is empty, so the subscription is subscribed.
    Wait.until(3_000, fn ->
      assert %SubscriptionFsm{
               state: :subscribed,
               data: %SubscriptionState{catch_up_failures: 0}
             } = fsm(subscription)
    end)

    lock = lock_stream_events(postgrex_config)

    # Ask the subscribed subscription to catch up, with the store's own cast.
    GenServer.cast(subscription, :catch_up)

    assert %{attempt: 1, retry_interval: 1_000} = assert_held_read(subscription, ref)

    release(lock)
  end

  test "losing the advisory lock while held resets the backoff", %{
    postgrex_config: postgrex_config,
    subscription_name: subscription_name
  } do
    lock = lock_stream_events(postgrex_config)

    {subscription, ref} = subscribe(subscription_name, retry_interval: 4_000)

    assert %{attempt: 1} = assert_held_read(subscription, ref)
    assert %{attempt: 2, retry_interval: 2_000} = assert_held_read(subscription, ref)

    %SubscriptionFsm{data: %SubscriptionState{lock_ref: lock_ref}} = fsm(subscription)

    send(subscription, {Decree.Store.AdvisoryLocks, :lock_released, lock_ref, :shutdown})

    assert %SubscriptionFsm{
             state: :disconnected,
             data: %SubscriptionState{catch_up_failures: 0, catch_up_retry_token: nil}
           } = fsm(subscription)

    # Resubscribe now, rather than after the retry interval.
    attempt_subscribe(subscription)

    assert %{attempt: 1, retry_interval: 1_000} = assert_held_read(subscription, ref)

    release(lock)
  end

  def handle_telemetry_event(@catch_up_error, _measurements, metadata, config) do
    %{test: test, subscription_name: subscription_name} = config

    if metadata.subscription_name == subscription_name do
      held_at = System.monotonic_time(:millisecond)

      send(test, {:held_read, Map.put(metadata, :held_at, held_at)})
    end
  end

  defp subscribe(subscription_name, opts \\ []) do
    opts =
      Keyword.merge(
        [timeout: @query_timeout, retry_interval: @retry_interval],
        opts
      )

    {:ok, subscription} =
      EventStore.subscribe_to_all_streams(subscription_name, self(), opts)

    assert_receive {:subscribed, ^subscription}

    {subscription, Process.monitor(subscription)}
  end

  defp fsm(subscription) do
    %Subscription{subscription: %SubscriptionFsm{} = fsm} = :sys.get_state(subscription)

    fsm
  end

  defp assert_held_read(subscription, ref, timeout \\ 5_000) do
    receive do
      {:held_read, metadata} ->
        metadata

      {:DOWN, ^ref, :process, ^subscription, reason} ->
        flunk("subscription DOWN: " <> inspect(reason))
    after
      timeout -> flunk("no held catch-up read")
    end
  end

  # Each held read comes at least the wait the one before it reported after it, and
  # not much later.
  defp assert_waited(held_reads) do
    held_reads
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.each(fn [previous, next] ->
      %{retry_interval: wait, attempt: attempt, held_at: previous_at} = previous
      %{held_at: next_at} = next

      gap = next_at - previous_at

      assert gap >= wait - @early_tolerance,
             "held read #{attempt + 1} came #{gap}ms after read #{attempt}, " <>
               "which reported a #{wait}ms wait"

      assert gap <= wait + @late_tolerance,
             "held read #{attempt + 1} came #{gap}ms after read #{attempt}, " <>
               "which reported a #{wait}ms wait"
    end)
  end

  # Receive events sent to any subscriber, acking each batch for its subscriber, until
  # every expected event number has been received. Returns `{event_number,
  # subscriber}` for every event received, in order.
  defp receive_and_ack_all(subscription, expected, deliveries \\ []) do
    received = Enum.map(deliveries, &elem(&1, 0))

    if Enum.all?(expected, &(&1 in received)) do
      Enum.reverse(deliveries)
    else
      assert_receive {:events, events, subscriber}, 5_000

      %RecordedEvent{event_number: last} = List.last(events)
      :ok = Subscription.ack(subscription, last, subscriber)

      deliveries =
        Enum.reduce(events, deliveries, fn %RecordedEvent{event_number: n}, acc ->
          [{n, subscriber} | acc]
        end)

      receive_and_ack_all(subscription, expected, deliveries)
    end
  end

  # Append `count` events to a stream, and wait until the store's notification of them
  # has been read and published, so no notification read is left to block on the lock.
  defp append_events_and_await_notification(stream_uuid, count) do
    test = self()

    listener =
      spawn_link(fn ->
        :ok = EventStore.subscribe("$all")
        send(test, {:listening, self()})

        await_notified(test, count, 0)
      end)

    assert_receive {:listening, ^listener}

    :ok = EventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(count))

    assert_receive {:notified, ^listener}
  end

  defp await_notified(test, count, notified) when notified >= count,
    do: send(test, {:notified, self()})

  defp await_notified(test, count, notified) do
    receive do
      {:events, events} -> await_notified(test, count, notified + length(events))
    end
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
