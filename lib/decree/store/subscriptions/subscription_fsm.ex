defmodule Decree.Store.Subscriptions.SubscriptionFsm do
  @moduledoc false

  alias Decree.Store.{AdvisoryLocks, PubSub, RecordedEvent, Storage}
  alias Decree.Store.Streams.Stream
  alias Decree.Store.Subscriptions.{SubscriptionState, Subscriber}

  use Decree.Store.Subscriptions.GeneratedFsm,
    initial_state: :initial,
    initial_data: %SubscriptionState{}

  require Logger

  def new(stream_uuid, subscription_name, opts) do
    new(
      data: %SubscriptionState{
        conn: Keyword.fetch!(opts, :conn),
        event_store: Keyword.fetch!(opts, :event_store),
        stream_uuid: stream_uuid,
        subscription_name: subscription_name,
        serializer: Keyword.fetch!(opts, :serializer),
        schema: Keyword.fetch!(opts, :schema),
        correlation_id_type: Keyword.get(opts, :correlation_id_type, "uuid"),
        causation_id_type: Keyword.get(opts, :causation_id_type, "uuid"),
        start_from: opts[:start_from] || 0,
        mapper: opts[:mapper],
        selector: opts[:selector],
        partition_by: opts[:partition_by],
        buffer_size: opts[:buffer_size] || 1,
        checkpoint_after: opts[:checkpoint_after] || 0,
        checkpoint_threshold: opts[:checkpoint_threshold] || 1,
        query_timeout: opts[:query_timeout] || 15_000,
        # The full wait before a resubscribe, and the cap of the backoff after a failed
        # subscribe attempt or catch-up read. A started subscription always passes the
        # event store's `subscription_retry_interval` (or the caller's
        # `retry_interval`); the default is that setting's default, for an FSM built
        # without one.
        retry_interval: Keyword.get(opts, :retry_interval, 60_000),
        max_size: opts[:max_size] || 1_000,
        transient: Keyword.get(opts, :transient, false)
      }
    )
  end

  # The main flow between states in this finite state machine is:
  #
  #   initial -> request_catch_up -> catching_up -> subscribed
  #
  # A catch-up read that returns `{:error, reason}` holds the subscription in
  # `catch_up_retry` until a retry timer fires, then reads again:
  #
  #   request_catch_up | catching_up -> catch_up_retry -> (read again)
  #
  # A catch-up read that meets an event of a type this instance does not know records it
  # as `hold_pending` and reads nothing at or after it. Once the events before it are
  # drained (acked or filtered, none queued or in flight), the subscription holds before
  # it in `held`: it has written its checkpoint and released its lock, and probes on a
  # backoff timer, without the lock, until another instance has delivered the event or
  # this one knows its type (sparkle-space/sparklespace#7804):
  #
  #   (any delivering state) -> held -> disconnected | (subscribe as disconnected does)
  #

  defstate initial do
    defevent subscribe, data: %SubscriptionState{transient: true} = data do
      data = SubscriptionState.reset_event_tracking(data)

      with :ok <- subscribe_to_events(data) do
        last_seen = data.start_from

        data = %SubscriptionState{
          data
          | last_received: last_seen,
            last_sent: last_seen,
            last_ack: last_seen,
            subscribe_failures: 0
        }

        notify_subscribed(data)

        next_state(:request_catch_up, data)
      else
        failure -> next_state(:initial, subscribe_failed(data, failure))
      end
    end

    defevent subscribe,
      data: %SubscriptionState{} = data do
      data = SubscriptionState.reset_event_tracking(data)

      with {:ok, subscription} <- create_subscription(data),
           {:ok, lock_ref} <- try_acquire_exclusive_lock(data, subscription),
           :ok <- subscribe_to_events(data) do
        %Storage.Subscription{subscription_id: subscription_id, last_seen: last_seen} =
          subscription

        last_seen = last_seen || 0

        data = %SubscriptionState{
          data
          | subscription_id: subscription_id,
            lock_ref: lock_ref,
            last_received: last_seen,
            last_sent: last_seen,
            last_ack: last_seen,
            subscribe_failures: 0
        }

        notify_subscribed(data)

        next_state(:request_catch_up, data)
      else
        failure -> next_state(:initial, subscribe_failed(data, failure))
      end
    end
  end

  defstate request_catch_up do
    defevent catch_up, data: %SubscriptionState{} = data do
      catch_up_from_stream(data)
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        catch_up_from_stream(data)
      else
        reply -> respond(reply)
      end
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:request_catch_up, persist_checkpoint(data))
    end
  end

  defstate catching_up do
    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        catch_up_from_stream(data)
      else
        reply -> respond(reply)
      end
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:catching_up, persist_checkpoint(data))
    end
  end

  defstate subscribed do
    # Notify events when subscribed. While a hold is pending nothing at or after the held
    # event is enqueued: the catch-all only tracks `last_received`.
    defevent notify_events(events), data: %SubscriptionState{hold_pending: nil} = data do
      %SubscriptionState{last_received: last_received} = data

      expected_event = last_received + 1

      case first_event_number(events) do
        past when past < expected_event ->
          Logger.debug(describe(data) <> " received past event(s), ignoring")

          # Ignore already seen events
          next_state(:subscribed, data)

        future when future > expected_event ->
          Logger.debug(describe(data) <> " received unexpected event(s), requesting catch up")

          # Missed event(s), request catch-up with any unseen events from storage
          next_state(:request_catch_up, data)

        ^expected_event ->
          Logger.debug(describe(data) <> " is enqueueing #{length(events)} event(s)")

          # Subscriber is up-to-date, so enqueue events to send
          data =
            data
            |> enqueue_events(events)
            |> notify_subscribers()

          if over_capacity?(data) do
            # Too many pending events, must wait for these to be processed.
            next_state(:max_capacity, data)
          else
            # Remain subscribed, waiting for subscriber to ack already sent events.
            next_state(:subscribed, data)
          end
      end
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        next_state(:subscribed, data)
      else
        reply -> respond(reply)
      end
    end

    defevent catch_up, data: %SubscriptionState{} = data do
      next_state(:request_catch_up, data)
    end

    # The publisher stopped a batch before `event_number`, an event of a type this
    # instance does not know, and broadcast only the events before it. Read on from
    # storage, as for a gap in the events received: the catch-up read meets the event and
    # the subscription holds before it.
    defevent notify_pending(event_number), data: %SubscriptionState{hold_pending: nil} = data do
      %SubscriptionState{last_received: last_received} = data

      if event_number > last_received do
        next_state(:request_catch_up, track_last_received(data, event_number))
      else
        next_state(:subscribed, data)
      end
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:subscribed, persist_checkpoint(data))
    end
  end

  defstate max_capacity do
    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        if empty_queue?(data) do
          # No further pending events so catch up with any unseen.
          next_state(:request_catch_up, data)
        else
          # Pending events remain, wait until subscriber ack's.
          next_state(:max_capacity, data)
        end
      else
        reply -> respond(reply)
      end
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:max_capacity, persist_checkpoint(data))
    end
  end

  # Holding after a catch-up read returned `{:error, reason}`, until the backoff timer
  # armed by `hold_catch_up/2` fires (sparkle-space/sparklespace#7307).
  #
  # The read changed nothing, so the cursor (`last_sent`, `last_received`, `last_ack`),
  # the subscribers and their in-flight events are as they were. Retrying reads from
  # `last_sent + 1` again: events already sent are not read twice, and none is skipped.
  #
  # Only the timer's `retry_catch_up` event reads. Every other event either keeps the
  # state or leaves it through the catch-all handlers:
  #
  #   - `ack`: records the ack, sends any queued events and checkpoints, like the other
  #     states, but does not read (in `catching_up` an ack reads, which would tie the
  #     read rate to the ack rate);
  #   - `checkpoint`: persists the checkpoint;
  #   - `notify_events` (catch-all): tracks `last_received` only; the retried read
  #     fetches those events from storage;
  #   - `catch_up` (catch-all): ignored, so a `:catch_up` cast queued before the read
  #     failed does not read early;
  #   - `connect_subscriber`, `subscribe`, `unsubscribe`, `terminate` (catch-all): as
  #     in the other states; an `unsubscribe` of the last subscriber still persists
  #     the checkpoint and unsubscribes;
  #   - `disconnect` (catch-all): the advisory lock was lost, so go to `disconnected`;
  #     the pending timer's token is cleared, so its event is ignored, and the count
  #     of failed reads is reset, so the backoff starts again at its shortest wait.
  defstate catch_up_retry do
    defevent retry_catch_up(token),
      data: %SubscriptionState{catch_up_retry_token: token} = data do
      catch_up_from_stream(%SubscriptionState{data | catch_up_retry_token: nil})
    end

    defevent ack(ack, subscriber), data: %SubscriptionState{} = data do
      with {:ok, data} <- ack_events(data, ack, subscriber) do
        next_state(:catch_up_retry, data)
      else
        reply -> respond(reply)
      end
    end

    defevent checkpoint(), data: %SubscriptionState{} = data do
      next_state(:catch_up_retry, persist_checkpoint(data))
    end
  end

  # Lost the advisory lock: parked until the lock is taken again. The first attempt
  # comes after the full retry interval, as does each one that finds the lock taken
  # (`{:error, :lock_already_taken}`); after any other failed attempt the wait is the
  # backoff of `resubscribe_wait/1`. Subscribers were sent `{:subscription_lock_lost,
  # subscription}` on the way in, and are sent `{:subscribed, subscription}` again on
  # the way out.
  defstate disconnected do
    # Attempt to subscribe
    defevent subscribe, data: %SubscriptionState{} = data do
      resubscribe(data)
    end
  end

  # Holding before an event of a type this instance does not know (#7804). The events
  # before it were delivered and checkpointed, every subscriber was sent
  # `{:subscription_lock_lost, subscription}`, and the advisory lock was released, so an
  # instance that knows the type can take the lock and deliver the event. Nothing is
  # delivered here and no lock is held; `Subscription.status/1` reports `:parked`.
  #
  # `Subscription` arms a timer with the wait drawn into `held`, and its `probe` reads, on
  # the main pool and without the lock:
  #
  #   - the subscription's row: at or past the held event, or gone, means another instance
  #     delivered it, so go to `disconnected`, a silent standby on the full interval;
  #   - else the held event's type: known now, so subscribe as `disconnected` does (row,
  #     lock, catch-up); still unknown, or the probe failed, so stay held, alarm again and
  #     wait longer.
  #
  # Every other event is handled by the catch-alls: events and notices track
  # `last_received` only, an ack or a checkpoint is ignored (nothing is in flight), and a
  # late `:lock_released` for the released lock does not match `lock_ref` (now `nil`).
  defstate held do
    defevent probe, data: %SubscriptionState{held: %{event_number: event_number}} = data do
      case probe_held(data, event_number) do
        :delivered ->
          next_state(:disconnected, %SubscriptionState{data | held: nil, subscribe_failures: 0})

        :known ->
          resubscribe(%SubscriptionState{data | held: nil})

        :unknown ->
          hold_again(data, nil)

        {:error, reason} ->
          hold_again(data, reason)
      end
    end
  end

  defstate unsubscribed do
    defevent unsubscribe(_subscriber), data: %SubscriptionState{} = data do
      next_state(:unsubscribed, data)
    end
  end

  # Catch-all event handlers

  defevent ack(_ack, _subscriber), data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defevent checkpoint(), data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defevent connect_subscriber(subscriber, opts),
    data: %SubscriptionState{} = data,
    state: state do
    data =
      data
      |> monitor_subscriber(subscriber, opts)
      |> notify_subscribers()

    # A subscriber connecting while the subscription is parked without its lock (or held,
    # having released it) is told it is subscribed once the lock is taken.
    unless state in [:initial, :disconnected, :held] do
      notify_subscribed(subscriber)
    end

    next_state(state, data)
  end

  defevent subscribe, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # Ignore notify events unless subscribed
  defevent notify_events(events), data: %SubscriptionState{} = data, state: state do
    next_state(state, track_last_received(data, events))
  end

  # A position notice outside `subscribed`: track `last_received` only, as for events.
  defevent notify_pending(event_number), data: %SubscriptionState{} = data, state: state do
    next_state(state, track_last_received(data, event_number))
  end

  defevent catch_up, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # Checked after every event (`Subscription` sends it): once a hold is pending and the
  # events before the held one are drained, none queued and none in flight (acked, or
  # filtered by the selector), hold before it. A subscriber that never acks an event before
  # it keeps the subscription where it is, as it would without the hold.
  defevent hold,
    data:
      %SubscriptionState{
        hold_pending: {_event_number, _event_type},
        queue_size: 0,
        in_flight_event_numbers: []
      } = data,
    state: state,
    when: state in [:request_catch_up, :catching_up, :subscribed, :max_capacity, :catch_up_retry] do
    hold_subscription(data)
  end

  defevent hold, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # A probe timer that fired after the subscription left `held`.
  defevent probe, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # A retry timer from an earlier hold, whose token is no longer the pending one.
  defevent retry_catch_up(_token), data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # Losing the advisory lock also ends any run of failed catch-up reads: the pending
  # retry timer's token is cleared, so its event is ignored, and the backoff starts
  # again from its shortest wait once the subscription has resubscribed.
  #
  # Every subscriber is told (`{:subscription_lock_lost, subscription}`), as another
  # instance may take the lock and deliver the same events from now on.
  defevent disconnect(lock_ref), data: %SubscriptionState{lock_ref: lock_ref} = data do
    notify_lock_lost(data)

    data =
      %SubscriptionState{data | lock_ref: nil, catch_up_retry_token: nil, catch_up_failures: 0}
      |> SubscriptionState.reset_event_tracking()
      |> purge_in_flight_events()

    next_state(:disconnected, data)
  end

  # A lock released that is not the subscription's current lock: one taken by a
  # lock attempt whose call timed out here, and was served afterwards.
  defevent disconnect(_lock_ref), data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defevent unsubscribe(pid), data: %SubscriptionState{} = data, state: state do
    data =
      data
      |> remove_subscriber(pid)
      |> notify_subscribers()

    case has_subscribers?(data) do
      true ->
        next_state(state, data)

      false ->
        data = persist_checkpoint(data)

        next_state(:unsubscribed, data)
    end
  end

  defevent terminate, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  defp create_subscription(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      query_timeout: query_timeout,
      schema: schema,
      start_from: start_from,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name
    } = data

    Storage.Subscription.subscribe_to_stream(
      conn,
      stream_uuid,
      subscription_name,
      start_from,
      schema: schema,
      timeout: query_timeout
    )
  end

  defp try_acquire_exclusive_lock(
         %SubscriptionState{} = data,
         %Storage.Subscription{} = subscription
       ) do
    %Storage.Subscription{subscription_id: subscription_id} = subscription
    %SubscriptionState{event_store: event_store, query_timeout: timeout} = data

    server = Module.concat(event_store, AdvisoryLocks)

    # A lock attempt that does not answer in time (or an `AdvisoryLocks` that is
    # not running) is a failed attempt, handled by `subscribe_failed/2`, rather than
    # a crash.
    try do
      AdvisoryLocks.try_advisory_lock(server, subscription_id, timeout)
    catch
      :exit, reason -> {:error, reason}
    end
  end

  # A failed subscribe attempt, in `initial` or `disconnected`. The FSM stays parked,
  # and `Subscription` arms the next attempt with `resubscribe_wait/1`.
  #
  # `:lock_already_taken` is the normal answer for a standby: it stays silent and keeps
  # the full interval, so idle instances do not poll. The lock pool answered, so any
  # outage is over.
  defp subscribe_failed(%SubscriptionState{} = data, {:error, :lock_already_taken}) do
    %SubscriptionState{data | subscribe_failures: 0}
  end

  # Any other failure takes the backoff and counts towards it, whichever step produced
  # it. This is one rule, not one per step: a `create_subscription` failure is usually
  # the main pool in the same outage as the lock pool, a permanent one (a missing
  # schema) reaches the cap after a few attempts, and `subscribe_to_events/1` cannot
  # fail, as `PubSub`'s registry has duplicate keys.
  #
  # The first failure of a run logs the reason, so an outage logs once, not per attempt.
  defp subscribe_failed(%SubscriptionState{subscribe_failures: failures} = data, failure) do
    data = %SubscriptionState{data | subscribe_failures: failures + 1}

    if failures == 0 do
      Logger.warning(
        describe(data) <>
          " failed to subscribe due to: " <>
          inspect(failure) <> ", retrying in #{resubscribe_wait(data)}ms"
      )
    end

    data
  end

  @doc """
  The wait before the next subscribe attempt of a parked subscription: the full
  `retry_interval` until an attempt fails with anything but `:lock_already_taken`, then
  the capped backoff over the consecutive failed attempts. A held subscription waits the
  wait `held_wait/2` drew for its next probe.
  """
  def resubscribe_wait(%SubscriptionState{held: %{wait: wait}}), do: wait

  def resubscribe_wait(%SubscriptionState{subscribe_failures: 0} = data) do
    %SubscriptionState{retry_interval: retry_interval} = data

    retry_interval
  end

  def resubscribe_wait(%SubscriptionState{} = data) do
    %SubscriptionState{retry_interval: retry_interval, subscribe_failures: failures} = data

    retry_backoff(failures, retry_interval)
  end

  defp subscribe_to_events(%SubscriptionState{} = data) do
    %SubscriptionState{event_store: event_store, stream_uuid: stream_uuid} = data

    # With the publisher's position notices, so a batch cut before an event of an unknown
    # type still leads to a catch-up read, which holds before it.
    PubSub.subscribe(event_store, stream_uuid, notices: true)
  end

  # Take the row and the lock again and catch up from the row's position: the subscribe of
  # `disconnected`, and of `held` once the held event's type is known.
  #
  # `last_received` is raised to the row's position. Another instance can have delivered
  # events this one never received (the publisher here cut their batch before an unknown
  # type); left behind, an empty catch-up read would never find the subscription caught up.
  defp resubscribe(%SubscriptionState{} = data) do
    with {:ok, subscription} <- create_subscription(data),
         {:ok, lock_ref} <- try_acquire_exclusive_lock(data, subscription) do
      %Storage.Subscription{
        subscription_id: subscription_id,
        last_seen: last_seen
      } = subscription

      %SubscriptionState{last_received: last_received} = data

      last_ack = last_seen || 0

      data = %SubscriptionState{
        data
        | subscription_id: subscription_id,
          lock_ref: lock_ref,
          last_received: max(last_received, last_ack),
          last_sent: last_ack,
          last_ack: last_ack,
          subscribe_failures: 0
      }

      notify_subscribed(data)

      next_state(:request_catch_up, data)
    else
      failure -> next_state(:disconnected, subscribe_failed(data, failure))
    end
  end

  defp monitor_subscriber(%SubscriptionState{} = data, pid, opts) when is_pid(pid) do
    %SubscriptionState{subscribers: subscribers, buffer_size: buffer_size} = data

    subscriber = %Subscriber{
      pid: pid,
      ref: Process.monitor(pid),
      buffer_size: Keyword.get(opts, :buffer_size, buffer_size)
    }

    %SubscriptionState{data | subscribers: Map.put(subscribers, pid, subscriber)}
  end

  defp remove_subscriber(%SubscriptionState{subscribers: subscribers} = data, subscriber_pid)
       when is_pid(subscriber_pid) do
    case subscriber_by_pid(subscribers, subscriber_pid) do
      {:ok, %Subscriber{} = subscriber} ->
        %Subscriber{in_flight: in_flight} = subscriber

        # Prepend in-flight events sent to the removed subscriber back into the
        # pending queue so they can be sent to another available subscriber.

        data =
          in_flight
          |> Enum.sort_by(fn %RecordedEvent{event_number: event_number} -> -event_number end)
          |> Enum.reduce(data, fn event, data -> enqueue_event(data, event, &:queue.in_r/2) end)
          |> SubscriptionState.track_in_flight(in_flight)

        %SubscriptionState{data | subscribers: Map.delete(subscribers, subscriber_pid)}

      {:error, :unknown_subscriber} ->
        data
    end
  end

  defp has_subscribers?(%SubscriptionState{subscribers: subscribers}), do: subscribers != %{}

  # Notify all connected subscribers that this subscription has successfully subscribed.
  defp notify_subscribed(%SubscriptionState{subscribers: subscribers}) do
    for {pid, _subscriber} <- subscribers do
      notify_subscribed(pid)
    end

    :ok
  end

  defp notify_subscribed(subscriber) when is_pid(subscriber) do
    send(subscriber, {:subscribed, self()})
  end

  # Notify all connected subscribers that this subscription has lost its advisory lock.
  defp notify_lock_lost(%SubscriptionState{subscribers: subscribers}) do
    for {pid, _subscriber} <- subscribers do
      send(pid, {:subscription_lock_lost, self()})
    end

    :ok
  end

  defp track_last_received(%SubscriptionState{} = data, events) when is_list(events) do
    track_last_received(data, last_event_number(events))
  end

  defp track_last_received(%SubscriptionState{} = data, event_number)
       when is_number(event_number) do
    %SubscriptionState{last_received: last_received} = data

    %SubscriptionState{data | last_received: max(last_received, event_number)}
  end

  defp track_sent(%SubscriptionState{} = data, event_number) do
    data
    |> SubscriptionState.track_last_sent(event_number)
    |> SubscriptionState.track_in_flight(event_number)
  end

  defp first_event_number([%RecordedEvent{event_number: event_number} | _]), do: event_number

  defp last_event_number([%RecordedEvent{event_number: event_number}]), do: event_number
  defp last_event_number([_event | events]), do: last_event_number(events)

  # A hold is pending: nothing at or after the held event is read. Wait in `catching_up`
  # for the events before it to drain; the `hold` event then holds.
  def catch_up_from_stream(%SubscriptionState{hold_pending: {_event_number, _event_type}} = data) do
    next_state(:catching_up, data)
  end

  def catch_up_from_stream(%SubscriptionState{queue_size: 0} = data) do
    %SubscriptionState{last_sent: last_sent, last_received: last_received} = data

    result = read_stream_forward(data)

    # A read that succeeds, or finds the stream deleted or not yet created, ends a run
    # of failed reads, so the next failure waits the shortest backoff again.
    data =
      case result do
        {:error, reason} when reason not in [:stream_deleted, :stream_not_found] -> data
        _result -> %SubscriptionState{data | catch_up_failures: 0}
      end

    case result do
      # The read met an event of a type this instance does not know: send the events
      # before it, and hold before it once they are drained (the `hold` event).
      {:ok, events, {_event_number, _event_type} = pending} ->
        data =
          %SubscriptionState{data | hold_pending: pending}
          |> enqueue_events(events)
          |> notify_subscribers()

        next_state(:catching_up, data)

      {:ok, [], nil} ->
        if last_sent == last_received do
          # Subscriber is up-to-date with latest published events
          next_state(:subscribed, data)
        else
          # Need to catch-up with events published while catching up
          next_state(:request_catch_up, data)
        end

      {:ok, events, nil} ->
        data = data |> enqueue_events(events) |> notify_subscribers()

        if empty_queue?(data) do
          # Request next batch of events
          next_state(:request_catch_up, data)
        else
          # Wait until subscribers have ack'd in-flight events
          next_state(:catching_up, data)
        end

      {:error, :stream_deleted} ->
        # Don't allow subscriptions to deleted streams to receive any events
        next_state(:unsubscribed, data)

      {:error, :stream_not_found} ->
        # Allow subscriptions to streams which don't yet exist, but might be created later
        next_state(:subscribed, data)

      {:error, reason} ->
        # Any other read error, such as a Postgres error or a query or queue timeout:
        # hold the subscription and retry the read on a backoff timer. This covers a
        # read that returns an error. An event of a type this instance does not know is
        # not an error: the read stops before it and the subscription holds there (see
        # `held`). Any other event that fails to deserialize, such as a corrupt payload,
        # raises in the read and still stops the subscription.
        hold_catch_up(data, reason)
    end
  end

  def catch_up_from_stream(%SubscriptionState{} = data) do
    next_state(:catching_up, data)
  end

  # Keep the cursor and the subscribers, and retry the catch-up read on a timer, never
  # with an immediate `:catch_up` cast, which bounds the read rate while the error
  # persists. The wait is `jittered_backoff/2` over the consecutive failed reads: drawn
  # between half of and the whole of a capped doubling backoff, so subscriptions held on
  # the same error do not retry in step. It is drawn once per hold: the retry timer
  # waits it, and the warning each held read logs and the
  # `[:decree, :store, :subscription, :catch_up, :error]` telemetry event it emits both
  # report it, with the attempt number.
  defp hold_catch_up(%SubscriptionState{} = data, reason) do
    %SubscriptionState{
      event_store: event_store,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      retry_interval: max_retry_interval,
      catch_up_failures: catch_up_failures
    } = data

    attempt = catch_up_failures + 1
    retry_interval = jittered_backoff(attempt, max_retry_interval)

    Logger.warning(
      describe(data) <>
        " failed to catch up due to: " <>
        inspect(reason) <> ", attempt #{attempt}, retrying in #{retry_interval}ms"
    )

    :telemetry.execute(
      [:decree, :store, :subscription, :catch_up, :error],
      %{system_time: System.system_time()},
      %{
        event_store: event_store,
        subscription_name: subscription_name,
        stream_uuid: stream_uuid,
        reason: reason,
        retry_interval: retry_interval,
        attempt: attempt
      }
    )

    # A new token per hold: a timer left from an earlier hold is ignored.
    token = make_ref()

    Process.send_after(self(), {:retry_catch_up, token}, retry_interval)

    next_state(:catch_up_retry, %SubscriptionState{
      data
      | catch_up_retry_token: token,
        catch_up_failures: attempt
    })
  end

  # The capped doubling backoff after the `attempt`th consecutive failure:
  # `min(1_000, retry_interval)` after the first, doubling after each further one, up to
  # `retry_interval`. A failed subscribe attempt waits it as it is (`resubscribe_wait/1`);
  # a failed catch-up read and a held subscription's probe wait `jittered_backoff/2`
  # over it. A `retry_interval` below one second caps it at `retry_interval` from the
  # first failure; only a subscription's own `retry_interval` option can be that short,
  # since the event store's `subscription_retry_interval` is raised to at least one
  # second.
  defp retry_backoff(attempt, retry_interval) do
    first = min(1_000, retry_interval)

    # The exponent is capped so a long run of failures stays a small integer.
    min(first * Integer.pow(2, min(attempt - 1, 32)), retry_interval)
  end

  # Hold before the event of a type this instance does not know (#7804), once the events
  # before it are drained. In order: write the checkpoint now, whatever the checkpoint
  # threshold or delay, so the next owner starts at the held event; tell every subscriber
  # the lock is lost (an event handler stays alive and drops its batch); release the lock;
  # drop the event tracking; and alarm. Another instance can now take the lock and deliver
  # the event. A checkpoint write that fails is only logged, as everywhere: the next owner
  # then delivers some events again, and skips none.
  #
  # A transient subscription has no row and no lock, and its position lives in its process:
  # it alarms and stops (`Subscription` stops it under `{:shutdown, {:unknown_event_type,
  # type}}`), so its subscriber resubscribes from its own position.
  defp hold_subscription(%SubscriptionState{hold_pending: {event_number, event_type}} = data) do
    data = data |> cancel_checkpoint_timer() |> persist_checkpoint()

    case data do
      %SubscriptionState{transient: true} ->
        held = %{event_number: event_number, type: event_type, attempt: 1, wait: nil}
        data = %SubscriptionState{data | hold_pending: nil, held: held}

        alarm(data, nil)

        next_state(:held, data)

      %SubscriptionState{transient: false} ->
        notify_lock_lost(data)
        release_held_lock(data)

        %SubscriptionState{retry_interval: retry_interval} = data

        held = %{
          event_number: event_number,
          type: event_type,
          attempt: 1,
          wait: held_wait(1, retry_interval)
        }

        data =
          %SubscriptionState{
            data
            | lock_ref: nil,
              catch_up_retry_token: nil,
              catch_up_failures: 0,
              held: held
          }
          |> SubscriptionState.reset_event_tracking()
          |> purge_in_flight_events()

        alarm(data, nil)

        next_state(:held, data)
    end
  end

  # A probe found the event still not deliverable here: stay held, wait longer, alarm again.
  defp hold_again(%SubscriptionState{held: held} = data, reason) do
    %SubscriptionState{retry_interval: retry_interval} = data
    %{attempt: attempt} = held

    attempt = attempt + 1

    data = %SubscriptionState{
      data
      | held: %{held | attempt: attempt, wait: held_wait(attempt, retry_interval)}
    }

    alarm(data, reason)

    next_state(:held, data)
  end

  # Release the advisory lock before holding. `{:error, :not_found}` means it was already
  # released (a lost session), and a call that does not answer still leaves the lock to be
  # unlocked, or its session ended, by `AdvisoryLocks`: either way the subscription holds
  # with `lock_ref` cleared, so a late `:lock_released` for it is ignored.
  defp release_held_lock(%SubscriptionState{lock_ref: nil}), do: :ok

  defp release_held_lock(%SubscriptionState{lock_ref: lock_ref} = data) do
    %SubscriptionState{event_store: event_store, query_timeout: query_timeout} = data

    server = Module.concat(event_store, AdvisoryLocks)

    try do
      case AdvisoryLocks.unlock(server, lock_ref, query_timeout) do
        :ok -> :ok
        {:error, :not_found} -> :ok
      end
    catch
      :exit, reason ->
        Logger.warning(
          describe(data) <> " did not hear back on releasing its lock due to: " <> inspect(reason)
        )

        :ok
    end
  end

  defp cancel_checkpoint_timer(%SubscriptionState{checkpoint_timer_ref: nil} = data), do: data

  defp cancel_checkpoint_timer(%SubscriptionState{checkpoint_timer_ref: ref} = data) do
    _result = Process.cancel_timer(ref)

    %SubscriptionState{data | checkpoint_timer_ref: nil}
  end

  # The probe of a held subscription, on the main pool and without the lock: `:delivered`
  # when its row is at or past the held event, or gone (or replaced by a row of another
  # id); else `:known` or `:unknown` for the held event's type; `{:error, reason}` when a
  # read failed. An event no longer there reads as known, so the subscribe that follows
  # finds out what the stream holds now.
  defp probe_held(%SubscriptionState{} = data, event_number) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscription_id: subscription_id,
      query_timeout: query_timeout
    } = data

    row =
      Storage.Subscription.subscription(conn, stream_uuid, subscription_name,
        schema: schema,
        timeout: query_timeout
      )

    case row do
      {:error, :subscription_not_found} ->
        :delivered

      {:ok, %Storage.Subscription{subscription_id: id}} when id != subscription_id ->
        :delivered

      {:ok, %Storage.Subscription{last_seen: last_seen}}
      when is_integer(last_seen) and last_seen >= event_number ->
        :delivered

      {:ok, %Storage.Subscription{}} ->
        probe_held_type(data, event_number)

      {:error, _reason} = error ->
        error
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp probe_held_type(%SubscriptionState{} = data, event_number) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid,
      query_timeout: query_timeout,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = data

    case Stream.read_event_type(conn, stream_uuid, event_number,
           schema: schema,
           timeout: query_timeout,
           correlation_id_type: correlation_id_type,
           causation_id_type: causation_id_type
         ) do
      {:ok, nil} -> :known
      {:ok, type} -> if RecordedEvent.known_type?(serializer, type), do: :known, else: :unknown
      {:error, _reason} = error -> error
    end
  end

  @doc """
  A wait drawn at random after the `attempt`th consecutive failure: an equal jitter over
  the capped doubling backoff (`min(1_000, retry_interval)` after the first failure,
  doubling after each further one, up to `retry_interval`), so that
  `max(1, div(base, 2)) <= wait <= max(1, base)`, where `base <= retry_interval` is that backoff.
  It is never under one millisecond, so nothing that waits it retries in a tight loop.

  A failed catch-up read waits it before the next read, and a held subscription before
  its next probe (`held_wait/2`).
  """
  def jittered_backoff(attempt, retry_interval) do
    base = retry_backoff(attempt, retry_interval)
    half = div(base, 2)

    max(1, half + :rand.uniform(half + 1) - 1)
  end

  @doc """
  The wait before the next probe of a held subscription, after its `attempt`th alarm: the
  same draw as a failed catch-up read's wait, `jittered_backoff/2`, so that
  `max(1, div(base, 2)) <= wait <= max(1, base)`, where `base <= retry_interval` is the
  capped doubling backoff. It is never under one millisecond, so a held subscription never
  probes in a tight loop.
  """
  def held_wait(attempt, retry_interval), do: jittered_backoff(attempt, retry_interval)

  # The alarm of a held subscription: a warning naming the event's type and number, never
  # its data, and the `[:decree, :store, :subscription, :held]` telemetry event (documented
  # on `c:Decree.Store.subscribe_to_stream/4`). Raised on every hold and on every probe
  # that leaves the subscription held.
  defp alarm(%SubscriptionState{held: held} = data, reason) do
    %SubscriptionState{
      event_store: event_store,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      transient: transient
    } = data

    %{event_number: event_number, type: event_type, attempt: attempt, wait: wait} = held

    next =
      case transient do
        true -> ", stopping"
        false -> ", attempt #{attempt}, probing again in #{wait}ms"
      end

    because =
      case reason do
        nil -> ""
        reason -> " (the probe failed due to: " <> inspect(reason) <> ")"
      end

    Logger.warning(
      describe(data) <>
        " is held before event #{event_number}: its type #{inspect(event_type)} is not " <>
        "known to this instance" <> because <> next
    )

    :telemetry.execute(
      [:decree, :store, :subscription, :held],
      %{system_time: System.system_time()},
      %{
        event_store: event_store,
        subscription_name: subscription_name,
        stream_uuid: stream_uuid,
        event_type: event_type,
        event_number: event_number,
        retry_interval: wait,
        attempt: attempt,
        transient: transient
      }
    )
  end

  defp read_stream_forward(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid,
      last_sent: last_sent,
      max_size: max_size,
      query_timeout: query_timeout,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = data

    Stream.read_stream_forward_known(conn, stream_uuid, last_sent + 1, max_size,
      schema: schema,
      serializer: serializer,
      timeout: query_timeout,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    )
  end

  defp enqueue_events(%SubscriptionState{} = data, []), do: data

  defp enqueue_events(%SubscriptionState{} = data, [event | events]) do
    %SubscriptionState{acknowledged_event_numbers: acknowledged_event_numbers} = data
    %RecordedEvent{event_number: event_number} = event

    data =
      if selected?(event, data) do
        # Unfiltered event, enqueue to send to a subscriber
        enqueue_event(data, event)
      else
        # Filtered event, don't send to subscriber, but track it as ack'd.
        %SubscriptionState{
          data
          | acknowledged_event_numbers: MapSet.put(acknowledged_event_numbers, event_number)
        }
        |> track_sent(event_number)
      end

    data
    |> track_last_received(event_number)
    |> enqueue_events(events)
  end

  defp enqueue_event(%SubscriptionState{} = data, event, enqueue \\ &:queue.in/2) do
    %SubscriptionState{partitions: partitions, queue_size: queue_size} = data

    partition_key = partition_key(data, event)

    partitions =
      partitions
      |> Map.put_new(partition_key, :queue.new())
      |> Map.update!(partition_key, fn pending_events -> enqueue.(event, pending_events) end)

    %SubscriptionState{data | partitions: partitions, queue_size: queue_size + 1}
  end

  def partition_key(%SubscriptionState{partition_by: nil}, %RecordedEvent{}), do: nil

  def partition_key(%SubscriptionState{partition_by: partition_by}, %RecordedEvent{} = event)
      when is_function(partition_by, 1),
      do: partition_by.(event)

  # Attempt to notify subscribers with any pending events. Partitions are
  # selected by peeking at the event number of their queue to ensure earlier
  # events are sent first.
  defp notify_subscribers(%SubscriptionState{partitions: partitions} = data) do
    partitions
    |> Enum.sort_by(fn {_partition_key, pending_events} -> peek_event_number(pending_events) end)
    |> Enum.reduce(data, fn {partition_key, _pending_events}, data ->
      notify_partition_subscriber(data, partition_key)
    end)
    |> checkpoint_acknowledged()
  end

  defp peek_event_number(pending_events) do
    case :queue.peek(pending_events) do
      {:value, %RecordedEvent{event_number: event_number}} -> event_number
      :empty -> nil
    end
  end

  defp notify_partition_subscriber(
         %SubscriptionState{} = data,
         partition_key,
         events_to_send \\ []
       ) do
    %SubscriptionState{
      partitions: partitions,
      subscribers: subscribers,
      queue_size: queue_size
    } = data

    with pending_events when not is_nil(pending_events) <- Map.get(partitions, partition_key),
         {{:value, event}, pending_events} <- :queue.out(pending_events),
         {:ok, subscriber} <- next_available_subscriber(data, partition_key) do
      %RecordedEvent{event_number: event_number} = event
      %Subscriber{pid: subscriber_pid} = subscriber

      subscriber = Subscriber.track_in_flight(subscriber, event, partition_key)

      partitions =
        case :queue.is_empty(pending_events) do
          true -> Map.delete(partitions, partition_key)
          false -> Map.put(partitions, partition_key, pending_events)
        end

      %SubscriptionState{
        data
        | partitions: partitions,
          subscribers: Map.put(subscribers, subscriber_pid, subscriber),
          queue_size: max(queue_size - 1, 0)
      }
      |> track_sent(event_number)
      |> notify_partition_subscriber(partition_key, [{subscriber_pid, event} | events_to_send])
    else
      _ ->
        # No further queued event or available subscriber, send ready events to
        # subscribers then stop notifying.
        send_queued_events(events_to_send, data)
    end
  end

  # Send events to the subscriber
  defp send_queued_events([], data), do: data

  defp send_queued_events(events_to_send, data) do
    events_to_send
    |> Enum.group_by(fn {pid, _event} -> pid end, fn {_pid, event} -> event end)
    |> Enum.each(fn {pid, events} ->
      mapped_events = events |> Enum.reverse() |> map(data)

      send(pid, {:events, mapped_events})
    end)

    data
  end

  # Select the next available subscriber based upon their partition key, buffer
  # size and number of currently in-flight events.
  #
  # Uses a round robin strategy for balancing events between subscribers.
  #
  # Events will be distributed to subscribers based upon their partition key
  # when a `partition_by/1` function is provided. This is used to guarantee
  # ordering of events for each partition.
  defp next_available_subscriber(%SubscriptionState{} = data, partition_key) do
    %SubscriptionState{subscribers: subscribers} = data

    partition_subscriber =
      Enum.find(subscribers, fn {_pid, subscriber} ->
        Subscriber.in_partition?(subscriber, partition_key)
      end)

    subscribers =
      case partition_subscriber do
        nil -> subscribers
        subscriber -> [subscriber]
      end

    subscribers
    |> Enum.sort_by(fn {_pid, %Subscriber{last_sent: last_sent}} -> last_sent end)
    |> Enum.find(fn {_pid, subscriber} -> Subscriber.available?(subscriber) end)
    |> case do
      nil -> {:error, :no_available_subscriber}
      {_pid, subscriber} -> {:ok, subscriber}
    end
  end

  defp selected?(event, %SubscriptionState{selector: selector}) when is_function(selector, 1),
    do: selector.(event)

  defp selected?(_event, %SubscriptionState{}), do: true

  defp map(events, %SubscriptionState{mapper: mapper}) when is_function(mapper, 1),
    do: Enum.map(events, mapper)

  defp map(events, _mapper), do: events

  defp ack_events(%SubscriptionState{} = data, ack, subscriber_pid) do
    %SubscriptionState{
      subscribers: subscribers,
      acknowledged_event_numbers: acknowledged_event_numbers
    } = data

    with {:ok, subscriber} <- subscriber_by_pid(subscribers, subscriber_pid),
         {:ok, subscriber, acknowledged_events} <- Subscriber.acknowledge(subscriber, ack) do
      acknowledged_event_numbers =
        acknowledged_events
        |> Enum.map(& &1.event_number)
        |> Enum.reduce(acknowledged_event_numbers, &MapSet.put(&2, &1))

      data =
        %SubscriptionState{
          data
          | subscribers: Map.put(subscribers, subscriber_pid, subscriber),
            acknowledged_event_numbers: acknowledged_event_numbers
        }
        |> notify_subscribers()

      {:ok, data}
    end
  end

  defp subscriber_by_pid(subscribers, subscriber_pid) do
    case Map.get(subscribers, subscriber_pid) do
      %Subscriber{} = subscriber -> {:ok, subscriber}
      nil -> {:error, :unknown_subscriber}
    end
  end

  defp checkpoint_acknowledged(data, persist \\ false)

  defp checkpoint_acknowledged(%SubscriptionState{in_flight_event_numbers: []} = data, persist) do
    if persist, do: maybe_persist_checkpoint(data), else: data
  end

  defp checkpoint_acknowledged(%SubscriptionState{} = data, persist) do
    %SubscriptionState{
      acknowledged_event_numbers: acknowledged_event_numbers,
      checkpoints_pending: checkpoints_pending,
      in_flight_event_numbers: [ack | in_flight_event_numbers]
    } = data

    if MapSet.member?(acknowledged_event_numbers, ack) do
      %SubscriptionState{
        data
        | acknowledged_event_numbers: MapSet.delete(acknowledged_event_numbers, ack),
          in_flight_event_numbers: in_flight_event_numbers,
          checkpoints_pending: checkpoints_pending + 1,
          last_ack: ack
      }
      |> checkpoint_acknowledged(true)
    else
      if persist, do: maybe_persist_checkpoint(data), else: data
    end
  end

  defp maybe_persist_checkpoint(%SubscriptionState{transient: true} = data) do
    %SubscriptionState{data | checkpoints_pending: 0}
  end

  defp maybe_persist_checkpoint(%SubscriptionState{transient: false} = data) do
    %SubscriptionState{
      checkpoint_after: checkpoint_after,
      checkpoints_pending: checkpoints_pending,
      checkpoint_threshold: checkpoint_threshold,
      checkpoint_timer_ref: checkpoint_timer_ref
    } = data

    cond do
      checkpoints_pending >= checkpoint_threshold ->
        persist_checkpoint(data)

      checkpoint_after > 0 ->
        if checkpoint_timer_ref, do: Process.cancel_timer(checkpoint_timer_ref)

        checkpoint_timer_ref = Process.send_after(self(), :checkpoint, checkpoint_after)

        %SubscriptionState{data | checkpoint_timer_ref: checkpoint_timer_ref}

      true ->
        data
    end
  end

  defp persist_checkpoint(%SubscriptionState{transient: true} = data) do
    %SubscriptionState{data | checkpoints_pending: 0}
  end

  defp persist_checkpoint(%SubscriptionState{transient: false} = data) do
    %SubscriptionState{checkpoints_pending: checkpoints_pending} = data

    if checkpoints_pending > 0 do
      write_checkpoint(data)
    end

    %SubscriptionState{data | checkpoints_pending: 0}
  end

  # Write the position to the subscription's own row, by its `subscription_id`.
  # Keyed by name, a subscription whose row was deleted and created again (with a
  # new id, by an operator reset or another instance) would write its position
  # into the new row, past where that row's subscription starts.
  #
  # A row that no longer exists is only logged: the subscription keeps running,
  # and its position is not written anywhere.
  defp write_checkpoint(%SubscriptionState{subscription_id: subscription_id} = data)
       when is_integer(subscription_id) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      last_ack: last_ack,
      query_timeout: query_timeout
    } = data

    case Storage.Subscription.ack_last_seen_event_by_id(conn, subscription_id, last_ack,
           schema: schema,
           timeout: query_timeout
         ) do
      {:error, :subscription_not_found} ->
        Logger.warning(
          describe(data) <>
            " (subscription_id #{subscription_id}) did not write its checkpoint " <>
            "(event #{last_ack}): its subscription row no longer exists"
        )

      _reply ->
        :ok
    end
  end

  # No row id is known: the subscription has never taken its lock, so it has
  # acknowledged nothing. Kept for completeness.
  defp write_checkpoint(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      last_ack: last_ack,
      query_timeout: query_timeout
    } = data

    Storage.Subscription.ack_last_seen_event(conn, stream_uuid, subscription_name, last_ack,
      schema: schema,
      timeout: query_timeout
    )
  end

  # Purge all subscriber in-flight events and subscription event queue.
  defp purge_in_flight_events(%SubscriptionState{} = data) do
    %SubscriptionState{subscribers: subscribers} = data

    subscribers =
      Enum.reduce(subscribers, %{}, fn {pid, subscriber}, acc ->
        Map.put(acc, pid, Subscriber.reset_in_flight(subscriber))
      end)

    %SubscriptionState{data | subscribers: subscribers}
  end

  defp empty_queue?(%SubscriptionState{queue_size: 0}), do: true
  defp empty_queue?(%SubscriptionState{}), do: false

  defp over_capacity?(%SubscriptionState{queue_size: queue_size, max_size: max_size}),
    do: queue_size >= max_size

  defp describe(%SubscriptionState{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect(name)}@#{inspect(stream_uuid)}"
end
