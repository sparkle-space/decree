defmodule Decree.Store.Subscriptions.SubscriptionFsm do
  @moduledoc false

  alias Decree.Store.{AdvisoryLocks, LogTerms, PubSub, RecordedEvent, Storage}
  alias Decree.Store.Streams.{Stream, StreamInfo}
  alias Decree.Store.Subscriptions.{SubscriptionState, Subscriber}

  use Decree.Store.Subscriptions.GeneratedFsm,
    initial_state: :initial,
    initial_data: %SubscriptionState{}

  require Logger

  @all_stream "$all"

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
        # The interval of the head check while subscribed. A started subscription always
        # passes the event store's `subscription_head_check_interval` (or the caller's
        # `head_check_interval`); the default is that setting's default.
        head_check_interval: Keyword.get(opts, :head_check_interval, 60_000),
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
  # A catch-up read that meets an event of a type this instance does not know
  # (sparkle-space/sparklespace#7804), or of a known type whose deserialize raises or exits
  # (sparkle-space/sparklespace#8357), records it as `hold_pending` and reads nothing at or
  # after it. Once the events before it are drained (acked or filtered, none queued or in
  # flight), the subscription holds before it in `held`: it has written its checkpoint and
  # released its lock, and probes on a backoff timer, without the lock, until another
  # instance has delivered the event, or this one knows its type or can deserialize it:
  #
  #   (any delivering state) -> held -> disconnected | (subscribe as disconnected does)
  #
  # Apart from the head check, no timer reads a subscribed subscription's stream: its
  # position moves forward only when the publisher wakes it with events or a position
  # notice. `Subscription` sends it a `check_head` on a timer, which reads the stream's
  # head and, on a wake-up confirmed lost, catches up (sparkle-space/sparklespace#8066):
  #
  #   subscribed -> (check_head) -> request_catch_up
  #
  # A stream subscription follows its stream across a hard delete
  # (sparkle-space/sparklespace#8386). Its cursor is a position in one stream, known by its
  # `stream_id` (`SubscriptionState.stream_id`); a hard delete removes the stream, and an
  # append to the same uuid creates it again, from version 1, under a new `stream_id`. The
  # hard delete puts every subscription row of the stream back to 0
  # (`DeleteStream.hard_delete/3`), and a running subscription that finds its stream gone or
  # replaced follows it (`follow_stream/2`): it drops its position, its queue and the
  # tracking of what it acknowledged, retires its subscribers' in-flight events (their acks
  # are taken and dropped, `Subscriber.retire_in_flight/1`), puts its row back to 0 (only
  # from a position it may have written itself, `reset_checkpoint/1`) and reads the stream
  # from its start, adopting the `stream_id` that read finds:
  #
  #   (any delivering state) -> (follow) -> request_catch_up
  #
  # A `$all` subscription never follows: `$all` is never hard deleted, and its
  # `stream_id` stays `nil`. A stream subscription reads the `stream_id` before its row at
  # every subscribe, so a hard delete between the two leaves the row at 0 or the read
  # replaced. Each signal that can learn the stream's id compares it with the one followed:
  #
  #   state               signal                               on another id, or no stream
  #   initial             subscribe (reads the id, then row)   takes the id read
  #   disconnected, held  subscribe, probe -> resubscribe      follows, unless the row is
  #                                                             above its own last ack: then
  #                                                             takes the row (another
  #                                                             instance delivered it)
  #   request_catch_up,   the catch-up read (it returns the    follows; the events read
  #   catching_up,          id it read from)                     are dropped
  #   catch_up_retry
  #   subscribed          check_head (reads the id)            follows
  #   subscribed          notify_events, past events (reads    follows; a failed read asks
  #                         the id)                              for a catch-up read
  #   subscribed          notify_events, expected events,      adopts the id; with no
  #                         no id known (reads the id)           stream, enqueues them as
  #                                                              before, no id known
  #   any but unsubscribed  ack, checkpoint, unsubscribe,      a checkpoint write after the
  #                         hold, terminate (the write)          delete writes nothing and
  #                                                              sets `stream_gone`; the
  #                                                              `check_stream` event that
  #                                                              follows every event then
  #                                                              follows in a delivering
  #                                                              state
  #   any                 ack of a retired event               taken and dropped: `:ok`, no
  #                                                              position moves
  #
  # A follow sends each subscriber `{:subscription_stream_recreated, subscription}` before
  # any event of the stream read again.
  #
  # Every other cell is as before: `notify_events` outside `subscribed` tracks
  # `last_received` only, and the catch-up read that follows compares the id; a head check
  # outside `subscribed`, or with a hold pending, changes nothing.

  defstate initial do
    defevent subscribe, data: %SubscriptionState{transient: true} = data do
      data = SubscriptionState.reset_event_tracking(data)

      with {:ok, stream_id} <- read_stream_id(data),
           :ok <- subscribe_to_events(data) do
        last_seen = data.start_from

        data = %SubscriptionState{
          data
          | last_received: last_seen,
            last_sent: last_seen,
            last_ack: last_seen,
            stream_id: stream_id,
            stream_gone: false,
            subscribe_failures: 0,
            subscribe_wait: nil
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

      # The stream's id is read before the row: a hard delete in between has reset the row
      # to 0, and one after it replaces the id the catch-up read then finds.
      with {:ok, stream_id} <- read_stream_id(data),
           {:ok, subscription} <- create_subscription(data),
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
            stream_id: stream_id,
            stream_gone: false,
            subscribe_failures: 0,
            subscribe_wait: nil
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

          # Ignore already seen events, unless they are a stream's first events after a hard
          # delete and a recreate under its uuid.
          past_events(data)

        future when future > expected_event ->
          Logger.debug(describe(data) <> " received unexpected event(s), requesting catch up")

          # Missed event(s), request catch-up with any unseen events from storage
          next_state(:request_catch_up, data)

        ^expected_event ->
          expected_events(data, events)
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

    # The head check (sparkle-space/sparklespace#8066). No other timer reads a subscribed
    # subscription's stream: its position moves forward only when the publisher wakes it,
    # and a wake-up can be lost: the notifications connection reconnects
    # (`auto_reconnect`) and listens again with no catch-up, the publisher's read of a
    # notification's events fails and is only logged, and Postgrex drops a notification
    # that arrives during an idle ping (elixir-ecto/postgrex#795). Each would leave the
    # subscription behind until the next append to its stream.
    #
    # `Subscription` sends this every quarter to half of `head_check_interval`. It reads
    # the stream's head: one statement on the main pool, no lock, and no cache, as a head
    # read is a ledger read. A head is a missed wake-up only when the previous check saw
    # it and it is still above `last_received` now: a read can see an append before its
    # wake-up arrives, and acting on the first sighting would count a late wake-up as a
    # lost one. So a lost wake-up is seen within one period, confirmed within the next,
    # and repaired within `head_check_interval`, through the catch-up a gap in the events
    # received takes.
    #
    # `last_received` is never raised to the head: a catch-up read that finds nothing
    # while `last_sent != last_received` asks to catch up again, which a head with no
    # event under it would turn into a loop. Such a head exists: a hard delete removes
    # `$all`'s rows of the stream's events but never lowers `$all`'s `stream_version`.
    # `head_repaired` instead repairs each head once, not again at every check. It records
    # the head read now, not the one confirmed: the catch-up starts after this read and
    # appends are serialised on `$all`'s row, so it reads every event committed at or
    # below this head, and a later lost wake-up needs a head above it. Recording the
    # confirmed head would repair again when the head rose between the two sightings
    # with nothing readable above `last_received`.
    #
    # A failed read is retried at the next check, without a catch-up error: it is logged
    # at debug level only and leaves `head_seen` as it was.
    #
    # The same read gives the stream's id: another id than the one followed, or no stream
    # after one was known, is a hard delete, and the subscription follows the stream
    # (sparkle-space/sparklespace#8386).
    defevent check_head, data: %SubscriptionState{hold_pending: nil} = data do
      case read_head(data) do
        {:ok, head, stream_id} ->
          if stream_changed?(data, stream_id) do
            follow_stream(data, :head_check)
          else
            data = adopt_stream_id(data, stream_id)

            %SubscriptionState{
              head_seen: target,
              last_received: last_received,
              head_repaired: head_repaired
            } = data

            if is_integer(target) and target > last_received and target > head_repaired do
              missed_wake_up(data, head, target)

              next_state(:request_catch_up, %SubscriptionState{
                data
                | head_repaired: head,
                  head_seen: head
              })
            else
              next_state(:subscribed, %SubscriptionState{data | head_seen: head})
            end
          end

        {:error, reason} ->
          Logger.debug(
            describe(data) <> " head check failed due to: " <> inspect(LogTerms.reduce(reason))
          )

          next_state(:subscribed, data)
      end
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
  # jittered backoff that attempt drew (`resubscribe_wait/1`). Subscribers were sent
  # `{:subscription_lock_lost, subscription}` on the way in, and are sent
  # `{:subscribed, subscription}` again on the way out.
  defstate disconnected do
    # Attempt to subscribe
    defevent subscribe, data: %SubscriptionState{} = data do
      resubscribe(data)
    end
  end

  # Holding before an event of a type this instance does not know (#7804), or of a known
  # type it could not deserialize (#8357); `held.cause` says which. The events before it
  # were delivered and checkpointed, every subscriber was sent
  # `{:subscription_lock_lost, subscription}`, and the advisory lock was released, so an
  # instance that can read the event can take the lock and deliver it. Nothing is
  # delivered here and no lock is held; `Subscription.status/1` reports `:parked`.
  #
  # `Subscription` arms a timer with the wait drawn into `held`, and its `probe` reads, on
  # the main pool and without the lock:
  #
  #   - the subscription's row: at or past the held event, or gone, means another instance
  #     delivered it, so go to `disconnected`, a silent standby on the full interval;
  #   - else, held on an unknown type, the held event's type: known now, so subscribe as
  #     `disconnected` does (row, lock, catch-up); still unknown, or the probe failed, so
  #     stay held, alarm again and wait longer;
  #   - else, held on an undecodable event, the one event, read and deserialized as the
  #     catch-up read does: it decodes now, is gone, or its type is now unknown, so
  #     subscribe (the catch-up read then finds what the stream holds now); still
  #     undecodable, or the read failed, so stay held, alarm again and wait longer.
  #
  # A probe whose read finds the stream deleted (`:stream_deleted`) or gone
  # (`:stream_not_found`, after a hard delete) has not failed: either probe subscribes, and
  # the catch-up read decides as it does for any subscription, `unsubscribed` for a deleted
  # stream and `subscribed` for a missing one (sparkle-space/sparklespace#8357).
  #
  # Every other event is handled by the catch-alls: events and notices track
  # `last_received` only, an ack or a checkpoint is ignored (nothing is in flight), and a
  # late `:lock_released` for the released lock does not match `lock_ref` (now `nil`).
  defstate held do
    defevent probe, data: %SubscriptionState{held: %{} = held} = data do
      case probe_held(data, held) do
        :delivered ->
          next_state(:disconnected, %SubscriptionState{
            data
            | held: nil,
              subscribe_failures: 0,
              subscribe_wait: nil
          })

        :known ->
          resubscribe(%SubscriptionState{data | held: nil})

        :unknown ->
          hold_again(data, nil)

        # Still undecodable: the cause carries the class this read raised or exited with.
        {:undecodable, _class} = cause ->
          hold_again(%SubscriptionState{data | held: %{held | cause: cause}}, nil)

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
        hold_pending: {_event_number, _event_type, _cause},
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

  # Checked after every event, after `hold` (`Subscription` sends it): a checkpoint write
  # found no stream with the followed `stream_id`, so the stream was hard deleted. A
  # delivering state follows it now, rather than at the next read, head check or batch. A
  # held subscription's resubscribe compares the id itself; any other state does nothing.
  defevent check_stream,
    data: %SubscriptionState{stream_gone: true} = data,
    state: state,
    when: state in [:request_catch_up, :catching_up, :subscribed, :max_capacity, :catch_up_retry] do
    follow_stream(data, :checkpoint)
  end

  defevent check_stream, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # A probe timer that fired after the subscription left `held`.
  defevent probe, data: %SubscriptionState{} = data, state: state do
    next_state(state, data)
  end

  # A head check outside `subscribed` (or with a hold pending) changes nothing.
  defevent check_head, data: %SubscriptionState{} = data, state: state do
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
    %SubscriptionState{data | subscribe_failures: 0, subscribe_wait: nil}
  end

  # Any other failure takes the backoff and counts towards it, whichever step produced
  # it. This is one rule, not one per step: a `create_subscription` failure is usually
  # the main pool in the same outage as the lock pool, a permanent one (a missing
  # schema) reaches the cap after a few attempts, and `subscribe_to_events/1` cannot
  # fail, as `PubSub`'s registry has duplicate keys.
  #
  # The wait is `jittered_backoff/2` over the consecutive failed attempts, drawn here once
  # per failed attempt and kept in `subscribe_wait`, as a held subscription keeps its
  # probe's wait: every pass through the parked state reads the same wait, so a later pass
  # does not re-arm on a shorter draw, and the warning names the wait that is armed.
  #
  # The first failure of a run logs the reason, so an outage logs once, not per attempt.
  defp subscribe_failed(%SubscriptionState{subscribe_failures: failures} = data, failure) do
    %SubscriptionState{retry_interval: retry_interval} = data

    attempt = failures + 1

    data = %SubscriptionState{
      data
      | subscribe_failures: attempt,
        subscribe_wait: jittered_backoff(attempt, retry_interval)
    }

    if failures == 0 do
      Logger.warning(
        describe(data) <>
          " failed to subscribe due to: " <>
          inspect(LogTerms.reduce(failure)) <> ", retrying in #{resubscribe_wait(data)}ms"
      )
    end

    data
  end

  @doc """
  The wait before the next subscribe attempt of a parked subscription: the full
  `retry_interval` until an attempt fails with anything but `:lock_already_taken`, then
  the wait the last failed attempt drew, `jittered_backoff/2` over the consecutive failed
  attempts. A held subscription waits the wait `held_wait/2` drew for its next probe.

  A failed attempt's wait is drawn once, when it fails, and every later call answers that
  draw: `subscribe_failures` above 0 always comes with a drawn `subscribe_wait`, and 0 with
  none. The first wait is the full `retry_interval`, never jittered: it is not a
  backoff over failed attempts, and it is the poll cadence of a standby, whose attempts
  find the lock taken, so jittering it would make idle instances poll sooner.
  """
  def resubscribe_wait(%SubscriptionState{held: %{wait: wait}}), do: wait

  def resubscribe_wait(%SubscriptionState{subscribe_failures: 0, subscribe_wait: nil} = data) do
    %SubscriptionState{retry_interval: retry_interval} = data

    retry_interval
  end

  def resubscribe_wait(%SubscriptionState{subscribe_failures: failures, subscribe_wait: wait})
      when failures > 0 and is_integer(wait),
      do: wait

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
  #
  # Unless the stream was hard deleted since this instance last held the lock
  # (sparkle-space/sparklespace#8386). The stream's id is read before the row. Another id,
  # or no stream, after one was known: a row at or below this instance's own last ack is
  # the hard delete's reset, or a position in the deleted stream (a hard delete by a release
  # before this one), so the subscription follows the stream from its start, as it does
  # whatever the row says when there is no stream; a row above it, beside a recreated
  # stream, was written by another instance in that stream, and is taken as it is. Either
  # way `last_received` is not raised: a position received in the deleted stream is not one
  # in the recreated stream, and an empty read would never reach it. A row below this
  # instance's own last ack with the same stream was reset under it, by a hard delete or an
  # operator: its position is taken for all three.
  defp resubscribe(%SubscriptionState{} = data) do
    with {:ok, stream_id} <- read_stream_id(data),
         {:ok, subscription} <- create_subscription(data),
         {:ok, lock_ref} <- try_acquire_exclusive_lock(data, subscription) do
      %Storage.Subscription{
        subscription_id: subscription_id,
        last_seen: last_seen
      } = subscription

      %SubscriptionState{last_received: last_received, last_ack: own_last_ack} = data

      last_ack = last_seen || 0
      changed? = stream_changed?(data, stream_id)

      data = %SubscriptionState{
        data
        | subscription_id: subscription_id,
          lock_ref: lock_ref,
          stream_gone: false,
          subscribe_failures: 0,
          subscribe_wait: nil
      }

      data =
        cond do
          changed? and (is_nil(stream_id) or last_ack <= own_last_ack) ->
            follow_stream_data(data, :subscribe)

          changed? or last_ack < own_last_ack ->
            %SubscriptionState{
              data
              | last_received: last_ack,
                last_sent: last_ack,
                last_ack: last_ack,
                stream_id: stream_id
            }

          true ->
            %SubscriptionState{
              data
              | last_received: max(last_received, last_ack),
                last_sent: last_ack,
                last_ack: last_ack,
                stream_id: stream_id
            }
        end

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
  def catch_up_from_stream(
        %SubscriptionState{hold_pending: {_event_number, _event_type, _cause}} = data
      ) do
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
      # The read found another stream than the one followed: it was hard deleted and created
      # again. The events read are the recreated stream's, from the old position: dropped,
      # and the stream is read again from its start (sparkle-space/sparklespace#8386).
      {:ok, _events, _pending, stream_id}
      when stream_id != data.stream_id and
             not is_nil(data.stream_id) ->
        follow_stream(data, :catch_up)

      {:ok, events, pending, stream_id} ->
        data
        |> adopt_stream_id(stream_id)
        |> caught_up(events, pending, last_sent, last_received)

      {:error, :stream_deleted} ->
        # Don't allow subscriptions to deleted streams to receive any events
        next_state(:unsubscribed, data)

      # The followed stream was hard deleted and not created again (yet).
      {:error, :stream_not_found} when not is_nil(data.stream_id) ->
        follow_stream(data, :catch_up)

      {:error, :stream_not_found} ->
        # Allow subscriptions to streams which don't yet exist, but might be created later
        next_state(:subscribed, data)

      {:error, reason} ->
        # Any other read error, such as a Postgres error or a query or queue timeout:
        # hold the subscription and retry the read on a backoff timer. This covers a
        # read that returns an error. An event of a type this instance does not know, or
        # of a known type whose deserialize raises or exits (a corrupt payload), is not an
        # error: the read stops before it and the subscription holds there (see `held`).
        # A crash in the serializer's type check, or a throw, still raises in the read.
        hold_catch_up(data, reason)
    end
  end

  def catch_up_from_stream(%SubscriptionState{} = data) do
    next_state(:catching_up, data)
  end

  # A catch-up read of the stream followed: `last_sent` and `last_received` are as they were
  # before the read.
  #
  # The read met an event of a type this instance does not know, or one it could not
  # deserialize: send the events before it, and hold before it once they are drained (the
  # `hold` event).
  defp caught_up(
         %SubscriptionState{} = data,
         events,
         {_event_number, _event_type, _cause} = pending,
         _sent,
         _received
       ) do
    data =
      %SubscriptionState{data | hold_pending: pending}
      |> enqueue_events(events)
      |> notify_subscribers()

    next_state(:catching_up, data)
  end

  defp caught_up(%SubscriptionState{} = data, [], nil, last_sent, last_received) do
    if last_sent == last_received do
      # Subscriber is up-to-date with latest published events
      next_state(:subscribed, data)
    else
      # Need to catch-up with events published while catching up
      next_state(:request_catch_up, data)
    end
  end

  defp caught_up(%SubscriptionState{} = data, events, nil, _last_sent, _last_received) do
    data = data |> enqueue_events(events) |> notify_subscribers()

    if empty_queue?(data) do
      # Request next batch of events
      next_state(:request_catch_up, data)
    else
      # Wait until subscribers have ack'd in-flight events
      next_state(:catching_up, data)
    end
  end

  # Batches the publisher sent with events at or below `last_received`, while subscribed:
  # usually a late notification of events a catch-up read already delivered, ignored. A
  # stream recreated after a hard delete starts again from version 1, so its first batch
  # looks the same: the stream's id is read (one `streams` row, no lock) and, if it is
  # another than the one followed, or there is no stream, the subscription follows it
  # (sparkle-space/sparklespace#8386). A failed read asks for a catch-up read, which reads
  # the id with the events. A subscription that knows no id (`$all`, or a stream subscription
  # whose stream has not been read since it was created) ignores the batch, as before.
  defp past_events(%SubscriptionState{stream_id: nil} = data), do: next_state(:subscribed, data)

  defp past_events(%SubscriptionState{} = data) do
    case read_stream_id(data) do
      {:ok, stream_id} ->
        if stream_changed?(data, stream_id),
          do: follow_stream(data, :notify),
          else: next_state(:subscribed, data)

      {:error, reason} ->
        Logger.debug(
          describe(data) <>
            " did not read its stream's id due to: " <>
            inspect(LogTerms.reduce(reason)) <> ", requesting catch up"
        )

        next_state(:request_catch_up, data)
    end
  end

  # The next events, while subscribed. A stream subscription that knows no stream id (its
  # stream did not exist when it was last read, or has been hard deleted since) reads it
  # first (one `streams` row, no lock), so its position is known to be in that stream; a
  # failed read asks for a catch-up read, which reads the id with the events
  # (sparkle-space/sparklespace#8386). No stream means the events' stream was hard deleted
  # after their append: they are enqueued as before, with no id known, and the next batch
  # reads it again.
  defp expected_events(
         %SubscriptionState{stream_id: nil, stream_uuid: stream_uuid} = data,
         events
       )
       when stream_uuid != @all_stream do
    case read_stream_id(data) do
      {:ok, nil} ->
        enqueue_expected_events(data, events)

      {:ok, stream_id} ->
        enqueue_expected_events(%SubscriptionState{data | stream_id: stream_id}, events)

      {:error, reason} ->
        Logger.debug(
          describe(data) <>
            " did not read its stream's id due to: " <>
            inspect(LogTerms.reduce(reason)) <> ", requesting catch up"
        )

        next_state(:request_catch_up, data)
    end
  end

  defp expected_events(%SubscriptionState{} = data, events),
    do: enqueue_expected_events(data, events)

  defp enqueue_expected_events(%SubscriptionState{} = data, events) do
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

  # The stream's `stream_id` now, `nil` when there is no stream: one `streams` row, read
  # without a lock. `$all` is never hard deleted and is never read for it.
  defp read_stream_id(%SubscriptionState{stream_uuid: @all_stream}), do: {:ok, nil}

  defp read_stream_id(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      stream_uuid: stream_uuid,
      query_timeout: query_timeout
    } = data

    case Storage.stream_info(conn, stream_uuid, schema: schema, timeout: query_timeout) do
      {:ok, %StreamInfo{stream_id: stream_id}} -> {:ok, stream_id}
      {:error, _reason} = error -> error
    end
  catch
    :exit, reason -> {:error, reason}
  end

  # Whether `stream_id`, read now (`nil` for no stream), says the stream followed was hard
  # deleted: it is another id, or none, after one was known.
  defp stream_changed?(%SubscriptionState{stream_id: nil}, _stream_id), do: false

  defp stream_changed?(%SubscriptionState{stream_id: followed}, stream_id),
    do: stream_id != followed

  # Take the id read as the stream followed, when none is known. `$all` never takes one.
  defp adopt_stream_id(%SubscriptionState{stream_uuid: @all_stream} = data, _stream_id), do: data
  defp adopt_stream_id(%SubscriptionState{} = data, nil), do: data

  defp adopt_stream_id(%SubscriptionState{stream_id: nil} = data, stream_id),
    do: %SubscriptionState{data | stream_id: stream_id}

  defp adopt_stream_id(%SubscriptionState{} = data, _stream_id), do: data

  # The stream followed was hard deleted (and maybe created again under its uuid): follow it
  # from its start (sparkle-space/sparklespace#8386).
  defp follow_stream(%SubscriptionState{} = data, trigger) do
    next_state(:request_catch_up, follow_stream_data(data, trigger))
  end

  # In order: alarm with the positions dropped; cancel a pending checkpoint and drop the
  # queue, the tracking of acknowledged events and any pending hold, all of the deleted
  # stream; retire every subscriber's in-flight events (`Subscriber.retire_in_flight/1`:
  # their acks are taken and dropped, and nothing more is sent to a subscriber until it has
  # acknowledged them, so a late ack is never one for a recreated event of the same number);
  # send every subscriber `{:subscription_stream_recreated, subscription}`, ahead of any
  # event of the stream read again, so one that remembers event numbers (an event handler's
  # already-seen check) forgets them; put the row back to 0 (`reset_checkpoint/1`); and
  # reset the cursor, the head check and the id, which the catch-up read that follows takes
  # from the stream it finds. A hard delete and no stream yet: that read finds none and the
  # subscription waits in `subscribed`, at 0, so a recreate is read from its first event.
  defp follow_stream_data(%SubscriptionState{} = data, trigger) do
    stream_recreated(data, trigger)

    data =
      data
      |> cancel_checkpoint_timer()
      |> SubscriptionState.reset_event_tracking()
      |> retire_in_flight_events()
      |> notify_stream_recreated()
      |> reset_checkpoint()

    %SubscriptionState{
      data
      | last_received: 0,
        last_sent: 0,
        last_ack: 0,
        stream_id: nil,
        stream_gone: false,
        head_seen: nil,
        head_repaired: 0,
        catch_up_retry_token: nil
    }
  end

  defp notify_stream_recreated(%SubscriptionState{subscribers: subscribers} = data) do
    for {pid, _subscriber} <- subscribers do
      send(pid, {:subscription_stream_recreated, self()})
    end

    data
  end

  defp retire_in_flight_events(%SubscriptionState{subscribers: subscribers} = data) do
    subscribers =
      Map.new(subscribers, fn {pid, sub} -> {pid, Subscriber.retire_in_flight(sub)} end)

    %SubscriptionState{data | subscribers: subscribers}
  end

  # Put a persistent subscription's row back to 0, by its id, when it is at or below this
  # subscription's last ack in the deleted stream, a position it may have written itself.
  # The hard delete has normally done so already; a hard delete by a release before this one
  # has not. A row above it was written by another holder in the recreated stream: a holder
  # that lost its lock and has not yet heard can still lower a row the new holder wrote at or
  # below that position, which delivers those events again and loses none. A failed write is
  # logged: the row then keeps a position of the deleted stream, as before this fix.
  defp reset_checkpoint(%SubscriptionState{transient: true} = data), do: data
  defp reset_checkpoint(%SubscriptionState{subscription_id: nil} = data), do: data

  defp reset_checkpoint(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      subscription_id: subscription_id,
      last_ack: last_ack,
      query_timeout: query_timeout
    } = data

    case Storage.Subscription.reset_last_seen_by_id(conn, subscription_id, last_ack,
           schema: schema,
           timeout: query_timeout
         ) do
      :ok ->
        data

      {:error, reason} ->
        Logger.warning(
          describe(data) <>
            " did not put its checkpoint back to 0 due to: " <> inspect(LogTerms.reduce(reason))
        )

        data
    end
  catch
    :exit, reason ->
      Logger.warning(
        describe(data) <>
          " did not put its checkpoint back to 0 due to: " <> inspect(LogTerms.reduce(reason))
      )

      data
  end

  # A subscription follows its stream after a hard delete: a warning naming the positions
  # dropped (never an id or any event data) and the
  # `[:decree, :store, :subscription, :stream_recreated]` telemetry event (documented on
  # `c:Decree.Store.subscribe_to_stream/4`).
  defp stream_recreated(%SubscriptionState{} = data, trigger) do
    %SubscriptionState{
      event_store: event_store,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      last_received: last_received,
      last_ack: last_seen,
      transient: transient
    } = data

    Logger.warning(
      describe(data) <>
        " found its stream hard deleted, and maybe created again: it drops its position " <>
        "#{last_seen} (last received #{last_received}) and reads the stream from its start"
    )

    :telemetry.execute(
      [:decree, :store, :subscription, :stream_recreated],
      %{system_time: System.system_time()},
      %{
        event_store: event_store,
        subscription_name: subscription_name,
        stream_uuid: stream_uuid,
        last_seen: last_seen,
        last_received: last_received,
        transient: transient,
        trigger: trigger
      }
    )
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
        inspect(LogTerms.reduce(reason)) <>
        ", attempt #{attempt}, retrying in #{retry_interval}ms"
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
  # `retry_interval`. A failed subscribe attempt, a failed catch-up read and a held
  # subscription's probe each wait `jittered_backoff/2` over it. A `retry_interval` below
  # one second caps it at `retry_interval` from the first failure; only a subscription's
  # own `retry_interval` option can be that short, since the event store's
  # `subscription_retry_interval` is raised to at least one second.
  defp retry_backoff(attempt, retry_interval) do
    first = min(1_000, retry_interval)

    # The exponent is capped so a long run of failures stays a small integer.
    min(first * Integer.pow(2, min(attempt - 1, 32)), retry_interval)
  end

  # Hold before the event of a type this instance does not know (#7804), or of a known type
  # it could not deserialize (#8357), once the events before it are drained. In order:
  # write the checkpoint now, whatever the checkpoint threshold or delay, so the next owner
  # starts at the held event; tell every subscriber the lock is lost (an event handler stays
  # alive and drops its batch); release the lock; drop the event tracking; and alarm.
  # Another instance can now take the lock and deliver the event. A checkpoint write that
  # fails is only logged, as everywhere: the next owner then delivers some events again,
  # and skips none.
  #
  # A transient subscription has no row and no lock, and its position lives in its process:
  # it alarms and stops (`Subscription` stops it under `{:shutdown, {:unknown_event_type,
  # type}}` or `{:shutdown, {:undecodable_event, class}}`), so its subscriber resubscribes
  # from its own position.
  defp hold_subscription(
         %SubscriptionState{hold_pending: {event_number, event_type, cause}} = data
       ) do
    data = data |> cancel_checkpoint_timer() |> persist_checkpoint()

    case data do
      %SubscriptionState{transient: true} ->
        held = %{
          event_number: event_number,
          type: event_type,
          cause: cause,
          attempt: 1,
          wait: nil
        }

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
          cause: cause,
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
          describe(data) <>
            " did not hear back on releasing its lock due to: " <>
            inspect(LogTerms.reduce(reason))
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
  # id); else, held on an unknown type, `:known` or `:unknown` for the held event's type,
  # and held on an undecodable event, `:known` or `{:undecodable, class}` for the event
  # read again; `{:error, reason}` when a read failed. An event no longer there, or a stream
  # deleted or gone, reads as known, so the subscribe that follows finds out what the stream
  # holds now.
  defp probe_held(%SubscriptionState{} = data, held) do
    %{event_number: event_number, cause: cause} = held

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

      {:ok, %Storage.Subscription{}} when cause == :unknown_type ->
        probe_held_type(data, event_number)

      {:ok, %Storage.Subscription{}} ->
        probe_held_event(data, event_number)

      {:error, _reason} = error ->
        error
    end
  catch
    :exit, reason -> {:error, reason}
  end

  # The undecodable held event read again, alone, by the catch-up read's own reader: still
  # undecodable at the same number means held again; decoded, gone, replaced by a later
  # event, or now of an unknown type means subscribe, and the catch-up read that follows
  # delivers it or holds as it finds.
  defp probe_held_event(%SubscriptionState{} = data, event_number) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      serializer: serializer,
      stream_uuid: stream_uuid,
      query_timeout: query_timeout,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = data

    case Stream.read_stream_forward_known(conn, stream_uuid, event_number, 1,
           schema: schema,
           serializer: serializer,
           timeout: query_timeout,
           correlation_id_type: correlation_id_type,
           causation_id_type: causation_id_type
         ) do
      {:ok, _events, {^event_number, _type, {:undecodable, _class} = cause}, _id} -> cause
      {:ok, _events, _pending, _id} -> :known
      {:error, reason} when reason in [:stream_deleted, :stream_not_found] -> :known
      {:error, _reason} = error -> error
    end
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
      {:error, reason} when reason in [:stream_deleted, :stream_not_found] -> :known
      {:error, _reason} = error -> error
    end
  end

  @doc """
  A wait drawn at random after the `attempt`th consecutive failure: an equal jitter over
  the capped doubling backoff (`min(1_000, retry_interval)` after the first failure,
  doubling after each further one, up to `retry_interval`), so that
  `max(1, div(base, 2)) <= wait <= max(1, base)`, where `base <= retry_interval` is that backoff.
  It is never under one millisecond, so nothing that waits it retries in a tight loop.

  A failed catch-up read waits it before the next read, a failed subscribe attempt before
  the next attempt (`resubscribe_wait/1`), and a held subscription before its next probe
  (`held_wait/2`).
  """
  def jittered_backoff(attempt, retry_interval) do
    attempt
    |> retry_backoff(retry_interval)
    |> equal_jitter()
  end

  @doc """
  The wait before the next head check of a subscribed subscription: an equal jitter over
  half of `head_check_interval`, so that
  `max(1, div(interval, 4)) <= wait <= max(1, div(interval, 2))`. It is never under one
  millisecond. Two checks in a row confirm a lost wake-up, so one is repaired within
  `head_check_interval`.
  """
  def head_check_wait(interval), do: equal_jitter(div(interval, 2))

  # The one jitter of the subscription's waits: drawn at random between half of and the
  # whole of `base`, and never under one millisecond.
  defp equal_jitter(base) do
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

  # The alarm of a held subscription: a warning naming the event's number, and its type when
  # the type is unknown or the class of the error its deserialize raised or exited with
  # (`LogTerms.reduce/1`), never its data, and a failed probe's error by its class; and the
  # `[:decree, :store, :subscription, :held]` telemetry event for an unknown type, or
  # `[:decree, :store, :subscription, :held_undecodable]` for an undecodable event (both
  # documented on `c:Decree.Store.subscribe_to_stream/4`). Raised on every hold and on every
  # probe that leaves the subscription held.
  defp alarm(%SubscriptionState{held: held} = data, reason) do
    %SubscriptionState{
      event_store: event_store,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      transient: transient
    } = data

    %{
      event_number: event_number,
      type: event_type,
      cause: cause,
      attempt: attempt,
      wait: wait
    } = held

    next =
      case transient do
        true -> ", stopping"
        false -> ", attempt #{attempt}, probing again in #{wait}ms"
      end

    because =
      case reason do
        nil -> ""
        reason -> " (the probe failed due to: " <> inspect(LogTerms.reduce(reason)) <> ")"
      end

    {why, event, extra} =
      case cause do
        :unknown_type ->
          {"its type #{inspect(event_type)} is not known to this instance",
           [:decree, :store, :subscription, :held], %{}}

        {:undecodable, class} ->
          {"it could not be deserialized due to: " <> inspect(class),
           [:decree, :store, :subscription, :held_undecodable], %{error: class}}
      end

    Logger.warning(
      describe(data) <> " is held before event #{event_number}: " <> why <> because <> next
    )

    :telemetry.execute(
      event,
      %{system_time: System.system_time()},
      Map.merge(
        %{
          event_store: event_store,
          subscription_name: subscription_name,
          stream_uuid: stream_uuid,
          event_type: event_type,
          event_number: event_number,
          retry_interval: wait,
          attempt: attempt,
          transient: transient
        },
        extra
      )
    )
  end

  # The stream's head, its `stream_version` as stored: a stream not yet created reads as 0,
  # with its `stream_id` (`nil` for no stream). One statement, without a lock, and read raw,
  # with no expected-version check. A read that fails, or does not answer, is
  # `{:error, reason}`.
  defp read_head(%SubscriptionState{} = data) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      stream_uuid: stream_uuid,
      query_timeout: query_timeout
    } = data

    case Storage.stream_info(conn, stream_uuid, schema: schema, timeout: query_timeout) do
      {:ok, %StreamInfo{stream_version: head, stream_id: stream_id}} -> {:ok, head, stream_id}
      {:error, _reason} = error -> error
    end
  catch
    :exit, reason -> {:error, reason}
  end

  # A head check confirmed a lost wake-up: a warning naming the positions (never any event
  # data) and the `[:decree, :store, :subscription, :missed_wake_up]` telemetry event
  # (documented on `c:Decree.Store.subscribe_to_stream/4`).
  defp missed_wake_up(%SubscriptionState{} = data, head, seen_head) do
    %SubscriptionState{
      event_store: event_store,
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      last_received: last_received,
      last_ack: last_seen,
      head_check_interval: head_check_interval
    } = data

    Logger.warning(
      describe(data) <>
        " missed a wake-up: the stream's head was #{seen_head} at the last check and is " <>
        "#{head} now, but the last event received is #{last_received} " <>
        "(last acknowledged #{last_seen}), catching up"
    )

    :telemetry.execute(
      [:decree, :store, :subscription, :missed_wake_up],
      %{system_time: System.system_time()},
      %{
        event_store: event_store,
        subscription_name: subscription_name,
        stream_uuid: stream_uuid,
        head: head,
        seen_head: seen_head,
        last_received: last_received,
        last_seen: last_seen,
        head_check_interval: head_check_interval
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

    data = if checkpoints_pending > 0, do: write_checkpoint(data), else: data

    %SubscriptionState{data | checkpoints_pending: 0}
  end

  # Write the position to the subscription's own row, by its `subscription_id`.
  # Keyed by name, a subscription whose row was deleted and created again (with a
  # new id, by an operator reset or another instance) would write its position
  # into the new row, past where that row's subscription starts.
  #
  # A row that no longer exists is only logged: the subscription keeps running,
  # and its position is not written anywhere.
  #
  # A stream subscription that knows its stream's id writes only while that stream exists
  # (sparkle-space/sparklespace#8386): after a hard delete, an ack of an event of the deleted
  # stream writes nothing, so it cannot put back a position the hard delete reset, and sets
  # `stream_gone`, so the subscription follows the stream (`check_stream`). `$all`, and a
  # stream subscription that knows no id (it has acknowledged nothing since), write as before.
  defp write_checkpoint(%SubscriptionState{subscription_id: subscription_id} = data)
       when is_integer(subscription_id) do
    %SubscriptionState{
      conn: conn,
      schema: schema,
      last_ack: last_ack,
      stream_id: stream_id,
      query_timeout: query_timeout
    } = data

    reply =
      case stream_id do
        nil ->
          Storage.Subscription.ack_last_seen_event_by_id(conn, subscription_id, last_ack,
            schema: schema,
            timeout: query_timeout
          )

        stream_id ->
          Storage.Subscription.ack_last_seen_event_in_stream(
            conn,
            subscription_id,
            last_ack,
            stream_id,
            schema: schema,
            timeout: query_timeout
          )
      end

    case reply do
      {:error, :subscription_not_found} ->
        Logger.warning(
          describe(data) <>
            " (subscription_id #{subscription_id}) did not write its checkpoint " <>
            "(event #{last_ack}): its subscription row no longer exists"
        )

        data

      {:error, :stream_changed} ->
        Logger.debug(
          describe(data) <>
            " did not write its checkpoint (event #{last_ack}): its stream was hard deleted"
        )

        %SubscriptionState{data | stream_gone: true}

      _reply ->
        data
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

    data
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
    do: "Subscription #{inspect(name)}@#{inspect(LogTerms.suffix(stream_uuid))}"
end
