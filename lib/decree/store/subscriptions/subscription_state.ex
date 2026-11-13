defmodule Decree.Store.Subscriptions.SubscriptionState do
  @moduledoc false

  alias Decree.Store.RecordedEvent
  alias __MODULE__

  defstruct [
    :conn,
    :event_store,
    :serializer,
    :schema,
    :stream_uuid,
    :start_from,
    :subscription_name,
    :subscription_id,
    :selector,
    :mapper,
    :max_size,
    :partition_by,
    :query_timeout,
    :lock_ref,
    # The full wait before a resubscribe, and the cap of the backoff after a failed
    # subscribe attempt or a catch-up read that returned `{:error, reason}`; and the
    # token of the pending catch-up retry timer (see `SubscriptionFsm`).
    :retry_interval,
    :catch_up_retry_token,
    # The interval of the head check of a subscribed subscription: it reads its stream's
    # head every quarter to half of it (see `SubscriptionFsm`).
    :head_check_interval,
    correlation_id_type: "uuid",
    causation_id_type: "uuid",
    last_received: 0,
    last_sent: 0,
    last_ack: 0,
    queue_size: 0,
    buffer_size: 1,
    checkpoint_after: 0,
    checkpoint_threshold: 1,
    checkpoint_timer_ref: nil,
    checkpoints_pending: 0,
    # Consecutive failed catch-up reads, reset by the next successful read.
    catch_up_failures: 0,
    # Consecutive failed subscribe attempts other than `:lock_already_taken`, reset by
    # the next successful attempt or `:lock_already_taken`.
    subscribe_failures: 0,
    subscribers: %{},
    partitions: %{},
    acknowledged_event_numbers: MapSet.new(),
    in_flight_event_numbers: [],
    transient: false,
    # A catch-up read met an event of a type this instance does not know, as
    # `{event_number, event_type}`: nothing at or after it is read or enqueued, and once
    # the events before it are drained the subscription holds (see `SubscriptionFsm`).
    hold_pending: nil,
    # While held, `%{event_number:, type:, attempt:, wait:}`: the event held before, the
    # number of the alarm (one per hold and per failed probe), and the wait drawn for
    # the next probe.
    held: nil,
    # The stream's head read at the previous head check, `nil` until the first succeeds.
    # A head check repairs only a head seen at two checks in a row, so a wake-up still on
    # its way at the first is not taken for a lost one.
    head_seen: nil,
    # The last head a head check repaired. A head is repaired once: one with no event
    # left to read under it (`$all`'s after a hard delete) stays above `last_received`,
    # which is never raised to it.
    head_repaired: 0
  ]

  def reset_event_tracking(%SubscriptionState{} = state) do
    %SubscriptionState{
      state
      | queue_size: 0,
        partitions: %{},
        acknowledged_event_numbers: MapSet.new(),
        in_flight_event_numbers: [],
        checkpoints_pending: 0,
        hold_pending: nil
    }
  end

  def track_in_flight(%SubscriptionState{} = state, event_number) when is_number(event_number) do
    %SubscriptionState{in_flight_event_numbers: in_flight_event_numbers} = state

    in_flight_event_numbers =
      Enum.sort([event_number | in_flight_event_numbers])
      |> Enum.uniq()

    %SubscriptionState{state | in_flight_event_numbers: in_flight_event_numbers}
  end

  def track_in_flight(%SubscriptionState{} = state, []), do: state

  def track_in_flight(%SubscriptionState{} = state, events) when is_list(events) do
    %SubscriptionState{in_flight_event_numbers: in_flight_event_numbers} = state

    in_flight_event_numbers =
      events
      |> Enum.map(fn %RecordedEvent{event_number: event_number} -> event_number end)
      |> Enum.concat(in_flight_event_numbers)
      |> Enum.sort()
      |> Enum.uniq()

    %SubscriptionState{state | in_flight_event_numbers: in_flight_event_numbers}
  end

  def track_last_sent(%SubscriptionState{} = data, event_number) when is_number(event_number) do
    %SubscriptionState{last_sent: last_sent} = data

    %SubscriptionState{data | last_sent: max(last_sent, event_number)}
  end
end
