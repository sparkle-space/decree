defmodule Decree.Store.Subscriptions.Subscription do
  @moduledoc false

  # Subscription to a single, or all, event streams.
  #
  # A subscription is persistent so that resuming the subscription will continue
  # from the last acknowledged event. This guarantees at least once delivery of
  # every event appended to storage.

  use GenServer, restart: :temporary

  require Logger

  alias Decree.Store.{LogTerms, RecordedEvent}
  alias Decree.Store.Subscriptions.{SubscriptionFsm, Subscription, SubscriptionState}

  defstruct [
    :stream_uuid,
    :subscription_name,
    :subscription,
    # The pending subscribe timer of a parked subscription, as `{ref, due_at, token}`
    # with `due_at` in monotonic milliseconds and `token` the one in its
    # `{:subscribe_to_stream, token}` message, or `nil`. There is at most one.
    :subscribe_timer,
    # The pending head-check timer, as `{ref, token}` with `token` the one in its
    # `{:check_head, token}` message, or `nil`. There is at most one (see
    # `arm_head_check/1`).
    :head_check_timer
  ]

  def start_link(opts) do
    {start_opts, subscription_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    stream_uuid = Keyword.fetch!(subscription_opts, :stream_uuid)
    subscription_name = Keyword.fetch!(subscription_opts, :subscription_name)

    state = %Subscription{
      stream_uuid: stream_uuid,
      subscription_name: subscription_name,
      subscription: SubscriptionFsm.new(stream_uuid, subscription_name, subscription_opts)
    }

    GenServer.start_link(__MODULE__, state, start_opts)
  end

  @doc """
  Connect a subscriber to a started subscription.
  """
  def connect(subscription, subscriber, subscription_opts) do
    GenServer.call(subscription, {:connect, subscriber, subscription_opts})
  end

  @doc """
  Confirm receipt of an event by its event number for a given subscriber.
  """
  def ack(subscription, ack, subscriber) when is_integer(ack) and is_pid(subscriber) do
    GenServer.call(subscription, {:ack, ack, subscriber})
  end

  @doc """
  Confirm receipt of one or more events by:

    * an event number,
    * a list of events; or
    * an `Decree.Store.RecordedEvent` struct.

  """
  def ack(subscription, ack)

  def ack(subscription, ack) when is_integer(ack) do
    GenServer.call(subscription, {:ack, ack, self()})
  end

  def ack(subscription, events) when is_list(events) do
    Subscription.ack(subscription, List.last(events))
  end

  def ack(subscription, %RecordedEvent{} = event) do
    %RecordedEvent{event_number: event_number} = event

    GenServer.call(subscription, {:ack, event_number, self()})
  end

  @doc """
  Unsubscribe a subscriber from the subscription.
  """
  def unsubscribe(subscription) do
    GenServer.call(subscription, {:unsubscribe, self()})
  end

  @doc false
  def last_seen(subscription) do
    GenServer.call(subscription, :last_seen)
  end

  @doc false
  # What `Decree.Store.Subscriptions.local_subscriptions/2` reports for this
  # subscription.
  def info(subscription, timeout) do
    GenServer.call(subscription, :info, timeout)
  end

  @doc false
  @impl GenServer
  def init(%Subscription{} = state) do
    Process.flag(:trap_exit, true)

    {:ok, state}
  end

  # Only the pending timer's message attempts. Its timer is cleared first, so a failed
  # attempt arms the next one.
  @impl true
  def handle_info(
        {:subscribe_to_stream, token},
        %Subscription{subscribe_timer: {_ref, _due_at, token}} = state
      ) do
    Logger.debug(describe(state) <> " subscribe to stream")

    state = cancel_subscribe_timer(state)

    %Subscription{subscription: subscription} = state

    # A held subscription's timer probes rather than subscribes (see `SubscriptionFsm`).
    state =
      case subscription do
        %SubscriptionFsm{state: :held} -> SubscriptionFsm.probe(subscription)
        %SubscriptionFsm{} -> SubscriptionFsm.subscribe(subscription)
      end
      |> apply_subscription_to_state(state)

    noreply(state)
  end

  # A timer that fired before it was cancelled or replaced leaves its message in the
  # mailbox. It is ignored, so it cannot cancel the timer pending now and shorten that
  # wait.
  @impl GenServer
  def handle_info({:subscribe_to_stream, _token}, %Subscription{} = state) do
    {:noreply, state}
  end

  # The pending head-check timer. Its slot is cleared first; a subscribed subscription
  # then checks its stream's head (see `SubscriptionFsm`), and the next pass through
  # `subscribed` arms the next check. In any other state the timer only clears its slot.
  @impl GenServer
  def handle_info({:check_head, token}, %Subscription{head_check_timer: {_ref, token}} = state) do
    state = %Subscription{state | head_check_timer: nil}

    case state do
      %Subscription{subscription: %SubscriptionFsm{state: :subscribed} = subscription} ->
        subscription
        |> SubscriptionFsm.check_head()
        |> apply_subscription_to_state(state)
        |> noreply()

      %Subscription{} ->
        noreply(state)
    end
  end

  # A head-check message whose timer is no longer the pending one.
  @impl GenServer
  def handle_info({:check_head, _token}, %Subscription{} = state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:events, events}, %Subscription{} = state) do
    Logger.debug(describe(state) <> " received #{length(events)} event(s)")

    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.notify_events(events)
      |> apply_subscription_to_state(state)

    noreply(state)
  end

  # The publisher stopped a batch before an event of a type this instance does not know.
  @impl GenServer
  def handle_info({:events_pending, event_number}, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.notify_pending(event_number)
      |> apply_subscription_to_state(state)

    noreply(state)
  end

  @impl GenServer
  def handle_info(:checkpoint, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.checkpoint()
      |> apply_subscription_to_state(state)

    noreply(state)
  end

  # The retry timer armed when a catch-up read failed.
  @impl GenServer
  def handle_info({:retry_catch_up, token}, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.retry_catch_up(token)
      |> apply_subscription_to_state(state)

    noreply(state)
  end

  @impl GenServer
  def handle_info(
        {Decree.Store.AdvisoryLocks, :lock_released, lock_ref, reason},
        %Subscription{} = state
      ) do
    %Subscription{subscription: subscription} = state

    Logger.debug(
      describe(state) <> " advisory lock lost due to: " <> inspect(LogTerms.reduce(reason))
    )

    state =
      subscription
      |> SubscriptionFsm.disconnect(lock_ref)
      |> apply_subscription_to_state(state)

    noreply(state)
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, reason}, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    Logger.debug(
      describe(state) <>
        " subscriber #{inspect(pid)} down due to: " <> inspect(LogTerms.reduce(reason))
    )

    state =
      subscription
      |> SubscriptionFsm.unsubscribe(pid)
      |> apply_subscription_to_state(state)

    if unsubscribed?(state) do
      {:stop, reason, state}
    else
      noreply(state)
    end
  end

  @impl GenServer
  def handle_info({:EXIT, _from, reason}, %Subscription{} = state) do
    {:stop, reason, state}
  end

  @impl GenServer
  def handle_cast(:catch_up, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.catch_up()
      |> apply_subscription_to_state(state)

    noreply(state)
  end

  @impl GenServer
  def handle_call({:ack, ack, subscriber}, _from, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    {reply, subscription} =
      case SubscriptionFsm.ack(subscription, ack, subscriber) do
        {reply, subscription} -> {reply, subscription}
        subscription -> {:ok, subscription}
      end

    state = apply_subscription_to_state(subscription, state)

    reply(reply, state)
  end

  @impl GenServer
  def handle_call({:connect, subscriber, opts}, _from, %Subscription{} = state) do
    %Subscription{
      subscription:
        %SubscriptionFsm{data: %SubscriptionState{subscribers: subscribers}} = subscription
    } = state

    Logger.debug(describe(state) <> " attempting to connect subscriber " <> inspect(subscriber))

    with :ok <- ensure_not_already_subscribed(subscribers, subscriber),
         :ok <- ensure_within_concurrency_limit(subscribers, opts) do
      state =
        subscription
        |> SubscriptionFsm.connect_subscriber(subscriber, opts)
        |> SubscriptionFsm.subscribe()
        |> apply_subscription_to_state(state)

      reply({:ok, self()}, state)
    else
      {:error, _error} = reply ->
        {:reply, reply, state}
    end
  end

  @impl GenServer
  def handle_call({:unsubscribe, pid}, _from, %Subscription{} = state) do
    %Subscription{subscription: subscription} = state

    state =
      subscription
      |> SubscriptionFsm.unsubscribe(pid)
      |> apply_subscription_to_state(state)

    if unsubscribed?(state) do
      {:stop, :shutdown, :ok, state}
    else
      reply(:ok, state)
    end
  end

  @impl GenServer
  def handle_call(:last_seen, _from, %Subscription{subscription: subscription} = state) do
    %SubscriptionFsm{data: %SubscriptionState{last_ack: last_seen}} = subscription

    {:reply, last_seen, state}
  end

  @impl GenServer
  def handle_call(:info, _from, %Subscription{subscription: subscription} = state) do
    %SubscriptionFsm{state: fsm_state, data: %SubscriptionState{} = data} = subscription

    info = %{
      subscription_name: data.subscription_name,
      stream_uuid: data.stream_uuid,
      subscription_id: data.subscription_id,
      pid: self(),
      transient: data.transient,
      status: status(fsm_state),
      last_seen: data.last_ack
    }

    {:reply, info, state}
  end

  @impl GenServer
  def terminate(_reason, state) do
    %Subscription{subscription: subscription} = state

    # Checkpoint subscription if needed before terminating
    SubscriptionFsm.checkpoint(subscription)

    state
  end

  # A subscription no longer parked (or held) cancels its pending timer: left to run, it
  # would shorten the full wait after a lock lost before it fires.
  #
  # Every event is followed by the FSM's `hold` check, so a subscription holds before an
  # event of an unknown type, or one it could not deserialize, as soon as the events before
  # it are drained, whichever event drained them (an ack, a read, a filtered batch). Then by
  # its `check_stream` check, so a subscription whose checkpoint write found its stream hard
  # deleted follows the stream at once (sparkle-space/sparklespace#8386).
  defp apply_subscription_to_state(%SubscriptionFsm{} = subscription, %Subscription{} = state) do
    subscription = subscription |> SubscriptionFsm.hold() |> SubscriptionFsm.check_stream()

    %SubscriptionFsm{state: fsm} = subscription

    state =
      if fsm in [:initial, :disconnected, :held],
        do: state,
        else: cancel_subscribe_timer(state)

    %Subscription{state | subscription: subscription}
    |> handle_subscription_state()
    |> arm_head_check()
  end

  # A subscribed subscription with no head check pending arms one, after a wait drawn by
  # `SubscriptionFsm.head_check_wait/1`. A pending one is never reset: this runs after
  # every event, so resetting it would put off a busy subscription's check indefinitely.
  defp arm_head_check(
         %Subscription{
           head_check_timer: nil,
           subscription: %SubscriptionFsm{state: :subscribed, data: data}
         } = state
       ) do
    %SubscriptionState{head_check_interval: interval} = data

    token = make_ref()

    ref =
      Process.send_after(self(), {:check_head, token}, SubscriptionFsm.head_check_wait(interval))

    %Subscription{state | head_check_timer: {ref, token}}
  end

  defp arm_head_check(%Subscription{} = state), do: state

  # A held transient subscription stops (see `noreply/1`); it has nothing to probe.
  defp handle_subscription_state(
         %Subscription{
           subscription: %SubscriptionFsm{state: :held, data: %SubscriptionState{transient: true}}
         } = state
       ) do
    state
  end

  # Attempt to subscribe to an initial or disconnected subscription, or probe a held one,
  # after the wait the FSM gives. Every pass through a parked state comes here (a
  # subscriber connecting, an ack, events), so a timer is armed only when none is pending
  # or when it is due sooner. A held subscription's wait is drawn once per hold or probe,
  # and a failed subscribe attempt's once per attempt, and kept in its state, so a later
  # pass does not shorten it.
  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: fsm, data: data}} = state
       )
       when fsm in [:initial, :disconnected, :held] do
    wait = SubscriptionFsm.resubscribe_wait(data)
    due_at = System.monotonic_time(:millisecond) + wait

    case state do
      %Subscription{subscribe_timer: {_ref, pending_due_at, _token}}
      when pending_due_at <= due_at ->
        state

      %Subscription{} ->
        state = cancel_subscribe_timer(state)
        token = make_ref()
        ref = Process.send_after(self(), {:subscribe_to_stream, token}, wait)

        %Subscription{state | subscribe_timer: {ref, due_at, token}}
    end
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :request_catch_up}} = state
       ) do
    Logger.debug(describe(state) <> " catching-up")

    :ok = GenServer.cast(self(), :catch_up)

    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :max_capacity}} = state
       ) do
    state
  end

  # A failed catch-up read is retried by the timer the FSM armed, not by a cast.
  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :catch_up_retry}} = state
       ) do
    state
  end

  defp handle_subscription_state(
         %Subscription{subscription: %SubscriptionFsm{state: :unsubscribed}} = state
       ) do
    Logger.debug(describe(state) <> " has no subscribers, shutting down")

    state
  end

  # No-op for all other subscription states.
  defp handle_subscription_state(%Subscription{} = state), do: state

  defp cancel_subscribe_timer(%Subscription{subscribe_timer: nil} = state), do: state

  defp cancel_subscribe_timer(%Subscription{subscribe_timer: {ref, _due_at, _token}} = state) do
    _result = Process.cancel_timer(ref)

    %Subscription{state | subscribe_timer: nil}
  end

  # Prevent duplicate subscriptions from same process.
  defp ensure_not_already_subscribed(subscribers, pid) do
    unless Map.has_key?(subscribers, pid) do
      :ok
    else
      {:error, :already_subscribed}
    end
  end

  # Prevent more subscribers than requested concurrency limit.
  defp ensure_within_concurrency_limit(subscribers, opts) do
    concurrency_limit = Keyword.get(opts, :concurrency_limit, 1)

    if map_size(subscribers) < concurrency_limit do
      :ok
    else
      {:error, :too_many_subscribers}
    end
  end

  # Parked: not yet subscribed, or lost its advisory lock and waiting to take it
  # again. Every state past those holds the lock (a persistent subscription) and
  # delivers events.
  defp status(:unsubscribed), do: :unsubscribed
  # Held before an event of an unknown type, or one it could not deserialize: its lock
  # released, it delivers nothing.
  defp status(fsm_state) when fsm_state in [:initial, :disconnected, :held], do: :parked
  defp status(_fsm_state), do: :subscribed

  def unsubscribed?(%Subscription{subscription: %SubscriptionFsm{state: :unsubscribed}}), do: true
  def unsubscribed?(%Subscription{}), do: false

  # A transient subscription held before an event of a type this instance does not know, or
  # one it could not deserialize, stops: it has no lock to release for another instance,
  # and its position lives in this process, so its subscriber resubscribes from its own
  # position.
  defp noreply(%Subscription{} = state) do
    case stop_reason(state) do
      nil -> {:noreply, state}
      reason -> {:stop, reason, state}
    end
  end

  defp reply(reply, %Subscription{} = state) do
    case stop_reason(state) do
      nil -> {:reply, reply, state}
      reason -> {:stop, reason, reply, state}
    end
  end

  defp stop_reason(%Subscription{
         subscription: %SubscriptionFsm{
           state: :held,
           data: %SubscriptionState{transient: true, held: %{type: type, cause: :unknown_type}}
         }
       }),
       do: {:shutdown, {:unknown_event_type, type}}

  defp stop_reason(%Subscription{
         subscription: %SubscriptionFsm{
           state: :held,
           data: %SubscriptionState{transient: true, held: %{cause: {:undecodable, class}}}
         }
       }),
       do: {:shutdown, {:undecodable_event, class}}

  defp stop_reason(%Subscription{}), do: nil

  defp describe(%Subscription{stream_uuid: stream_uuid, subscription_name: name}),
    do: "Subscription #{inspect(name)}@#{inspect(LogTerms.suffix(stream_uuid))}"
end
