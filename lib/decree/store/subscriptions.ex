defmodule Decree.Store.Subscriptions do
  @moduledoc false

  alias Decree.Store.{AdvisoryLocks, Storage}
  alias Decree.Store.Subscriptions.Subscription
  alias Decree.Store.Subscriptions.Supervisor, as: SubscriptionsSupervisor

  def subscribe_to_stream(subscriber, opts \\ []) do
    case SubscriptionsSupervisor.start_subscription(opts) do
      {:ok, subscription} ->
        Subscription.connect(subscription, subscriber, opts)

      {:error, {:already_started, subscription}} ->
        case Keyword.get(opts, :concurrency_limit) do
          nil ->
            {:error, :subscription_already_exists}

          concurrency_limit when is_number(concurrency_limit) ->
            Subscription.connect(subscription, subscriber, opts)
        end
    end
  end

  defdelegate unsubscribe_from_stream(event_store, stream_uuid, name), to: SubscriptionsSupervisor
  defdelegate stop_subscription(event_store, stream_uuid, name), to: SubscriptionsSupervisor
  defdelegate stop_subscription(event_store, subscription), to: SubscriptionsSupervisor

  @doc """
  The subscriptions running in the event store instance `event_store`, each read
  with a bounded wait of `timeout` milliseconds, as `{infos, omitted}`.

  The reads run concurrently. A subscription that does not answer in time is
  left out of `infos` and counted in `omitted`; a subscription that stops
  meanwhile is gone, so it is left out and not counted.
  """
  def local_subscriptions(event_store, timeout) do
    supervisor = Module.concat(event_store, SubscriptionsSupervisor)

    pids =
      for {_id, pid, _type, _modules} <- DynamicSupervisor.which_children(supervisor),
          is_pid(pid),
          do: pid

    pids
    |> Task.async_stream(&subscription_info/1,
      timeout: timeout,
      on_timeout: :kill_task,
      max_concurrency: max(length(pids), 1)
    )
    |> Enum.reduce({[], 0}, fn
      {:ok, {:ok, info}}, {infos, omitted} -> {[info | infos], omitted}
      # The subscription stopped during its read.
      {:ok, :error}, acc -> acc
      {:exit, :timeout}, {infos, omitted} -> {infos, omitted + 1}
    end)
    |> then(fn {infos, omitted} -> {Enum.reverse(infos), omitted} end)
  end

  defp subscription_info(pid) do
    {:ok, Subscription.info(pid, :infinity)}
  catch
    :exit, _reason -> :error
  end

  @doc """
  For each subscription id, whether its `subscriptions` row exists and whether the
  event store instance's own lock session holds its advisory lock.
  """
  def subscription_locks(event_store, subscription_ids, timeout) do
    AdvisoryLocks.subscription_locks(
      Module.concat(event_store, AdvisoryLocks),
      subscription_ids,
      timeout
    )
  end

  defdelegate delete_subscription(conn, stream_uuid, subscription_name, opts), to: Storage

  @doc """
  Get the delay between subscription retry attempts, in milliseconds, from the
  event store config.

  A subscription waits this long before it retries subscribing after losing its
  advisory lock or finding it taken by another instance. It is also the cap of the
  backoff after any other failed subscribe attempt, or a catch-up read that returned an
  error: the backoff is one second after the first failure, doubling after each
  further consecutive one up to this interval. A failed subscribe attempt and a failed
  catch-up read each wait a time drawn at random between half of and the whole of it,
  never under one millisecond. (Only a subscription's own
  `retry_interval` option can be shorter than one second, which caps the backoff at
  that interval from the first failure.)

  The default value is one minute and the minimum allowed value is one second.
  """
  def retry_interval(event_store, config) do
    case Keyword.get(config, :subscription_retry_interval) do
      interval when is_integer(interval) and interval > 0 ->
        # Ensure interval is no less than one second
        max(interval, 1_000)

      nil ->
        # Default to 60 seconds when not configured
        60_000

      invalid ->
        raise ArgumentError,
          message:
            "Invalid `:subscription_retry_interval` setting in " <>
              inspect(event_store) <>
              " config. Expected an integer in milliseconds but got: " <> inspect(invalid)
    end
  end

  # The longest timer `Process.send_after/3` can arm (2^32 - 1 ms). See
  # `max_head_check_interval/0`.
  @max_head_check_interval 4_294_967_295

  @doc """
  Get the head-check interval, in milliseconds, from the event store config
  (`subscription_head_check_interval`).

  A subscription that is subscribed, waiting for the store to wake it with new events,
  reads its stream's head (its `stream_version`) every quarter to half of this interval,
  drawn at random per check. A head above the last event it received, seen at one check
  and still above it at the next, is a missed wake-up: the subscription catches up from
  storage. So a lost wake-up is repaired within this interval (see "Missed wake-ups" in
  `c:Decree.Store.subscribe_to_stream/4`). A subscription's own `head_check_interval`
  option overrides it, and only that option can be shorter than one second.

  The default value is one minute, the minimum allowed value is one second and the
  maximum is `max_head_check_interval/0` (4_294_967_295 ms, about 49.7 days); a larger
  value raises an `ArgumentError`, like an invalid one.
  """
  def head_check_interval(event_store, config) do
    case Keyword.get(config, :subscription_head_check_interval) do
      interval
      when is_integer(interval) and interval > 0 and interval <= @max_head_check_interval ->
        # Ensure interval is no less than one second
        max(interval, 1_000)

      nil ->
        # Default to 60 seconds when not configured
        60_000

      invalid ->
        raise ArgumentError,
          message:
            "Invalid `:subscription_head_check_interval` setting in " <>
              inspect(event_store) <>
              " config. Expected a positive integer in milliseconds, at most " <>
              Integer.to_string(@max_head_check_interval) <> ", but got: " <> inspect(invalid)
    end
  end

  @doc """
  The largest head-check interval, in milliseconds, that the event store's
  `subscription_head_check_interval` setting or a subscription's own
  `head_check_interval` option may take: 4_294_967_295, the longest timer
  `Process.send_after/3` can arm. A head check waits at most half the interval, so any
  interval up to this maximum arms.
  """
  def max_head_check_interval, do: @max_head_check_interval

  @doc """
  Get the inactivity period, in milliseconds, after which a subscription process
  will be automatically hibernated.

  From Erlang/OTP 20, subscription processes will automatically hibernate to
  save memory after `15_000` milliseconds of inactivity. This can be changed by
  configuring the `:subscription_hibernate_after` option for the event store
  module.

  You can also set it to `:infinity` to fully disable it.
  """
  def hibernate_after(event_store, config) do
    case Keyword.get(config, :subscription_hibernate_after) do
      interval when is_integer(interval) and interval >= 0 ->
        interval

      :infinity ->
        :infinity

      nil ->
        # Default to 15 seconds when not configured
        15_000

      invalid ->
        raise ArgumentError,
          message:
            "Invalid `:subscription_hibernate_after` setting in " <>
              inspect(event_store) <>
              " config. Expected an integer (in milliseconds) or `:infinity` but got: " <>
              inspect(invalid)
    end
  end
end
