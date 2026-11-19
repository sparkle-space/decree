defmodule Decree.Store.Notifications.Listener do
  @moduledoc """
  A GenStage producer which listens for event stream notifications from PostgreSQL using the
  PostgreSQL `LISTEN/NOTIFY` mechanism and emits `Decree.Store.Notifications.Notification` events
  for subscribed `Decree.Store.Notifications.Publisher` processes.

  The notification payload contains the first and last event number of the appended events.
  """

  use GenStage

  require Logger

  alias Decree.Store.LogTerms
  alias Decree.Store.Notifications.{Connection, Listener, Notification}

  defstruct [:listen_to, :query_timeout, demand: 0, queue: :queue.new()]

  def start_link(opts) do
    {start_opts, listener_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    listen_to = Keyword.fetch!(listener_opts, :listen_to)
    query_timeout = Keyword.fetch!(listener_opts, :query_timeout)

    state = %Listener{listen_to: listen_to, query_timeout: query_timeout}

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  def init(%Listener{} = state) do
    {:producer, listen_for_events(state)}
  end

  # Notification received from PostgreSQL's `NOTIFY`
  def handle_info({:notification, _connection_pid, _ref, channel, payload}, %Listener{} = state) do
    notification = Notification.new(payload)

    Logger.debug(
      "Listener received notification on channel " <>
        inspect(channel) <> " with payload: " <> inspect(describe(notification))
    )

    state = enqueue(notification, state)

    dispatch_events([], state)
  end

  def handle_demand(incoming_demand, %Listener{} = state) do
    %Listener{demand: pending_demand} = state

    state = %Listener{state | demand: pending_demand + incoming_demand}

    dispatch_events([], state)
  end

  # The connection listens on every connect by itself once this process is registered, so
  # neither answer nor an exit from the call (a connection that is down, or restarting) needs
  # handling here.
  defp listen_for_events(%Listener{} = state) do
    %Listener{listen_to: listen_to, query_timeout: query_timeout} = state

    try do
      Connection.listen(listen_to, query_timeout)
    catch
      :exit, _reason -> :ok
    end

    state
  end

  defp dispatch_events(events, %Listener{demand: 0} = state) do
    {:noreply, Enum.reverse(events), state}
  end

  defp dispatch_events(events, %Listener{} = state) do
    %Listener{demand: demand, queue: queue} = state

    case :queue.out(queue) do
      {{:value, event}, queue} ->
        state = %Listener{state | demand: max(demand - 1, 0), queue: queue}
        dispatch_events([event | events], state)

      {:empty, _queue} ->
        {:noreply, Enum.reverse(events), state}
    end
  end

  # The payload as it came, with the stream's id reduced to its suffix: the id can be a
  # user's own.
  defp describe(%Notification{} = notification) do
    %Notification{
      stream_uuid: stream_uuid,
      stream_id: stream_id,
      from_stream_version: from_stream_version,
      to_stream_version: to_stream_version
    } = notification

    Enum.join(
      [LogTerms.suffix(stream_uuid), stream_id, from_stream_version, to_stream_version],
      ","
    )
  end

  defp enqueue(%Notification{} = notification, %Listener{} = state) do
    %Listener{queue: queue} = state

    %Listener{state | queue: :queue.in(notification, queue)}
  end
end
