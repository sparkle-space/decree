defmodule Decree.Store.Notifications.Publisher do
  @moduledoc """
  A GenStage consumer which subscribes to a `Decree.Store.Notifications.Listener` and receives
  `Decree.Store.Notifications.Notification` events, reading the corresponding range of events from
  storage, deserializing them, and broadcasting them to PubSub subscribers for the relevant storage
  stream.
  """

  use GenStage

  require Logger

  alias Decree.Store.{PubSub, RecordedEvent, Storage}
  alias Decree.Store.Notifications.Notification

  defmodule State do
    defstruct [
      :conn,
      :event_store,
      :query_timeout,
      :schema,
      :serializer,
      :subscribe_to,
      correlation_id_type: "uuid",
      causation_id_type: "uuid"
    ]

    def new(opts) do
      %State{
        conn: Keyword.fetch!(opts, :conn),
        event_store: Keyword.fetch!(opts, :event_store),
        query_timeout: Keyword.fetch!(opts, :query_timeout),
        schema: Keyword.fetch!(opts, :schema),
        serializer: Keyword.fetch!(opts, :serializer),
        subscribe_to: Keyword.fetch!(opts, :subscribe_to),
        correlation_id_type: Keyword.get(opts, :correlation_id_type, "uuid"),
        causation_id_type: Keyword.get(opts, :causation_id_type, "uuid")
      }
    end
  end

  def start_link(opts) do
    {start_opts, reader_opts} =
      Keyword.split(opts, [:name, :timeout, :debug, :spawn_opt, :hibernate_after])

    state = State.new(reader_opts)

    GenStage.start_link(__MODULE__, state, start_opts)
  end

  # Starts a permanent subscription to the listener producer stage which will
  # automatically start requesting items.
  def init(%State{} = state) do
    %State{subscribe_to: subscribe_to} = state

    {:consumer, state, [subscribe_to: [{subscribe_to, max_demand: 1}]]}
  end

  # Fetch events from storage and pass onwards to subscribers
  def handle_events(events, _from, state) do
    %State{event_store: event_store} = state

    events
    |> Stream.map(&read_events(&1, state))
    |> Stream.reject(&is_nil/1)
    |> Enum.each(fn {stream_uuid, batch, pending} ->
      broadcast(event_store, stream_uuid, batch, pending)
    end)

    {:noreply, [], state}
  end

  # Each notification's range is read and published on its own. A range that reaches an event
  # whose type this instance does not know is cut there: the events before it are broadcast,
  # and the subscription processes are sent a position notice for it (see `PubSub`), so they
  # read on and hold before it. Nothing at or after it is broadcast, so no subscriber skips it,
  # and nothing raises, so the notifications tree is not restarted.

  defp read_events(%Notification{} = notification, %State{} = state) do
    %Notification{
      stream_uuid: stream_uuid,
      stream_id: stream_id,
      from_stream_version: from_stream_version,
      to_stream_version: to_stream_version
    } = notification

    %State{
      conn: conn,
      query_timeout: query_timeout,
      schema: schema,
      serializer: serializer,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = state

    count = to_stream_version - from_stream_version + 1

    try do
      case Storage.read_stream_forward(conn, stream_id, from_stream_version, count,
             schema: schema,
             timeout: query_timeout,
             correlation_id_type: correlation_id_type,
             causation_id_type: causation_id_type
           ) do
        {:ok, events} ->
          {deserialized_events, pending} = RecordedEvent.deserialize_known(events, serializer)

          {stream_uuid, deserialized_events, pending}

        {:error, error} ->
          Logger.error(
            "Decree.Store notifications failed to read events due to: " <> inspect(error)
          )

          nil
      end
    catch
      :exit, ex ->
        Logger.error("Decree.Store notifications failed to read events due to: " <> inspect(ex))
        nil
    end
  end

  defp broadcast(event_store, stream_uuid, events, pending) do
    if events != [], do: PubSub.broadcast(event_store, stream_uuid, {:events, events})

    case pending do
      nil ->
        :ok

      {event_number, event_type} ->
        Logger.warning(
          "Decree.Store notifications stopped a batch of #{inspect(stream_uuid)} at event " <>
            "#{event_number}: its type #{inspect(event_type)} is not known to this instance"
        )

        PubSub.broadcast(event_store, stream_uuid, {:events_pending, event_number})
    end
  end
end
