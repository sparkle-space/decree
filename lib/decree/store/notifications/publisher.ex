defmodule Decree.Store.Notifications.Publisher do
  @moduledoc """
  A GenStage consumer which subscribes to a `Decree.Store.Notifications.Listener` and receives
  `Decree.Store.Notifications.Notification` events, reading the corresponding range of events from
  storage, deserializing them, and broadcasting them to PubSub subscribers for the relevant storage
  stream.
  """

  use GenStage

  require Logger

  alias Decree.Store.{LogTerms, PubSub, RecordedEvent, Storage}
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
  # read on and hold before it. Nothing raises, so the notifications tree is not restarted.
  #
  # The cut holds within the notification only: a later notification's events are broadcast
  # as usual. A subscription process sees the gap (it expects `last_received + 1`) and reads
  # from storage, where it holds; an aggregate stops on the gap; a plain `subscribe/2`
  # subscriber checks continuity itself.
  #
  # A type provider that crashes while asked whether it knows a type, with an exception other
  # than the ones it reads as an unknown type, is not an unknown type: the crash raises, as
  # decree 772c21f ruled, and the publisher goes down. The question is asked outside any rescue.
  #
  # Any other event of a known type that fails to deserialize, such as a corrupt payload,
  # raises in the serializer. Events are deserialized one at a time, in order, and a raise is
  # caught around each known event's deserialize only: the batch is cut at that event, as at
  # an unknown type, and the events before it are broadcast. Whichever of the two cuts comes
  # first wins. The cut is logged at `:error` by the exception's module, the event's number
  # and the stream's suffix, never the message, which can carry the payload's bytes: a raise
  # would crash the publisher, restart the notifications tree (`one_for_all`) and, past its
  # restart intensity, the store's whole tree (sparkle-space/sparklespace#8349). No position
  # notice is sent: a subscription process would take it for an unknown type and hold.
  #
  # A subscription recovers the rest from storage: its next notification shows a gap
  # (`last_received + 1`), and while `subscribed` a head its head check saw and still has not
  # received at the next check starts a catch-up read (`SubscriptionFsm`). That read meets the
  # same event and raises in the subscription process, which is `:temporary`. An aggregate or
  # a plain `subscribe/2` subscriber is sent nothing for the event that failed and the events
  # after it in that notification until a later append shows the gap; an aggregate's next
  # append then fails closed on `:wrong_expected_version`, and its rebuild raises on the event.
  #
  # A storage error is answered as `{:error, reason}`, and an exit or a raise in the storage
  # read is caught: each skips the notification, logged by class. The read raises when a row
  # cannot be adapted into a `RecordedEvent`, as a text correlation or causation id read by a
  # store configured for uuid ids; its message can carry the row's value, and crashing would
  # restart the notifications tree as above. The rescue covers that read only, not the
  # deserialize, which is cut per event.

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

    read =
      try do
        Storage.read_stream_forward(conn, stream_id, from_stream_version, count,
          schema: schema,
          timeout: query_timeout,
          correlation_id_type: correlation_id_type,
          causation_id_type: causation_id_type
        )
      rescue
        exception -> {:error, exception}
      catch
        :exit, reason -> {:error, reason}
      end

    case read do
      {:ok, events} ->
        {deserialized_events, pending} = deserialize(events, stream_uuid, serializer, [])

        {stream_uuid, deserialized_events, pending}

      {:error, error} ->
        failed_to_read(error)
    end
  end

  # One event at a time: an unknown type is the cut at that event, with its position notice,
  # and so is a raise in a known event's deserialize; the events before it stand. Whether the
  # type is known is asked outside the rescue, so a type provider's crash raises.
  defp deserialize([], _stream_uuid, _serializer, acc), do: {Enum.reverse(acc), nil}

  defp deserialize([event | events], stream_uuid, serializer, acc) do
    %RecordedEvent{event_number: event_number, event_type: event_type} = event

    if RecordedEvent.known_type?(serializer, event_type) do
      case deserialize_one(event, serializer) do
        {:ok, deserialized} ->
          deserialize(events, stream_uuid, serializer, [deserialized | acc])

        {:raised, exception} ->
          failed_to_deserialize(stream_uuid, event, exception)

          {Enum.reverse(acc), nil}
      end
    else
      {Enum.reverse(acc), {event_number, event_type}}
    end
  end

  defp deserialize_one(event, serializer) do
    {:ok, RecordedEvent.deserialize(event, serializer)}
  rescue
    exception -> {:raised, exception}
  end

  defp failed_to_deserialize(stream_uuid, %RecordedEvent{event_number: event_number}, exception) do
    Logger.error(
      "Decree.Store notifications failed to deserialize event #{event_number} of " <>
        inspect(LogTerms.suffix(stream_uuid)) <>
        " due to: " <>
        inspect(LogTerms.reduce(exception)) <> ", broadcasting the events before it"
    )
  end

  defp failed_to_read(reason) do
    Logger.error(
      "Decree.Store notifications failed to read events due to: " <>
        inspect(LogTerms.reduce(reason))
    )

    nil
  end

  defp broadcast(event_store, stream_uuid, events, pending) do
    if events != [], do: PubSub.broadcast(event_store, stream_uuid, {:events, events})

    case pending do
      nil ->
        :ok

      {event_number, event_type} ->
        Logger.warning(
          "Decree.Store notifications stopped a batch of #{inspect(LogTerms.suffix(stream_uuid))} at event " <>
            "#{event_number}: its type #{inspect(event_type)} is not known to this instance"
        )

        PubSub.broadcast(event_store, stream_uuid, {:events_pending, event_number})
    end
  end
end
