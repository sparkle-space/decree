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
  # read on and hold before it. Nothing in that cut raises, so the notifications tree is not
  # restarted; the one deliberate raise is a type provider's crash, below.
  #
  # The cut holds within the notification only: a later notification's events are broadcast
  # as usual. A subscription process sees the gap (it expects `last_received + 1`) and reads
  # from storage, where it holds; an aggregate stops on the gap; a plain `subscribe/2`
  # subscriber checks continuity itself.
  #
  # A type provider that crashes while asked whether it knows a type, with an exception other
  # than the ones it reads as an unknown type, is not an unknown type: the crash raises, as
  # decree 772c21f ruled, and the publisher goes down. The question is asked outside any
  # rescue; only an exit from it is caught, below.
  #
  # Any other event of a known type that fails to deserialize, such as a corrupt payload,
  # raises in the serializer. Events are deserialized one at a time, in order, and a raise is
  # caught around each known event's deserialize only: the batch is cut at that event, as at
  # an unknown type, and the events before it are broadcast. Whichever of the two cuts comes
  # first wins. An exit, as from a serializer or a dependency calling a dead or slow process,
  # is the same cut whether it comes from the type question or from the deserialize. The cut
  # is logged at `:error` by the exception's module or the exit's class, the event's number
  # and the stream's suffix, never the message or the exit's arguments, which can carry the
  # payload's bytes: a raise or an exit would crash the publisher, restart the notifications
  # tree (`one_for_all`) and, past its restart intensity, the store's whole tree
  # (sparkle-space/sparklespace#8349). No position notice is sent: a subscription process
  # would take it for an unknown type and hold. A throw is not caught at either step.
  #
  # A subscription recovers the rest from storage: its next notification shows a gap
  # (`last_received + 1`), and while `subscribed` a head its head check saw and still has not
  # received at the next check starts a catch-up read (`SubscriptionFsm`). That read meets the
  # same event and holds before it (sparkle-space/sparklespace#8357). An aggregate or
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
  # and so is a raise or an exit in a known event's deserialize, or an exit from the type
  # question; the events before it stand. Whether the type is known is asked once, outside
  # any rescue, so a type provider's crash raises. The answer and the exit are tagged apart,
  # so an answer of any shape is data: every answer but `false` or `nil` is a known type.
  defp deserialize([], _stream_uuid, _serializer, acc), do: {Enum.reverse(acc), nil}

  defp deserialize([event | events], stream_uuid, serializer, acc) do
    %RecordedEvent{event_number: event_number, event_type: event_type} = event

    case known_type(serializer, event_type) do
      {:exited, reason} ->
        failed_to_deserialize(stream_uuid, event, reason)

        {Enum.reverse(acc), nil}

      {:returned, unknown} when unknown in [false, nil] ->
        {Enum.reverse(acc), {event_number, event_type}}

      {:returned, _known} ->
        case deserialize_one(event, serializer) do
          {:ok, deserialized} ->
            deserialize(events, stream_uuid, serializer, [deserialized | acc])

          {:failed, reason} ->
            failed_to_deserialize(stream_uuid, event, reason)

            {Enum.reverse(acc), nil}
        end
    end
  end

  defp known_type(serializer, event_type) do
    {:returned, RecordedEvent.known_type?(serializer, event_type)}
  catch
    :exit, reason -> {:exited, reason}
  end

  defp deserialize_one(event, serializer) do
    {:ok, RecordedEvent.deserialize_known_type(event, serializer)}
  rescue
    exception -> {:failed, exception}
  catch
    :exit, reason -> {:failed, reason}
  end

  defp failed_to_deserialize(stream_uuid, %RecordedEvent{event_number: event_number}, reason) do
    Logger.error(
      "Decree.Store notifications failed to deserialize event #{event_number} of " <>
        inspect(LogTerms.suffix(stream_uuid)) <>
        " due to: " <>
        inspect(LogTerms.reduce(reason)) <> ", broadcasting the events before it"
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
