defmodule Decree.Store.Subscriptions.Subscriber do
  @moduledoc false

  defstruct [
    :pid,
    :ref,
    :partition_key,
    last_sent: 0,
    buffer_size: 1,
    in_flight: [],
    # The numbers of events sent before the subscription followed its stream after a hard
    # delete, newest first, and not yet acknowledged (sparkle-space/sparklespace#8386).
    # They are no longer in flight: their acks are taken and dropped, and the subscriber is
    # sent nothing more until it has acknowledged them, so an ack is never read as one for
    # an event of the recreated stream with the same number.
    stale: []
  ]

  alias Decree.Store.RecordedEvent
  alias Decree.Store.Subscriptions.Subscriber

  @doc """
  Subscriber is available to receive events when the number of in-flight events
  is less than its configured buffer size. By default this is set to one event.
  A subscriber with events of a hard deleted stream still to acknowledge is not
  available.
  """
  def available?(%Subscriber{stale: [_ | _]}), do: false

  def available?(%Subscriber{in_flight: []}), do: true

  def available?(%Subscriber{in_flight: in_flight, buffer_size: buffer_size}),
    do: length(in_flight) < buffer_size

  @doc """
  Is the given event in the same partition as any in-flight events?
  """
  def in_partition?(%Subscriber{partition_key: nil}, _partition_key), do: false
  def in_partition?(%Subscriber{partition_key: partition_key}, partition_key), do: true
  def in_partition?(%Subscriber{}, _partition_key), do: false

  def track_in_flight(%Subscriber{} = subscriber, %RecordedEvent{} = event, partition_key) do
    %Subscriber{in_flight: in_flight} = subscriber
    %RecordedEvent{event_number: event_number} = event

    %Subscriber{
      subscriber
      | in_flight: [event | in_flight],
        last_sent: event_number,
        partition_key: partition_key
    }
  end

  def reset_in_flight(%Subscriber{} = subscriber) do
    %Subscriber{subscriber | in_flight: [], stale: [], partition_key: nil}
  end

  @doc """
  The subscription follows its stream after a hard delete: the events in flight are of the
  deleted stream. They become stale, to be acknowledged and dropped.
  """
  def retire_in_flight(%Subscriber{} = subscriber) do
    %Subscriber{in_flight: in_flight, stale: stale} = subscriber

    numbers = Enum.map(in_flight, fn %RecordedEvent{event_number: number} -> number end)

    %Subscriber{subscriber | in_flight: [], stale: numbers ++ stale, partition_key: nil}
  end

  @doc """
  Acknowledge the in-flight event by number and all events sent to the
  subscriber before the ack'd event.

  While the subscriber has stale events, nothing has been sent to it since they went
  stale, so an ack is of one of them: the oldest stale event with that number is dropped,
  with every stale event sent before it, and no event is acknowledged.
  """
  def acknowledge(%Subscriber{stale: [_ | _] = stale} = subscriber, ack) do
    oldest_first = Enum.reverse(stale)

    case Enum.find_index(oldest_first, &(&1 == ack)) do
      nil ->
        {:error, :unexpected_ack}

      index ->
        stale = oldest_first |> Enum.drop(index + 1) |> Enum.reverse()

        {:ok, %Subscriber{subscriber | stale: stale}, []}
    end
  end

  def acknowledge(%Subscriber{} = subscriber, ack) do
    %Subscriber{in_flight: in_flight} = subscriber

    case ack_event_index(in_flight, ack) do
      nil ->
        {:error, :unexpected_ack}

      index ->
        # All in-flight events up to the ack'd event number are also ack'd
        {in_flight, acknowledged_events} = Enum.split(in_flight, index)

        subscriber =
          case in_flight do
            [] ->
              %Subscriber{subscriber | in_flight: [], partition_key: nil}

            in_flight ->
              %Subscriber{subscriber | in_flight: in_flight}
          end

        {:ok, subscriber, acknowledged_events}
    end
  end

  defp ack_event_index(in_flight, ack) do
    Enum.find_index(in_flight, fn
      %RecordedEvent{event_number: ^ack} -> true
      %RecordedEvent{} -> false
    end)
  end
end
