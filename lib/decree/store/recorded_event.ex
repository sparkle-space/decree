defmodule Decree.Store.RecordedEvent do
  @moduledoc """
  `Decree.Store.RecordedEvent` contains the persisted data and metadata for a
  single event.

  Events are immutable once recorded.

  ## Recorded event fields

    - `event_number` - position of the event within the stream.
      This will be identical to the `stream_version` when fetching events from a
      single stream. For the `$all` stream it will be the globally ordered event
      number.
    - `event_id` - a globally unique UUID to identify the event.
    - `stream_uuid` - the original stream identity for the event.
    - `stream_version` - the original version of the stream for the event.
    - `correlation_id` - an optional string identifier used to correlate related
      messages.
    - `causation_id` - an optional string identifier used to identify which
      message you are responding to.
    - `data` - the deserialized event data.
    - `metadata` - a deserialized map of event metadata.
    - `created_at` - a `DateTime` (in UTC) indicating when the event was
      created.

  """

  alias Decree.Store.{LogTerms, RecordedEvent, UnknownEventTypeError}

  @type uuid :: String.t()

  @type t :: %RecordedEvent{
          event_number: non_neg_integer(),
          event_id: uuid(),
          stream_uuid: String.t(),
          stream_version: non_neg_integer(),
          correlation_id: String.t() | nil,
          causation_id: String.t() | nil,
          event_type: String.t(),
          data: any(),
          metadata: map() | nil,
          created_at: DateTime.t()
        }

  defstruct [
    :event_number,
    :event_id,
    :stream_uuid,
    :stream_version,
    :correlation_id,
    :causation_id,
    :event_type,
    :data,
    :metadata,
    :created_at
  ]

  # Raises `Decree.Store.UnknownEventTypeError` for an event of a type the serializer does
  # not know (`c:Decree.Store.Serializer.known_type?/1`), before any of its data is read. Any
  # other failure to deserialize, such as a corrupt payload, raises as the serializer raises
  # it.
  def deserialize(%RecordedEvent{} = recorded_event, serializer) do
    %RecordedEvent{event_type: event_type, event_number: event_number} = recorded_event

    if known_type?(serializer, event_type) do
      deserialize_known_type(recorded_event, serializer)
    else
      raise UnknownEventTypeError, type: event_type, event_number: event_number
    end
  end

  @doc false
  # Deserializes the events in order up to the first one whose type the serializer does not
  # know, and answers `{deserialized, nil}` when every type is known, or
  # `{deserialized_prefix, {event_number, event_type}}` naming the first unknown one. Nothing
  # at or after that event is deserialized.
  @spec deserialize_known([t()], module()) ::
          {[t()], nil | {non_neg_integer(), String.t()}}
  def deserialize_known(recorded_events, serializer) when is_list(recorded_events) do
    deserialize_known(recorded_events, serializer, [])
  end

  defp deserialize_known([], _serializer, acc), do: {Enum.reverse(acc), nil}

  defp deserialize_known([recorded_event | recorded_events], serializer, acc) do
    %RecordedEvent{event_type: event_type, event_number: event_number} = recorded_event

    if known_type?(serializer, event_type) do
      event = deserialize_known_type(recorded_event, serializer)

      deserialize_known(recorded_events, serializer, [event | acc])
    else
      {Enum.reverse(acc), {event_number, event_type}}
    end
  end

  @doc false
  # A subscription's catch-up read: deserializes the events in order up to the first one the
  # subscription must hold before, and answers `{deserialized, nil}` when there is none, or
  # `{deserialized_prefix, {event_number, event_type, cause}}` naming it. The cause is
  # `:unknown_type` for a type the serializer does not know, or `{:undecodable, class}` for a
  # known type whose deserialize raised or exited, `class` being what
  # `Decree.Store.LogTerms.reduce/1` keeps of the exception or exit reason, never its message
  # or the event's data (sparkle-space/sparklespace#8357). Nothing at or after that event is
  # deserialized. Whether a type is known is asked outside the rescue, so a crash there still
  # raises, and a throw is caught at neither step.
  @spec deserialize_until_hold([t()], module()) ::
          {[t()],
           nil
           | {non_neg_integer(), String.t(), :unknown_type | {:undecodable, term()}}}
  def deserialize_until_hold(recorded_events, serializer) when is_list(recorded_events) do
    deserialize_until_hold(recorded_events, serializer, [])
  end

  defp deserialize_until_hold([], _serializer, acc), do: {Enum.reverse(acc), nil}

  defp deserialize_until_hold([recorded_event | recorded_events], serializer, acc) do
    %RecordedEvent{event_type: event_type, event_number: event_number} = recorded_event

    if known_type?(serializer, event_type) do
      case guarded_deserialize_known_type(recorded_event, serializer) do
        {:ok, event} ->
          deserialize_until_hold(recorded_events, serializer, [event | acc])

        {:undecodable, class} ->
          {Enum.reverse(acc), {event_number, event_type, {:undecodable, class}}}
      end
    else
      {Enum.reverse(acc), {event_number, event_type, :unknown_type}}
    end
  end

  defp guarded_deserialize_known_type(recorded_event, serializer) do
    {:ok, deserialize_known_type(recorded_event, serializer)}
  rescue
    error -> {:undecodable, LogTerms.reduce(error)}
  catch
    :exit, reason -> {:undecodable, LogTerms.reduce(reason)}
  end

  @doc false
  # Whether the serializer can build an event of `type`. A serializer without the optional
  # `c:Decree.Store.Serializer.known_type?/1` knows every type.
  def known_type?(serializer, type) do
    if Code.ensure_loaded?(serializer) and function_exported?(serializer, :known_type?, 1) do
      serializer.known_type?(type)
    else
      true
    end
  end

  @doc false
  # Deserializes an event whose type the caller has already found known, without asking
  # again.
  def deserialize_known_type(%RecordedEvent{} = recorded_event, serializer) do
    %RecordedEvent{data: data, metadata: metadata, event_type: event_type} = recorded_event

    %RecordedEvent{
      recorded_event
      | data: serializer.deserialize(data, type: event_type),
        metadata: serializer.deserialize(metadata, [])
    }
  end

  def fetch(map, key) when is_map(map) do
    Map.fetch(map, key)
  end

  def get_and_update(map, key, fun) when is_map(map) do
    Map.get_and_update(map, key, fun)
  end
end
