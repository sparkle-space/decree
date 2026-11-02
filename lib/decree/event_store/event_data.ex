defmodule Decree.EventStore.EventData do
  @moduledoc """
  EventData contains the data for a single event before being persisted to
  storage.
  """

  @type uuid :: String.t()

  @type t :: %Decree.EventStore.EventData{
          causation_id: uuid() | nil,
          correlation_id: uuid(),
          event_type: String.t(),
          data: struct(),
          metadata: map(),
          tags: [String.t()],
          required_tag_kinds: list()
        }

  defstruct [
    :causation_id,
    :correlation_id,
    :event_type,
    :data,
    :metadata,
    tags: [],
    required_tag_kinds: []
  ]
end
