defmodule Decree.Store.EventData do
  @moduledoc """
  EventData contains the data for a single event before being persisted to storage

  `tags` are stored with the event, in the same transaction. A repeated tag is stored
  once, and a tag over 512 bytes refuses the append with `{:error, :tag_too_long}`.
  """

  defstruct [
    :event_id,
    :correlation_id,
    :causation_id,
    :event_type,
    :data,
    :metadata,
    tags: []
  ]

  alias Decree.Store.EventData

  # The `event_tags_tag_length` CHECK's limit, in bytes.
  @max_tag_bytes 512

  @type uuid :: String.t()

  @type t :: %EventData{
          event_id: uuid() | nil,
          correlation_id: String.t() | nil,
          causation_id: String.t() | nil,
          event_type: String.t(),
          data: term,
          metadata: term | nil,
          tags: [String.t()]
        }

  @doc """
  The most bytes a tag can have.
  """
  def max_tag_bytes, do: @max_tag_bytes

  def fetch(map, key) when is_map(map) do
    Map.fetch(map, key)
  end

  def get_and_update(map, key, fun) when is_map(map) do
    Map.get_and_update(map, key, fun)
  end
end
