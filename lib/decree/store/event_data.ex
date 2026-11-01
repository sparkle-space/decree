defmodule Decree.Store.EventData do
  @moduledoc """
  EventData contains the data for a single event before being persisted to storage

  `tags` are stored with the event, in the same transaction. A repeated tag is stored
  once. A tag that is not a string Postgres can hold refuses the append with
  `{:error, :invalid_tag}`, and one over 512 bytes with `{:error, :tag_too_long}`.
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

  @doc """
  Whether a value is a string Postgres can hold as a tag: a binary of valid UTF-8 with no
  NUL byte. Its length is `max_tag_bytes/0`'s question.
  """
  @spec valid_tag?(term) :: boolean
  def valid_tag?(tag) when is_binary(tag) do
    String.valid?(tag) and not String.contains?(tag, <<0>>)
  end

  def valid_tag?(_tag), do: false

  def fetch(map, key) when is_map(map) do
    Map.fetch(map, key)
  end

  def get_and_update(map, key, fun) when is_map(map) do
    Map.get_and_update(map, key, fun)
  end
end
