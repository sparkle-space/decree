defmodule Decree.Store.ReadResult do
  @moduledoc """
  What `c:Decree.Store.read_query/2` answers, all of it from one database snapshot.

  ## Fields

    - `events` - the matching `Decree.Store.RecordedEvent`s, in the read's direction.
      `event_number` is the event's `$all` position, and `stream_uuid` and
      `stream_version` are its original stream's, exactly as a row of
      `c:Decree.Store.read_all_streams_forward/3`. A match only the read's
      `position_items` matched has no event here, only its entries in the maps below.
    - `after` - the `$all` head at the read's snapshot. It is not the last match: an
      event that does not match still moves it.
    - `read_at` - a `DateTime`, the database's clock when the read ran, taken once.
    - `command_stamps` - `%{position => stamp}` for each match carrying the read's
      `command_tag`. A match without that tag has no entry. A `nil` stamp means the
      tag's row carries no stamp, which is not the same as having no row.
    - `matched_items` - `%{position => indexes}`, the sorted 0-based indexes of the
      items that matched each match, the ones `position_items` answers with no event
      included.
    - `tags` - `%{position => tags}` for each match one of the read's `tag_items` matched,
      its tags sorted bytewise, from the same snapshot as the rest. It leaves out what
      `c:Decree.Store.read_tags/2` leaves out, so a position with no tag left, or one no
      `tag_items` item matched, has no entry. `%{}` when the read names no `tag_items`.

  """

  alias Decree.Store.RecordedEvent

  @type position :: non_neg_integer()

  @type t :: %__MODULE__{
          events: [RecordedEvent.t()],
          after: non_neg_integer(),
          read_at: DateTime.t(),
          command_stamps: %{position() => DateTime.t() | nil},
          matched_items: %{position() => [non_neg_integer()]},
          tags: %{position() => [String.t()]}
        }

  defstruct [:events, :after, :read_at, :command_stamps, :matched_items, tags: %{}]
end
