defmodule Decree.Event.TagContext do
  @moduledoc """
  What `Decree.Event.Tags.tags/2` may read besides the event.

    - `metadata` - this event's copy of the dispatch metadata, in plaintext: what will be
      stored, before personal entries are encrypted.

    - `command` - the command struct that produced the event.

  """

  @type t :: %__MODULE__{metadata: map(), command: struct()}

  defstruct [:metadata, :command]
end
