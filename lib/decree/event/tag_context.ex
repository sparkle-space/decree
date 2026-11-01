defmodule Decree.Event.TagContext do
  @moduledoc """
  What `Decree.Event.Tags.tags/2` may read besides the event.

    - `metadata` - the dispatch metadata, in plaintext, as it will be stored.

    - `command` - the command struct that produced the event.

  """

  @type t :: %__MODULE__{metadata: map(), command: struct()}

  defstruct [:metadata, :command]
end
