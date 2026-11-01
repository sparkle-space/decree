defmodule Decree.Store.AppendResult do
  @moduledoc """
  What an append wrote, answered by `c:Decree.Store.append_to_stream/4` and
  `c:Decree.Store.append_to_streams/2` when called with `returning: :append_result`.

  ## Fields

    - `position` - the last `$all` position the append wrote. An append's events take
      contiguous positions, so this and the event count give every one of them. It stands
      for the whole append, over the 1,000-event split of `c:Decree.Store.append_to_stream/4`
      as well.
    - `stream_versions` - `%{stream_uuid => version}`, each stream's version after the
      append.

  """

  @type t :: %__MODULE__{
          position: non_neg_integer(),
          stream_versions: %{String.t() => non_neg_integer()}
        }

  @enforce_keys [:position, :stream_versions]
  defstruct [:position, :stream_versions]
end
