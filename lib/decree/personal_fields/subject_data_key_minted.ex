defmodule Decree.PersonalFields.SubjectDataKeyMinted do
  @moduledoc """
  A subject's data key, wrapped, as the first event of its stream.

  `wrapped_key` is base64, so that JSON never sees raw bytes. `Inspect` leaves it out. See
  `Decree.PersonalFields.KeyStream`, the only writer.
  """

  if Code.ensure_loaded?(Jason), do: @derive(Jason.Encoder)
  @derive [Decree.Event.Tags, {Inspect, except: [:wrapped_key]}]
  defstruct [:subject_id, :key_id, :wrapped_key, :master_tag, :minted_at]

  @type t :: %__MODULE__{
          subject_id: String.t(),
          key_id: String.t(),
          wrapped_key: String.t(),
          master_tag: String.t(),
          minted_at: String.t()
        }
end
