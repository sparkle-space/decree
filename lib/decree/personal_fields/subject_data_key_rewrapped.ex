defmodule Decree.PersonalFields.SubjectDataKeyRewrapped do
  @moduledoc """
  A subject's data key under a new wrap, with its `key_id` unchanged.

  `wrapped_key` is base64, so that JSON never sees raw bytes. `Inspect` leaves it out. See
  `Decree.PersonalFields.KeyStream`, the only writer.
  """

  if Code.ensure_loaded?(Jason), do: @derive(Jason.Encoder)
  @derive [Decree.Event.Tags, {Inspect, except: [:wrapped_key]}]
  defstruct [:subject_id, :key_id, :wrapped_key, :master_tag, :rewrapped_at]

  @type t :: %__MODULE__{
          subject_id: String.t(),
          key_id: String.t(),
          wrapped_key: String.t(),
          master_tag: String.t(),
          rewrapped_at: String.t()
        }
end
