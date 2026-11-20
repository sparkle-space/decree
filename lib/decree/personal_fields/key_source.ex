defmodule Decree.PersonalFields.KeySource do
  @moduledoc """
  Where the data keys of `Decree.PersonalFields` come from.

  A host names its key source in source, on the `use` line, and nowhere else:

      defmodule MyApp.App do
        use Decree.Application,
          otp_app: :my_app,
          key_source: MyApp.SubjectKeys
      end

  `lookup/1` answers one of three things:

    - `{:ok, key_id, key}` - the subject's 32-byte data key, and a non-empty id naming it.

    - `:erased` - the subject's key was erased. Its values read as absent, and a new value is
      not stored.

    - `:unknown` - anything else. Nothing that fails to find a key may answer `:erased`, because
      an append would then drop the value for good, and a reader would read it as gone.

  Decree treats a raise, an exit, a throw and any other answer as `:unknown`, without logging
  what was raised. It holds no answer beyond one append, one fold or one delivered event, so an
  implementation decides every time.

  `Decree.PersonalFields.KeyStream` is what an implementation reads a subject's wrapped key
  from.
  """

  @doc """
  Answers the subject's data key, or `:erased`, or `:unknown`.
  """
  @callback lookup(subject_id :: String.t()) ::
              {:ok, key_id :: String.t(), key :: <<_::256>>} | :erased | :unknown
end
