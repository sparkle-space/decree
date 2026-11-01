defprotocol Decree.Event.PersonalFields do
  @moduledoc """
  Which of an event's fields and metadata entries hold personal data, and whose.

  `declaration/1` answers a `Decree.PersonalFields.Declaration`, or `nil` for an event that
  declares nothing. The protocol falls back to `Any`, which answers `nil`, so such an event
  costs one protocol dispatch and is otherwise untouched: its stored JSON and metadata are
  what they would be without this protocol.

  An event declares by deriving:

      defmodule UserInvited do
        @derive [
          Jason.Encoder,
          {Decree.Event.PersonalFields,
           personal: [email: {:first_present, [:target_user_id, :invitation_id]}],
           personal_metadata: [email: :target_user_id, password_hash: :target_user_id]}
        ]
        defstruct [:invitation_id, :target_user_id, :email]
      end

  `personal:` lists the event's own fields. `personal_metadata:` lists metadata entries, which
  need not be fields of the struct. Each names a subject, the person whose key protects the
  value, in one of three forms:

    - `:x` - the subject is `event.x`. A nil `event.x` fails the append.

    - `{:first_present, [:a, :b]}` - the first non-nil of those fields. If all are nil the
      append fails.

    - `{:when_present, :x}` - the subject is `event.x`, and with a nil `event.x` the value is
      stored in the clear.

  The derive refuses, with a `CompileError`, any other form, a declared field or subject field
  that is not in the struct, a subject field that is itself declared personal, an option it
  does not know, and a declaration with neither list non-empty. A typo must never leave a field
  in the clear.

  `Decree.PersonalFields` says what is done with a declaration.
  """

  alias Decree.PersonalFields.Declaration

  @fallback_to_any true

  @impl true
  defmacro __deriving__(module, options) do
    fields = module |> Macro.struct_info!(__CALLER__) |> Enum.map(& &1.field)
    declaration = Declaration.build!(module, fields, options, __CALLER__)

    quote do
      defimpl Decree.Event.PersonalFields, for: unquote(module) do
        def declaration(_event), do: unquote(Macro.escape(declaration))
      end
    end
  end

  @doc """
  Answers the event's declaration, or `nil` when it declares nothing.
  """
  @spec declaration(t) :: Declaration.t() | nil
  def declaration(event)
end

defimpl Decree.Event.PersonalFields, for: Any do
  def declaration(_event), do: nil
end
