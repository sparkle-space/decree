defmodule Decree.PersonalFields.Aggregate do
  @moduledoc """
  What an aggregate reads of its personal fields.

      defmodule User do
        use Decree.PersonalFields.Aggregate

        @personal_fold [OAuthAccountLinked, PasskeyRegistered]
        @personal_state [oauth_links: :provider_uid, email: :self]
        @personal_state_subject :user_id

        defstruct [:user_id, :email, oauth_links: []]
      end

    - `@personal_fold` lists the events `apply/2` reads a declared field of. They are decrypted
      in the fold, body fields only, because `apply/2` never sees metadata. Every other event
      reaches `apply/2` as stored, so an aggregate that folds a value it never reads is not
      locked by one subject's lost key. An event left off the list leaves an envelope in state.

    - `@personal_state` lists the state a snapshot must not hold in the clear. `state_field:
      entry_key` covers a field holding a list of maps with atom or string keys, and encrypts
      that key of each entry. `state_field: :self` covers a scalar, and encrypts the field's own
      value. `:self` is reserved, so it can't name an entry key. A value of any other shape under
      an `entry_key` records no snapshot. Those values are encrypted in the snapshot, and
      in-memory state stays plaintext.

    - `@personal_state_subject` is the state field that holds their subject.

  It generates `__personal_fold__/0`, `__personal_state__/0` and `__personal_state_subject__/0`.
  An aggregate without it pays one `function_exported?/3` per fold.

  A list entry holding its key in both atom and string form records no snapshot.

  An aggregate that uses this module can hold decrypted values in memory, from an event or from
  a snapshot, whatever `@personal_fold` lists. So on every `apply/2`, listed event or not, and
  on `execute/2` and `before_execute`, a raise reaches logs and telemetry as a
  `Decree.PersonalFields.RedactedError`, and an exit or a throw leaves reduced by
  `Decree.PersonalFields.RedactedError.reduce/1`. An aggregate that doesn't use it is untouched.

  A `Decree.Decision` uses this module too, for what `evolve/2` and `followup_query/3` read:
  `@personal_fold` means there what it means here, and a raise from a decision that uses it is
  redacted the same way. `@personal_state` and `@personal_state_subject` don't apply, because a
  decision has no snapshot. See "Personal fields" in `Decree.Decision`.
  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      @before_compile unquote(__MODULE__)
    end
  end

  @doc false
  defmacro __before_compile__(env) do
    fold = Module.get_attribute(env.module, :personal_fold) || []
    state = Module.get_attribute(env.module, :personal_state) || []
    subject = Module.get_attribute(env.module, :personal_state_subject)

    validate!(env, fold, state, subject)

    quote do
      @doc false
      def __personal_fold__, do: unquote(fold)

      @doc false
      def __personal_state__, do: unquote(state)

      @doc false
      def __personal_state_subject__, do: unquote(subject)
    end
  end

  defp validate!(env, fold, state, subject) do
    refuse = fn problem ->
      raise CompileError,
        file: env.file,
        line: env.line,
        description: "#{inspect(env.module)} declares personal state: #{problem}"
    end

    unless is_list(fold) and Enum.all?(fold, &is_atom/1) do
      refuse.("`@personal_fold` must be a list of modules, got: #{inspect(fold)}")
    end

    unless Keyword.keyword?(state) and Enum.all?(state, fn {_field, key} -> is_atom(key) end) do
      refuse.("`@personal_state` must be `state_field: entry_key`, got: #{inspect(state)}")
    end

    if state != [] do
      fields = env.module |> Macro.struct_info!(env) |> Enum.map(& &1.field)

      unless is_atom(subject) and subject != nil and subject in fields do
        refuse.("`@personal_state_subject` must name a state field, got: #{inspect(subject)}")
      end

      for {field, _key} <- state, field not in fields do
        refuse.("the state field #{inspect(field)} is not in the struct")
      end

      for {field, :self} <- state, length(Keyword.get_values(state, field)) > 1 do
        refuse.("#{inspect(field)} is `:self`, so it can't list an entry key too")
      end
    end

    :ok
  end
end
