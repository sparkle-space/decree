defmodule Decree.Command do
  @moduledoc """
  A command whose `command_id` its requester mints.

  Every command a router dispatches carries a `command_id`: a lowercase canonical UUID that
  whoever asks for the command (a LiveView, a seed, an event handler, an external agent)
  mints once and sends with it. A dispatch refuses a command without one, before the
  identity is extracted and before any aggregate or decision runs:

    - `{:error, :command_id_required}` - the field is missing, nil, or a binary that is empty
      or only whitespace.
    - `{:error, :invalid_command_id}` - anything else that is not a canonical UUID: an
      uppercase or padded UUID, a UUID without dashes, the raw 16-byte form, an integer, an
      atom.

  Nothing repairs a `command_id`: no path generates, fills, trims or downcases one, and the
  `command_uuid` a dispatch mints is a different value that never stands in for it. The
  refusal is the default middleware `Decree.Middleware.CommandId`, which runs after every
  middleware a router registers.

  ## Declaring a command

      defmodule OpenAccount do
        use Decree.Command

        defstruct [:account_id, :command_id, :initial_balance]
      end

  `use Decree.Command` marks the module and checks, once the module has compiled, that it
  defines a struct with a `:command_id` field: a module that does not fails to compile. It
  does not define the struct, or give the field a default: a default would be one id for
  every command of the module.

  A router refuses a command that is not a `Decree.Command` when it compiles only when it
  asks to, with `use Decree.Commands.Router, require_decree_command: true`. The default is
  off for now. The dispatch refusal does not depend on it: a plain struct with a canonical
  `command_id` dispatches.

  ## Checking before dispatch

  A facade whose side effect precedes dispatch asks `check_id/1`, which is the rule the
  dispatch applies, so it answers the same refusal first.
  """

  @doc false
  defmacro __using__(_opts) do
    quote do
      @after_compile Decree.Command

      @doc false
      def __decree_command__, do: true
    end
  end

  # Called with no export test, because under the compiler the module is not loaded here, so
  # `function_exported?/3` would answer no and refuse every command. A direct call loads it.
  @doc false
  def __after_compile__(%Macro.Env{module: module} = env, _bytecode) do
    if not struct_with_command_id?(module) do
      raise CompileError,
        file: env.file,
        line: env.line,
        description:
          "#{inspect(module)} uses Decree.Command and defines no struct with a " <>
            "`:command_id` field: add `:command_id` to its `defstruct`"
    end
  end

  defp struct_with_command_id?(module) do
    Map.has_key?(module.__struct__(), :command_id)
  rescue
    UndefinedFunctionError -> false
  end

  @doc """
  Whether a `command_id` is one a dispatch takes: `:ok` for a lowercase canonical UUID, and
  otherwise the refusal the dispatch answers.

  nil, and a binary that is empty or only whitespace, are `:command_id_required`. Any other
  binary that is not a canonical UUID (`Decree.UUID.canonical?/1`), and anything that is not
  a binary, is `:invalid_command_id`. The value is never repaired.

      :ok = Decree.Command.check_id("6f9619ff-8b86-d011-b42d-00c04fc964ff")
      {:error, :invalid_command_id} = Decree.Command.check_id("6F9619FF-8B86-D011-B42D-00C04FC964FF")
      {:error, :command_id_required} = Decree.Command.check_id(nil)
  """
  @spec check_id(term()) :: :ok | {:error, :command_id_required | :invalid_command_id}
  def check_id(nil), do: {:error, :command_id_required}

  def check_id(command_id) when is_binary(command_id) do
    cond do
      String.trim(command_id) == "" -> {:error, :command_id_required}
      Decree.UUID.canonical?(command_id) -> :ok
      true -> {:error, :invalid_command_id}
    end
  end

  def check_id(_command_id), do: {:error, :invalid_command_id}

  @doc """
  Whether a command struct, or a command module, uses `Decree.Command`.
  """
  @spec command?(struct() | module() | term()) :: boolean()
  def command?(%module{}), do: command?(module)

  def command?(module) when is_atom(module) do
    Code.ensure_loaded?(module) and function_exported?(module, :__decree_command__, 0)
  end

  def command?(_value), do: false
end
