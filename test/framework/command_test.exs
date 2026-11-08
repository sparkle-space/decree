defmodule Decree.CommandTest do
  use Decree.PostgresCase

  import Decree.BuildFixture, only: [build: 2]
  import ExUnit.CaptureIO

  # `use Decree.Command` declares a command whose `command_id` the requester mints, and
  # `Decree.Command.check_id/1` is the refusal a dispatch answers, so a facade whose side
  # effect precedes dispatch can answer it first. Every module that uses it here compiles at
  # run time, under a name of its own.

  alias Decree.{CommandIdCases, DefaultApp, UUID}

  defmodule Plain do
    @moduledoc false
    defstruct [:id, :command_id]
  end

  defmodule Checked do
    @moduledoc false
    defstruct [:id, :command_id]
  end

  defmodule CheckedAggregate do
    @moduledoc false
    defstruct [:id]

    def execute(%__MODULE__{}, %Checked{}), do: []
    def apply(%__MODULE__{} = state, _event), do: state
  end

  defmodule CheckedRouter do
    @moduledoc false
    use Decree.Commands.Router

    dispatch Checked, to: CheckedAggregate, identity: :id
  end

  defp unique(name), do: "Decree.CommandTest.#{name}#{System.unique_integer([:positive])}"

  defp compile(name, body) do
    [{module, _binary}] =
      Code.compile_string("""
      defmodule #{name} do
        use Decree.Command

        #{body}
      end
      """)

    module
  end

  describe "`use Decree.Command`" do
    test "fails to compile a module whose struct has no `:command_id`" do
      assert_raise CompileError, ~r/:command_id/, fn ->
        compile(unique("NoField"), "defstruct [:id]")
      end
    end

    test "fails to compile a module with no struct" do
      assert_raise CompileError, ~r/:command_id/, fn ->
        compile(unique("NoStruct"), "def id, do: nil")
      end
    end

    test "fails under the compiler too, where the module is not loaded yet" do
      name = unique("Built")

      capture_io(:stderr, fn ->
        assert {_dir, {:error, [%{message: message}], _warnings}} =
                 build(
                   name,
                   "defmodule #{name} do\n  use Decree.Command\n  defstruct [:id]\nend\n"
                 )

        assert message =~ name
        assert message =~ ":command_id"
      end)
    end

    test "builds under the compiler, where the module is not loaded yet" do
      name = unique("BuiltWithId")

      assert {_dir, {:ok, [_module], _warnings}} =
               build(
                 name,
                 "defmodule #{name} do\n  use Decree.Command\n  defstruct [:id, :command_id]\nend\n"
               )
    end

    test "compiles a struct with `:command_id`, and marks it" do
      module = compile(unique("WithId"), "defstruct [:id, :command_id]")

      assert module.__decree_command__()
      assert Decree.Command.command?(module)
      assert Decree.Command.command?(struct(module))
    end

    test "compiles a computed field list" do
      module = compile(unique("Computed"), "@fields [:id, :command_id]\ndefstruct @fields")

      assert Decree.Command.command?(module)
    end

    test "does not define the struct" do
      module = compile(unique("OwnStruct"), "defstruct [:command_id, owner: :me]")

      assert %{command_id: nil, owner: :me} = struct(module)

      assert module.__struct__() |> Map.keys() |> Enum.sort() == [
               :__struct__,
               :command_id,
               :owner
             ]
    end
  end

  describe "`command?/1`" do
    test "is false for a struct that does not use it" do
      refute Decree.Command.command?(Plain)
      refute Decree.Command.command?(%Plain{})
    end

    test "is false for what is not a module" do
      refute Decree.Command.command?(Decree.CommandTest.NoSuchModule)
      refute Decree.Command.command?(%{command_id: UUID.uuid4()})
      refute Decree.Command.command?("Decree.CommandTest.Plain")
    end
  end

  describe "`check_id/1`" do
    test "takes a canonical UUID" do
      assert :ok = Decree.Command.check_id(UUID.uuid4())
      assert :ok = Decree.Command.check_id(CommandIdCases.canonical())
    end

    for {name, value, refusal} <- CommandIdCases.refusals() do
      test "answers what a dispatch answers for #{name}: `#{inspect(refusal)}`" do
        start_supervised!(DefaultApp)

        value = unquote(Macro.escape(value))
        command = %Checked{id: UUID.uuid4(), command_id: value}

        assert {:error, unquote(refusal)} = Decree.Command.check_id(value)

        assert {:error, unquote(refusal)} =
                 CheckedRouter.dispatch(command, application: DefaultApp)
      end
    end

    test "answers what a dispatch answers for a canonical UUID" do
      start_supervised!(DefaultApp)

      command = %Checked{id: UUID.uuid4(), command_id: UUID.uuid4()}

      assert :ok = Decree.Command.check_id(command.command_id)
      assert :ok = CheckedRouter.dispatch(command, application: DefaultApp)
    end
  end
end
