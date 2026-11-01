defmodule Decree.Commands.StreamTagOptionTest do
  use ExUnit.Case, async: true

  defp compile_router(name, stream_tag, attribute \\ "") do
    Code.eval_string("""
    alias Decree.ExampleDomain.BankAccount
    alias Decree.ExampleDomain.BankAccount.Commands.OpenAccount

    defmodule #{name} do
      use Decree.Commands.Router

      #{attribute}

      dispatch OpenAccount,
        to: BankAccount,
        identity: :account_number,
        stream_tag: #{stream_tag}
    end
    """)
  end

  for {name, stream_tag} <- [
        {"false", "false"},
        {"true", "true"},
        {"nil", "nil"},
        {"a string", ~s("user")},
        {"a number", "1"},
        {"a list", "[:user]"},
        {"an atom holding a colon", ~s(:"a:b")},
        {"an empty atom", ~s(:"")},
        {"a module", "Some.Module"},
        {"an atom with a trailing newline", ~s(:"user\\n")}
      ] do
    test "refuses #{name} when the router compiles" do
      assert_raise ArgumentError, ~r/invalid `stream_tag` option/, fn ->
        compile_router(
          "Decree.Commands.StreamTagOptionTest.Refused#{:erlang.unique_integer([:positive])}",
          unquote(stream_tag)
        )
      end
    end
  end

  test "accepts an atom" do
    assert {{:module, _module, _binary, _term}, _binding} =
             compile_router("Decree.Commands.StreamTagOptionTest.Accepted", ":user")
  end

  test "accepts a value only known once the router's body runs" do
    assert {{:module, _module, _binary, _term}, _binding} =
             compile_router(
               "Decree.Commands.StreamTagOptionTest.FromAttribute",
               "@kind",
               "@kind :attribute_kind"
             )
  end

  test "is optional" do
    assert {{:module, _module, _binary, _term}, _binding} =
             Code.eval_string("""
             alias Decree.ExampleDomain.BankAccount
             alias Decree.ExampleDomain.BankAccount.Commands.OpenAccount

             defmodule Decree.Commands.StreamTagOptionTest.NotNamed do
               use Decree.Commands.Router

               dispatch OpenAccount, to: BankAccount, identity: :account_number
             end
             """)
  end
end
