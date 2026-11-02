defmodule Decree.Decision.RuleUseTest do
  use ExUnit.Case, async: true

  alias Decree.Decision.Rule.Result
  alias Decree.RuleFixtures.{EmailRule, Read, RegisterUser, ScriptedRule, TightRule}

  defp compile(opts \\ "", body \\ "") do
    name = "Decree.Decision.RuleUseTest.Fixture#{System.unique_integer([:positive])}"

    Code.eval_string("""
    defmodule #{name} do
      use Decree.Decision.Rule#{opts}

      def query(_command, _context), do: []

      #{body}
    end
    """)

    Module.concat([name])
  end

  describe "bounds" do
    test "are a decision's, by default and lowered" do
      assert %{max_items: 16, max_probes: 48, max_events: 1_000, max_batch_events: 100} =
               ScriptedRule.__rule__()

      assert %{max_events: 3, max_batch_events: 2} = TightRule.__rule__()
    end

    test "may not be raised past a default without a reason" do
      for {bound, value} <- [
            max_items: 17,
            max_probes: 49,
            max_events: 1_001,
            max_batch_events: 101
          ] do
        assert_raise ArgumentError, ~r/`#{bound}` past its default/, fn ->
          compile(", #{bound}: #{value}")
        end
      end
    end

    test "may be raised with a reason" do
      rule = compile(~s(, max_items: 17, bound_reason: "one tenant has 17"))

      assert %{max_items: 17} = rule.__rule__()
    end

    test "refuse an unknown option" do
      assert_raise ArgumentError, ~r/unknown option: :max_things/, fn ->
        compile(", max_things: 1")
      end
    end
  end

  describe "the build refuses" do
    for {callback, source} <- [
          {"evolve/2", "def evolve(state, _event), do: state"},
          {"decide/2", "def decide(_state, _command), do: nil"},
          {"admit/2", "def admit(_state, _command), do: :ok"},
          {"scope_tags/1", "def scope_tags(_command), do: []"},
          {"initial_state/0", "def initial_state, do: nil"},
          {"command_window_ms/0", "def command_window_ms, do: 1"},
          {"handle/2", "def handle(_event, _metadata), do: :ok"}
        ] do
      test "`#{callback}`" do
        assert_raise ArgumentError, ~r/defines `#{unquote(callback)}`/, fn ->
          compile("", unquote(source))
        end
      end
    end

    test "being an event handler" do
      assert_raise ArgumentError, ~r/declares Decree.Event.Handler/, fn ->
        compile("", "@behaviour Decree.Event.Handler")
      end
    end

    test "`create:`, whatever it says" do
      for value <- ["false", ~s([types: ["Elixir.Fact"]])] do
        assert_raise ArgumentError, ~r/with `create:`/, fn -> compile(", create: #{value}") end
      end
    end
  end

  test "says whether it has a second read" do
    assert %{followup_query?: true, create: false} = EmailRule.__rule__()
    assert %{followup_query?: false} = compile().__rule__()
  end

  describe "a result" do
    setup do
      [result: %Result{events: [%Read{secret: "the-marker"}]}]
    end

    test "shows nothing of what it holds", %{result: result} do
      refute inspect(result) =~ "the-marker"
      assert inspect(result) == "#Decree.Decision.Rule.Result<...>"
    end

    test "shows nothing inside a command", %{result: result} do
      refute inspect(%RegisterUser{rule_result: result}) =~ "the-marker"
    end

    test "can't be written as JSON, and the failure holds nothing", %{result: result} do
      error = assert_raise ArgumentError, fn -> Jason.encode!(%{note: result}) end

      refute inspect(error, structs: false) =~ "the-marker"
    end
  end
end
