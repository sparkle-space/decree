defmodule Decree.Decision.UseTest do
  use ExUnit.Case, async: true

  alias Decree.DecisionFixtures.{ClaimDecision, TightDecision}

  defp compile(opts, body \\ "") do
    name = "Decree.Decision.UseTest.Fixture#{System.unique_integer([:positive])}"

    Code.eval_string("""
    defmodule #{name} do
      use Decree.Decision#{opts}

      def query(_command, _context), do: []
      def initial_state, do: nil
      def evolve(state, _event), do: state
      def decide(_state, _command), do: nil

      #{body}
    end
    """)

    Module.concat([name])
  end

  describe "bounds" do
    test "default to what was measured" do
      assert %{max_items: 16, max_probes: 48, max_events: 1_000, max_batch_events: 100} =
               ClaimDecision.__decision__()
    end

    test "may be lowered with no reason" do
      assert %{max_items: 3, max_probes: 4, max_events: 3, max_batch_events: 2} =
               TightDecision.__decision__()
    end

    test "must be positive integers" do
      for bound <- [:max_items, :max_probes, :max_events, :max_batch_events],
          value <- ["0", "-1", "1.5", ~s("1"), "nil", ":many"] do
        assert_raise ArgumentError, ~r/invalid `#{bound}`/, fn ->
          compile(", #{bound}: #{value}")
        end
      end
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

        for reason <- [~s(""), ~s("  "), "nil", ":because"] do
          assert_raise ArgumentError, ~r/bound_reason/, fn ->
            compile(", #{bound}: #{value}, bound_reason: #{reason}")
          end
        end

        decision = compile(", #{bound}: #{value}, bound_reason: \"measured\"")

        assert decision.__decision__()[bound] == value
      end
    end

    test "a value at its default needs no reason" do
      assert compile(", max_items: 16, max_probes: 48, max_events: 1000, max_batch_events: 100")
    end

    test "refuse an unknown option" do
      assert_raise ArgumentError, ~r/unknown option/, fn -> compile(", max_things: 1") end
    end
  end

  describe "optional callbacks" do
    test "`admit/2` defaults to `:ok` and can be overridden" do
      assert compile("").admit(:state, :command) == :ok

      assert compile("", "def admit(_state, _command), do: {:error, :no}").admit(:s, :c) ==
               {:error, :no}
    end

    test "`followup_query/3` is noticed when it is defined" do
      assert %{followup_query?: false} = compile("").__decision__()

      assert %{followup_query?: true} =
               compile("", "def followup_query(_c, _t, _e), do: []").__decision__()
    end
  end

  describe "a decision reacts to no event" do
    test "defining `handle/2` fails to compile" do
      assert_raise ArgumentError, ~r/reacts to no event/, fn ->
        compile("", "def handle(_event, _metadata), do: :ok")
      end
    end

    test "declaring `Decree.Event.Handler` fails to compile" do
      assert_raise ArgumentError, ~r/reacts to no event/, fn ->
        compile("", "@behaviour Decree.Event.Handler")
      end
    end

    test "no module of decree is a process manager" do
      {:ok, modules} = :application.get_key(:decree, :modules)

      names =
        modules
        |> Enum.map(&inspect/1)
        |> Enum.filter(&(&1 =~ ~r/process_?manager|saga/i))

      assert names == []
    end

    test "a decision's callbacks take no event to react to and no subscription" do
      callbacks = Decree.Decision.behaviour_info(:callbacks)

      assert Enum.sort(callbacks) == [
               admit: 2,
               decide: 2,
               evolve: 2,
               followup_query: 3,
               initial_state: 0,
               query: 2
             ]
    end
  end
end
