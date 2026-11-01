defmodule Decree.Decision.UseTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureIO

  alias Decree.DecisionFixtures.{ClaimDecision, TightDecision}

  # A decision the build takes, but for what a test adds.
  defp compile(opts, body \\ "") do
    source(", create: false" <> opts, "def command_window_ms, do: 1\n" <> body)
  end

  defp source(opts, body) do
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

  describe "`create:`" do
    test "is required" do
      assert_raise ArgumentError, ~r/no `create:` option/, fn ->
        source("", "def command_window_ms, do: 1")
      end
    end

    test "takes `false` or a list of types, and `__decision__/0` holds it" do
      assert %{create: false} = compile("").__decision__()

      assert %{create: ["Elixir.A", "Elixir.B"]} =
               source(
                 ~S|, create: [types: ["Elixir.A", "Elixir.B"]]|,
                 "def command_window_ms, do: 1"
               ).__decision__()
    end

    test "refuses anything else" do
      for create <- [
            "true",
            "nil",
            ":always",
            ~s(["Elixir.A"]),
            "[types: []]",
            "[types: nil]",
            ~s([types: "Elixir.A"]),
            ~s([types: [""]]),
            "[types: [Elixir.A]]",
            ~s([types: ["Elixir.A"], tags: ["t"]]),
            ~s([tags: ["t"]])
          ] do
        assert_raise ArgumentError, ~r/invalid `create`/, fn ->
          source(", create: #{create}", "def command_window_ms, do: 1")
        end
      end
    end

    test "cannot be a replay item" do
      assert_raise ArgumentError, ~r/`replay:` in `create:`/, fn ->
        source(~S|, create: [types: ["Elixir.A"], replay: true]|, "def command_window_ms, do: 1")
      end
    end
  end

  describe "`command_window_ms/0`" do
    test "is required" do
      assert_raise ArgumentError, ~r/does not define `command_window_ms\/0`/, fn ->
        source(", create: false", "")
      end
    end
  end

  describe "optional callbacks" do
    test "`admit/2` defaults to `:ok` and can be overridden" do
      assert compile("").admit(:state, :command) == :ok

      assert compile("", "def admit(_state, _command), do: {:error, :no}").admit(:s, :c) ==
               {:error, :no}
    end

    test "`scope_tags/1` defaults to `[]` and can be overridden" do
      assert compile("").scope_tags(:command) == []

      decision = compile("", ~S|def scope_tags(_command), do: ["a:b"]|)

      assert decision.scope_tags(:command) == ["a:b"]
    end

    test "`followup_query/3` is noticed when it is defined" do
      assert %{followup_query?: false} = compile("").__decision__()

      assert %{followup_query?: true} =
               compile("", "def followup_query(_c, _t, _e), do: []").__decision__()
    end

    test "`index_families/0` is read when it is defined" do
      assert %{index_families: families} = compile("").__decision__()
      assert families == %{}

      assert %{index_families: %{"email" => :digest}} =
               compile("", ~s(def index_families, do: %{"email" => :digest})).__decision__()
    end
  end

  describe "a bad `index_families/0`" do
    for {name, answer} <- [
          {"an atom key", ~s(%{email: :digest})},
          {"an empty-string key", ~s(%{"" => :digest})},
          {"a key with a colon", ~s(%{"email:x" => :digest})},
          {"an unknown family", ~s(%{"email" => :digests})},
          {"a map that is good but for one family", ~s(%{"a" => :digest, "b" => :plan})},
          {"something other than a map", ~s([{"email", :digest}])},
          {"nil", ~s(nil)}
        ] do
      test "fails the build for #{name}" do
        body = "def index_families, do: #{unquote(answer)}"

        assert_raise ArgumentError, ~r/UseTest\.Fixture\d+ has an invalid `index_families/, fn ->
          compile("", body)
        end
      end
    end

    test "fails the build when the callback raises" do
      assert_raise RuntimeError, "no families", fn ->
        compile("", ~s[def index_families, do: raise("no families")])
      end
    end

    # `Code.eval_string/1` is not what a build runs. `compile_to_path/3` is what `mix compile`
    # calls, and under it the module is not loaded when the check runs, which is where an
    # export test would pass every bad declaration.
    test "fails under the compiler too, where the module is not loaded yet" do
      name = "Decree.Decision.UseTest.Compiled#{System.unique_integer([:positive])}"
      dir = Path.join(System.tmp_dir!(), name)
      path = Path.join(dir, "decision.ex")

      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      File.write!(path, """
      defmodule #{name} do
        use Decree.Decision

        def query(_command, _context), do: []
        def initial_state, do: nil
        def evolve(state, _event), do: state
        def decide(_state, _command), do: nil
        def index_families, do: %{email: :digest}
      end
      """)

      capture_io(:stderr, fn ->
        assert {:error, [%{message: message}], _warnings} =
                 Kernel.ParallelCompiler.compile_to_path([path], dir, return_diagnostics: true)

        assert message =~ "#{name} has an invalid `index_families/0`"
      end)
    end

    test "a valid map compiles" do
      body = ~s(def index_families, do: %{"email" => :digest, "label" => :plain})

      assert %{index_families: %{"email" => :digest, "label" => :plain}} =
               compile("", body).__decision__()
    end

    test "an empty map compiles" do
      assert %{index_families: families} =
               compile("", "def index_families, do: %{}").__decision__()

      assert families == %{}
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
               command_window_ms: 0,
               decide: 2,
               evolve: 2,
               followup_query: 3,
               index_families: 0,
               initial_state: 0,
               query: 2,
               scope_tags: 1
             ]
    end
  end
end
