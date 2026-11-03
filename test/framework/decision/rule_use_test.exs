defmodule Decree.Decision.RuleUseTest do
  use ExUnit.Case, async: true

  import Decree.BuildFixture, only: [build: 2]
  import ExUnit.CaptureIO

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

      assert %{max_events: 12, max_batch_events: 2} = TightRule.__rule__()
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

    test "`activation:`, whatever it says" do
      for value <- ["nil", ~s([types: ["Elixir.On"], tags: ["feature:x"]])] do
        assert_raise ArgumentError, ~r/with `activation:`/, fn ->
          compile(", activation: #{value}")
        end
      end
    end
  end

  describe "`index_families/0`" do
    # A `use` after `use Decree.Decision.Rule`, so its `@before_compile` runs after that one's.
    defp injecting(name, families) do
      """
      defmodule #{name}.Families do
        defmacro __using__(_opts) do
          quote do
            @before_compile #{name}.Families
          end
        end

        defmacro __before_compile__(_env) do
          quote do
            def index_families, do: #{families}
          end
        end
      end

      defmodule #{name} do
        use Decree.Decision.Rule
        use #{name}.Families

        def query(_command, _context), do: []
      end
      """
    end

    defp declaring(name, body) do
      """
      defmodule #{name} do
        use Decree.Decision.Rule

        def query(_command, _context), do: []
        #{body}
      end
      """
    end

    defp build_name(label) do
      "Decree.Decision.RuleUseTest.#{label}#{System.unique_integer([:positive])}"
    end

    test "defaults to none, and is read when it is defined" do
      assert %{index_families: families} = compile().__rule__()
      assert families == %{}

      assert %{index_families: %{"email" => :digest}} =
               compile("", ~s(def index_families, do: %{"email" => :digest})).__rule__()
    end

    test "a later `use` can inject it" do
      name = build_name("Injected")
      rule = Module.concat([name])

      assert {dir, {:ok, _modules, _warnings}} =
               build(name, injecting(name, ~s(%{"email" => :digest})))

      assert {:module, ^rule} = :code.load_abs(~c"#{Path.join(dir, "Elixir." <> name)}")

      on_exit(fn ->
        :code.purge(rule)
        :code.delete(rule)
      end)

      assert %{index_families: %{"email" => :digest}} = rule.__rule__()
    end

    for {name, answer} <- [
          {"an atom key", ~s(%{email: :digest})},
          {"a key with a colon", ~s(%{"email:x" => :digest})},
          {"an unknown family", ~s(%{"email" => :digests})},
          {"something other than a map", ~s([{"email", :digest}])},
          {"nil", ~s(nil)}
        ] do
      test "fails the build for #{name}" do
        body = "def index_families, do: #{unquote(answer)}"

        assert_raise ArgumentError, ~r/Fixture\d+ has an invalid `index_families/, fn ->
          compile("", body)
        end
      end
    end

    for kind <- ~w(command command_kind correlation tag_index tag_index_state) do
      test "fails the build listing `#{kind}`" do
        body = ~s(def index_families, do: %{"#{unquote(kind)}" => :digest})

        assert_raise ArgumentError, ~r/lists `#{unquote(kind)}` in `index_families\/0`/, fn ->
          compile("", body)
        end
      end
    end

    test "fails under the compiler too, where the module is not loaded yet" do
      name = build_name("Compiled")

      capture_io(:stderr, fn ->
        assert {_dir, {:error, [%{message: message}], _warnings}} =
                 build(name, declaring(name, "def index_families, do: %{email: :digest}"))

        assert message =~ "#{name} has an invalid `index_families/0`"
      end)
    end

    test "fails the build when a later `use` injects a bad one" do
      name = build_name("InjectedBad")

      capture_io(:stderr, fn ->
        assert {_dir, {:error, [%{message: message}], _warnings}} =
                 build(name, injecting(name, "%{email: :digest}"))

        assert message =~ "#{name} has an invalid `index_families/0`"
      end)
    end

    test "fails the build when it is a `defp`, and writes no module" do
      name = build_name("Private")
      body = ~s(defp index_families, do: %{"email" => :digest})

      capture_io(:stderr, fn ->
        assert {dir, {:error, errors, _warnings}} = build(name, declaring(name, body))

        assert Enum.any?(
                 errors,
                 &(&1.message =~ "index_families/0" and &1.message =~ ~r/\bdefp\b/)
               )

        assert File.ls!(dir) == ["decision.ex"]
      end)
    end

    test "the same module with a `def` builds, so it is the `defp` that fails" do
      name = build_name("Public")
      body = ~s(def index_families, do: %{"email" => :digest})

      assert {dir, {:ok, _modules, _warnings}} = build(name, declaring(name, body))
      assert "Elixir.#{name}.beam" in File.ls!(dir)
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
