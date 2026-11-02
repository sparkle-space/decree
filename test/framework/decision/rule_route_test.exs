defmodule Decree.Decision.RuleRouteTest do
  use Decree.RuleCase

  import ExUnit.CaptureLog

  alias Decree.EventStore.Adapters.EventStore, as: Adapter
  alias Decree.FrameworkStorage

  alias Decree.RuleFixtures.{
    AdoptedAggregate,
    ComposedRouter,
    DeclaringStore,
    OutsideRouter,
    PickyAggregate,
    PlainRouter,
    Router,
    StrayTargetApp,
    UndeclaredApp,
    Unrouted
  }

  alias Decree.RuleFixtures.OutsideRouter.Target

  defp compile(body) do
    router = "Decree.Decision.RuleRouteTest.Fixture#{System.unique_integer([:positive])}"

    Code.eval_string("""
    alias Decree.DecisionFixtures.{Claim, ClaimDecision, ParityAggregate, ParityAggregateCommand}
    alias Decree.RuleFixtures.{RegisterUser, ScriptedRule, User}

    defmodule #{router} do
      use Decree.Commands.Router

      #{body}
    end
    """)

    Module.concat([router])
  end

  describe "the build refuses" do
    test "a command with no `:rule_result` field" do
      assert_raise ArgumentError, ~r/has no `:rule_result` field/, fn ->
        compile("""
        dispatch ParityAggregateCommand,
          to: ParityAggregate,
          identity: :id,
          identity_prefix: "x-",
          rule: ScriptedRule
        """)
      end
    end

    test "`rule:` naming anything but a rule" do
      for rule <- ["ClaimDecision", "Decree.RuleFixtures.Nowhere", ~s("EmailRule")] do
        assert_raise ArgumentError, ~r/invalid `rule` option/, fn ->
          compile("dispatch RegisterUser, to: User, identity: :user_id, rule: #{rule}")
        end
      end
    end

    test "`rule:` on `decide/2`, because a decision is its own rule" do
      assert_raise RuntimeError, ~r/unexpected decide parameter "rule"/, fn ->
        compile("""
        decide Claim,
          to: ClaimDecision,
          identity: :claim_id,
          identity_prefix: "x-",
          rule: ScriptedRule
        """)
      end
    end
  end

  describe "`__conditional_targets__/0`" do
    test "on a router names the aggregates of its rule routes, once each" do
      assert Enum.sort(Router.__conditional_targets__()) ==
               Enum.sort(Stores.targets())

      assert OutsideRouter.__conditional_targets__() == [Target]
    end

    test "on a router with no rule route is empty" do
      assert PlainRouter.__conditional_targets__() == []
    end

    test "names a route's aggregate, not its handler" do
      router =
        compile("""
        defmodule Handler do
          def handle(_state, _command), do: []
        end

        dispatch RegisterUser, to: Handler, aggregate: User, identity: :user_id, rule: ScriptedRule
        """)

      assert router.__conditional_targets__() == [User]
    end

    test "on a composite and an application gathers what they include" do
      assert ComposedRouter.__conditional_targets__() == [Target]

      assert Enum.sort(App.__conditional_targets__()) == Enum.sort(Stores.targets())
    end
  end

  describe "at start" do
    test "a rule route's aggregate with no store target raises, naming both" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: message}, _stacktrace}} =
               UndeclaredApp.start_link()

      assert message =~ inspect(UndeclaredApp)
      assert message =~ "conditional_appends"

      assert Enum.any?(
               [User, RuledAggregate, AdoptedAggregate, PickyAggregate],
               &(message =~ inspect(&1))
             )
    end

    test "a store target no route declares warns, and starts" do
      log =
        capture_log(fn ->
          start_supervised!(StrayTargetApp)
        end)

      assert log =~ "declares the conditional target #{inspect(User)}"
    end

    test "matching declarations start with no warning" do
      log =
        capture_log(fn ->
          start_supervised!({App, name: :agreeing_rules})
        end)

      refute log =~ "conditional target"
    end
  end

  describe "the adapter's `conditional_targets/1`" do
    test "reads the store's `use` line, with no store running" do
      {:ok, _child_spec, meta} = Adapter.child_spec(App, event_store: DeclaringStore)

      assert Adapter.conditional_targets(meta) == [User]
    end

    test "reads what the application's config gives the store" do
      {:ok, _child_spec, meta} =
        Adapter.child_spec(App,
          event_store: DeclaringStore,
          conditional_appends: [targets: [RuledAggregate]]
        )

      assert Adapter.conditional_targets(meta) == [RuledAggregate]
    end

    test "answers `[]` for a store that declares nothing" do
      {:ok, _child_spec, meta} =
        Adapter.child_spec(App, FrameworkStorage.event_store_config() |> Keyword.delete(:adapter))

      assert Adapter.conditional_targets(meta) == []
    end
  end

  test "a router outside the application is refused by the store at dispatch" do
    command = ruled([], Unrouted)

    assert {:error, :undeclared_conditional_target} =
             OutsideRouter.dispatch(command, application: App)

    assert stored("outside-" <> command.id, App) == []
  end
end
