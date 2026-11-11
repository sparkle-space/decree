defmodule Decree.Decision.RuleRouteTest do
  use Decree.RuleCase

  import ExUnit.CaptureLog

  alias Decree.EventStore.Adapters.EventStore, as: Adapter
  alias Decree.FrameworkStorage

  alias Decree.RuleFixtures.{
    AdoptedAggregate,
    ComposedRouter,
    CountingStore,
    DecisionPrefixRouter,
    DeclaringStore,
    OutsideRouter,
    PickyAggregate,
    PlainApp,
    PlainDeclaringApp,
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

  # A store whose `init/1` counts its calls, and declares `first_targets` on the first only.
  defp counting_store(counter, first_targets) do
    FrameworkStorage.event_store_config(
      event_store: CountingStore,
      init_calls: counter,
      first_targets: first_targets
    )
  end

  describe "at start" do
    test "a rule route's aggregate with no store target raises, naming both" do
      Process.flag(:trap_exit, true)

      assert {:error, {:shutdown, {:failed_to_start_child, id, failure}}} =
               UndeclaredApp.start_link()

      assert id == {Decree.Application.Supervisor, :conditional_targets}
      assert {:EXIT, {%ArgumentError{message: message}, _stacktrace}} = failure

      assert message =~ inspect(UndeclaredApp)
      assert message =~ "conditional_appends"

      assert Enum.any?(
               [User, RuledAggregate, AdoptedAggregate, PickyAggregate],
               &(message =~ inspect(&1))
             )
    end

    test "a store target no rule route names warns, and starts" do
      log =
        capture_log(fn ->
          start_supervised!(StrayTargetApp)
        end)

      assert log =~ "declares the conditional target #{inspect(Target)}"
    end

    test "an application with no rule route is not checked, and starts with no warning" do
      log =
        capture_log(fn ->
          start_supervised!(PlainDeclaringApp)
        end)

      refute log =~ "conditional target"
    end

    test "an application with no rule route calls its store's `init/1` once" do
      counter = :counters.new(1, [])

      start_supervised!(
        {PlainApp, name: :counting_plain, event_store: counting_store(counter, [])}
      )

      assert :counters.get(counter, 1) == 1
    end

    # The store's `init/1` declares the targets on its first call and nothing after. The check
    # reads what the running store parsed, so it agrees with what that store refuses.
    test "checks what the running store declares" do
      counter = :counters.new(1, [])
      event_store = counting_store(counter, Stores.targets())

      start_supervised!({App, name: :counting_rules, event_store: event_store})

      assert :ok = App.dispatch(ruled(), application: :counting_rules)
      assert :counters.get(counter, 1) == 1
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
    test "reads the running store's `use` line" do
      start_supervised!({DeclaringStore, name: :declaring_store})

      meta = %{event_store: DeclaringStore, name: :declaring_store}

      assert Adapter.conditional_targets(meta) == [User]
    end

    test "reads what the application's config gave the running store" do
      {_adapter, meta} = Decree.Application.event_store_adapter(App)

      assert Enum.sort(Adapter.conditional_targets(meta)) == Enum.sort(Stores.targets())
    end

    test "answers `[]` for a running store that declares nothing" do
      start_supervised!(PlainApp)

      {_adapter, meta} = Decree.Application.event_store_adapter(PlainApp)

      assert Adapter.conditional_targets(meta) == []
    end
  end

  test "a router outside the application is refused by the store at dispatch" do
    command = ruled([], Unrouted)

    assert {:error, :undeclared_conditional_target} =
             OutsideRouter.dispatch(command, application: App)

    assert stored("outside-" <> command.id, App) == []
  end

  # The store declares the prefix and no `User` target, and no start check saw the router. A
  # rule's condition names its aggregate, so the prefix does not declare it.
  test "a rule route under a declared prefix is refused while its target is not declared" do
    event_store =
      FrameworkStorage.event_store_config(
        event_store: Decree.RuleFixtures.RecordingStore,
        conditional_appends: [stream_prefixes: ["decision-"]]
      )

    start_supervised!({PlainApp, name: :decision_prefix, event_store: event_store})

    command = register(digest())

    assert {:error, :undeclared_conditional_target} =
             DecisionPrefixRouter.dispatch(command, application: :decision_prefix)

    assert stored("decision-" <> command.user_id, :decision_prefix) == []
  end

  test "a rule route under a declared prefix appends once its target is declared" do
    event_store =
      FrameworkStorage.event_store_config(
        event_store: Decree.RuleFixtures.RecordingStore,
        conditional_appends: [stream_prefixes: ["decision-"], targets: [User]]
      )

    start_supervised!({PlainApp, name: :decision_target, event_store: event_store})

    command = register(digest())

    assert :ok = DecisionPrefixRouter.dispatch(command, application: :decision_target)

    assert [%{data: %Registered{}}] = stored("decision-" <> command.user_id, :decision_target)
  end
end
