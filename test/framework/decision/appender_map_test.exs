defmodule Decree.Decision.AppenderMapTest do
  use ExUnit.Case, async: true

  alias Decree.Decision.AppenderMap

  defmodule EmailClaimed do
    @enforce_keys [:digest]
    defstruct [:digest]

    def __tag_kinds__, do: ["email", "tenant"]
    def __conditional_tag_kinds__, do: ["tenant", "phantom"]
  end

  defmodule NoExports do
    defstruct [:id]
  end

  defmodule NotAnEvent do
  end

  defmodule ClaimDecision do
  end

  defmodule OtherDecision do
  end

  defmodule CreateDecision do
    use Decree.DecisionFixtures.Scripting,
      create: [types: ["Elixir.Decree.Decision.AppenderMapTest.EmailClaimed"]]
  end

  defmodule TwoFactsDecision do
    use Decree.DecisionFixtures.Scripting,
      create: [
        types: [
          "Elixir.Decree.Decision.AppenderMapTest.EmailClaimed",
          "Elixir.Decree.Decision.AppenderMapTest.NoExports"
        ]
      ]
  end

  defmodule NoCreateDecision do
    use Decree.DecisionFixtures.Scripting
  end

  defmodule ClaimCommand do
  end

  defmodule ImportCommand do
  end

  defmodule UserAggregate do
  end

  @claimed Atom.to_string(EmailClaimed)
  @no_exports Atom.to_string(NoExports)

  defp item(type \\ @claimed, tags \\ ["email:d"]), do: [types: [type], tags: tags]

  defp own_row, do: %{EmailClaimed => [{ClaimCommand, ClaimDecision}]}

  describe "required-tag coverage" do
    test "a kind the module lists passes" do
      assert AppenderMap.check([{ClaimDecision, [item()]}], own_row(), []) == []
    end

    test "a map item reads as a keyword one" do
      item = %{types: [@claimed], tags: ["email:d"]}

      assert AppenderMap.check([{ClaimDecision, [item]}], own_row(), []) == []
    end

    test "the framework's kinds need no listing, a tag with no colon being its own kind" do
      tags = ["command:1", "command_kind:K", "correlation:c", "correlation"]

      assert AppenderMap.check([{ClaimDecision, [item(@claimed, tags)]}], own_row(), []) == []
    end

    test "a kind the implementation can't produce is a problem" do
      assert AppenderMap.check([{ClaimDecision, [item(@claimed, ["phone:1"])]}], own_row(), []) ==
               [{:missing_tag_kind, @claimed, "phone", ClaimDecision}]
    end

    test "a conditional kind it can't produce is still one" do
      assert AppenderMap.check([{ClaimDecision, [item(@claimed, ["phantom:1"])]}], own_row(), []) ==
               [{:missing_tag_kind, @claimed, "phantom", ClaimDecision}]
    end

    test "a module with no `__tag_kinds__/0` produces the framework's kinds only" do
      appenders = %{NoExports => [{ClaimCommand, ClaimDecision}]}

      assert AppenderMap.check(
               [{ClaimDecision, [item(@no_exports, ["command:1", "email:d"])]}],
               appenders,
               []
             ) == [{:missing_tag_kind, @no_exports, "email", ClaimDecision}]
    end

    test "a kind is named once, however many items name it" do
      items = [item(@claimed, ["phone:1"]), item(@claimed, ["phone:2"])]

      assert AppenderMap.check([{ClaimDecision, items}], own_row(), []) ==
               [{:missing_tag_kind, @claimed, "phone", ClaimDecision}]
    end

    test "only the items naming the type count for it" do
      items = [item(@claimed, ["email:d"]), item(@no_exports, ["command:1"])]

      appenders = %{
        EmailClaimed => [{ClaimCommand, ClaimDecision}],
        NoExports => [{ClaimCommand, ClaimDecision}]
      }

      assert AppenderMap.check([{ClaimDecision, items}], appenders, []) == []
    end

    test "an item naming no types, the command item's shape, names nothing" do
      assert AppenderMap.check(
               [{ClaimDecision, [[tags: ["command:1"]], item()]}],
               own_row(),
               []
             ) == []
    end
  end

  describe "condition coverage" do
    @appended %{EmailClaimed => [{ClaimCommand, ClaimDecision}, {ImportCommand, UserAggregate}]}

    test "the rule's own decision covers its row" do
      assert AppenderMap.check([{ClaimDecision, [item()]}], own_row(), []) == []
    end

    test "an aggregate appending the type is unchecked" do
      assert AppenderMap.check([{ClaimDecision, [item()]}], @appended, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, UserAggregate, ClaimDecision}]
    end

    test "`conditional:` covers it" do
      assert AppenderMap.check([{ClaimDecision, [item()]}], @appended,
               conditional: [ImportCommand]
             ) ==
               []
    end

    test "`dominant:` covers it, for that event and that command" do
      assert AppenderMap.check([{ClaimDecision, [item()]}], @appended,
               dominant: [{EmailClaimed, ImportCommand}]
             ) == []

      assert [{:unchecked_appender, _, ImportCommand, _, _}] =
               AppenderMap.check([{ClaimDecision, [item()]}], @appended,
                 dominant: [{NoExports, ImportCommand}]
               )
    end

    test "another decision covers it with at least these kinds" do
      appenders = %{EmailClaimed => [{ImportCommand, OtherDecision}]}

      wider = item(@claimed, ["email:d", "tenant:t"])

      assert AppenderMap.check(
               [{ClaimDecision, [item()]}, {OtherDecision, [wider]}],
               appenders,
               []
             ) == []
    end

    test "another decision with fewer kinds does not" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, ClaimDecision}, {ImportCommand, OtherDecision}]
      }

      wider = item(@claimed, ["email:d", "tenant:t"])

      assert AppenderMap.check(
               [{ClaimDecision, [wider]}, {OtherDecision, [item()]}],
               appenders,
               []
             ) == [{:unchecked_appender, @claimed, ImportCommand, OtherDecision, ClaimDecision}]
    end

    test "another decision not naming the type does not" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, ClaimDecision}, {ImportCommand, OtherDecision}],
        NoExports => [{ClaimCommand, OtherDecision}]
      }

      assert AppenderMap.check(
               [{ClaimDecision, [item()]}, {OtherDecision, [item(@no_exports, [])]}],
               appenders,
               []
             ) == [{:unchecked_appender, @claimed, ImportCommand, OtherDecision, ClaimDecision}]
    end

    test "every rule of the other decision naming the type has to cover it" do
      appenders = %{EmailClaimed => [{ImportCommand, OtherDecision}]}

      rules = [
        {ClaimDecision, [item()]},
        {OtherDecision, [item()]},
        {OtherDecision, [item(@claimed, ["tenant:t"])]}
      ]

      assert AppenderMap.check(rules, appenders, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, OtherDecision, ClaimDecision}]
    end

    test "two rules of the other decision, both covering, answer nothing" do
      appenders = %{EmailClaimed => [{ImportCommand, OtherDecision}]}

      rules = [
        {ClaimDecision, [item()]},
        {OtherDecision, [item()]},
        {OtherDecision, [item(@claimed, ["email:e", "tenant:t"])]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "a rule of the other decision not naming the type says nothing" do
      appenders = %{
        EmailClaimed => [{ImportCommand, OtherDecision}],
        NoExports => [{ImportCommand, OtherDecision}]
      }

      rules = [
        {ClaimDecision, [item()]},
        {OtherDecision, [item()]},
        {OtherDecision, [item(@no_exports, [])]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "a type with no row is a problem, and so is an empty one" do
      for appenders <- [%{}, %{EmailClaimed => []}] do
        assert AppenderMap.check([{ClaimDecision, [item()]}], appenders, []) ==
                 [{:no_appender_row, @claimed, ClaimDecision}]
      end
    end
  end

  describe "a create" do
    @created %{EmailClaimed => [{ClaimCommand, CreateDecision}]}

    test "with no items has no fact item" do
      assert AppenderMap.check([{CreateDecision, []}], @created, []) ==
               [{:no_fact_item, CreateDecision}]
    end

    test "an item with another tag count is no fact item" do
      for tags <- [[], ["email:d", "tenant:t"]] do
        assert AppenderMap.check([{CreateDecision, [item(@claimed, tags)]}], @created, []) ==
                 [{:no_fact_item, CreateDecision}]
      end
    end

    test "an item naming a subset of the declared types is no fact item" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, TwoFactsDecision}],
        NoExports => [{ClaimCommand, TwoFactsDecision}]
      }

      for type <- [@claimed, @no_exports] do
        assert AppenderMap.check(
                 [{TwoFactsDecision, [item(type, ["correlation:c"])]}],
                 appenders,
                 []
               ) == [{:no_fact_item, TwoFactsDecision}]
      end
    end

    test "an item naming a type past the declared ones is no fact item" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, CreateDecision}],
        NoExports => [{ClaimCommand, CreateDecision}]
      }

      item = [types: [@claimed, @no_exports], tags: ["correlation:c"]]

      assert AppenderMap.check([{CreateDecision, [item]}], appenders, []) ==
               [{:no_fact_item, CreateDecision}]
    end

    test "a fact item behind a query item is no fact item" do
      items = [item(@claimed, ["email:d", "tenant:t"]), item()]

      assert AppenderMap.check([{CreateDecision, items}], @created, []) ==
               [{:no_fact_item, CreateDecision}]
    end

    test "with no fact item at the head keeps all its items for another rule" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      query_item = item(@claimed, ["email:d", "tenant:t"])
      rules = [{ClaimDecision, [item()]}, {CreateDecision, [query_item]}]

      assert AppenderMap.check(rules, appenders, []) == [{:no_fact_item, CreateDecision}]
    end

    test "the declared types may come in any order" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, TwoFactsDecision}],
        NoExports => [{ClaimCommand, TwoFactsDecision}]
      }

      item = [types: [@no_exports, @claimed], tags: ["correlation:c"]]

      assert AppenderMap.check([{TwoFactsDecision, [item]}], appenders, []) == []
    end

    test "comes ahead of the rule's type problems" do
      item = item(@claimed, ["email:d", "phone:1"])

      assert AppenderMap.check([{CreateDecision, [item]}], %{}, []) == [
               {:no_fact_item, CreateDecision},
               {:missing_tag_kind, @claimed, "phone", CreateDecision},
               {:no_appender_row, @claimed, CreateDecision}
             ]
    end

    test "`verify!/3` raises naming it" do
      error =
        assert_raise ArgumentError, fn ->
          AppenderMap.verify!([{CreateDecision, []}], @created, [])
        end

      assert error.message =~ inspect({:no_fact_item, CreateDecision})
    end

    test "with the fact item is clean, a map item included" do
      for fact <- [item(), %{types: [@claimed], tags: ["email:d"]}] do
        assert AppenderMap.check([{CreateDecision, [fact]}], @created, []) == []
      end

      assert AppenderMap.verify!([{CreateDecision, [item()]}], @created, []) == :ok
    end

    test "with the fact item, a plain aggregate row is unchecked" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, CreateDecision}, {ImportCommand, UserAggregate}]
      }

      assert AppenderMap.check([{CreateDecision, [item()]}], appenders, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, UserAggregate, CreateDecision}]
    end

    test "with the fact item, a kind the module can't produce is a problem" do
      assert AppenderMap.check([{CreateDecision, [item(@claimed, ["phone:1"])]}], @created, []) ==
               [{:missing_tag_kind, @claimed, "phone", CreateDecision}]
    end

    test "the fact item need not be alone" do
      items = [item(), item(@claimed, ["phone:1", "email:d"])]

      assert AppenderMap.check([{CreateDecision, items}], @created, []) ==
               [{:missing_tag_kind, @claimed, "phone", CreateDecision}]
    end

    test "a kind on the fact item alone does not cover another rule" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, ClaimDecision}, {ImportCommand, CreateDecision}]
      }

      rules = [{ClaimDecision, [item()]}, {CreateDecision, [item()]}]

      assert AppenderMap.check(rules, appenders, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, CreateDecision, ClaimDecision}]
    end

    test "the same kind on a query item of the create's rule covers another rule" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, ClaimDecision}, {ImportCommand, CreateDecision}]
      }

      # The second has the fact item's shape, and one item is dropped, not every such item.
      for query_item <- [item(@claimed, ["email:d", "email:e"]), item(@claimed, ["email:e"])] do
        rules = [{ClaimDecision, [item()]}, {CreateDecision, [item(), query_item]}]

        assert AppenderMap.check(rules, appenders, []) == []
      end
    end

    test "the fact item is dropped, and not the query item after it" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      fact = item(@claimed, ["tenant:t"])
      rules = [{ClaimDecision, [item()]}, {CreateDecision, [fact, item()]}]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "a fact item alone still reads the type, for a rule asking no kind of it" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      rules = [{ClaimDecision, [item(@claimed, [])]}, {CreateDecision, [item()]}]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "two rules of one create, one a fact item alone, do not cover another rule" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      fact = item(@claimed, ["tenant:t"])

      rules = [
        {ClaimDecision, [item()]},
        {CreateDecision, [fact, item()]},
        {CreateDecision, [fact]}
      ]

      assert AppenderMap.check(rules, appenders, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, CreateDecision, ClaimDecision}]
    end

    test "two rules of one create, both covering, answer nothing" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      fact = item(@claimed, ["tenant:t"])

      rules = [
        {ClaimDecision, [item()]},
        {CreateDecision, [fact, item()]},
        {CreateDecision, [fact, item(@claimed, ["email:e", "tenant:t"])]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "a rule of one create not naming the type says nothing" do
      appenders = %{
        EmailClaimed => [{ImportCommand, CreateDecision}],
        NoExports => [{ImportCommand, CreateDecision}]
      }

      rules = [
        {ClaimDecision, [item(@no_exports, ["correlation:c"])]},
        {CreateDecision, [item(), item(@no_exports, ["correlation:c"])]},
        {CreateDecision, [item()]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "is nothing to a decision that says `create: false`" do
      assert AppenderMap.check([{NoCreateDecision, []}], %{}, []) == []
    end

    test "is nothing to a module with no `__decision__/0`" do
      assert AppenderMap.check([{ClaimDecision, []}], %{}, []) == []
    end
  end

  describe "a declared aggregate's rule" do
    import Decree.BuildFixture, only: [build_in_order: 2]
    import ExUnit.CaptureIO, only: [capture_io: 2]

    alias Decree.Decision.Rule.Result
    alias Decree.Event.TagContext
    alias Decree.RuleFixtures.{EmailRule, Erased, EraseUser, Registered, RegisterUser, User}

    @registered Atom.to_string(Registered)

    defp part_one do
      EmailRule.query(%RegisterUser{digest: "d", rule_result: %Result{}}, %TagContext{})
    end

    defp part_two do
      registration = %Registered{user_id: "candidate", digest: "d"}

      EmailRule.followup_query(%RegisterUser{}, %TagContext{}, [registration])
    end

    defp row, do: %{Registered => [{RegisterUser, User}]}

    test "is no create, having no `__decision__/0`" do
      refute function_exported?(EmailRule, :__decision__, 0)

      assert AppenderMap.check([{EmailRule, []}], %{}) == []
    end

    test "listed by its module passes with its command in `conditional:`" do
      assert AppenderMap.check([{EmailRule, part_one()}], row(), conditional: [RegisterUser]) ==
               []
    end

    test "listed by its module is unchecked without it" do
      assert AppenderMap.check([{EmailRule, part_one()}], row()) ==
               [{:unchecked_appender, @registered, RegisterUser, User, EmailRule}]
    end

    test "both its parts go through, each type with its own rows" do
      items = part_one() ++ part_two()

      appenders =
        Map.merge(row(), %{
          Erased => [{EraseUser, User}],
          Decree.RuleFixtures.Removed => [{Decree.RuleFixtures.RemoveUser, User}]
        })

      assert AppenderMap.check([{EmailRule, items}], appenders,
               conditional: [RegisterUser],
               dominant: [
                 {Erased, EraseUser},
                 {Decree.RuleFixtures.Removed, Decree.RuleFixtures.RemoveUser}
               ]
             ) == []

      assert [
               {:unchecked_appender, _erased, EraseUser, User, EmailRule},
               {:unchecked_appender, _removed, Decree.RuleFixtures.RemoveUser, User, EmailRule}
             ] = AppenderMap.check([{EmailRule, items}], appenders, conditional: [RegisterUser])
    end

    test "a kind its event can't produce is a problem" do
      items = [[types: [@registered], tags: ["phone:1"]]]

      assert [{:missing_tag_kind, @registered, "phone", EmailRule} | _] =
               AppenderMap.check([{EmailRule, items}], row(), conditional: [RegisterUser])
    end

    defmodule ImportUser do
    end

    # An aggregate whose routes go `to:` a handler has `apply/2` and no `execute/2`.
    defmodule HandledUser do
      def apply(state, _event), do: state
    end

    defp imported, do: %{Registered => [{RegisterUser, User}, {ImportUser, User}]}

    test "listed under the aggregate is not its own row" do
      assert AppenderMap.check([{User, part_one()}], row()) ==
               [{:unchecked_appender, @registered, RegisterUser, User, User}]
    end

    test "listed under the aggregate leaves a plain command unchecked" do
      assert AppenderMap.check([{User, part_one()}], imported(), conditional: [RegisterUser]) ==
               [{:unchecked_appender, @registered, ImportUser, User, User}]
    end

    test "listed by its module leaves the same plain command unchecked" do
      rules = [{EmailRule, part_one()}]

      assert AppenderMap.check(rules, imported(), conditional: [RegisterUser]) ==
               [{:unchecked_appender, @registered, ImportUser, User, EmailRule}]
    end

    test "listed under the aggregate covers no other rule, with these kinds" do
      other = {ClaimDecision, part_one()}

      assert AppenderMap.check([other, {User, part_one()}], row()) == [
               {:unchecked_appender, @registered, RegisterUser, User, ClaimDecision},
               {:unchecked_appender, @registered, RegisterUser, User, User}
             ]
    end

    test "listed under the aggregate covers no other rule, with more kinds" do
      other = {ClaimDecision, part_one()}
      wider = {User, [[types: [@registered], tags: ["email:d", "user:u"]]]}

      assert AppenderMap.check([other, wider], row()) == [
               {:unchecked_appender, @registered, RegisterUser, User, ClaimDecision},
               {:unchecked_appender, @registered, RegisterUser, User, User}
             ]
    end

    test "under the aggregate, `conditional:` covers what it names and nothing else" do
      rules = [{ClaimDecision, part_one()}, {User, part_one()}]

      assert AppenderMap.check(rules, row(), conditional: [RegisterUser]) == []

      assert AppenderMap.check(rules, imported(), conditional: [RegisterUser]) == [
               {:unchecked_appender, @registered, ImportUser, User, ClaimDecision},
               {:unchecked_appender, @registered, ImportUser, User, User}
             ]
    end

    test "an aggregate with `apply/2` and no `execute/2` covers nothing by its name" do
      refute function_exported?(HandledUser, :execute, 2)

      appenders = %{Registered => [{RegisterUser, HandledUser}]}
      rules = [{ClaimDecision, part_one()}, {HandledUser, part_one()}]

      assert AppenderMap.check(rules, appenders) == [
               {:unchecked_appender, @registered, RegisterUser, HandledUser, ClaimDecision},
               {:unchecked_appender, @registered, RegisterUser, HandledUser, HandledUser}
             ]
    end

    # A decision or a rule may have helpers named like an aggregate's callbacks.
    defmodule HelperDecision do
      use Decree.Decision, create: false

      @impl Decree.Decision
      def command_window_ms, do: Decree.DecisionFixtures.command_window_ms()

      @impl Decree.Decision
      def query(_command, _tag_context), do: []

      @impl Decree.Decision
      def initial_state, do: []

      @impl Decree.Decision
      def evolve(state, event), do: state ++ [event]

      @impl Decree.Decision
      def decide(_state, _command), do: []

      def execute(state, _command), do: state
      def apply(state, _event), do: state
    end

    defmodule HelperRule do
      use Decree.Decision.Rule

      @impl Decree.Decision.Rule
      def query(_command, _tag_context), do: []

      def execute(state, _command), do: state
      def apply(state, _event), do: state
    end

    for {label, key} <- [{"a decision", HelperDecision}, {"a rule", HelperRule}] do
      test "#{label} with `execute/2` and `apply/2` helpers covers its own row" do
        key = unquote(key)

        assert function_exported?(key, :execute, 2) and function_exported?(key, :apply, 2)

        assert AppenderMap.check([{key, [item()]}], %{EmailClaimed => [{ClaimCommand, key}]}) ==
                 []
      end

      test "#{label} with `execute/2` and `apply/2` helpers covers another rule's row" do
        key = unquote(key)
        rules = [{ClaimDecision, [item()]}, {key, [item()]}]

        assert AppenderMap.check(rules, %{EmailClaimed => [{ClaimCommand, key}]}) == []
      end
    end

    test "a key that names no module covers nothing by its name" do
      missing = Module.concat([Nope, Missing])
      rules = [{ClaimDecision, [item()]}, {missing, [item()]}]

      assert AppenderMap.check(rules, %{EmailClaimed => [{ClaimCommand, missing}]}) == [
               {:unchecked_appender, @claimed, ClaimCommand, missing, ClaimDecision},
               {:unchecked_appender, @claimed, ClaimCommand, missing, missing}
             ]
    end

    defp later_name(label) do
      "Decree.Decision.AppenderMapTest.#{label}#{System.unique_integer([:positive])}"
    end

    # A build check in the first file, run before the key the second file defines is compiled.
    defp check_source(name, key, items) do
      """
      defmodule #{name}.Check do
        Decree.Decision.AppenderMap.verify!(
          [{#{key}, #{inspect(items)}}],
          %{#{inspect(EmailClaimed)} => [{#{inspect(ClaimCommand)}, #{key}}]}
        )
      end
      """
    end

    test "an aggregate compiled later in the same build covers nothing by its name" do
      name = later_name("LaterAggregate")
      key = Module.concat([name, Aggregate])

      aggregate = """
      defmodule #{inspect(key)} do
        def execute(state, _command), do: state
        def apply(state, _event), do: state
      end
      """

      check = check_source(name, inspect(key), [item()])

      capture_io(:stderr, fn ->
        assert {_dir, {:error, [%{message: message}], _warnings}} =
                 build_in_order(name, [check, aggregate])

        assert message =~ inspect({:unchecked_appender, @claimed, ClaimCommand, key, key})
      end)
    end

    test "a decision compiled later in the same build covers its own row" do
      name = later_name("LaterDecision")
      key = Module.concat([name, Decision])

      decision = """
      defmodule #{inspect(key)} do
        use Decree.Decision, create: false

        @impl Decree.Decision
        def command_window_ms, do: 300_000

        @impl Decree.Decision
        def query(_command, _tag_context), do: []

        @impl Decree.Decision
        def initial_state, do: []

        @impl Decree.Decision
        def evolve(state, event), do: state ++ [event]

        @impl Decree.Decision
        def decide(_state, _command), do: []

        def execute(state, _command), do: state
      end
      """

      check = check_source(name, inspect(key), [item()])

      assert {_dir, {:ok, _modules, _warnings}} = build_in_order(name, [check, decision])
    end

    test "a create decision compiled later in the same build still needs its fact item" do
      name = later_name("LaterCreate")
      key = Module.concat([name, Create])

      create = """
      defmodule #{inspect(key)} do
        use Decree.Decision, create: [types: [#{inspect(@claimed)}]]

        @impl Decree.Decision
        def command_window_ms, do: 300_000

        @impl Decree.Decision
        def query(_command, _tag_context), do: []

        @impl Decree.Decision
        def initial_state, do: []

        @impl Decree.Decision
        def evolve(state, event), do: state ++ [event]

        @impl Decree.Decision
        def decide(_state, _command), do: []
      end
      """

      check = check_source(name, inspect(key), [item(@claimed, ["email:d", "tenant:t"])])

      capture_io(:stderr, fn ->
        assert {_dir, {:error, [%{message: message}], _warnings}} =
                 build_in_order(name, [check, create])

        assert message =~ inspect({:no_fact_item, key})
      end)
    end
  end

  describe "a type" do
    test "with no module is a problem and not a crash" do
      type = "Elixir.Decree.Decision.AppenderMapTest.NeverDefined"

      assert AppenderMap.check([{ClaimDecision, [item(type, ["email:d"])]}], %{}, []) ==
               [{:unknown_type, type, ClaimDecision}]
    end

    test "that names no atom at all is one too" do
      type = "not an atom #{System.unique_integer()}"

      assert AppenderMap.check([{ClaimDecision, [item(type)]}], %{}, []) ==
               [{:unknown_type, type, ClaimDecision}]
    end

    test "naming a module with no struct is one too" do
      type = Atom.to_string(NotAnEvent)

      assert AppenderMap.check([{ClaimDecision, [item(type)]}], %{}, []) ==
               [{:unknown_type, type, ClaimDecision}]
    end

    test "with `@enforce_keys` builds nothing that trips them" do
      assert AppenderMap.check([{ClaimDecision, [item()]}], own_row()) == []
    end
  end

  describe "`verify!/3`" do
    test "answers `:ok` with no problems" do
      assert AppenderMap.verify!([{ClaimDecision, [item()]}], own_row(), []) == :ok
    end

    test "raises `ArgumentError` naming every problem" do
      rules = [
        {ClaimDecision,
         [item(@claimed, ["phone:1"]), item(@no_exports, []), item("no such type", [])]}
      ]

      error =
        assert_raise ArgumentError, fn -> AppenderMap.verify!(rules, @appended, []) end

      for problem <- [
            {:missing_tag_kind, @claimed, "phone", ClaimDecision},
            {:unchecked_appender, @claimed, ImportCommand, UserAggregate, ClaimDecision},
            {:no_appender_row, @no_exports, ClaimDecision},
            {:unknown_type, "no such type", ClaimDecision}
          ] do
        assert error.message =~ inspect(problem)
      end
    end
  end
end
