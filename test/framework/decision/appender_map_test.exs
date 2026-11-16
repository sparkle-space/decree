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
    use Decree.DecisionFixtures.Scripting
  end

  defmodule OtherDecision do
    use Decree.DecisionFixtures.Scripting
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

  defmodule PlainModule do
  end

  @claimed Atom.to_string(EmailClaimed)
  @no_exports Atom.to_string(NoExports)

  defp item(type \\ @claimed, tags \\ ["email:d"]), do: [types: [type], tags: tags]

  defp own_row, do: %{EmailClaimed => [{ClaimCommand, ClaimDecision}]}

  describe "a key" do
    test "that is a bare module raises naming it, before any problem is computed" do
      # The bare key comes second, after a key whose module would raise if it were asked.
      rules = [{{:decision, PlainModule}, [item("no such type")]}, {ClaimDecision, [item()]}]

      for run <- [&AppenderMap.check/3, &AppenderMap.verify!/3] do
        error = assert_raise ArgumentError, fn -> run.(rules, own_row(), []) end

        assert error.message =~ inspect(ClaimDecision)
        refute error.message =~ "unknown_type"
      end
    end

    test "with another role, or no module, raises naming it" do
      for key <- [{:aggregator, UserAggregate}, {:decision, "ClaimDecision"}, {ClaimDecision}] do
        error =
          assert_raise ArgumentError, fn ->
            AppenderMap.check([{key, [item()]}], own_row(), [])
          end

        assert error.message =~ inspect(key)
      end
    end

    test "that is no `{key, items}` pair raises naming it" do
      assert_raise ArgumentError, ~r/ClaimDecision/, fn ->
        AppenderMap.check([{{:decision, ClaimDecision}, [item()], :extra}], own_row(), [])
      end
    end

    test "under a rule role, a module that is no rule raises" do
      for module <- [ClaimDecision, PlainModule] do
        assert_raise UndefinedFunctionError, fn ->
          AppenderMap.check([{{:rule, module}, []}], %{}, [])
        end
      end
    end

    test "under an aggregate role is asked nothing, whatever the module is" do
      for module <- [CreateDecision, Decree.RuleFixtures.EmailRule, PlainModule] do
        assert AppenderMap.check([{{:aggregate, module}, []}], %{}, []) == []
      end
    end

    test "under an aggregate role, a decision module is no create and covers no row" do
      appenders = %{EmailClaimed => [{ClaimCommand, CreateDecision}]}

      assert AppenderMap.check([{{:aggregate, CreateDecision}, [item()]}], appenders, []) ==
               [{:unchecked_appender, @claimed, ClaimCommand, CreateDecision, CreateDecision}]
    end

    test "under a rule role, a row targeting the module is not covered" do
      appenders = %{EmailClaimed => [{ClaimCommand, Decree.RuleFixtures.EmailRule}]}
      rule = Decree.RuleFixtures.EmailRule

      assert AppenderMap.check([{{:rule, rule}, [item()]}], appenders, []) ==
               [{:unchecked_appender, @claimed, ClaimCommand, rule, rule}]
    end
  end

  describe "a compile cycle" do
    # `compile_to_path/3` is what `mix compile` runs; `compile/2` loads what it builds, which
    # would hide the cycle. The check names a decision whose own body needs the checking module,
    # so asking the decision waits on a module that waits on the check.
    test "between the check and a decision key's module fails the build loudly" do
      n = System.unique_integer([:positive])
      check = "Decree.AppenderMapCycle#{n}.Check"
      decision = "Decree.AppenderMapCycle#{n}.Decision"
      dir = Path.join(System.tmp_dir!(), "appender_map_cycle_#{n}")

      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf(dir) end)

      check_path = Path.join(dir, "check.ex")
      decision_path = Path.join(dir, "decision.ex")

      File.write!(check_path, """
      defmodule #{check} do
        Decree.Decision.AppenderMap.verify!([{{:decision, #{decision}}, []}], %{})

        def marker, do: :marker
      end
      """)

      File.write!(decision_path, """
      defmodule #{decision} do
        use Decree.DecisionFixtures.Scripting

        @marker #{check}.marker()
        def marker, do: @marker
      end
      """)

      {result, stderr} =
        ExUnit.CaptureIO.with_io(:stderr, fn ->
          Kernel.ParallelCompiler.compile_to_path([check_path, decision_path], dir,
            return_diagnostics: true
          )
        end)

      assert {:error, errors, _warnings} = result
      assert errors != []

      text = stderr <> inspect(errors)

      assert text =~ "deadlock"
      assert text =~ decision
      refute File.exists?(Path.join(dir, "Elixir.#{check}.beam"))
    end
  end

  describe "required-tag coverage" do
    test "a kind the module lists passes" do
      assert AppenderMap.check([{{:decision, ClaimDecision}, [item()]}], own_row(), []) == []
    end

    test "a map item reads as a keyword one" do
      item = %{types: [@claimed], tags: ["email:d"]}

      assert AppenderMap.check([{{:decision, ClaimDecision}, [item]}], own_row(), []) == []
    end

    test "the framework's kinds need no listing, a tag with no colon being its own kind" do
      tags = ["command:1", "command_kind:K", "correlation:c", "correlation"]

      assert AppenderMap.check(
               [{{:decision, ClaimDecision}, [item(@claimed, tags)]}],
               own_row(),
               []
             ) == []
    end

    test "a kind the implementation can't produce is a problem" do
      assert AppenderMap.check(
               [{{:decision, ClaimDecision}, [item(@claimed, ["phone:1"])]}],
               own_row(),
               []
             ) ==
               [{:missing_tag_kind, @claimed, "phone", ClaimDecision}]
    end

    test "a conditional kind it can't produce is still one" do
      assert AppenderMap.check(
               [{{:decision, ClaimDecision}, [item(@claimed, ["phantom:1"])]}],
               own_row(),
               []
             ) ==
               [{:missing_tag_kind, @claimed, "phantom", ClaimDecision}]
    end

    test "a module with no `__tag_kinds__/0` produces the framework's kinds only" do
      appenders = %{NoExports => [{ClaimCommand, ClaimDecision}]}

      assert AppenderMap.check(
               [{{:decision, ClaimDecision}, [item(@no_exports, ["command:1", "email:d"])]}],
               appenders,
               []
             ) == [{:missing_tag_kind, @no_exports, "email", ClaimDecision}]
    end

    test "a kind is named once, however many items name it" do
      items = [item(@claimed, ["phone:1"]), item(@claimed, ["phone:2"])]

      assert AppenderMap.check([{{:decision, ClaimDecision}, items}], own_row(), []) ==
               [{:missing_tag_kind, @claimed, "phone", ClaimDecision}]
    end

    test "only the items naming the type count for it" do
      items = [item(@claimed, ["email:d"]), item(@no_exports, ["command:1"])]

      appenders = %{
        EmailClaimed => [{ClaimCommand, ClaimDecision}],
        NoExports => [{ClaimCommand, ClaimDecision}]
      }

      assert AppenderMap.check([{{:decision, ClaimDecision}, items}], appenders, []) == []
    end

    test "an item naming no types, the command item's shape, names nothing" do
      assert AppenderMap.check(
               [{{:decision, ClaimDecision}, [[tags: ["command:1"]], item()]}],
               own_row(),
               []
             ) == []
    end
  end

  describe "condition coverage" do
    @appended %{EmailClaimed => [{ClaimCommand, ClaimDecision}, {ImportCommand, UserAggregate}]}

    test "the rule's own decision covers its row" do
      assert AppenderMap.check([{{:decision, ClaimDecision}, [item()]}], own_row(), []) == []
    end

    test "an aggregate appending the type is unchecked" do
      assert AppenderMap.check([{{:decision, ClaimDecision}, [item()]}], @appended, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, UserAggregate, ClaimDecision}]
    end

    test "`conditional:` covers it" do
      assert AppenderMap.check([{{:decision, ClaimDecision}, [item()]}], @appended,
               conditional: [ImportCommand]
             ) ==
               []
    end

    test "`dominant:` covers it, for that event and that command" do
      assert AppenderMap.check([{{:decision, ClaimDecision}, [item()]}], @appended,
               dominant: [{EmailClaimed, ImportCommand}]
             ) == []

      assert [{:unchecked_appender, _, ImportCommand, _, _}] =
               AppenderMap.check([{{:decision, ClaimDecision}, [item()]}], @appended,
                 dominant: [{NoExports, ImportCommand}]
               )
    end

    test "another decision covers it with at least these kinds" do
      appenders = %{EmailClaimed => [{ImportCommand, OtherDecision}]}

      wider = item(@claimed, ["email:d", "tenant:t"])

      assert AppenderMap.check(
               [{{:decision, ClaimDecision}, [item()]}, {{:decision, OtherDecision}, [wider]}],
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
               [{{:decision, ClaimDecision}, [wider]}, {{:decision, OtherDecision}, [item()]}],
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
               [
                 {{:decision, ClaimDecision}, [item()]},
                 {{:decision, OtherDecision}, [item(@no_exports, [])]}
               ],
               appenders,
               []
             ) == [{:unchecked_appender, @claimed, ImportCommand, OtherDecision, ClaimDecision}]
    end

    test "every rule of the other decision naming the type has to cover it" do
      appenders = %{EmailClaimed => [{ImportCommand, OtherDecision}]}

      rules = [
        {{:decision, ClaimDecision}, [item()]},
        {{:decision, OtherDecision}, [item()]},
        {{:decision, OtherDecision}, [item(@claimed, ["tenant:t"])]}
      ]

      assert AppenderMap.check(rules, appenders, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, OtherDecision, ClaimDecision}]
    end

    test "two rules of the other decision, both covering, answer nothing" do
      appenders = %{EmailClaimed => [{ImportCommand, OtherDecision}]}

      rules = [
        {{:decision, ClaimDecision}, [item()]},
        {{:decision, OtherDecision}, [item()]},
        {{:decision, OtherDecision}, [item(@claimed, ["email:e", "tenant:t"])]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "a rule of the other decision not naming the type says nothing" do
      appenders = %{
        EmailClaimed => [{ImportCommand, OtherDecision}],
        NoExports => [{ImportCommand, OtherDecision}]
      }

      rules = [
        {{:decision, ClaimDecision}, [item()]},
        {{:decision, OtherDecision}, [item()]},
        {{:decision, OtherDecision}, [item(@no_exports, [])]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "a type with no row is a problem, and so is an empty one" do
      for appenders <- [%{}, %{EmailClaimed => []}] do
        assert AppenderMap.check([{{:decision, ClaimDecision}, [item()]}], appenders, []) ==
                 [{:no_appender_row, @claimed, ClaimDecision}]
      end
    end
  end

  describe "a create" do
    @created %{EmailClaimed => [{ClaimCommand, CreateDecision}]}

    test "with no items has no fact item" do
      assert AppenderMap.check([{{:decision, CreateDecision}, []}], @created, []) ==
               [{:no_fact_item, CreateDecision}]
    end

    test "an item with another tag count is no fact item" do
      for tags <- [[], ["email:d", "tenant:t"]] do
        assert AppenderMap.check(
                 [{{:decision, CreateDecision}, [item(@claimed, tags)]}],
                 @created,
                 []
               ) ==
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
                 [{{:decision, TwoFactsDecision}, [item(type, ["correlation:c"])]}],
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

      assert AppenderMap.check([{{:decision, CreateDecision}, [item]}], appenders, []) ==
               [{:no_fact_item, CreateDecision}]
    end

    test "a fact item behind a query item is no fact item" do
      items = [item(@claimed, ["email:d", "tenant:t"]), item()]

      assert AppenderMap.check([{{:decision, CreateDecision}, items}], @created, []) ==
               [{:no_fact_item, CreateDecision}]
    end

    test "with no fact item at the head keeps all its items for another rule" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      query_item = item(@claimed, ["email:d", "tenant:t"])

      rules = [
        {{:decision, ClaimDecision}, [item()]},
        {{:decision, CreateDecision}, [query_item]}
      ]

      assert AppenderMap.check(rules, appenders, []) == [{:no_fact_item, CreateDecision}]
    end

    test "the declared types may come in any order" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, TwoFactsDecision}],
        NoExports => [{ClaimCommand, TwoFactsDecision}]
      }

      item = [types: [@no_exports, @claimed], tags: ["correlation:c"]]

      assert AppenderMap.check([{{:decision, TwoFactsDecision}, [item]}], appenders, []) == []
    end

    test "comes ahead of the rule's type problems" do
      item = item(@claimed, ["email:d", "phone:1"])

      assert AppenderMap.check([{{:decision, CreateDecision}, [item]}], %{}, []) == [
               {:no_fact_item, CreateDecision},
               {:missing_tag_kind, @claimed, "phone", CreateDecision},
               {:no_appender_row, @claimed, CreateDecision}
             ]
    end

    test "`verify!/3` raises naming it" do
      error =
        assert_raise ArgumentError, fn ->
          AppenderMap.verify!([{{:decision, CreateDecision}, []}], @created, [])
        end

      assert error.message =~ inspect({:no_fact_item, CreateDecision})
    end

    test "with the fact item is clean, a map item included" do
      for fact <- [item(), %{types: [@claimed], tags: ["email:d"]}] do
        assert AppenderMap.check([{{:decision, CreateDecision}, [fact]}], @created, []) == []
      end

      assert AppenderMap.verify!([{{:decision, CreateDecision}, [item()]}], @created, []) == :ok
    end

    test "with the fact item, a plain aggregate row is unchecked" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, CreateDecision}, {ImportCommand, UserAggregate}]
      }

      assert AppenderMap.check([{{:decision, CreateDecision}, [item()]}], appenders, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, UserAggregate, CreateDecision}]
    end

    test "with the fact item, a kind the module can't produce is a problem" do
      assert AppenderMap.check(
               [{{:decision, CreateDecision}, [item(@claimed, ["phone:1"])]}],
               @created,
               []
             ) ==
               [{:missing_tag_kind, @claimed, "phone", CreateDecision}]
    end

    test "the fact item need not be alone" do
      items = [item(), item(@claimed, ["phone:1", "email:d"])]

      assert AppenderMap.check([{{:decision, CreateDecision}, items}], @created, []) ==
               [{:missing_tag_kind, @claimed, "phone", CreateDecision}]
    end

    test "a kind on the fact item alone does not cover another rule" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, ClaimDecision}, {ImportCommand, CreateDecision}]
      }

      rules = [{{:decision, ClaimDecision}, [item()]}, {{:decision, CreateDecision}, [item()]}]

      assert AppenderMap.check(rules, appenders, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, CreateDecision, ClaimDecision}]
    end

    test "the same kind on a query item of the create's rule covers another rule" do
      appenders = %{
        EmailClaimed => [{ClaimCommand, ClaimDecision}, {ImportCommand, CreateDecision}]
      }

      # The second has the fact item's shape, and one item is dropped, not every such item.
      for query_item <- [item(@claimed, ["email:d", "email:e"]), item(@claimed, ["email:e"])] do
        rules = [
          {{:decision, ClaimDecision}, [item()]},
          {{:decision, CreateDecision}, [item(), query_item]}
        ]

        assert AppenderMap.check(rules, appenders, []) == []
      end
    end

    test "the fact item is dropped, and not the query item after it" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      fact = item(@claimed, ["tenant:t"])

      rules = [
        {{:decision, ClaimDecision}, [item()]},
        {{:decision, CreateDecision}, [fact, item()]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "a fact item alone still reads the type, for a rule asking no kind of it" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      rules = [
        {{:decision, ClaimDecision}, [item(@claimed, [])]},
        {{:decision, CreateDecision}, [item()]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "two rules of one create, one a fact item alone, do not cover another rule" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      fact = item(@claimed, ["tenant:t"])

      rules = [
        {{:decision, ClaimDecision}, [item()]},
        {{:decision, CreateDecision}, [fact, item()]},
        {{:decision, CreateDecision}, [fact]}
      ]

      assert AppenderMap.check(rules, appenders, []) ==
               [{:unchecked_appender, @claimed, ImportCommand, CreateDecision, ClaimDecision}]
    end

    test "two rules of one create, both covering, answer nothing" do
      appenders = %{EmailClaimed => [{ImportCommand, CreateDecision}]}

      fact = item(@claimed, ["tenant:t"])

      rules = [
        {{:decision, ClaimDecision}, [item()]},
        {{:decision, CreateDecision}, [fact, item()]},
        {{:decision, CreateDecision}, [fact, item(@claimed, ["email:e", "tenant:t"])]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "a rule of one create not naming the type says nothing" do
      appenders = %{
        EmailClaimed => [{ImportCommand, CreateDecision}],
        NoExports => [{ImportCommand, CreateDecision}]
      }

      rules = [
        {{:decision, ClaimDecision}, [item(@no_exports, ["correlation:c"])]},
        {{:decision, CreateDecision}, [item(), item(@no_exports, ["correlation:c"])]},
        {{:decision, CreateDecision}, [item()]}
      ]

      assert AppenderMap.check(rules, appenders, []) == []
    end

    test "is nothing to a decision that says `create: false`" do
      assert AppenderMap.check([{{:decision, NoCreateDecision}, []}], %{}, []) == []
    end

    # Was "is nothing to a module with no `__decision__/0`": the key says decision, so it is asked.
    test "under a decision key, a module with no `__decision__/0` raises" do
      assert_raise UndefinedFunctionError, fn ->
        AppenderMap.check([{{:decision, PlainModule}, []}], %{}, [])
      end
    end
  end

  describe "a declared aggregate's rule" do
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

    test "is no create under a rule key" do
      assert AppenderMap.check([{{:rule, EmailRule}, []}], %{}) == []
    end

    test "under a decision key raises, having no `__decision__/0`" do
      assert_raise UndefinedFunctionError, fn ->
        AppenderMap.check([{{:decision, EmailRule}, []}], %{})
      end
    end

    test "listed by its module passes with its command in `conditional:`" do
      assert AppenderMap.check([{{:rule, EmailRule}, part_one()}], row(),
               conditional: [RegisterUser]
             ) ==
               []
    end

    test "listed by its module is unchecked without it" do
      assert AppenderMap.check([{{:rule, EmailRule}, part_one()}], row()) ==
               [{:unchecked_appender, @registered, RegisterUser, User, EmailRule}]
    end

    test "both its parts go through, each type with its own rows" do
      items = part_one() ++ part_two()

      appenders =
        Map.merge(row(), %{
          Erased => [{EraseUser, User}],
          Decree.RuleFixtures.Removed => [{Decree.RuleFixtures.RemoveUser, User}]
        })

      assert AppenderMap.check([{{:rule, EmailRule}, items}], appenders,
               conditional: [RegisterUser],
               dominant: [
                 {Erased, EraseUser},
                 {Decree.RuleFixtures.Removed, Decree.RuleFixtures.RemoveUser}
               ]
             ) == []

      assert [
               {:unchecked_appender, _erased, EraseUser, User, EmailRule},
               {:unchecked_appender, _removed, Decree.RuleFixtures.RemoveUser, User, EmailRule}
             ] =
               AppenderMap.check([{{:rule, EmailRule}, items}], appenders,
                 conditional: [RegisterUser]
               )
    end

    test "a kind its event can't produce is a problem" do
      items = [[types: [@registered], tags: ["phone:1"]]]

      assert [{:missing_tag_kind, @registered, "phone", EmailRule} | _] =
               AppenderMap.check([{{:rule, EmailRule}, items}], row(),
                 conditional: [RegisterUser]
               )
    end

    test "UF-1: an item's `key:` changes nothing it answers" do
      items = part_one() ++ [[types: [@registered], tags: ["phone:1"]]] ++ part_two()

      keyed =
        items
        |> Enum.with_index()
        |> Enum.map(fn
          {item, 0} -> item |> Map.new() |> Map.put(:key, nil)
          {item, index} -> item ++ [key: {:item, index}]
        end)

      unkeyed_answer = AppenderMap.check([{{:rule, EmailRule}, items}], row())

      assert [_problem | _problems] = unkeyed_answer
      assert AppenderMap.check([{{:rule, EmailRule}, keyed}], row()) == unkeyed_answer
    end

    defmodule ImportUser do
    end

    defp imported, do: %{Registered => [{RegisterUser, User}, {ImportUser, User}]}

    test "listed by its module leaves a plain command unchecked" do
      rules = [{{:rule, EmailRule}, part_one()}]

      assert AppenderMap.check(rules, imported(), conditional: [RegisterUser]) ==
               [{:unchecked_appender, @registered, ImportUser, User, EmailRule}]
    end

    # FW-5's residual, flipped: an aggregate key certifies none of its own commands.
    test "listed under an aggregate key certifies none of its commands" do
      assert AppenderMap.check([{{:aggregate, User}, part_one()}], imported()) == [
               {:unchecked_appender, @registered, RegisterUser, User, User},
               {:unchecked_appender, @registered, ImportUser, User, User}
             ]

      assert AppenderMap.check([{{:aggregate, User}, part_one()}], imported(),
               conditional: [RegisterUser]
             ) == [{:unchecked_appender, @registered, ImportUser, User, User}]
    end

    test "listed under an aggregate key still has its own items checked" do
      items = [[types: [@registered], tags: ["phone:1"]]]

      assert AppenderMap.check([{{:aggregate, User}, items}], row(), conditional: [RegisterUser]) ==
               [{:missing_tag_kind, @registered, "phone", User}]
    end

    test "an aggregate key covers no other rule's row either" do
      rules = [{{:rule, EmailRule}, part_one()}, {{:aggregate, User}, part_one()}]

      assert AppenderMap.check(rules, row()) == [
               {:unchecked_appender, @registered, RegisterUser, User, EmailRule},
               {:unchecked_appender, @registered, RegisterUser, User, User}
             ]
    end

    defmodule RuleTargeted do
    end

    test "a rule key's command is covered only by `conditional:`, never by the rule as target" do
      # A row whose target is the rule's own module, and the rule listed twice: neither the
      # target arm nor another rule's coverage applies to a rule key.
      appenders = %{Registered => [{RuleTargeted, EmailRule}]}
      rules = [{{:rule, EmailRule}, part_one()}, {{:rule, EmailRule}, part_one()}]

      assert AppenderMap.check(rules, appenders) == [
               {:unchecked_appender, @registered, RuleTargeted, EmailRule, EmailRule},
               {:unchecked_appender, @registered, RuleTargeted, EmailRule, EmailRule}
             ]

      assert AppenderMap.check(rules, appenders, conditional: [RuleTargeted]) == []
    end
  end

  describe "a type" do
    test "with no module is a problem and not a crash" do
      type = "Elixir.Decree.Decision.AppenderMapTest.NeverDefined"

      assert AppenderMap.check([{{:decision, ClaimDecision}, [item(type, ["email:d"])]}], %{}, []) ==
               [{:unknown_type, type, ClaimDecision}]
    end

    test "that names no atom at all is one too" do
      type = "not an atom #{System.unique_integer()}"

      assert AppenderMap.check([{{:decision, ClaimDecision}, [item(type)]}], %{}, []) ==
               [{:unknown_type, type, ClaimDecision}]
    end

    test "naming a module with no struct is one too" do
      type = Atom.to_string(NotAnEvent)

      assert AppenderMap.check([{{:decision, ClaimDecision}, [item(type)]}], %{}, []) ==
               [{:unknown_type, type, ClaimDecision}]
    end

    test "with `@enforce_keys` builds nothing that trips them" do
      assert AppenderMap.check([{{:decision, ClaimDecision}, [item()]}], own_row()) == []
    end
  end

  describe "`verify!/3`" do
    test "answers `:ok` with no problems" do
      assert AppenderMap.verify!([{{:decision, ClaimDecision}, [item()]}], own_row(), []) == :ok
    end

    test "raises `ArgumentError` naming every problem" do
      rules = [
        {{:decision, ClaimDecision},
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
