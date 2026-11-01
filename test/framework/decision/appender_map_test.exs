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

    test "a type with no row is a problem, and so is an empty one" do
      for appenders <- [%{}, %{EmailClaimed => []}] do
        assert AppenderMap.check([{ClaimDecision, [item()]}], appenders, []) ==
                 [{:no_appender_row, @claimed, ClaimDecision}]
      end
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
