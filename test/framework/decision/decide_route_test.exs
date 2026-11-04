defmodule Decree.Decision.DecideRouteTest do
  use ExUnit.Case, async: true

  alias Decree.DecisionFixtures.{ParityRouter, Router}

  defp unique(name), do: "Decree.Decision.DecideRouteTest.#{name}#{System.unique_integer()}"

  defp compile(body, name \\ "Fixture") do
    router = name |> unique() |> String.replace("-", "")

    Code.eval_string("""
    alias Decree.DecisionFixtures.{ClaimDecision, ScriptedDecision, ParityAggregate}
    alias Decree.DecisionFixtures.{Claim, Scripted, ParityAggregateCommand, ByFunction}

    defmodule #{router} do
      use Decree.Commands.Router

      #{body}
    end
    """)

    Module.concat([router])
  end

  describe "the `decide` macro" do
    test "takes a command or a list" do
      router =
        compile("""
        decide [Claim, Scripted], to: ClaimDecision, identity: :claim_id, identity_prefix: "x-"
        """)

      assert Enum.sort(router.__registered_commands__()) ==
               Enum.sort([Decree.DecisionFixtures.Claim, Decree.DecisionFixtures.Scripted])
    end

    test "raises on an unknown param, as `dispatch` does" do
      for param <- ["aggregate: X", "function: :f", "before_execute: :b", "lifespan: L"] do
        assert_raise RuntimeError, ~r/unexpected decide parameter/, fn ->
          compile("""
          decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "x-", #{param}
          """)
        end
      end
    end

    test "needs `to`" do
      assert_raise RuntimeError, ~r/missing required parameter: :to/, fn ->
        compile(~s(decide Claim, identity: :claim_id, identity_prefix: "x-"))
      end
    end

    test "refuses a command registered twice" do
      assert_raise ArgumentError, ~r/already been registered/, fn ->
        compile("""
        decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "x-"
        decide Claim, to: ScriptedDecision, identity: :claim_id, identity_prefix: "y-"
        """)
      end
    end

    test "validates `stream_tag` as `dispatch` does" do
      assert_raise ArgumentError, ~r/invalid `stream_tag` option/, fn ->
        compile("""
        decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "x-", stream_tag: true
        """)
      end
    end

    test "joins `identify` by decision module, whichever comes first" do
      for body <- [
            """
            identify ClaimDecision, by: :claim_id, prefix: "x-"
            decide Claim, to: ClaimDecision
            """,
            """
            decide Claim, to: ClaimDecision
            identify ClaimDecision, by: :claim_id, prefix: "x-"
            """
          ] do
        assert compile(body).__stream_prefixes__() == %{decision: ["x-"], other: []}
      end
    end
  end

  describe "a build-time refusal" do
    test "for no identity" do
      for body <- [
            ~s(decide Claim, to: ClaimDecision, identity_prefix: "x-"),
            ~s(decide Claim, to: ClaimDecision, identity: nil, identity_prefix: "x-")
          ] do
        assert_raise ArgumentError, ~r/has no identity/, fn -> compile(body) end
      end
    end

    test "for no prefix" do
      for prefix <- ["", ~s(, identity_prefix: nil), ~s(, identity_prefix: "")] do
        assert_raise ArgumentError, ~r/has no stream prefix/, fn ->
          compile("decide Claim, to: ClaimDecision, identity: :claim_id#{prefix}")
        end
      end

      assert_raise ArgumentError, ~r/has no stream prefix/, fn ->
        compile("""
        identify ClaimDecision, by: :claim_id
        decide Claim, to: ClaimDecision, identity_prefix: "shadowed-"
        """)
      end
    end

    test "for a function prefix" do
      assert_raise ArgumentError, ~r/has a function prefix/, fn ->
        compile("""
        def prefix, do: "x-"
        decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: &__MODULE__.prefix/0
        """)
      end

      assert_raise ArgumentError, ~r/has a function prefix/, fn ->
        compile("""
        def prefix, do: "x-"
        identify ClaimDecision, by: :claim_id, prefix: &__MODULE__.prefix/0
        decide Claim, to: ClaimDecision
        """)
      end
    end

    test "for a function identity with no `stream_tag:`" do
      assert_raise ArgumentError, ~r/function identity with no `stream_tag:`/, fn ->
        compile("""
        decide ByFunction,
          to: ClaimDecision,
          identity: &ByFunction.identity/1,
          identity_prefix: "x-"
        """)
      end

      assert_raise ArgumentError, ~r/function identity with no `stream_tag:`/, fn ->
        compile("""
        identify ClaimDecision, by: &ByFunction.identity/1, prefix: "x-"
        decide ByFunction, to: ClaimDecision
        """)
      end

      assert compile("""
             decide ByFunction,
               to: ClaimDecision,
               identity: &ByFunction.identity/1,
               identity_prefix: "x-",
               stream_tag: :claim
             """)
    end

    for {name, decision, aggregate} <- [
          {"the same", "shared-", "shared-"},
          {"an aggregate's within a decision's", "shared-more-", "shared-"},
          {"a decision's within an aggregate's", "shared-", "shared-more-"}
        ] do
      test "for an aggregate route's prefix overlapping: #{name}" do
        assert_raise ArgumentError, ~r/overlapping the decision prefix/, fn ->
          compile("""
          decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "#{unquote(decision)}"
          dispatch ParityAggregateCommand,
            to: ParityAggregate,
            identity: :id,
            identity_prefix: "#{unquote(aggregate)}"
          """)
        end
      end

      test "for an `identify` prefix overlapping: #{name}" do
        assert_raise ArgumentError, ~r/overlapping the decision prefix/, fn ->
          compile("""
          identify ParityAggregate, by: :id, prefix: "#{unquote(aggregate)}"
          decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "#{unquote(decision)}"
          dispatch ParityAggregateCommand, to: ParityAggregate
          """)
        end
      end
    end

    test "does not fire for prefixes that merely share a start" do
      router =
        compile("""
        decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "shared-a-"
        dispatch ParityAggregateCommand,
          to: ParityAggregate,
          identity: :id,
          identity_prefix: "shared-b-"
        """)

      assert router.__stream_prefixes__() == %{decision: ["shared-a-"], other: ["shared-b-"]}
    end

    test "does not reach an aggregate's function prefix or bare identity" do
      router =
        compile("""
        def prefix, do: "x-"
        decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "x-"
        dispatch ParityAggregateCommand,
          to: ParityAggregate,
          identity: :id,
          identity_prefix: &__MODULE__.prefix/0
        """)

      assert router.__stream_prefixes__() == %{decision: ["x-"], other: []}
    end
  end

  describe "`__stream_prefixes__/0`" do
    test "lists a router's binary prefixes" do
      assert ParityRouter.__stream_prefixes__() == %{
               decision: ["parity-decision-"],
               other: ["parity-aggregate-"]
             }
    end

    test "lists an unrouted `identify` as another prefix" do
      router =
        compile("""
        identify ParityAggregate, by: :id, prefix: "lone-"
        decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "x-"
        """)

      assert router.__stream_prefixes__() == %{decision: ["x-"], other: ["lone-"]}
    end
  end

  describe "across a composite router" do
    defp composite(routers) do
      name = unique("Composite") |> String.replace("-", "")

      Code.eval_string("""
      defmodule #{name} do
        use Decree.Commands.CompositeRouter

        #{Enum.map_join(routers, "\n", &"router #{inspect(&1)}")}
      end
      """)

      Module.concat([name])
    end

    test "accumulates every router's prefixes, through a composite of composites" do
      inner = composite([ParityRouter])
      outer = composite([inner, Router])

      assert outer.__stream_prefixes__().other == ["parity-aggregate-"]

      assert Enum.sort(outer.__stream_prefixes__().decision) ==
               Enum.sort([
                 "parity-decision-",
                 "claim-",
                 "scripted-",
                 "tight-",
                 "create-",
                 "function-",
                 "activating-",
                 "digesting-"
               ])
    end

    test "raises when two routers overlap, in either order" do
      decisions =
        compile(~s(decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "z-1-"))

      aggregates =
        compile("""
        dispatch ParityAggregateCommand, to: ParityAggregate, identity: :id, identity_prefix: "z-"
        """)

      for routers <- [[decisions, aggregates], [aggregates, decisions]] do
        assert_raise ArgumentError, ~r/overlapping the decision prefix/, fn ->
          composite(routers)
        end
      end
    end

    test "raises when a composite meets a router" do
      decisions =
        compile(~s(decide Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "y-1-"))

      aggregates =
        compile("""
        dispatch ParityAggregateCommand, to: ParityAggregate, identity: :id, identity_prefix: "y-"
        """)

      inner = composite([decisions])

      assert_raise ArgumentError, ~r/overlapping the decision prefix/, fn ->
        composite([inner, aggregates])
      end
    end

    test "an application is a composite" do
      # The fixture ledger's prefix is the tag index facts'.
      assert Decree.DecisionFixtures.App.__stream_prefixes__().other ==
               ["parity-aggregate-", "tag-index-"]
    end
  end
end
