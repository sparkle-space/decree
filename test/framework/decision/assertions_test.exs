defmodule Decree.Decision.AssertionsTest do
  use Decree.DecisionCase, decision: Decree.DecisionFixtures.ClaimDecision

  alias Decree.DecisionFixtures.{Claim, ClaimDecision, Claimed, Scripted, ScriptedDecision}
  alias Decree.DecisionFixtures.TightDecision

  defp command, do: %Claim{command_id: "c", claim_id: "id", scope: "s"}

  describe "`Decree.DecisionCase`" do
    test "asserts events, state and errors, as `Decree.AggregateCase` does" do
      assert_events(command(), %Claimed{claim_id: "id", scope: "s"})
      assert_state(command(), :claimed)
      assert_error([%Claimed{}], command(), :already_claimed)
    end
  end

  describe "`decision_result/4`" do
    test "answers as a dispatch with `returning:`" do
      assert decision_result(ClaimDecision, [], command()) ==
               {:ok, [%Claimed{claim_id: "id", scope: "s"}]}

      assert decision_result(ClaimDecision, [], command(), returning: :aggregate_state) ==
               {:ok, :claimed}

      assert decision_result(ClaimDecision, [%Claimed{}], command()) ==
               {:error, :already_claimed}
    end

    test "starts from a given state" do
      assert decision_result(ClaimDecision, [], command(), state: :claimed) ==
               {:error, :already_claimed}
    end

    test "answers `decide`'s shapes as a dispatch does" do
      for {answer, expected} <- [
            {:duplicate, {:error, :duplicate_command}},
            {[], {:error, :decision_returned_no_events}},
            {:ok, {:error, :decision_returned_no_events}},
            {nil, {:error, :decision_returned_no_events}},
            {{:streams, []}, {:error, :decision_returned_no_events}},
            {{:error, :refused}, {:error, :refused}},
            {:nonsense, {:error, :invalid_decision_return}}
          ] do
        command = %Scripted{script: %{decide: fn _state, _command -> answer end}}

        assert decision_result(ScriptedDecision, [], command) == expected
      end
    end

    test "flattens a batch's members, in order" do
      id = "00000000-0000-4000-8000-000000000000"

      decide = fn _state, _command ->
        {:streams, [{:user, id, %Claimed{note: "a"}}, {:order, id, [%Claimed{note: "b"}]}]}
      end

      assert decision_result(ScriptedDecision, [], %Scripted{script: %{decide: decide}}) ==
               {:ok, [%Claimed{note: "a"}, %Claimed{note: "b"}]}
    end

    test "refuses over `max_batch_events`" do
      decide = fn _state, _command -> [%Claimed{}, %Claimed{}, %Claimed{}] end

      assert decision_result(TightDecision, [], %Scripted{script: %{decide: decide}}) ==
               {:error, :decision_batch_too_large}
    end

    test "runs `admit/2` first" do
      admit = fn _state, _command -> {:error, :no} end

      assert decision_result(ScriptedDecision, [], %Scripted{script: %{admit: admit}}) ==
               {:error, :no}
    end
  end

  describe "`assert_decides/5`" do
    test "takes an event, a list or a batch, and answers the state" do
      assert assert_decides(ClaimDecision, [], command(), %Claimed{claim_id: "id", scope: "s"}) ==
               :claimed

      assert assert_decides(ClaimDecision, [], command(), [%Claimed{claim_id: "id", scope: "s"}]) ==
               :claimed

      id = "00000000-0000-4000-8000-000000000000"
      event = %Claimed{note: "a"}
      decide = fn _state, _command -> {:streams, [{:user, id, event}]} end

      assert_decides(
        ScriptedDecision,
        [],
        %Scripted{script: %{decide: decide}},
        {:streams, [{:user, id, [event]}]}
      )
    end

    test "fails on other events" do
      assert_raise ExUnit.AssertionError, ~r/but it decided/, fn ->
        assert_decides(ClaimDecision, [], command(), %Claimed{claim_id: "other", scope: "s"})
      end
    end

    test "fails on a refusal" do
      assert_raise ExUnit.AssertionError, ~r/but it refused with/, fn ->
        assert_decides(ClaimDecision, [%Claimed{}], command(), %Claimed{})
      end
    end
  end

  describe "`assert_refuses/5`" do
    test "passes on the reason" do
      assert assert_refuses(ClaimDecision, [%Claimed{}], command(), :already_claimed) == :ok
    end

    test "fails on another reason" do
      assert_raise ExUnit.AssertionError, ~r/but it refused with/, fn ->
        assert_refuses(ClaimDecision, [%Claimed{}], command(), :other)
      end
    end

    test "fails on events" do
      assert_raise ExUnit.AssertionError, ~r/but it decided/, fn ->
        assert_refuses(ClaimDecision, [], command(), :already_claimed)
      end
    end
  end

  describe "`assert_query_items/4`" do
    test "checks `query/2`" do
      item = [types: [Atom.to_string(Claimed)], tags: ["scope:s"]]

      assert assert_query_items(ClaimDecision, command(), [item]) == :ok

      assert_raise ExUnit.AssertionError, ~r/but it queried/, fn ->
        assert_query_items(ClaimDecision, command(), [])
      end
    end

    test "gives `query/2` the metadata" do
      query = fn _command, %{metadata: metadata} -> [metadata] end
      command = %Scripted{script: %{query: query}}

      assert_query_items(ScriptedDecision, command, [%{"a" => 1}], metadata: %{"a" => 1})
    end

    test "checks `followup_query/3` when given business events" do
      followup = fn _command, _context, [event] -> [[types: [event]]] end
      command = %Scripted{script: %{followup: followup}}

      assert_query_items(ScriptedDecision, command, [[types: [:event]]],
        business_events: [:event]
      )
    end
  end
end
