defmodule Decree.Decision.RetryTest do
  use Decree.DecisionStoreCase

  alias Decree.Commands.ExecutionResult

  @retry [:decree, :decision, :retry]

  # The dispatch's task blocks in a callback, holding no connection, while the test appends
  # through the store's own path and then lets it go. No step here waits on a clock.
  defp dispatch_async(command, opts \\ []) do
    Task.async(fn -> App.dispatch(command, [timeout: 60_000] ++ opts) end)
  end

  defp holding_decide(test) do
    fn state, command ->
      hold(test, {:deciding, state})

      %Claimed{claim_id: command.claim_id, scope: command.scope, note: "decided"}
    end
  end

  setup do
    attach_telemetry([@retry, [:decree, :decision, :read]])
    :ok
  end

  describe "between the read and the append" do
    test "a matching event fails the condition, and `decide` runs again on the fresh read" do
      test = self()
      command = scripted(decide: holding_decide(test))
      scope = command.scope

      task = dispatch_async(command)

      assert_receive {{:deciding, []}, pid}

      seed(%Claimed{scope: scope, note: "rival"}, ["scope:" <> scope])
      send(pid, :go)

      assert_receive {{:deciding, [%Claimed{note: "rival"}]}, ^pid}
      send(pid, :go)

      assert :ok = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1},
                       %{reason: :append_condition_failed, decision: decision, application: App}}

      assert decision == Decree.DecisionFixtures.ScriptedDecision
      refute_received {:telemetry, @retry, _, _}

      assert [%Claimed{note: "decided"}] =
               Enum.map(stored("scripted-" <> command.claim_id), & &1.data)
    end

    test "a refusal on the fresh read is the answer" do
      test = self()

      decide = fn
        [], _command ->
          hold(test, :deciding)

          %Claimed{}

        [%Claimed{}], _command ->
          {:error, :taken}
      end

      command = scripted(decide: decide)
      scope = command.scope

      task = dispatch_async(command)

      assert_receive {:deciding, pid}

      seed(%Claimed{scope: scope}, ["scope:" <> scope])
      send(pid, :go)

      assert {:error, :taken} = Task.await(task, :infinity)
      assert stored("scripted-" <> command.claim_id) == []
    end

    test "an event of this command fails the condition, and refuses on the fresh read" do
      test = self()
      command = scripted(decide: holding_decide(test))

      task = dispatch_async(command)

      assert_receive {{:deciding, []}, pid}

      seed(%Released{scope: "x"}, ["command:" <> command.command_id])
      send(pid, :go)

      assert {:error, :reused_command_id} = Task.await(task, :infinity)
      assert stored("scripted-" <> command.claim_id) == []
    end

    test "an event no item matches fails nothing" do
      test = self()
      command = scripted(decide: holding_decide(test))

      task = dispatch_async(command)

      assert_receive {{:deciding, []}, pid}

      seed(%Claimed{scope: "elsewhere"}, ["scope:elsewhere"])
      send(pid, :go)

      assert :ok = Task.await(task, :infinity)
      refute_received {:telemetry, @retry, _, _}
    end
  end

  describe "between the two reads" do
    defp holding_followup(test, items) do
      fn _command, _tag_context, _events ->
        hold(test, :between_reads)

        items
      end
    end

    defp counting_decide(test) do
      fn state, command ->
        send(test, {:decided, state})

        %Claimed{claim_id: command.claim_id, scope: command.scope}
      end
    end

    test "an event only the first read's item matches fails on the first read's `after`" do
      test = self()
      scope = scope()

      released = [types: [Atom.to_string(Released)], tags: ["scope:" <> scope]]

      command =
        scripted(
          scope: scope,
          followup: holding_followup(test, [released]),
          decide: counting_decide(test)
        )

      task = dispatch_async(command)

      assert_receive {:between_reads, pid}

      seed(%Claimed{scope: scope, note: "rival"}, ["scope:" <> scope])
      send(pid, :go)

      assert_receive {:decided, []}
      assert_receive {:between_reads, ^pid}
      send(pid, :go)

      assert_receive {:decided, [%Claimed{note: "rival"}]}
      assert :ok = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, _, %{reason: :append_condition_failed}}
    end

    test "an event the second read's item matches fails on the same `after`" do
      test = self()
      scope = scope()

      released = [types: [Atom.to_string(Released)], tags: ["scope:" <> scope]]

      command =
        scripted(
          scope: scope,
          followup: holding_followup(test, [released]),
          decide: counting_decide(test)
        )

      task = dispatch_async(command)

      assert_receive {:between_reads, pid}

      seed(%Released{scope: scope}, ["scope:" <> scope])
      send(pid, :go)

      # The second read sees it, and the condition still refuses what came after the first.
      assert_receive {:decided, [%Released{}]}
      assert_receive {:between_reads, ^pid}
      send(pid, :go)

      assert_receive {:decided, [%Released{}]}
      assert :ok = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, _, %{reason: :append_condition_failed}}
    end
  end

  describe "F5, a command match only the second read has" do
    test "restarts from step 1, using a retry" do
      test = self()
      released = [types: [Atom.to_string(Released)], tags: ["scope:nothing"]]

      command =
        scripted(
          followup: fn _command, _tag_context, _events ->
            hold(test, :between_reads)

            [released]
          end
        )

      task = dispatch_async(command)

      assert_receive {:between_reads, pid}

      seed(%Released{scope: "x"}, ["command:" <> command.command_id])
      send(pid, :go)

      assert_receive {:between_reads, ^pid}
      send(pid, :go)

      assert {:error, :reused_command_id} = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :new_command_match}}
      refute_received {:telemetry, @retry, _, _}
    end

    test "with no retry left answers `:too_many_attempts`" do
      test = self()
      released = [types: [Atom.to_string(Released)], tags: ["scope:nothing"]]

      command =
        scripted(
          followup: fn _command, _tag_context, _events ->
            hold(test, :between_reads)

            [released]
          end
        )

      task = dispatch_async(command, retry_attempts: 0)

      assert_receive {:between_reads, pid}

      seed(%Released{scope: "x"}, ["command:" <> command.command_id])
      send(pid, :go)

      assert {:error, :too_many_attempts} = Task.await(task, :infinity)
    end
  end

  describe "exhaustion" do
    # Every attempt loses, to an event this `decide` commits itself.
    defp losing_decide(test) do
      fn _state, command ->
        send(test, :decided)
        seed(%Claimed{scope: command.scope}, ["scope:" <> command.scope])

        %Claimed{claim_id: command.claim_id, scope: command.scope}
      end
    end

    # Each rival grows the fold, so `decide` must keep losing whatever it sees.
    test "answers `:too_many_attempts`, never a duplicate" do
      command = scripted(decide: losing_decide(self()))

      assert {:error, :too_many_attempts} = App.dispatch(command, retry_attempts: 2)

      assert_received :decided
      assert_received :decided
      assert_received :decided
      refute_received :decided

      assert stored("scripted-" <> command.claim_id) == []
    end

    test "`retry_attempts: 0` tries once" do
      command = scripted(decide: losing_decide(self()))

      assert {:error, :too_many_attempts} = App.dispatch(command, retry_attempts: 0)

      assert_received :decided
      refute_received :decided
      refute_received {:telemetry, @retry, _, _}
    end

    test "`retry_attempts: 0` works when nothing contends" do
      assert {:ok, %ExecutionResult{}} =
               App.dispatch(scripted(), retry_attempts: 0, returning: :execution_result)
    end
  end
end
