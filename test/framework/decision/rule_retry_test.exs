defmodule Decree.Decision.RuleRetryTest do
  use Decree.RuleCase

  alias Decree.Aggregates.Aggregate
  alias Decree.Event.Mapper
  alias Decree.Registration
  alias Decree.RuleFixtures.Stopped
  alias Decree.Store.Wait

  @read [:decree, :decision, :read]
  @retry [:decree, :decision, :retry]

  setup do
    attach_telemetry([@read, @retry])
    :ok
  end

  # An event the condition matches, so this attempt loses it.
  defp lose_condition(%{scope: scope}) do
    seed(%Read{scope: scope}, ["scope:" <> scope], App)
  end

  # An event on the aggregate's own stream it has not applied, so this attempt loses its version.
  defp lose_version(command) do
    :ok =
      EventStore.append_to_stream(App, ruled_stream(command), :any_version, [
        Mapper.map_to_event_data(%Noted{id: command.id, note: "rival"})
      ])
  end

  # Runs each attempt's step in order, then appends.
  defp in_turn(test, steps) do
    {:ok, turns} = Agent.start_link(fn -> steps end)

    fn %RuledAggregate{} = state, %{id: id, rule_result: %Result{events: events}} = command ->
      send(test, {:attempt, state, length(events)})

      Agent.get_and_update(turns, fn
        [] -> {nil, []}
        [step | steps] -> {step, steps}
      end)
      |> case do
        nil -> :ok
        step -> step.(command)
      end

      %Noted{id: id, note: "decided"}
    end
  end

  defp attempts(acc \\ []) do
    receive do
      {:attempt, state, read} -> attempts([{state.notes, read} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  describe "a failed condition" do
    test "runs the command again, on what the store holds now" do
      command = ruled(execute: in_turn(self(), [&lose_condition/1]))

      assert :ok = App.dispatch(command)

      assert attempts() == [{[], 0}, {[], 1}]
      assert [{%{attempt: 1}, metadata}] = received(@retry)

      assert metadata == %{
               application: App,
               decision: Decree.RuleFixtures.ScriptedRule,
               reason: :append_condition_failed
             }

      assert [%{attempt: 1}, %{attempt: 2}] = @read |> received() |> Enum.map(&elem(&1, 1))
    end

    test "re-decides on the new read" do
      test = self()

      execute = fn %RuledAggregate{}, %{id: id, rule_result: %Result{events: events}} = command ->
        send(test, {:saw, length(events)})

        case events do
          [] ->
            lose_condition(command)

            %Noted{id: id, note: "from an empty read"}

          [_rival] ->
            {:error, :taken}
        end
      end

      command = ruled(execute: execute)

      assert {:error, :taken} = App.dispatch(command)

      assert_received {:saw, 0}
      assert_received {:saw, 1}
      assert stored(ruled_stream(command), App) == []
    end

    test "that is spent answers `:too_many_attempts`, with nothing appended" do
      lose = fn command -> lose_condition(command) end

      command = ruled(execute: in_turn(self(), [lose, lose, lose]))

      assert {:error, :too_many_attempts} = App.dispatch(command, retry_attempts: 2)

      assert attempts() == [{[], 0}, {[], 1}, {[], 2}]
      assert stored(ruled_stream(command), App) == []
    end

    test "that is spent leaves the state as it was" do
      lose = fn command -> lose_condition(command) end

      command = ruled(execute: in_turn(self(), [lose, lose]))

      assert {:error, :too_many_attempts} = App.dispatch(command, retry_attempts: 1)

      assert %RuledAggregate{notes: []} =
               App.aggregate_state(RuledAggregate, ruled_stream(command))

      assert Aggregate.aggregate_version(App, RuledAggregate, ruled_stream(command)) == 0
    end

    test "with `retry_attempts: 0` tries once" do
      command = ruled(execute: in_turn(self(), [&lose_condition/1]))

      assert {:error, :too_many_attempts} = App.dispatch(command, retry_attempts: 0)

      assert attempts() == [{[], 0}]
      assert received(@retry) == []
    end

    test "with `retry_attempts: 0` works when nothing contends" do
      assert :ok = App.dispatch(ruled(), retry_attempts: 0)
    end
  end

  describe "the two budgets" do
    test "a lost version doesn't spend the condition's, and the reverse" do
      for steps <- [
            [&lose_condition/1, &lose_version/1],
            [&lose_version/1, &lose_condition/1]
          ] do
        command = ruled(execute: in_turn(self(), steps))

        assert :ok = App.dispatch(command, retry_attempts: 1)
        assert [_first, _second, _third] = attempts()
      end
    end

    test "each stays its own size: one condition retry is one" do
      lose = fn command -> lose_condition(command) end

      command = ruled(execute: in_turn(self(), [lose, lose]))

      assert {:error, :too_many_attempts} = App.dispatch(command, retry_attempts: 1)
      assert [_first, _second] = attempts()
    end

    test "each stays its own size: one version retry is one" do
      lose = fn command -> lose_version(command) end

      command = ruled(execute: in_turn(self(), [lose, lose]))

      assert {:error, :too_many_attempts} = App.dispatch(command, retry_attempts: 1)
      assert [_first, _second] = attempts()
    end

    test "bound one dispatch at 2r + 1 runs of `execute/2`" do
      command =
        ruled(
          execute:
            in_turn(self(), [
              &lose_condition/1,
              &lose_version/1,
              &lose_condition/1,
              &lose_version/1
            ])
        )

      assert {:error, :too_many_attempts} = App.dispatch(command, retry_attempts: 1)
      assert [_first, _second, _third] = attempts()
    end

    test "count every attempt in the read's telemetry" do
      command = ruled(execute: in_turn(self(), [&lose_condition/1, &lose_version/1]))

      assert :ok = App.dispatch(command)

      assert [%{attempt: 1}, %{attempt: 2}, %{attempt: 3}] =
               @read |> received() |> Enum.map(&elem(&1, 1))
    end
  end

  describe "a lost expected version" do
    test "rebuilds the state and reads again before deciding again" do
      test = self()
      scope = scope()

      step = fn command ->
        lose_version(command)
        lose_condition(%{scope: scope})
      end

      command = ruled(scope: scope, execute: in_turn(test, [step]))

      assert :ok = App.dispatch(command)

      # The second attempt sees the rival in its state and the new event in its read. The state
      # came first: a read of the stale state would have lost the condition instead.
      assert attempts() == [{[], 0}, {["rival"], 1}]
      assert received(@retry) == []
    end
  end

  describe "a lifespan stop" do
    defp mailbox_holds_a_command?(pid) do
      {:messages, messages} = Process.info(pid, :messages)

      Enum.any?(messages, &match?({:"$gen_call", _from, {:execute_command, _context}}, &1))
    end

    test "under a queued command re-dispatches it, which reads afresh" do
      test = self()
      id = UUID.uuid4()

      first =
        ruled(
          [
            id: id,
            query: fn _command, _tag_context ->
              hold(test, :reading)

              []
            end
          ],
          Stopped
        )

      second =
        ruled(
          [
            id: id,
            query: fn _command, _tag_context ->
              send(test, {:queued_read, self()})

              []
            end,
            execute: fn %RuledAggregate{notes: notes}, command ->
              send(test, {:queued_state, notes})

              %Noted{id: command.id, note: "second"}
            end
          ],
          Stopped
        )

      task = dispatch_async(first)

      assert_receive {:reading, pid}

      queued = dispatch_async(second)

      Wait.until(5_000, fn -> assert mailbox_holds_a_command?(pid) end)

      send(pid, :go)

      assert :ok = Task.await(task, :infinity)
      assert :ok = Task.await(queued, :infinity)

      # It never ran on the aggregate that stopped.
      assert_received {:queued_read, reader}
      assert reader != pid
      assert_received {:queued_state, [0]}

      assert [%{data: %Noted{}}, %{data: %Noted{note: "second"}}] =
               stored("ruled-" <> id, App)
    end

    test "with no retry left answers `:too_many_attempts`" do
      test = self()
      id = UUID.uuid4()

      first =
        ruled(
          [
            id: id,
            query: fn _command, _tag_context ->
              hold(test, :reading)

              []
            end
          ],
          Stopped
        )

      task = dispatch_async(first)

      assert_receive {:reading, pid}

      queued = dispatch_async(ruled([id: id], Stopped), retry_attempts: 0)

      Wait.until(5_000, fn -> assert mailbox_holds_a_command?(pid) end)

      send(pid, :go)

      assert :ok = Task.await(task, :infinity)
      assert {:error, :too_many_attempts} = Task.await(queued, :infinity)

      assert [_only] = stored("ruled-" <> id, App)
    end
  end

  describe "the aggregate killed during a blocked read" do
    for {name, script} <- [
          {"the first", :query},
          {"the second", :followup}
        ] do
      test "in #{name} read answers an error, and nothing commits twice" do
        test = self()
        scope = scope()
        seed(%Read{scope: scope}, ["scope:" <> scope], App)

        blocking =
          case unquote(script) do
            :query ->
              [
                query: fn command, _tag_context ->
                  hold(test, :reading)

                  [[types: [Atom.to_string(Read)], tags: ["scope:" <> command.scope]]]
                end
              ]

            :followup ->
              [
                followup: fn _command, _tag_context, _events ->
                  hold(test, :reading)

                  []
                end
              ]
          end

        command = ruled([scope: scope] ++ blocking)

        task = dispatch_async(command)

        assert_receive {:reading, pid}

        reference = Process.monitor(pid)
        Process.exit(pid, :kill)
        assert_receive {:DOWN, ^reference, :process, ^pid, :killed}

        assert {:error, :aggregate_execution_failed} = Task.await(task, :infinity)
        assert stored(ruled_stream(command), App) == []

        name = Aggregate.name(App, RuledAggregate, ruled_stream(command))

        Wait.until(5_000, fn ->
          assert Registration.whereis_name(App, name) == :undefined
        end)

        # The same command again, on a fresh aggregate that is not held.
        assert :ok = App.dispatch(%{command | script: %{}})
        assert [_once] = stored(ruled_stream(command), App)
      end
    end
  end
end
