defmodule Decree.Decision.MiddlewareParityTest do
  use Decree.DecisionStoreCase

  alias Decree.DecisionFixtures.{ParityAggregateCommand, ParityCommand, StrongHandler}
  alias Decree.Helpers.Wait
  alias Decree.Middleware.{ConsistencyGuarantee, Pipeline}

  @shared_assigns [
    :aggregate_identity,
    :aggregate_state,
    :aggregate_uuid,
    :aggregate_version,
    :dispatcher_pid,
    :events
  ]

  defmodule StrongRouter do
    use Decree.Commands.Router

    decide(Scripted,
      to: Decree.DecisionFixtures.ScriptedDecision,
      identity: :claim_id,
      identity_prefix: "scripted-",
      consistency: :strong
    )
  end

  # Every stage a dispatch ran through, in order, with its pipeline.
  defp stages(acc \\ []) do
    receive do
      {:middleware, stage, pipeline} -> stages([{stage, pipeline} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # One dispatch down each route of the same router, with the same fields.
  defp both(fields, opts \\ []) do
    id = fn -> UUID.uuid4() end
    scope = scope()

    {aggregate_answer, aggregate} =
      run(struct!(ParityAggregateCommand, [id: id.(), scope: scope] ++ fields), opts)

    {decision_answer, decision} =
      run(
        struct!(
          ParityCommand,
          [command_id: UUID.uuid4(), id: id.(), scope: scope <> "-d"] ++ fields
        ),
        opts
      )

    {aggregate_answer, aggregate, decision_answer, decision}
  end

  defp run(command, opts) do
    answer = App.dispatch(command, opts)

    {answer, stages()}
  end

  defp stage_names(stages), do: Enum.map(stages, &elem(&1, 0))

  defp pipeline_fields(%Pipeline{} = pipeline) do
    pipeline
    |> Map.from_struct()
    |> Map.keys()
    |> Enum.sort()
    |> Enum.reject(&is_nil(Map.get(pipeline, &1)))
  end

  describe "the same sequence of calls" do
    test "on success" do
      {:ok, aggregate, :ok, decision} = both([])

      assert stage_names(aggregate) == [:before_dispatch, :after_dispatch]
      assert stage_names(decision) == stage_names(aggregate)

      {:after_dispatch, aggregate_pipeline} = List.last(aggregate)
      {:after_dispatch, decision_pipeline} = List.last(decision)

      assert pipeline_fields(aggregate_pipeline) == pipeline_fields(decision_pipeline)

      for assign <- @shared_assigns do
        assert Map.has_key?(aggregate_pipeline.assigns, assign), inspect(assign)
        assert Map.has_key?(decision_pipeline.assigns, assign), inspect(assign)
      end

      assert aggregate_pipeline.assigns.aggregate_version == 1
      assert decision_pipeline.assigns.aggregate_version == 1
      assert [%{}] = aggregate_pipeline.assigns.events
      assert [%{}] = decision_pipeline.assigns.events

      refute Map.has_key?(aggregate_pipeline.assigns, :position)
      refute Map.has_key?(aggregate_pipeline.assigns, :stream_versions)
      assert is_integer(decision_pipeline.assigns.position)

      uuid = decision_pipeline.assigns.aggregate_uuid
      assert decision_pipeline.assigns.stream_versions == %{uuid => 1}
    end

    test "on a business refusal" do
      {aggregate_answer, aggregate, decision_answer, decision} = both(refuse: :nope)

      assert aggregate_answer == {:error, :nope}
      assert decision_answer == {:error, :nope}

      assert stage_names(aggregate) == [:before_dispatch, :after_failure]
      assert stage_names(decision) == stage_names(aggregate)

      {:after_failure, aggregate_pipeline} = List.last(aggregate)
      {:after_failure, decision_pipeline} = List.last(decision)

      assert aggregate_pipeline.assigns.error == :nope
      assert decision_pipeline.assigns.error == :nope
      assert aggregate_pipeline.response == decision_pipeline.response
      assert Map.keys(aggregate_pipeline.assigns) == Map.keys(decision_pipeline.assigns)
      assert pipeline_fields(aggregate_pipeline) == pipeline_fields(decision_pipeline)
    end

    test "on a halted pipeline" do
      {aggregate_answer, aggregate, decision_answer, decision} = both(halt: true)

      assert aggregate_answer == {:error, :halted_here}
      assert decision_answer == {:error, :halted_here}

      assert stage_names(aggregate) == [:before_dispatch, :after_failure]
      assert stage_names(decision) == stage_names(aggregate)

      {:after_failure, aggregate_pipeline} = List.last(aggregate)
      {:after_failure, decision_pipeline} = List.last(decision)

      assert aggregate_pipeline.halted
      assert decision_pipeline.halted
      assert Map.keys(aggregate_pipeline.assigns) == Map.keys(decision_pipeline.assigns)
    end
  end

  describe "`ExtractAggregateIdentity` on a decision route" do
    test "assigns the stream and the identity" do
      {_answer, stages} =
        run(%ParityCommand{command_id: UUID.uuid4(), id: UUID.uuid4(), scope: scope()}, [])

      {:after_dispatch, pipeline} = List.last(stages)

      assert pipeline.assigns.aggregate_uuid ==
               "parity-decision-" <> pipeline.assigns.aggregate_identity
    end

    test "answers `:invalid_aggregate_identity` as an aggregate route does" do
      for id <- [nil, ""] do
        assert {:error, :invalid_aggregate_identity} =
                 App.dispatch(%ParityAggregateCommand{id: id, scope: "s"})

        assert {:error, :invalid_aggregate_identity} =
                 App.dispatch(%ParityCommand{command_id: UUID.uuid4(), id: id, scope: "s"})
      end
    end

    test "answers an identity that has no string alike" do
      for command <- [
            %ParityAggregateCommand{id: %{}, scope: "s"},
            %ParityCommand{command_id: UUID.uuid4(), id: %{}, scope: "s"}
          ] do
        assert {:error, {:unsupported_aggregate_identity_type, %{}}} = App.dispatch(command)
      end
    end
  end

  describe "`ConsistencyGuarantee` on a decision route" do
    setup do
      start_supervised!(StrongHandler, shutdown: :brutal_kill)

      await_subscribed()

      :ok
    end

    defp await_subscribed do
      event_store = Module.concat([App, "FrameworkEventStore"])

      Wait.until(15_000, fn ->
        assert {subscriptions, 0} =
                 Decree.FrameworkEventStore.local_subscriptions(name: event_store)

        assert %{status: :subscribed} =
                 Enum.find(subscriptions, &(&1.subscription_name == "DecisionStrongHandler"))
      end)
    end

    defp with_consistency_timeout(timeout) do
      previous = Application.get_env(:decree, :dispatch_consistency_timeout)

      Application.put_env(:decree, :dispatch_consistency_timeout, timeout)
      on_exit(fn -> Application.put_env(:decree, :dispatch_consistency_timeout, previous) end)
    end

    test "`:strong` waits for the handler on a single stream" do
      with_consistency_timeout(30_000)

      assert :ok = App.dispatch(scripted(), consistency: :strong, timeout: 30_000)
    end

    test "`:strong` answers a timeout when the handler holds the stream" do
      with_consistency_timeout(200)

      assert {:error, :consistency_timeout} =
               App.dispatch(scripted(scope: "held"), consistency: :strong)
    end

    test "a route's own `consistency:` applies" do
      with_consistency_timeout(200)

      assert {:error, :consistency_timeout} =
               StrongRouter.dispatch(scripted(scope: "held"), application: App)
    end

    test "`:strong` waits for every member of a batch" do
      with_consistency_timeout(30_000)

      decide = fn _state, _command ->
        {:streams,
         [
           {:user, UUID.uuid4(), %Claimed{scope: "a"}},
           {:order, UUID.uuid4(), %Claimed{scope: "b"}}
         ]}
      end

      assert :ok = App.dispatch(scripted(decide: decide), consistency: :strong, timeout: 30_000)
    end

    # The first member acks and the second never does, so only a wait for the second can
    # time out. The wait ends on the answer, so this reads no clock of its own.
    test "`:strong` answers a timeout when the handler holds a later member" do
      with_consistency_timeout(1_000)

      decide = fn _state, _command ->
        {:streams,
         [
           {:user, UUID.uuid4(), %Claimed{scope: "a"}},
           {:order, UUID.uuid4(), %Claimed{scope: "held"}}
         ]}
      end

      assert {:error, :consistency_timeout} =
               App.dispatch(scripted(decide: decide), consistency: :strong)
    end

    # The first wait ends at 0.92 T, so a timeout per member answers at 1.92 T at the soonest.
    # One deadline answers at T, which leaves a loaded host 0.9 T of the bound.
    test "`:strong` answers a batch within one timeout" do
      timeout = 2_000

      with_consistency_timeout(timeout)

      # In the order the wait walks them, so the late member is the one waited for first.
      [late, held] = Map.keys(%{UUID.uuid4() => 1, UUID.uuid4() => 1})

      decide = fn _state, _command ->
        {:streams,
         [
           {:user, late, %Claimed{scope: "late", note: div(timeout * 92, 100)}},
           {:order, held, %Claimed{scope: "held"}}
         ]}
      end

      started = System.monotonic_time(:millisecond)

      assert {:error, :consistency_timeout} =
               App.dispatch(scripted(decide: decide), consistency: :strong)

      assert System.monotonic_time(:millisecond) - started < timeout * 1.9
    end

    test "a batch takes an infinite timeout" do
      with_consistency_timeout(:infinity)

      decide = fn _state, _command ->
        {:streams,
         [
           {:user, UUID.uuid4(), %Claimed{scope: "a"}},
           {:order, UUID.uuid4(), %Claimed{scope: "b"}}
         ]}
      end

      assert :ok = App.dispatch(scripted(decide: decide), consistency: :strong, timeout: 30_000)
    end

    # No `Subscriptions` runs for this application, so a wait would exit.
    test "a member with no time left answers the timeout, with no wait" do
      with_consistency_timeout(0)

      pipeline = %Pipeline{
        application: NoSubscriptionsApp,
        consistency: :strong,
        assigns: %{dispatcher_pid: self(), events: [%Claimed{}], stream_versions: %{"s" => 1}}
      }

      assert %Pipeline{response: {:error, :consistency_timeout}} =
               ConsistencyGuarantee.after_dispatch(pipeline)
    end
  end
end
