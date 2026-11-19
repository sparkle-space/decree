defmodule Decree.Decision.MiddlewareParityTest do
  use Decree.DecisionStoreCase

  alias Decree.DecisionFixtures.{ParityAggregateCommand, ParityCommand}
  alias Decree.Middleware.Pipeline

  @shared_assigns [
    :aggregate_identity,
    :aggregate_state,
    :aggregate_uuid,
    :aggregate_version,
    :events
  ]

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
      run(
        struct!(
          ParityAggregateCommand,
          [command_id: UUID.uuid4(), id: id.(), scope: scope] ++ fields
        ),
        opts
      )

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

    test "on a duplicate" do
      command = %ParityCommand{command_id: UUID.uuid4(), id: UUID.uuid4(), scope: scope()}

      assert :ok = App.dispatch(command)
      assert [:before_dispatch, :after_dispatch] = stages() |> stage_names()

      assert {:error, :duplicate_command} = App.dispatch(command)

      duplicate = stages()

      assert stage_names(duplicate) == [:before_dispatch, :after_failure]

      {:after_failure, pipeline} = List.last(duplicate)

      assert pipeline.assigns.error == :duplicate_command
      assert pipeline.response == {:error, :duplicate_command}
    end

    test "on a reused id" do
      command = %ParityCommand{command_id: UUID.uuid4(), id: UUID.uuid4(), scope: scope()}

      assert :ok = App.dispatch(command)
      assert [:before_dispatch, :after_dispatch] = stages() |> stage_names()

      assert {:error, :reused_command_id} =
               App.dispatch(%ParityCommand{command | id: UUID.uuid4()})

      reused = stages()

      assert stage_names(reused) == [:before_dispatch, :after_failure]

      {:after_failure, pipeline} = List.last(reused)

      assert pipeline.assigns.error == :reused_command_id
      assert pipeline.response == {:error, :reused_command_id}
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
                 App.dispatch(%ParityAggregateCommand{
                   command_id: UUID.uuid4(),
                   id: id,
                   scope: "s"
                 })

        assert {:error, :invalid_aggregate_identity} =
                 App.dispatch(%ParityCommand{command_id: UUID.uuid4(), id: id, scope: "s"})
      end
    end

    test "answers an identity that is not a binary as an aggregate route does" do
      for command <- [
            %ParityAggregateCommand{command_id: UUID.uuid4(), id: %{}, scope: "s"},
            %ParityCommand{command_id: UUID.uuid4(), id: %{}, scope: "s"}
          ] do
        assert {:error, :invalid_aggregate_identity} = App.dispatch(command)
      end
    end
  end
end
