defmodule Decree.Commands.DispatchRetryExhaustedTest do
  use Decree.PostgresCase

  alias Decree.Aggregates.LifespanAggregate
  alias Decree.Aggregates.LifespanAggregate.Command
  alias Decree.Commands.ExhaustedRetryRouter
  alias Decree.{DefaultApp, Registration, UUID}
  alias Decree.Helpers.{CommandAuditMiddleware, Wait}

  defmodule ExhaustedRetryRouter do
    use Decree.Commands.Router

    middleware CommandAuditMiddleware

    dispatch [Command],
      to: LifespanAggregate,
      identity: :uuid,
      lifespan: Decree.Aggregates.Lifespan
  end

  setup do
    start_supervised!(CommandAuditMiddleware)
    start_supervised!(DefaultApp)

    :ok
  end

  # A command queued for an aggregate that its lifespan stops is retried by the
  # dispatcher. With no retry left it answers `{:error, :too_many_attempts}`
  # through the failure middleware, as an exhausted `:wrong_expected_version`
  # retry does, instead of crashing the caller.
  test "a command whose aggregate stops under it, with no retry left, answers `:too_many_attempts`" do
    uuid = UUID.uuid4()

    {:ok, ^uuid} =
      Decree.Aggregates.Supervisor.open_aggregate(DefaultApp, LifespanAggregate, uuid)

    pid = Registration.whereis_name(DefaultApp, {DefaultApp, LifespanAggregate, uuid})

    :ok = :sys.suspend(pid)

    {stopping, queued} =
      try do
        stopping =
          Task.async(fn ->
            ExhaustedRetryRouter.dispatch(
              %Command{command_id: UUID.uuid4(), uuid: uuid, action: :event, lifespan: :stop},
              application: DefaultApp
            )
          end)

        wait_for_queued_commands(pid, 1)

        queued =
          Task.async(fn ->
            ExhaustedRetryRouter.dispatch(
              %Command{command_id: UUID.uuid4(), uuid: uuid, action: :noop},
              application: DefaultApp,
              retry_attempts: 0
            )
          end)

        wait_for_queued_commands(pid, 2)

        {stopping, queued}
      after
        :sys.resume(pid)
      end

    assert :ok = Task.await(stopping)
    assert {:error, :too_many_attempts} = Task.await(queued)

    assert {2, 1, 1} = CommandAuditMiddleware.count_commands()
  end

  defp wait_for_queued_commands(pid, count) do
    Wait.until(fn ->
      {:messages, messages} = Process.info(pid, :messages)

      assert Enum.count(messages, &match?({:"$gen_call", _from, {:execute_command, _}}, &1)) ==
               count
    end)
  end
end
