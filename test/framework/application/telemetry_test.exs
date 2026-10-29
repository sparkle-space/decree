defmodule Decree.Application.TelemetryTest do
  use Decree.PostgresCase

  alias Decree.DefaultApp
  alias Decree.Middleware.Commands.Fail
  alias Decree.Middleware.Commands.IncrementCount
  alias Decree.Middleware.Commands.RaiseError
  alias Decree.UUID

  setup do
    start_supervised!(DefaultApp)
    attach_telemetry()

    :ok
  end

  defmodule TestRouter do
    use Decree.Commands.Router

    alias Decree.Middleware.Commands.CommandHandler
    alias Decree.Middleware.Commands.CounterAggregateRoot

    dispatch IncrementCount,
      to: CommandHandler,
      aggregate: CounterAggregateRoot,
      identity: :aggregate_uuid

    dispatch RaiseError,
      to: CommandHandler,
      aggregate: CounterAggregateRoot,
      identity: :aggregate_uuid
  end

  test "emit `[:decree, :application, :dispatch, :start | :stop]` event" do
    command = %IncrementCount{aggregate_uuid: UUID.uuid4()}

    assert :ok = TestRouter.dispatch(command, application: DefaultApp)

    assert_receive {[:decree, :application, :dispatch, :start], 1, _meas, _meta}
    assert_receive {[:decree, :aggregate, :execute, :start], 2, _meas, _meta}
    assert_receive {[:decree, :aggregate, :execute, :stop], 3, _meas, _meta}
    assert_receive {[:decree, :application, :dispatch, :stop], 4, _meas, meta}

    assert %{application: DefaultApp, error: nil, execution_context: %{command: ^command}} = meta
  end

  test "emit `[:decree, :application, :dispatch, :start | :stop]` event on error" do
    command = %RaiseError{aggregate_uuid: UUID.uuid4()}
    error = %RuntimeError{message: "failed"}

    assert {:error, ^error} = TestRouter.dispatch(command, application: DefaultApp)

    assert_receive {[:decree, :application, :dispatch, :start], 1, _meas, _meta}
    assert_receive {[:decree, :aggregate, :execute, :start], 2, _meas, _meta}
    assert_receive {[:decree, :aggregate, :execute, :exception], 3, _meas, _meta}
    assert_receive {[:decree, :application, :dispatch, :stop], 4, _meas, meta}

    assert %{application: DefaultApp, error: ^error, execution_context: %{command: ^command}} =
             meta
  end

  test "emit `[:decree, :application, :dispatch, :start | :stop]` event on unregistered command" do
    command = %Fail{aggregate_uuid: UUID.uuid4()}
    error = :unregistered_command

    assert {:error, ^error} = TestRouter.dispatch(command, application: DefaultApp)

    assert_receive {[:decree, :application, :dispatch, :start], 1, _meas, _meta}

    assert_receive {[:decree, :application, :dispatch, :stop], 2, _meas, meta}

    assert %{application: DefaultApp, error: ^error, execution_context: %{command: ^command}} =
             meta
  end

  defp attach_telemetry do
    agent = start_supervised!({Agent, fn -> 1 end})

    :telemetry.attach_many(
      "test-handler",
      [
        [:decree, :aggregate, :execute, :start],
        [:decree, :aggregate, :execute, :stop],
        [:decree, :aggregate, :execute, :exception],
        [:decree, :application, :dispatch, :start],
        [:decree, :application, :dispatch, :stop]
      ],
      fn event_name, measurements, metadata, reply_to ->
        num = Agent.get_and_update(agent, fn n -> {n, n + 1} end)
        send(reply_to, {event_name, num, measurements, metadata})
      end,
      self()
    )

    on_exit(fn ->
      :telemetry.detach("test-handler")
    end)
  end
end
