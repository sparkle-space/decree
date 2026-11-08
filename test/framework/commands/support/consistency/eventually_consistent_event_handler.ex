defmodule Decree.Commands.EventuallyConsistentEventHandler do
  use Decree.Event.Handler,
    application: Decree.Commands.ConsistencyApp,
    name: "EventuallyConsistentEventHandler",
    consistency: :eventual

  alias Decree.Commands.ConsistencyAggregateRoot.{
    ConsistencyCommand,
    ConsistencyEvent,
    DispatchRequestedEvent
  }

  alias Decree.Commands.ConsistencyApp

  # Simulate slow event handler.
  def handle(%ConsistencyEvent{}, _metadata) do
    :timer.sleep(:infinity)

    :ok
  end

  # Dispatch a command.
  def handle(%DispatchRequestedEvent{uuid: uuid, delay: delay}, _metadata) do
    command = %ConsistencyCommand{uuid: uuid, command_id: Decree.UUID.uuid4(), delay: delay}

    ConsistencyApp.dispatch(command)
  end
end
