defmodule Decree.Commands.OptionalStronglyConsistentEventHandler do
  use Decree.Event.Handler,
    application: Decree.Commands.ConsistencyApp,
    name: "OptionalStronglyConsistentEventHandler",
    consistency: :strong

  alias Decree.Commands.ConsistencyAggregateRoot.{
    ConsistencyCommand,
    ConsistencyEvent,
    DispatchRequestedEvent
  }

  alias Decree.Commands.ConsistencyApp

  def handle(%ConsistencyEvent{delay: delay}, _metadata) do
    :timer.sleep(round(delay / 10))
    :ok
  end

  # handle event by dispatching a command
  def handle(%DispatchRequestedEvent{uuid: uuid, delay: delay}, _metadata) do
    :timer.sleep(round(delay / 10))

    command = %ConsistencyCommand{uuid: uuid, delay: delay}

    ConsistencyApp.dispatch(command, consistency: :strong)
  end
end
