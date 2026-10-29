defmodule Decree.Event.SimpleErrorEventHandler do
  @moduledoc false
  use Decree.Event.Handler,
    application: Decree.DefaultApp,
    name: __MODULE__

  alias Decree.Event.ErrorAggregate.Events.{
    ErrorEvent,
    ExceptionEvent,
    InvalidReturnValueEvent
  }

  # Simulate event handling error reply
  def handle(%ErrorEvent{}, _metadata) do
    {:error, :failed}
  end

  # Simulate event handling exception
  def handle(%ExceptionEvent{}, _metadata) do
    raise "exception"
  end

  # Simulate event handling returning an invalid value
  def handle(%InvalidReturnValueEvent{}, _metadata) do
    nil
  end
end
