defmodule Decree.Commands.ExampleAggregate do
  alias Decree.Commands.ExampleAggregate
  alias Decree.Commands.ExampleCommand

  defstruct [:id]

  def execute(%ExampleAggregate{}, %ExampleCommand{}), do: []
end
