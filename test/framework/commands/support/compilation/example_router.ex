defmodule Decree.Commands.ExampleRouter do
  use Decree.Commands.Router

  alias Decree.Commands.{ExampleAggregate, ExampleCommand}

  dispatch ExampleCommand, to: ExampleAggregate, identity: :id
end
