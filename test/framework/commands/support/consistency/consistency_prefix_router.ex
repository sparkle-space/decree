defmodule Decree.Commands.ConsistencyPrefixRouter do
  use Decree.Commands.Router

  alias Decree.Commands.ConsistencyAggregateRoot

  alias ConsistencyAggregateRoot.{
    ConsistencyCommand,
    NoOpCommand,
    RequestDispatchCommand
  }

  identify ConsistencyAggregateRoot,
    by: :uuid,
    prefix: "example-prefix-"

  dispatch [ConsistencyCommand, NoOpCommand, RequestDispatchCommand],
    to: ConsistencyAggregateRoot
end
