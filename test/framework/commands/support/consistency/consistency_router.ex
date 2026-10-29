defmodule Decree.Commands.ConsistencyRouter do
  use Decree.Commands.Router

  alias Decree.Commands.ConsistencyAggregateRoot
  alias ConsistencyAggregateRoot.ConsistencyCommand
  alias ConsistencyAggregateRoot.NoOpCommand
  alias ConsistencyAggregateRoot.RequestDispatchCommand

  dispatch [ConsistencyCommand, NoOpCommand, RequestDispatchCommand],
    to: ConsistencyAggregateRoot,
    identity: :uuid
end
