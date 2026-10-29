defmodule Decree.Aggregates.DefaultLifespanRouter do
  use Decree.Commands.Router

  alias Decree.Aggregates.LifespanAggregate
  alias Decree.Aggregates.LifespanAggregate.Command

  dispatch [Command], to: LifespanAggregate, identity: :uuid
end
