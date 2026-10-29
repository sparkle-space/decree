defmodule Decree.Aggregates.LifespanRouter do
  use Decree.Commands.Router

  alias Decree.Aggregates.Lifespan
  alias Decree.Aggregates.LifespanAggregate
  alias Decree.Aggregates.LifespanAggregate.Command

  dispatch [Command], to: LifespanAggregate, identity: :uuid, lifespan: Lifespan
end
