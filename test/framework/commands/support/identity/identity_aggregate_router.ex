defmodule Decree.Commands.IdentityAggregateRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Commands.IdentityAggregate
  alias Decree.Commands.IdentityAggregate.IdentityCommand

  identify IdentityAggregate,
    by: :uuid,
    prefix: "prefix-"

  dispatch IdentityCommand, to: IdentityAggregate
end
