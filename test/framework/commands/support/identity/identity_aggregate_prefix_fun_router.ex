defmodule Decree.Commands.IdentityAggregatePrefixFunRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Commands.{IdentityAggregate, IdentityAggregatePrefixFunRouter}
  alias Decree.Commands.IdentityAggregate.IdentityCommand

  identify IdentityAggregate,
    by: :uuid,
    prefix: &IdentityAggregatePrefixFunRouter.prefix/0

  dispatch IdentityCommand, to: IdentityAggregate

  def prefix do
    "funprefix-"
  end
end
