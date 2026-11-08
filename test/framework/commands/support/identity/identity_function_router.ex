defmodule Decree.Commands.IdentityFunctionRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Commands.{IdentityFunctionAggregate, IdentityFunctionRouter}
  alias Decree.Commands.IdentityFunctionAggregate.IdentityFunctionCommand

  # The function answers a canonical identity, and the prefix names the stream.
  identify IdentityFunctionAggregate,
    by: &IdentityFunctionRouter.aggregate_identity/1,
    prefix: "identityfun-"

  dispatch IdentityFunctionCommand, to: IdentityFunctionAggregate

  def aggregate_identity(%{uuid: uuid}), do: uuid
end
