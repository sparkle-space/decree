defmodule Decree.Commands.IdentityFunctionRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Commands.{IdentityFunctionAggregate, IdentityFunctionRouter}
  alias Decree.Commands.IdentityFunctionAggregate.IdentityFunctionCommand

  identify IdentityFunctionAggregate,
    by: &IdentityFunctionRouter.aggregate_identity/1

  dispatch IdentityFunctionCommand, to: IdentityFunctionAggregate

  def aggregate_identity(%{uuid: uuid}), do: "identityfun-" <> uuid
end
