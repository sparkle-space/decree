defmodule Decree.Commands.AggregateRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Commands.AggregateRoot
  alias Decree.Commands.AggregateRoot.{Command, Command2}

  dispatch Command, to: AggregateRoot, identity: :uuid
  dispatch Command2, to: AggregateRoot, function: :custom_function_name, identity: :uuid
end
