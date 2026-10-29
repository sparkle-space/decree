defmodule Decree.Commands.TimeoutRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Commands.{TimeoutAggregateRoot, TimeoutCommand, TimeoutCommandHandler}

  dispatch TimeoutCommand,
    to: TimeoutCommandHandler,
    aggregate: TimeoutAggregateRoot,
    identity: :aggregate_uuid,
    timeout: 1_000
end
