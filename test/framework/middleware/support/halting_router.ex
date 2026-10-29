defmodule Decree.HaltingRouter do
  use Decree.Commands.Router

  alias Decree.HaltingMiddleware
  alias Decree.Helpers.CommandAuditMiddleware
  alias Decree.Middleware.Commands.{CommandHandler, CounterAggregateRoot, IncrementCount}

  middleware CommandAuditMiddleware
  middleware HaltingMiddleware

  dispatch IncrementCount,
    to: CommandHandler,
    aggregate: CounterAggregateRoot,
    identity: :aggregate_uuid
end
