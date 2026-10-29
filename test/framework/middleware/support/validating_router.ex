defmodule Decree.ValidatingRouter do
  use Decree.Commands.Router

  alias Decree.Helpers.CommandAuditMiddleware
  alias Decree.Middleware.Commands.{CommandHandler, CounterAggregateRoot, Validate}
  alias Decree.ValidationMiddleware

  middleware CommandAuditMiddleware
  middleware ValidationMiddleware

  dispatch Validate,
    to: CommandHandler,
    aggregate: CounterAggregateRoot,
    identity: :aggregate_uuid
end
