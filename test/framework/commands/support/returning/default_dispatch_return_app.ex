defmodule Decree.Commands.DefaultDispatchReturnApp do
  alias Decree.ExampleDomain.BankRouter

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      event_store: Decree.FrameworkEventStore
    ],
    pubsub: :local,
    registry: :local,
    default_dispatch_opts: [
      returning: :aggregate_version
    ]

  router(BankRouter)

  # One event store process per application instance (see Decree.FrameworkStorage).
  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end
