defmodule Decree.Commands.DefaultDispatchReturnApp do
  alias Decree.ExampleDomain.BankRouter

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.EventStore.Adapters.InMemory,
      serializer: Decree.Serialization.JsonSerializer
    ],
    pubsub: :local,
    registry: :local,
    default_dispatch_opts: [
      consistency: :eventual,
      returning: :aggregate_version
    ]

  router(BankRouter)
end
