defmodule Decree.ExampleDomain.BankApp do
  alias Decree.EventStore.Adapters.InMemory
  alias Decree.ExampleDomain.BankRouter
  alias Decree.Serialization.JsonSerializer

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: InMemory,
      serializer: JsonSerializer
    ],
    pubsub: :local,
    registry: :local

  router(BankRouter)
end
