defmodule Decree.ExampleApplication do
  alias Decree.EventStore.Adapters.InMemory
  alias Decree.Serialization.JsonSerializer

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: InMemory,
      serializer: JsonSerializer
    ]

  alias Decree.ExampleDomain.BankRouter

  router(BankRouter)
end
