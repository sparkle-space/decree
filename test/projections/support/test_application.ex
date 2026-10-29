defmodule TestApplication do
  use Decree.Application,
    otp_app: :my_app,
    event_store: [
      adapter: Decree.EventStore.Adapters.InMemory,
      serializer: Decree.Serialization.JsonSerializer
    ]
end
