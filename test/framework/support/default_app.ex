defmodule Decree.DefaultApp do
  alias Decree.EventStore.Adapters.InMemory
  alias Decree.Serialization.JsonSerializer

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: InMemory,
      serializer: JsonSerializer
    ],
    pubsub: :local,
    registry: :local
end
