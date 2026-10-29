defmodule Decree.DistributedApp do
  alias Decree.EventStore.Adapters.InMemory
  alias Decree.Serialization.JsonSerializer

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: InMemory,
      serializer: JsonSerializer
    ],
    pubsub: [
      phoenix_pubsub: [
        adapter: Phoenix.PubSub.PG2,
        pool_size: 1
      ]
    ],
    registry: :global
end
