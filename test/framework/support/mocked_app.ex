defmodule Decree.MockedApp do
  alias Decree.EventStore.Adapters.Mock, as: MockEventStore

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: MockEventStore
    ],
    registry: :local,
    pubsub: :local
end
