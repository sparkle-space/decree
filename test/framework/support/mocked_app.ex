defmodule Decree.MockedApp do
  alias Decree.EventStore.Mock, as: MockEventStore

  use Decree.Application,
    otp_app: :decree,
    event_store: Decree.FrameworkStorage.event_store_config(event_store: MockEventStore),
    registry: :local,
    pubsub: :local
end
