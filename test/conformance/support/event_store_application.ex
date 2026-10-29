defmodule EventStoreApplication do
  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.EventStore.Adapters.EventStore,
      event_store: TestEventStore
    ]
end
