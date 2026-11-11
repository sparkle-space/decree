defmodule EventStoreApplication do
  use Decree.Application,
    otp_app: :decree,
    event_store: Decree.EventStore.ConformanceStorage.event_store_config()
end
