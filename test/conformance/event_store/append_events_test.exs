defmodule Decree.EventStore.Adapters.EventStore.AppendEventsTest do
  use Decree.EventStore.EventStoreTestCase

  use Decree.EventStore.AppendEventsTestCase,
    event_store: Decree.EventStore.Adapters.EventStore
end
