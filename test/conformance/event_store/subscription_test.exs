defmodule Decree.EventStore.Adapters.EventStore.SubscriptionTest do
  use Decree.EventStore.EventStoreTestCase

  use Decree.EventStore.SubscriptionTestCase,
    event_store: Decree.EventStore.Adapters.EventStore
end
