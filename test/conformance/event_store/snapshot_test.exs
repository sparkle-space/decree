defmodule Decree.EventStore.Adapters.EventStore.SnapshotTest do
  use Decree.EventStore.EventStoreTestCase

  use Decree.EventStore.SnapshotTestCase,
    event_store: Decree.EventStore.Adapters.EventStore
end
