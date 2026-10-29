defmodule Decree.EventStore.Adapters.EventStore.EventStoreTest do
  use ExUnit.Case

  setup do
    start_supervised!(EventStoreApplication)

    :ok
  end

  test "should configure event store in application" do
    assert {Decree.EventStore.Adapters.EventStore, %{event_store: TestEventStore}} =
             Decree.Application.event_store_adapter(EventStoreApplication)
  end
end
