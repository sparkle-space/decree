defmodule Decree.EventStore.EventStoreTest do
  use ExUnit.Case

  setup do
    start_supervised!(EventStoreApplication)

    :ok
  end

  test "should configure event store in application" do
    assert %{event_store: ConformanceEventStore} =
             Decree.Application.event_store(EventStoreApplication)
  end
end
