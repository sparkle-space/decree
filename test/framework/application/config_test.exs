defmodule Decree.Application.ConfigTest do
  use ExUnit.Case

  alias Decree.Application.Config
  alias Decree.ExampleApplication
  alias Decree.Helpers.Wait

  describe "a Decree application" do
    setup do
      start_supervised!(ExampleApplication)

      :ok
    end

    test "should get application event store config" do
      assert {Decree.EventStore.Adapters.InMemory,
              %{name: Decree.ExampleApplication.EventStore}} ==
               Config.get(ExampleApplication, :event_store)
    end

    test "should get application pubsub config" do
      assert {Decree.PubSub.LocalPubSub,
              %{
                pubsub_name: Decree.ExampleApplication.LocalPubSub,
                tracker_name: Decree.ExampleApplication.LocalPubSub.Tracker
              }} == Config.get(ExampleApplication, :pubsub)
    end

    test "should get application registry config" do
      assert {Decree.Registration.LocalRegistry,
              %{registry_name: Decree.ExampleApplication.LocalRegistry}} ==
               Config.get(ExampleApplication, :registry)
    end

    test "should remove config when application stopped" do
      :ok = stop_supervised(ExampleApplication)

      Wait.until(fn ->
        assert_raise(
          RuntimeError,
          "could not lookup Decree.ExampleApplication because it was not started or it does not exist",
          fn ->
            Config.get(ExampleApplication, :event_store)
          end
        )
      end)
    end
  end
end
