defmodule Decree.ApplicationTest do
  use Decree.PostgresCase

  alias Decree.Application
  alias Decree.ExampleApplication
  alias Decree.RuntimeConfiguredApplication
  alias Decree.UnconfiguredApplication

  describe "a Decree application" do
    setup do
      pid = start_supervised!(ExampleApplication)

      [pid: pid]
    end

    test "should register process using application name", %{pid: pid} do
      assert Process.whereis(ExampleApplication) == pid
    end

    test "should not allow an application to be started more than once", %{pid: pid} do
      assert {:error, {:already_started, ^pid}} = ExampleApplication.start_link()
    end

    test "should expose an event store adapter" do
      assert Application.event_store_adapter(ExampleApplication) ==
               {Decree.EventStore.Adapters.EventStore,
                %{
                  event_store: Decree.FrameworkEventStore,
                  name: Decree.ExampleApplication.EventStore
                }}
    end

    test "should expose a pubsub adapter" do
      assert Application.pubsub_adapter(ExampleApplication) ==
               {Decree.PubSub.LocalPubSub,
                %{
                  pubsub_name: Decree.ExampleApplication.LocalPubSub,
                  tracker_name: Decree.ExampleApplication.LocalPubSub.Tracker
                }}
    end

    test "should expose a registry adapter" do
      assert Application.registry_adapter(ExampleApplication) ==
               {Decree.Registration.LocalRegistry,
                %{registry_name: Decree.ExampleApplication.LocalRegistry}}
    end

    test "should allow application config to be provided by `init/1` callback function" do
      start_supervised!(RuntimeConfiguredApplication)

      assert Application.event_store_adapter(RuntimeConfiguredApplication) ==
               {Decree.EventStore.Adapters.EventStore,
                %{
                  event_store: Decree.FrameworkEventStore,
                  name: Decree.RuntimeConfiguredApplication.EventStore
                }}

      assert Application.pubsub_adapter(RuntimeConfiguredApplication) ==
               {Decree.PubSub.PhoenixPubSub,
                %{
                  pubsub_name: Decree.RuntimeConfiguredApplication.PhoenixPubSub,
                  tracker_name: Decree.RuntimeConfiguredApplication.PhoenixPubSub.Tracker
                }}

      assert Application.registry_adapter(RuntimeConfiguredApplication) ==
               {Decree.Registration.LocalRegistry,
                %{registry_name: Decree.RuntimeConfiguredApplication.LocalRegistry}}
    end

    test "should fail to start unconfigured application" do
      Process.flag(:trap_exit, true)

      {:error, reason} = UnconfiguredApplication.start_link()

      assert match?(
               {%ArgumentError{
                  message:
                    "missing :event_store config for application Decree.UnconfiguredApplication"
                }, _},
               reason
             )
    end
  end
end
