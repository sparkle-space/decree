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

    test "should expose its event store" do
      assert Application.event_store(ExampleApplication) == %{
               event_store: Decree.FrameworkEventStore,
               name: Decree.ExampleApplication.FrameworkEventStore
             }
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

      assert Application.event_store(RuntimeConfiguredApplication) == %{
               event_store: Decree.FrameworkEventStore,
               name: Decree.RuntimeConfiguredApplication.FrameworkEventStore
             }

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

    test "should refuse an `adapter:` in its event store config" do
      Process.flag(:trap_exit, true)

      event_store = [adapter: SomeAdapter, event_store: Decree.FrameworkEventStore]

      assert {:error, {%ArgumentError{message: message}, _stacktrace}} =
               ExampleApplication.start_link(name: :adapter_configured, event_store: event_store)

      assert message ==
               "the :adapter option in the :event_store config of application " <>
                 ":adapter_configured is no longer supported: the event store adapter layer " <>
                 "was removed. Name the Decree.Store module in :event_store instead, " <>
                 "e.g. `event_store: [event_store: MyApp.EventStore]`"
    end

    test "should refuse the removed global registry" do
      Process.flag(:trap_exit, true)

      assert {:error, {%ArgumentError{message: message}, _stacktrace}} =
               ExampleApplication.start_link(name: :globally_registered, registry: :global)

      assert message ==
               "the :global registry of application :globally_registered was removed: " <>
                 "use `registry: :local`, or name a custom registry module"
    end
  end
end
