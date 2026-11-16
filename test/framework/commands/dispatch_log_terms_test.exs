defmodule Decree.Commands.DispatchLogTermsTest do
  use Decree.PostgresCase

  # A dispatch's log lines name a command by its module and a stream by the last eight
  # characters of its id, never a command's fields or a full id
  # (sparkle-space/sparklespace#8358). Each test asserts its line still appears, and that the
  # term it planted appears nowhere in the log.

  import ExUnit.CaptureLog

  alias Decree.Commands.{AggregateRouter, ConsistencyApp, UnregisteredCommand}
  alias Decree.Commands.Composite.ExampleCompositeRouter
  alias Decree.Commands.Composite.UnregisteredCommand, as: CompositeUnregisteredCommand
  alias Decree.Commands.ConsistencyAggregateRoot.ConsistencyCommand
  alias Decree.Commands.StronglyConsistentEventHandler
  alias Decree.DefaultApp
  alias Decree.Helpers.Wait
  alias Decree.UUID

  @marker "dispatch-log-terms-marker"

  describe "an unregistered command" do
    setup do
      start_supervised!(DefaultApp)

      :ok
    end

    test "is named by its module by a router" do
      log =
        capture_log(fn ->
          assert {:error, :unregistered_command} =
                   AggregateRouter.dispatch(%UnregisteredCommand{aggregate_uuid: @marker},
                     application: DefaultApp
                   )
        end)

      assert log =~
               "attempted to dispatch an unregistered command: Decree.Commands.UnregisteredCommand\n"

      refute log =~ @marker
    end

    test "is named by its module by a composite router" do
      log =
        capture_log(fn ->
          assert {:error, :unregistered_command} =
                   ExampleCompositeRouter.dispatch(%CompositeUnregisteredCommand{uuid: @marker},
                     application: DefaultApp
                   )
        end)

      assert log =~
               "attempted to dispatch an unregistered command: " <>
                 "Decree.Commands.Composite.UnregisteredCommand\n"

      refute log =~ @marker
    end
  end

  describe "a consistency timeout" do
    setup do
      start_supervised!(ConsistencyApp)
      start_supervised!(StronglyConsistentEventHandler, shutdown: :brutal_kill)

      event_store = Module.concat([ConsistencyApp, "FrameworkEventStore"])

      Wait.until(15_000, fn ->
        assert {subscriptions, 0} =
                 Decree.FrameworkEventStore.local_subscriptions(name: event_store)

        assert "StronglyConsistentEventHandler" in for(
                 %{subscription_name: name, status: :subscribed} <- subscriptions,
                 do: name
               )
      end)

      :ok
    end

    test "names the aggregate's stream by suffix" do
      uuid = UUID.uuid4()

      log =
        capture_log(fn ->
          # The handler takes longer than the configured one-second consistency timeout.
          command = %ConsistencyCommand{command_id: UUID.uuid4(), uuid: uuid, delay: 5_000}

          assert {:error, :consistency_timeout} =
                   ConsistencyApp.dispatch(command, consistency: :strong)
        end)

      assert log =~
               "Consistency timeout waiting for aggregate #{inspect(suffix(uuid))} at version 1"

      refute log =~ uuid
    end
  end

  defp suffix(id), do: "..." <> String.slice(id, -8, 8)
end
