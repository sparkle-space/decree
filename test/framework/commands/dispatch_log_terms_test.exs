defmodule Decree.Commands.DispatchLogTermsTest do
  use Decree.PostgresCase

  # A dispatch's log lines name a command by its module and a stream by the last eight
  # characters of its id, never a command's fields or a full id
  # (sparkle-space/sparklespace#8358). Each test asserts its line still appears, and that the
  # term it planted appears nowhere in the log.

  import ExUnit.CaptureLog

  alias Decree.Commands.{AggregateRouter, UnregisteredCommand}
  alias Decree.Commands.Composite.ExampleCompositeRouter
  alias Decree.Commands.Composite.UnregisteredCommand, as: CompositeUnregisteredCommand
  alias Decree.DefaultApp

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
end
