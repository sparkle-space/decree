defmodule Decree.Commands.CommandIdentityTest do
  use Decree.PostgresCase

  alias Decree.ExampleDomain.BankAccount.Commands.OpenAccount
  alias Decree.ExampleDomain.BankApp
  alias Decree.Helpers.CommandAuditMiddleware
  alias Decree.UUID

  setup do
    start_supervised!(CommandAuditMiddleware)
    start_supervised!(BankApp)

    :ok
  end

  describe "provide command identity" do
    test "should generate a command_uuid if none is provided" do
      command = %OpenAccount{
        command_id: UUID.uuid4(),
        account_number: "ACC123",
        initial_balance: 1_000
      }

      :ok = BankApp.dispatch(command)
      [command_uuid] = CommandAuditMiddleware.dispatched_commands(& &1.command_uuid)
      assert is_binary(UUID.string_to_binary!(command_uuid))
    end

    test "should accept provided command_uuid" do
      command = %OpenAccount{
        command_id: UUID.uuid4(),
        account_number: "ACC123",
        initial_balance: 1_000
      }

      command_uuid = UUID.uuid4()
      :ok = BankApp.dispatch(command, command_uuid: command_uuid)
      assert [^command_uuid] = CommandAuditMiddleware.dispatched_commands(& &1.command_uuid)
    end
  end
end
