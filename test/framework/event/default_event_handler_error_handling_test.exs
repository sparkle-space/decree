defmodule Decree.Event.DefaultEventHandlerErrorHandlingTest do
  use ExUnit.Case

  alias Decree.DefaultApp
  alias Decree.Event.DefaultErrorEventHandler
  alias Decree.ExampleDomain.BankAccount.Commands.OpenAccount
  alias Decree.ExampleDomain.BankRouter
  alias Decree.Helpers.CommandAuditMiddleware
  alias Decree.UUID

  setup do
    start_supervised!(CommandAuditMiddleware)
    start_supervised!(DefaultApp)

    :ok
  end

  test "should stop event handler on error" do
    account_number = UUID.uuid4()

    {:ok, handler} = DefaultErrorEventHandler.start_link()

    Process.unlink(handler)
    ref = Process.monitor(handler)

    command = %OpenAccount{account_number: account_number, initial_balance: 1_000}

    :ok = BankRouter.dispatch(command, application: DefaultApp)

    assert_receive {:DOWN, ^ref, _, _, _}
    refute Process.alive?(handler)
  end
end
