defmodule Decree.Commands.OpenAccountBonusHandler do
  alias Decree.ExampleDomain.BankApp

  use Decree.Event.Handler,
    application: BankApp,
    name: "OpenAccountBonus"

  alias Decree.ExampleDomain.BankAccount.Commands.DepositMoney
  alias Decree.ExampleDomain.BankAccount.Events.BankAccountOpened
  alias Decree.ExampleDomain.BankRouter
  alias Decree.UUID

  def handle(%BankAccountOpened{} = command, metadata) do
    %BankAccountOpened{account_number: account_number} = command
    %{event_id: causation_id, correlation_id: correlation_id} = metadata

    deposit_welcome_bonus = %DepositMoney{
      account_number: account_number,
      command_id: UUID.uuid4(),
      transfer_uuid: UUID.uuid4(),
      amount: 100
    }

    BankRouter.dispatch(deposit_welcome_bonus,
      application: BankApp,
      causation_id: causation_id,
      correlation_id: correlation_id
    )
  end
end
