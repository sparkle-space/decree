defmodule Decree.Aggregate.MultiBankRouter do
  use Decree.Commands.Router

  alias Decree.Aggregate.Multi.BankAccount
  alias Decree.Aggregate.Multi.BankAccount.Commands.{OpenAccount, WithdrawMoney}

  dispatch [OpenAccount, WithdrawMoney], to: BankAccount, identity: :account_number
end
