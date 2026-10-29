defmodule Decree.Aggregates.BankRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.ExampleDomain.{
    BankAccount,
    DepositMoneyHandler,
    OpenAccountHandler,
    WithdrawMoneyHandler
  }

  alias BankAccount.Commands.{CloseAccount, DepositMoney, OpenAccount, WithdrawMoney}

  identify BankAccount, by: :account_number

  dispatch OpenAccount, to: OpenAccountHandler, aggregate: BankAccount
  dispatch DepositMoney, to: DepositMoneyHandler, aggregate: BankAccount
  dispatch WithdrawMoney, to: WithdrawMoneyHandler, aggregate: BankAccount
  dispatch CloseAccount, to: OpenAccountHandler, aggregate: BankAccount
end
