defmodule Decree.Commands.Composite.BankAccountRouter do
  @moduledoc false

  use Decree.Commands.Router

  alias Decree.ExampleDomain.{
    BankAccount,
    DepositMoneyHandler,
    OpenAccountHandler,
    WithdrawMoneyHandler
  }

  alias Decree.ExampleDomain.BankAccount.Commands.{DepositMoney, OpenAccount, WithdrawMoney}

  identify BankAccount, by: :account_number, opaque: true

  dispatch OpenAccount, to: OpenAccountHandler, aggregate: BankAccount
  dispatch DepositMoney, to: DepositMoneyHandler, aggregate: BankAccount
  dispatch WithdrawMoney, to: WithdrawMoneyHandler, aggregate: BankAccount
end
