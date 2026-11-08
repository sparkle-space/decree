defmodule Decree.ExampleDomain.BankRouter do
  @moduledoc false

  use Decree.Commands.Router

  alias Decree.ExampleDomain.BankAccount
  alias Decree.ExampleDomain.BankAccount.Commands.CloseAccount
  alias Decree.ExampleDomain.BankAccount.Commands.DepositMoney
  alias Decree.ExampleDomain.BankAccount.Commands.OpenAccount
  alias Decree.ExampleDomain.BankAccount.Commands.WithdrawMoney
  alias Decree.ExampleDomain.DepositMoneyHandler
  alias Decree.ExampleDomain.MoneyTransfer
  alias Decree.ExampleDomain.MoneyTransfer.Commands.TransferMoney
  alias Decree.ExampleDomain.OpenAccountHandler
  alias Decree.ExampleDomain.TransferMoneyHandler
  alias Decree.ExampleDomain.WithdrawMoneyHandler
  alias Decree.Helpers.CommandAuditMiddleware

  middleware CommandAuditMiddleware

  # Bank account. Commanded's example domain names an account by its number ("ACC123").
  identify BankAccount, by: :account_number, opaque: true
  dispatch OpenAccount, to: OpenAccountHandler, aggregate: BankAccount
  dispatch DepositMoney, to: DepositMoneyHandler, aggregate: BankAccount
  dispatch WithdrawMoney, to: WithdrawMoneyHandler, aggregate: BankAccount
  dispatch CloseAccount, to: OpenAccountHandler, aggregate: BankAccount

  # Money transfer
  identify MoneyTransfer, by: :transfer_uuid
  dispatch TransferMoney, to: TransferMoneyHandler, aggregate: MoneyTransfer
end
