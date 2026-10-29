defmodule Decree.ExampleDomain.WithdrawMoneyHandler do
  @moduledoc false

  @behaviour Decree.Commands.Handler

  alias Decree.ExampleDomain.BankAccount
  alias Decree.ExampleDomain.BankAccount.Commands.WithdrawMoney

  def handle(%BankAccount{} = aggregate, %WithdrawMoney{} = withdraw_money) do
    BankAccount.withdraw(aggregate, withdraw_money)
  end
end
