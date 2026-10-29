defmodule Decree.ExampleDomain.DepositMoneyHandler do
  @moduledoc false

  @behaviour Decree.Commands.Handler

  alias Decree.ExampleDomain.BankAccount
  alias Decree.ExampleDomain.BankAccount.Commands.DepositMoney

  def handle(%BankAccount{} = aggregate, %DepositMoney{} = deposit_money) do
    aggregate
    |> BankAccount.deposit(deposit_money)
  end
end
