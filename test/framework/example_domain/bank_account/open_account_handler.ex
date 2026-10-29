defmodule Decree.ExampleDomain.OpenAccountHandler do
  @moduledoc false

  @behaviour Decree.Commands.Handler

  alias Decree.ExampleDomain.BankAccount
  alias Decree.ExampleDomain.BankAccount.Commands.{CloseAccount, OpenAccount}

  def handle(%BankAccount{} = aggregate, %OpenAccount{} = open_account) do
    BankAccount.open_account(aggregate, open_account)
  end

  def handle(%BankAccount{} = aggregate, %CloseAccount{} = close_account) do
    BankAccount.close_account(aggregate, close_account)
  end
end
