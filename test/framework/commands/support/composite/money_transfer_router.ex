defmodule Decree.Commands.Composite.MoneyTransferRouter do
  @moduledoc false

  use Decree.Commands.Router

  alias Decree.ExampleDomain.MoneyTransfer
  alias Decree.ExampleDomain.MoneyTransfer.Commands.TransferMoney
  alias Decree.ExampleDomain.TransferMoneyHandler

  identify MoneyTransfer, by: :transfer_uuid
  dispatch TransferMoney, to: TransferMoneyHandler, aggregate: MoneyTransfer
end
