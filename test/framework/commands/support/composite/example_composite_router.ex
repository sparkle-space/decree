defmodule Decree.Commands.Composite.ExampleCompositeRouter do
  @moduledoc false

  use Decree.Commands.CompositeRouter

  alias Decree.Commands.Composite.BankAccountRouter
  alias Decree.Commands.Composite.MoneyTransferRouter

  router(BankAccountRouter)
  router(MoneyTransferRouter)
end
