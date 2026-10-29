defmodule Decree.Event.DefaultErrorEventHandler do
  @moduledoc false

  use Decree.Event.Handler,
    application: Decree.DefaultApp,
    name: __MODULE__

  alias Decree.ExampleDomain.BankAccount.Events.BankAccountOpened

  def handle(%BankAccountOpened{}, _metadata), do: {:error, :failed}
end
