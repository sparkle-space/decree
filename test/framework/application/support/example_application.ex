defmodule Decree.ExampleApplication do
  use Decree.Application,
    otp_app: :decree,
    event_store: [
      event_store: Decree.FrameworkEventStore
    ]

  alias Decree.ExampleDomain.BankRouter

  router(BankRouter)

  # One event store process per application instance (see Decree.FrameworkStorage).
  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end
