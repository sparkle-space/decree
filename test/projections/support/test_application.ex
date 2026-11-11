defmodule TestApplication do
  use Decree.Application,
    otp_app: :my_app,
    event_store: [
      event_store: Decree.FrameworkEventStore
    ]

  # One event store process per application instance (see Decree.FrameworkStorage).
  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end
