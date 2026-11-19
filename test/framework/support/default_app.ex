defmodule Decree.DefaultApp do
  use Decree.Application,
    otp_app: :decree,
    event_store: [
      event_store: Decree.FrameworkEventStore
    ],
    registry: :local

  # One event store process per application instance (see Decree.FrameworkStorage).
  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end
