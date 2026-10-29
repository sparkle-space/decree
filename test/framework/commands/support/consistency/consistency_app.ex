defmodule Decree.Commands.ConsistencyApp do
  use Decree.Application, otp_app: :decree

  alias Decree.Commands.ConsistencyRouter

  router(ConsistencyRouter)

  # One event store process per application instance (see Decree.FrameworkStorage).
  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end
