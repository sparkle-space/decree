defmodule Decree.Commands.ConsistencyApp do
  use Decree.Application, otp_app: :decree

  alias Decree.Commands.ConsistencyRouter

  router(ConsistencyRouter)
end
