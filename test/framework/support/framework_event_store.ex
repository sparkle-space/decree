defmodule Decree.FrameworkEventStore do
  @moduledoc false

  # The Postgres event store behind every framework and projections test application.
  use Decree.Store, otp_app: :decree
end
