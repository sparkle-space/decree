defmodule Decree.Projections.Repo do
  use Ecto.Repo,
    otp_app: :decree,
    adapter: Ecto.Adapters.Postgres
end
