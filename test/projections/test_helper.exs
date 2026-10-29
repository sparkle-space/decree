alias Decree.Projections.Repo

{:ok, _} = Repo.start_link()

defmodule CreateProjections do
  use Ecto.Migration

  def change do
    create table(:projections) do
      add(:name, :text)
    end
  end
end

Ecto.Migrator.up(Repo, 20_170_609_120_000, CreateProjections, log: false)

# The `projection_versions` tables (upstream's priv/repo/migrations).
Ecto.Migrator.run(Repo, "test/projections/migrations", :up, all: true, log: false)

# Decree.EventStore.Adapters.Mock is defined once, in test/framework/support/mocks.ex.

ExUnit.start()

Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)
