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

Ecto.Migrator.up(Repo, 20_170_609_120_000, CreateProjections)

Mox.defmock(Decree.EventStore.Adapters.Mock, for: Decree.EventStore.Adapter)

ExUnit.start()

Ecto.Adapters.SQL.Sandbox.mode(Repo, :manual)
