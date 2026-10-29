defmodule Decree.Store.MigrationFilesTest do
  use ExUnit.Case, async: true

  alias Decree.Store.Tasks.Migrations

  # `Decree.Store.Tasks.Migrate` reads each migration from
  # `Application.app_dir(:decree, "priv/decree/store/migrations/v<version>.sql")`, a path
  # the rename moved from `priv/event_store`. Only the excluded `:migration` tests run a
  # migration, so this checks the path without a database.
  test "every migration version the store knows ships as a file under priv" do
    dir = Application.app_dir(:decree, "priv/decree/store/migrations")

    for version <- Migrations.available_migrations() do
      path = Path.join(dir, "v#{version}.sql")
      assert File.regular?(path), "missing migration v#{version}: #{path}"
    end
  end
end
