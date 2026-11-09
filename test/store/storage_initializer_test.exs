defmodule Decree.Store.StorageInitializerTest do
  use ExUnit.Case

  alias Decree.Store.Config
  alias Decree.Store.Storage.Database
  alias Decree.Store.StorageInitializer

  setup do
    config =
      TestEventStore.config()
      |> Config.default_postgrex_opts()
      |> Keyword.update!(:database, fn database -> database <> "_temp" end)

    on_exit(fn -> force_drop(config) end)

    [config: config]
  end

  # A session left connected to the database, as one a test before it has not yet
  # closed, must not stop the database being recreated.
  test "recreates a database a session is still connected to", %{config: config} do
    :ok = Database.create(config)

    {:ok, conn} = config |> Keyword.put(:backoff_type, :stop) |> Postgrex.start_link()
    Process.unlink(conn)
    Postgrex.query!(conn, "SELECT 1", [])

    assert StorageInitializer.recreate_database!(config) == :ok
    assert Database.create(config) == {:error, :already_up}
  end

  defp force_drop(config) do
    database = Keyword.fetch!(config, :database)
    maintenance = Keyword.get(config, :default_database, "postgres")

    :ok =
      config
      |> Keyword.put(:database, maintenance)
      |> Database.execute(~s[DROP DATABASE IF EXISTS "#{database}" WITH (FORCE)])
  end
end
