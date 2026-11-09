defmodule Decree.Store.StorageInitializer do
  alias Decree.Store.Storage.{Database, Initializer}

  def reset_storage! do
    config = TestEventStore.config()

    with {:ok, conn} <- Postgrex.start_link(config) do
      try do
        Initializer.reset!(conn, config)
      after
        :ok = GenServer.stop(conn)
      end

      :ok
    end
  end

  # A plain `DROP DATABASE` fails while any session is still connected, as one a
  # test before has not yet closed. `WITH (FORCE)` ends those sessions, and any other
  # failure raises here rather than as `:already_up` from the create after it.
  # `Database.drop/1` backs `decree.store.drop`, so it keeps its plain drop.
  def drop_database!(config) do
    database = Keyword.fetch!(config, :database)
    maintenance_database = Keyword.get(config, :default_database, "postgres")

    :ok =
      config
      |> Keyword.put(:database, maintenance_database)
      |> Database.execute(~s[DROP DATABASE IF EXISTS "#{database}" WITH (FORCE)])
  end

  def recreate_database!(config) do
    drop_database!(config)
    :ok = Database.create(config)
  end

  def restore_dump!(config, dump_path) do
    recreate_database!(config)
    {_, 0} = Database.restore(config, dump_path)

    :ok
  end

  # The `:migration` tests drop and recreate TestEventStore's database, which also
  # holds SchemaEventStore's "example" schema. Recreate it as the `test` alias does
  # (create, then initialise each event store on it), so that the tests run after
  # them find both schemas.
  def recreate_test_event_store_database! do
    drop_database!(TestEventStore.config())

    for event_store <- [TestEventStore, SchemaEventStore] do
      config = event_store.config()

      Decree.Store.Tasks.Create.exec(config, quiet: true)
      :ok = Decree.Store.Tasks.Init.exec(config, quiet: true)
    end

    :ok
  end
end
