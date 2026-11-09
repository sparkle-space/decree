defmodule Decree.Store.StorageInitializer do
  alias Decree.Store.Storage.Initializer

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

  def recreate_database!(config) do
    Decree.Store.Storage.Database.drop(config)
    :ok = Decree.Store.Storage.Database.create(config)
  end

  # The `:migration` tests drop and recreate TestEventStore's database, which also
  # holds SchemaEventStore's "example" schema. Recreate it as the `test` alias does
  # (create, then initialise each event store on it), so that the tests run after
  # them find both schemas.
  def recreate_test_event_store_database! do
    Decree.Store.Storage.Database.drop(TestEventStore.config())

    for event_store <- [TestEventStore, SchemaEventStore] do
      config = event_store.config()

      Decree.Store.Tasks.Create.exec(config, quiet: true)
      :ok = Decree.Store.Tasks.Init.exec(config, quiet: true)
    end

    :ok
  end
end
