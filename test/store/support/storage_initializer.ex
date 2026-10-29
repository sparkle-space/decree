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
end
