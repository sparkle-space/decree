defmodule Decree.FrameworkStorage do
  @moduledoc false

  # The framework and projections suites run their test applications on
  # `Decree.EventStore.Adapters.EventStore` over one Postgres database.

  alias Decree.EventStore.Adapters.EventStore, as: Adapter
  alias Decree.Store.Config
  alias Decree.Store.Storage.Initializer

  @doc """
  Names the event store process after the application instance, so several
  applications, or several named instances of one, can run at once.

  Called from the test applications' `init/1` callback. An event store given
  at start (a mock adapter) is left as it is.
  """
  def name_event_store(config) do
    instance = Keyword.get(config, :name) || Keyword.fetch!(config, :application)

    Keyword.update(config, :event_store, [], fn event_store ->
      if Keyword.get(event_store, :adapter) == Adapter do
        Keyword.put_new(event_store, :name, Module.concat([instance, EventStore]))
      else
        event_store
      end
    end)
  end

  @doc """
  The Postgres event store config for an application instance.
  """
  def event_store_config(opts \\ []) do
    Keyword.merge([adapter: Adapter, event_store: Decree.FrameworkEventStore], opts)
  end

  @doc """
  Creates and initialises a schema in the framework database, for an application
  instance that needs its own event store (`prefix:` in its event store config).
  """
  def create_schema!(schema) do
    config = config(schema)

    Decree.Store.Tasks.Create.exec(config, quiet: true)
    Decree.Store.Tasks.Init.exec(config, quiet: true)

    :ok
  end

  @doc """
  Empties a schema of the framework event store database (truncates every table).
  """
  def reset!(schema \\ "public") do
    config = config(schema)

    {:ok, conn} = Postgrex.start_link(Config.default_postgrex_opts(config))

    try do
      Initializer.reset!(conn, config)
    after
      GenServer.stop(conn)
    end

    :ok
  end

  def config(schema \\ "public") do
    Decree.FrameworkEventStore
    |> Config.parsed(:decree)
    |> Keyword.put(:schema, schema)
  end
end
