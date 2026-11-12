defmodule Decree.EventStore.ConformanceStorage do
  alias Decree.Store.Config
  alias Decree.Store.Storage.Initializer

  def connect(config) do
    postgrex_config = Config.default_postgrex_opts(config)

    Postgrex.start_link(postgrex_config)
  end

  def config(schema \\ "public") do
    ConformanceEventStore
    |> Config.parsed(:decree)
    |> Keyword.put(:schema, schema)
  end

  def reset!(conn, config), do: Initializer.reset!(conn, config)

  # The conformance application's event store config, `opts` added to it.
  def event_store_config(opts \\ []) do
    Keyword.merge([event_store: ConformanceEventStore], opts)
  end
end
