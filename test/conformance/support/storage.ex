defmodule Decree.EventStore.Adapters.EventStore.Storage do
  alias Decree.Store.Config
  alias Decree.Store.Storage.Initializer

  def connect(config) do
    postgrex_config = Config.default_postgrex_opts(config)

    Postgrex.start_link(postgrex_config)
  end

  def config(schema \\ "public") do
    TestEventStore
    |> Config.parsed(:decree)
    |> Keyword.put(:schema, schema)
  end

  def reset!(conn, config), do: Initializer.reset!(conn, config)
end
