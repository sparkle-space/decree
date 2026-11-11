defmodule Decree.EventStore.EventStoreTestCase do
  use ExUnit.CaseTemplate

  alias Decree.EventStore.ConformanceStorage, as: Storage

  setup_all do
    config = Storage.config()

    {:ok, conn} = Storage.connect(config)

    [config: config, conn: conn]
  end

  setup %{config: config, conn: conn} do
    {:ok, application} = start_event_store()

    on_exit(fn ->
      Storage.reset!(conn, config)
    end)

    [application: application]
  end

  # Starts an instance of the conformance application, whose event store the cases run
  # against through `Decree.EventStore`, and answers the instance. `name:` names the instance,
  # and its store process after it; every other option is the store's config.
  def start_event_store(config \\ []) do
    {name, config} = Keyword.pop(config, :name, EventStoreApplication)

    event_store =
      config
      |> Keyword.put_new(:name, Module.concat([name, ConformanceEventStore]))
      |> Storage.event_store_config()

    start_supervised!({EventStoreApplication, name: name, event_store: event_store})

    {:ok, name}
  end
end
