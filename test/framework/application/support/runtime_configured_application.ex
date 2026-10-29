defmodule Decree.RuntimeConfiguredApplication do
  alias Decree.EventStore.Adapters.InMemory
  alias Decree.Serialization.JsonSerializer

  use Decree.Application, otp_app: :decree

  def init(config) do
    config =
      config
      |> Keyword.put(:event_store, adapter: InMemory, serializer: JsonSerializer)
      |> Keyword.put(:pubsub, phoenix_pubsub: [adapter: Phoenix.PubSub.PG2, pool_size: 1])
      |> Keyword.put(:registry, :local)

    {:ok, config}
  end
end
