defmodule Decree.MockEventStoreCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  import Mox

  alias Decree.EventStore.Mock, as: MockEventStore
  alias Decree.MockedApp

  using do
    quote do
      import Mox

      alias Decree.EventStore.Mock, as: MockEventStore
    end
  end

  setup [:set_mox_global, :stub_event_store]

  setup do
    # Tests here also start Postgres-backed applications: start them on an empty store.
    Decree.FrameworkStorage.reset!()

    start_supervised!(MockedApp)

    :ok
  end

  def stub_event_store(_context) do
    stub_no_store_process()

    stub(MockEventStore, :ack, fn _subscription, _event_number ->
      :ok
    end)

    stub(MockEventStore, :subscribe_to_all_streams, fn
      _handler_name, subscriber, _opts ->
        send(subscriber, {:subscribed, self()})

        {:ok, self()}
    end)

    stub(MockEventStore, :subscribe_to_stream, fn
      _stream, _handler_name, subscriber, _opts ->
        send(subscriber, {:subscribed, self()})

        {:ok, self()}
    end)

    :ok
  end

  @doc """
  Has an application on the mock start no store process: the store's child answers
  `:ignore`.
  """
  def stub_no_store_process do
    stub(MockEventStore, :child_spec, fn config ->
      %{id: MockEventStore, start: {MockEventStore, :start_link, [config]}}
    end)

    stub(MockEventStore, :start_link, fn _config -> :ignore end)

    :ok
  end

  @doc """
  Starts an instance of `Decree.MockedApp` for a test whose mock is in Mox's private mode, and
  answers its name. Its application supervisor may call the mock, which starts no store
  process; every expectation is the test's own.
  """
  def start_mocked_app! do
    name = Module.concat(MockedApp, "Instance#{System.unique_integer([:positive])}")

    allow(MockEventStore, self(), fn -> Process.whereis(name) end)
    stub_no_store_process()

    ExUnit.Callbacks.start_supervised!({MockedApp, name: name})

    name
  end

  @doc """
  Starts a stand-in for a store subscription process, which answers the unsubscribe call
  `Decree.EventStore.unsubscribe/2` makes of it with `:ok`. A subscription the stubs above
  hand out is its subscriber itself, which cannot call itself.
  """
  def start_stand_in_subscription! do
    ExUnit.Callbacks.start_supervised!(__MODULE__.StandInSubscription)
  end

  defmodule StandInSubscription do
    @moduledoc false

    use GenServer

    def start_link(_arg), do: GenServer.start_link(__MODULE__, nil)

    @impl GenServer
    def init(nil), do: {:ok, nil}

    @impl GenServer
    def handle_call({:unsubscribe, _subscriber}, _from, state), do: {:reply, :ok, state}
  end
end
