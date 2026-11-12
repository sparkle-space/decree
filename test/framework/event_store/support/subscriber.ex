defmodule Decree.EventStore.Subscriber do
  use GenServer

  alias Decree.EventStore
  alias Decree.EventStore.Subscriber

  defmodule State do
    defstruct [
      :subscription_opts,
      :application,
      :owner,
      :subscription,
      received_events: [],
      subscribed?: false
    ]
  end

  alias Subscriber.State

  def start_link(application, owner, subscription_opts \\ []) do
    state = %State{
      application: application,
      owner: owner,
      subscription_opts: subscription_opts
    }

    GenServer.start_link(__MODULE__, state)
  end

  def init(%State{} = state) do
    %State{
      application: application,
      owner: owner,
      subscription_opts: opts
    } = state

    case EventStore.subscribe_to(application, :all, "subscriber", self(), :origin, opts) do
      {:ok, subscription} ->
        state = %State{state | subscription: subscription}

        {:ok, state}

      {:error, error} ->
        send(owner, {:subscribe_error, error, self()})

        {:ok, state}
    end
  end

  def ack(subscriber, events),
    do: GenServer.call(subscriber, {:ack, events})

  def subscribed?(subscriber),
    do: GenServer.call(subscriber, :subscribed?)

  def received_events(subscriber),
    do: GenServer.call(subscriber, :received_events)

  def handle_call({:ack, events}, _from, %State{} = state) do
    %State{
      application: application,
      subscription: subscription
    } = state

    :ok = EventStore.ack_event(application, subscription, List.last(events))

    {:reply, :ok, state}
  end

  def handle_call(:subscribed?, _from, %State{} = state) do
    %State{subscribed?: subscribed?} = state

    {:reply, subscribed?, state}
  end

  def handle_call(:received_events, _from, %State{} = state) do
    %State{received_events: received_events} = state

    {:reply, received_events, state}
  end

  def handle_info({:subscribed, subscription}, %State{subscription: subscription} = state) do
    %State{owner: owner} = state

    send(owner, {:subscribed, self(), subscription})

    {:noreply, %State{state | subscribed?: true}}
  end

  def handle_info({:events, events}, %State{} = state) do
    %State{owner: owner, received_events: received_events} = state

    send(owner, {:events, self(), events})

    state = %State{state | received_events: received_events ++ events}

    {:noreply, state}
  end
end
