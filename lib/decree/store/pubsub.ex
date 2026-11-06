defmodule Decree.Store.PubSub do
  @moduledoc """
  Pub/sub using Elixir's local `Registry` module.
  """

  @doc """
  Return the child spec.
  """
  @spec child_spec(Decree.Store.t()) :: [:supervisor.child_spec()]
  def child_spec(event_store) do
    registry_name = registry_name(event_store)

    [
      Supervisor.child_spec(
        {
          Registry,
          keys: :duplicate, name: registry_name, partitions: System.schedulers_online()
        },
        id: registry_name
      )
    ]
  end

  @doc """
  Subscribes the caller to the given topic.

  `notices: true` also sends the caller `{:events_pending, event_number}` when the publisher
  stops a batch before an event whose type this instance does not know. A subscription
  process registers with it; an aggregate or a transient subscriber does not.

  The publisher cuts each notification's batch on its own: an event appended after the
  unknown one, in a later append, is still broadcast. A subscriber without notices checks
  `stream_version` or `event_number` continuity itself.
  """
  @spec subscribe(
          Decree.Store.t(),
          binary,
          selector: (Decree.Store.RecordedEvent.t() -> any()),
          mapper: (Decree.Store.RecordedEvent.t() -> any()),
          notices: boolean()
        ) :: :ok | {:error, term}
  def subscribe(event_store, topic, opts \\ [])

  def subscribe(event_store, topic, opts) do
    registry_name = registry_name(event_store)

    with {:ok, _} <- Registry.register(registry_name, topic, opts) do
      :ok
    end
  end

  @doc """
  Broadcasts message on given topic.
  """
  @spec broadcast(Decree.Store.t(), binary, term) :: :ok
  def broadcast(event_store, topic, message) do
    registry_name = registry_name(event_store)

    Registry.dispatch(registry_name, topic, fn entries ->
      for {pid, opts} <- entries do
        notify_subscriber(pid, message, opts)
      end
    end)
  end

  defp notify_subscriber(_pid, {:events, []}, _), do: nil

  defp notify_subscriber(pid, {:events, events}, opts) do
    selector = Keyword.get(opts, :selector)
    mapper = Keyword.get(opts, :mapper)

    events = events |> filter(selector) |> map(mapper)

    send(pid, {:events, events})
  end

  # A position notice from the publisher: the events of this batch from `event_number` on
  # were not broadcast, as the first is of a type this instance does not know. Only a
  # subscription process registers for it (`notices: true`); every other subscriber sees the
  # events before it, and a later batch's events still, so it checks continuity itself.
  defp notify_subscriber(pid, {:events_pending, _event_number} = message, opts) do
    if Keyword.get(opts, :notices, false), do: send(pid, message)
  end

  defp notify_subscriber(pid, message, _opts) do
    send(pid, message)
  end

  defp filter(events, selector) when is_function(selector, 1), do: Enum.filter(events, selector)
  defp filter(events, _selector), do: events

  defp map(events, mapper) when is_function(mapper, 1), do: Enum.map(events, mapper)
  defp map(events, _mapper), do: events

  defp registry_name(event_store), do: Module.concat([event_store, Decree.Store.PubSub])
end
