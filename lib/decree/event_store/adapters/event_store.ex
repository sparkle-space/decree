defmodule Decree.EventStore.Adapters.EventStore do
  @moduledoc """
  Decree.Store adapter for Decree.
  """

  alias Decree.EventStore.Adapters.EventStore.Mapper
  alias Decree.Store.Config
  alias Decree.Store.ReadResult

  @behaviour Decree.EventStore.Adapter

  @all_stream "$all"

  @impl Decree.EventStore.Adapter
  def child_spec(application, config) do
    {event_store, config} = Keyword.pop(config, :event_store)

    verify_event_store!(application, event_store)

    name = Keyword.get(config, :name, event_store)

    # Rename `prefix` config to `schema`
    config =
      case Keyword.pop(config, :prefix) do
        {nil, config} -> config
        {prefix, config} -> Keyword.put(config, :schema, prefix)
      end

    child_spec = [{event_store, config}]
    adapter_meta = %{event_store: event_store, name: name}

    {:ok, child_spec, adapter_meta}
  end

  @impl Decree.EventStore.Adapter
  def append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts \\ []) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    events = Enum.map(events, &Mapper.to_event_data/1)

    opts = Keyword.put(opts, :name, name)
    event_store.append_to_stream(stream_uuid, expected_version, events, opts)
  end

  @impl Decree.EventStore.Adapter
  def append_to_streams(adapter_meta, appends, opts) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    appends =
      Enum.map(appends, fn {stream_uuid, expected_version, events} ->
        {stream_uuid, expected_version, Enum.map(events, &Mapper.to_event_data/1)}
      end)

    event_store.append_to_streams(appends, Keyword.put(opts, :name, name))
  end

  @impl Decree.EventStore.Adapter
  def read_query(adapter_meta, items, opts) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    with {:ok, %ReadResult{events: events} = result} <-
           event_store.read_query(items, Keyword.put(opts, :name, name)) do
      {:ok, %ReadResult{result | events: Enum.map(events, &Mapper.from_recorded_event/1)}}
    end
  end

  @impl Decree.EventStore.Adapter
  def read_tags(adapter_meta, positions, opts) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    event_store.read_tags(positions, Keyword.put(opts, :name, name))
  end

  @impl Decree.EventStore.Adapter
  def stream_forward(adapter_meta, stream_uuid, start_version \\ 0, read_batch_size \\ 1_000) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    opts = [name: name, read_batch_size: read_batch_size]

    case event_store.stream_forward(stream_uuid, start_version, opts) do
      {:error, error} -> {:error, error}
      stream -> Stream.map(stream, &Mapper.from_recorded_event/1)
    end
  end

  @impl Decree.EventStore.Adapter
  def read_stream_head(adapter_meta, stream_uuid) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    case event_store.read_stream_backward(stream_uuid, -1, 1, name: name) do
      {:ok, [event]} -> {:ok, Mapper.from_recorded_event(event)}
      # The store reads the stream's row, then its events: a hard delete can land between them.
      {:ok, []} -> {:error, :stream_not_found}
      {:error, _error} = error -> error
    end
  end

  @impl Decree.EventStore.Adapter
  def deletable_stream_prefixes(adapter_meta) do
    {_event_store, name} = extract_adapter_meta(adapter_meta)

    # `delete_stream/4` refuses a hard delete without this setting, so what a store declares
    # could never be deleted. The config's parse makes it a boolean.
    if Config.lookup(name, :enable_hard_deletes) do
      case Config.lookup(name, :deletable) do
        nil -> []
        deletable -> Keyword.get(deletable, :stream_prefixes, [])
      end
    else
      []
    end
  end

  @impl Decree.EventStore.Adapter
  def conditional_targets(adapter_meta) do
    {_event_store, name} = extract_adapter_meta(adapter_meta)

    case Config.lookup(name, :conditional_appends) do
      nil -> []
      conditional_appends -> Keyword.get(conditional_appends, :targets, [])
    end
  end

  @impl Decree.EventStore.Adapter
  def subscribe(adapter_meta, :all), do: subscribe(adapter_meta, @all_stream)

  @impl Decree.EventStore.Adapter
  def subscribe(adapter_meta, stream_uuid) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    event_store.subscribe(stream_uuid, name: name, mapper: &Mapper.from_recorded_event/1)
  end

  @impl Decree.EventStore.Adapter
  def subscribe_to(adapter_meta, :all, subscription_name, subscriber, start_from, opts) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    event_store.subscribe_to_all_streams(
      subscription_name,
      subscriber,
      subscription_options(name, start_from, opts)
    )
  end

  @impl Decree.EventStore.Adapter
  def subscribe_to(adapter_meta, stream_uuid, subscription_name, subscriber, start_from, opts) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    event_store.subscribe_to_stream(
      stream_uuid,
      subscription_name,
      subscriber,
      subscription_options(name, start_from, opts)
    )
  end

  @impl Decree.EventStore.Adapter
  def ack_event(adapter_meta, subscription, %Decree.EventStore.RecordedEvent{} = event) do
    %Decree.EventStore.RecordedEvent{event_number: event_number} = event

    {event_store, _name} = extract_adapter_meta(adapter_meta)

    event_store.ack(subscription, event_number)
  end

  @impl Decree.EventStore.Adapter
  def unsubscribe(_adapter_meta, subscription) do
    Decree.Store.Subscriptions.Subscription.unsubscribe(subscription)
  end

  @impl Decree.EventStore.Adapter
  def delete_subscription(adapter_meta, :all, subscription_name) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    event_store.delete_subscription(@all_stream, subscription_name, name: name)
  end

  @impl Decree.EventStore.Adapter
  def delete_subscription(adapter_meta, stream_uuid, subscription_name) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    event_store.delete_subscription(stream_uuid, subscription_name, name: name)
  end

  @impl Decree.EventStore.Adapter
  def read_snapshot(adapter_meta, source_uuid) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    with {:ok, snapshot_data} <- event_store.read_snapshot(source_uuid, name: name) do
      snapshot = Mapper.from_snapshot_data(snapshot_data)

      {:ok, snapshot}
    end
  end

  @impl Decree.EventStore.Adapter
  def record_snapshot(adapter_meta, %Decree.EventStore.SnapshotData{} = snapshot) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    snapshot
    |> Mapper.to_snapshot_data()
    |> event_store.record_snapshot(name: name)
  end

  @impl Decree.EventStore.Adapter
  def delete_snapshot(adapter_meta, source_uuid) do
    {event_store, name} = extract_adapter_meta(adapter_meta)

    event_store.delete_snapshot(source_uuid, name: name)
  end

  defp subscription_options(name, start_from, opts) do
    opts
    |> Keyword.merge(
      name: name,
      start_from: start_from,
      mapper: &Mapper.from_recorded_event/1
    )
    |> Keyword.update(:partition_by, nil, fn
      nil ->
        nil

      partition_by when is_function(partition_by, 1) ->
        fn %Decree.Store.RecordedEvent{} = event ->
          Mapper.from_recorded_event(event) |> partition_by.()
        end
    end)
  end

  defp extract_adapter_meta(adapter_meta) do
    event_store = Map.fetch!(adapter_meta, :event_store)
    name = Map.fetch!(adapter_meta, :name)

    {event_store, name}
  end

  defp verify_event_store!(application, event_store) do
    unless event_store do
      raise ArgumentError,
            "missing :event_store option for event store adapter in application " <>
              inspect(application)
    end

    unless implements?(event_store, Decree.Store) do
      raise ArgumentError,
            "module " <>
              inspect(event_store) <>
              " is not a Decree.Store, " <>
              "ensure you pass an event store module to the :event_store config in application " <>
              inspect(application)
    end
  end

  # Returns `true` if module implements behaviour.
  defp implements?(module, behaviour) do
    behaviours = Keyword.take(module.__info__(:attributes), [:behaviour])

    [behaviour] in Keyword.values(behaviours)
  end
end
