defmodule Decree.Event.PartitionEventHandler do
  @moduledoc false
  alias Decree.Event.ConcurrencyEvent

  use Decree.Event.Handler,
    application: Decree.DefaultApp,
    name: __MODULE__,
    concurrency: 5

  @impl Decree.Event.Handler
  def after_start(_state) do
    Process.send(:test, {:init, self()}, [])
  end

  @impl Decree.Event.Handler
  def init(config) do
    Process.send(:test, {:init, config, self()}, [])

    {:ok, config}
  end

  @impl Decree.Event.Handler
  def handle(%ConcurrencyEvent{} = event, _metadata) do
    %ConcurrencyEvent{stream_uuid: stream_uuid} = event

    Process.send(:test, {:event, stream_uuid, self()}, [])
  end

  @impl Decree.Event.Handler
  def partition_by(%ConcurrencyEvent{} = event, metadata) do
    %ConcurrencyEvent{partition: partition} = event

    Process.send(:test, {:partition_by, event, metadata}, [])

    partition
  end
end
