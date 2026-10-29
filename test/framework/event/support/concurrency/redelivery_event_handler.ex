defmodule Decree.Event.RedeliveryEventHandler do
  @moduledoc false

  # A `concurrency: 2` handler whose `handle/2` blocks on the event with index 1
  # until the test sends `:proceed`. Every call reports to the test process, so the
  # test sees which instance handles each event, and whether it is handled at all.

  alias Decree.Event.ConcurrencyEvent

  use Decree.Event.Handler,
    application: Decree.DefaultApp,
    name: __MODULE__,
    concurrency: 2

  @impl Decree.Event.Handler
  def handle(%ConcurrencyEvent{} = event, _metadata),
    do: Decree.Event.RedeliveryEventHandler.handle_event(event)

  @doc false
  def handle_event(%ConcurrencyEvent{index: index}) do
    send(:test, {:handling, index, self()})

    if index == 1 do
      receive do
        :proceed -> :ok
      end
    end

    send(:test, {:handled, index, self()})

    :ok
  end
end

defmodule Decree.Event.PartitionedRedeliveryEventHandler do
  @moduledoc false

  # `Decree.Event.RedeliveryEventHandler`, partitioned by the event's `partition`.

  alias Decree.Event.ConcurrencyEvent

  use Decree.Event.Handler,
    application: Decree.DefaultApp,
    name: __MODULE__,
    concurrency: 2

  @impl Decree.Event.Handler
  def handle(%ConcurrencyEvent{} = event, _metadata),
    do: Decree.Event.RedeliveryEventHandler.handle_event(event)

  @impl Decree.Event.Handler
  def partition_by(%ConcurrencyEvent{partition: partition}, _metadata), do: partition
end
