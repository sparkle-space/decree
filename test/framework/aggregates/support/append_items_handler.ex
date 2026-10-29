defmodule Decree.Aggregates.AppendItemsHandler do
  @moduledoc false
  @behaviour Decree.Commands.Handler

  alias Decree.Aggregates.ExampleAggregate
  alias Decree.Aggregates.ExampleAggregate.Commands.AppendItems

  def handle(%ExampleAggregate{} = aggregate, %AppendItems{count: count}) do
    ExampleAggregate.append_items(aggregate, count)
  end
end
