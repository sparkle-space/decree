defmodule Decree.Commands.AggregateRoot do
  @moduledoc false
  alias Decree.Commands.AggregateRoot

  defmodule Command, do: defstruct(uuid: nil, command_id: nil)
  defmodule Command2, do: defstruct(uuid: nil, command_id: nil)

  defstruct uuid: nil

  def execute(%AggregateRoot{}, %Command{}), do: []

  def custom_function_name(%AggregateRoot{}, %Command2{}), do: []
end
