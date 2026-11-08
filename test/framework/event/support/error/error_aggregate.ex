defmodule Decree.Event.ErrorAggregate do
  @moduledoc false

  defstruct [:uuid]

  defmodule Commands do
    defmodule RaiseError do
      defstruct [:uuid, :command_id, :strategy, :delay, :reply_to]
    end

    defmodule RaiseException do
      defstruct [:uuid, :command_id, :strategy, :delay, :reply_to]
    end
  end

  defmodule Events do
    defmodule ErrorEvent do
      @derive Jason.Encoder
      defstruct [:uuid, :strategy, :delay, :reply_to]
    end

    defmodule ExceptionEvent do
      @derive Jason.Encoder
      defstruct [:uuid, :strategy, :delay, :reply_to]
    end

    defmodule InvalidReturnValueEvent do
      @derive Jason.Encoder
      defstruct [:uuid, :reply_to]
    end
  end

  alias Decree.Event.ErrorAggregate
  alias Commands.{RaiseError, RaiseException}
  alias Events.{ErrorEvent, ExceptionEvent}

  def execute(%ErrorAggregate{}, %RaiseError{} = command) do
    struct(ErrorEvent, Map.from_struct(command))
  end

  def execute(%ErrorAggregate{}, %RaiseException{} = command) do
    struct(ExceptionEvent, Map.from_struct(command))
  end

  def apply(%ErrorAggregate{} = aggregate, _event),
    do: aggregate
end
