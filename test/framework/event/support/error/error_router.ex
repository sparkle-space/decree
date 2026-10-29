defmodule Decree.Event.ErrorRouter do
  @moduledoc false

  use Decree.Commands.Router

  alias Decree.Event.ErrorAggregate
  alias Decree.Event.ErrorAggregate.Commands.{RaiseError, RaiseException}

  dispatch [RaiseError, RaiseException], to: ErrorAggregate, identity: :uuid
end
