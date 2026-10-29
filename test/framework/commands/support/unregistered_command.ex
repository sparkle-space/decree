defmodule Decree.Commands.UnregisteredCommand do
  @moduledoc false
  alias Decree.UUID

  defstruct aggregate_uuid: UUID.uuid4()
end
