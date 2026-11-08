defmodule Decree.Commands.TimeoutCommand do
  @moduledoc false
  defstruct aggregate_uuid: nil, command_id: nil, sleep_in_ms: nil
end
