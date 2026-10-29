defmodule Decree.Commands.TimeoutCommandHandler do
  @moduledoc false
  @behaviour Decree.Commands.Handler

  alias Decree.Commands.{TimeoutAggregateRoot, TimeoutCommand}

  def handle(%TimeoutAggregateRoot{}, %TimeoutCommand{sleep_in_ms: sleep_in_ms}) do
    :timer.sleep(sleep_in_ms)
    []
  end
end
