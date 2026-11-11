defmodule Decree.AckingStore do
  @moduledoc false

  # An application's event store for a handler test that runs no store: `ack/2`, the only call
  # such a test makes of it, sends the subscription process the event number it acknowledged.
  # A test associates `config/0` as an application's `:event_store` config.

  def config, do: %{event_store: __MODULE__, name: __MODULE__}

  def ack(subscription_pid, event_number) do
    send(subscription_pid, {:acked, event_number})

    :ok
  end
end
