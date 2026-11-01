defmodule Decree.Event.GracefulShutdownHandler do
  use Decree.Event.Handler,
    application: Decree.MockedApp,
    name: __MODULE__,
    personal_fields: :opaque

  def handle(event, metadata) do
    %{reply_to: reply_to} = event

    send(reply_to, {:event, event, metadata})

    receive do
      :continue ->
        send(reply_to, {:continue, event, metadata})

        :ok
    end

    :ok
  end
end
