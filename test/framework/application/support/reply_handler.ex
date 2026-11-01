defmodule Decree.ReplyHandler do
  use Decree.Event.Handler,
    application: Decree.ExampleApplication,
    name: __MODULE__,
    personal_fields: :opaque

  def handle(event, _metadata) do
    reply_to = Agent.get(:reply_to, fn reply_to -> reply_to end)

    send(reply_to, {:event, self(), event})

    :ok
  end
end
