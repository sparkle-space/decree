defmodule Decree.Event.EchoHandler do
  use Decree.Event.Handler,
    application: Decree.MockedApp,
    name: __MODULE__

  alias Decree.Event.ReplyEvent

  @impl Decree.Event.Handler
  def handle(%ReplyEvent{} = event, metadata) do
    %ReplyEvent{reply_to: reply_to} = event

    reply_to = :erlang.list_to_pid(reply_to)

    send(reply_to, {:event, self(), event, metadata})

    :ok
  end
end
