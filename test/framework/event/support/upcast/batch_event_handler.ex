defmodule Decree.Event.Upcast.BatchEventHandler do
  use Decree.Event.Handler,
    application: Decree.DefaultApp,
    name: __MODULE__,
    batch_size: 5

  def handle_batch([{_first, _metadata} | _rest] = events), do: send_reply(events)

  defp send_reply([{%{reply_to: reply_to}, _metadata} | _rest] = events) do
    send(:erlang.list_to_pid(reply_to), events)
    :ok
  end
end
