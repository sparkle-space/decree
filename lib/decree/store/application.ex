defmodule Decree.Store.Application do
  @moduledoc false

  # The store layer's process tree. decree is one OTP application: `Decree.start/2`
  # starts this supervisor before the framework's own children.

  def child_spec(_arg) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, []}, type: :supervisor}
  end

  def start_link do
    children = [
      Decree.Store.Config.Store,
      {Registry, keys: :duplicate, name: MonitoredServer.Registry}
    ]

    opts = [strategy: :one_for_one, name: Decree.Store.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
