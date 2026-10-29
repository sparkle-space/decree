defmodule Decree.Store.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      Decree.Store.Config.Store,
      {Registry, keys: :duplicate, name: MonitoredServer.Registry}
    ]

    opts = [strategy: :one_for_one, name: Decree.Store.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
