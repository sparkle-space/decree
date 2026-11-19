defmodule Decree.RuntimeConfiguredApplication do
  use Decree.Application, otp_app: :decree

  def init(config) do
    config =
      config
      |> Keyword.put(:event_store, Decree.FrameworkStorage.event_store_config())
      |> Keyword.put(:registry, :local)
      |> Decree.FrameworkStorage.name_event_store()

    {:ok, config}
  end
end
