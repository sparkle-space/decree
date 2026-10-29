defmodule Decree.Middleware.TenantApp do
  use Decree.Application, otp_app: :decree

  alias Decree.Middleware.TenantRouter

  router(TenantRouter)

  # One event store process per application instance (see Decree.FrameworkStorage).
  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}

  def tenant_application_name(tenant_id) do
    Module.concat([__MODULE__, "tenant#{tenant_id}"])
  end
end
