defmodule Decree.Middleware.TenantApp do
  use Decree.Application, otp_app: :decree

  alias Decree.Middleware.TenantRouter

  router(TenantRouter)

  def tenant_application_name(tenant_id) do
    Module.concat([__MODULE__, "tenant#{tenant_id}"])
  end
end
