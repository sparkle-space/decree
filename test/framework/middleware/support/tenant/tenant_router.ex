defmodule Decree.Middleware.TenantRouter do
  use Decree.Commands.Router

  alias Decree.Middleware.Tenant
  alias Decree.Middleware.Tenant.Commands.RegisterTenant
  alias Decree.Middleware.TenantMiddleware

  middleware(TenantMiddleware)

  dispatch(RegisterTenant, to: Tenant, identity: :tenant_id)
end
