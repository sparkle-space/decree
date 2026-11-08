defmodule Decree.Middleware.ApplicationMiddlewareTest do
  use Decree.PostgresCase

  alias Decree.Middleware.Tenant.Commands.RegisterTenant
  alias Decree.Middleware.TenantApp
  alias Decree.UUID

  setup do
    tenant_ids = for _tenant <- 1..3, do: UUID.uuid4()

    for tenant_id <- tenant_ids do
      start_supervised!({TenantApp, name: TenantApp.tenant_application_name(tenant_id)})
    end

    [tenant_ids: tenant_ids]
  end

  test "dispatch command to dynamic application set in middleware", %{tenant_ids: tenant_ids} do
    for tenant_id <- tenant_ids do
      command = %RegisterTenant{
        command_id: UUID.uuid4(),
        tenant_id: tenant_id,
        name: "Tenant #{tenant_id}"
      }

      assert :ok = TenantApp.dispatch(command)

      name = TenantApp.tenant_application_name(tenant_id)
      pid = Process.whereis(name)
      assert is_pid(pid)
    end
  end

  test "raise `RuntimeError` when application not started" do
    command = %RegisterTenant{command_id: UUID.uuid4(), tenant_id: UUID.uuid4(), name: "Tenant 4"}

    assert_raise RuntimeError, fn ->
      :ok = TenantApp.dispatch(command)
    end
  end
end
