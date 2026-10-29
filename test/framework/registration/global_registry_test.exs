defmodule Decree.GlobalRegistryTest do
  alias Decree.Registration.GlobalRegistry
  alias Decree.RegistrationTestCase

  use RegistrationTestCase, registry: GlobalRegistry

  setup do
    {:ok, child_spec, registry_meta} = GlobalRegistry.child_spec(GlobalRegistry, [])

    for child <- child_spec, do: start_supervised!(child)

    [registry_meta: registry_meta]
  end
end
