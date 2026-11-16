defmodule Decree.Registration.LocalRegistryLogTermsTest do
  use ExUnit.Case, async: true

  # A registered process's unexpected message is logged by its class, never its contents
  # (sparkle-space/sparklespace#8358).

  import ExUnit.CaptureLog

  alias Decree.Registration.LocalRegistry

  @marker "local-registry-log-terms-marker"

  test "an unexpected message is named by its class" do
    log =
      capture_log([level: :debug], fn ->
        assert {:noreply, :state} = LocalRegistry.handle_info({:unexpected, @marker}, :state)
      end)

    assert log =~ "received unexpected message in handle_info/2: {:unexpected, :redacted}"
    refute log =~ @marker
  end
end
