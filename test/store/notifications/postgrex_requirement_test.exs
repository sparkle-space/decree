defmodule Decree.Store.Notifications.PostgrexRequirementTest do
  # The Postgrex series decree accepts (sparkle-space/sparklespace#8532). The store's own
  # LISTEN connection, `Decree.Store.Notifications.Connection`, passes the notification
  # fences on 0.23.0, and CI runs them on that one series, so decree accepts it alone
  # (founder ruling of 2026-11-23). Before 1.0 a Postgrex minor may break its API, so the
  # next one is out until it passes the same fences.
  use ExUnit.Case, async: true

  setup_all do
    {:postgrex, requirement} = List.keyfind(Mix.Project.config()[:deps], :postgrex, 0)

    %{requirement: Version.parse_requirement!(requirement)}
  end

  test "accepts Postgrex 0.23", %{requirement: requirement} do
    for version <- ["0.23.0", "0.23.9"] do
      assert Version.match?(version, requirement), "#{version} is not accepted"
    end
  end

  test "accepts no Postgrex series the fences do not run on", %{requirement: requirement} do
    for version <- ["0.22.4", "0.24.0", "1.0.0"] do
      refute Version.match?(version, requirement), "#{version} is accepted"
    end
  end

  test "the Postgrex the suite runs on is one the requirement accepts", %{
    requirement: requirement
  } do
    assert Version.match?(to_string(Application.spec(:postgrex, :vsn)), requirement)
  end
end
