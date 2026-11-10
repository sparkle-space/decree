defmodule Mix.Tasks.Decree.Verify.EventlessTest do
  @moduledoc """
  `mix decree.verify.eventless` over compiled routers (sparkle-space/sparklespace#7865):
  the population is each router's registry, the source index the project's
  `elixirc_paths`. The fixtures are in `test/framework/verify/support/`.
  """

  # Swaps the Mix shell and reads the real project's sources.
  use ExUnit.Case, async: false

  alias Decree.VerifyFixtures.Eventless.EventlessRouter
  alias Decree.VerifyFixtures.Eventless.GreenRouter
  alias Mix.Tasks.Decree.Verify.Eventless, as: EventlessTask

  @fixtures "test/framework/verify/support/eventless_fixtures.ex"

  setup do
    previous = Mix.shell()
    Mix.shell(Mix.Shell.Process)
    on_exit(fn -> Mix.shell(previous) end)
  end

  defp run!(args), do: EventlessTask.run(args ++ ["--no-compile"])

  defp errors do
    receive do
      {:mix_shell, :error, [line]} -> [line | errors()]
    after
      0 -> []
    end
  end

  test "raises on an eventless arm and prints one line per finding" do
    assert_raise Mix.Error, ~r/decree\.verify\.eventless failed/, fn ->
      run!(["--router", inspect(EventlessRouter)])
    end

    assert ["decree.verify.eventless: 2 finding(s):" | lines] = errors()

    assert [execute, rename] = lines

    assert execute ==
             "  #{@fixtures}:19  Decree.VerifyFixtures.Eventless.Account.execute/2  :ok" <>
               "  (reached from Decree.VerifyFixtures.Eventless.Account.execute clause at " <>
               "#{@fixtures}:19)"

    assert rename ==
             "  #{@fixtures}:30  Decree.VerifyFixtures.Eventless.Profile.rename/2  []" <>
               "  (reached from Decree.VerifyFixtures.Eventless.Profile.rename clause at " <>
               "#{@fixtures}:30)"
  end

  test "passes a router whose every routed handler returns events" do
    run!(["--router", inspect(GreenRouter)])

    assert_received {:mix_shell, :info, [line]}
    assert line =~ "decree.verify.eventless: OK - 2 handler clause(s) in 1 routed module(s)"
    assert errors() == []
  end

  test "reads every `--router`" do
    assert_raise Mix.Error, fn ->
      run!(["--router", inspect(GreenRouter), "--router", inspect(EventlessRouter)])
    end

    assert ["decree.verify.eventless: 2 finding(s):" | _lines] = errors()
  end

  test "raises with the usage when no `--router` is given" do
    assert_raise Mix.Error, ~r/usage: mix decree\.verify\.eventless --router/, fn ->
      run!([])
    end
  end

  test "raises for a module that is not a router" do
    assert_raise Mix.Error, ~r/not a router/, fn ->
      run!(["--router", "Enum"])
    end

    assert_raise Mix.Error, ~r/not a router/, fn ->
      run!(["--router", "Decree.VerifyFixtures.NoSuchRouter"])
    end
  end
end
