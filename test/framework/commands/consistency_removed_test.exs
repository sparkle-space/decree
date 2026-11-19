defmodule Decree.Commands.ConsistencyRemovedTest do
  # `consistency:` is removed (sparkle-space/sparklespace#8559): a dispatch returns once its events
  # are written, and an event handler catches up on its own. Every door that took the option
  # refuses it, whatever its value, at compile time where it is a `use` or route option and with
  # an `ArgumentError` at run time.
  use Decree.PostgresCase

  alias Decree.ExampleApplication
  alias Decree.ExampleDomain.BankAccount.Commands.OpenAccount
  alias Decree.ExampleDomain.{BankApp, BankRouter}
  alias Decree.Middleware.Pipeline
  alias Decree.UUID

  @removed ~r/`consistency:`.*removed/s

  defmodule PlainHandler do
    use Decree.Event.Handler,
      application: Decree.ExampleDomain.BankApp,
      name: "ConsistencyRemovedPlainHandler"
  end

  defmodule InitHandler do
    use Decree.Event.Handler,
      application: Decree.ExampleDomain.BankApp,
      name: "ConsistencyRemovedInitHandler"

    def init(config), do: {:ok, Keyword.put(config, :consistency, :eventual)}
  end

  defmodule DroppingInitHandler do
    use Decree.Event.Handler,
      application: Decree.ExampleDomain.BankApp,
      name: "ConsistencyRemovedDroppingInitHandler"

    def init(config), do: {:ok, Keyword.delete(config, :consistency)}
  end

  defp open_account do
    %OpenAccount{command_id: UUID.uuid4(), account_number: UUID.uuid4(), initial_balance: 1_000}
  end

  describe "the strong path is gone" do
    test "no `ConsistencyGuarantee` middleware and no strong subscription registry" do
      refute Code.ensure_loaded?(Decree.Middleware.ConsistencyGuarantee)
      refute Code.ensure_loaded?(Decree.Subscriptions)
      refute Code.ensure_loaded?(Decree.Subscriptions.Registry)
    end

    test "a pipeline carries no consistency" do
      refute Map.has_key?(%Pipeline{}, :consistency)
    end
  end

  describe "dispatch/2 on an aggregate route" do
    setup do
      start_supervised!(Decree.Helpers.CommandAuditMiddleware)
      start_supervised!(BankApp)
      :ok
    end

    for consistency <- [:strong, :eventual, [Decree.ExampleDomain.BankRouter]] do
      test "refuses `consistency: #{inspect(consistency)}` through the application" do
        assert_raise ArgumentError, @removed, fn ->
          BankApp.dispatch(open_account(), consistency: unquote(consistency))
        end

        assert :ok = BankApp.dispatch(open_account())
      end
    end

    test "refuses `consistency:` through the router" do
      assert_raise ArgumentError, @removed, fn ->
        BankRouter.dispatch(open_account(), application: BankApp, consistency: :strong)
      end

      assert :ok = BankRouter.dispatch(open_account(), application: BankApp)
    end
  end

  describe "an event handler" do
    setup do
      start_supervised!(BankApp)
      :ok
    end

    test "refuses `consistency:` given to start_link/1" do
      assert_raise ArgumentError, @removed, fn ->
        PlainHandler.start_link(consistency: :eventual)
      end
    end

    test "refuses `consistency:` returned from init/1" do
      assert_raise ArgumentError, @removed, fn -> InitHandler.start_link() end
    end

    test "refuses `consistency:` given to start_link/1 when init/1 drops it" do
      assert_raise ArgumentError, @removed, fn ->
        DroppingInitHandler.start_link(consistency: :strong)
      end
    end

    test "refuses `consistency:` given to the handler process's own start_link/4" do
      {application, name, config} =
        Decree.Event.Handler.parse_config!(PlainHandler,
          application: BankApp,
          name: "ConsistencyRemovedPlainHandler"
        )

      assert_raise ArgumentError, @removed, fn ->
        Decree.Event.Handler.start_link(
          application,
          name,
          PlainHandler,
          Keyword.put(config, :consistency, :strong)
        )
      end
    end
  end

  describe "a `use` or route option fails the build" do
    test "on `use Decree.Event.Handler`" do
      assert_raise ArgumentError, @removed, fn ->
        compile(
          quote do
            defmodule Decree.Commands.ConsistencyRemovedTest.UseHandler do
              use Decree.Event.Handler,
                application: Decree.ExampleDomain.BankApp,
                name: "ConsistencyRemovedUseHandler",
                consistency: :strong
            end
          end
        )
      end
    end

    test "on `use Decree.Projections.Ecto`" do
      assert_raise ArgumentError, @removed, fn ->
        compile(
          quote do
            defmodule Decree.Commands.ConsistencyRemovedTest.UseProjector do
              use Decree.Projections.Ecto,
                application: Decree.ExampleDomain.BankApp,
                name: "ConsistencyRemovedUseProjector",
                repo: Decree.Projections.Repo,
                consistency: :strong
            end
          end
        )
      end
    end

    test "on a `dispatch` route" do
      assert_raise ArgumentError, @removed, fn ->
        compile(
          quote do
            defmodule Decree.Commands.ConsistencyRemovedTest.DispatchRoute do
              use Decree.Commands.Router

              dispatch Decree.ExampleDomain.BankAccount.Commands.OpenAccount,
                to: Decree.ExampleDomain.BankAccount,
                identity: :account_number,
                opaque: true,
                consistency: :strong
            end
          end
        )
      end
    end

    test "on a `decide` route" do
      assert_raise ArgumentError, @removed, fn ->
        compile(
          quote do
            defmodule Decree.Commands.ConsistencyRemovedTest.DecideRoute do
              use Decree.Commands.Router

              decide(Decree.DecisionFixtures.Scripted,
                to: Decree.DecisionFixtures.ScriptedDecision,
                identity: :claim_id,
                identity_prefix: "scripted-",
                consistency: :eventual
              )
            end
          end
        )
      end
    end

    test "on `use Decree.Commands.Router, default_consistency:`" do
      assert_raise ArgumentError, ~r/`default_consistency:`.*removed/s, fn ->
        compile(
          quote do
            defmodule Decree.Commands.ConsistencyRemovedTest.RouterDefault do
              use Decree.Commands.Router, default_consistency: :strong
            end
          end
        )
      end
    end

    test "on `use Decree.Application, default_dispatch_opts: [consistency: ...]`" do
      assert_raise ArgumentError, @removed, fn ->
        compile(
          quote do
            defmodule Decree.Commands.ConsistencyRemovedTest.AppDefault do
              use Decree.Application,
                otp_app: :decree,
                event_store: [event_store: Decree.FrameworkEventStore],
                default_dispatch_opts: [consistency: :eventual]
            end
          end
        )
      end
    end

    test "on `use Decree.Commands.CompositeRouter, default_dispatch_opts: [consistency: ...]`" do
      assert_raise ArgumentError, @removed, fn ->
        compile(
          quote do
            defmodule Decree.Commands.ConsistencyRemovedTest.CompositeDefault do
              use Decree.Commands.CompositeRouter,
                default_dispatch_opts: [consistency: :strong]
            end
          end
        )
      end
    end
  end

  describe "the application config" do
    for {key, value} <- [default_consistency: :eventual, dispatch_consistency_timeout: 1_000] do
      test "refuses `config :decree, #{inspect(key)}` at start" do
        key = unquote(key)
        previous = Application.fetch_env(:decree, key)

        on_exit(fn ->
          case previous do
            {:ok, value} -> Application.put_env(:decree, key, value)
            :error -> Application.delete_env(:decree, key)
          end
        end)

        Application.put_env(:decree, key, unquote(value))

        message =
          start_refused(fn -> ExampleApplication.start_link(name: :"consistency_#{key}") end)

        assert message =~ "#{inspect(key)}"
        assert message =~ "removed"
      end
    end
  end

  defp compile(quoted), do: Code.compile_quoted(quoted)

  # The refusal is an `ArgumentError`, raised in the caller or answered by the supervisor.
  defp start_refused(start) do
    Process.flag(:trap_exit, true)

    try do
      start.()
    rescue
      error in ArgumentError -> error.message
    else
      {:error, {%ArgumentError{message: message}, _stacktrace}} -> message
      other -> flunk("expected the start to be refused, got: " <> inspect(other))
    end
  end
end

defmodule Decree.Decision.ConsistencyRemovedTest do
  use Decree.DecisionStoreCase

  alias Decree.DecisionFixtures.ParityCommand

  defp command do
    %ParityCommand{command_id: UUID.uuid4(), id: UUID.uuid4(), scope: "consistency-removed"}
  end

  for consistency <- [:strong, :eventual] do
    test "dispatch/2 on a decision route refuses `consistency: #{inspect(consistency)}`" do
      assert_raise ArgumentError, ~r/`consistency:`.*removed/s, fn ->
        App.dispatch(command(), consistency: unquote(consistency))
      end

      assert :ok = App.dispatch(command())
    end
  end
end
