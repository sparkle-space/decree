defmodule Decree.Event.EventHandlerUnknownEventTypeTest do
  use Decree.PostgresCase

  # An event handler's follow-up command, refused because its target stream holds an event
  # of a type this release does not know (sparkle-space/sparklespace#7804), never reaches the
  # handler's own `error/3`, which could skip the event and lose its effect. The
  # application's `on_event_handler_error` decides, and nothing can skip at this seam.
  #
  # The handler is part-way through a known event on `@trigger` and deposits into the
  # account `Control` names. The poisoned account's stream holds an event whose type names
  # no module, so `Decree.Serialization.JsonSerializer` does not know it and the aggregate
  # answers `{:error, :unknown_event_type}`, as it does for a newer release's event.

  import ExUnit.CaptureLog

  alias Decree.EventStore
  alias Decree.EventStore.EventData
  alias Decree.ExampleDomain.BankApp
  alias Decree.ExampleDomain.BankAccount.Commands.{DepositMoney, OpenAccount}
  alias Decree.FrameworkStorage
  alias Decree.Helpers.{CommandAuditMiddleware, Wait}
  alias Decree.Store.Config
  alias Decree.UUID

  @trigger "x16-handler-trigger"
  @unknown_type "Elixir.Decree.X16.NotYetReleasedEvent"

  defmodule Trigger do
    @derive Jason.Encoder
    defstruct [:note]
  end

  defmodule Control do
    @moduledoc false

    # Where the handler's follow-up goes, and the pid the handler and postures report to,
    # since they run in their own processes.

    use Agent

    def start_link(state), do: Agent.start_link(fn -> state end, name: __MODULE__)

    def target, do: Agent.get(__MODULE__, & &1.target)
    def retarget(account_number), do: Agent.update(__MODULE__, &%{&1 | target: account_number})

    def notify(message) do
      send(Agent.get(__MODULE__, & &1.sink), message)

      :ok
    end
  end

  defmodule FollowUpHandler do
    @moduledoc false

    # Deposits into `Control`'s target for each trigger, and answers the dispatch's error.
    # Its own `error/3` answers `:skip`, and must never be called for an unknown type.

    use Decree.Event.Handler,
      application: Decree.ExampleDomain.BankApp,
      name: __MODULE__,
      subscribe_to: "x16-handler-trigger"

    alias Decree.Event.EventHandlerUnknownEventTypeTest.{Control, Trigger}
    alias Decree.ExampleDomain.BankAccount.Commands.DepositMoney
    alias Decree.ExampleDomain.BankApp
    alias Decree.UUID

    def handle(%Trigger{}, _metadata) do
      command = %DepositMoney{
        account_number: Control.target(),
        transfer_uuid: UUID.uuid4(),
        amount: 1
      }

      case BankApp.dispatch(command) do
        :ok -> Control.notify(:followed_up)
        error -> error
      end
    end

    def handle(_event, _metadata), do: :ok

    def error(error, _event, _failure_context) do
      Control.notify({:module_error, error})

      :skip
    end
  end

  defmodule RetryingErrors do
    @moduledoc false

    # An `on_event_handler_error` module that retries, as a host's hold does. It counts.

    alias Decree.Event.EventHandlerUnknownEventTypeTest.Control
    alias Decree.Event.FailureContext

    def error(error, _event, %FailureContext{context: context} = failure_context) do
      failures = Map.get(context, :failures, 0) + 1
      Control.notify({:held, failures, error})

      {:retry, 10,
       %FailureContext{failure_context | context: Map.put(context, :failures, failures)}}
    end
  end

  defmodule SkippingErrors do
    @moduledoc false

    # An `on_event_handler_error` module that tries to skip.

    alias Decree.Event.EventHandlerUnknownEventTypeTest.Control

    def error(error, _event, _failure_context) do
      Control.notify({:asked_to_skip, error})

      :skip
    end
  end

  @handler_name inspect(FollowUpHandler)

  setup do
    start_supervised!(CommandAuditMiddleware)

    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})

    [conn: conn, poisoned: UUID.uuid4(), healthy: UUID.uuid4()]
  end

  describe "a follow-up refused over an unknown event type" do
    test "holds the handler past its own `error/3`, then handles the event once", context do
      %{conn: conn} = context

      start(context, RetryingErrors)

      capture_log(fn ->
        start_supervised!({FollowUpHandler, start_from: :origin})

        for attempt <- 1..5 do
          assert_receive {:held, ^attempt, {:error, :unknown_event_type}}, 5_000
          assert acked(conn, @handler_name) == 0
        end

        refute_received :followed_up
        refute_received {:module_error, _error}

        # A pod that knows the type serves the command.
        Control.retarget(context.healthy)

        assert_receive :followed_up, 5_000
        Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)

        refute_receive :followed_up
        refute_received {:module_error, _error}
      end)
    end

    for posture <- [nil, :stop, :backoff] do
      test "under `#{inspect(posture)}` stops the handler with nothing acked; a restart delivers it again",
           context do
        %{conn: conn} = context

        start(context, unquote(posture))
        Process.flag(:trap_exit, true)

        capture_log(fn ->
          {:ok, handler} = FollowUpHandler.start_link(start_from: :origin)

          assert_receive {:EXIT, ^handler, :unknown_event_type}, 5_000
        end)

        assert acked(conn, @handler_name) == 0
        refute_received {:module_error, _error}
        refute_received :followed_up

        Control.retarget(context.healthy)
        start_supervised!({FollowUpHandler, start_from: :origin})

        assert_receive :followed_up, 5_000
        Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)
        refute_receive :followed_up
        refute_received {:module_error, _error}
      end
    end

    test "under a module that answers `:skip` stops the handler", context do
      %{conn: conn} = context

      start(context, SkippingErrors)
      Process.flag(:trap_exit, true)

      capture_log(fn ->
        {:ok, handler} = FollowUpHandler.start_link(start_from: :origin)

        assert_receive {:asked_to_skip, {:error, :unknown_event_type}}, 5_000
        assert_receive {:EXIT, ^handler, :unknown_event_type}, 5_000
      end)

      assert acked(conn, @handler_name) == 0
      refute_received {:module_error, _error}
      refute_received :followed_up
    end

    test "logs at warning, without the event, and reports the error in telemetry", context do
      start(context, RetryingErrors)
      attach_telemetry([[:decree, :event, :handle, :stop]])

      log =
        capture_log([level: :warning], fn ->
          start_supervised!({FollowUpHandler, start_from: :origin})
          assert_receive {:held, 2, {:error, :unknown_event_type}}, 5_000
        end)

      assert log =~
               "can't handle event #1: its follow-up met an event type this release does not know"

      refute log =~ "[error]"
      refute log =~ "failed to handle event"
      refute log =~ "Trigger{"
      refute log =~ "from a newer release"

      assert_received {:telemetry, [:decree, :event, :handle, :stop], _measurements, metadata}
      assert metadata.error == :unknown_event_type
    end
  end

  # Starts the application under `posture`, opens both accounts, poisons one with an event
  # of an unknown type, and stores the trigger the handler is part-way through.
  defp start(context, posture) do
    %{poisoned: poisoned, healthy: healthy} = context

    start_supervised!({BankApp, on_event_handler_error: posture})

    for account_number <- [poisoned, healthy] do
      :ok = BankApp.dispatch(%OpenAccount{account_number: account_number, initial_balance: 10})
    end

    :ok =
      EventStore.append_to_stream(BankApp, poisoned, 1, [
        %EventData{
          event_type: @unknown_type,
          data: %{"account_number" => poisoned, "note" => "from a newer release"},
          metadata: %{}
        }
      ])

    start_supervised!({Control, %{sink: self(), target: poisoned}})

    :ok =
      EventStore.append_to_stream(BankApp, @trigger, 0, [
        %EventData{
          event_type: Atom.to_string(Trigger),
          data: %Trigger{note: "known"},
          metadata: %{}
        }
      ])

    # The poisoned account's aggregate is resident at version 1; the follow-up's append
    # fails on the wrong expected version and the rebuild meets the unknown type.
    assert {:error, :unknown_event_type} =
             capture_log_result(fn ->
               BankApp.dispatch(%DepositMoney{
                 account_number: poisoned,
                 transfer_uuid: UUID.uuid4(),
                 amount: 1
               })
             end)

    :ok
  end

  defp capture_log_result(fun) do
    {result, _log} = with_log(fun)

    result
  end

  # A subscription's stored position: nil or 0 means nothing was acked.
  defp acked(conn, name) do
    %{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT last_seen FROM subscriptions WHERE subscription_name = $1",
        [name]
      )

    case rows do
      [[last_seen]] -> last_seen || 0
      [] -> 0
    end
  end

  defp attach_telemetry(events) do
    id = make_ref()
    test = self()

    :telemetry.attach_many(
      id,
      events,
      fn event, measurements, metadata, _config ->
        send(test, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end
end
