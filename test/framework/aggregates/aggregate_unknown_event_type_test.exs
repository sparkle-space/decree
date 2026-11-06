defmodule Decree.Aggregates.AggregateUnknownEventTypeTest do
  use Decree.PostgresCase

  # An aggregate whose stream holds an event of a type this release does not know (one a
  # newer release appended) refuses, never folding past it; an event handler's
  # subscription holds before it and the handler lives on (sparkle-space/sparklespace#7804).
  #
  # The type names no module, so `Decree.Serialization.JsonSerializer` (the framework
  # store's serializer, over the default type provider) does not know it.

  import ExUnit.CaptureLog

  alias Decree.Aggregates.Aggregate
  alias Decree.EventStore
  alias Decree.EventStore.EventData
  alias Decree.ExampleDomain.{BankAccount, BankApp}
  alias Decree.ExampleDomain.BankAccount.AccountBalanceHandler
  alias Decree.ExampleDomain.BankAccount.Commands.{DepositMoney, OpenAccount}
  alias Decree.ExampleDomain.BankAccount.Events.MoneyDeposited
  alias Decree.Helpers.{CommandAuditMiddleware, Wait}
  alias Decree.{Registration, UUID}

  @unknown_type "Elixir.Decree.X16.NotYetReleasedEvent"
  @populate_stop [:decree, :aggregate, :populate, :stop]
  @held [:decree, :store, :subscription, :held]

  setup do
    start_supervised!(BankApp)
    start_supervised!(CommandAuditMiddleware)

    test = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        [@populate_stop, @held],
        fn event, measurements, metadata, _config ->
          send(test, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    account_number = UUID.uuid4()

    :ok = BankApp.dispatch(%OpenAccount{account_number: account_number, initial_balance: 1_000})

    [account_number: account_number]
  end

  describe "an aggregate" do
    test "refuses a command on a cold rebuild", %{account_number: account_number} do
      :ok = Aggregate.shutdown(BankApp, BankAccount, account_number)

      append_unknown(account_number, 1)

      log =
        capture_log(fn ->
          assert {:error, :unknown_event_type} = BankApp.dispatch(deposit(account_number))
        end)

      assert log =~ "unknown event type"
      refute log =~ "terminating"
      assert resident(account_number) == :undefined

      assert_received {:telemetry, @populate_stop, %{count: 0},
                       %{error: :unknown_event_type, event_type: @unknown_type}}

      # Nothing was appended past it.
      assert [_opened, %{event_type: @unknown_type}] = stored(account_number)

      # A cold read exits as a call to a stopping aggregate does.
      name = Registration.via_tuple(BankApp, {BankApp, BankAccount, account_number})

      assert catch_exit(Aggregate.aggregate_state(BankApp, BankAccount, account_number)) ==
               {{:shutdown, :unknown_event_type},
                {GenServer, :call, [name, :aggregate_state, 5_000]}}
    end

    test "refuses on the rebuild after a wrong expected version", %{
      account_number: account_number
    } do
      pid = resident(account_number)
      ref = Process.monitor(pid)

      # Appended by a newer release: the publisher sends the aggregate nothing for it, so
      # the aggregate still holds version 1 and appends at it.
      append_unknown(account_number, 1)

      capture_log(fn ->
        assert {:error, :unknown_event_type} = BankApp.dispatch(deposit(account_number))
      end)

      assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :unknown_event_type}}
      assert [_opened, %{event_type: @unknown_type}] = stored(account_number)
    end

    test "a resident aggregate stops on the gap the publisher leaves", %{
      account_number: account_number
    } do
      pid = resident(account_number)
      ref = Process.monitor(pid)

      append_unknown(account_number, 1)

      :ok =
        EventStore.append_to_stream(BankApp, account_number, 2, [
          %EventData{
            event_type: Atom.to_string(MoneyDeposited),
            data: %MoneyDeposited{
              account_number: account_number,
              transfer_uuid: UUID.uuid4(),
              amount: 1,
              balance: 1_001
            },
            metadata: %{}
          }
        ])

      capture_log(fn ->
        assert_receive {:DOWN, ^ref, :process, ^pid, :unexpected_event_received}, 5_000
      end)
    end
  end

  describe "an event handler" do
    test "its subscription holds before the event, and the handler lives on", %{
      account_number: account_number
    } do
      handler = start_supervised!(AccountBalanceHandler)

      Wait.until(5_000, fn -> assert AccountBalanceHandler.current_balance() == 1_000 end)

      %{subscription: %{subscription_pid: subscription}} = :sys.get_state(handler)
      handler_ref = Process.monitor(handler)
      subscription_ref = Process.monitor(subscription)

      capture_log(fn ->
        append_unknown(account_number, 1)

        assert_receive {:telemetry, @held, _measurements,
                        %{event_type: @unknown_type, event_number: 2, transient: false}},
                       5_000
      end)

      store = Module.concat([BankApp, "FrameworkEventStore"])
      {infos, 0} = Decree.FrameworkEventStore.local_subscriptions(name: store)

      assert %{status: :parked} = Enum.find(infos, &(&1.pid == subscription))

      assert Process.alive?(handler)
      assert Process.alive?(subscription)
      assert %{subscription: %{subscription_pid: ^subscription}} = :sys.get_state(handler)
      refute_received {:DOWN, ^handler_ref, :process, _pid, _reason}
      refute_received {:DOWN, ^subscription_ref, :process, _pid, _reason}

      # The handler handled nothing past the event it never saw.
      assert AccountBalanceHandler.current_balance() == 1_000
    end
  end

  defp append_unknown(account_number, expected_version) do
    :ok =
      EventStore.append_to_stream(BankApp, account_number, expected_version, [
        %EventData{
          event_type: @unknown_type,
          data: %{"account_number" => account_number, "note" => "from a newer release"},
          metadata: %{}
        }
      ])
  end

  defp deposit(account_number) do
    %DepositMoney{account_number: account_number, transfer_uuid: UUID.uuid4(), amount: 100}
  end

  defp resident(account_number),
    do: Registration.whereis_name(BankApp, {BankApp, BankAccount, account_number})

  # The stream's rows, read without deserializing them.
  defp stored(account_number) do
    {:ok, conn} =
      Decree.FrameworkStorage.config()
      |> Decree.Store.Config.default_postgrex_opts()
      |> Postgrex.start_link()

    try do
      %Postgrex.Result{rows: rows} =
        Postgrex.query!(
          conn,
          """
          SELECT e.event_type FROM stream_events se
          JOIN streams s ON s.stream_id = se.stream_id
          JOIN events e ON e.event_id = se.event_id
          WHERE s.stream_uuid = $1 ORDER BY se.stream_version
          """,
          [account_number]
        )

      Enum.map(rows, fn [event_type] -> %{event_type: event_type} end)
    after
      GenServer.stop(conn)
    end
  end
end
