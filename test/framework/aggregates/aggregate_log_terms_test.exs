defmodule Decree.Aggregates.AggregateLogTermsTest do
  use Decree.PostgresCase

  # An aggregate's log lines name it by its module and the last eight characters of its id,
  # a command or an event by its module, and an error or message by its class, never a full
  # id, a command's or an event's fields, or an exception's message
  # (sparkle-space/sparklespace#8358). Each test asserts its line still appears, and that
  # neither the aggregate's full id nor the marker it planted appears anywhere in the log.

  import ExUnit.CaptureLog

  alias Decree.Aggregates.{DefaultLifespanRouter, LifespanAggregate, LifespanRouter}
  alias Decree.Aggregates.LifespanAggregate.{Command, Event}
  alias Decree.DefaultApp
  alias Decree.EventStore.RecordedEvent
  alias Decree.{Registration, UUID}

  @marker "aggregate-log-terms-marker"

  setup do
    start_supervised!(DefaultApp)

    [aggregate_uuid: UUID.uuid4()]
  end

  test "opening an aggregate and executing a command name it by suffix and the command by module",
       %{aggregate_uuid: uuid} do
    log =
      capture_log([level: :debug], fn ->
        assert :ok =
                 DefaultLifespanRouter.dispatch(command(uuid, :event), application: DefaultApp)
      end)

    assert log =~
             "Locating aggregate process for `Decree.Aggregates.LifespanAggregate` with UUID " <>
               inspect(suffix(uuid))

    assert log =~
             "Decree.Aggregates.LifespanAggregate<#{suffix(uuid)}@0> executing command: " <>
               "Decree.Aggregates.LifespanAggregate.Command\n"

    refute log =~ uuid
  end

  test "an invalid lifespan names its value by class", %{aggregate_uuid: uuid} do
    log =
      capture_log([level: :debug], fn ->
        command = %Command{command(uuid, :noop) | lifespan: {:bogus, @marker}}

        assert :ok = LifespanRouter.dispatch(command, application: DefaultApp)
      end)

    assert log =~
             "Invalid timeout for aggregate lifespan Decree.Aggregates.Lifespan, expected a " <>
               "non-negative integer, `:infinity`, `:hibernate`, `:stop`, or `{:stop, reason}` " <>
               "but got: {:bogus, :redacted}"

    refute log =~ @marker
    refute log =~ uuid
  end

  test "events received from the subscription are named by their stream versions", %{
    aggregate_uuid: uuid
  } do
    pid = open(uuid)

    log =
      capture_log([level: :debug], fn ->
        send(pid, {:events, [recorded(uuid, 1, %Event{uuid: uuid, lifespan: :infinity})]})

        assert %LifespanAggregate{uuid: ^uuid} =
                 Decree.Aggregates.Aggregate.aggregate_state(DefaultApp, LifespanAggregate, uuid)
      end)

    assert log =~
             "Decree.Aggregates.LifespanAggregate<#{suffix(uuid)}@0> received events: [1]"

    refute log =~ uuid
  end

  test "an unexpected event is named by its module, and the stop by its class", %{
    aggregate_uuid: uuid
  } do
    pid = open(uuid)
    ref = Process.monitor(pid)

    log =
      capture_log([level: :debug], fn ->
        send(pid, {:events, [recorded(uuid, 5, %Event{uuid: uuid, lifespan: @marker})]})

        assert_receive {:DOWN, ^ref, :process, ^pid, :unexpected_event_received}
      end)

    assert log =~
             "Decree.Aggregates.LifespanAggregate<#{suffix(uuid)}@0> received an unexpected " <>
               "event: Decree.Aggregates.LifespanAggregate.Event"

    assert log =~
             "Decree.Aggregates.LifespanAggregate<#{suffix(uuid)}@0> stopping due to: " <>
               ":unexpected_event_received"

    # The stop is not a shutdown, so OTP's crash report follows, printing the process's
    # registered name and its last message: not decree's lines, so only those are checked.
    lines = log |> String.split("\n") |> Enum.filter(&(&1 =~ "LifespanAggregate<"))

    assert length(lines) >= 2

    for line <- lines do
      refute line =~ @marker
      refute line =~ uuid
    end
  end

  test "an unexpected message is named by its class", %{aggregate_uuid: uuid} do
    pid = open(uuid)

    log =
      capture_log([level: :debug], fn ->
        send(pid, {:unexpected, @marker, uuid})

        # A call after the message: the aggregate has handled it.
        Decree.Aggregates.Aggregate.aggregate_state(DefaultApp, LifespanAggregate, uuid)
      end)

    assert log =~ "received unexpected message in handle_info/2: {:unexpected, :redacted}"

    refute log =~ @marker
    refute log =~ uuid
  end

  test "a raise in execute is named by the exception's module, its stacktrace scrubbed", %{
    aggregate_uuid: uuid
  } do
    log =
      capture_log([level: :debug], fn ->
        # No `execute/2` clause matches the action: `FunctionClauseError`, whose frame holds
        # the command.
        assert {:error, %FunctionClauseError{}} =
                 DefaultLifespanRouter.dispatch(command(uuid, :unmatched),
                   application: DefaultApp
                 )
      end)

    assert log =~
             "Decree.Aggregates.LifespanAggregate<#{suffix(uuid)}@0> raised executing a " <>
               "command: ** (FunctionClauseError)"

    assert log =~ "Decree.Aggregates.LifespanAggregate.execute/2"

    refute log =~ "no function clause matching"

    # The default lifespan stops the aggregate after the raise, so OTP's crash report
    # follows, printing the process's registered name: not decree's lines, so only those are
    # checked (each starts with the aggregate's description or is a stack frame of its raise).
    lines = log |> String.split("\n") |> Enum.reject(&(&1 =~ ~r/GenServer|State:|\*\* \(stop\)/))

    for line <- lines, do: refute(line =~ uuid)
  end

  test "a failed snapshot is named by its error's class", %{aggregate_uuid: uuid} do
    stop_supervised!(DefaultApp)

    start_supervised!(
      {DefaultApp,
       snapshotting: %{LifespanAggregate => [snapshot_every: 100, snapshot_version: 1]}}
    )

    # The stream's row must exist for the snapshot's INSERT to be reached.
    assert :ok = DefaultLifespanRouter.dispatch(command(uuid, :event), application: DefaultApp)
    open(uuid)

    # A store error whose message carries the marker: a trigger refusing this aggregate's
    # snapshot row. The aggregate writes on its application's pool, so the trigger is
    # committed, scoped to this aggregate and dropped after the test.
    refuse_snapshot(uuid)

    log =
      capture_log([level: :debug], fn ->
        assert {:error, %Postgrex.Error{}} =
                 Decree.Aggregates.Aggregate.take_snapshot(DefaultApp, LifespanAggregate, uuid)
      end)

    assert log =~
             "Decree.Aggregates.LifespanAggregate<#{suffix(uuid)}@1> snapshot failed due to: " <>
               "Postgrex.Error"

    refute log =~ @marker
    refute log =~ uuid
  end

  defp refuse_snapshot(uuid) do
    query!(
      "CREATE FUNCTION public.aggregate_log_terms_refuse() RETURNS trigger LANGUAGE plpgsql " <>
        "AS $$ BEGIN RAISE EXCEPTION '#{@marker} %', NEW.source_uuid; END $$"
    )

    query!(
      "CREATE TRIGGER aggregate_log_terms_refuse BEFORE INSERT ON public.versioned_snapshots " <>
        "FOR EACH ROW WHEN (NEW.source_uuid = '#{uuid}') " <>
        "EXECUTE FUNCTION public.aggregate_log_terms_refuse()"
    )

    on_exit(fn ->
      query!("DROP TRIGGER IF EXISTS aggregate_log_terms_refuse ON public.versioned_snapshots")
      query!("DROP FUNCTION IF EXISTS public.aggregate_log_terms_refuse()")
    end)
  end

  defp query!(statement) do
    config = Decree.FrameworkStorage.config()
    {:ok, conn} = Postgrex.start_link(Decree.Store.Config.default_postgrex_opts(config))

    try do
      Postgrex.query!(conn, statement, [])
    after
      GenServer.stop(conn)
    end
  end

  defp command(uuid, action) do
    %Command{command_id: UUID.uuid4(), uuid: uuid, action: action}
  end

  defp open(uuid) do
    {:ok, ^uuid} =
      Decree.Aggregates.Supervisor.open_aggregate(DefaultApp, LifespanAggregate, uuid)

    Registration.whereis_name(DefaultApp, {DefaultApp, LifespanAggregate, uuid})
  end

  defp recorded(uuid, version, data) do
    %RecordedEvent{
      event_id: UUID.uuid4(),
      event_number: version,
      stream_id: uuid,
      stream_version: version,
      causation_id: nil,
      correlation_id: nil,
      event_type: Atom.to_string(Event),
      data: data,
      metadata: %{},
      created_at: DateTime.utc_now()
    }
  end

  defp suffix(id), do: "..." <> String.slice(id, -8, 8)
end
