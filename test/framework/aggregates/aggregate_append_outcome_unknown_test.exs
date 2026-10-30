defmodule Decree.Aggregates.AggregateAppendOutcomeUnknownTest do
  use Decree.PostgresCase

  alias Decree.Aggregates.{Aggregate, LostAppendAdapter, LostAppendAggregate, LostAppendApp}
  alias Decree.Aggregates.LostAppendAggregate.{Close, Open}
  alias Decree.Store.AppendError
  alias Decree.{Registration, UUID}
  alias Decree.Helpers.{CommandAuditMiddleware, Wait}

  setup do
    start_supervised!(CommandAuditMiddleware)
    start_supervised!(LostAppendApp)

    uuid = UUID.uuid4()
    on_exit(fn -> LostAppendAdapter.unflag(uuid) end)

    %{uuid: uuid}
  end

  # An append that was written but whose response was lost answers `:unavailable`.
  # The aggregate kept its pre-append state, so it must stop: the next command opens
  # a fresh aggregate that rebuilds from the stream and sees the written event.
  test "an `:unavailable` append error stops the aggregate, so the next command sees the written event",
       %{uuid: uuid} do
    pid = open_aggregate(uuid)
    ref = Process.monitor(pid)

    LostAppendAdapter.flag(uuid, :unavailable)

    assert {:error, %AppendError{class: :unavailable}} =
             LostAppendApp.dispatch(%Open{uuid: uuid})

    assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :append_outcome_unknown}}
    Wait.until(fn -> assert whereis(uuid) == :undefined end)

    # Refused on the pre-append state (`:not_open`); accepted once the `Opened`
    # event written by the lost append is applied.
    assert :ok = LostAppendApp.dispatch(%Close{uuid: uuid})

    assert is_pid(whereis(uuid))
    refute whereis(uuid) == pid
  end

  # A command already queued for the aggregate when it stops is retried by the
  # dispatcher on a fresh aggregate, which sees the written event.
  test "a command queued behind an `:unavailable` append is retried on a fresh aggregate",
       %{uuid: uuid} do
    pid = open_aggregate(uuid)
    LostAppendAdapter.flag(uuid, :unavailable)

    {open, close} = queue_behind_lost_append(pid, uuid, [])

    assert {:error, %AppendError{class: :unavailable}} = Task.await(open)
    assert :ok = Task.await(close)

    refute whereis(uuid) == pid
    assert {2, 1, 1} = CommandAuditMiddleware.count_commands()
  end

  # With no retry left, the queued command answers `:too_many_attempts` through the
  # failure middleware, as an exhausted `:wrong_expected_version` retry does.
  test "a queued command with no retry left answers `:too_many_attempts`", %{uuid: uuid} do
    pid = open_aggregate(uuid)
    LostAppendAdapter.flag(uuid, :unavailable)

    {open, close} = queue_behind_lost_append(pid, uuid, retry_attempts: 0)

    assert {:error, %AppendError{class: :unavailable}} = Task.await(open)
    assert {:error, :too_many_attempts} = Task.await(close)

    assert {2, 0, 2} = CommandAuditMiddleware.count_commands()
  end

  for class <- [:retryable, :rejected] do
    test "a `#{class}` append error keeps the aggregate running", %{uuid: uuid} do
      pid = open_aggregate(uuid)

      LostAppendAdapter.flag(uuid, unquote(class))

      assert {:error, %AppendError{class: unquote(class)}} =
               LostAppendApp.dispatch(%Open{uuid: uuid})

      # The same process answers a call after the failed command, so it did not stop.
      assert 0 == Aggregate.aggregate_version(LostAppendApp, LostAppendAggregate, uuid)
      assert whereis(uuid) == pid

      assert :ok = LostAppendApp.dispatch(%Open{uuid: uuid})
      assert whereis(uuid) == pid
    end
  end

  # Dispatches `Open` (whose append is lost) and then `Close` while the aggregate
  # is suspended, so both calls sit in its mailbox, in that order, before it runs.
  defp queue_behind_lost_append(pid, uuid, close_opts) do
    :ok = :sys.suspend(pid)

    try do
      open = Task.async(fn -> LostAppendApp.dispatch(%Open{uuid: uuid}) end)
      wait_for_queued_commands(pid, 1)

      close = Task.async(fn -> LostAppendApp.dispatch(%Close{uuid: uuid}, close_opts) end)
      wait_for_queued_commands(pid, 2)

      {open, close}
    after
      :sys.resume(pid)
    end
  end

  defp wait_for_queued_commands(pid, count) do
    Wait.until(fn ->
      {:messages, messages} = Process.info(pid, :messages)

      assert Enum.count(messages, &match?({:"$gen_call", _from, {:execute_command, _}}, &1)) ==
               count
    end)
  end

  defp open_aggregate(uuid) do
    {:ok, ^uuid} =
      Decree.Aggregates.Supervisor.open_aggregate(LostAppendApp, LostAppendAggregate, uuid)

    pid = whereis(uuid)
    assert is_pid(pid)
    pid
  end

  defp whereis(uuid),
    do: Registration.whereis_name(LostAppendApp, {LostAppendApp, LostAppendAggregate, uuid})
end
