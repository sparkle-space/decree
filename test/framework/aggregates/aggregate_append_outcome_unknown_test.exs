defmodule Decree.Aggregates.AggregateAppendOutcomeUnknownTest do
  use Decree.PostgresCase

  alias Decree.Aggregates.{Aggregate, LostAppendAdapter, LostAppendAggregate, LostAppendApp}
  alias Decree.Aggregates.LostAppendAggregate.{Close, Open}
  alias Decree.Store.AppendError
  alias Decree.{Registration, UUID}
  alias Decree.Helpers.Wait

  setup do
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
