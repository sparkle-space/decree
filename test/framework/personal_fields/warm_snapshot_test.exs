defmodule Decree.PersonalFields.WarmSnapshotTest do
  use Decree.PersonalFields.Case

  alias Decree.Aggregate
  alias Decree.Aggregates.Aggregate, as: AggregateProcess
  alias Decree.EventStore.SnapshotData
  alias Decree.PersonalFields.Fixtures.Account
  alias Decree.PersonalFields.Fixtures.Events.{Linked, Registered}

  # `Account` declares no snapshot on its definition: these run on the `snapshotting:`
  # config, the fallback, at an interval no dispatch here reaches.
  setup do
    start_supervised!(
      {App, snapshotting: %{Account => [snapshot_every: 100, snapshot_version: 4]}}
    )

    :ok
  end

  test "records the personal state as ciphertext, through the snapshot's encryption",
       %{conn: conn, stream: stream, subject: subject} do
    dispatch!(stream, [
      %Registered{user_id: subject, email: email()},
      %Linked{user_id: subject, provider: "p", provider_uid: "uid-at-provider"}
    ])

    :ok = AggregateProcess.shutdown(App, Account, stream)

    assert Aggregate.warm_snapshot(App, Account, stream) == {:ok, :recorded}

    assert [row] = rows(conn, "versioned_snapshots", "data")
    assert row =~ "$pf"
    refute row =~ "uid-at-provider"
    refute row =~ "secret.example"

    assert {:ok,
            %SnapshotData{
              source_version: 2,
              snapshot_version: 4,
              data: %Account{links: [%{provider_uid: %{"$pf": 1}}]}
            }} = EventStore.read_snapshot(App, stream, 4)

    assert %Account{links: [%{provider_uid: "uid-at-provider"}], email: "alice@secret.example"} =
             AggregateProcess.aggregate_state(App, Account, stream)
  end

  test "an unreadable key answers its error, and records nothing", context do
    %{conn: conn, stream: stream, subject: subject} = context

    dispatch!(stream, %Linked{user_id: subject, provider: "p", provider_uid: "uid"})
    :ok = AggregateProcess.shutdown(App, Account, stream)

    Keys.set(subject, :unknown)

    assert Aggregate.warm_snapshot(App, Account, stream) == {:error, :key_unknown}
    assert rows(conn, "versioned_snapshots", "data") == []
  end
end
