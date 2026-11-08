defmodule Decree.Aggregates.SnapshotVersionsTest do
  # The `:type_provider` app env is global: this module is not async.
  use Decree.PostgresCase

  alias Decree.Aggregate
  alias Decree.Aggregates.Aggregate, as: AggregateProcess
  alias Decree.Aggregates.{AppendItemsHandler, ExampleAggregate, ExecutionContext, Supervisor}
  alias Decree.Aggregates.ExampleAggregate.Commands.AppendItems
  alias Decree.DefaultApp
  alias Decree.Event.Mapper
  alias Decree.EventStore
  alias Decree.EventStore.{SnapshotData, TypeProvider}
  alias Decree.FrameworkStorage
  alias Decree.PersonalFields.Fixtures.ShortTypes
  alias Decree.Snapshotting
  alias Decree.Store.Config
  alias Decree.UUID

  defmodule Tallied do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:n]
  end

  defmodule Tally do
    @moduledoc false
    use Decree.Aggregate, snapshot: [every: 100, version: 3]

    alias Decree.Aggregates.SnapshotVersionsTest.Tallied

    @derive Jason.Encoder
    defstruct count: 0

    def apply(%__MODULE__{} = state, %Tallied{n: n}), do: %__MODULE__{state | count: n}
  end

  defmodule Other do
    @moduledoc false
    use Decree.Aggregate, snapshot: [every: 100, version: 1]

    @derive Jason.Encoder
    defstruct count: 0
  end

  defmodule Undeclared do
    @moduledoc false
    defstruct count: 0
  end

  setup do
    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})

    [conn: conn]
  end

  describe "a version bump" do
    test "folds from events for the new version, and leaves the old version's row" do
      start_supervised!(
        {DefaultApp,
         snapshotting: %{ExampleAggregate => [snapshot_every: 1, snapshot_version: 1]}}
      )

      aggregate_uuid = UUID.uuid4()
      append_items(aggregate_uuid, 2)

      # Behind the snapshot in the aggregate's mailbox.
      AggregateProcess.aggregate_state(DefaultApp, ExampleAggregate, aggregate_uuid)

      assert {:ok, %SnapshotData{source_version: 2, snapshot_version: 1} = snapshot} =
               EventStore.read_snapshot(DefaultApp, aggregate_uuid, 1)

      # A version-1 row the events can't give.
      outdated = %ExampleAggregate{items: ["from version 1"], last_index: 2}
      :ok = EventStore.record_snapshot(DefaultApp, %SnapshotData{snapshot | data: outdated})

      stop_supervised!(DefaultApp)

      start_supervised!(
        {DefaultApp,
         snapshotting: %{ExampleAggregate => [snapshot_every: 1, snapshot_version: 2]}}
      )

      assert AggregateProcess.aggregate_state(DefaultApp, ExampleAggregate, aggregate_uuid) ==
               %ExampleAggregate{items: [1, 2], last_index: 2}

      assert {:error, :snapshot_not_found} =
               EventStore.read_snapshot(DefaultApp, aggregate_uuid, 2)

      version_1 = Snapshotting.new(DefaultApp, aggregate_uuid, snapshot_every: 1)

      assert {:ok, %SnapshotData{data: ^outdated}} = Snapshotting.read_snapshot(version_1)
    end
  end

  describe "`Decree.Aggregate.warm_snapshot/3`" do
    setup do
      start_supervised!(DefaultApp)

      :ok
    end

    test "records a row at the head, then answers current until the stream moves" do
      stream = UUID.uuid4()
      tally(stream, 1..2)

      assert Aggregate.warm_snapshot(DefaultApp, Tally, stream) == {:ok, :recorded}

      assert {:ok,
              %SnapshotData{
                source_uuid: ^stream,
                source_version: 2,
                snapshot_version: 3,
                data: %Tally{count: 2},
                metadata: %{"snapshot_module_version" => 3}
              }} = EventStore.read_snapshot(DefaultApp, stream, 3)

      assert Aggregate.warm_snapshot(DefaultApp, Tally, stream) == {:ok, :current}

      tally(stream, 3..4)

      assert Aggregate.warm_snapshot(DefaultApp, Tally, stream) == {:ok, :recorded}

      assert {:ok, %SnapshotData{source_version: 4, data: %Tally{count: 4}}} =
               EventStore.read_snapshot(DefaultApp, stream, 3)

      assert Aggregate.warm_snapshot(DefaultApp, Tally, stream) == {:ok, :current}
    end

    test "starts no aggregate process" do
      stream = UUID.uuid4()
      tally(stream, 1..1)

      assert Aggregate.warm_snapshot(DefaultApp, Tally, stream) == {:ok, :recorded}

      assert Decree.Registration.whereis_name(
               DefaultApp,
               AggregateProcess.name(DefaultApp, Tally, stream)
             ) == :undefined
    end

    test "answers empty for a stream with no events, and records nothing", %{conn: conn} do
      stream = UUID.uuid4()

      assert Aggregate.warm_snapshot(DefaultApp, Tally, stream) == {:ok, :empty}
      assert {:error, :snapshot_not_found} = EventStore.read_snapshot(DefaultApp, stream, 3)
      assert count_rows(conn, "versioned_snapshots") == 0
    end

    test "is refused for an aggregate without snapshots" do
      stream = UUID.uuid4()

      assert Aggregate.warm_snapshot(DefaultApp, Undeclared, stream) ==
               {:error, :snapshotting_not_configured}
    end
  end

  describe "`Decree.Aggregate.prune_snapshots/2`" do
    setup do
      start_supervised!(DefaultApp)

      :ok
    end

    test "deletes the module's rows below its declared version, by the configured type",
         %{conn: conn} do
      Application.put_env(:decree, :type_provider, ShortTypes)
      on_exit(fn -> Application.delete_env(:decree, :type_provider) end)

      tally_type = TypeProvider.to_string(%Tally{})
      assert tally_type == "Tally"

      pruned = UUID.uuid4()
      by_module_name = UUID.uuid4()
      other = UUID.uuid4()

      for stream <- [pruned, by_module_name, other], do: tally(stream, 1..1)

      for version <- 1..3, do: record(pruned, version, tally_type)
      record(by_module_name, 1, Atom.to_string(Tally))
      record(other, 1, TypeProvider.to_string(%Other{}))

      assert Aggregate.prune_snapshots(DefaultApp, Tally) == {:ok, 2}

      kept =
        Enum.sort([
          [by_module_name, 1, Atom.to_string(Tally)],
          [other, 1, "Other"],
          [pruned, 3, "Tally"]
        ])

      assert versions(conn) == kept

      assert Aggregate.prune_snapshots(DefaultApp, Tally) == {:ok, 0}
    end

    test "is refused for an aggregate without snapshots" do
      assert Aggregate.prune_snapshots(DefaultApp, Undeclared) ==
               {:error, :snapshotting_not_configured}
    end
  end

  defp tally(stream, range) do
    events = Mapper.map_to_event_data(Enum.map(range, &%Tallied{n: &1}))

    :ok = EventStore.append_to_stream(DefaultApp, stream, :any_version, events)
  end

  defp record(stream, version, source_type) do
    :ok =
      EventStore.record_snapshot(DefaultApp, %SnapshotData{
        source_uuid: stream,
        source_version: 1,
        source_type: source_type,
        snapshot_version: version,
        data: %{"count" => 1},
        metadata: %{"snapshot_module_version" => version}
      })
  end

  defp versions(conn) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT source_uuid, snapshot_version, source_type FROM versioned_snapshots",
        []
      )

    Enum.sort(rows)
  end

  defp count_rows(conn, table) do
    %Postgrex.Result{rows: [[count]]} = Postgrex.query!(conn, "SELECT count(*) FROM #{table}", [])

    count
  end

  defp append_items(aggregate_uuid, count) do
    context = %ExecutionContext{
      command: %AppendItems{count: count},
      handler: AppendItemsHandler,
      function: :handle
    }

    {:ok, ^aggregate_uuid} =
      Supervisor.open_aggregate(DefaultApp, ExampleAggregate, aggregate_uuid)

    {:ok, _count, _events, _aggregate_state} =
      AggregateProcess.execute(DefaultApp, ExampleAggregate, aggregate_uuid, context)
  end
end
