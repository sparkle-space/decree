defmodule Decree.AggregateDefinitionTest do
  use Decree.PostgresCase

  import ExUnit.CaptureLog

  alias Decree.Aggregate
  alias Decree.Aggregates.Aggregate, as: AggregateProcess
  alias Decree.Aggregates.{ExecutionContext, Supervisor}
  alias Decree.DefaultApp
  alias Decree.EventStore
  alias Decree.EventStore.SnapshotData
  alias Decree.UUID

  defmodule Counted do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:n]
  end

  defmodule Count do
    @moduledoc false
    defstruct [:command_id, :times]
  end

  defmodule Declared do
    @moduledoc false
    use Decree.Aggregate, snapshot: [every: 3, version: 2]

    alias Decree.AggregateDefinitionTest.{Count, Counted}

    @derive Jason.Encoder
    defstruct count: 0

    def execute(%__MODULE__{count: count}, %Count{times: times}),
      do: Enum.map(1..times, &%Counted{n: count + &1})

    def apply(%__MODULE__{} = state, %Counted{n: n}), do: %__MODULE__{state | count: n}
  end

  defmodule DefaultVersion do
    @moduledoc false
    use Decree.Aggregate, snapshot: [every: 5]

    defstruct count: 0
  end

  defmodule Undeclared do
    @moduledoc false
    use Decree.Aggregate

    defstruct count: 0
  end

  defmodule Plain do
    @moduledoc false
    defstruct count: 0
  end

  describe "`use Decree.Aggregate, snapshot: ...`" do
    test "answers the declared options" do
      assert Declared.__decree_aggregate__(:snapshot) == [snapshot_every: 3, snapshot_version: 2]
    end

    test "defaults the version to 1, as config does" do
      assert DefaultVersion.__decree_aggregate__(:snapshot) ==
               [snapshot_every: 5, snapshot_version: 1]
    end

    test "with no `snapshot:` declares nothing" do
      assert Undeclared.__decree_aggregate__(:snapshot) == nil
    end

    for {{name, options, option}, index} <-
          Enum.with_index([
            {"`every: 0`", "snapshot: [every: 0]", "every"},
            {"`every: -1`", "snapshot: [every: -1]", "every"},
            {~s(`every: "3"`), ~s(snapshot: [every: "3"]), "every"},
            {"no `every`", "snapshot: [version: 2]", "every"},
            {"`version: -1`", "snapshot: [every: 1, version: -1]", "version"},
            {"an unknown `snapshot:` key", "snapshot: [every: 1, often: 2]", "often"},
            {"an unknown option", "snapshot: [every: 1], snapshots: true", "snapshots"}
          ]) do
      test "fails to compile with #{name}" do
        module = "Decree.AggregateDefinitionTest.Invalid#{unquote(index)}"

        error =
          assert_raise ArgumentError, fn ->
            Code.compile_string("""
            defmodule #{module} do
              use Decree.Aggregate, #{unquote(options)}

              defstruct count: 0
            end
            """)
          end

        assert error.message =~ module
        assert error.message =~ unquote(option)
      end
    end
  end

  describe "`Decree.Aggregate.snapshot_options/2`" do
    test "answers the definition's options" do
      start_supervised!(DefaultApp)

      assert Aggregate.snapshot_options(DefaultApp, Declared) ==
               [snapshot_every: 3, snapshot_version: 2]
    end

    test "falls back to the `snapshotting:` config, normalised" do
      start_supervised!(
        {DefaultApp,
         snapshotting: %{
           Plain => [snapshot_every: 4],
           Undeclared => [snapshot_every: 6, snapshot_version: 3]
         }}
      )

      assert Aggregate.snapshot_options(DefaultApp, Plain) ==
               [snapshot_every: 4, snapshot_version: 1]

      assert Aggregate.snapshot_options(DefaultApp, Undeclared) ==
               [snapshot_every: 6, snapshot_version: 3]
    end

    test "prefers the definition, and warns at start for a module in both" do
      log =
        capture_log(fn ->
          start_supervised!(
            {DefaultApp,
             snapshotting: %{
               Declared => [snapshot_every: 9, snapshot_version: 7],
               Plain => [snapshot_every: 4]
             }}
          )
        end)

      assert log =~ inspect(Declared)
      refute log =~ inspect(Plain)

      assert Aggregate.snapshot_options(DefaultApp, Declared) ==
               [snapshot_every: 3, snapshot_version: 2]
    end

    test "answers [] when neither enables snapshots" do
      start_supervised!(
        {DefaultApp, snapshotting: %{Undeclared => [snapshot_every: nil], Count => []}}
      )

      assert Aggregate.snapshot_options(DefaultApp, Plain) == []
      assert Aggregate.snapshot_options(DefaultApp, Undeclared) == []
      assert Aggregate.snapshot_options(DefaultApp, Count) == []
    end
  end

  describe "an aggregate with a definition and no config" do
    setup do
      start_supervised!(DefaultApp)

      :ok
    end

    test "records a row at its version after `every` events, and a cold read uses it" do
      aggregate_uuid = UUID.uuid4()

      count(aggregate_uuid, 2)

      assert {:error, :snapshot_not_found} =
               EventStore.read_snapshot(DefaultApp, aggregate_uuid, 2)

      count(aggregate_uuid, 1)

      # Behind the snapshot in the aggregate's mailbox.
      assert %Declared{count: 3} =
               AggregateProcess.aggregate_state(DefaultApp, Declared, aggregate_uuid)

      assert {:ok, %SnapshotData{} = snapshot} =
               EventStore.read_snapshot(DefaultApp, aggregate_uuid, 2)

      assert %SnapshotData{
               source_uuid: ^aggregate_uuid,
               source_version: 3,
               snapshot_version: 2,
               data: %Declared{count: 3},
               metadata: %{"snapshot_module_version" => 2}
             } = snapshot

      assert {:error, :snapshot_not_found} =
               EventStore.read_snapshot(DefaultApp, aggregate_uuid, 1)

      # A row the events can't give, so the cold read below can only have come from it.
      :ok =
        EventStore.record_snapshot(DefaultApp, %SnapshotData{
          snapshot
          | data: %Declared{count: 42},
            created_at: nil
        })

      :ok = AggregateProcess.shutdown(DefaultApp, Declared, aggregate_uuid)

      assert AggregateProcess.aggregate_state(DefaultApp, Declared, aggregate_uuid) ==
               %Declared{count: 42}
    end
  end

  defp count(aggregate_uuid, times) do
    context = %ExecutionContext{
      command: %Count{command_id: UUID.uuid4(), times: times},
      handler: Declared,
      function: :execute
    }

    {:ok, ^aggregate_uuid} = Supervisor.open_aggregate(DefaultApp, Declared, aggregate_uuid)

    {:ok, _version, _events, _state} =
      AggregateProcess.execute(DefaultApp, Declared, aggregate_uuid, context)
  end
end
