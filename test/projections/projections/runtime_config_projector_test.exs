defmodule Decree.Projections.RuntimeConfigProjectorTest do
  use Decree.PostgresCase

  alias Decree.EventStore.Adapters.Mock, as: MockEventStore
  alias Decree.EventStore.RecordedEvent
  alias Decree.Helpers.Wait
  alias Decree.Projections.Events.AnEvent
  alias Decree.Projections.{Projection, ProjectionAssertions, Repo, RuntimeConfigProjector}
  alias Decree.UUID

  import Mox
  import ProjectionAssertions

  setup [:set_mox_global, :stub_event_store, :verify_on_exit!]

  setup do
    start_supervised!({TestApplication, event_store: [adapter: MockEventStore]})
    Ecto.Adapters.SQL.Sandbox.checkout(Repo)
  end

  describe "runtime config projector" do
    setup do
      projector1 =
        start_supervised!(
          {RuntimeConfigProjector, application: TestApplication, name: "RuntimeProjector1"}
        )

      projector2 =
        start_supervised!(
          {RuntimeConfigProjector, application: TestApplication, name: "RuntimeProjector2"}
        )

      Ecto.Adapters.SQL.Sandbox.allow(Repo, self(), projector1)
      Ecto.Adapters.SQL.Sandbox.allow(Repo, self(), projector2)

      [projector1: projector1, projector2: projector2]
    end

    test "should handle a projected event", %{projector1: projector1} do
      send_events(projector1, [
        %RecordedEvent{
          event_number: 1,
          event_id: UUID.uuid4(),
          data: %AnEvent{pid: self()},
          metadata: %{}
        }
      ])

      assert_receive {:project, "AnEvent"}

      # The projector sends `{:project, _}` from inside its Ecto.Multi, before the
      # transaction commits: wait for the commit rather than race it. The wait ends on
      # the first read that sees it, so its bound is spent only when a test is failing.
      Wait.until(5_000, fn ->
        assert_projections(Projection, ["AnEvent"])
        assert last_seen_event("RuntimeProjector1") == 1
      end)

      assert last_seen_event("RuntimeProjector2") == nil
    end
  end

  defp send_events(projector, events) do
    send(projector, {:events, events})
  end

  defp stub_event_store(_context) do
    stub(MockEventStore, :ack_event, fn _adapter_meta, _pid, _event -> :ok end)

    stub(MockEventStore, :child_spec, fn _application, _config ->
      {:ok, [], %{}}
    end)

    stub(MockEventStore, :subscribe_to, fn
      _event_store, :all, _handler_name, _handler, _subscribe_from, _opts ->
        {:ok, self()}
    end)

    :ok
  end
end
