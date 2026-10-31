defmodule Decree.Store.Storage.ProtectAgainstUpdateAndDeleteTest do
  use Decree.Store.StorageCase

  alias Decree.Store.Config

  describe "event store table protection" do
    test "cannot delete from the event tables", %{conn: conn} do
      for table <- ["events", "streams", "stream_events", "event_tags", "event_types"] do
        assert_cannot_delete(conn, table)
      end
    end

    test "cannot update `events` table", %{conn: conn} do
      assert {:error, %Postgrex.Error{postgres: %{message: "EventStore: Cannot update events"}}} =
               Postgrex.query(conn, "UPDATE events SET created_at = NOW();", [])
    end

    test "cannot update `stream_events` table", %{conn: conn} do
      assert {:error,
              %Postgrex.Error{postgres: %{message: "EventStore: Cannot update stream events"}}} =
               Postgrex.query(conn, "UPDATE stream_events SET stream_id = 1;", [])
    end

    test "cannot update `event_tags` table", %{conn: conn} do
      assert {:error,
              %Postgrex.Error{postgres: %{message: "EventStore: Cannot update event tags"}}} =
               Postgrex.query(conn, "UPDATE event_tags SET tag = 'x';", [])
    end

    test "cannot update `event_types` table", %{conn: conn} do
      assert {:error,
              %Postgrex.Error{postgres: %{message: "EventStore: Cannot update event types"}}} =
               Postgrex.query(conn, "UPDATE event_types SET event_type = 'x';", [])
    end
  end

  describe "event store enable hard deletes" do
    test "can delete from the event tables" do
      config =
        TestEventStore.config(enable_hard_deletes: true)
        |> Config.default_postgrex_opts()

      conn = start_supervised!({Postgrex, config})

      Postgrex.transaction(conn, fn transaction ->
        {:ok, %Postgrex.Result{}} =
          Postgrex.query(transaction, "SET SESSION eventstore.enable_hard_deletes TO 'on';", [])

        for table <- ["events", "streams", "stream_events", "event_tags", "event_types"] do
          assert {:ok, %Postgrex.Result{}} =
                   Postgrex.query(transaction, "DELETE FROM #{table};", [])
        end
      end)
    end
  end

  defp assert_cannot_delete(conn, table) do
    expected_message = "EventStore: Cannot delete #{String.replace(table, "_", " ")}"

    assert {:error, %Postgrex.Error{postgres: %{message: ^expected_message}}} =
             Postgrex.query(conn, "DELETE FROM #{table};", [])
  end
end
