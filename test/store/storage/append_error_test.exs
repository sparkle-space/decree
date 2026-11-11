defmodule Decree.Store.Storage.AppendErrorTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{AppendError, EventData, EventFactory, UUID}
  alias Decree.Store.Storage.{Appender, CreateStream}
  alias TestEventStore, as: EventStore

  describe "an appender failure reaches the caller as `%AppendError{}`" do
    test "a rejected append (a NOT NULL violation)" do
      stream_uuid = UUID.uuid4()

      assert {:error, %AppendError{class: :rejected, code: :not_null_violation} = error} =
               EventStore.append_to_stream(stream_uuid, :any_version, [untyped_event()])

      assert %Postgrex.Error{postgres: %{pg_code: "23502"}} = error.reason
      assert AppendError.message(error) =~ "rejected"

      assert {:error, :stream_not_found} = EventStore.read_stream_forward(stream_uuid)
    end

    test "a rejected append of 1,000 or more events (the transaction path)" do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_events(999) ++ [untyped_event()]

      assert {:error, %AppendError{class: :rejected, code: :not_null_violation}} =
               EventStore.append_to_stream(stream_uuid, :any_version, events)

      assert {:error, :stream_not_found} = EventStore.read_stream_forward(stream_uuid)
    end
  end

  describe "the four domain results stay atoms" do
    test "`:duplicate_event` through `append_to_stream/4`" do
      [%EventData{} = event] = EventFactory.create_events(1)
      event = %EventData{event | event_id: UUID.uuid4()}

      assert :ok = EventStore.append_to_stream(UUID.uuid4(), :any_version, [event])

      assert {:error, :duplicate_event} =
               EventStore.append_to_stream(UUID.uuid4(), :any_version, [event])
    end

    test "`:duplicate_stream_uuid` from the appender", %{conn: conn, schema: schema} do
      stream_uuid = UUID.uuid4()
      {:ok, _stream_id} = CreateStream.execute(conn, stream_uuid, schema: schema)

      # No stream id: the appender's statement creates the stream, which exists.
      recorded_events = EventFactory.create_recorded_events(1, stream_uuid)

      assert {:error, :duplicate_stream_uuid} =
               Appender.append(conn, nil, recorded_events, schema: schema)
    end

    test "`:wrong_expected_version` from the appender", %{conn: conn, schema: schema} do
      stream_uuid = UUID.uuid4()
      {:ok, stream_id} = CreateStream.execute(conn, stream_uuid, schema: schema)

      :ok =
        Appender.append(conn, stream_id, EventFactory.create_recorded_events(1, stream_uuid),
          schema: schema
        )

      # Another writer's event at the same stream version.
      assert {:error, :wrong_expected_version} =
               Appender.append(
                 conn,
                 stream_id,
                 EventFactory.create_recorded_events(1, stream_uuid),
                 schema: schema
               )
    end
  end

  describe "classification" do
    test "an unreachable or stopped database is `:unavailable`" do
      for {code, pg_code} <- [
            connection_failure: "08006",
            transaction_resolution_unknown: "08007",
            protocol_violation: "08P01",
            too_many_connections: "53300",
            disk_full: "53100",
            admin_shutdown: "57P01",
            cannot_connect_now: "57P03",
            query_canceled: "57014",
            idle_session_timeout: "57P05",
            system_error: "58000",
            io_error: "58030",
            # Class 40, but the outcome is unknown, as after a lost connection.
            statement_completion_unknown: "40003",
            # Class 25, but the server ended the session.
            idle_in_transaction_session_timeout: "25P03",
            # Class 25: a write reached a primary demoted by a failover.
            read_only_sql_transaction: "25006"
          ] do
        assert %AppendError{class: :unavailable, code: ^code} =
                 AppendError.from_error(postgrex_error(code, pg_code))
      end

      connection_error = DBConnection.ConnectionError.exception("tcp recv: closed")

      assert %AppendError{class: :unavailable, code: nil, reason: ^connection_error} =
               AppendError.from_error(connection_error)

      # Postgrex's `sync_error/2` disconnect: the connection broke mid-query.
      disconnect = %Postgrex.Error{message: "unexpected postgres status: transaction"}

      assert %AppendError{class: :unavailable, code: nil, reason: ^disconnect} =
               AppendError.from_error(disconnect)
    end

    test "a transaction rollback or an object not in its prerequisite state is `:retryable`" do
      for {code, pg_code} <- [
            serialization_failure: "40001",
            deadlock_detected: "40P01",
            lock_not_available: "55P03",
            object_in_use: "55006"
          ] do
        assert %AppendError{class: :retryable, code: ^code} =
                 AppendError.from_error(postgrex_error(code, pg_code))
      end
    end

    test "any other error is `:rejected`" do
      for {code, pg_code} <- [
            not_null_violation: "23502",
            check_violation: "23514",
            invalid_text_representation: "22P02",
            undefined_table: "42P01",
            in_failed_sql_transaction: "25P02",
            raise_exception: "P0001",
            # Class 40, but a deferred constraint fails again on a retry.
            transaction_integrity_constraint_violation: "40002",
            undefined_file: "58P01",
            duplicate_file: "58P02"
          ] do
        assert %AppendError{class: :rejected, code: ^code} =
                 AppendError.from_error(postgrex_error(code, pg_code))
      end

      for reason <- [
            Postgrex.QueryError.exception("too many parameters"),
            DBConnection.OwnershipError.exception("cannot find owner"),
            :some_other_reason
          ] do
        assert %AppendError{class: :rejected, code: nil, reason: ^reason} =
                 AppendError.from_error(reason)
      end
    end

    test "the message names the class and the original error" do
      error = AppendError.from_error(postgrex_error(:serialization_failure, "40001"))

      assert AppendError.message(error) =~ "retryable"
      assert AppendError.message(error) =~ "serialization_failure"
    end
  end

  defp untyped_event do
    %EventData{event_type: nil, data: %{"untyped" => true}, metadata: %{}}
  end

  defp postgrex_error(code, pg_code) do
    %Postgrex.Error{
      postgres: %{code: code, pg_code: pg_code, severity: "ERROR", message: "test"}
    }
  end
end
