defmodule Decree.Store.Storage.StorageLogTermsTest do
  use Decree.Store.StorageCase

  # The store's log lines name a failure by its class, never by a value the error carries,
  # and a stream id by its last eight characters at most (sparkle-space/sparklespace#8352).
  #
  # Every error here is a real `Postgrex.Error` from Postgres: a statement-level trigger, or a
  # table swapped for a view over a function, raises with the marker in its message and its
  # detail, as a unique violation's detail carries a key's value. The DDL runs in the test's
  # own transaction, which is rolled back, so nothing outlives the test.

  import ExUnit.CaptureLog
  import Decree.Store.SubscriptionHelpers, only: [wait_for_lock_pool: 1]

  alias Decree.Store.{
    AppendError,
    DenyListSerializer,
    EventData,
    EventFactory,
    NewReleaseEvent,
    RecordedEvent,
    Storage,
    UUID
  }

  alias Decree.Store.Notifications
  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Storage.{Appender, DeleteStream, Snapshot}
  alias Decree.Store.Streams.StreamInfo

  @marker "log-hygiene-marker-1f2e3d"
  @raise ~s[RAISE EXCEPTION 'refused: %', '#{@marker}' USING DETAIL = 'Key (id)=(#{@marker}) is refused.', ERRCODE = 'check_violation';]

  setup %{conn: conn, schema: schema} do
    stream_uuid = UUID.uuid4()

    :ok = TestEventStore.append_to_stream(stream_uuid, 0, EventFactory.create_events(1))

    {:ok, %StreamInfo{stream_id: stream_id}} =
      Storage.stream_info(conn, stream_uuid, schema: schema)

    [stream_uuid: stream_uuid, stream_id: stream_id]
  end

  describe "a storage error is logged by its class" do
    test "an append", %{conn: conn, schema: schema} do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_recorded_events(1, stream_uuid)

      log =
        refused(conn, schema, {:trigger, "events"}, fn tx ->
          assert {:error, %AppendError{code: :check_violation}} =
                   Appender.append(tx, nil, events, schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to append events to stream")
    end

    test "an append to several streams", %{conn: conn, schema: schema} do
      stream_uuid = UUID.uuid4()
      events = EventFactory.create_recorded_events(1, stream_uuid)
      member = %{stream_id: nil, expected_version: :any_version, events: events, tags: []}

      log =
        refused(conn, schema, {:trigger, "events"}, fn tx ->
          assert {:error, %AppendError{code: :check_violation}} =
                   Appender.append_streams(tx, [member], schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to append events to streams")
    end

    test "a snapshot read", %{conn: conn, schema: schema, stream_uuid: stream_uuid} do
      log =
        refused(conn, schema, {:view, "versioned_snapshots"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   Snapshot.read_snapshot(tx, stream_uuid, schema: schema, snapshot_version: 1)
        end)

      assert_reduced(log, stream_uuid, "Failed to read snapshot for source")
    end

    test "a snapshot write", %{conn: conn, schema: schema, stream_uuid: stream_uuid} do
      snapshot = %SnapshotData{
        source_uuid: stream_uuid,
        source_version: 1,
        source_type: "Elixir.Decree.Store.LogHygieneSnapshot",
        data: "{}",
        metadata: "{}",
        snapshot_version: 1
      }

      log =
        refused(conn, schema, {:trigger, "versioned_snapshots"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   Snapshot.record_snapshot(tx, snapshot, schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to record snapshot for source")
    end

    test "a snapshot delete", %{conn: conn, schema: schema, stream_uuid: stream_uuid} do
      log =
        refused(conn, schema, {:trigger, "versioned_snapshots"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   Snapshot.delete_snapshot(tx, stream_uuid, schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to delete snapshot for source")
    end

    test "a snapshot prune", %{conn: conn, schema: schema, stream_uuid: stream_uuid} do
      log =
        refused(conn, schema, {:trigger, "versioned_snapshots"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   Snapshot.prune_snapshots(tx, "Elixir.Decree.Store.LogHygieneSnapshot", 1,
                     schema: schema
                   )
        end)

      assert_reduced(log, stream_uuid, "Failed to prune snapshots of type")
    end

    test "a soft delete", %{conn: conn, schema: schema} = context do
      %{stream_uuid: stream_uuid, stream_id: stream_id} = context

      log =
        refused(conn, schema, {:trigger, "streams"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   DeleteStream.soft_delete(tx, stream_id, schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to soft delete stream")
    end

    test "a hard delete", %{conn: conn, schema: schema} = context do
      %{stream_uuid: stream_uuid, stream_id: stream_id} = context

      log =
        refused(conn, schema, {:trigger, "streams"}, fn tx ->
          Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

          assert {:error, %Postgrex.Error{}} =
                   DeleteStream.hard_delete(tx, stream_id, schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to hard delete stream")
    end

    test "a delete of a stream's tags", %{conn: conn, schema: schema} = context do
      %{stream_uuid: stream_uuid, stream_id: stream_id} = context

      log =
        refused(conn, schema, {:trigger, "event_tags"}, fn tx ->
          Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

          assert {:error, %Postgrex.Error{}} =
                   DeleteStream.delete_tags(tx, stream_id, ["subject"], schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to delete tags of stream")
    end

    test "a subscription's create", %{conn: conn, schema: schema, stream_uuid: stream_uuid} do
      log =
        refused(conn, schema, {:trigger, "subscriptions"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   Storage.subscribe_to_stream(tx, stream_uuid, "log-hygiene", nil,
                     schema: schema
                   )
        end)

      assert_reduced(log, stream_uuid, "Failed to create stream create subscription on stream")
    end

    test "a subscription's ack", %{conn: conn, schema: schema, stream_uuid: stream_uuid} do
      log =
        refused(conn, schema, {:trigger, "subscriptions"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   Storage.ack_last_seen_event(tx, stream_uuid, "log-hygiene", 1, schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to ack last seen event on stream")
    end

    test "a subscription's ack by id", %{conn: conn, schema: schema, stream_uuid: stream_uuid} do
      log =
        refused(conn, schema, {:trigger, "subscriptions"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   Storage.ack_last_seen_event_by_id(tx, 1, 1, schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to ack last seen event for subscription id")
    end

    test "a subscription's delete", %{conn: conn, schema: schema, stream_uuid: stream_uuid} do
      log =
        refused(conn, schema, {:trigger, "subscriptions"}, fn tx ->
          assert {:error, %Postgrex.Error{}} =
                   Storage.delete_subscription(tx, stream_uuid, "log-hygiene", schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to delete subscription to stream")
    end

    test "a stream read", %{conn: conn, schema: schema} = context do
      %{stream_uuid: stream_uuid, stream_id: stream_id} = context

      log =
        refused(conn, schema, {:view, "stream_events"}, fn tx ->
          assert {:error, _reason} =
                   Storage.read_stream_forward(tx, stream_id, 1, 10, schema: schema)
        end)

      assert_reduced(log, stream_uuid, "Failed to read events from stream due to")
      assert log =~ "Failed to read events from stream id #{stream_id} due to"
    end
  end

  describe "the notifications publisher" do
    @marker_schema "decree_log_marker"
    @marker_store DecreeLogMarkerStore

    setup %{conn: conn} do
      drop = ~s[DROP SCHEMA IF EXISTS "#{@marker_schema}" CASCADE]
      Postgrex.query!(conn, drop, [])

      # The schema's three tables the publisher's read joins are views over a function that
      # raises, so its read of any notification fails at Postgres.
      Postgrex.transaction(conn, fn tx ->
        Postgrex.query!(tx, ~s[CREATE SCHEMA "#{@marker_schema}"], [])

        for table <- ["streams", "stream_events", "events"] do
          create_raising_view(tx, @marker_schema, table, ~s["public".#{table}])
        end
      end)

      on_exit(fn ->
        {:ok, observer} = Postgrex.start_link(TestEventStore.config())
        Postgrex.query!(observer, drop, [])
        GenServer.stop(observer)
      end)

      config =
        TestEventStore.config()
        |> Keyword.put(:schema, @marker_schema)
        |> Keyword.put(:conn, start_supervised!({Postgrex, TestEventStore.config()}))

      start_supervised!({Notifications.Supervisor, {@marker_store, config}})

      :ok = forward_logs()
    end

    test "a failed read", %{conn: conn, stream_uuid: stream_uuid} do
      log =
        capture_log([level: :debug], fn ->
          Postgrex.query!(conn, "SELECT pg_notify($1, $2)", [
            @marker_schema <> ".events",
            stream_uuid <> ",1,1,1"
          ])

          assert_receive {:logged, "Decree.Store notifications failed to read events" <> _rest}
        end)

      assert_reduced(log, stream_uuid, "Decree.Store notifications failed to read events")
    end
  end

  describe "a held subscription's probe" do
    @old :log_marker_old_store
    @new_type "Elixir.Decree.Store.NewReleaseEvent"

    # An old instance holds a subscription before an event of a type it does not know; the
    # `subscriptions` table is then swapped for a view over a function that raises, committed,
    # so the held subscription's probe, on its own pool, fails at Postgres.
    setup do
      DenyListSerializer.deny([@new_type])
      on_exit(fn -> DenyListSerializer.allow_all() end)

      on_exit(fn ->
        {:ok, observer} = Postgrex.start_link(TestEventStore.config())

        for statement <- [
              ~s[DROP VIEW IF EXISTS "public".subscriptions],
              ~s[DROP FUNCTION IF EXISTS "public".log_hygiene_subscriptions()],
              ~s[ALTER TABLE IF EXISTS "public".subscriptions_hidden RENAME TO subscriptions]
            ] do
          Postgrex.query!(observer, statement, [])
        end

        GenServer.stop(observer)
      end)

      start_supervised!({TestEventStore, name: @old, serializer: DenyListSerializer},
        id: @old
      )

      wait_for_lock_pool(@old)

      :ok = forward_logs()
    end

    test "logs a failed probe by its class", %{conn: conn, schema: schema} do
      name = UUID.uuid4()

      {line, log} =
        with_log([level: :debug], fn ->
          {:ok, subscription} =
            TestEventStore.subscribe_to_all_streams(name, self(),
              name: @old,
              start_from: :origin,
              retry_interval: 200
            )

          assert_receive {:subscribed, ^subscription}

          :ok =
            TestEventStore.append_to_stream(UUID.uuid4(), 0, [
              EventFactory.create_event(nil, 2),
              %EventData{event_type: @new_type, data: %NewReleaseEvent{event: 3}, metadata: %{}}
            ])

          ack_through(subscription, 2)

          # Held, with its checkpoint written: only now is the table swapped.
          await_logged("is held before event 3")

          refuse(conn, schema, {:view, "subscriptions"})

          await_logged("the probe failed due to")
        end)

      assert line =~ "the probe failed due to: {Postgrex.Error, :check_violation}"
      refute line =~ @marker
      refute log =~ @marker
    end
  end

  describe "a stream's id on the store's debug lines" do
    test "is its suffix", %{stream_uuid: stream_uuid} do
      name = "log-hygiene"

      log =
        capture_log([level: :debug], fn ->
          {:ok, subscription} =
            TestEventStore.subscribe_to_stream(stream_uuid, name, self(), start_from: :origin)

          assert_receive {:subscribed, ^subscription}
          assert_receive {:events, [first]}
          :ok = TestEventStore.ack(subscription, first)

          :ok = TestEventStore.append_to_stream(stream_uuid, 1, EventFactory.create_events(1))
          assert_receive {:events, [second]}
          :ok = TestEventStore.ack(subscription, second)

          :ok = TestEventStore.unsubscribe_from_stream(stream_uuid, name)
          :ok = TestEventStore.delete_subscription(stream_uuid, name)
        end)

      for line <- [
            "Appended 1 event(s) to stream",
            "Listener received notification",
            "Attempting to create subscription on stream",
            "Created subscription on stream",
            "Attempting to delete subscription on stream",
            "Deleted subscription to stream",
            "attempting to connect subscriber"
          ] do
        assert log =~ line
      end

      assert log =~ suffix(stream_uuid)
      refute log =~ stream_uuid
    end
  end

  defp assert_reduced(log, stream_uuid, line) do
    assert log =~ line
    refute log =~ @marker
    refute log =~ stream_uuid
    assert log =~ "Postgrex.Error" or log =~ "check_violation"
  end

  defp suffix(stream_uuid), do: "..." <> String.slice(stream_uuid, -8, 8)

  # Runs `fun` with a transaction in which `target` raises, and rolls it back.
  defp refused(conn, schema, target, fun) do
    capture_log([level: :debug], fn ->
      {:error, :rolled_back} =
        Postgrex.transaction(conn, fn tx ->
          refuse(tx, schema, target)
          fun.(tx)
          Postgrex.rollback(tx, :rolled_back)
        end)
    end)
  end

  defp refuse(tx, schema, {:trigger, table}) do
    Postgrex.query!(
      tx,
      ~s[CREATE FUNCTION "#{schema}".log_hygiene_refuse() RETURNS trigger LANGUAGE plpgsql ] <>
        ~s[AS $$ BEGIN #{@raise} END $$],
      []
    )

    Postgrex.query!(
      tx,
      ~s[CREATE TRIGGER log_hygiene_refuse BEFORE INSERT OR UPDATE OR DELETE ON "#{schema}".#{table} ] <>
        ~s[FOR EACH STATEMENT EXECUTE FUNCTION "#{schema}".log_hygiene_refuse()],
      []
    )
  end

  defp refuse(tx, schema, {:view, table}) do
    Postgrex.query!(tx, ~s[ALTER TABLE "#{schema}".#{table} RENAME TO #{table}_hidden], [])

    create_raising_view(tx, schema, table, ~s["#{schema}".#{table}_hidden])
  end

  defp create_raising_view(tx, schema, table, row_type) do
    Postgrex.query!(
      tx,
      ~s[CREATE FUNCTION "#{schema}".log_hygiene_#{table}() RETURNS SETOF #{row_type} ] <>
        ~s[LANGUAGE plpgsql AS $$ BEGIN #{@raise} END $$],
      []
    )

    Postgrex.query!(
      tx,
      ~s[CREATE VIEW "#{schema}".#{table} AS SELECT * FROM "#{schema}".log_hygiene_#{table}()],
      []
    )
  end

  # Acks every event the subscription sends until it has acked event `last`.
  defp ack_through(subscription, last) do
    assert_receive {:events, events}, 5_000

    for %RecordedEvent{event_number: number} <- events do
      :ok = TestEventStore.ack(subscription, number)
    end

    if Enum.any?(events, &(&1.event_number >= last)),
      do: :ok,
      else: ack_through(subscription, last)
  end

  # Waits for a forwarded log line containing `text`, and answers it.
  defp await_logged(text) do
    receive do
      {:logged, line} -> if line =~ text, do: line, else: await_logged(text)
    after
      5_000 -> flunk("no log line containing #{inspect(text)}")
    end
  end

  # Forwards each log line's text to the test, so it can wait for one.
  defp forward_logs do
    id = :"decree_log_hygiene_#{System.unique_integer([:positive])}"
    :ok = :logger.add_handler(id, __MODULE__.Forward, %{config: %{pid: self()}, level: :all})
    on_exit(fn -> :logger.remove_handler(id) end)
  end

  defmodule Forward do
    @moduledoc false

    def log(%{msg: {:string, text}}, %{config: %{pid: pid}}),
      do: send(pid, {:logged, IO.chardata_to_string(text)})

    def log(_event, _config), do: :ok
  end
end
