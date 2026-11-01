defmodule Decree.Store.Storage.AppendDeadlockTest do
  use Decree.Store.StorageCase

  import ExUnit.CaptureLog

  alias Decree.Store.{AppendError, AppendResult, EventFactory, LockSession}
  alias TestEventStore, as: EventStore

  # A real `deadlock_detected`. Session X takes `$all`, an append on the stream takes
  # the stream's row and waits on `$all`, then X waits on the stream's row. X runs
  # with a long `deadlock_timeout`, so the append's check finds the cycle and the
  # append is the victim: its own check fires one second (the default) after it
  # started waiting, by when X has queued.
  @all_stream 0

  setup context do
    %{postgrex_config: postgrex_config, schema: schema} = context

    observer = LockSession.connect(postgrex_config)
    appender = LockSession.connect(postgrex_config)

    :ok =
      EventStore.append_to_stream("stream", :any_version, EventFactory.create_events(1),
        conn: observer
      )

    %Postgrex.Result{rows: [[stream_id]]} =
      Postgrex.query!(
        observer,
        ~s(SELECT stream_id FROM "#{schema}".streams WHERE stream_uuid = 'stream'),
        []
      )

    session_x = LockSession.start(postgrex_config)

    LockSession.run(session_x, fn tx ->
      Postgrex.query!(tx, "SET LOCAL deadlock_timeout = '5s'", [])
    end)

    LockSession.lock_row(session_x, schema, @all_stream)

    [
      observer: observer,
      appender: appender,
      appender_backend: LockSession.backend_pid(appender),
      session_x: session_x,
      stream_id: stream_id
    ]
  end

  test "the victim append is retried and its events are written once", context do
    %{session_x: session_x, appender: appender} = context

    log =
      capture_log(fn ->
        append = async_append(appender, 3)

        force_deadlock(context, context.appender_backend)

        assert {:ok, :ok} = LockSession.commit(session_x)
        assert :ok = Task.await(append, 15_000)
      end)

    assert log =~ "deadlock_detected"
    assert stream_versions(context) == [1, 2, 3, 4]
    assert all_version(context) == 4
  end

  test "the retry keeps `returning:`, and answers what it wrote", context do
    %{session_x: session_x, appender: appender} = context

    log =
      capture_log(fn ->
        append = async_append_result(appender, 3)

        force_deadlock(context, context.appender_backend)

        assert {:ok, :ok} = LockSession.commit(session_x)

        assert Task.await(append, 15_000) ==
                 {:ok, %AppendResult{position: 4, stream_versions: %{"stream" => 4}}}
      end)

    assert log =~ "deadlock_detected"
  end

  test "the path for 1,000 or more events keeps `returning:` through its retry", context do
    %{session_x: session_x, appender: appender} = context

    log =
      capture_log(fn ->
        append = async_append_result(appender, 1_000)

        force_deadlock(context, context.appender_backend)

        assert {:ok, :ok} = LockSession.commit(session_x)

        assert Task.await(append, 15_000) ==
                 {:ok, %AppendResult{position: 1_001, stream_versions: %{"stream" => 1_001}}}
      end)

    assert log =~ "deadlock_detected"
  end

  test "a multi-stream victim keeps `returning:` through its retry", context do
    %{session_x: session_x, appender: appender} = context

    log =
      capture_log(fn ->
        append =
          Task.async(fn ->
            EventStore.append_to_streams(multi_stream_appends(),
              conn: appender,
              returning: :append_result
            )
          end)

        force_deadlock(context, context.appender_backend)

        assert {:ok, :ok} = LockSession.commit(session_x)

        assert Task.await(append, 15_000) ==
                 {:ok,
                  %AppendResult{position: 3, stream_versions: %{"stream" => 2, "other" => 1}}}
      end)

    assert log =~ "deadlock_detected"
  end

  test "the path for 1,000 or more events retries its whole transaction", context do
    %{session_x: session_x, appender: appender} = context

    log =
      capture_log(fn ->
        append = async_append(appender, 1_000)

        force_deadlock(context, context.appender_backend)

        assert {:ok, :ok} = LockSession.commit(session_x)
        assert :ok = Task.await(append, 15_000)
      end)

    assert log =~ "deadlock_detected"
    assert stream_versions(context) == Enum.to_list(1..1_001)
    assert all_version(context) == 1_001
  end

  test "a second deadlock is answered as the typed error", context do
    %{session_x: session_x, appender: appender} = context

    append = async_append(appender, 3)

    force_deadlock(context, context.appender_backend)

    # X lets go of the stream's row, keeping `$all`, so the retry takes the row and
    # waits on `$all` again, and X closes the same cycle.
    LockSession.run(session_x, fn tx -> Postgrex.query!(tx, "ROLLBACK TO SAVEPOINT x", []) end)

    force_deadlock(context, context.appender_backend)

    assert {:error, %AppendError{class: :retryable, code: :deadlock_detected}} =
             Task.await(append, 15_000)

    assert {:ok, :ok} = LockSession.commit(session_x)

    assert stream_versions(context) == [1]
    assert all_version(context) == 1
  end

  test "inside a caller's transaction the deadlock is answered, not retried", context do
    %{session_x: session_x, postgrex_config: postgrex_config} = context

    caller = LockSession.start(postgrex_config)
    events = EventFactory.create_events(3)

    append =
      LockSession.start_run(caller, fn tx ->
        EventStore.append_to_stream("stream", :any_version, events, conn: tx)
      end)

    force_deadlock(context, caller.backend)

    # A retry would have answered `in_failed_sql_transaction` from the aborted
    # transaction, not this.
    assert {:error, %AppendError{class: :retryable, code: :deadlock_detected}} =
             LockSession.await_run(append)

    assert {:error, :rollback} = LockSession.commit(caller)
    assert {:ok, :ok} = LockSession.commit(session_x)

    assert stream_versions(context) == [1]
    assert all_version(context) == 1
  end

  # The retry keeps the condition and its `after`. X commits a match while the retry
  # waits, so an append that lost its condition on the way back would be written.
  test "a retry evaluates the condition again, with the same `after`", context do
    %{session_x: session_x, appender: appender} = context

    start_supervised!(
      {TestEventStore, name: :declared_store, conditional_appends: [stream_prefixes: ["stream"]]}
    )

    events = EventFactory.create_events(1)

    log =
      capture_log(fn ->
        append =
          Task.async(fn ->
            EventStore.append_to_stream("stream", :any_version, events,
              name: :declared_store,
              conn: appender,
              condition: [items: [[tags: ["contested"]]], after: 1]
            )
          end)

        force_deadlock(context, context.appender_backend)

        match = %{EventFactory.create_event(nil) | tags: ["contested"]}

        LockSession.run(session_x, fn tx ->
          assert :ok = EventStore.append_to_stream("other", 0, [match], conn: tx)
        end)

        assert {:ok, :ok} = LockSession.commit(session_x)
        assert {:error, :append_condition_failed} = Task.await(append, 15_000)
      end)

    assert log =~ "deadlock_detected"
    assert stream_versions(context) == [1]
    assert all_version(context) == 2
  end

  test "a multi-stream victim is retried and its events are written once", context do
    %{session_x: session_x, appender: appender} = context

    appends = multi_stream_appends()

    log =
      capture_log(fn ->
        append = Task.async(fn -> EventStore.append_to_streams(appends, conn: appender) end)

        force_deadlock(context, context.appender_backend)

        assert {:ok, :ok} = LockSession.commit(session_x)
        assert :ok = Task.await(append, 15_000)
      end)

    assert log =~ "deadlock_detected"
    assert stream_versions(context) == [1, 2]
    assert all_version(context) == 3
  end

  test "a multi-stream victim in a caller's transaction is answered, not retried", context do
    %{session_x: session_x, postgrex_config: postgrex_config} = context

    caller = LockSession.start(postgrex_config)
    appends = multi_stream_appends()

    append =
      LockSession.start_run(caller, fn tx ->
        EventStore.append_to_streams(appends, conn: tx)
      end)

    force_deadlock(context, caller.backend)

    assert {:error, %AppendError{class: :retryable, code: :deadlock_detected}} =
             LockSession.await_run(append)

    assert {:error, :rollback} = LockSession.commit(caller)
    assert {:ok, :ok} = LockSession.commit(session_x)

    assert stream_versions(context) == [1]
    assert all_version(context) == 1
  end

  test "an append through a `DBConnection.run/3` checkout is retried", context do
    assert_run_checkout_retries(context, 3)

    assert stream_versions(context) == [1, 2, 3, 4]
    assert all_version(context) == 4
  end

  test "1,000 or more events through a `DBConnection.run/3` checkout are retried", context do
    assert_run_checkout_retries(context, 1_000)

    assert stream_versions(context) == Enum.to_list(1..1_001)
    assert all_version(context) == 1_001
  end

  defp assert_run_checkout_retries(context, count) do
    %{session_x: session_x, appender: appender} = context

    events = EventFactory.create_events(count)

    log =
      capture_log(fn ->
        append =
          Task.async(fn ->
            DBConnection.run(
              appender,
              fn checkout ->
                assert %DBConnection{conn_mode: nil} = checkout

                EventStore.append_to_stream("stream", :any_version, events, conn: checkout)
              end,
              timeout: 15_000
            )
          end)

        force_deadlock(context, context.appender_backend)

        assert {:ok, :ok} = LockSession.commit(session_x)
        assert :ok = Task.await(append, 15_000)
      end)

    assert log =~ "deadlock_detected"
  end

  # Returns once the victim has let go of the stream's row, which only its abort does.
  defp force_deadlock(context, victim_backend) do
    %{observer: observer, session_x: session_x, schema: schema, stream_id: stream_id} = context

    LockSession.wait_until_blocked(observer, victim_backend, session_x.backend)

    LockSession.run(session_x, fn tx -> Postgrex.query!(tx, "SAVEPOINT x", []) end)
    LockSession.lock_row(session_x, schema, stream_id)
  end

  # `other` sorts first and is new, so the victim holds it too when the cycle closes.
  defp multi_stream_appends do
    [
      {"stream", :any_version, EventFactory.create_events(1)},
      {"other", :any_version, EventFactory.create_events(1)}
    ]
  end

  defp async_append(conn, count) do
    events = EventFactory.create_events(count)

    Task.async(fn ->
      EventStore.append_to_stream("stream", :any_version, events, conn: conn)
    end)
  end

  defp async_append_result(conn, count) do
    events = EventFactory.create_events(count)

    Task.async(fn ->
      EventStore.append_to_stream("stream", :any_version, events,
        conn: conn,
        returning: :append_result
      )
    end)
  end

  defp stream_versions(context) do
    %{observer: observer, schema: schema, stream_id: stream_id} = context

    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        observer,
        ~s(SELECT stream_version FROM "#{schema}".stream_events) <>
          " WHERE stream_id = $1 ORDER BY stream_version",
        [stream_id]
      )

    Enum.map(rows, &hd/1)
  end

  defp all_version(context) do
    %{observer: observer, schema: schema} = context

    %Postgrex.Result{rows: [[version]]} =
      Postgrex.query!(
        observer,
        ~s(SELECT stream_version FROM "#{schema}".streams WHERE stream_id = 0),
        []
      )

    version
  end
end
