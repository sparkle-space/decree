defmodule Decree.Store.Storage.AppendLockOrderTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{EventFactory, LockSession}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  # An append takes its stream's row, then `$all`. These tests hold each lock from a
  # second session, so what an append has and has not taken shows in Postgres.
  @all_stream 0

  setup %{postgrex_config: postgrex_config} do
    observer = LockSession.connect(postgrex_config)
    appender = LockSession.connect(postgrex_config)
    second_appender = LockSession.connect(postgrex_config)

    [
      observer: observer,
      appender: appender,
      appender_backend: LockSession.backend_pid(appender),
      second_appender: second_appender,
      second_appender_backend: LockSession.backend_pid(second_appender)
    ]
  end

  describe "a plain append" do
    test "blocked on its stream's row has not taken `$all`", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      stream_id = create_stream(context, "held-stream")
      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, stream_id)

      append = async_append(appender, "held-stream", :any_version, 2)
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      # Only an append that holds no `$all` lets this one through.
      assert :ok =
               EventStore.append_to_stream(
                 "other-stream",
                 :any_version,
                 EventFactory.create_events(1),
                 conn: context.second_appender
               )

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)

      assert all_stream_ids(context) == [
               stream_id(context, "held-stream"),
               stream_id(context, "other-stream"),
               stream_id(context, "held-stream"),
               stream_id(context, "held-stream")
             ]
    end

    test "blocked on `$all` holds its stream's row", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      stream_id = create_stream(context, "stream")
      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      append = async_append(appender, "stream", :any_version, 1)
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
               Postgrex.query(
                 observer,
                 ~s(SELECT 1 FROM "#{context.schema}".streams WHERE stream_id = $1) <>
                   " FOR NO KEY UPDATE NOWAIT",
                 [stream_id]
               )

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)
    end
  end

  describe "a hard delete" do
    test "first: the append blocks, then answers `:not_found` and writes nothing", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      stream_id = create_stream(context, "deleted-stream")
      all_version = all_version(context)
      event_count = event_count(context)

      deleter = LockSession.start(context.postgrex_config)

      LockSession.run(deleter, fn tx ->
        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

        assert :ok =
                 Decree.Store.Storage.hard_delete_stream(tx, stream_id, schema: context.schema)
      end)

      append = async_append(appender, "deleted-stream", :any_version, 2)
      LockSession.wait_until_blocked(observer, appender_backend, deleter.backend)

      assert {:ok, :ok} = LockSession.commit(deleter)

      assert {:error, :not_found} = Task.await(append)
      assert stream_id(context, "deleted-stream") == nil
      assert event_count(context) == event_count - 1
      assert all_version(context) == all_version
    end

    test "second: blocked on the stream, it fails the foreign key and the append stands",
         context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context
      %{second_appender: deleter, second_appender_backend: deleter_backend} = context

      stream_id = create_stream(context, "kept-stream")
      all_version = all_version(context)

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      append = async_append(appender, "kept-stream", :any_version, 2)
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      delete =
        Task.async(fn ->
          Stream.delete(deleter, "kept-stream", :any_version, :hard,
            schema: context.schema,
            enable_hard_deletes: true
          )
        end)

      LockSession.wait_until_blocked(observer, deleter_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)

      # The delete's statement took its snapshot before the append committed, so its
      # DELETE of `stream_events` missed the appended rows, and removing the stream's
      # row fails on them. Nothing is lost, the delete is refused, and no deadlock.
      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation}}} =
               Task.await(delete)

      assert stream_id(context, "kept-stream") == stream_id
      assert stream_versions(context, stream_id) == [1, 2, 3]
      assert all_version(context) == all_version + 2
    end
  end

  describe "a link" do
    test "first: the append blocks, then numbers after the linked events", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      source_id = create_stream(context, "source")
      target_id = create_stream(context, "target")
      linked = read_events(context, "source")

      linker = LockSession.start(context.postgrex_config)

      LockSession.run(linker, fn tx ->
        assert :ok = EventStore.link_to_stream("target", :any_version, linked, conn: tx)
      end)

      append = async_append(appender, "target", :any_version, 1)
      LockSession.wait_until_blocked(observer, appender_backend, linker.backend)

      assert {:ok, :ok} = LockSession.commit(linker)
      assert :ok = Task.await(append)

      assert stream_versions(context, source_id) == [1]
      assert stream_versions(context, target_id) == [1, 2, 3]
    end

    test "first, then an integer expected version answers `:wrong_expected_version`", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      create_stream(context, "source")
      target_id = create_stream(context, "target")
      linked = read_events(context, "source")

      linker = LockSession.start(context.postgrex_config)

      LockSession.run(linker, fn tx ->
        assert :ok = EventStore.link_to_stream("target", :any_version, linked, conn: tx)
      end)

      append = async_append(appender, "target", 1, 1)
      LockSession.wait_until_blocked(observer, appender_backend, linker.backend)

      assert {:ok, :ok} = LockSession.commit(linker)
      assert {:error, :wrong_expected_version} = Task.await(append)

      assert stream_versions(context, target_id) == [1, 2]
    end

    test "second: blocked on the stream, it numbers after the append, with no deadlock",
         context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context
      %{second_appender: linker, second_appender_backend: linker_backend} = context

      source_id = create_stream(context, "source")
      target_id = create_stream(context, "target")
      linked = read_events(context, "source")

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      append = async_append(appender, "target", :any_version, 1)
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      link =
        Task.async(fn ->
          EventStore.link_to_stream("target", :any_version, linked, conn: linker)
        end)

      LockSession.wait_until_blocked(observer, linker_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)
      assert :ok = Task.await(link)

      assert stream_versions(context, source_id) == [1]
      assert stream_versions(context, target_id) == [1, 2, 3]
    end
  end

  describe "a second writer of a stream whose first writer is blocked on `$all`" do
    setup context do
      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      [holder: holder]
    end

    test "with `:any_version` numbers from the locked row", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      stream_id = create_stream_after_lock(context)

      first = async_append(appender, "stream", :any_version, 2)
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      second = async_append(second_appender, "stream", :any_version, 3)
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(first)
      assert :ok = Task.await(second)

      assert stream_versions(context, stream_id) == [1, 2, 3, 4, 5]
    end

    test "creating the same stream is retried once and appends after it", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      first = async_append(appender, "new-stream", :any_version, 2)
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      second = async_append(second_appender, "new-stream", :any_version, 3)
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(first)
      assert :ok = Task.await(second)

      assert stream_versions(context, stream_id(context, "new-stream")) == [1, 2, 3, 4, 5]
    end

    test "creating it with `:no_stream` answers `:stream_exists` from its retry", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      first = async_append(appender, "new-stream", :no_stream, 2)
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      second = async_append(second_appender, "new-stream", :no_stream, 3)
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(first)
      assert {:error, :stream_exists} = Task.await(second)

      assert stream_versions(context, stream_id(context, "new-stream")) == [1, 2]
    end
  end

  defp async_append(conn, stream_uuid, expected_version, count) do
    events = EventFactory.create_events(count)

    Task.async(fn ->
      EventStore.append_to_stream(stream_uuid, expected_version, events, conn: conn)
    end)
  end

  # A stream with one event, through a committed append.
  defp create_stream(context, stream_uuid) do
    :ok =
      EventStore.append_to_stream(stream_uuid, :any_version, EventFactory.create_events(1),
        conn: context.observer
      )

    stream_id(context, stream_uuid)
  end

  # A row of its own for a stream created while `$all` is held: no event, so no `$all`.
  defp create_stream_after_lock(context) do
    {:ok, stream_id} =
      Decree.Store.Storage.CreateStream.execute(context.observer, "stream",
        schema: context.schema
      )

    stream_id
  end

  defp read_events(context, stream_uuid) do
    {:ok, events} = EventStore.read_stream_forward(stream_uuid, 0, 10, conn: context.observer)

    events
  end

  defp stream_id(context, stream_uuid) do
    query!(
      context,
      ~s(SELECT stream_id FROM "#{context.schema}".streams WHERE stream_uuid = $1),
      [
        stream_uuid
      ]
    )
    |> List.first([nil])
    |> hd()
  end

  defp all_version(context) do
    [[version]] =
      query!(
        context,
        ~s(SELECT stream_version FROM "#{context.schema}".streams WHERE stream_id = 0),
        []
      )

    version
  end

  defp event_count(context) do
    [[count]] = query!(context, ~s[SELECT count(*) FROM "#{context.schema}".events], [])

    count
  end

  defp stream_versions(context, stream_id) do
    context
    |> query!(
      ~s(SELECT stream_version FROM "#{context.schema}".stream_events) <>
        " WHERE stream_id = $1 ORDER BY stream_version",
      [stream_id]
    )
    |> Enum.map(&hd/1)
  end

  # The stream each `$all` position came from, in position order.
  defp all_stream_ids(context) do
    context
    |> query!(
      ~s(SELECT original_stream_id FROM "#{context.schema}".stream_events) <>
        " WHERE stream_id = 0 ORDER BY stream_version",
      []
    )
    |> Enum.map(&hd/1)
  end

  defp query!(context, sql, params) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(context.observer, sql, params)

    rows
  end
end
