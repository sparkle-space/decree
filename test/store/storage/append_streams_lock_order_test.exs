defmodule Decree.Store.Storage.AppendStreamsLockOrderTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{EventFactory, LockSession, Storage}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  # A multi-stream append takes its members' rows in the bytewise order of their names,
  # then `$all`. These tests hold each lock from a second session, so what an append has
  # and has not taken shows in Postgres, never in timing.
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

  describe "the order of a multi-stream append's locks" do
    test "is the sorted one, whatever the caller's order", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      stream_a = create_stream(context, "a")
      stream_m = create_stream(context, "m")
      stream_z = create_stream(context, "z")

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, stream_m)

      append = async_append(appender, [{"z", 1}, {"m", 1}, {"a", 1}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      assert_row_locked(context, stream_a)
      assert_row_free(context, stream_z)

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

      assert stream_versions(context, "a") == [1, 2]
      assert stream_versions(context, "m") == [1, 2]
      assert stream_versions(context, "z") == [1, 2]
    end

    test "ends at `$all`: blocked there, it holds every member's row", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      stream_ids = for stream_uuid <- ["z", "m", "a"], do: create_stream(context, stream_uuid)

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      append = async_append(appender, [{"z", 1}, {"m", 1}, {"a", 1}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      for stream_id <- stream_ids, do: assert_row_locked(context, stream_id)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)
    end

    # `B` sorts first in bytes and last under a locale. Under the collation of a store
    # whose default is `en_US.utf8`, as CI's and any `initdb` default is, a sort without
    # `COLLATE "C"` takes `a-stream` first, and this order is the one that would differ.
    test "is the bytes', not the database's collation", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      assert [[true]] = rows(context, "SELECT 'a-stream' < 'B-stream'", [])
      assert [[false]] = rows(context, ~s(SELECT 'a-stream' COLLATE "C" < 'B-stream'), [])

      stream_a = create_stream(context, "a-stream")
      stream_b = create_stream(context, "B-stream")

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, stream_a)

      append = async_append(appender, [{"a-stream", 1}, {"B-stream", 1}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      assert_row_locked(context, stream_b)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)
    end
  end

  describe "two multi-stream appends over the same streams" do
    setup context do
      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      [holder: holder]
    end

    test "in opposite orders queue on `$all`, with no deadlock", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      create_stream_row(context, "x")
      create_stream_row(context, "y")

      first = async_append(appender, [{"x", :any_version}, {"y", :any_version}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      second = async_append(second_appender, [{"y", :any_version}, {"x", :any_version}])
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(first)
      assert :ok = Task.await(second)

      assert stream_versions(context, "x") == [1, 2]
      assert stream_versions(context, "y") == [1, 2]

      assert all_events(context) == [{"x", 1}, {"y", 1}, {"y", 2}, {"x", 2}]
    end

    test "creating the same streams in opposite orders: the loser retries and appends after",
         context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      first = async_append(appender, [{"new-x", :any_version}, {"new-y", :any_version}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      # The insert of `new-x` waits on the first append's uncommitted row.
      second = async_append(second_appender, [{"new-y", :any_version}, {"new-x", :any_version}])
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(first)
      assert :ok = Task.await(second)

      assert stream_versions(context, "new-x") == [1, 2]
      assert stream_versions(context, "new-y") == [1, 2]
    end

    test "creating them with `:no_stream`: the loser's retry reads them again", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      first = async_append(appender, [{"new-x", :no_stream}, {"new-y", :no_stream}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      second = async_append(second_appender, [{"new-y", :no_stream}, {"new-x", :no_stream}])
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(first)
      assert {:error, :stream_exists} = Task.await(second)

      assert stream_versions(context, "new-x") == [1]
      assert stream_versions(context, "new-y") == [1]
    end

    test "a plain append racing one to create a stream: the plain one first", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      plain = async_plain_append(appender, "new-a")
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      multi = async_append(second_appender, [{"new-z", :any_version}, {"new-a", :any_version}])
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      # Blocked at `new-a`, which sorts first, it has not reached `new-z`.
      assert stream_id(context, "new-z") == nil

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(plain)
      assert :ok = Task.await(multi)

      assert stream_versions(context, "new-a") == [1, 2]
      assert stream_versions(context, "new-z") == [1]
    end

    test "a plain append racing one to create a stream: the multi-stream one first", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      multi = async_append(appender, [{"new-z", :any_version}, {"new-a", :any_version}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      plain = async_plain_append(second_appender, "new-z")
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(multi)
      assert :ok = Task.await(plain)

      assert stream_versions(context, "new-a") == [1]
      assert stream_versions(context, "new-z") == [1, 2]
    end
  end

  # The creator holds its new row uncommitted, waiting on `$all`, so the loser reads no
  # stream and its insert waits on the creator's entry in the unique index.
  describe "losing a race to create a stream" do
    setup context do
      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      creator = async_plain_append(context.appender, "new")
      LockSession.wait_until_blocked(context.observer, context.appender_backend, holder.backend)

      [holder: holder, creator: creator]
    end

    test "a multi-stream append in a caller's transaction answers it", context do
      appends = [
        {"new", :any_version, EventFactory.create_events(1)},
        {"a-first", :any_version, EventFactory.create_events(1)}
      ]

      assert_race_answered(context, &EventStore.append_to_streams(appends, conn: &1))

      # It sorts first, so the loser had created it before it waited.
      assert stream_id(context, "a-first") == nil
    end

    test "a plain append in a caller's transaction answers it", context do
      events = EventFactory.create_events(1)

      assert_race_answered(
        context,
        &EventStore.append_to_stream("new", :any_version, events, conn: &1)
      )
    end

    test "1,000 or more events in a caller's transaction answer it", context do
      events = EventFactory.create_events(1_000)

      assert_race_answered(
        context,
        &EventStore.append_to_stream("new", :any_version, events, conn: &1)
      )
    end

    test "a `DBConnection.run/3` checkout is no transaction, and retries", context do
      %{observer: observer, holder: holder, creator: creator} = context
      %{appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      events = EventFactory.create_events(1)

      loser =
        Task.async(fn ->
          DBConnection.run(second_appender, fn checkout ->
            assert %DBConnection{conn_mode: nil} = checkout

            EventStore.append_to_stream("new", :any_version, events, conn: checkout)
          end)
        end)

      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(creator)
      assert :ok = Task.await(loser)

      assert stream_versions(context, "new") == [1, 2]
    end
  end

  describe "a plain append on a member stream" do
    setup context do
      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      create_stream_row(context, "x")
      create_stream_row(context, "y")

      [holder: holder]
    end

    test "after the multi-stream one waits on it, with no deadlock", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      multi = async_append(appender, [{"y", :any_version}, {"x", :any_version}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      plain = async_plain_append(second_appender, "x")
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(multi)
      assert :ok = Task.await(plain)

      assert all_events(context) == [{"y", 1}, {"x", 1}, {"x", 2}]
    end

    test "before the multi-stream one is waited on, with no deadlock", context do
      %{observer: observer, holder: holder} = context
      %{appender: appender, appender_backend: appender_backend} = context
      %{second_appender: second_appender, second_appender_backend: second_backend} = context

      plain = async_plain_append(appender, "y")
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      multi = async_append(second_appender, [{"y", :any_version}, {"x", :any_version}])
      LockSession.wait_until_blocked(observer, second_backend, appender_backend)

      # The multi-stream one holds `x`, which sorts first, while it waits on `y`.
      assert_row_locked(context, stream_id(context, "x"))

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(plain)
      assert :ok = Task.await(multi)

      assert all_events(context) == [{"y", 1}, {"y", 2}, {"x", 1}]
    end
  end

  describe "a link" do
    test "first: the multi-stream append waits on it, holding the members before", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      create_stream(context, "source")
      member = create_stream(context, "a-member")
      create_stream(context, "m-target")
      linked = read_events(context, "source")

      linker = LockSession.start(context.postgrex_config)

      LockSession.run(linker, fn tx ->
        assert :ok = EventStore.link_to_stream("m-target", :any_version, linked, conn: tx)
      end)

      append = async_append(appender, [{"m-target", :any_version}, {"a-member", :any_version}])
      LockSession.wait_until_blocked(observer, appender_backend, linker.backend)

      assert_row_locked(context, member)

      assert {:ok, :ok} = LockSession.commit(linker)
      assert :ok = Task.await(append)

      assert stream_versions(context, "m-target") == [1, 2, 3]
      assert stream_versions(context, "a-member") == [1, 2]
    end

    test "second: blocked on a member, it numbers after the append, with no deadlock",
         context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context
      %{second_appender: linker, second_appender_backend: linker_backend} = context

      create_stream(context, "source")
      create_stream(context, "target")
      create_stream(context, "other")
      linked = read_events(context, "source")

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      append = async_append(appender, [{"target", :any_version}, {"other", :any_version}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      link =
        Task.async(fn ->
          EventStore.link_to_stream("target", :any_version, linked, conn: linker)
        end)

      LockSession.wait_until_blocked(observer, linker_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)
      assert :ok = Task.await(link)

      assert stream_versions(context, "target") == [1, 2, 3]
    end

    # The link's foreign key takes KEY SHARE on the original stream's row, which FOR NO KEY
    # UPDATE lets through. Were it to wait, it would wait on an append waiting on `$all`.
    test "of a member's events does not wait on the row the append holds", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      original = create_stream(context, "original")
      create_stream(context, "other")
      linked = read_events(context, "original")

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      append = async_append(appender, [{"original", :any_version}, {"other", :any_version}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      assert_row_locked(context, original)

      assert :ok =
               EventStore.link_to_stream("target", :any_version, linked,
                 conn: context.second_appender
               )

      assert_row_locked(context, original)
      assert stream_versions(context, "target") == [1]

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)

      assert stream_versions(context, "original") == [1, 2]
    end
  end

  describe "a hard delete of a member" do
    test "first: the append blocks, then answers `:not_found` and writes nothing", context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context

      stream_id = create_stream(context, "m-deleted")
      create_stream(context, "z-kept")
      all_version = all_version(context)
      event_count = count(context, "events")

      deleter = LockSession.start(context.postgrex_config)

      LockSession.run(deleter, fn tx ->
        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

        assert :ok = Storage.hard_delete_stream(tx, stream_id, schema: context.schema)
      end)

      append =
        async_append(appender, [
          {"z-kept", :any_version},
          {"m-deleted", :any_version},
          {"a-new", :any_version}
        ])

      LockSession.wait_until_blocked(observer, appender_backend, deleter.backend)

      assert {:ok, :ok} = LockSession.commit(deleter)

      assert {:error, :not_found} = Task.await(append)
      assert stream_id(context, "m-deleted") == nil
      assert stream_id(context, "a-new") == nil
      assert stream_versions(context, "z-kept") == [1]
      assert count(context, "events") == event_count - 1
      assert all_version(context) == all_version
    end

    test "second: blocked on the member, it fails the foreign key and the append stands",
         context do
      %{observer: observer, appender: appender, appender_backend: appender_backend} = context
      %{second_appender_backend: deleter_backend} = context

      stream_id = create_stream(context, "kept")
      create_stream(context, "other")
      all_version = all_version(context)

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      append = async_append(appender, [{"other", :any_version}, {"kept", :any_version}])
      LockSession.wait_until_blocked(observer, appender_backend, holder.backend)

      delete =
        Task.async(fn ->
          Stream.delete(context.second_appender, "kept", :any_version, :hard,
            schema: context.schema,
            enable_hard_deletes: true
          )
        end)

      LockSession.wait_until_blocked(observer, deleter_backend, appender_backend)

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)

      assert {:error, %Postgrex.Error{postgres: %{code: :foreign_key_violation}}} =
               Task.await(delete)

      assert stream_id(context, "kept") == stream_id
      assert stream_versions(context, "kept") == [1, 2]
      assert all_version(context) == all_version + 2
    end
  end

  # Whatever `append` does in the transaction loses to `creator`. A retry there would have
  # answered `in_failed_sql_transaction`, not the race.
  defp assert_race_answered(context, append) do
    %{observer: observer, holder: holder, creator: creator} = context

    caller = LockSession.start(context.postgrex_config)

    reply =
      LockSession.start_run(caller, fn tx ->
        assert %DBConnection{conn_mode: :transaction} = tx

        append.(tx)
      end)

    LockSession.wait_until_blocked(observer, caller.backend, context.appender_backend)

    assert {:ok, :ok} = LockSession.commit(holder)
    assert :ok = Task.await(creator)

    assert {:error, :duplicate_stream_uuid} = LockSession.await_run(reply)

    # Aborted, so a following statement fails and the commit rolls back.
    assert {:error, _error} = LockSession.run(caller, &Postgrex.query(&1, "SELECT 1", []))
    assert {:error, :rollback} = LockSession.commit(caller)

    assert stream_versions(context, "new") == [1]
    assert all_events(context) == [{"new", 1}]
  end

  defp async_append(conn, members) do
    appends =
      Enum.map(members, fn {stream_uuid, expected_version} ->
        {stream_uuid, expected_version, EventFactory.create_events(1)}
      end)

    Task.async(fn -> EventStore.append_to_streams(appends, conn: conn) end)
  end

  defp async_plain_append(conn, stream_uuid) do
    events = EventFactory.create_events(1)

    Task.async(fn ->
      EventStore.append_to_stream(stream_uuid, :any_version, events, conn: conn)
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
  defp create_stream_row(context, stream_uuid) do
    {:ok, stream_id} =
      Storage.CreateStream.execute(context.observer, stream_uuid, schema: context.schema)

    stream_id
  end

  defp read_events(context, stream_uuid) do
    {:ok, events} = EventStore.read_stream_forward(stream_uuid, 0, 10, conn: context.observer)

    events
  end
end
