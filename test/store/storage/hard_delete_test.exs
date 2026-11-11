defmodule Decree.Store.Storage.HardDeleteTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{EventData, LockSession, UUID}
  alias Decree.Store.Storage
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  # A hard delete takes the stream's row, deletes the events with their tag and type rows, and
  # then deletes the stream's row. Each of these holds a session open across a step, so what
  # each side holds shows in Postgres.
  setup %{postgrex_config: postgrex_config} do
    observer = LockSession.connect(postgrex_config)
    deleter = LockSession.connect(postgrex_config)

    [
      observer: observer,
      deleter: deleter,
      deleter_backend: LockSession.backend_pid(deleter),
      appender: LockSession.connect(postgrex_config)
    ]
  end

  describe "against a plain append" do
    test "delete first: the append blocks, answers `:not_found`, and nothing is left", context do
      %{appender: appender, observer: observer} = context

      :ok = EventStore.append_to_stream("stream", 0, [event(["tenant:a"])])

      deleter = LockSession.start(context.postgrex_config)

      LockSession.run(deleter, fn tx ->
        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

        assert :ok =
                 Storage.hard_delete_stream(tx, stream_id(context, "stream"),
                   schema: context.schema
                 )
      end)

      events = [event(["tenant:b"])]

      append =
        Task.async(fn ->
          EventStore.append_to_stream("stream", :any_version, events, conn: appender)
        end)

      LockSession.wait_until_blocked(observer, LockSession.backend_pid(appender), deleter.backend)

      assert {:ok, :ok} = LockSession.commit(deleter)
      assert {:error, :not_found} = Task.await(append)

      assert_nothing_left(context)
    end

    test "append first: the delete blocks, then removes what the append wrote", context do
      %{deleter_backend: deleter_backend, observer: observer} = context

      :ok = EventStore.append_to_stream("stream", 0, [event(["tenant:a"])])

      appender = LockSession.start(context.postgrex_config)

      LockSession.run(appender, fn tx ->
        assert :ok =
                 EventStore.append_to_stream("stream", :any_version, [event(["tenant:b"])],
                   conn: tx
                 )
      end)

      delete = async_delete(context, "stream")
      LockSession.wait_until_blocked(observer, deleter_backend, appender.backend)

      assert {:ok, :ok} = LockSession.commit(appender)
      assert :ok = Task.await(delete)

      assert_nothing_left(context)
    end
  end

  # The erasure path. A tag or type row has no foreign key, so one left behind is silent: these
  # assertions are the only thing that sees it. They read rows by event id and by position,
  # captured before the delete, so a count that happens to match is not enough.
  describe "erasure" do
    test "takes the stream's events from `$all`, with their tag and type rows, and leaves another stream's",
         context do
      :ok = EventStore.append_to_stream("stream", 0, [event(["tenant:a"]), event(["tenant:b"])])
      :ok = EventStore.append_to_stream("other", 0, [event(["tenant:c"])])

      {:ok, deleted} = EventStore.read_stream_forward("stream")
      {:ok, [kept]} = EventStore.read_stream_forward("other")

      deleted_ids = Enum.map(deleted, &UUID.string_to_binary!(&1.event_id))
      kept_ids = [UUID.string_to_binary!(kept.event_id)]

      deleted_positions = all_positions_of(context, deleted_ids)
      [kept_position] = kept_positions = all_positions_of(context, kept_ids)

      assert length(deleted_positions) == 2

      assert tag_rows_at(context, deleted_positions) |> Enum.map(&List.last/1) ==
               ~w(tenant:a tenant:b)

      assert length(type_rows_at(context, deleted_positions)) == 2

      kept_event = event_rows(context, kept_ids)
      kept_all = all_rows(context, kept_ids)
      kept_tags = tag_rows_at(context, kept_positions)
      kept_types = type_rows_at(context, kept_positions)

      assert [[^kept_position, "tenant:c"]] = kept_tags
      assert [[^kept_position, _event_type]] = kept_types

      assert :ok = delete(context, "stream")

      assert event_rows(context, deleted_ids) == []
      assert all_rows(context, deleted_ids) == []
      assert stream_versions(context, "stream") == []
      assert tag_rows_at(context, deleted_positions) == []
      assert type_rows_at(context, deleted_positions) == []

      assert event_rows(context, kept_ids) == kept_event
      assert all_rows(context, kept_ids) == kept_all
      assert stream_versions(context, "other") == [1]
      assert tag_rows(context) == kept_tags
      assert type_rows_at(context, kept_positions) == kept_types
      assert all_positions(context) == kept_positions
      assert count(context, "events") == 1
      assert count(context, "event_types") == 1

      assert {:ok, [read]} = EventStore.read_all_streams_forward()
      assert read.event_id == kept.event_id
      assert read.stream_uuid == "other"
    end

    defp event_rows(context, ids) do
      rows(
        context,
        ~s(SELECT event_id, event_type FROM "#{context.schema}".events) <>
          " WHERE event_id = ANY($1) ORDER BY event_id",
        [ids]
      )
    end

    defp all_rows(context, ids) do
      rows(
        context,
        ~s(SELECT event_id, stream_version, original_stream_id, original_stream_version) <>
          ~s( FROM "#{context.schema}".stream_events) <>
          " WHERE stream_id = 0 AND event_id = ANY($1) ORDER BY stream_version",
        [ids]
      )
    end

    defp all_positions_of(context, ids), do: context |> all_rows(ids) |> Enum.map(&Enum.at(&1, 1))

    defp tag_rows_at(context, positions) do
      rows(
        context,
        ~s(SELECT position, tag FROM "#{context.schema}".event_tags) <>
          " WHERE position = ANY($1) ORDER BY 1, 2",
        [positions]
      )
    end

    defp type_rows_at(context, positions) do
      rows(
        context,
        ~s(SELECT position, event_type FROM "#{context.schema}".event_types) <>
          " WHERE position = ANY($1) ORDER BY 1, 2",
        [positions]
      )
    end
  end

  # `link_to_stream` is gone (#8075), but a store can still hold a link row written before it
  # was: `seed_legacy_link/3` writes one. The delete's `linked_events` CTE takes it.
  describe "a stream holding a link row written before #8075" do
    test "takes the linked event from everywhere, with its tags and types", context do
      :ok = EventStore.append_to_stream("source", 0, [event(["tenant:a"])])
      :ok = EventStore.append_to_stream("other", 0, [event(["tenant:b"])])

      {:ok, [linked]} = EventStore.read_stream_forward("source")
      :ok = seed_legacy_link(context, "link-stream", [linked])

      linked_ids = [UUID.string_to_binary!(linked.event_id)]

      assert stream_versions(context, "link-stream") == [1]
      assert [[_event_id, _event_type]] = event_rows(context, linked_ids)

      assert :ok = delete(context, "link-stream")

      assert stream_id(context, "link-stream") == nil
      assert stream_versions(context, "source") == []
      assert event_rows(context, linked_ids) == []
      assert all_positions(context) == [2]
      assert tag_rows(context) == [[2, "tenant:b"]]
      assert rows(context, ~s(SELECT position FROM "#{context.schema}".event_types), []) == [[2]]
    end
  end

  describe "the fence" do
    setup %{deleter: deleter} do
      :ok = EventStore.append_to_stream("stream", 0, [event(["tenant:a"])])
      :ok = EventStore.append_to_stream("subject", 0, [event(["email:x"])])

      [raw_delete: &Postgrex.query(deleter, ~s(DELETE FROM "public".#{&1}), [])]
    end

    test "is closed again on the connection after a hard delete", context do
      assert :ok = delete(context, "stream")

      assert_fenced(context)
    end

    test "is closed again on the connection after `delete_stream_tags`", context do
      assert {:ok, 1} =
               Stream.delete_tags(context.deleter, "subject", ["email"],
                 schema: context.schema,
                 enable_hard_deletes: true
               )

      assert_fenced(context)
    end

    # A nested transaction is the caller's, so only the close can shut the fence here. The
    # savepoint keeps that transaction alive past each refusal, and the commit shows the close
    # took nothing with it.
    test "is closed again inside a caller's transaction after a hard delete", context do
      assert {:ok, :ok} =
               Postgrex.transaction(context.deleter, fn tx ->
                 assert :ok = Stream.delete(tx, "stream", :any_version, :hard, opts(context))

                 assert_fenced_in(context, tx)
               end)

      assert stream_versions(context, "stream") == []
      assert tag_rows(context) == [[2, "email:x"]]
    end

    test "is closed again inside a caller's transaction after `delete_stream_tags`", context do
      assert {:ok, :ok} =
               Postgrex.transaction(context.deleter, fn tx ->
                 assert {:ok, 1} = Stream.delete_tags(tx, "subject", ["email"], opts(context))

                 assert_fenced_in(context, tx)
               end)

      assert tag_rows(context) == [[1, "tenant:a"]]
    end

    # The stream's version moves between the delete's read and its lock, so this is the error
    # arm with the fence open, and there is no close on it. The rollback is what stands in.
    test "a failed delete leaves a caller's transaction rolled back, with nothing run after",
         context do
      %{deleter: deleter, deleter_backend: deleter_backend, observer: observer} = context

      appender = LockSession.start(context.postgrex_config)

      LockSession.run(appender, fn tx ->
        assert :ok =
                 EventStore.append_to_stream("stream", :any_version, [event(["tenant:b"])],
                   conn: tx
                 )
      end)

      delete =
        Task.async(fn ->
          Postgrex.transaction(deleter, fn tx ->
            assert {:error, :wrong_expected_version} =
                     Stream.delete(tx, "stream", 1, :hard, opts(context))

            assert {:error, %DBConnection.ConnectionError{message: "transaction rolling back"}} =
                     Postgrex.query(tx, raw_delete_sql(context, "event_tags"), [])
          end)
        end)

      LockSession.wait_until_blocked(observer, deleter_backend, appender.backend)

      assert {:ok, :ok} = LockSession.commit(appender)
      assert {:error, :rollback} = Task.await(delete)

      assert stream_versions(context, "stream") == [1, 2]
      assert tag_rows(context) == [[1, "tenant:a"], [2, "email:x"], [3, "tenant:b"]]
    end

    defp assert_fenced(%{raw_delete: raw_delete}) do
      for table <- ["event_tags", "event_types"] do
        assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
                 raw_delete.(table)
      end
    end

    defp assert_fenced_in(context, tx) do
      for table <- ["event_tags", "event_types"] do
        assert {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} =
                 Postgrex.query(tx, raw_delete_sql(context, table), [], mode: :savepoint)
      end

      :ok
    end

    defp raw_delete_sql(context, table), do: ~s(DELETE FROM "#{context.schema}".#{table})

    defp opts(context), do: [schema: context.schema, enable_hard_deletes: true]

    test "answers `:not_supported` with `enable_hard_deletes` off" do
      assert {:error, :not_supported} = EventStore.delete_stream("stream", :any_version, :hard)
      assert {:error, :not_supported} = EventStore.delete_stream_tags("subject", ["email"])
    end
  end

  describe "with the row gone or changed under the lock" do
    test "`:stream_not_found`, from any expected version", context do
      assert {:error, :stream_not_found} =
               LockSession.run(LockSession.start(context.postgrex_config), fn tx ->
                 Storage.hard_delete_stream(tx, 12_345,
                   schema: context.schema,
                   expected_version: 0
                 )
               end)
    end

    test "`:stream_deleted` for a soft delete since the read", context do
      :ok = EventStore.append_to_stream("stream", 0, [event([])])
      stream_id = stream_id(context, "stream")
      :ok = EventStore.delete_stream("stream", :any_version, :soft)

      session = LockSession.start(context.postgrex_config)

      assert {:error, :stream_deleted} =
               LockSession.run(session, fn tx ->
                 Storage.hard_delete_stream(tx, stream_id, schema: context.schema)
               end)

      assert {:ok, :ok} = LockSession.commit(session)
    end
  end

  defp delete(context, stream_uuid) do
    Stream.delete(context.deleter, stream_uuid, :any_version, :hard,
      schema: context.schema,
      enable_hard_deletes: true
    )
  end

  defp async_delete(context, stream_uuid), do: Task.async(fn -> delete(context, stream_uuid) end)

  defp assert_nothing_left(context) do
    assert stream_id(context, "stream") == nil
    assert_nothing_left_of(context, "stream")

    assert all_positions(context) == []
    assert count(context, "events") == 0
    assert count(context, "event_tags") == 0
    assert count(context, "event_types") == 0
  end

  defp assert_nothing_left_of(context, stream_uuid) do
    assert stream_versions(context, stream_uuid) == []
    assert tag_rows(context) == []
  end

  defp tag_rows(context) do
    rows(context, ~s(SELECT position, tag FROM "#{context.schema}".event_tags ORDER BY 1, 2), [])
  end

  defp event(tags) do
    %EventData{
      event_type: "Elixir.Decree.Store.EventFactory.Event",
      data: %Decree.Store.EventFactory.Event{event: 1},
      metadata: %{},
      tags: tags
    }
  end
end
