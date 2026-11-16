defmodule Decree.Store.Storage.SubscriptionCheckpointHardDeleteTest do
  use Decree.Store.StorageCase

  # A stream subscription's checkpoint write (`subscription_ack_by_id_in_stream`) against a
  # real hard delete of its stream, in every order (sparkle-space/sparklespace#8386). The
  # write takes the stream's row FOR KEY SHARE before its subscription row; the hard delete
  # deletes the stream's row and then resets the stream's subscription rows to 0. Whatever
  # the order, the row ends at 0 once the delete commits, and neither waits on the other in
  # a cycle. Each order is driven by sessions held open (`LockSession`), and its waits are
  # read from Postgres, never from timing.

  alias Decree.Store.{Config, EventFactory, LockSession, StoreRows, TagIndex, UUID}
  alias Decree.Store.Sql.Statements
  alias Decree.Store.Storage
  alias Decree.Store.Storage.{DeleteStream, Initializer}
  alias Decree.Store.Storage.Stream, as: StreamStorage

  setup_all do
    config = SchemaEventStore.config()
    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(config)}, id: :schema_conn)

    [schema_conn: conn, schema_config: config]
  end

  setup %{schema_conn: conn, schema_config: config} do
    Initializer.reset!(conn, config)
    start_supervised!(SchemaEventStore)

    :ok
  end

  for {name, store, schema} <- [
        {"public", TestEventStore, "public"},
        {"a named schema", SchemaEventStore, "example"}
      ] do
    describe "a stream subscription's checkpoint and a hard delete under #{name}" do
      setup context do
        store = unquote(store)
        schema = unquote(schema)

        stop_supervised!(store)
        start_supervised!({store, enable_hard_deletes: true})

        %{conn: conn} = context

        stream_uuid = UUID.uuid4()
        :ok = store.append_to_stream(stream_uuid, 0, EventFactory.create_events(3))
        stream_id = StoreRows.stream_id(%{observer: conn, schema: schema}, stream_uuid)

        {:ok, %Storage.Subscription{subscription_id: subscription_id}} =
          Storage.Subscription.subscribe_to_stream(conn, stream_uuid, UUID.uuid4(), 0,
            schema: schema
          )

        :ok =
          Storage.Subscription.ack_last_seen_event_by_id(conn, subscription_id, 2, schema: schema)

        [
          store: store,
          schema: schema,
          stream_uuid: stream_uuid,
          stream_id: stream_id,
          subscription_id: subscription_id
        ]
      end

      test "a write holding its stream's row makes the delete wait, which then resets the row",
           context do
        %{store: store, conn: observer, postgrex_config: postgrex_config} = context

        writer = LockSession.start(postgrex_config)

        assert :ok = LockSession.run(writer, &ack_in_stream(&1, context, 3))

        erase =
          Task.async(fn -> store.delete_stream(context.stream_uuid, :any_version, :hard) end)

        LockSession.wait_until_waiting(observer, writer.backend, 1)
        {:ok, :ok} = LockSession.commit(writer)

        assert Task.await(erase) == :ok
        assert last_seen(context) == [[0]]

        assert StoreRows.stream_id(
                 %{observer: observer, schema: context.schema},
                 context.stream_uuid
               ) == nil
      end

      # The write's one statement holds the stream's row FOR KEY SHARE while it waits on its
      # own subscription row, held by another session; the delete's DELETE then waits on the
      # write. No cycle: the write completes once its row is free, and the delete after it.
      test "a write waiting on its own row, holding its stream's row, lets the delete wait behind it",
           context do
        %{store: store, conn: observer, postgrex_config: postgrex_config} = context

        holder = LockSession.start(postgrex_config)

        LockSession.run(holder, fn tx ->
          Postgrex.query!(
            tx,
            ~s(SELECT 1 FROM "#{context.schema}".subscriptions WHERE subscription_id = $1) <>
              " FOR UPDATE",
            [context.subscription_id]
          )
        end)

        write =
          Task.async(fn -> ack_in_stream(LockSession.connect(postgrex_config), context, 3) end)

        LockSession.wait_until_waiting(observer, holder.backend, 1)

        erase =
          Task.async(fn -> store.delete_stream(context.stream_uuid, :any_version, :hard) end)

        # The write waits on the holder, and the delete on the write.
        LockSession.wait_until_waiting(observer, holder.backend, 2)
        {:ok, :ok} = LockSession.commit(holder)

        assert Task.await(write) == :ok
        assert Task.await(erase) == :ok
        assert last_seen(context) == [[0]]
      end

      # What keeps the two from a cycle: the delete takes no subscription row before its
      # stream row's DELETE. While that DELETE waits on a KEY SHARE (a checkpoint write's, or a
      # versioned snapshot write's), the stream's subscription rows are free, so a checkpoint
      # write that holds the stream's row never waits on the delete for its own row.
      test "a delete waiting on its stream's row holds none of its subscription rows", context do
        %{store: store, conn: observer, postgrex_config: postgrex_config, schema: schema} =
          context

        key_share = LockSession.start(postgrex_config)

        LockSession.run(key_share, fn tx ->
          Postgrex.query!(
            tx,
            ~s(SELECT 1 FROM "#{schema}".streams WHERE stream_id = $1 FOR KEY SHARE),
            [context.stream_id]
          )
        end)

        erase =
          Task.async(fn -> store.delete_stream(context.stream_uuid, :any_version, :hard) end)

        LockSession.wait_until_waiting(observer, key_share.backend, 1)

        probe =
          Postgrex.query(
            observer,
            ~s(SELECT 1 FROM "#{schema}".subscriptions WHERE subscription_id = $1) <>
              " FOR UPDATE NOWAIT",
            [context.subscription_id]
          )

        assert match?({:ok, %Postgrex.Result{num_rows: 1}}, probe),
               "the hard delete holds a subscription row while it waits on its stream's row: " <>
                 inspect(probe)

        {:ok, :ok} = LockSession.commit(key_share)

        assert Task.await(erase) == :ok
        assert last_seen(context) == [[0]]
      end

      test "a delete held before commit makes a write wait, which then writes nothing",
           context do
        %{conn: observer, postgrex_config: postgrex_config} = context

        # The store's own hard delete, all five statements, held open before commit.
        eraser = LockSession.start(postgrex_config)

        :ok =
          LockSession.run(eraser, fn tx ->
            Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])
            DeleteStream.hard_delete(tx, context.stream_id, schema: context.schema)
          end)

        write =
          Task.async(fn -> ack_in_stream(LockSession.connect(postgrex_config), context, 3) end)

        LockSession.wait_until_waiting(observer, eraser.backend, 1)
        {:ok, :ok} = LockSession.commit(eraser)

        assert Task.await(write) == {:error, :stream_changed}
        assert last_seen(context) == [[0]]
      end

      test "a write inside the delete's transaction, before its DELETE, does not wait, and the delete then resets the row",
           context do
        %{conn: observer, postgrex_config: postgrex_config, schema: schema} = context

        # The hard delete's first three statements: the stream's row FOR NO KEY UPDATE, the
        # tag index check, and the events' delete.
        eraser = LockSession.start(postgrex_config)

        :ok =
          LockSession.run(eraser, fn tx ->
            Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])
            :ok = StreamStorage.lock_at_version(tx, context.stream_id, :any_version, schema, [])

            Postgrex.query!(tx, Statements.query_index_facts(schema), [
              context.stream_id,
              TagIndex.fact_tag()
            ])

            Postgrex.query!(tx, Statements.hard_delete_stream(schema), [context.stream_id])

            :ok
          end)

        # KEY SHARE does not conflict with NO KEY UPDATE: the write answers at once.
        write = Task.async(fn -> ack_in_stream(observer, context, 3) end)
        assert Task.yield(write, 5_000) == {:ok, :ok}
        assert last_seen(context) == [[3]]

        # The delete's last two statements: the stream's row, then the subscription rows.
        LockSession.run(eraser, fn tx ->
          %Postgrex.Result{rows: [[_stream_id, stream_uuid]]} =
            Postgrex.query!(tx, Statements.delete_stream_row(schema), [context.stream_id])

          Postgrex.query!(tx, Statements.reset_stream_subscriptions(schema), [stream_uuid])
        end)

        {:ok, :ok} = LockSession.commit(eraser)

        assert last_seen(context) == [[0]]

        assert StoreRows.stream_id(%{observer: observer, schema: schema}, context.stream_uuid) ==
                 nil
      end
    end
  end

  defp ack_in_stream(conn, context, last_seen) do
    %{subscription_id: subscription_id, stream_id: stream_id, schema: schema} = context

    Storage.Subscription.ack_last_seen_event_in_stream(
      conn,
      subscription_id,
      last_seen,
      stream_id,
      schema: schema
    )
  end

  defp last_seen(context) do
    %{conn: conn, schema: schema, subscription_id: subscription_id} = context

    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        ~s(SELECT last_seen FROM "#{schema}".subscriptions WHERE subscription_id = $1),
        [subscription_id]
      )

    rows
  end
end
