defmodule Decree.Store.Storage.HardDeleteIndexFactTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{EventData, LockSession, Storage, TagIndex}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  # A hard delete refuses a stream holding or linking a tag index fact: a disable it took with
  # it would leave an earlier enable standing.
  setup %{postgrex_config: postgrex_config} do
    deleter = LockSession.connect(postgrex_config)

    [
      observer: LockSession.connect(postgrex_config),
      deleter: deleter,
      deleter_backend: LockSession.backend_pid(deleter),
      linker: LockSession.connect(postgrex_config)
    ]
  end

  describe "a stream that links a disable fact" do
    test "is refused, and every row stays: events, links and tags", context do
      ledger!()
      :ok = EventStore.append_to_stream("link-stream", 0, [event(["tenant:a"])])

      {:ok, [_enable, disable]} = EventStore.read_stream_forward("ledger")
      :ok = EventStore.link_to_stream("link-stream", :any_version, [disable])

      before = snapshot(context)

      assert {:error, :hard_delete_of_index_fact} = delete(context, "link-stream")

      assert snapshot(context) == before
      assert stream_versions(context, "link-stream") == [1, 2]
      assert stream_versions(context, "ledger") == [1, 2]
    end
  end

  describe "a stream holding a fact of its own" do
    test "is refused, and every row stays", context do
      ledger!()

      before = snapshot(context)

      assert {:error, :hard_delete_of_index_fact} = delete(context, "ledger")

      assert snapshot(context) == before
    end

    test "is refused at every expected version", context do
      ledger!()

      for expected_version <- [:any_version, :stream_exists, 2] do
        assert {:error, :hard_delete_of_index_fact} =
                 delete(context, "ledger", expected_version)
      end
    end
  end

  describe "a stream holding no fact" do
    test "hard-deletes as before, beside a store holding facts", context do
      ledger!()
      :ok = EventStore.append_to_stream("plain", 0, [event(["tenant:a"])])

      assert :ok = delete(context, "plain")

      assert stream_id(context, "plain") == nil
      assert stream_versions(context, "ledger") == [1, 2]
      assert length(tag_rows(context)) == 4
    end
  end

  describe "against a link of a fact" do
    setup do
      ledger!()
      :ok = EventStore.append_to_stream("link-stream", 0, [event(["tenant:a"])])

      {:ok, [_enable, disable]} = EventStore.read_stream_forward("ledger")

      [disable: disable]
    end

    test "link first: the delete waits on the stream's row, then sees the link and refuses",
         context do
      %{deleter_backend: deleter_backend, disable: disable, observer: observer} = context

      linker = LockSession.start(context.postgrex_config)

      assert :ok =
               LockSession.run(linker, fn tx ->
                 EventStore.link_to_stream("link-stream", :any_version, [disable], conn: tx)
               end)

      delete = Task.async(fn -> delete(context, "link-stream") end)
      LockSession.wait_until_blocked(observer, deleter_backend, linker.backend)

      assert {:ok, :ok} = LockSession.commit(linker)
      assert {:error, :hard_delete_of_index_fact} = Task.await(delete)

      assert stream_versions(context, "link-stream") == [1, 2]
      assert stream_versions(context, "ledger") == [1, 2]
    end

    test "delete first: the link waits on the stream's row, then finds it gone", context do
      %{disable: disable, observer: observer} = context

      deleter = LockSession.start(context.postgrex_config)
      stream_id = stream_id(context, "link-stream")

      LockSession.run(deleter, fn tx ->
        Postgrex.query!(tx, "SET LOCAL eventstore.enable_hard_deletes TO 'on'", [])

        assert :ok = Storage.hard_delete_stream(tx, stream_id, schema: context.schema)
      end)

      link =
        Task.async(fn ->
          EventStore.link_to_stream("link-stream", :any_version, [disable], conn: context.linker)
        end)

      LockSession.wait_until_blocked(
        observer,
        LockSession.backend_pid(context.linker),
        deleter.backend
      )

      assert {:ok, :ok} = LockSession.commit(deleter)
      assert {:error, :not_found} = Task.await(link)

      assert stream_id(context, "link-stream") == nil
      assert stream_versions(context, "ledger") == [1, 2]
    end
  end

  # An enable and then a disable of one family, on a stream of their own.
  defp ledger! do
    :ok =
      EventStore.append_to_stream("ledger", 0, [
        event([TagIndex.fact_tag(), TagIndex.state_tag("entity", nil, :enabled)]),
        event([TagIndex.fact_tag(), TagIndex.state_tag("entity", nil, :disabled)])
      ])
  end

  defp delete(context, stream_uuid, expected_version \\ :any_version) do
    Stream.delete(context.deleter, stream_uuid, expected_version, :hard,
      schema: context.schema,
      enable_hard_deletes: true
    )
  end

  # Every row a hard delete could take.
  defp snapshot(context) do
    for table <- ["streams", "stream_events", "events", "event_tags", "event_types"] do
      {table, count(context, table)}
    end ++ [tags: tag_rows(context), positions: all_positions(context)]
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
