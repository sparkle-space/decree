defmodule Decree.Store.Storage.HardDeleteIndexFactTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{EventData, LockSession, TagIndex}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  # A hard delete refuses a stream holding a tag index fact: a disable it took with it would
  # leave an earlier enable standing.
  setup %{postgrex_config: postgrex_config} do
    [
      observer: LockSession.connect(postgrex_config),
      deleter: LockSession.connect(postgrex_config)
    ]
  end

  # The link row is one a store still holds from before #8075 removed links:
  # `seed_legacy_link/3` writes it.
  describe "a stream that links a disable fact" do
    test "is refused, and every row stays: events, links and tags", context do
      ledger!()
      :ok = EventStore.append_to_stream("link-stream", 0, [event(["tenant:a"])])

      {:ok, [_enable, disable]} = EventStore.read_stream_forward("ledger")
      :ok = seed_legacy_link(context, "link-stream", [disable])

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
