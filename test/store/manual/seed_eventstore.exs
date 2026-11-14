#
# Seed Decree.Store with data to test migrations.
#
#   MIX_ENV=test mix es.reset
#   MIX_ENV=test mix run test/manual/seed_eventstore.exs
#
#   pg_dump eventstore_test > test/store/fixture/eventstore_seed.sql
#   pg_dump -Fc eventstore_test > test/store/fixture/eventstore_seed.dump
#

defmodule Event do
  @derive Jason.Encoder
  defstruct [:data, version: "1"]
end

defmodule Seed do
  alias Decree.Store.{EventData, UUID}

  def run(opts \\ []) do
    append_events(opts)
    # The committed dumps were seeded with links, by a step removed with links in
    # sparkle-space/sparklespace#8075, and `migrated_event_store_test.exs` "read linked stream
    # forward" reads them. A re-seed has none, so that test would then have nothing to read.
    # They were seeded with ten rows of the one-row `snapshots` table too, by a step removed
    # with the store's unversioned snapshot write in sparkle-space/sparklespace#8200, and
    # `migrated_event_store_test.exs` checks the migration keeps them. A re-seed has none.
    subscribe_to_streams()
  end

  defp append_events(opts) do
    stream_count = Keyword.get(opts, :stream_count, 10)
    event_count = Keyword.get(opts, :event_count, 10)

    for stream_index <- 1..stream_count do
      events =
        for event_number <- 1..event_count do
          %EventData{
            correlation_id: UUID.uuid4(),
            causation_id: UUID.uuid4(),
            event_type: "Elixir.Event",
            data: %Event{data: event_number},
            metadata: %{"user" => "user@example.com", "timestamp" => DateTime.utc_now()}
          }
        end

      :ok = TestEventStore.append_to_stream("stream-#{stream_index}", 0, events)
    end
  end

  defp subscribe_to_streams do
    {:ok, _subscription} =
      TestEventStore.subscribe_to_all_streams("subscription-all-origin", self(),
        start_from: :origin
      )

    {:ok, _subscription} =
      TestEventStore.subscribe_to_all_streams("subscription-all-current", self(),
        start_from: :current
      )

    {:ok, _subscription} =
      TestEventStore.subscribe_to_stream("stream-1", "subscription-stream-1-origin", self(),
        start_from: :origin
      )

    {:ok, _subscription} =
      TestEventStore.subscribe_to_stream("stream-1", "subscription-stream-1-current", self(),
        start_from: :current
      )
  end
end

{:ok, _pid} = TestEventStore.start_link()

# Seed.run(stream_count: 1_000, event_count: 100)
Seed.run()
