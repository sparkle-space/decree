defmodule Decree.Store.Subscriptions.AllStreamsSubscriptionTest do
  use Decree.Store.Subscriptions.StreamSubscriptionTestCase, stream_uuid: "$all"

  alias Decree.Store.{EventFactory, ProcessHelper, RecordedEvent, UUID}
  alias Decree.Store.Storage.Appender
  alias Decree.Store.StoreRows
  alias Decree.Store.Subscriptions.SubscriptionFsm

  defp create_recorded_events(_context, count, initial_event_number \\ 1) do
    stream_uuid = UUID.uuid4()

    EventFactory.create_recorded_events(count, stream_uuid, initial_event_number)
  end

  defp append_events_to_stream(context) do
    %{
      conn: conn,
      schema: schema,
      correlation_id_type: correlation_id_type,
      causation_id_type: causation_id_type
    } = context

    stream_uuid = UUID.uuid4()
    recorded_events = EventFactory.create_recorded_events(3, stream_uuid)

    {:ok, stream_id} = StoreRows.insert_stream(conn, schema, stream_uuid)

    :ok =
      Appender.append(conn, stream_id, recorded_events,
        schema: schema,
        correlation_id_type: correlation_id_type,
        causation_id_type: causation_id_type
      )

    [recorded_events: recorded_events]
  end
end
