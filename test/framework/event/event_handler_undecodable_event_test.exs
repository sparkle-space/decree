defmodule Decree.Event.EventHandlerUndecodableEventTest do
  use Decree.PostgresCase

  # An event handler whose stream holds an event of a known type whose bytes the serializer
  # cannot decode (sparkle-space/sparklespace#8357). The store's subscription used to raise in
  # its catch-up read and die; the handler, which stops when its subscription goes down, then
  # stopped, its supervisor restarted it (`:permanent`), the new subscription read the same
  # event and died again, until the supervisor gave up. Now the subscription holds before the
  # event: the handler handles the events before it, hears that its subscription lost its
  # lock, and stays up.
  #
  # The event is planted through `Appender.append/4` with data that is not JSON and prints as
  # text, so a log line or crash report that carries the serializer's error shows the marker.

  import ExUnit.CaptureLog

  alias Decree.FrameworkStorage
  alias Decree.Store.{Config, RecordedEvent}
  alias Decree.Store.Storage.Appender
  alias Decree.UUID

  @marker "undecodable-handler-payload-marker"
  @stream "x8357-undecodable-handler-0f3c9a71"
  @sink :event_handler_undecodable_event_test

  defmodule Good do
    @derive Jason.Encoder
    defstruct [:note]
  end

  defmodule GoodHandler do
    @moduledoc false

    use Decree.Event.Handler,
      application: Decree.DefaultApp,
      name: __MODULE__,
      subscribe_to: "x8357-undecodable-handler-0f3c9a71"

    alias Decree.Event.EventHandlerUndecodableEventTest.Good

    def handle(%Good{}, %{event_number: event_number}) do
      send(:event_handler_undecodable_event_test, {:handled, event_number})

      :ok
    end
  end

  setup do
    Process.register(self(), @sink)

    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})

    [conn: conn]
  end

  test "a default handler handles the events before it and stays up", %{conn: conn} do
    # On the old code the handler's restarts end its supervisor: trapped, so the outcome is
    # recorded, and the log assertions run first.
    Process.flag(:trap_exit, true)

    log =
      capture_log([level: :debug], fn ->
        start_supervised!(Decree.DefaultApp)

        events = [event(1, Jason.encode!(%Good{note: "good"})), event(2, @marker)]

        :ok = Appender.append(conn, nil, events, schema: "public")

        handler = start_supervised!({GoodHandler, start_from: :origin})
        ref = Process.monitor(handler)

        handled =
          receive do
            {:handled, 1} -> :handled
          after
            5_000 -> :not_handled
          end

        # A bounded window in which a crash loop would show.
        down =
          receive do
            {:DOWN, ^ref, :process, ^handler, reason} -> {:down, reason}
          after
            2_000 -> :up
          end

        send(self(), {:outcome, handled, down, Process.alive?(handler)})
      end)

    refute log =~ @marker
    refute log =~ @stream

    assert_received {:outcome, :handled, :up, true}
    refute_received {:handled, _event_number}
    refute_received {:EXIT, _pid, _reason}
  end

  defp event(number, data) do
    %RecordedEvent{
      event_id: UUID.uuid4(),
      event_number: number,
      stream_uuid: @stream,
      stream_version: number,
      correlation_id: nil,
      causation_id: nil,
      event_type: Atom.to_string(Good),
      data: data,
      metadata: "{}",
      created_at: DateTime.utc_now()
    }
  end
end
