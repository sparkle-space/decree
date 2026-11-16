defmodule Decree.Event.EventHandlerLogTermsTest do
  use Decree.MockEventStoreCase

  # An event handler's log lines name an event by its number, its type and the last eight
  # characters of its stream's id, an error, a stop reason or a message by its class, and a
  # raise by the exception's module and a scrubbed stacktrace: never an event's data or
  # metadata, a full stream id or an exception's message (sparkle-space/sparklespace#8358).
  # Each test asserts its line still appears, and that the marker it planted and the event's
  # full stream id appear nowhere in the log. Replies to `error/3` and telemetry are not
  # changed. Every stop here is a `{:shutdown, _}`, so no crash report prints the stop
  # reason or the last message, which are not decree's lines.

  import ExUnit.CaptureLog

  alias Decree.Helpers.EventFactory

  @marker "handler-log-terms-marker"

  defmodule Fail do
    @derive Jason.Encoder
    defstruct [:marker]
  end

  defmodule Raise do
    @derive Jason.Encoder
    defstruct [:marker]
  end

  defmodule Invalid do
    @derive Jason.Encoder
    defstruct [:marker]
  end

  defmodule BadResponse do
    @derive Jason.Encoder
    defstruct [:marker]
  end

  defmodule LogTermsHandler do
    @moduledoc false

    use Decree.Event.Handler,
      application: Decree.MockedApp,
      name: __MODULE__

    alias Decree.Event.EventHandlerLogTermsTest.{BadResponse, Fail, Invalid, Raise}

    def handle(%Fail{marker: marker}, _metadata), do: {:error, {:failed, marker}}
    def handle(%Raise{marker: marker}, _metadata), do: raise("raised " <> marker)
    def handle(%Invalid{marker: marker}, _metadata), do: {:bogus, marker}
    def handle(%BadResponse{marker: marker}, _metadata), do: {:error, {:shutdown, marker}}

    def error({:error, _reason}, %BadResponse{marker: marker}, _failure_context),
      do: {:bogus_response, marker}

    def error({:error, _reason}, %{marker: marker}, _failure_context),
      do: {:stop, {:shutdown, {:stopped, marker}}}
  end

  defmodule ResetStoppingHandler do
    @moduledoc false

    use Decree.Event.Handler,
      application: Decree.MockedApp,
      name: __MODULE__

    def before_reset, do: {:stop, {:shutdown, {:reset_stopped, "handler-log-terms-marker"}}}
  end

  defmodule LogTermsBatchHandler do
    @moduledoc false

    use Decree.Event.Handler,
      application: Decree.MockedApp,
      name: __MODULE__,
      batch_size: 10,
      personal_fields: :opaque

    alias Decree.Event.EventHandlerLogTermsTest.{Fail, Invalid, Raise}

    def handle_batch([{%Fail{marker: marker}, _metadata} | _rest]),
      do: {:error, {:failed, marker}}

    def handle_batch([{%Raise{marker: marker}, _metadata} | _rest]),
      do: raise("raised " <> marker)

    def handle_batch([{%Invalid{marker: marker}, _metadata} | _rest]), do: {:bogus, marker}

    def error(_error, _events, _failure_context), do: {:stop, {:shutdown, :batch_failed}}
  end

  describe "a handled event" do
    test "an error reply is named by its class, and so is the requested stop" do
      {log, [event]} = run(LogTermsHandler, [%Fail{marker: @marker}])

      assert log =~
               "#{inspect(LogTermsHandler)} failed to handle #{identity(event)}, due to: " <>
                 "{:failed, :redacted}"

      assert log =~ "#{inspect(LogTermsHandler)} has requested to stop: {:shutdown, :redacted}"
      assert log =~ "#{inspect(LogTermsHandler)} is shutting down due to {:shutdown, :redacted}"

      refute_leak(log, event)
    end

    test "a raise is named by the exception's module, its stacktrace scrubbed" do
      {log, [event]} = run(LogTermsHandler, [%Raise{marker: @marker}])

      assert log =~
               "#{inspect(LogTermsHandler)} failed to handle #{identity(event)}, due to: " <>
                 "** (RuntimeError)"

      assert log =~ "#{inspect(LogTermsHandler)}.handle/2"

      refute_leak(log, event)
    end

    test "an invalid return value is named by its class" do
      {log, [event]} = run(LogTermsHandler, [%Invalid{marker: @marker}])

      assert log =~
               "#{inspect(LogTermsHandler)} failed to handle #{identity(event)}, `handle/2` " <>
                 "function returned an invalid value: {:bogus, :redacted}, expected `:ok` or " <>
                 "`{:error, term}`"

      refute_leak(log, event)
    end

    test "an invalid error response is named by its class" do
      {log, [event]} = run(LogTermsHandler, [%BadResponse{marker: @marker}])

      assert log =~
               "#{inspect(LogTermsHandler)} returned an invalid error response: " <>
                 "{:bogus_response, :redacted}"

      refute_leak(log, event)
    end
  end

  describe "a handled batch" do
    test "an error reply is named by its class, and the batch by its first and last events" do
      {log, [first, last]} =
        run(LogTermsBatchHandler, [%Fail{marker: @marker}, %Fail{marker: @marker}])

      assert log =~
               "#{inspect(LogTermsBatchHandler)} failed to handle batch from #{identity(first)} " <>
                 "to #{identity(last)} due to: {:failed, :redacted}"

      refute_leak(log, first)
    end

    test "a raise is named by the exception's module, its stacktrace scrubbed" do
      {log, [first, last]} =
        run(
          LogTermsBatchHandler,
          [%Raise{marker: @marker}, %Raise{marker: @marker}]
        )

      assert log =~
               "#{inspect(LogTermsBatchHandler)} failed to handle batch from #{identity(first)} " <>
                 "to #{identity(last)} due to: ** (RuntimeError)"

      assert log =~ "#{inspect(LogTermsBatchHandler)}.handle_batch/1"

      refute_leak(log, first)
    end

    test "an invalid return value is named by its class" do
      {log, [first, last]} =
        run(
          LogTermsBatchHandler,
          [%Invalid{marker: @marker}, %Invalid{marker: @marker}]
        )

      assert log =~
               "#{inspect(LogTermsBatchHandler)} failed to handle batch from #{identity(first)} " <>
                 "to #{identity(last)}, `handle_batch/1` returned an invalid value: " <>
                 "{:bogus, :redacted}, expected `:ok` or `{:error, term}`"

      refute_leak(log, first)
    end
  end

  test "an unexpected message is named by its class" do
    handler = start_supervised!(LogTermsHandler, restart: :temporary)

    log =
      capture_log([level: :debug], fn ->
        send(handler, {:unexpected, @marker})

        # A call after the message: the handler has handled it.
        :sys.get_state(handler)
      end)

    assert log =~
             "#{inspect(LogTermsHandler)} received unexpected message: {:unexpected, :redacted}"

    refute log =~ @marker
  end

  test "a failed subscribe is named by its class" do
    expect(MockEventStore, :subscribe_to_all_streams, fn _name, _subscriber, _opts ->
      {:error, {:refused, @marker}}
    end)

    log =
      capture_log([level: :debug], fn ->
        handler = start_supervised!(LogTermsHandler, restart: :temporary)

        # A call after `handle_continue/2`: the handler has tried to subscribe.
        :sys.get_state(handler)
      end)

    assert log =~
             "#{inspect(LogTermsHandler)} failed to subscribe to event store due to: " <>
               "{:refused, :redacted}, retrying in "

    refute log =~ @marker
  end

  test "a stop requested by `before_reset/0` is named by its class" do
    handler = start_supervised!(ResetStoppingHandler, restart: :temporary)
    ref = Process.monitor(handler)

    log =
      capture_log([level: :debug], fn ->
        send(handler, :reset)

        assert_receive {:DOWN, ^ref, :process, ^handler, {:shutdown, {:reset_stopped, @marker}}}
      end)

    assert log =~
             "#{inspect(ResetStoppingHandler)} `before_reset/0` callback has requested to stop. " <>
               "(reason: {:shutdown, :redacted})"

    refute log =~ @marker
  end

  # Starts `handler`, sends it `events` as one delivery and waits for it to stop. Answers the
  # log and the recorded events.
  defp run(handler_module, events) do
    handler = start_supervised!(handler_module, restart: :temporary)
    ref = Process.monitor(handler)

    recorded = EventFactory.map_to_recorded_events(events)

    log =
      capture_log([level: :debug], fn ->
        send(handler, {:events, recorded})

        assert_receive {:DOWN, ^ref, :process, ^handler, {:shutdown, _reason}}
      end)

    {log, recorded}
  end

  defp identity(%{event_number: number, event_type: type, stream_id: stream_id}) do
    "event ##{number} (#{type}) of #{inspect("..." <> String.slice(stream_id, -8, 8))}"
  end

  defp refute_leak(log, %{stream_id: stream_id}) do
    refute log =~ @marker
    refute log =~ stream_id
  end
end
