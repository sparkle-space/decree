defmodule Decree.Event.EventHandlerPartitionLogTermsTest do
  use Decree.PostgresCase

  # A raise in a handler's `partition_by/2` is logged by the exception's module and a scrubbed
  # stacktrace, never the exception's message or the event it was given
  # (sparkle-space/sparklespace#8358). The event still goes to partition 1 and is handled.

  import ExUnit.CaptureLog

  alias Decree.EventStore
  alias Decree.EventStore.EventData

  @marker "partition-log-terms-marker"
  @stream "x8358-partition-log-terms-5be1d0c4"

  defmodule Partitioned do
    @derive Jason.Encoder
    defstruct [:marker]
  end

  defmodule PartitionRaisingHandler do
    @moduledoc false

    use Decree.Event.Handler,
      application: Decree.DefaultApp,
      name: __MODULE__,
      subscribe_to: "x8358-partition-log-terms-5be1d0c4"

    alias Decree.Event.EventHandlerPartitionLogTermsTest.Partitioned

    def partition_by(%Partitioned{marker: marker}, _metadata), do: raise("partition " <> marker)

    def handle(%Partitioned{}, %{event_number: event_number}) do
      send(:event_handler_partition_log_terms_test, {:handled, event_number})

      :ok
    end
  end

  test "a raise in `partition_by/2` is named by the exception's module" do
    Process.register(self(), :event_handler_partition_log_terms_test)

    log =
      capture_log([level: :debug], fn ->
        start_supervised!(Decree.DefaultApp)
        start_supervised!({PartitionRaisingHandler, start_from: :origin})

        :ok =
          EventStore.append_to_stream(Decree.DefaultApp, @stream, 0, [
            %EventData{
              event_type: Atom.to_string(Partitioned),
              data: %Partitioned{marker: @marker},
              metadata: %{}
            }
          ])

        assert_receive {:handled, 1}, 5_000
      end)

    assert log =~
             "#{inspect(PartitionRaisingHandler)}.partition_by/2 raised, partitioning by 1: " <>
               "** (RuntimeError)"

    assert log =~ "event_handler_partition_log_terms_test.exs"

    refute log =~ @marker
    refute log =~ @stream
  end
end
