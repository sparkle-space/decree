defmodule Decree.Event.EventHandlerHardDeleteRecreateTest do
  use Decree.PostgresCase

  # A handler subscribed to one stream, running while the stream is hard deleted and an append
  # to its uuid creates it again from version 1 (sparkle-space/sparklespace#8386). The store's
  # subscription follows the stream and delivers the recreated events from version 1; the
  # handler hands each of them to `handle/2` or `handle_batch/1`, and does not take them for
  # events it has already seen (it remembers the last event number it acknowledged, which was
  # a position in the deleted stream).

  alias Decree.EventStore
  alias Decree.Event.Mapper

  defmodule HardDeleteApp do
    use Decree.Application,
      otp_app: :decree,
      event_store: [
        event_store: Decree.FrameworkEventStore,
        enable_hard_deletes: true
      ],
      pubsub: :local,
      registry: :local

    def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
  end

  defmodule Marked do
    @derive Jason.Encoder
    defstruct [:marker, :reply_to]
  end

  defmodule OneByOneHandler do
    use Decree.Event.Handler,
      application: HardDeleteApp,
      name: __MODULE__,
      subscribe_to: "recreated-one-by-one",
      personal_fields: :opaque

    def handle(%Marked{marker: marker, reply_to: reply_to}, _metadata) do
      send(:erlang.list_to_pid(reply_to), {:handled, __MODULE__, [marker]})

      :ok
    end
  end

  defmodule BatchHandler do
    use Decree.Event.Handler,
      application: HardDeleteApp,
      name: __MODULE__,
      subscribe_to: "recreated-batch",
      batch_size: 10,
      personal_fields: :opaque

    def handle_batch([{%Marked{reply_to: reply_to}, _metadata} | _rest] = events) do
      markers = Enum.map(events, fn {%Marked{marker: marker}, _metadata} -> marker end)

      send(:erlang.list_to_pid(reply_to), {:handled, __MODULE__, markers})

      :ok
    end
  end

  defmodule BufferingHandler do
    use Decree.Event.Handler,
      application: HardDeleteApp,
      name: __MODULE__,
      subscribe_to: "recreated-buffering",
      batch_size: 10,
      batch_timeout: 1_500,
      personal_fields: :opaque

    def handle_batch([{%Marked{reply_to: reply_to}, _metadata} | _rest] = events) do
      markers = Enum.map(events, fn {%Marked{marker: marker}, _metadata} -> marker end)

      send(:erlang.list_to_pid(reply_to), {:handled, __MODULE__, markers})

      :ok
    end
  end

  @store HardDeleteApp.FrameworkEventStore

  setup do
    start_supervised!(HardDeleteApp)

    :ok
  end

  describe "a handler subscribed to a stream that is hard deleted and recreated" do
    test "at concurrency 1 (the default), handles every recreated event" do
      start_supervised!(OneByOneHandler)
      stream = "recreated-one-by-one"

      append(stream, [1, 2, 3])
      assert handled(OneByOneHandler, 3) == [1, 2, 3]

      hard_delete_and_recreate(stream, [101, 102])

      got = handled(OneByOneHandler, 2)

      assert got == [101, 102],
             "concurrency 1: expected the recreated events [101, 102] to be handled, " <>
               "handled #{inspect(got, charlists: :as_lists)}"
    end

    test "batching (`batch_size:`), hands every recreated event to handle_batch/1" do
      start_supervised!(BatchHandler)
      stream = "recreated-batch"

      append(stream, [1, 2, 3])
      assert handled(BatchHandler, 3) == [1, 2, 3]

      hard_delete_and_recreate(stream, [101, 102])

      got = handled(BatchHandler, 2)

      assert got == [101, 102],
             "batch_size: expected the recreated events [101, 102] to be handled, " <>
               "handled #{inspect(got, charlists: :as_lists)}"
    end

    # The deleted stream's events are still in the handler's batch buffer when the stream is
    # recreated: they are dropped unhandled, as a lost lock drops them, and the recreated
    # events are handled.
    test "batching with a batch timeout, drops the deleted stream's buffered events" do
      start_supervised!(BufferingHandler)
      stream = "recreated-buffering"

      append(stream, [1, 2, 3])
      hard_delete_and_recreate(stream, [101, 102])

      got = handled(BufferingHandler, 2, 5_000)

      assert got == [101, 102],
             "batch_timeout: expected only the recreated events [101, 102] to be handled, " <>
               "handled #{inspect(got, charlists: :as_lists)}"

      refute_receive {:handled, BufferingHandler, _markers}, 2_000
    end
  end

  defp append(stream, markers, expected_version \\ :any_version) do
    reply_to = :erlang.pid_to_list(self())

    events =
      markers
      |> Enum.map(&%Marked{marker: &1, reply_to: reply_to})
      |> Mapper.map_to_event_data()

    :ok = EventStore.append_to_stream(HardDeleteApp, stream, expected_version, events)
  end

  defp hard_delete_and_recreate(stream, markers) do
    :ok = Decree.FrameworkEventStore.delete_stream(stream, :any_version, :hard, name: @store)

    append(stream, markers, 0)
  end

  # The markers `handler` handled, in order, until `count` arrived or `timeout` passed
  # without one.
  defp handled(handler, count, timeout \\ 2_000, acc \\ [])

  defp handled(_handler, count, _timeout, acc) when count <= 0, do: acc

  defp handled(handler, count, timeout, acc) do
    receive do
      {:handled, ^handler, markers} ->
        handled(handler, count - length(markers), timeout, acc ++ markers)
    after
      timeout -> acc
    end
  end
end
