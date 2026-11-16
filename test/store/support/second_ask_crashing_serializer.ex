defmodule Decree.Store.SecondAskCrashingSerializer do
  @moduledoc false

  # The store's JSON serializer, except that `known_type?/1` answers its first call and
  # raises on every later one, with the call's ordinal in the message, so a test sees which
  # ask crashed. A type provider cannot stand in for it: the framework serializer's
  # `deserialize/2` asks the provider too. Arm it with `arm!/0`; the count is global, so only
  # a test that is not async may arm it.

  @behaviour Decree.Store.Serializer

  alias Decree.Store.JsonSerializer

  @key {__MODULE__, :calls}

  # Starts the count for the rest of the calling test, erasing it when the test exits.
  def arm! do
    :persistent_term.put(@key, :counters.new(1, [:atomics]))

    ExUnit.Callbacks.on_exit(fn -> :persistent_term.erase(@key) end)
  end

  @impl Decree.Store.Serializer
  def serialize(term), do: JsonSerializer.serialize(term)

  @impl Decree.Store.Serializer
  def deserialize(binary, config), do: JsonSerializer.deserialize(binary, config)

  @impl Decree.Store.Serializer
  def known_type?(type) do
    calls = :persistent_term.get(@key)
    :ok = :counters.add(calls, 1, 1)

    case :counters.get(calls, 1) do
      1 -> JsonSerializer.known_type?(type)
      call -> raise "known_type? crashed on call #{call}"
    end
  end
end
