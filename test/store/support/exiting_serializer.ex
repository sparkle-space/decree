defmodule Decree.Store.ExitingSerializer do
  @moduledoc false

  # The store's JSON serializer, except that two calls exit as a call to a dead process
  # does: `known_type?/1` for `Decree.Store.ExitOnAskEvent`'s type, and an event's
  # `deserialize/2` for data carrying `data_marker/0`. Each exit reason carries a marker, so
  # a test sees whether it reached a log. A store's metadata is deserialized as usual.

  @behaviour Decree.Store.Serializer

  alias Decree.Store.JsonSerializer

  @exit_type "Elixir.Decree.Store.ExitOnAskEvent"
  @data_marker "exit-on-deserialize-marker"
  @type_marker "exit-on-ask-marker"

  def data_marker, do: @data_marker
  def type_marker, do: @type_marker

  @impl Decree.Store.Serializer
  def serialize(term), do: JsonSerializer.serialize(term)

  @impl Decree.Store.Serializer
  def deserialize(binary, config) do
    if Keyword.has_key?(config, :type) and String.contains?(binary, @data_marker) do
      exit({:noproc, {GenServer, :call, [:dead_codec, {:decode, binary}, 5_000]}})
    end

    JsonSerializer.deserialize(binary, config)
  end

  @impl Decree.Store.Serializer
  def known_type?(@exit_type),
    do: exit({:noproc, {GenServer, :call, [:dead_registry, {:known?, @type_marker}, 5_000]}})

  def known_type?(type), do: JsonSerializer.known_type?(type)
end

defmodule Decree.Store.ExitOnAskEvent do
  @moduledoc false

  # A type every instance knows, except that `Decree.Store.ExitingSerializer` exits when
  # asked whether it knows it.
  @derive Jason.Encoder
  defstruct [:event]
end
