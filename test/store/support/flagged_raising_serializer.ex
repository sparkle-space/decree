defmodule Decree.Store.FlaggedRaisingSerializer do
  @moduledoc false

  # The store's JSON serializer, except that while `raising!/0` is in force an event's
  # `deserialize/2` raises for data carrying `marker/0`, with that data in its message, as a
  # codec that cannot read a payload can (sparkle-space/sparklespace#8357). After
  # `decoding!/0` the same bytes decode, as when the codec is fixed. Metadata always decodes.
  # The flag is global, so only a test that is not async may set it.

  @behaviour Decree.Store.Serializer

  alias Decree.Store.JsonSerializer

  @key {__MODULE__, :raising}
  @marker "flagged-undecodable-marker"

  def marker, do: @marker

  def raising!, do: :persistent_term.put(@key, true)

  def decoding! do
    _erased = :persistent_term.erase(@key)

    :ok
  end

  @impl Decree.Store.Serializer
  def serialize(term), do: JsonSerializer.serialize(term)

  @impl Decree.Store.Serializer
  def deserialize(binary, config) do
    if Keyword.has_key?(config, :type) and :persistent_term.get(@key, false) and
         String.contains?(binary, @marker) do
      raise ArgumentError, "cannot decode " <> binary
    end

    JsonSerializer.deserialize(binary, config)
  end

  @impl Decree.Store.Serializer
  def known_type?(type), do: JsonSerializer.known_type?(type)
end
