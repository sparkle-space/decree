defmodule Decree.Store.DenyListSerializer do
  @moduledoc false

  # "Old code" for the unknown-event-type tests (sparkle-space/sparklespace#7804): the
  # store's JSON serializer, except that the types on its deny list are ones this release
  # does not know. For those it answers `known_type?/1` false AND raises on `deserialize/2`,
  # as `Decree.Store.JsonSerializer` does for a module that is not defined, so a path that
  # forgot to ask would crash as old code does. An event store instance started with it
  # stands for an old pod; one with the plain serializer, on the same database, for a new
  # one. The list is global, so only a test that is not async may set it.

  @behaviour Decree.Store.Serializer

  alias Decree.Store.JsonSerializer

  @key {__MODULE__, :denied}

  def deny(types) when is_list(types), do: :persistent_term.put(@key, types)

  def allow_all do
    _erased = :persistent_term.erase(@key)

    :ok
  end

  @impl Decree.Store.Serializer
  def serialize(term), do: JsonSerializer.serialize(term)

  @impl Decree.Store.Serializer
  def deserialize(binary, config) do
    case Keyword.get(config, :type) do
      nil ->
        JsonSerializer.deserialize(binary, config)

      type ->
        if denied?(type) do
          raise ArgumentError, "errors were found at the given arguments: not an atom"
        end

        JsonSerializer.deserialize(binary, config)
    end
  end

  @impl Decree.Store.Serializer
  def known_type?(type), do: not denied?(type) and JsonSerializer.known_type?(type)

  defp denied?(type), do: type in :persistent_term.get(@key, [])
end

defmodule Decree.Store.NewReleaseEvent do
  @moduledoc false

  # An event type a newer release introduced: denied to the "old" instance in the
  # unknown-event-type tests.
  @derive Jason.Encoder
  defstruct [:event]
end
