if Code.ensure_loaded?(Jason) do
  defmodule Decree.Serialization.JsonSerializer do
    @moduledoc """
    A serializer that uses the JSON format and Jason library.
    """

    alias Decree.EventStore.TypeProvider
    alias Decree.Serialization.JsonDecoder
    alias Decree.Serialization.Serializer

    @behaviour Serializer

    @doc """
    Serialize given term to JSON binary data.
    """
    @impl Serializer
    def serialize(term) do
      Jason.encode!(term)
    end

    @doc """
    Deserialize given JSON binary data to the expected type.
    """
    @impl Serializer
    def deserialize(binary, config \\ [])

    def deserialize(binary, config) do
      {type, opts} =
        case Keyword.get(config, :type) do
          nil -> {nil, []}
          type -> {TypeProvider.to_struct(type), [keys: :atoms]}
        end

      binary
      |> Jason.decode!(opts)
      |> to_struct(type)
      |> JsonDecoder.decode()
    end

    @doc """
    Whether the configured `Decree.EventStore.TypeProvider` can build the type, as
    `deserialize/2` asks it to. Used by the event store as its serializer's
    `c:Decree.Store.Serializer.known_type?/1`.

    Answers `false`, so the event holds, when the provider cannot map the type name: when it
    raises `ArgumentError` (the default provider, for a name that is no existing atom),
    `UndefinedFunctionError` (a name that is no struct module) or `FunctionClauseError` (a
    custom provider with clauses only for the names it knows, meeting a newer one). Any
    other exception the provider raises is a crash, not an unknown type, and is raised.
    """
    def known_type?(type) when is_binary(type) do
      _struct = TypeProvider.to_struct(type)

      true
    rescue
      _error in [ArgumentError, UndefinedFunctionError, FunctionClauseError] -> false
    end

    defp to_struct(data, nil), do: data
    defp to_struct(data, struct), do: struct(struct, data)
  end

  require Protocol

  Protocol.derive(Jason.Encoder, Decree.EventStore.SnapshotData)
end
