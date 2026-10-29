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

    defp to_struct(data, nil), do: data
    defp to_struct(data, struct), do: struct(struct, data)
  end

  require Protocol

  Protocol.derive(Jason.Encoder, Decree.EventStore.SnapshotData)
end
