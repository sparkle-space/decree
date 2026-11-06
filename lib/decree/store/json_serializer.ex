if Code.ensure_loaded?(Jason) do
  defmodule Decree.Store.JsonSerializer do
    @moduledoc """
    A serializer that uses the JSON format.
    """

    @behaviour Decree.Store.Serializer

    @doc """
    Serialize given term to JSON binary data.
    """
    def serialize(term) do
      Jason.encode!(term)
    end

    @doc """
    Deserialize given JSON binary data to the expected type.
    """
    def deserialize(binary, config) do
      case Keyword.get(config, :type, nil) do
        nil ->
          Jason.decode!(binary)

        type ->
          type
          |> String.to_existing_atom()
          |> struct(Jason.decode!(binary, keys: :atoms!))
      end
    end

    @doc """
    Whether the type names a loaded struct module, the only types `deserialize/2` can build.
    """
    def known_type?(type) when is_binary(type) do
      module = String.to_existing_atom(type)

      Code.ensure_loaded?(module) and function_exported?(module, :__struct__, 0)
    rescue
      ArgumentError -> false
    end
  end
end
