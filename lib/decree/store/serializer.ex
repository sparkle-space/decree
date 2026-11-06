defmodule Decree.Store.Serializer do
  @moduledoc """
  Specification of a serializer to convert between an Elixir term and its
  representation in the database.
  """

  @type t :: module
  @type config :: Keyword.t()

  @doc """
  Serialize the given term to a representation that can be stored by the
  database.
  """
  @callback serialize(any) :: binary | map

  @doc """
  Deserialize the given data to the corresponding term.
  """
  @callback deserialize(binary | map, config) :: any

  @doc """
  Whether `deserialize/2` can build an event of the given type: `false` for a type this
  release does not define, such as one appended by a newer release.

  Optional. A serializer that does not implement it knows every type. A read that meets an
  unknown type raises `Decree.Store.UnknownEventTypeError`, and a subscription holds before
  the event instead of delivering past it.
  """
  @callback known_type?(type :: String.t()) :: boolean()

  @optional_callbacks known_type?: 1

  @doc """
  Get the serializer module from the given config for the event store.
  """
  def serializer(event_store, config) do
    case Keyword.fetch(config, :serializer) do
      {:ok, serializer} ->
        serializer

      :error ->
        raise ArgumentError,
          message: "#{inspect(event_store)} configuration expects :serializer to be configured"
    end
  end
end
