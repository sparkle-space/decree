defmodule Decree.Middleware.ExtractAggregateIdentity do
  @moduledoc """
  An internal `Decree.Middleware` that extracts the target aggregate's
  identity from the command.

  It runs after every middleware a router registers, and after `Decree.Middleware.CommandId`,
  so it reads the command they leave. It resolves the identity once per dispatch, and assigns
  it twice: `:aggregate_uuid` is the stream name, with any `identify` prefix, and
  `:aggregate_identity` is the same string before the prefix.

  The identity is checked as the command's field, or the route's identity function, gave it,
  before the prefix: it must be a lowercase canonical UUID (`Decree.UUID.canonical?/1`), or,
  on a route declared `opaque: true`, a non-empty binary. Anything else, a missing field and
  nil included, answers `{:error, :invalid_aggregate_identity}` and halts. Nothing converts,
  trims or downcases it, so the stream is named by exactly what the requester sent.
  """

  @behaviour Decree.Middleware

  alias Decree.Middleware.Pipeline
  import Pipeline

  def before_dispatch(%Pipeline{} = pipeline) do
    with {:ok, identity} <- identity(pipeline),
         aggregate_uuid when is_binary(aggregate_uuid) <- prefix(identity, pipeline) do
      pipeline
      |> assign(:aggregate_identity, identity)
      |> assign(:aggregate_uuid, aggregate_uuid)
    else
      {:error, _error} = response ->
        pipeline
        |> respond(response)
        |> halt()
    end
  end

  def after_dispatch(%Pipeline{} = pipeline), do: pipeline

  def after_failure(%Pipeline{} = pipeline), do: pipeline

  # Extract identity using a field in the command.
  defp extract_aggregate_uuid(%Pipeline{command: command, identity: identity})
       when is_atom(identity) do
    Map.get(command, identity)
  end

  # Extract identity using a user defined function.
  defp extract_aggregate_uuid(%Pipeline{command: command, identity: identity})
       when is_function(identity, 1) do
    identity.(command)
  end

  defp extract_aggregate_uuid(%Pipeline{}), do: nil

  # The raw value, before any prefix: canonical, or a non-empty binary on an opaque route.
  defp identity(%Pipeline{identity_opaque: opaque} = pipeline) do
    identity = extract_aggregate_uuid(pipeline)

    if valid_identity?(identity, opaque) do
      {:ok, identity}
    else
      {:error, :invalid_aggregate_identity}
    end
  end

  defp valid_identity?(identity, true), do: is_binary(identity) and identity != ""
  defp valid_identity?(identity, _opaque), do: Decree.UUID.canonical?(identity)

  # No aggregate identity prefix.
  defp prefix(aggregate_uuid, %Pipeline{identity_prefix: identity_prefix})
       when identity_prefix in [nil, ""],
       do: aggregate_uuid

  # Extract identity prefix using a user defined function.
  defp prefix(aggregate_uuid, %Pipeline{identity_prefix: identity_prefix})
       when is_function(identity_prefix, 0) do
    identity_prefix.() <> aggregate_uuid
  end

  # Identity prefix is a string to prepend to aggregate identity.
  defp prefix(aggregate_uuid, %Pipeline{identity_prefix: identity_prefix})
       when is_binary(identity_prefix),
       do: identity_prefix <> aggregate_uuid

  defp prefix(_aggregate_uuid, %Pipeline{}), do: {:error, :invalid_aggregate_identity_prefix}
end
