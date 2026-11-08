defmodule Decree.Middleware.CommandId do
  @moduledoc """
  An internal `Decree.Middleware` that refuses a command whose `command_id` is not a
  lowercase canonical UUID.

  It is a default middleware, like `Decree.Middleware.ExtractAggregateIdentity`: it runs
  after every middleware a router registers and before the identity is extracted, on an
  aggregate route and a decision route alike. It answers `Decree.Command.check_id/1` on the
  command's `:command_id` field, a missing field being nil, and halts on a refusal, so no
  aggregate is opened and no decision runs. It never reads the dispatch's `command_uuid`, and
  never writes the command.
  """

  @behaviour Decree.Middleware

  alias Decree.Middleware.Pipeline
  import Pipeline

  def before_dispatch(%Pipeline{command: command} = pipeline) do
    case Decree.Command.check_id(Map.get(command, :command_id)) do
      :ok ->
        pipeline

      {:error, _reason} = refusal ->
        pipeline
        |> respond(refusal)
        |> halt()
    end
  end

  def after_dispatch(%Pipeline{} = pipeline), do: pipeline

  def after_failure(%Pipeline{} = pipeline), do: pipeline
end
