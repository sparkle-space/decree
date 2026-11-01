defmodule Decree.Middleware.ConsistencyGuarantee do
  @moduledoc """
  An internal `Decree.Middleware` that blocks after successful command
  dispatch until the requested dispatch consistency has been met.

  Only applies when the requested consistency is `:strong`. Has no effect for
  `:eventual` consistency.
  """

  @behaviour Decree.Middleware

  require Logger

  alias Decree.Middleware.Pipeline
  alias Decree.Subscriptions

  import Pipeline

  def before_dispatch(%Pipeline{} = pipeline) do
    Pipeline.assign(pipeline, :dispatcher_pid, self())
  end

  def after_dispatch(%Pipeline{consistency: :eventual} = pipeline),
    do: pipeline

  def after_dispatch(%Pipeline{assigns: %{events: []}} = pipeline),
    do: pipeline

  # A decision's batch spans several streams, so every one of them is waited for.
  def after_dispatch(%Pipeline{assigns: %{stream_versions: stream_versions}} = pipeline) do
    Enum.reduce_while(stream_versions, pipeline, fn {stream_uuid, version}, pipeline ->
      case wait_for(pipeline, stream_uuid, version) do
        {:ok, pipeline} -> {:cont, pipeline}
        {:timeout, pipeline} -> {:halt, pipeline}
      end
    end)
  end

  def after_dispatch(%Pipeline{} = pipeline) do
    %Pipeline{
      assigns: %{aggregate_uuid: aggregate_uuid, aggregate_version: aggregate_version}
    } = pipeline

    {_outcome, pipeline} = wait_for(pipeline, aggregate_uuid, aggregate_version)

    pipeline
  end

  def after_failure(%Pipeline{} = pipeline), do: pipeline

  defp wait_for(%Pipeline{} = pipeline, stream_uuid, version) do
    %Pipeline{
      application: application,
      consistency: consistency,
      assigns: %{dispatcher_pid: dispatcher_pid}
    } = pipeline

    opts = [consistency: consistency, exclude: dispatcher_pid]

    case Subscriptions.wait_for(application, stream_uuid, version, opts) do
      :ok ->
        {:ok, pipeline}

      {:error, :timeout} ->
        Logger.warning(fn ->
          "Consistency timeout waiting for aggregate #{inspect(stream_uuid)} at version #{inspect(version)}"
        end)

        {:timeout, respond(pipeline, {:error, :consistency_timeout})}
    end
  end
end
