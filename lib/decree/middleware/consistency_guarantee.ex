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

  # A decision's batch spans several streams, so every one of them is waited for, and the one
  # timeout is the batch's: a dispatch waits no longer for having written to more streams.
  def after_dispatch(%Pipeline{assigns: %{stream_versions: stream_versions}} = pipeline) do
    deadline = deadline(Subscriptions.default_consistency_timeout())

    Enum.reduce_while(stream_versions, pipeline, fn {stream_uuid, version}, pipeline ->
      case wait_until(pipeline, stream_uuid, version, deadline) do
        {:ok, pipeline} -> {:cont, pipeline}
        {:timeout, pipeline} -> {:halt, pipeline}
      end
    end)
  end

  def after_dispatch(%Pipeline{} = pipeline) do
    %Pipeline{
      assigns: %{aggregate_uuid: aggregate_uuid, aggregate_version: aggregate_version}
    } = pipeline

    timeout = Subscriptions.default_consistency_timeout()

    {_outcome, pipeline} = wait_for(pipeline, aggregate_uuid, aggregate_version, timeout)

    pipeline
  end

  def after_failure(%Pipeline{} = pipeline), do: pipeline

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp wait_until(%Pipeline{} = pipeline, stream_uuid, version, :infinity) do
    wait_for(pipeline, stream_uuid, version, :infinity)
  end

  defp wait_until(%Pipeline{} = pipeline, stream_uuid, version, deadline) do
    case deadline - System.monotonic_time(:millisecond) do
      remaining when remaining > 0 -> wait_for(pipeline, stream_uuid, version, remaining)
      _no_time_left -> timed_out(pipeline, stream_uuid, version)
    end
  end

  defp wait_for(%Pipeline{} = pipeline, stream_uuid, version, timeout) do
    %Pipeline{
      application: application,
      consistency: consistency,
      assigns: %{dispatcher_pid: dispatcher_pid}
    } = pipeline

    opts = [consistency: consistency, exclude: dispatcher_pid]

    case Subscriptions.wait_for(application, stream_uuid, version, opts, timeout) do
      :ok -> {:ok, pipeline}
      {:error, :timeout} -> timed_out(pipeline, stream_uuid, version)
    end
  end

  defp timed_out(%Pipeline{} = pipeline, stream_uuid, version) do
    Logger.warning(fn ->
      "Consistency timeout waiting for aggregate #{inspect(stream_uuid)} at version #{inspect(version)}"
    end)

    {:timeout, respond(pipeline, {:error, :consistency_timeout})}
  end
end
