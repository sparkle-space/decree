defmodule Decree.HaltingMiddleware do
  @behaviour Decree.Middleware

  import Decree.Middleware.Pipeline

  def before_dispatch(pipeline) do
    halt(pipeline)
  end

  def after_dispatch(pipeline), do: pipeline
  def after_failure(pipeline), do: pipeline
end
