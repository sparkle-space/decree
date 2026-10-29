defmodule Decree.Middleware do
  @moduledoc """
  Middleware provides an extension point to add functions that you want to be
  called for every command the router dispatches.

  Examples include command validation, authorization, and logging.

  Implement the `Decree.Middleware` behaviour in your module and define the
  `c:before_dispatch/1`, `c:after_dispatch/1`, and `c:after_failure/1` callback
  functions.

  ## Example middleware

      defmodule NoOpMiddleware do
        @behaviour Decree.Middleware

        alias Decree.Middleware.Pipeline
        import Pipeline

        def before_dispatch(%Pipeline{command: command} = pipeline) do
          pipeline
        end

        def after_dispatch(%Pipeline{command: command} = pipeline) do
          pipeline
        end

        def after_failure(%Pipeline{command: command} = pipeline) do
          pipeline
        end
      end

  Import the `Decree.Middleware.Pipeline` module to access convenience
  functions.

    * `assign/3` - puts a key and value into the `assigns` map
    * `halt/1` - stops execution of further middleware downstream and prevents
      dispatch of the command when used in a `before_dispatch` callback

  """

  alias Decree.Middleware.Pipeline

  @type pipeline :: %Pipeline{}

  @callback before_dispatch(pipeline) :: pipeline
  @callback after_dispatch(pipeline) :: pipeline
  @callback after_failure(pipeline) :: pipeline
end
