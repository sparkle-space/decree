defmodule Decree.Store.Tasks.Init do
  @moduledoc """
  Task to initalize the Decree.Store database
  """

  import Decree.Store.Tasks.Output

  alias Decree.Store.Config
  alias Decree.Store.Storage.Initializer

  @doc """
  Runs task

  ## Parameters

    - config: the parsed Decree.Store config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task
    - quiet: set to `true` to silence output; a failure still raises

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)
    schema = Keyword.fetch!(config, :schema)

    {:ok, conn} =
      config
      |> Config.default_postgrex_opts()
      |> Postgrex.start_link()

    query = """
      SELECT EXISTS (
        SELECT 1
        FROM   information_schema.tables
        WHERE  table_schema = $1
        AND    table_name = 'events'
      )
    """

    case Postgrex.query!(conn, query, [schema]) do
      %{rows: [[true]]} ->
        write_info("The Decree.Store database has already been initialized.", opts)

      %{rows: [[false]]} ->
        Initializer.run!(conn, config)

        write_info("The Decree.Store database has been initialized.", opts)
    end

    true = Process.unlink(conn)
    true = Process.exit(conn, :shutdown)

    :ok
  end
end
