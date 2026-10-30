defmodule Decree.Store.Tasks.Create do
  @moduledoc """
  Task to create the Decree.Store
  """

  import Decree.Store.Tasks.Output

  alias Decree.Store.Storage.Database
  alias Decree.Store.Storage.Schema

  @doc """
  Runs database and schema create task.

  ## Parameters

    - config: the parsed Decree.Store config

  ## Opts

    - is_mix: set to `true` if running as part of a Mix task
    - quiet: set to `true` to silence output; a failure still raises

  """
  def exec(config, opts \\ []) do
    opts = Keyword.merge([is_mix: false, quiet: false], opts)

    case Database.create(config) do
      :ok ->
        write_info("The Decree.Store database has been created.", opts)

      {:error, :already_up} ->
        write_info("The Decree.Store database already exists.", opts)

      {:error, term} ->
        raise_msg(
          "The Decree.Store database couldn't be created, reason given: #{inspect(term)}.",
          opts
        )
    end

    case Schema.create(config) do
      :ok ->
        write_info("The Decree.Store schema has been created.", opts)

      {:error, :already_up} ->
        write_info("The Decree.Store schema already exists.", opts)

      {:error, term} ->
        raise_msg(
          "The Decree.Store schema couldn't be created, reason given: #{inspect(term)}.",
          opts
        )
    end
  end
end
