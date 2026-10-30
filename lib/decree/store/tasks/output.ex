defmodule Decree.Store.Tasks.Output do
  @moduledoc false

  def write_info(msg, opts) do
    unless opts[:quiet] do
      info(msg, opts[:is_mix])
    end
  end

  defp info(msg, true) do
    Mix.shell().info(msg)
  end

  defp info(msg, _) do
    IO.puts(msg)
  end

  # `quiet` silences output only: a failure always raises, so a quiet task that
  # fails still fails (a Mix task exits non-zero).
  @spec raise_msg(msg :: String.t(), opts :: Keyword.t()) :: no_return()
  def raise_msg(msg, opts) do
    do_raise(msg, opts[:is_mix])
  end

  @spec do_raise(msg :: String.t(), boolean()) :: no_return()
  defp do_raise(msg, true) do
    Mix.raise(msg)
  end

  defp do_raise(msg, _) do
    raise msg
  end
end
