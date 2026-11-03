defmodule GateBench.MixProject do
  use Mix.Project

  # One fixture, two arms: DECREE_PATH picks main's export or the branch.
  def project do
    [
      app: :gate_bench,
      version: "0.1.0",
      elixir: "~> 1.18",
      deps: [{:decree, path: System.fetch_env!("DECREE_PATH")}, {:jason, "~> 1.4"}],
      deps_path: System.fetch_env!("DECREE_DEPS"),
      lockfile: System.fetch_env!("DECREE_LOCK"),
      build_path: System.fetch_env!("GATE_BUILD"),
      consolidate_protocols: true
    ]
  end

  def application, do: [extra_applications: [:logger]]
end
