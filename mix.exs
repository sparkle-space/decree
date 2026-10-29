defmodule Decree.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :decree,
      version: @version,
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: ["test/framework", "test/store", "test/conformance", "test/projections"],
      test_ignore_filters: [
        &String.contains?(&1, "/support/"),
        &String.starts_with?(&1, "test/framework/helpers/"),
        &String.starts_with?(&1, "test/framework/example_domain/"),
        &String.starts_with?(&1, "test/store/manual/"),
        &String.starts_with?(&1, "test/store/fixture/"),
        &String.starts_with?(&1, "test/projections/migrations/")
      ],
      deps: deps(),
      aliases: aliases(),
      start_permanent: Mix.env() == :prod,
      consolidate_protocols: Mix.env() != :test,
      name: "decree",
      source_url: "https://github.com/sparkle-space/decree"
    ]
  end

  # One OTP application: `Decree.start/2` starts the store's processes, then the
  # framework's.
  def application do
    [
      extra_applications: [:crypto, :eex, :logger, :ssl],
      mod: {Decree, []}
    ]
  end

  # The framework test helpers (`Decree.Helpers.*`, `Decree.Assertions.EventAssertions`,
  # `Decree.AggregateCase`) and every suite's support modules compile only in :test.
  defp elixirc_paths(:test) do
    [
      "lib",
      "test/framework/helpers",
      "test/framework/support",
      "test/framework/aggregates/support",
      "test/framework/application/support",
      "test/framework/commands/support",
      "test/framework/event/support",
      "test/framework/event_store/support",
      "test/framework/example_domain",
      "test/framework/middleware/support",
      "test/framework/pubsub/support",
      "test/framework/registration/support",
      "test/store/support",
      "test/store/subscriptions/support",
      "test/conformance/support",
      "test/projections/support"
    ]
  end

  defp elixirc_paths(_env), do: ["lib"]

  defp deps do
    [
      # Store layer
      {:fsm, "~> 0.3"},
      {:gen_stage, "~> 1.2"},
      {:postgrex, "~> 0.17"},

      # Framework layer
      {:backoff, "~> 1.1"},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:telemetry_registry, "~> 0.2 or ~> 0.3"},

      # Ecto projections
      {:ecto, "~> 3.11"},
      {:ecto_sql, "~> 3.11"},

      # Optional dependencies
      {:jason, "~> 1.4", optional: true},
      {:phoenix_pubsub, "~> 2.1", optional: true},

      # Test tooling
      {:mox, "~> 1.1", only: :test}
    ]
  end

  # `test` creates and initialises every test event store (config/test.exs
  # `event_stores:`) and the projections repo's database, then runs the four suites.
  defp aliases do
    [
      "decree.store.setup": ["decree.store.create", "decree.store.init"],
      "decree.store.reset": ["decree.store.drop", "decree.store.setup"],
      test: [
        "decree.store.create --quiet",
        "decree.store.init --quiet",
        "ecto.create --quiet",
        "test"
      ]
    ]
  end
end
