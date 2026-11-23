defmodule Decree.MixProject do
  use Mix.Project

  @version "0.2.1"
  @source_url "https://github.com/sparkle-space/decree"

  def project do
    [
      app: :decree,
      version: @version,
      elixir: "~> 1.20",
      elixirc_paths: elixirc_paths(Mix.env()),
      # A warning in decree's own code fails the test build: lib/ and the support modules here,
      # the test files through the `test` alias. Not in other envs, so a consumer's build of
      # decree never fails on a newer compiler's warning.
      elixirc_options: [warnings_as_errors: Mix.env() == :test],
      test_paths: [
        "test/framework",
        "test/store",
        "test/conformance",
        "test/projections",
        "test/release"
      ],
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
      description: description(),
      package: package(),
      docs: docs(),
      source_url: @source_url
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
      "test/framework/decision/support",
      "test/framework/event/support",
      "test/framework/event_store/support",
      "test/framework/example_domain",
      "test/framework/middleware/support",
      "test/framework/registration/support",
      "test/framework/verify/support",
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
      # Below 0.23 until a released 0.23 passes the notification fences. decree's own
      # LISTEN connection stands in for the unreleased idle-ping fix, temporarily
      # (UPSTREAM.md, Dependency pins; sparkle-space/sparklespace#8532).
      {:postgrex, "~> 0.22.4"},

      # Framework layer
      {:backoff, "~> 1.1"},
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:telemetry_registry, "~> 0.2 or ~> 0.3"},

      # Ecto projections
      {:ecto, "~> 3.11"},
      {:ecto_sql, "~> 3.11"},

      # Optional dependencies
      {:jason, "~> 1.4", optional: true},

      # Test tooling
      {:mox, "~> 1.1", only: :test},

      # Docs
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},

      # CI's Deps Audit job runs `mix deps.audit`
      {:mix_audit, "~> 2.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp description do
    "An event-sourcing toolkit: a Postgres event store and a CQRS framework with decision " <>
      "rules, conditions an append checks across streams. Derived from Commanded and EventStore."
  end

  # NOTICE carries the upstream MIT notices, so it ships with LICENSE.
  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/main/CHANGELOG.md"
      },
      files: ~w(lib priv mix.exs .formatter.exs README.md CHANGELOG.md LICENSE NOTICE)
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: ["README.md", "CHANGELOG.md"],
      # Internal modules and functions the docs name without documenting them.
      skip_code_autolink_to: [
        "Decree.Decision.Dispatch.shape/1",
        "Decree.Event.Tags.impl_for/1",
        "Decree.Store.Application",
        "Decree.Store.LogTerms.reduce/1",
        "Decree.Store.Subscriptions.GeneratedFsm",
        "Decree.Store.Subscriptions.max_head_check_interval/0",
        "Decree.Store.Supervisor"
      ],
      groups_for_modules: [
        Store: [~r/^Decree\.Store(\.|$)/],
        Framework: [~r/^Decree(\.|$)/]
      ]
    ]
  end

  # `test` creates, initialises and migrates every test event store (config/test.exs
  # `event_stores:`), creates the projections repo's database, then runs the five suites. A
  # warning in a test file fails the run; CI's `mix test --include migration` keeps the flag.
  defp aliases do
    [
      "decree.store.setup": ["decree.store.create", "decree.store.init"],
      "decree.store.reset": ["decree.store.drop", "decree.store.setup"],
      test: [
        "decree.store.create --quiet",
        "decree.store.init --quiet",
        "decree.store.migrate --quiet",
        "ecto.create --quiet",
        "test --warnings-as-errors"
      ]
    ]
  end
end
