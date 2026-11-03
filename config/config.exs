import Config

config :logger, level: :warning

config :gate_bench, Gate.Store,
  serializer: Decree.Store.JsonSerializer,
  column_data_type: "bytea",
  password: System.get_env("GATE_PG_PASSWORD", "postgres"),
  hostname: System.get_env("GATE_PG_HOST", "127.0.0.1"),
  username: System.get_env("GATE_PG_USER", "postgres"),
  port: String.to_integer(System.get_env("GATE_PG_PORT", "5432")),
  database: System.get_env("GATE_DB", "gate"),
  pool_size: 2,
  conditional_appends: [stream_prefixes: ["gd-"], targets: [Gate.UserAgg]]

config :gate_bench, event_stores: [Gate.Store]

config :gate_bench, Gate.App,
  event_store: [adapter: Decree.EventStore.Adapters.EventStore, event_store: Gate.Store],
  pubsub: :local,
  registry: :local

config :gate_bench, Gate.NoopApp,
  event_store: [adapter: Gate.NoopAdapter],
  pubsub: :local,
  registry: :local
