import Config

# Every suite runs against Postgres. Host and port come from the environment so a
# developer can point the suite at their own instance; CI uses the defaults.
pg = [
  username: System.get_env("DECREE_PG_USER", "postgres"),
  password: System.get_env("DECREE_PG_PASSWORD", "postgres"),
  hostname: System.get_env("DECREE_PG_HOST", "localhost"),
  port: String.to_integer(System.get_env("DECREE_PG_PORT", "5432"))
]

# A partitioned CI run (`MIX_TEST_PARTITION`) gets its own databases.
db = fn name -> name <> System.get_env("MIX_TEST_PARTITION", "") end

config :logger, level: :debug
config :logger, :default_handler, false

config :ex_unit,
  assert_receive_timeout: 2_000,
  refute_receive_timeout: 100,
  capture_log: true,
  exclude: [:distributed, :ignore, :manual, :migration]

# Framework test helpers (`Decree.Assertions.EventAssertions`).
config :decree,
  assert_receive_event_timeout: 1_000,
  refute_receive_event_timeout: 100,
  dispatch_consistency_timeout: 100

# Store suite (test/store), as upstream EventStore configured it.
store_config =
  pg ++
    [
      idle_interval: 100,
      database: db.("eventstore_test"),
      pool_size: 1,
      serializer: Decree.Store.JsonSerializer,
      subscription_retry_interval: 1_000
    ]

config :decree, TestEventStore, store_config
config :decree, SecondEventStore, Keyword.put(store_config, :database, db.("eventstore_test_2"))
config :decree, SchemaEventStore, store_config

# Conformance suite (test/conformance): the framework adapter over the store.
config :decree,
       ConformanceEventStore,
       pg ++
         [
           database: db.("decree_conformance_test"),
           serializer: Decree.Serialization.JsonSerializer,
           pool_size: 5,
           pool_overflow: 0
         ]

# Framework and projections suites: every test application's event store.
config :decree,
       Decree.FrameworkEventStore,
       pg ++
         [
           database: db.("decree_framework_test"),
           serializer: Decree.Serialization.JsonSerializer,
           pool_size: 2
         ]

config :decree,
  event_stores: [
    TestEventStore,
    SecondEventStore,
    SchemaEventStore,
    ConformanceEventStore,
    Decree.FrameworkEventStore
  ]

framework_app_config = [
  event_store: [
    adapter: Decree.EventStore.Adapters.EventStore,
    event_store: Decree.FrameworkEventStore
  ],
  pubsub: :local,
  registry: :local
]

config :decree, Decree.Commands.ConsistencyApp, framework_app_config
config :decree, Decree.DefaultApp, []
config :decree, Decree.Middleware.TenantApp, framework_app_config

# Projections suite (test/projections).
config :decree,
  ecto_repos: [Decree.Projections.Repo],
  repo: Decree.Projections.Repo

config :decree,
       Decree.Projections.Repo,
       pg ++
         [
           database: db.("decree_projections_test"),
           pool: Ecto.Adapters.SQL.Sandbox
         ]
