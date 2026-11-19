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

# `assert_receive_timeout` here and `assert_receive_event_timeout` below each bound a
# wait for a positive signal: the wait ends on the message, so the bound is spent
# only when a test is failing, and 2s or 1s is shorter than a stall seen on a loaded
# host. The refute timeouts bound a timed absence, spent on every green run.
config :ex_unit,
  assert_receive_timeout: 5_000,
  refute_receive_timeout: 100,
  capture_log: true,
  exclude: [:distributed, :ignore, :manual, :migration]

# Framework test helpers (`Decree.Assertions.EventAssertions`).
# `dispatch_consistency_timeout` bounds a positive signal too: a green `:strong` dispatch
# ends on the acknowledgement, and only the tests that expect the timeout spend it.
# 100 ms timed out on a loaded CI host.
config :decree,
  assert_receive_event_timeout: 5_000,
  refute_receive_event_timeout: 100,
  dispatch_consistency_timeout: 1_000

# Decree's suite exercises named families with these kinds.
config :decree, :tag_index_named_families, ~w(tenant_slug plan_slug email oauth_uid)

# Neither of these takes effect: the two options are read from the `use` line alone, which is
# what `Decree.PersonalFields.KeySourceTest` proves with this entry.
config :decree, Decree.PersonalFields.Fixtures.EnvApp,
  key_source: Decree.PersonalFields.Fixtures.Keys,
  personal_metadata_keys: [:email]

# Store suite (test/store), as upstream EventStore configured it, without its
# `idle_interval: 100`: Postgrex 0.22.4 dropped a notification that arrived during a
# connection's idle ping, so pinging every 100 ms lost about 1 in 150 notifications
# after an idle spell (elixir-ecto/postgrex#795). decree's LISTEN connection now sends no
# idle ping, and `test/store/notifications/notifications_connection_test.exs` proves it.
store_config =
  pg ++
    [
      database: db.("eventstore_test"),
      pool_size: 1,
      # One connection queues every read behind the last: the 50 ms default target
      # drops a checkout on a loaded host, as reading 8,001 events did.
      queue_target: 1_000,
      queue_interval: 5_000,
      serializer: Decree.Store.JsonSerializer,
      subscription_retry_interval: 1_000
    ]

config :decree, TestEventStore, store_config
config :decree, SecondEventStore, Keyword.put(store_config, :database, db.("eventstore_test_2"))
config :decree, SchemaEventStore, store_config

# Conformance suite (test/conformance): the `Decree.EventStore` facade over the store.
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
