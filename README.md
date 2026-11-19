# decree

An event-sourcing toolkit for Elixir: a Postgres event store (`Decree.Store`) and a CQRS framework (`Decree`) on top of it, with decision rules: conditions an append checks across streams, such as "one account per handle".

decree is built for sparkle.space and for the projects sparkle.space produces. That is its first purpose. If it is useful to you too, you are welcome to use it, and we hold it to the same standard either way.

## Status

decree is pre-1.0 (version 0.1.0). Until 1.0, a minor version (0.2.0) may change the API or the store schema; a patch version (0.1.1) does neither. `CHANGELOG.md` records each release and how to upgrade, and a commit on `main` between releases promises nothing.

It has one production user, sparkle.space. What that use proves, and what it does not:

- **In production:** the Postgres store and its migrations, aggregates and snapshots, the command router and middleware, event handlers, Ecto projections, upcasting, personal fields with per-subject keys, and decision rules on an aggregate's own stream (`Decree.Decision.Rule`).
- **Built and tested, with no production user yet:** decisions that own their streams (`Decree.Decision`), the multi-stream conditional append, the command window (`Decree.Aggregate`, "Command window"), `mix decree.verify.eventless`, and `:strong` dispatch consistency.

## Installation

Add decree from Hex, with [Jason](https://hex.pm/packages/jason) for the JSON serializer:

```elixir
# mix.exs
defp deps do
  [
    {:decree, "~> 0.1.0"},
    {:jason, "~> 1.4"}
  ]
end
```

`~> 0.1.0` takes patch versions only, since a minor version may break (Status). To format the router's `dispatch` and `identify` without parentheses, add `:decree` to `import_deps` in your `.formatter.exs`.

### Compatibility

decree needs Elixir 1.20 or later. CI tests one combination: Elixir 1.20.4 on Erlang/OTP 29.1.2, against PostgreSQL 18. Other versions of Erlang/OTP and PostgreSQL are not tested.

## A minimal application

One aggregate with a decision rule, a router, an application and an event handler. An account claims a handle, and the rule refuses a handle another account holds. The test `test/framework/readme_example_test.exs` compiles and runs the three blocks below from this file.

```elixir
# config/config.exs
import Config

config :my_app, event_stores: [MyApp.EventStore]

config :my_app, MyApp.EventStore,
  serializer: Decree.Serialization.JsonSerializer,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "my_app_eventstore",
  conditional_appends: [targets: [MyApp.Account]]

config :my_app, MyApp.App,
  event_store: [event_store: MyApp.EventStore],
  pubsub: :local,
  registry: :local
```

```elixir
# lib/my_app.ex
defmodule MyApp.EventStore do
  use Decree.Store, otp_app: :my_app
end

defmodule MyApp.RegisterAccount do
  use Decree.Command

  defstruct [:command_id, :account_id, :handle, :rule_result]
end

defmodule MyApp.AccountRegistered do
  @derive Jason.Encoder
  defstruct [:account_id, :handle]

  defimpl Decree.Event.Tags do
    def tags(%{handle: handle}, _context), do: {:ok, ["handle:" <> handle]}
  end
end

defmodule MyApp.UniqueHandle do
  use Decree.Decision.Rule

  @impl Decree.Decision.Rule
  def query(%MyApp.RegisterAccount{handle: handle}, _tag_context) do
    [[types: ["Elixir.MyApp.AccountRegistered"], tags: ["handle:" <> handle]]]
  end
end

defmodule MyApp.Account do
  alias Decree.Decision.Rule.Result
  alias MyApp.{AccountRegistered, RegisterAccount}

  defstruct [:account_id, :handle]

  def execute(
        %__MODULE__{account_id: nil},
        %RegisterAccount{rule_result: %Result{events: []}} = command
      ) do
    %AccountRegistered{account_id: command.account_id, handle: command.handle}
  end

  def execute(%__MODULE__{account_id: nil}, %RegisterAccount{}), do: {:error, :handle_taken}
  def execute(%__MODULE__{}, %RegisterAccount{}), do: {:error, :already_registered}

  def apply(%__MODULE__{} = account, %AccountRegistered{} = event) do
    %__MODULE__{account | account_id: event.account_id, handle: event.handle}
  end
end

defmodule MyApp.EnableTagIndex do
  use Decree.Command

  defstruct [:command_id, :ledger_id, :family]
end

defmodule MyApp.TagIndexEnabled do
  @derive Jason.Encoder
  defstruct [:family]

  defimpl Decree.Event.Tags do
    alias Decree.Store.TagIndex

    def tags(%{family: family}, _context) do
      {:ok, [TagIndex.fact_tag(), TagIndex.state_tag(family, nil, :enabled)]}
    end
  end
end

defmodule MyApp.TagIndexLedger do
  defstruct []

  def execute(%__MODULE__{}, %MyApp.EnableTagIndex{family: family}) do
    %MyApp.TagIndexEnabled{family: family}
  end

  def apply(%__MODULE__{} = ledger, %MyApp.TagIndexEnabled{}), do: ledger
end

defmodule MyApp.Router do
  use Decree.Commands.Router

  identify MyApp.Account, by: :account_id, prefix: "account-"
  identify MyApp.TagIndexLedger, by: :ledger_id, prefix: "tag-index-"

  dispatch MyApp.RegisterAccount, to: MyApp.Account, rule: MyApp.UniqueHandle
  dispatch MyApp.EnableTagIndex, to: MyApp.TagIndexLedger
end

defmodule MyApp.App do
  use Decree.Application, otp_app: :my_app

  router MyApp.Router
end

defmodule MyApp.Welcome do
  use Decree.Event.Handler, application: MyApp.App, name: "welcome"

  require Logger

  def handle(%MyApp.AccountRegistered{handle: handle}, _metadata) do
    Logger.info("welcome, @" <> handle)
  end
end
```

Create the store's database and tables, then start `MyApp.App` and `MyApp.Welcome` under your application's supervisor:

```bash
mix decree.store.create
mix decree.store.init
```

```elixir
# On a fresh store, before anything else is appended
:ok =
  MyApp.App.dispatch(%MyApp.EnableTagIndex{
    command_id: Decree.UUID.uuid4(),
    ledger_id: Decree.UUID.uuid4(),
    family: "entity"
  })

register = fn handle ->
  MyApp.App.dispatch(%MyApp.RegisterAccount{
    command_id: Decree.UUID.uuid4(),
    account_id: Decree.UUID.uuid4(),
    handle: handle
  })
end

:ok = register.("ada")
{:error, :handle_taken} = register.("ada")
:ok = register.("grace")
```

`MyApp.Welcome` logs "welcome, @ada" and "welcome, @grace".

Every command carries a `command_id`, a lowercase canonical UUID its sender mints: a dispatch refuses a command without one (`Decree.Command`). What the rule route needs, beyond a plain one:

- **The store must declare the aggregate** in `conditional_appends: [targets: ...]`. The application refuses to start when a rule route's aggregate is missing there.
- **The command needs a `:rule_result` field**, where `execute/2` finds what the rule's query matched.
- **The tag index must be enabled.** A condition finds events by their tags, so it holds only while the tag rows are complete, and the application records that as facts on the ledger. decree ships the grammar of those facts (`Decree.Store.TagIndex`), not an event or a command for them; `MyApp.TagIndexEnabled` and `MyApp.TagIndexLedger` above are the smallest version. The `handle` tag is in the `entity` family. Until its fact is appended, the rule's dispatch answers `{:error, :tag_index_not_enabled}`. Enable it on an empty store, so every later event is covered.

`Decree.Decision.Rule` covers the rest: a follow-up query, keys, bounds, retries and the required tags.

## Layers

One OTP application, `:decree`, in two layers:

- **Store** (`Decree.Store`, `Decree.Store.*`, `Mix.Decree.Store`, `Mix.Tasks.Decree.Store.*`; `lib/decree/store/`, `lib/mix/decree/`, `lib/mix/tasks/decree.store.*`): the Postgres event store. Streams, append with expected version, snapshots, persistent and transient subscriptions with acknowledgement and partitioning, schemas (prefixes), and the Mix tasks `decree.store.create`, `decree.store.init`, `decree.store.drop`, `decree.store.migrate` and `decree.store.migrations`.
- **Framework** (`Decree`, `Decree.*` outside `Decree.Store`): applications, command routing and dispatch, aggregates, decisions (`Decree.Decision`), event handlers, middleware, `Decree.EventStore`, the facade an application reaches its `Decree.Store` module through (named by `event_store: [event_store: MyApp.EventStore]`), and Ecto read-model projections (`Decree.Projections.Ecto`), and the Mix task `decree.verify.eventless` (`Mix.Tasks.Decree.Verify.Eventless` over `Decree.Verify.Eventless`).

`mix decree.verify.eventless --router MyApp.Router` fails on any routed handler arm that can accept a command without appending an event: an aggregate handler answering `[]`, `nil`, `:ok`, `{:ok, []}` or `{:ok, nil}`, which the aggregate accepts with no events, and a decision's `decide/2` answering those or `{:streams, []}`, which a dispatch refuses at runtime. It reads each router's registry (`__route__/1` names every routed module and function), walks their source under the project's `elixirc_paths` without compiling or loading it, and reports what it cannot classify as a finding too. It has no allow-list and no waiver; `--router` repeats, and `--no-compile` skips `mix compile`. `Decree.Verify.Eventless` documents the shapes, what the walk follows and its stated residuals.

Being one OTP application, the store's supervision tree (the supervisor registered as `Decree.Store.Supervisor`, started by `Decree.Store.Application`) is a child of `Decree.Supervisor` and shares its restart budget. Upstream, `:eventstore` was a separate application that stopped on its own when its supervisor exhausted its restart budget; in decree the failure escalates to `Decree.Supervisor`, and exhausting that budget stops `:decree`. The children, their names and their order are upstream's.

The store never references the framework. `scripts/check_layers.exs` enforces this from the compiled BEAM files and CI runs it:

```bash
mix run --no-start scripts/check_layers.exs
```

What is absent, on purpose:

- **No process managers.** Multi-step flows are event handlers that dispatch commands. The toolkit has no process-manager module, by design.
- **No in-memory event store.** Every test and every deployment runs on Postgres.

## Running the tests

The suites need a Postgres server (stock Postgres 18). Host, port and credentials come from the environment, defaulting to `localhost:5432`, user and password `postgres`:

```bash
export DECREE_PG_HOST=127.0.0.1 DECREE_PG_PORT=5432   # DECREE_PG_USER, DECREE_PG_PASSWORD
mise install
mix deps.get
mix test
```

CI runs the same suites on stock Postgres, with no durability options. Turning off `fsync`, `synchronous_commit` and `full_page_writes` on CI's Linux runners was measured on 2026-10-29: six runs of each averaged 117.5 s stock and 118.0 s tuned. That is no gain, so the options were not kept. Almost all of the suite's time is synchronous tests, not flushes.

The `test` alias creates and initialises every test event store (`decree.store.create`, `decree.store.init`) and the projections repo's database (`ecto.create`), then runs four suites in one VM:

| Suite | Path | Covers |
|---|---|---|
| framework | `test/framework` | Commanded's suite, on the Postgres store: applications, routing, dispatch, aggregates, snapshots, event handlers, middleware, upcasting, telemetry |
| store | `test/store` | EventStore's suite: storage, streams, subscriptions, notifications, advisory locks, schemas, config |
| conformance | `test/conformance` | the store conformance suite (below) |
| projections | `test/projections` | Commanded Ecto projections: projection versions, schema prefixes, error and after-update callbacks |

Framework and projections tests run their applications on `Decree.FrameworkEventStore` (or a test store that wraps it) over the `decree_framework_test` database; `Decree.PostgresCase` empties it before each test. Tests tagged `:migration` (they restore old database dumps with `pg_restore` and compare schemas with `pg_dump`), `:manual`, `:ignore` and `:distributed` are excluded. CI installs the Postgres 18 client and runs `mix test --include migration`; run the same locally when a Postgres 18 `pg_restore` and `pg_dump` are on `PATH`. `DROPPED_TESTS.md` lists every upstream test not carried over, and why.

## The conformance suite

`test/conformance` runs Commanded's shared event store test cases (`test/framework/event_store/support/*_test_case.ex`) through the `Decree.EventStore` facade of a started application, against its `Decree.Store` module on Postgres. It is the conformance suite for the one store implementation, and covers:

- append and expected version: `:any_version`, `:no_stream`, `:stream_exists` and an exact version, with the wrong-version errors; reading a stream forward, in batches, and the unknown-stream error;
- snapshots: record, read, read when absent, delete;
- transient subscriptions to one stream or all streams;
- persistent subscriptions to one stream or all streams: from the origin or the current position, selectors, duplicate-subscription refusal;
- delivery and redelivery: resuming from the checkpoint and from the last acknowledged event, unsubscribe and resubscribe, delete and recreate;
- acknowledgement and concurrency: several subscribers on one subscription, the concurrency limit, distribution, partitioning by a function, a stopped subscriber dropping out;
- stream prefixes: two event stores in separate schemas of one database, each seeing only its own streams.

## Formatting and compile checks

```bash
mix format --check-formatted
mix compile --warnings-as-errors
```

`Decree.Store.Subscriptions.GeneratedFsm` wraps the `fsm` dependency's `use Fsm`, marking its expansion as generated code so Elixir 1.20's type checker does not report warnings from the dependency's macro against decree.

## Dependency audit

CI's Deps Audit job fails the build on either check:

```bash
mix hex.audit
mix deps.audit
```

`mix hex.audit` reports retired Hex packages and, from Hex 2.5, packages with security advisories. `mix deps.audit` (`mix_audit`, a dev and test dependency) checks `mix.lock` against its own advisory list. The two lists differ, so CI runs both. `mix_audit` ignores a failed clone of its advisory repository and then reports no vulnerabilities, so the CI step also fails when the clone left no advisories. Neither audit scans a git dependency; `UPSTREAM.md` (Dependency pins) lists those.

## Provenance and licence

decree is derived from [Commanded](https://github.com/commanded/commanded), [EventStore](https://github.com/commanded/eventstore), [Commanded EventStore Adapter](https://github.com/commanded/commanded-eventstore-adapter) and [Commanded Ecto Projections](https://github.com/commanded/commanded-ecto-projections), all by Ben Smith under the MIT License. It copies the code sparkle.space uses, with no fork and no upstream history, and has changed it since.

- `LICENSE`: decree's MIT License.
- `NOTICE`: what decree is derived from, and the upstream MIT notices, unchanged.
- `PROVENANCE.tsv`: the upstream file and its SHA-256 for every copied file.
- `UPSTREAM.md`: the upstream versions, the fixes carried from upstream, every departure from it, and the dependency pins.
- `DROPPED_TESTS.md`: the upstream tests decree does not run, and why.

Issue references such as `sparkle-space/sparklespace#N` in the code, the docs and the commit history point at sparkle.space's own tracker, which is private.

## Contributing and security

Issues are welcome. Pull requests are by arrangement while decree is pre-1.0: see `CONTRIBUTING.md`. Report a vulnerability privately, as `SECURITY.md` describes.
