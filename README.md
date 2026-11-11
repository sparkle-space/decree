# decree

An event-sourcing toolkit for Elixir: a Postgres event store (`Decree.Store`) and a CQRS framework (`Decree`) on top of it.

Derived from [Commanded](https://github.com/commanded/commanded) and [EventStore](https://github.com/commanded/eventstore) by copying the code sparkle.space uses, with no fork and no upstream history. MIT; the upstream notices are in `NOTICE`, the per-file origin in `PROVENANCE.tsv`, and the upstream versions and the tailing window in `UPSTREAM.md`.

Status: bootstrap (sparkle.space P2, sparkle-space/sparklespace#7498). Private.

## Layers

One OTP application, `:decree`, in two layers:

- **Store** (`Decree.Store`, `Decree.Store.*`, `Mix.Decree.Store`, `Mix.Tasks.Decree.Store.*`; `lib/decree/store/`, `lib/mix/decree/`, `lib/mix/tasks/decree.store.*`): the Postgres event store. Streams, append with expected version, snapshots, persistent and transient subscriptions with acknowledgement and partitioning, schemas (prefixes), and the Mix tasks `decree.store.create`, `decree.store.init`, `decree.store.drop`, `decree.store.migrate` and `decree.store.migrations`.
- **Framework** (`Decree`, `Decree.*` outside `Decree.Store`): applications, command routing and dispatch, aggregates, decisions (`Decree.Decision`), event handlers, middleware, the event store adapter `Decree.EventStore.Adapters.EventStore` over the store, and Ecto read-model projections (`Decree.Projections.Ecto`), and the Mix task `decree.verify.eventless` (`Mix.Tasks.Decree.Verify.Eventless` over `Decree.Verify.Eventless`).

`mix decree.verify.eventless --router MyApp.Router` fails on any routed handler arm that can accept a command without appending an event: an aggregate handler answering `[]`, `nil`, `:ok`, `{:ok, []}` or `{:ok, nil}`, which the aggregate accepts with no events, and a decision's `decide/2` answering those or `{:streams, []}`, which a dispatch refuses at runtime. It reads each router's registry (`__route__/1` names every routed module and function), walks their source under the project's `elixirc_paths` without compiling or loading it, and reports what it cannot classify as a finding too. It has no allow-list and no waiver; `--router` repeats, and `--no-compile` skips `mix compile`. `Decree.Verify.Eventless` documents the shapes, what the walk follows and its stated residuals.

Being one OTP application, the store's supervision tree (the supervisor registered as `Decree.Store.Supervisor`, started by `Decree.Store.Application`) is a child of `Decree.Supervisor` and shares its restart budget. Upstream, `:eventstore` was a separate application that stopped on its own when its supervisor exhausted its restart budget; in decree the failure escalates to `Decree.Supervisor`, and exhausting that budget stops `:decree`. The children, their names and their order are upstream's.

The store never references the framework. `scripts/check_layers.exs` enforces this from the compiled BEAM files and CI runs it:

```bash
mix run --no-start scripts/check_layers.exs
```

What is absent, on purpose:

- **No process managers.** Multi-step flows are event handlers that dispatch commands. The toolkit has no process-manager module, by design.
- **No in-memory event store.** Every test and every deployment runs on Postgres (founder ruling, 2026-10-29).

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
| framework | `test/framework` | Commanded's suite, on the Postgres adapter: applications, routing, dispatch, aggregates, snapshots, event handlers, middleware, upcasting, telemetry |
| store | `test/store` | EventStore's suite: storage, streams, subscriptions, notifications, advisory locks, schemas, config |
| conformance | `test/conformance` | the store conformance suite (below) |
| projections | `test/projections` | Commanded Ecto projections: projection versions, schema prefixes, error and after-update callbacks |

Framework and projections tests run their applications on `Decree.EventStore.Adapters.EventStore` over the `decree_framework_test` database; `Decree.PostgresCase` empties it before each test. Tests tagged `:migration` (they restore old database dumps with `pg_restore` and compare schemas with `pg_dump`), `:manual`, `:ignore` and `:distributed` are excluded. CI installs the Postgres 18 client and runs `mix test --include migration`; run the same locally when a Postgres 18 `pg_restore` and `pg_dump` are on `PATH`. `DROPPED_TESTS.md` lists every upstream test not carried over, and why.

## The conformance suite

`test/conformance` runs Commanded's shared event store adapter test cases (`test/framework/event_store/support/*_test_case.ex`) against `Decree.EventStore.Adapters.EventStore` on Postgres. It is the conformance suite for the one store implementation, and covers:

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
