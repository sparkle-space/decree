# Rename map: Commanded and EventStore to decree

The complete mapping P3 applies to sparklespace to switch from the four upstream packages to decree. Every item is a mechanical rename; no persisted data changes (see the last section), so the switch needs no data migration.

`scripts/rename.exs` applied the same rules to decree's own copy of the code.

## Dependencies

| Before | After |
|---|---|
| `{:commanded, ...}` | `{:decree, ...}` |
| `{:eventstore, ...}` | (in `:decree`) |
| `{:commanded_eventstore_adapter, ...}` | (in `:decree`) |
| `{:commanded_ecto_projections, ...}` | (in `:decree`) |

decree is one OTP application. `Application.ensure_all_started(:decree)` starts what `:eventstore` and `:commanded` started (the store's config registry and monitored-server registry, then the framework's config registry).

## Module names

The rules, in order:

1. `Commanded.X` / `Commanded` → `Decree.X` / `Decree`.
2. `EventStore.X` / `EventStore` at the head of a module name → `Decree.Store.X` / `Decree.Store`. Not when preceded by a word character or a dot: `Sparklespace.EventStore` and `Decree.EventStore.Adapters.EventStore` keep their `EventStore`, and `Commanded.EventStore` (the framework's facade) becomes `Decree.EventStore` by rule 1, not `Decree.Store`.
3. `Mix.EventStore` → `Mix.Decree.Store`; `Mix.Tasks.EventStore.X` → `Mix.Tasks.Decree.Store.X`.

A file that binds the name `EventStore` with an alias (`alias Commanded.EventStore`, `alias Commanded.{.., EventStore}`, `alias X, as: EventStore`) keeps every bare `EventStore` after that alias: it means the alias, not the store.

Names derived with `Module.concat` follow the rename when both sides are renamed: `Module.concat([Sparklespace.EventStore, EventStore.Subscriptions.Registry])` becomes `Module.concat([Sparklespace.EventStore, Decree.Store.Subscriptions.Registry])`, the name decree registers. A derived name written out in full (`Sparklespace.EventStore.EventStore.AdvisoryLocks`) must be changed by hand to `Sparklespace.EventStore.Decree.Store.AdvisoryLocks`. Process names are not persisted; they matter only to code that looks processes up, and to a cluster running old and new code at once.

### `use`, behaviour and protocol targets

| Before | After |
|---|---|
| `use Commanded.Application` | `use Decree.Application` |
| `use Commanded.Commands.Router` | `use Decree.Commands.Router` |
| `use Commanded.Commands.CompositeRouter` | `use Decree.Commands.CompositeRouter` |
| `use Commanded.Event.Handler` | `use Decree.Event.Handler` |
| `use Commanded.Event.Upcaster`, `defimpl Commanded.Event.Upcaster` | `Decree.Event.Upcaster` |
| `use Commanded.Projections.Ecto` | `use Decree.Projections.Ecto` |
| `use EventStore` | `use Decree.Store` |
| `@behaviour Commanded.Middleware` | `@behaviour Decree.Middleware` |
| `@behaviour Commanded.Aggregates.AggregateLifespan` | `@behaviour Decree.Aggregates.AggregateLifespan` |
| `@behaviour EventStore.Serializer` | `@behaviour Decree.Store.Serializer` |
| `defimpl Commanded.Serialization.JsonDecoder` | `defimpl Decree.Serialization.JsonDecoder` |
| event store adapter `Commanded.EventStore.Adapters.EventStore` | `Decree.EventStore.Adapters.EventStore` |
| serializer `Commanded.Serialization.JsonSerializer` | `Decree.Serialization.JsonSerializer` |

`Commanded.Assertions.EventAssertions`, `Commanded.AggregateCase` and the other test helpers are not part of decree's library: they compile only in decree's own test environment. sparklespace uses none of them.

### Every module

All modules decree compiles from `lib/`. `Decree.Store.Subscriptions.GeneratedFsm` is new in decree (a compile-time wrapper around the `fsm` dependency's `use Fsm`) and has no upstream name.

| Upstream | decree |
|---|---|
| `Commanded` | `Decree` |
| `Commanded.Aggregate.Multi` | `Decree.Aggregate.Multi` |
| `Commanded.Aggregates.Aggregate` | `Decree.Aggregates.Aggregate` |
| `Commanded.Aggregates.AggregateLifespan` | `Decree.Aggregates.AggregateLifespan` |
| `Commanded.Aggregates.AggregateStateBuilder` | `Decree.Aggregates.AggregateStateBuilder` |
| `Commanded.Aggregates.DefaultLifespan` | `Decree.Aggregates.DefaultLifespan` |
| `Commanded.Aggregates.ExecutionContext` | `Decree.Aggregates.ExecutionContext` |
| `Commanded.Aggregates.Supervisor` | `Decree.Aggregates.Supervisor` |
| `Commanded.Application` | `Decree.Application` |
| `Commanded.Application.Config` | `Decree.Application.Config` |
| `Commanded.Application.Supervisor` | `Decree.Application.Supervisor` |
| `Commanded.Commands.CompositeRouter` | `Decree.Commands.CompositeRouter` |
| `Commanded.Commands.Dispatcher` | `Decree.Commands.Dispatcher` |
| `Commanded.Commands.Dispatcher.Payload` | `Decree.Commands.Dispatcher.Payload` |
| `Commanded.Commands.ExecutionResult` | `Decree.Commands.ExecutionResult` |
| `Commanded.Commands.Handler` | `Decree.Commands.Handler` |
| `Commanded.Commands.Router` | `Decree.Commands.Router` |
| `Commanded.Event.ErrorHandler` | `Decree.Event.ErrorHandler` |
| `Commanded.Event.FailureContext` | `Decree.Event.FailureContext` |
| `Commanded.Event.Handler` | `Decree.Event.Handler` |
| `Commanded.Event.Handler.Supervisor` | `Decree.Event.Handler.Supervisor` |
| `Commanded.Event.Mapper` | `Decree.Event.Mapper` |
| `Commanded.Event.Upcast` | `Decree.Event.Upcast` |
| `Commanded.Event.Upcaster` | `Decree.Event.Upcaster` |
| `Commanded.Event.Upcaster.Any` | `Decree.Event.Upcaster.Any` |
| `Commanded.EventStore` | `Decree.EventStore` |
| `Commanded.EventStore.Adapter` | `Decree.EventStore.Adapter` |
| `Commanded.EventStore.Adapters.EventStore` | `Decree.EventStore.Adapters.EventStore` |
| `Commanded.EventStore.Adapters.EventStore.Mapper` | `Decree.EventStore.Adapters.EventStore.Mapper` |
| `Commanded.EventStore.EventData` | `Decree.EventStore.EventData` |
| `Commanded.EventStore.RecordedEvent` | `Decree.EventStore.RecordedEvent` |
| `Commanded.EventStore.SnapshotData` | `Decree.EventStore.SnapshotData` |
| `Commanded.EventStore.Subscription` | `Decree.EventStore.Subscription` |
| `Commanded.EventStore.TypeProvider` | `Decree.EventStore.TypeProvider` |
| `Commanded.Middleware` | `Decree.Middleware` |
| `Commanded.Middleware.ConsistencyGuarantee` | `Decree.Middleware.ConsistencyGuarantee` |
| `Commanded.Middleware.ExtractAggregateIdentity` | `Decree.Middleware.ExtractAggregateIdentity` |
| `Commanded.Middleware.Pipeline` | `Decree.Middleware.Pipeline` |
| `Commanded.Projections.Ecto` | `Decree.Projections.Ecto` |
| `Commanded.PubSub` | `Decree.PubSub` |
| `Commanded.PubSub.Adapter` | `Decree.PubSub.Adapter` |
| `Commanded.PubSub.LocalPubSub` | `Decree.PubSub.LocalPubSub` |
| `Commanded.PubSub.PhoenixPubSub` | `Decree.PubSub.PhoenixPubSub` |
| `Commanded.PubSub.PhoenixPubSub.Tracker` | `Decree.PubSub.PhoenixPubSub.Tracker` |
| `Commanded.Registration` | `Decree.Registration` |
| `Commanded.Registration.Adapter` | `Decree.Registration.Adapter` |
| `Commanded.Registration.GlobalRegistry` | `Decree.Registration.GlobalRegistry` |
| `Commanded.Registration.LocalRegistry` | `Decree.Registration.LocalRegistry` |
| `Commanded.Serialization.JsonDecoder` | `Decree.Serialization.JsonDecoder` |
| `Commanded.Serialization.JsonDecoder.Any` | `Decree.Serialization.JsonDecoder.Any` |
| `Commanded.Serialization.JsonSerializer` | `Decree.Serialization.JsonSerializer` |
| `Commanded.Serialization.ModuleNameTypeProvider` | `Decree.Serialization.ModuleNameTypeProvider` |
| `Commanded.Serialization.Serializer` | `Decree.Serialization.Serializer` |
| `Commanded.Snapshotting` | `Decree.Snapshotting` |
| `EventStore` | `Decree.Store` |
| `EventStore.AdvisoryLocks` | `Decree.Store.AdvisoryLocks` |
| `EventStore.AdvisoryLocks.Lock` | `Decree.Store.AdvisoryLocks.Lock` |
| `EventStore.AdvisoryLocks.State` | `Decree.Store.AdvisoryLocks.State` |
| `EventStore.Application` | `Decree.Store.Application` |
| `EventStore.Config` | `Decree.Store.Config` |
| `EventStore.Config.Parser` | `Decree.Store.Config.Parser` |
| `EventStore.Config.Store` | `Decree.Store.Config.Store` |
| `EventStore.EExIOListEngine` | `Decree.Store.EExIOListEngine` |
| `EventStore.EventData` | `Decree.Store.EventData` |
| `EventStore.JsonSerializer` | `Decree.Store.JsonSerializer` |
| `EventStore.MonitoredServer` | `Decree.Store.MonitoredServer` |
| `EventStore.MonitoredServer.State` | `Decree.Store.MonitoredServer.State` |
| `EventStore.Notifications.Listener` | `Decree.Store.Notifications.Listener` |
| `EventStore.Notifications.Notification` | `Decree.Store.Notifications.Notification` |
| `EventStore.Notifications.Publisher` | `Decree.Store.Notifications.Publisher` |
| `EventStore.Notifications.Publisher.State` | `Decree.Store.Notifications.Publisher.State` |
| `EventStore.Notifications.Supervisor` | `Decree.Store.Notifications.Supervisor` |
| `EventStore.Page` | `Decree.Store.Page` |
| `EventStore.PubSub` | `Decree.Store.PubSub` |
| `EventStore.RecordedEvent` | `Decree.Store.RecordedEvent` |
| `EventStore.Serializer` | `Decree.Store.Serializer` |
| `EventStore.Snapshots.SnapshotData` | `Decree.Store.Snapshots.SnapshotData` |
| `EventStore.Snapshots.Snapshotter` | `Decree.Store.Snapshots.Snapshotter` |
| `EventStore.Sql.Init` | `Decree.Store.Sql.Init` |
| `EventStore.Sql.Reset` | `Decree.Store.Sql.Reset` |
| `EventStore.Sql.Statements` | `Decree.Store.Sql.Statements` |
| `EventStore.Storage` | `Decree.Store.Storage` |
| `EventStore.Storage.Appender` | `Decree.Store.Storage.Appender` |
| `EventStore.Storage.CreateStream` | `Decree.Store.Storage.CreateStream` |
| `EventStore.Storage.Database` | `Decree.Store.Storage.Database` |
| `EventStore.Storage.DeleteStream` | `Decree.Store.Storage.DeleteStream` |
| `EventStore.Storage.Initializer` | `Decree.Store.Storage.Initializer` |
| `EventStore.Storage.Lock` | `Decree.Store.Storage.Lock` |
| `EventStore.Storage.Reader` | `Decree.Store.Storage.Reader` |
| `EventStore.Storage.Reader.EventAdapter` | `Decree.Store.Storage.Reader.EventAdapter` |
| `EventStore.Storage.Reader.Query` | `Decree.Store.Storage.Reader.Query` |
| `EventStore.Storage.Schema` | `Decree.Store.Storage.Schema` |
| `EventStore.Storage.Snapshot` | `Decree.Store.Storage.Snapshot` |
| `EventStore.Storage.Stream` | `Decree.Store.Storage.Stream` |
| `EventStore.Storage.Subscription` | `Decree.Store.Storage.Subscription` |
| `EventStore.Storage.Subscription.Ack` | `Decree.Store.Storage.Subscription.Ack` |
| `EventStore.Storage.Subscription.Adapter` | `Decree.Store.Storage.Subscription.Adapter` |
| `EventStore.Storage.Subscription.CreateSubscription` | `Decree.Store.Storage.Subscription.CreateSubscription` |
| `EventStore.Storage.Subscription.Delete` | `Decree.Store.Storage.Subscription.Delete` |
| `EventStore.Storage.Subscription.QueryAllSubscriptions` | `Decree.Store.Storage.Subscription.QueryAllSubscriptions` |
| `EventStore.Storage.Subscription.QuerySubscription` | `Decree.Store.Storage.Subscription.QuerySubscription` |
| `EventStore.Streams.Stream` | `Decree.Store.Streams.Stream` |
| `EventStore.Streams.StreamInfo` | `Decree.Store.Streams.StreamInfo` |
| `EventStore.Subscriptions` | `Decree.Store.Subscriptions` |
| `EventStore.Subscriptions.Subscriber` | `Decree.Store.Subscriptions.Subscriber` |
| `EventStore.Subscriptions.Subscription` | `Decree.Store.Subscriptions.Subscription` |
| `EventStore.Subscriptions.SubscriptionFsm` | `Decree.Store.Subscriptions.SubscriptionFsm` |
| `EventStore.Subscriptions.SubscriptionState` | `Decree.Store.Subscriptions.SubscriptionState` |
| `EventStore.Subscriptions.Supervisor` | `Decree.Store.Subscriptions.Supervisor` |
| `EventStore.Supervisor` | `Decree.Store.Supervisor` |
| `EventStore.Tasks.Create` | `Decree.Store.Tasks.Create` |
| `EventStore.Tasks.Drop` | `Decree.Store.Tasks.Drop` |
| `EventStore.Tasks.Init` | `Decree.Store.Tasks.Init` |
| `EventStore.Tasks.Migrate` | `Decree.Store.Tasks.Migrate` |
| `EventStore.Tasks.Migration` | `Decree.Store.Tasks.Migration` |
| `EventStore.Tasks.Migrations` | `Decree.Store.Tasks.Migrations` |
| `EventStore.Tasks.Output` | `Decree.Store.Tasks.Output` |
| `EventStore.UUID` | `Decree.Store.UUID` |
| `Commanded.Subscriptions` | `Decree.Subscriptions` |
| `Commanded.Subscriptions.Registry` | `Decree.Subscriptions.Registry` |
| `Commanded.Telemetry` | `Decree.Telemetry` |
| `Commanded.UUID` | `Decree.UUID` |
| `Jason.Encoder.Commanded.EventStore.SnapshotData` | `Jason.Encoder.Decree.EventStore.SnapshotData` |
| `Mix.EventStore` | `Mix.Decree.Store` |
| `Mix.Tasks.EventStore.Create` | `Mix.Tasks.Decree.Store.Create` |
| `Mix.Tasks.EventStore.Drop` | `Mix.Tasks.Decree.Store.Drop` |
| `Mix.Tasks.EventStore.Init` | `Mix.Tasks.Decree.Store.Init` |
| `Mix.Tasks.EventStore.Migrate` | `Mix.Tasks.Decree.Store.Migrate` |
| `Mix.Tasks.EventStore.Migrations` | `Mix.Tasks.Decree.Store.Migrations` |

## OTP application and configuration

| Before | After |
|---|---|
| `:commanded`, `:eventstore`, `:commanded_eventstore_adapter`, `:commanded_ecto_projections` where they name the application (`Application.ensure_all_started/1`, `Application.get_env/2`, `Application.app_dir/2`, `otp_app:` defaults); not the `:eventstore` option key below | `:decree` |
| `config :commanded, key: ...` | `config :decree, key: ...` |
| `config :commanded_ecto_projections, repo: ..., schema_prefix: ...` | `config :decree, repo: ..., schema_prefix: ...` |
| `config :eventstore, event_stores: [...]` (EventStore's own repository only) | `config :decree, event_stores: [...]` |

The host application's own keys do not change: `otp_app: :sparklespace`, `config :sparklespace, event_stores: [...]` and `config :sparklespace, Sparklespace.EventStore, ...` stay as they are.

Keys decree reads from `:decree`, none shared between the merged libraries: `:aggregate_lifespan`, `:default_consistency`, `:default_dispatch_return`, `:dispatch_consistency_timeout`, `:include_aggregate_version`, `:include_execution_result`, `:subscriptions_ttl`, `:type_provider`, `:repo`, `:schema_prefix`, and one key per application or event store module configured under `:decree`.

## Mix tasks and aliases

| Before | After |
|---|---|
| `mix event_store.create` | `mix decree.store.create` |
| `mix event_store.init` | `mix decree.store.init` |
| `mix event_store.drop` | `mix decree.store.drop` |
| `mix event_store.migrate` | `mix decree.store.migrate` |
| `mix event_store.migrations` | `mix decree.store.migrations` |

The options are unchanged (`-e` / `--eventstore`, `--quiet`). `event_store.setup` and `event_store.reset` were aliases, not tasks: decree's `mix.exs` defines `decree.store.setup` and `decree.store.reset` for its own repository, and aliases are not inherited from dependencies, so sparklespace keeps its own aliases and points their steps at the `decree.store.*` tasks. The task modules are `Mix.Tasks.Decree.Store.Create` and so on; the shared helper is `Mix.Decree.Store`.

## Telemetry

Every `[:commanded, ...]` event becomes `[:decree, ...]`; the rest of the event name is unchanged. EventStore emits no telemetry.

| Before | After |
|---|---|
| `[:commanded, :application, :dispatch, :start \| :stop]` | `[:decree, :application, :dispatch, :start \| :stop]` |
| `[:commanded, :aggregate, :execute, :start \| :stop \| :exception]` | `[:decree, :aggregate, :execute, ...]` |
| `[:commanded, :aggregate, :populate, :start \| :stop]` | `[:decree, :aggregate, :populate, ...]` |
| `[:commanded, :event, :handle, :start \| :stop \| :exception]` | `[:decree, :event, :handle, ...]` |
| `[:commanded, :event, :batch, :start \| :stop \| :exception]` | `[:decree, :event, :batch, ...]` |
| `[:commanded, :event_store, <function>, :start \| :stop \| :exception]` | `[:decree, :event_store, <function>, ...]` |

sparklespace attaches to `[:commanded, :application, :dispatch, :stop]` and `[:commanded, :aggregate, :populate, :stop]` in `SparklespaceWeb.Telemetry.SlowLogger`, and to the dispatch events in several test helpers; each becomes `[:decree, ...]`. Telemetry measurement and metadata keys are unchanged.

## Logger metadata

None of the libraries sets Logger metadata named after the library, so nothing changes.

## Persisted strings unchanged

Nothing written to Postgres or sent over the wire changes, so P3 needs no data migration:

- the schema: the tables `events`, `streams`, `stream_events`, `subscriptions`, `snapshots`, `schema_migrations`, their columns and indexes, the triggers `no_update_events`, `no_delete_events`, `no_update_stream_events`, `no_delete_stream_events`, `no_delete_streams`, `event_notification`, and the functions `event_store_exception()`, `event_store_delete()` and `notify_events()`;
- the trigger error messages, prefixed `EventStore: ` (`EventStore: Cannot update events`, ...);
- the session settings `eventstore.enable_hard_deletes` and `eventstore.reset`, which the delete trigger reads;
- the `$all` stream row (stream id 0) and the `schema_migrations` version row (1.3.2);
- the migration scripts (`priv/decree/store/migrations/v*.sql`, moved from `priv/event_store/migrations/`, contents unchanged);
- the NOTIFY channel `<schema>.events` and its payload `stream_uuid,stream_id,first_version,last_version`;
- advisory-lock keys: subscription ids, and -1 for the migration lock;
- subscription names (the handler names the application chooses);
- event metadata keys;
- event-type and snapshot source-type strings: the application's own module names (`Elixir.Sparklespace...`) through `ModuleNameTypeProvider`. No library module name is persisted.

Not persisted and also kept:

- the Mix CLI option `--eventstore` / `-e` and its OptionParser key `:eventstore` (the `eventstore:` option of `Decree.Store.Tasks.Migrations.exec/2` too): `:eventstore` there names a command-line option, not the OTP application, so the application rename does not touch it;
- the registry name `MonitoredServer.Registry`;
- upstream links in the documentation.
