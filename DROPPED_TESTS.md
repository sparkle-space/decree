# Dropped tests

Every upstream test, test case or test configuration that decree does not run, and why. The four upstream suites live under `test/framework` (commanded), `test/store` (eventstore), `test/conformance` (commanded-eventstore-adapter) and `test/projections` (commanded-ecto-projections).

## Not copied (the copy commit)

From commanded's `test/`; eventstore, commanded-eventstore-adapter and commanded-ecto-projections were copied whole.

In-memory adapter (no in-memory store, founder ruling 2026-10-29). The shared test cases these files ran on the in-memory adapter run on Postgres in `test/conformance`.

- `event_store/adapters/in_memory/append_events_test.exs`
- `event_store/adapters/in_memory/event_store_prefix_test.exs`
- `event_store/adapters/in_memory/in_memory_test.exs`
- `event_store/adapters/in_memory/snapshot_test.exs`
- `event_store/adapters/in_memory/subscription_test.exs`
- `event_store/support/in_memory_test_case.ex`

Process managers (permanent founder lock: decree has none):

- `process_managers/dynamic_process_manager_application_test.exs`
- `process_managers/multi_routing_test.exs`
- `process_managers/process_manager_after_command_test.exs`
- `process_managers/process_manager_error_handling_state_test.exs`
- `process_managers/process_manager_error_handling_test.exs`
- `process_managers/process_manager_idle_timeout_test.exs`
- `process_managers/process_manager_init_test.exs`
- `process_managers/process_manager_instance_exception_test.exs`
- `process_managers/process_manager_instance_test.exs`
- `process_managers/process_manager_integration_test.exs`
- `process_managers/process_manager_routing_test.exs`
- `process_managers/process_manager_subscription_test.exs`
- `process_managers/process_manager_telemetry_test.exs`
- `process_managers/process_manager_timeout_test.exs`
- `process_managers/process_router_process_pending_events_test.exs`
- `process_managers/resume_process_manager_test.exs`
- `process_managers/support/after_command_process_manager.ex`
- `process_managers/support/dynamic_process_manager.ex`
- `process_managers/support/error/default_error_handling_process_manager.ex`
- `process_managers/support/error/error_aggregate.ex`
- `process_managers/support/error/error_app.ex`
- `process_managers/support/error/error_handling_process_manager.ex`
- `process_managers/support/error/error_router.ex`
- `process_managers/support/error/state_error_handling_process_manager.ex`
- `process_managers/support/example_aggregate.ex`
- `process_managers/support/example_app.ex`
- `process_managers/support/example_command_handler.ex`
- `process_managers/support/example_process_manager.ex`
- `process_managers/support/example_router.ex`
- `process_managers/support/identity/identity_process_manager.ex`
- `process_managers/support/init/runtime_config_process_manager.ex`
- `process_managers/support/multi/todo.ex`
- `process_managers/support/multi/todo_app.ex`
- `process_managers/support/multi/todo_list.ex`
- `process_managers/support/multi/todo_process_manager.ex`
- `process_managers/support/multi/todo_router.ex`
- `process_managers/support/null_handler.ex`
- `process_managers/support/null_router.ex`
- `process_managers/support/resume/resume_aggregate.ex`
- `process_managers/support/resume/resume_app.ex`
- `process_managers/support/resume/resume_command_handler.ex`
- `process_managers/support/resume/resume_process_manager.ex`
- `process_managers/support/resume/resume_router.ex`
- `process_managers/support/routing/routing_process_manager.ex`
- `process_managers/support/timeout_process_manager.ex`

## Deleted in the test port

Files (7), each with its reason:

- `test/framework/mix/tasks/reset_event_handler_test.exs` (1 test): tests `Mix.Tasks.Commanded.Reset`, which was not kept.
- `test/framework/subscriptions/distributed_subscriptions_test.exs` (1 test, tagged `:distributed` and excluded upstream too): needs `local_cluster` and starts process managers on every node.
- `test/framework/subscriptions/support/distributed_subscribers.ex`: support for the distributed test; defines process managers.
- `test/framework/support/distributed_app.ex`: the distributed test's application, on the in-memory adapter.
- `test/framework/example_domain/money_transfer/transfer_money_process_manager.ex`: a process manager.
- `test/framework/commands/support/consistency/strongly_consistent_process_manager.ex`: a process manager.
- `test/framework/event/support/upcast/process_manager.ex`: a process manager (and the application it ran in).

Test cases removed from files that stay (4):

- `test/framework/commands/correlation_causation_test.exs`: "`causation_id` should be copied onto commands/events by process manager" and "`correlation_id` should be copied onto commands/events by process manager" (both drive `TransferMoneyProcessManager`). The setup no longer starts the process manager.
- `test/framework/commands/dispatch_consistency_test.exs`: "process manager consistency should successfully dispatch command" (drives `StronglyConsistentProcessManager`).
- `test/framework/event/upcaster_test.exs`: "upcast events received by process manager will receive upcasted events".

## Deleted with the dead surface

The feature is deleted (#8075, founder ruling 2026-11-11): `link_to_stream/4`, `paginate_streams/1` with `Decree.Store.Page`, and the `:shared_connection_pool` option. Its upstream tests go with it. decree's own tests of these, which were never upstream's, are not listed.

Files (4):

- `test/store/storage/link_events_test.exs` (7 tests): `Appender.link/4`, the storage half of a link.
- `test/store/streams/paginate_streams_test.exs` (6 tests): `paginate_streams/1` and `Decree.Store.Page`.
- `test/store/subscriptions/linked_event_stream_subscription_test.exs` (1 test): a subscription to a stream of links.
- `test/store/shared_connection_pool_test.exs` (7 tests, one of them tagged `:manual`): the `:shared_connection_pool` option.

Test cases removed from files that stay:

- `test/store/event_store_test.exs`: the "link to event store" describe, "should link multiple events", "should read linked events", "should fail attempting to link to `$all` stream" and "allows to supply a db connection, so we can link to streams in a running transaction".
- `test/store/streams/single_stream_test.exs`: from "link events to stream", "should link events", "should link events with `:any_version` expected version", "should fail when wrong expected version", "should fail linking events that don't exist" and "should prevent duplicate linked events". "should guess the event type when not passed" stays, in a describe renamed "an event with no event type", without the setup step that appended to a second stream for the links.
- `test/store/streams/hard_delete_stream_test.exs`: "reading a stream should not include linked events from deleted stream".
- `test/store/streams/soft_delete_stream_test.exs`: "reading a stream will include linked events from deleted stream".
- `test/store/config_test.exs`: "parse `:shared_connection_pool`".

`test/store/manual/seed_eventstore.exs`, a script and not a test, loses its link step. The committed dumps were seeded with links before #8075, and `test/store/migrated_event_store_test.exs` "read linked stream forward" still reads them.

## Deleted with the adapter layer

The event store adapter layer, the global registry and `CreateStream` are deleted (#7869, founder ruling 2026-11-11). Their upstream tests go with them.

Files (1):

- `test/framework/registration/global_registry_test.exs` (8 tests, the shared `RegistrationTestCase` on `Decree.Registration.GlobalRegistry`): the global registry. `registry: :global` now raises at start, which `test/framework/application/application_test.exs` "should refuse the removed global registry" pins; the shared case still runs on the local registry.

Test cases removed from files that stay:

- `test/store/storage/stream_persistence_test.exs`: "create stream" and "create stream when already exists", the tests of `CreateStream.execute/3`, which had no caller in `lib`. The append path's `:duplicate_stream_uuid` stays pinned by `test/store/storage/append_error_test.exs`, `test/store/storage/append_function_test.exs` and `test/store/storage/append_streams_lock_order_test.exs`.

Kept and rewritten: the conformance case "should implement `Decree.EventStore.Adapter` behaviour" is "should implement `Decree.Store` behaviour", of the application's store module; `test/framework/event/event_handler_batch_test.exs` pins the acked event's number, which is what a store's `ack/2` receives, where it pinned the acked event.

Reached by another route, with the same assertions:

- `test/framework/personal_fields/key_stream_test.exs` "telemetry, when the append raises" raises the same class from the same store function, the store's "could not lookup ... not started" config lookup (`Decree.Store.Config.Store`), through a flagged append on `FailingStore`. The adapter-level `pretend_declared/1` it used has no store-level equivalent, because the deletable prefixes are read from the store's config, not through a store callback.
- `test/framework/event_handler_after_start_test.exs` "should be called again after a reset" and "should not apply events buffered before the reset", and `test/framework/event/event_handler_retry_wait_test.exs` "a reset during the wait drops the pending retry", run on a stand-in subscription process (`Decree.MockEventStoreCase.start_stand_in_subscription!/0`). `Decree.EventStore.unsubscribe/2` calls the subscription process and never the store module, so a stub of the store cannot answer it.
- `test/framework/event/event_handler_subscription_test.exs` asserts the `:origin` start in the stub's body, where the adapter stub matched it in its head. A mismatch still fails the subscribing handler.
- `test/framework/aggregates/aggregate_concurrency_test.exs` expects `subscribe_to_stream/4`, the store's callback for a single stream, where it expected `subscribe_to/6` with a binary stream.

## Deleted with the unversioned snapshot path

The store's unversioned snapshot read and write, and `Decree.EventStore.read_snapshot/2`, are deleted (#8200, founder ruling RD-1 2026-11-14): a snapshot is a row at a snapshot version, and a nil version answers `{:error, :invalid_snapshot_version}`. `test/store/storage/legacy_snapshot_removed_test.exs` and `test/framework/event_store/legacy_snapshot_removed_test.exs` pin the refusal.

Test cases removed from files that stay:

- `test/framework/event_store/telemetry_test.exs`: "emit `[:decree, :event_store, :read_snapshot, :start | :stop]` event", a `read_snapshot/2` read. The case "... event at a version" covers the same span.

Kept and rewritten, with the same assertions at snapshot version 1, each source's stream appended first because a versioned write needs it:

- the conformance `SnapshotTestCase` (`test/framework/event_store/support/snapshot_test_case.ex`, run by `test/conformance/event_store/snapshot_test.exs`): record, read, read when absent, delete;
- `test/framework/event_store/telemetry_test.exs` "emit `[:decree, :event_store, :record_snapshot, :start | :stop]` event", whose span numbers follow the append's, as the `ack_event` case's do;
- `test/store/storage/snapshot_test.exs`, `test/store/event_store_test.exs` "read a snapshot" and "delete a snapshot", and `test/store/multi_event_store_test.exs` "should record and read snapshot from multiple stores", whose second store still reads no snapshot before its own is recorded.

`test/store/migrated_event_store_test.exs` "read a snapshot" is "drops the legacy snapshot table, and an unversioned read is refused": the dump's ten rows were rows of `snapshots`, which store migration 1.11.0 drops with them, so its assertions on their source, version, type and data are gone with the table; it checks that the migrated store has no `snapshots` table, that an unversioned read of each of the ten sources answers `{:error, :invalid_snapshot_version}`, and that a read at version 1 answers `{:error, :snapshot_not_found}`. `test/store/manual/seed_eventstore.exs`, a script and not a test, loses its snapshot step; the committed dumps keep their rows.

## Upstream test configurations not run

- eventstore's `test.jsonb` (`MIX_ENV=jsonb`): runs the store suite with `EventStore.JsonbSerializer`, which was not kept.
- eventstore's `test.text_ids` (`MIX_ENV=text_ids`): runs the store suite with text correlation and causation ids. sparklespace uses the default uuid ids.
- eventstore's `test.migration` (`MIX_ENV=migration`) and the `:migration`-tagged tests (`migrate_event_store_test.exs`, `list_event_store_migrations_test.exs`, `migrated_event_store_test.exs`): they restore the dumps in `test/store/fixture` with `pg_restore` and compare schemas with `pg_dump`. Excluded by default, as upstream does. Run with `mix test --only migration` where `pg_dump` and `pg_restore` (version 18 or later, to match the server) are installed: on 2026-10-29 all 13 passed that way. They are not in CI: the runner image does not ship a version-18 client, and adding one is a follow-up; the store-migration path is P4's.
- `:ignore`: excluded, as upstream does. `:manual` is still excluded but tags no test: its one test, in `shared_connection_pool_test.exs`, went with that file (see "Deleted with the dead surface").

## Kept, with assertions adapted to Postgres

Not dropped; listed so the port can be audited:

- Every framework and projections test application runs on the `decree_framework_test` database through its `Decree.Store` module, `Decree.FrameworkEventStore` or a test store that wraps it (`Decree.DelegatingStore`), with one event store process per application instance (`Decree.FrameworkStorage.name_event_store/1`, called from each test application's `init/1`). `Decree.PostgresCase` (and `Decree.MockEventStoreCase`) empty the database before each test, as a fresh in-memory store did.
- `application/application_test.exs`, `application/config_test.exs`: the expected event store config is `%{event_store: Decree.FrameworkEventStore, name: ...}` (upstream's was the adapter tuple; the adapter layer went in #7869).
- `application/dynamic_applications_test.exs`: each named instance gets its own schema (`prefix:`), so the two instances keep separate streams, as two in-memory stores did; `length/1` on a stream became `Enum.count/1`.
- `aggregates/snapshotting_test.exs`: the Postgres store stamps `created_at` on a snapshot; the comparison ignores it.
- `aggregates/aggregate_subscription_test.exs`: the aggregate learns of an event appended to its stream through a Postgres notification; the setup waits for it.
- `event/event_handler_batch_subscribe_to_stream_test.exs`, `event/upcaster_test.exs` (batch handler): the store may deliver events appended together in one batch; the assertions accept any batching.
- `event/event_handler_concurrency_test.exs` (partitioning): the store keeps a partition's events serial but does not pin a partition to one handler process; the handler-count assertion became a range.
- `event_store/telemetry_test.exs`: valid snapshot and event data (Postgres enforces not-null columns), an ack on a live subscription, and `:ok` for deleting an unknown subscription.
- `middleware/middleware_test.exs`: the test router no longer lists `Commanded.Middleware.Logger`, which was not kept.
- `test/conformance`: its `TestEventStore` is `ConformanceEventStore` (the store suite has its own `TestEventStore`), on its own database.
- `test/store/advisory_locks_test.exs`: the derived process name is `TestEventStore.Decree.Store.AdvisoryLocks`.
- `test/store`: fixture paths moved to `test/store/fixture/`; `Decree.Store.EventFactory` reads only the serializer at compile time.
- `test/projections`: the `projection_versions` migrations (upstream's `priv/repo/migrations`) live in `test/projections/migrations` and run from the test helper; the helper no longer defines the event store mock, `Decree.EventStore.Mock` for `Decree.Store` (the framework support does); `projections/runtime_config_projector_test.exs` waits for the projector's transaction to commit (it raced it).

Flaky upstream tests made deterministic (test changes, not drops):

- `test/framework/subscriptions/subscriptions_test.exs`, "should not remove PID when process terminates": it spawned the registering process with `spawn_link` and monitored it afterwards, so a process that had already exited produced a `:noproc` `:DOWN` instead of `:normal` (seen on CI). It now uses `spawn_monitor`; the assertions are unchanged.
- The same spawn-then-monitor order, asserting any exit reason (so it could not fail, but raced the same way): `test/framework/aggregates/execute_command_test.exs` (`assert_aggregate_exit/4` now uses `spawn_monitor`) and `test/framework/helpers/process_helper.ex` (`shutdown/2` now monitors before it sends the exit, as `test/store/support/process_helper.ex` already did).
- `test/store/storage/append_events_test.exs`, "append single event with a db connection error": the append used `timeout: 0`, which raced the client timeout against the server. It returned `{:error, %DBConnection.ConnectionError{}}`, `{:error, :query_canceled}` (seen on CI) or, when a fast server finished first, `{:error, :not_found}` (seen locally; stream 1 does not exist, so the insert fails its foreign key). The last outcome means no timeout happened, so the test could pass without exercising its subject. It is now deterministic: a second connection holds `LOCK TABLE events IN ACCESS EXCLUSIVE MODE` in an open transaction (released after the assertion and in `on_exit`), the append uses `timeout: 100`, blocks on the lock, and the test accepts only `%DBConnection.ConnectionError{}` or `:query_canceled`. Its point, an error tuple instead of a raise, is unchanged.

## Test timeouts against upstream

decree runs the four suites in one VM with one `config/test.exs`, so each suite's receive timeouts are the shared values below. Compared with each upstream `config/test.exs` and `test/test_helper.exs`:

| Setting | framework (commanded) | store (eventstore) | conformance (commanded-eventstore-adapter) | projections (commanded-ecto-projections) | decree |
|---|---|---|---|---|---|
| ExUnit `assert_receive_timeout` | 1_000 | 2_000 | 1_000 | 100 (ExUnit default) | 2_000 |
| ExUnit `refute_receive_timeout` | 100 (default) | 100 | 100 (default) | 100 (default) | 100 |
| `assert_receive_event_timeout` (`Decree.Assertions.EventAssertions`) | 100 | not used | 1_000 | not used | 1_000 |
| `refute_receive_event_timeout` | 100 | not used | 1_000 | not used | 100 |
| `dispatch_consistency_timeout` | 100 | not used | not set | not set | 100 |

So: `assert_receive_timeout` rose from 1_000 to 2_000 for framework and conformance and from 100 to 2_000 for projections; framework's `assert_receive_event_timeout` rose from 100 to 1_000; conformance's `refute_receive_event_timeout` fell from 1_000 to 100 (no conformance test case calls `refute_receive_event`); the store's values and every `refute_receive_timeout` are unchanged.

## Audit

`git diff -M --diff-filter=D --name-only 6e97aeb -- test` (6e97aeb is the copy commit) lists the 7 files under "Deleted in the test port", the 4 under "Deleted with the dead surface" and `test/conformance/support/test_event_store.ex`, which is not deleted: it became `test/conformance/support/conformance_event_store.ex`, a file too small for git to pair as a rename.
