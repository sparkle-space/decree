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

## Upstream test configurations not run

- eventstore's `test.jsonb` (`MIX_ENV=jsonb`): runs the store suite with `EventStore.JsonbSerializer`, which was not kept.
- eventstore's `test.text_ids` (`MIX_ENV=text_ids`): runs the store suite with text correlation and causation ids. sparklespace uses the default uuid ids.
- eventstore's `test.migration` (`MIX_ENV=migration`) and the `:migration`-tagged tests (`migrate_event_store_test.exs`, `list_event_store_migrations_test.exs`, `migrated_event_store_test.exs`): they restore the dumps in `test/store/fixture` with `pg_restore` and compare schemas with `pg_dump`. Excluded by default, as upstream does. Run with `mix test --only migration` where `pg_dump` and `pg_restore` are installed. Run that way on 2026-10-29 (with `pg_restore` but no `pg_dump`), 4 or 5 of the 13 failed between runs against the code as copied: `decree.store.migrations` rejects `-e`, its output names the event store `default` where the tests expect `TestEventStore`, and the schema comparison needs `pg_dump`. They are not in CI; the store-migration path is P4's.
- `:manual` (`shared_connection_pool_test.exs`, one test) and `:ignore`: excluded, as upstream does.

## Kept, with assertions adapted to Postgres

Not dropped; listed so the port can be audited:

- Every framework and projections test application runs on `Decree.EventStore.Adapters.EventStore` over the `decree_framework_test` database (`Decree.FrameworkEventStore`), with one event store process per application instance (`Decree.FrameworkStorage.name_event_store/1`, called from each test application's `init/1`). `Decree.PostgresCase` (and `Decree.MockEventStoreCase`) empty the database before each test, as a fresh in-memory store did.
- `application/application_test.exs`, `application/config_test.exs`: the expected adapter tuple is the Postgres adapter's.
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
- `test/projections`: the `projection_versions` migrations (upstream's `priv/repo/migrations`) live in `test/projections/migrations` and run from the test helper; the helper no longer defines `Decree.EventStore.Adapters.Mock` (the framework support does); `projections/runtime_config_projector_test.exs` waits for the projector's transaction to commit (it raced it).

## Audit

`git diff -M --diff-filter=D --name-only 6e97aeb -- test` (6e97aeb is the copy commit) lists the 7 files under "Deleted in the test port" and `test/conformance/support/test_event_store.ex`, which is not deleted: it became `test/conformance/support/conformance_event_store.ex`, a file too small for git to pair as a rename.
