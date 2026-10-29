# Upstream

decree was copied from four MIT-licensed repositories, reviewed at these tags on 2026-10-29:

| Upstream | Repository | Tag | Commit | Reviewed |
|---|---|---|---|---|
| commanded | https://github.com/commanded/commanded | v1.4.11 | `91d97bcdf00c7e58e8a8a43f499fc863cef17dee` | 2026-10-29 |
| eventstore | https://github.com/commanded/eventstore | v1.4.10 | `49ba9f971bd8be8b9bd77702f9eab4013d3fbbdd` | 2026-10-29 |
| commanded_eventstore_adapter | https://github.com/commanded/commanded-eventstore-adapter | v.1.4.2 | `7f348605c3539dd8c61e0163a5068793619ed7ec` | 2026-10-29 |
| commanded_ecto_projections | https://github.com/commanded/commanded-ecto-projections | v1.4.0 | `e2252b86c70b2f123ecc8ec9bea587ec4095dadc` | 2026-10-29 |

`PROVENANCE.tsv` names the upstream file and its SHA-256 for every copied file.

## Tailing window

The tailing window starts on 2026-10-29 and ends on **2027-01-29**.

Tailing is a monthly read of each upstream's changelog, issues and pull requests for defects in code decree kept. Each finding is marked:

- **take**: the defect is in kept code; port the fix by hand (no merge, no cherry-pick), with a test;
- **already-have**: decree already has the fix or never had the defect;
- **skip**: the change touches code decree did not keep (process managers, the in-memory adapter, the other serializers) or is not a defect.

After 2027-01-29 upstream is ignored.

`scripts/upstream-review.sh` lists, per upstream, the commits since the reviewed commit and the issues and pull requests updated since the reviewed date. It only reads (`gh`):

```bash
scripts/upstream-review.sh               # since the reviewed date
scripts/upstream-review.sh 2026-11-29    # since a later review
```

## Carried fixes

Fixes decree carries in its copy of the upstream code, each with its own commit and regression test. A fix is cited by its commit title, which survives a rebase. A carried fix is checked against upstream in the monthly tailing read: if upstream fixes the same defect, its fix is compared with decree's and the finding is marked **already-have**.

| Fix | Upstream | decree commit (title) | Upstream merged (2026-10-29) |
|---|---|---|---|
| An event handler at `concurrency: n > 1` acked a redelivered event as already seen without handling it, losing it. The already-seen skip in `Decree.Event.Handler.handle_event/3` now applies at `concurrency: 1` only (option 1 of the upstream issue). Test: `test/framework/event/event_handler_concurrency_redelivery_test.exs`. sparkle-space/sparklespace#7429. | commanded/commanded#671 (issue; the patch is offered there, no PR open) | ":bug: Skip an already-seen event only at handler `concurrency: 1`" | No |
| A catch-up read error other than `:stream_deleted` or `:stream_not_found` raised `CaseClauseError` in `Decree.Store.Subscriptions.SubscriptionFsm.catch_up_from_stream/1` and stopped the subscription. An `{:error, reason}` arm now holds the subscription in `catch_up_retry` and retries the read on a capped exponential backoff (one second, doubling, up to `subscription_retry_interval`; a successful read or a lost advisory lock resets it), emitting `[:decree, :store, :subscription, :catch_up, :error]`. Test: `test/store/subscriptions/subscription_catch_up_error_test.exs`. sparkle-space/sparklespace#7307. | commanded/eventstore, `EventStore.Subscriptions.SubscriptionFsm.catch_up_from_stream/1` at v1.4.10 (no issue or PR filed; an upstream PR is the founder's call) | ":bug: Retry a failed subscription catch-up read on a timer", with the backoff from ":bug: Back off a failed subscription catch-up read up to the retry interval" and the lock-loss reset from ":white_check_mark: Prove the catch-up retry waits, keeps its cursor and takes parked events" | No |
| A subscription kept delivering after its advisory lock was lost when the lock connection reconnected on a new session inside a surviving pool (a terminated backend, a failover), because `AdvisoryLocks` watched only the pool process; a second instance then took the lock and both delivered. The lock pool now reports connects and disconnects to `AdvisoryLocks` (a DBConnection connection listener), each lock records the session that took it (`pg_backend_pid()`), and every lock on a session the pool has left is released (`:session_lost`). Subscribers are told `{:subscription_lock_lost, subscription}` on a loss and `{:subscribed, subscription}` again when the lock is retaken; `Decree.Event.Handler` drops its unflushed batch and runs `after_start/1` once per subscription. Tests: `test/store/advisory_lock_session_test.exs`, `test/store/advisory_locks_test.exs`, `test/framework/event_handler_after_start_test.exs`. sparkle-space/sparklespace#7543. | commanded/eventstore#330 (issue, open). The fork TrogonStack/commanded_eventstore merged a fix in its PR #24; a reporter's run of that fix still showed #331 and a window of up to 15 s on a hanging reconnect. Compare at the next tailing read. | ":bug: Release advisory locks lost when the lock connection reconnects (#330)", with ":bug: Tell subscribers when a subscription loses and retakes its lock" | No |
| `AdvisoryLocks` hung for good when its connection pool died while new connections were refused: it checked out through the `MonitoredServer` name, which forwards the checkout to a pool and is the process the checkout monitors, so a pool that died holding the request left it waiting with no reply and no `:DOWN`. Every subscription's lock call then timed out and crashed the subscription. `AdvisoryLocks` now queries the pool by pid (`MonitoredServer.whereis/2`), answers `{:error, :not_connected}` while the pool is down, and a lock call that does not answer leaves the subscription parked to retry instead of crashing it. Test: `test/store/advisory_lock_session_test.exs` (a refusing TCP proxy, `test/store/support/tcp_proxy.ex`). sparkle-space/sparklespace#7543. | commanded/eventstore#331 (issue, open) | ":bug: Query the advisory-lock pool by pid so a dying pool cannot hang AdvisoryLocks (#331)", with ":bug: Keep a subscription parked when its lock call does not answer" | No |
| A subscription acknowledged its checkpoint by `(stream_uuid, subscription_name)`, so a holder whose row was deleted and recreated under it (an operator reset) wrote its cursor into the new row. Checkpoints are now written by `subscription_id`; a holder whose row is gone writes nothing and logs a warning. Test: `test/store/subscriptions/subscription_checkpoint_row_test.exs`. sparkle-space/sparklespace#7543. | commanded/eventstore, `EventStore.Subscriptions.SubscriptionFsm.persist_checkpoint/1` at v1.4.10 (no issue or PR filed) | ":bug: Ack a subscription checkpoint by its subscription_id, not its name" | No |

## Tailing log

| Read | Upstream | Item | Mark | How decree dealt with it |
|---|---|---|---|---|
| 2026-10-29 | eventstore | commanded/eventstore#330: a subscription keeps processing events without its advisory lock after the lock connection reconnects | take | Fixed by hand with regression tests; see Carried fixes. sparkle-space/sparklespace#7543. |
| 2026-10-29 | eventstore | commanded/eventstore#331: `AdvisoryLocks` hangs for good when its connection pool dies while new connections are refused | take | Fixed by hand with regression tests; see Carried fixes. sparkle-space/sparklespace#7543. |
