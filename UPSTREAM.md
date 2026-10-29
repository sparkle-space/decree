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
| A catch-up read error other than `:stream_deleted` or `:stream_not_found` raised `CaseClauseError` in `Decree.Store.Subscriptions.SubscriptionFsm.catch_up_from_stream/1` and stopped the subscription. An `{:error, reason}` arm now holds the subscription in `catch_up_retry` and retries the read on a capped exponential backoff (one second, doubling, up to `subscription_retry_interval`), emitting `[:decree, :store, :subscription, :catch_up, :error]`. Test: `test/store/subscriptions/subscription_catch_up_error_test.exs`. sparkle-space/sparklespace#7307. | commanded/eventstore, `EventStore.Subscriptions.SubscriptionFsm.catch_up_from_stream/1` at v1.4.10 (no issue or PR filed; an upstream PR is the founder's call) | ":bug: Retry a failed subscription catch-up read on a timer", with the backoff from ":bug: Back off a failed subscription catch-up read up to the retry interval" | No |
