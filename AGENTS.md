# AGENTS.md — review guidelines for decree

decree is sparkle.space's event-sourcing toolkit: a Postgres event store (`Decree.Store`) and a CQRS framework (`Decree`), copied from Commanded and EventStore. `README.md` describes the layers and the suites; `UPSTREAM.md` lists the upstream versions, the carried fixes and the departures.

## Review Guidelines

Flag these as **P0** (must fix before merge):

- **A fix or feature without a test that fails first.** The fence lands in an earlier commit (`:white_check_mark: Fence …`) and is red against the old code; the code commit then turns it green. A fix with no regression test, or a test that passes on the old code, is the finding. A rule the sparkle.space app enforces retires only once a decree test proves the library fix (founder ruling Q-15 A).
- **Not additive first (RD-1).** A change adds its new API with the old path still working. Flipping a default to strict, or removing or renaming a public function, option or message shape, is a finding unless it is the planned strictness flip (DC-9 of the P5 chain). One PR adopts one decree child, so the app's pin can move one child at a time.
- **A departure from upstream with no `UPSTREAM.md` row.** A defect fix in copied code goes in "Carried fixes" with the upstream issue and the test. Any other change to copied code (new SQL, a replaced function, a new column) goes in "Departures" with the upstream file it replaces. The row names the decree commit by title and the test files.
- **Store SQL that is not schema-qualified.** The store runs under any schema, not only `public` (stream prefixes). Statements render the schema into the SQL (`"<%= schema %>".…`), and the PL/pgSQL functions take `p_schema` and set `search_path` for the call. A hard-coded `public`, an unqualified table or function name, or a schema name frozen in the catalog is the finding.
- **A store migration an old pod cannot live with.** A new `append_events_vN` or table is created beside the old one, and a migrated store keeps the old function for an old pod. `test/store/migrate_event_store_test.exs` must show a migrated schema identical to a new one. Dropping or altering what the previous release reads is the finding.
- **A suite weakened to pass.** CI runs `mix test --include migration` on Postgres 18; the `:migration` tests need a Postgres 18 `pg_restore` and `pg_dump`. Deleting, skipping, `@tag :ignore`-ing or loosening a test, or moving coverage out of `--include migration`, is the finding. Upstream tests we do not carry are listed with a reason in `DROPPED_TESTS.md`.
- **The conformance suite.** `test/conformance` runs the shared event store test cases through the `Decree.EventStore` facade over a `Decree.Store` on Postgres. A change to the store or the `Decree.EventStore` facade that alters behaviour those cases pin changes the cases in the same PR, with the reason. There is no in-memory event store (founder ruling): do not add one or a test double for the store.
- **A process manager.** decree has none and gets none (permanent founder lock). A multi-step flow is an event handler that dispatches commands.
- **The store referencing the framework.** `Decree.Store.*` never calls `Decree.*` outside the store. CI enforces it with `mix run --no-start scripts/check_layers.exs`.
- **A copied file with no `PROVENANCE.tsv` row.**

## Pins and the PR chain

- sparklespace pins decree at a full commit SHA. Only a sparklespace "tree PR" moves it, and each adopts one decree child. A fix to an adopted child lands on a branch cut from that child's merge commit and is merged forward, so every pin descends from the previous one. A decree PR that bundles two children, or reverts or drops an earlier merge, breaks this.
- decree's PRs are one writer's chain, merged in order. Rebase onto `main` instead of merging around the chain.

## Do not flag

- Formatting and warnings: CI runs `mix format --check-formatted` and `mix compile --warnings-as-errors`.
- Style differences from upstream in copied code, unless the change is behavioural.
- Missing docs on internal modules.

## Issue references

Tracking lives in `sparkle-space/sparklespace`. A PR cites its issue as `Refs sparkle-space/sparklespace#N`. That tracker is private, so a `sparklespace#N` link in decree's code, docs or history does not open for a reader outside sparkle.space.
