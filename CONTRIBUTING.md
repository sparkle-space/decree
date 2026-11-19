# Contributing to decree

decree is built for sparkle.space and the projects sparkle.space produces, and it is pre-1.0. Its contribution policy follows from that.

## Issues are welcome

Open an issue for a bug, a question, or a gap in the docs. A useful bug report names:

- the decree version you depend on, or the commit if you run `main`;
- your Elixir, Erlang/OTP and Postgres versions;
- what you did, what you expected and what happened, ideally as a failing test or a small script against a fresh store.

Report a security vulnerability privately instead, as `SECURITY.md` describes.

## Pull requests are by arrangement

While decree is pre-1.0, pull requests are by arrangement: open an issue first and describe the change. If it fits, we will agree with you how it lands. A pull request that arrives without that conversation may be closed unmerged, however good it is. One maintainer reviews every change, and the review bar below is the same for everyone.

## What a change needs

- **A test that fails first.** A fix or a feature lands with a test that fails on the code before it, in its own earlier commit, and passes after.
- **The suites, on Postgres.** `README.md`, "Running the tests", says how to run them. CI runs `mix test --include migration` on Postgres 18, `mix format --check-formatted`, `mix compile --warnings-as-errors`, `mix run --no-start scripts/check_layers.exs` and the dependency audits.
- **The upstream ledger.** A change to code copied from Commanded or EventStore gets a row in `UPSTREAM.md`: "Carried fixes" for an upstream defect, "Departures" for anything else.
- **The design's limits.** decree has no process managers (a multi-step flow is an event handler that dispatches commands) and no in-memory event store. A change that adds either will not be merged.

`AGENTS.md` holds the full review checklist.

## Licence

decree is MIT-licensed (`LICENSE`). By contributing, you agree that your contribution is licensed under the same terms.
