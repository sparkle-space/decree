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

- **A test that fails first.** A fix or a feature lands with a test that fails on the code before it, in its own earlier commit, and passes after. A fix to a flaky test alone may instead record a forced interleaving that fails before the fix and passes after.
- **The suites, on Postgres.** `README.md`, "Running the tests", says how to run them. CI runs `mix test --include migration` on Postgres 18, `mix format --check-formatted`, `mix compile --warnings-as-errors`, `mix run --no-start scripts/check_layers.exs`, the dependency audits and a dry run of the Hex package (`mix hex.build`).
- **The upstream ledger.** A change to code copied from Commanded or EventStore gets a row in `UPSTREAM.md`: "Carried fixes" for an upstream defect, "Departures" for anything else. A documentation-only edit (doc strings and comments) needs none.
- **The design's limits.** decree has no process managers (a multi-step flow is an event handler that dispatches commands) and no in-memory event store. A change that adds either will not be merged.

- **A CHANGELOG entry.** A change a user of the package would notice adds its entry under "Unreleased" in `CHANGELOG.md`, in the same pull request. A change to tests, CI or docs only needs none.

`AGENTS.md` holds the full review checklist.

## Releases

A merge to `main` publishes nothing by itself. A release is a pull request that changes the version in `mix.exs`, and two workflows make and publish it.

### Version rules before 1.0

- **A breaking change is a minor bump** (0.1.x to 0.2.0): a change to the API, the store schema or a behaviour a caller relies on. `~> 0.1.0` takes 0.1.x only, so a user opts in to it.
- **A fix or an additive change is a patch** (0.1.0 to 0.1.1).
- **1.0 is the founder's call.** A major bump from 0.x is refused unless the founder confirms it (`confirm_1_0`).

### The flow

1. **Prepare.** A maintainer runs the "Prepare release" workflow on `main` with `bump` set to patch, minor or major (Actions, "Prepare release", "Run workflow"; or `gh workflow run prepare-release.yml -f bump=patch`). It runs `scripts/release.exs bump`. That bumps `mix.exs`'s version and moves the CHANGELOG's "Unreleased" section under the new version and today's date (UTC). A minor or major bump also moves the requirement users copy, `~> X.Y.0`, and the series, X.Y.x, to the new version's:
   - README.md's install line, `{:decree, "~> X.Y.0"}`, and its sentence on `~> X.Y.0`;
   - README.md's Status, "(version X.Y.x)";
   - the CHANGELOG preamble's `~> X.Y.0`, above "Unreleased". A released section keeps the requirement it names;
   - SECURITY.md's supported series, "X.Y.x today".

   A patch moves none of them, since `~> X.Y.0` takes it. `test/framework/readme_example_test.exs` reads the requirement and the series from `mix.exs`'s version, so the release PR passes as generated. The workflow then commits `mix.exs`, `CHANGELOG.md`, `README.md` and `SECURITY.md` and opens a pull request from `release/vX.Y.Z`. An empty "Unreleased" section, or a reference the bump cannot find or finds twice, stops it before it writes anything. A version that is already tagged stops it before it pushes.
2. **Review the release PR.** GitHub starts no CI on a pull request that a workflow's token opened, so close and reopen the PR to run it. At 1.0.0, rewrite the README's pre-1.0 Status and its sentence on `~> X.Y.0` in the same PR: the bump moves their versions, not their meaning. The CHANGELOG section becomes the GitHub release body. Merge it like any PR.
3. **Publish.** The merge starts the "Publish" workflow:
   - "Release check" goes on only when the merge changed `mix.exs`'s version and that version is not the latest `v*` tag. A later merge that lands while the release waits for approval does not release again.
   - "Gates" runs the CI workflow's jobs again on the commit to publish.
   - "Publish to Hex" waits for approval in the `hex-publish` environment. It then tags `vX.Y.Z`, creates the GitHub release and runs `mix hex.publish` (package and docs). It authenticates with Hex Workload Identity, so no Hex key is stored. Last, it reads the release back from hex.pm.
4. **If a step fails,** re-run the failed jobs of that run. Each step first checks what is already done: a tag at this commit, an existing release, or the version already on Hex with its docs is skipped. A version on Hex without its docs gets its docs. A tag at another commit stops the run. If the release's run cannot finish because a fix had to land first, run "Publish" by hand on `main` (Actions, "Publish", "Run workflow"). A manual run releases main's commit when its version is not the latest `v*` tag.

## Licence

decree is MIT-licensed (`LICENSE`). By contributing, you agree that your contribution is licensed under the same terms.
