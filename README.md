# Read-index gate rig (sparkle-space/sparklespace#7752)

**A benchmark branch, never to be merged.** It holds scripts only. Issue #7752: decree's read statement, on a store below about 27k events, hash-joined over a sequential scan of `stream_events`. The fix (decree branch `fix/7752-read-index-lookups`) makes the statement use index lookups. This rig measures the fix against pre-FW-6 decree main on a **small** store, kept below that flip for the whole run, and on a **large** store (the existing behaviour). The gated limits are unchanged: Q2 (decision) and Q3 (cold rule) paired medians must be at most **+500 µs** over main. Every side is built from this clone's git objects with `git archive`, so nothing outside git is needed.

| Side | Commit | Built |
|---|---|---|
| main (pre-FW-6) | `0b41deec8a06882343db7cea97e004061bfd39ad` (FW-5's merge): the baseline, as in FW-6's gate | always |
| fix | `BRANCH_SHA`: the #7752 fix commit (set in `env.sh`, or `BRANCH_SHA=<sha>` in the environment) | always |
| fw6 (optional) | `747fcea72039bb36ed467cfe0a83bfd30296be61`: decree main's merge of FW-6, without the fix | only with `SMALL_BEFORE=1` |

## What it measures

Long-lived BEAMs, one per arm, share **one store** in the database `gate_7752`. There is no migration, and `append_events*` is identical on every side; setup prints its md5 for main and fix to show it.

| Arm | Code | Shape |
|---|---|---|
| `dec_main` | main | a decision with design doc §4's two-part query, both reads running: each dispatch first seeds a holder registered and then released under a fresh digest. No conflict, a new identity per dispatch |
| `dec_fix` | fix | the same decision, with the index item and an enabled index |
| `rcold_main` | main | a rule route with the same query, and a new aggregate per dispatch (cold) |
| `rcold_fix` | fix | the same rule route, with the index item and an enabled index |
| `tel_dec_main`, `tel_dec_fix` | main, fix | `STORE=large` only: the decision again; the value is the dispatch's own `read_query` time, from decree's telemetry |
| `tel_rcold_main`, `tel_rcold_fix` | main, fix | `STORE=large` only: the same for the cold rule route |
| `dec_fw6`, `rcold_fw6` | fw6 | `SMALL_BEFORE=1` only (small stores): the same decision and cold rule on FW-6 without the fix |

The four `tel_` arms are span arms. Each attaches a telemetry handler in its own BEAM, so the handlers never touch the gated arms. A span arm also ends itself if a dispatch sends any `read_tags` statement.

Every arm uses the same tags (`email:<fp>:<digest>`) and the same `index_families/0` (`%{"email" => :digest}`). The only difference within a pair is decree's code.

- **Gated quantities**, each a paired median with a threshold of **+500 µs**:
  - **Q2:** `dec_fix - dec_main`.
  - **Q3:** `rcold_fix - rcold_main`.
- **Span pairs** (large only), ungated and printed without a verdict: **RQ** `tel_dec_fix - tel_dec_main` and **RQR** `tel_rcold_fix - tel_rcold_main`.
- **Before pairs** (`SMALL_BEFORE=1`), printed as `BEFORE (not gated)` with no verdict: `dec_fw6 - dec_main` and `rcold_fw6 - rcold_main`.
- **BEAM flags:** every arm runs with `+sbwt none +sbwtdcpu none +sbwtdio none`, so idle arms don't busy-wait.
- **Rounds:** each round runs one block of `BENCH_N` dispatches per arm, the arm order rotating every round. There are **3 warm-up rounds** plus `BENCH_WARM` warm-up dispatches per arm, then `BLOCKS` counted rounds. A block's value is its median dispatch time in µs. A pair is the difference of two arms' blocks within the same round.

## Store modes

Pick one with `STORE=...` for **both** `./setup.sh` and `./run.sh` (the same value each time). Every default below can be overridden in the environment.

| `STORE` | Store at start | `BENCH_WARM` | `BENCH_N` | `BLOCKS` | Ceiling | Trace every | Arms |
|---|---|---|---|---|---|---|---|
| `large` (default) | the 8 facts only | 300 | 51 | 401 | none | 50 rounds | the eight arms above |
| `small2k` | near production size: `GATE_START_EVENTS=1000` | 50 | 5 | 51 | `SMALL_CEILING=6000` | 10 rounds | the four gated arms |
| `small20k` | just below the flip: `GATE_START_EVENTS=20000` | 50 | 11 | 41 | `SMALL_CEILING=27000` | 10 rounds | the four gated arms |

With `SMALL_BEFORE=1` (pass it to `setup.sh` too, which then also builds fw6) the two before-arms join, for six arms.

- **Small setup:** after the eight facts, `init.exs` appends gate-shaped filler until `$all` holds `GATE_START_EVENTS` events. Each filler pair is a `Gate.Registered` (tagged `email:<fp>:<digest>`) and a `Gate.Released` (tagged `user:<holder>`) on a fresh holder stream. Then it runs `ANALYZE` on every store table, so the planner has statistics as a production store's autovacuum would give it. Setup prints `store events after setup: N`.
- **Growth arithmetic:** every dispatch appends 3 events (the seed's two and the dispatch's one). The projected growth is `arms x 3 x (BENCH_WARM + BENCH_N x (BLOCKS + 3))`. `small2k`: 12 x (50 + 5 x 54) = 3840, so the store ends near 4.8k. `small20k`: 12 x (50 + 11 x 44) = 6408, so it ends near 26.4k. With `SMALL_BEFORE=1` there are six arms (18 per unit) and, unless you set `BLOCKS`, `env.sh` lowers it until start + growth fits under the ceiling.
- **Ceiling:** the driver prints the projection and **refuses to start** if start + projection exceeds `SMALL_CEILING`. Every trace sample also records the `$all` head (`store N` on each `TRACE` line); a sample above `SMALL_CEILING` stops the run with the verdict `INCOMPLETE (store left the small range)` and no numbers print.
- **Plan check (small only):** after the arms stop, inside the results block, the rig renders each side's own read statement (`Decree.Store.Storage.Reader.Query.read_query_statement`), runs `EXPLAIN` on the facts' query on the final store, and prints `plan check <side> at store N: Seq Scan on stream_events: yes|no`. Expected: `yes` for main (proof the store stayed below the flip) and `no` for the fix.
- The results print the store size at start and at end.

## Requirements

- **Toolchain:** the one decree's `mise.toml` pins at every commit, **Erlang 29.1.2** and **Elixir 1.20.4-otp-29**, through [mise](https://mise.jdx.dev). `setup.sh` runs `mise install` in the exported fix tree.
- **Postgres 18.6** with **`shared_buffers` = 3 GB**:
  - The original laptop used Docker:
    ```bash
    docker run -d --name fw6-gate-pg -e POSTGRES_PASSWORD=postgres -p 127.0.0.1:5432:5432 postgres:18.6 -c shared_buffers=3GB -c max_connections=200
    ```
  - **Native Postgres on Linux is fine and preferred** (no VM in the way). Set `shared_buffers = 3GB` and restart.
  - Point the rig at it with `GATE_PG_HOST`, `GATE_PG_PORT`, `GATE_PG_USER` and `GATE_PG_PASSWORD` (defaults: `127.0.0.1`, `5432`, `postgres`, `postgres`). If you use Docker under another container name, set `GATE_PG_CONTAINER`.
  - The rig creates and drops only the database `gate_7752`.
- Python 3 and `top`, plus `docker` if you use it, for the trace. The trace reads the store size with `psql`: through `docker exec $GATE_PG_CONTAINER psql` when `GATE_PG_CONTAINER` is set, else `psql` from PATH with the `GATE_PG_*` settings.

## Steps

Run order: **`small2k`, then `small20k`, then `large`**, each after a fresh `./setup.sh` with the same `STORE`.

```bash
git fetch origin main fix/7752-read-index-lookups   # the commits must be present
export BRANCH_SHA=<the fix commit>                  # unless env.sh names it
STORE=small2k ./setup.sh    # exports and builds the sides, makes a fresh small store
SMOKE=1 STORE=small2k ./run.sh   # optional: checks the run path in a minute or so; its numbers mean nothing
STORE=small2k ./setup.sh         # again: a fresh store for the real run
STORE=small2k ./run.sh
STORE=small20k ./setup.sh && STORE=small20k ./run.sh
STORE=large ./setup.sh && STORE=large ./run.sh    # about 25 to 45 minutes
```

`SMALL_BEFORE=1` (optional, small stores only) goes on both the `setup.sh` and the `run.sh` line. `setup.sh` ends with `SETUP OK`. Before that it prints the two `append_events functions md5` lines (main and fix), which must be equal, the Postgres version and `shared_buffers`, `facts appended: 8` and, for a small store, `store events after setup: N`. **Run `setup.sh` again before every run**: the store must be fresh.

## Start and void rules (coordinator rulings on #7627)

- **Start:**
  - **No BEAM or `mix` may be running anywhere on the machine.** `run.sh` checks this first, and the driver checks it again just before it starts the arms. Either check prints `START RULE NOT MET: foreign BEAM`, with each PID's full command line, and exits without running. Check yourself beforehand with `pgrep -fl 'beam\.smp|(^|/)mix( |$)'`.
  - Then `run.sh` waits until the 1-, 5- and 15-minute load averages are all under 4. It waits up to 60 minutes and otherwise exits without running.
- **Void** (coordinator ruling, 2026-11-03): the run is void if, in any trace sample, any of these holds:
  - **a BEAM or `mix` outside the rig is running** (the rig's own are the driver and its descendants, which are the four arms);
  - **a container other than the rig's own Postgres container (`GATE_PG_CONTAINER`) is above 10% CPU**;
  - **the 1-minute load is above 15**.
- **Not void:** OS and desktop processes (`loginwindow`, `WindowServer`, `kernel_task` and so on), Docker's VM, Postgres and the sampling tools, at any CPU. Each sample lists every process outside the rig above 10% under `others >10% (not void)`.
- The run **stops at the first void sample**, because measuring on would report nothing.
- The trace samples at the start, every 50 counted rounds (every 10 in a small store) and at the end. Each sample records the 1-minute load, the store size (the `$all` head), its void reasons (or `none`), the other busy processes, the five busiest processes and every container's CPU.
- **A void run prints no numbers.** The results block says `VOID`.
- **Raw rows:** every run that starts the arms, void or not, writes `gate-rows.json` next to the scripts: the commits, the store preset and start size, the arms, the trace, the verdict and every counted round's order and block values. It prints `rows written:` with the path, the round count and the file's sha256. An earlier `gate-rows.json` is renamed to `gate-rows.<time>.json`, never overwritten. The file is for the coordinator: **the operator never reads or interprets `gate-rows*.json`.**
- Keep the machine otherwise idle for the run: no builds, tests, browsers or video.

## Decomposition run (not the gate)

`DECOMPOSE=1 ./run.sh` runs the same arms ungated: no start rule, no threshold, and nothing voids or stops the run. The trace still records each condition that would void the gate, as `would void the gate:`. Every number prints, and `gate-rows.json` is written as usual. It adds `rt_fix`, the index facts' `read_tags` call alone: the round trip FW-6's dispatch no longer sends. It exists for `STORE=large` only; a small store refuses it. Run `./setup.sh` before it, as before any run.

## What to send back

Everything `run.sh` prints from `=== 7752 gate run` to `=== END RESULTS`:
- the store preset, the commits, host and CPU count;
- the start and end loads;
- the store line (`events at start`, the projection) and every `TRACE` line (each with `store N`);
- the verdict;
- the store size at start and end;
- each arm's level, Q2 and Q3 as paired medians with both halves, p10, p90, rounds, and passes or TRIPPED; the RQ and RQR span pairs (large) and the `BEFORE (not gated)` pairs (`SMALL_BEFORE=1`), only on a VALID verdict;
- the two `plan check` lines (small stores);
- the `rows written:` line.

Also send the md5, Postgres, `facts appended` and `store events after setup` lines from `setup.sh`. Keep `gate-rows*.json` unread and unchanged for the coordinator to ask for. **The operator never reads or interprets `gate-rows*.json`.**

## Files

- `env.sh`: the settings and the `STORE` presets.
- `setup.sh`: export, build, fresh store.
- `run.sh`: the start rule and the run.
- `drive.py`: the rotation, the trace, the ceiling, `gate-rows.json`, the plan check call and the results.
- `server.exs`: one arm.
- `init.exs`: store setup, the facts and the small-store filler.
- `plancheck.exs`: the end-of-run plan check.
- `lib/gate.ex` and `config/config.exs`: the fixture decision, rule, events and store.
- `mix.exs`: the bench project. `DECREE_PATH` picks the side.
