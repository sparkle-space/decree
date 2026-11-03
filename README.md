# FW-6 stop gate rig (sparkle-space/sparklespace#7627)

**A benchmark branch, never to be merged.** It holds scripts only. It measures decree's FW-6 change (PR sparkle-space/decree#20) against decree main. Both sides are built from this clone's git objects with `git archive`, so nothing outside git is needed.

| Side | Commit |
|---|---|
| main | `0b41deec8a06882343db7cea97e004061bfd39ad` (FW-5's merge) |
| branch | `57cc85c27f207c955ceca5eb55b902962eb92bfc` (decree#20 after the founder's design change: the dispatch's read returns the framework hits' tags, with no separate `read_tags`) |

## What it measures

Eight long-lived BEAMs, one per arm, share **one store**. FW-6 has no migration, and `append_events*` is identical on both sides; setup prints its md5 twice to show it.

| Arm | Code | Shape |
|---|---|---|
| `dec_main` | main | a decision with design doc §4's two-part query, both reads running: each dispatch first seeds a holder registered and then released under a fresh digest. No conflict, a new identity per dispatch |
| `dec_fw6` | branch | the same decision, with the index item and an enabled index |
| `rcold_main` | main | a rule route with the same query, and a new aggregate per dispatch (cold) |
| `rcold_fw6` | branch | the same rule route, with the index item and an enabled index |
| `tel_dec_main`, `tel_dec_fw6` | main, branch | the decision again; the value is the dispatch's own `read_query` time, from decree's telemetry |
| `tel_rcold_main`, `tel_rcold_fw6` | main, branch | the same for the cold rule route |

The four `tel_` arms are span arms. Each attaches a telemetry handler in its own BEAM, so the handlers never touch the four gated arms. A span arm also ends itself if a dispatch sends any `read_tags` statement: FW-6's dispatch sends none, and main's fresh dispatch sends none either.

Every arm uses the same tags (`email:<fp>:<digest>`) and the same `index_families/0` (`%{"email" => :digest}`). The only difference within a pair is decree's code.

- **Rounds:** blocks of **51** dispatches per arm per round. The arm order rotates every round. There are **3 warm-up rounds** plus **300 warm-up dispatches per arm**, then **401 counted rounds**. A block's value is its median dispatch time in µs. A pair is the difference of two arms' blocks within the same round.
- **BEAM flags:** every arm runs with `+sbwt none +sbwtdcpu none +sbwtdio none`, so idle arms don't busy-wait.
- **Gated quantities**, each a paired median with a threshold of **+500 µs**:
  - **Q2:** `dec_fw6 − dec_main`.
  - **Q3:** `rcold_fw6 − rcold_main`.
- **Span pairs**, ungated and printed without a verdict:
  - **RQ:** `tel_dec_fw6 − tel_dec_main`, what FW-6 adds to a decision's `read_query` time (the index item's rows and the tags returned with them).
  - **RQR:** the same for the rule route.

## Requirements

- **Toolchain:** the one decree's `mise.toml` pins at both commits, **Erlang 29.1.2** and **Elixir 1.20.4-otp-29**, through [mise](https://mise.jdx.dev). `setup.sh` runs `mise install` in the exported branch tree.
- **Postgres 18.6** with **`shared_buffers` = 3 GB**:
  - The original laptop used Docker:
    ```bash
    docker run -d --name fw6-gate-pg -e POSTGRES_PASSWORD=postgres -p 127.0.0.1:5432:5432 postgres:18.6 -c shared_buffers=3GB -c max_connections=200
    ```
  - **Native Postgres on Linux is fine and preferred** (no VM in the way). Set `shared_buffers = 3GB` and restart.
  - Point the rig at it with `GATE_PG_HOST`, `GATE_PG_PORT`, `GATE_PG_USER` and `GATE_PG_PASSWORD` (defaults: `127.0.0.1`, `5432`, `postgres`, `postgres`). If you use Docker under another container name, set `GATE_PG_CONTAINER`.
  - The rig creates and drops only the database `gate_fw6`.
- Python 3 and `top`, plus `docker` if you use it, for the trace.

## Steps

```bash
git fetch origin main feature/fw6-index-facts   # both commits must be present
./setup.sh    # exports both sides, builds them, and makes a fresh store with the eight index facts
SMOKE=1 ./run.sh   # optional: checks the run path in about a minute; its numbers mean nothing
./setup.sh         # again: a fresh store for the real run
./run.sh      # waits for the start rule, then runs; about 25 to 45 minutes
```

`setup.sh` ends with `SETUP OK`. Before that it prints the two `append_events functions md5` lines, which must be equal, the Postgres version and `shared_buffers`, and `facts appended: 8`. **Run `setup.sh` again before every run**: the store must be fresh.

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
- The trace samples at the start, every 50 counted rounds and at the end. Each sample records the 1-minute load, its void reasons (or `none`), the other busy processes, the five busiest processes and every container's CPU.
- **A void run prints no numbers.** The results block says `VOID`.
- **Raw rows:** every run that starts the arms, void or not, writes `gate-rows.json` next to the scripts: the commits, the arms, the trace, the verdict and every counted round's order and block values. It prints `rows written:` with the path, the round count and the file's sha256. An earlier `gate-rows.json` is renamed to `gate-rows.<time>.json`, never overwritten. The file is for the coordinator: the operator neither reads nor interprets it.
- Keep the machine otherwise idle for the run: no builds, tests, browsers or video.

## Decomposition run (not the gate)

`DECOMPOSE=1 ./run.sh` runs the same arms ungated: no start rule, no threshold, and nothing voids or stops the run. The trace still records each condition that would void the gate, as `would void the gate:`. Every number prints, and `gate-rows.json` is written as usual. It adds `rt_fw6`, the index facts' `read_tags` call alone: the round trip FW-6's dispatch no longer sends. Run `./setup.sh` before it, as before any run.

## What to send back

Everything `run.sh` prints from `=== FW-6 gate run` to `=== END RESULTS`:
- the commits, host and CPU count;
- the start and end loads;
- every `TRACE` line;
- the verdict;
- each arm's level, Q2 and Q3 as paired medians with both halves, p10, p90, rounds, and passes or TRIPPED, and the RQ and RQR span pairs (only on a VALID verdict);
- the `rows written:` line.

Also send the md5, Postgres and `facts appended` lines from `setup.sh`. Keep `gate-rows.json` unread and unchanged for the coordinator to ask for.

## Files

- `env.sh`: the settings.
- `setup.sh`: export, build, fresh store.
- `run.sh`: the start rule and the run.
- `drive.py`: the rotation, the trace, `gate-rows.json` and the results.
- `server.exs`: one arm.
- `init.exs`: store setup and the facts.
- `lib/gate.ex` and `config/config.exs`: the fixture decision, rule, events and store.
- `mix.exs`: the bench project. `DECREE_PATH` picks the side.
