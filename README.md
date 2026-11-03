# FW-6 stop gate rig (sparkle-space/sparklespace#7627)

**A benchmark branch, never to be merged.** It holds scripts only. It measures decree's FW-6 change (PR sparkle-space/decree#20) against decree main. Both sides are built from this clone's git objects with `git archive`, so nothing outside git is needed.

| Side | Commit |
|---|---|
| main | `0b41deec8a06882343db7cea97e004061bfd39ad` (FW-5's merge) |
| branch | `e5d1998834381b4b41cc439a8f4e1831cd0d965e` (decree#20's head when this rig was written) |

## What it measures

Four long-lived BEAMs, one per arm, share **one store**. FW-6 has no migration, and `append_events*` is identical on both sides; setup prints its md5 twice to show it.

| Arm | Code | Shape |
|---|---|---|
| `dec_main` | main | a decision with design doc §4's two-part query, both reads running: each dispatch first seeds a holder registered and then released under a fresh digest. No conflict, a new identity per dispatch |
| `dec_fw6` | branch | the same decision, with the index item and an enabled index |
| `rcold_main` | main | a rule route with the same query, and a new aggregate per dispatch (cold) |
| `rcold_fw6` | branch | the same rule route, with the index item and an enabled index |

Every arm uses the same tags (`email:<fp>:<digest>`) and the same `index_families/0` (`%{"email" => :digest}`). The only difference within a pair is decree's code.

- **Rounds:** blocks of **51** dispatches per arm per round. The arm order rotates every round. There are **3 warm-up rounds** plus **300 warm-up dispatches per arm**, then **401 counted rounds**. A block's value is its median dispatch time in µs. A pair is the difference of two arms' blocks within the same round.
- **BEAM flags:** every arm runs with `+sbwt none +sbwtdcpu none +sbwtdio none`, so idle arms don't busy-wait.
- **Gated quantities**, each a paired median with a threshold of **+500 µs**:
  - **Q2:** `dec_fw6 − dec_main`.
  - **Q3:** `rcold_fw6 − rcold_main`.

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
./run.sh      # waits for the start rule, then runs; about 25 to 40 minutes
```

`setup.sh` ends with `SETUP OK`. Before that it prints the two `append_events functions md5` lines, which must be equal, the Postgres version and `shared_buffers`, and `facts appended: 8`. **Run `setup.sh` again before every run**: the store must be fresh.

## Start and void rules (coordinator rulings on #7627)

- **Start:** `run.sh` starts only when the 1-, 5- and 15-minute load averages are all under 4. It waits up to 60 minutes and otherwise exits without running.
- **Void:** the run is void if **any process outside the rig is above 10% CPU** in any trace sample, if **any 1-minute load reading is above 15**, or if another BEAM was running when the arms started.
  - The rig is its four BEAMs, the driver, Postgres (native, or the container and Docker's VM) and the sampling tools.
  - The trace samples at the start, every 50 counted rounds and at the end. Each sample records the 1-minute load, the five busiest processes, every container's CPU, and anything foreign.
  - **A void run reports no numbers.** The results block says `VOID`.
- Keep the machine otherwise idle for the run: no builds, tests, browsers or video.

## What to send back

Everything `run.sh` prints from `=== FW-6 gate run` to `=== END RESULTS`:
- the commits, host and CPU count;
- the start and end loads;
- every `TRACE` line;
- the verdict;
- each arm's level, and Q2 and Q3 as paired medians with both halves, p10, p90, rounds, and passes or TRIPPED.

Also send the md5, Postgres and `facts appended` lines from `setup.sh`.

## Files

- `env.sh`: the settings.
- `setup.sh`: export, build, fresh store.
- `run.sh`: the start rule and the run.
- `drive.py`: the rotation, the trace and the results.
- `server.exs`: one arm.
- `init.exs`: store setup and the facts.
- `lib/gate.ex` and `config/config.exs`: the fixture decision, rule, events and store.
- `mix.exs`: the bench project. `DECREE_PATH` picks the side.
