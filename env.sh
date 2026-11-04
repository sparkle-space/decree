# Sourced by setup.sh and run.sh. Override any of these in the environment.
GATE_HOME=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export GATE_HOME
# pre-FW-6 decree main (FW-5's merge): the baseline.
export MAIN_SHA=${MAIN_SHA:-0b41deec8a06882343db7cea97e004061bfd39ad}
# The fix for sparklespace#7752 (decree branch fix/7752-read-index-lookups).
export BRANCH_SHA=${BRANCH_SHA:?set BRANCH_SHA to the fix commit}
# FW-6 without the fix (decree main's merge of FW-6); built only with SMALL_BEFORE=1.
export FW6_SHA=${FW6_SHA:-747fcea72039bb36ed467cfe0a83bfd30296be61}
export GATE_PG_HOST=${GATE_PG_HOST:-127.0.0.1}
export GATE_PG_PORT=${GATE_PG_PORT:-5432}
export GATE_PG_USER=${GATE_PG_USER:-postgres}
export GATE_PG_PASSWORD=${GATE_PG_PASSWORD:-postgres}
# The Docker container running Postgres, if any; its CPU is expected and not foreign.
export GATE_PG_CONTAINER=${GATE_PG_CONTAINER:-fw6-gate-pg}
export GATE_DB=gate_7752
export WORK=$GATE_HOME/work
export DECREE_DEPS=$WORK/fix/deps DECREE_LOCK=$WORK/fix/mix.lock MIX_ENV=prod
unset MIX_HOME MIX_ARCHIVES

# STORE=large (default): the long eight-arm run on the facts-only store.
# STORE=small2k / small20k: a store filled with gate-shaped filler, four gated arms.
export STORE=${STORE:-large}
export SMALL_BEFORE=${SMALL_BEFORE:-0}
case "$STORE" in
  large) ;;
  small2k|small20k)
    if [ "$STORE" = small2k ]; then
      _start=1000; _warm=50; _n=5; _blocks=51; _ceiling=6000
    else
      _start=20000; _warm=50; _n=11; _blocks=41; _ceiling=27000
    fi
    export GATE_START_EVENTS=${GATE_START_EVENTS:-$_start}
    export BENCH_WARM=${BENCH_WARM:-$_warm} BENCH_N=${BENCH_N:-$_n}
    export SMALL_CEILING=${SMALL_CEILING:-$_ceiling}
    _arms=4; [ "$SMALL_BEFORE" = 1 ] && _arms=6
    # Growth: every dispatch appends 3 events; blocks + 3 warm-up rounds of BENCH_N dispatches.
    # BLOCKS, if not set, is lowered until start + growth fits under the ceiling.
    if [ -z "${BLOCKS:-}" ]; then
      BLOCKS=$_blocks
      while [ $BLOCKS -gt 1 ] && [ $((GATE_START_EVENTS + _arms * 3 * (BENCH_WARM + BENCH_N * (BLOCKS + 3)))) -gt "$SMALL_CEILING" ]; do
        BLOCKS=$((BLOCKS - 1))
      done
    fi
    export BLOCKS
    ;;
  *) echo "STORE must be large, small2k or small20k"; return 2 2>/dev/null || exit 2 ;;
esac

# decree's own toolchain (mise.toml at all commits: erlang 29.1.2, elixir 1.20.4-otp-29).
toolchain_path() { (cd "$WORK/fix" && mise exec -- bash -c 'echo $PATH'); }
