# Sourced by setup.sh and run.sh. Override any of these in the environment.
GATE_HOME=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export GATE_HOME
# pre-FW-6 decree main (FW-5's merge): the baseline.
export MAIN_SHA=${MAIN_SHA:-0b41deec8a06882343db7cea97e004061bfd39ad}
# RIG=7752 (default): the #7752 read-index fix against pre-FW-6 main.
# RIG=fw6: bench/fw6-gate's gate (sparklespace#7627), FW-6 (decree#20 head 57cc85c) against the same
# main: that branch's eight arms are this rig's STORE=large arms with FW-6 as the branch side. The
# side is still named `fix` here (arms dec_fix, rcold_fix, ...).
export RIG=${RIG:-7752}
case "$RIG" in
  7752) _branch=7a1ef3208b93d2341c735cc4b8afff46282bd392; _db=gate_7752 ;;
  fw6) _branch=57cc85c27f207c955ceca5eb55b902962eb92bfc; _db=gate_fw6 ;;
  *) echo "RIG must be 7752 or fw6"; return 2 2>/dev/null || exit 2 ;;
esac
# The branch side: the fix for sparklespace#7752 (decree branch fix/7752-read-index-lookups), or FW-6.
export BRANCH_SHA=${BRANCH_SHA:-$_branch}
# FW-6 without the fix (decree main's merge of FW-6); built only with SMALL_BEFORE=1.
export FW6_SHA=${FW6_SHA:-747fcea72039bb36ed467cfe0a83bfd30296be61}
# Method A + B (sparklespace#7787): GATE_INSTANCE names an owned instance with production's
# settings, made by the sparklespace checkout's dev/bench/append-gate/prod-instance.sh, and AG_HOME
# is that checkout's dev/bench/append-gate. With GATE_INSTANCE set, the instance's port, password
# and container replace GATE_PG_PORT, GATE_PG_PASSWORD and GATE_PG_CONTAINER.
if [ -n "${GATE_INSTANCE:-}" ]; then
  : "${AG_HOME:?set AG_HOME to dev/bench/append-gate}"
  bash "$AG_HOME/prod-instance.sh" check "$GATE_INSTANCE" >/dev/null || { echo "instance check failed: run $AG_HOME/prod-instance.sh check $GATE_INSTANCE"; return 3 2>/dev/null || exit 3; }
  eval "$(bash "$AG_HOME/prod-instance.sh" env "$GATE_INSTANCE")"
  GATE_PG_PORT=$AG_PG_PORT GATE_PG_CONTAINER=$AG_CONTAINER GATE_PG_USER=postgres
  GATE_PG_PASSWORD="$(cat "$AG_PG_PASSWORD_FILE")"
  export AG_HOME GATE_INSTANCE
fi
export GATE_PG_HOST=${GATE_PG_HOST:-127.0.0.1}
export GATE_PG_PORT=${GATE_PG_PORT:-5432}
export GATE_PG_USER=${GATE_PG_USER:-postgres}
export GATE_PG_PASSWORD=${GATE_PG_PASSWORD:-postgres}
# The Docker container running Postgres, if any; its CPU is expected and not foreign.
export GATE_PG_CONTAINER=${GATE_PG_CONTAINER:-fw6-gate-pg}
export GATE_DB=$_db
export WORK=$GATE_HOME/work
export DECREE_DEPS=$WORK/fix/deps DECREE_LOCK=$WORK/fix/mix.lock MIX_ENV=prod
unset MIX_HOME MIX_ARCHIVES

# STORE=large (default): the long eight-arm run on the facts-only store.
# STORE=prod: the large store's four gated arms on a production-shaped store (method A; needs
#   GATE_INSTANCE and AG_HOME): the facts, then W1's shape through AppendGate.Prefill.
# STORE=small2k / small20k: a store filled with gate-shaped filler, four gated arms.
# CADENCE=N (STORE=prod only; 0 = none): every block is N appends after a CHECKPOINT, and its value
#   is its mean. A gated dispatch is two appends (the seed's and its own), so a block is N/2 of them.
export STORE=${STORE:-large}
export SMALL_BEFORE=${SMALL_BEFORE:-0}
export CADENCE=${CADENCE:-0}
case "$CADENCE" in ''|*[!0-9]*) echo "CADENCE must be a non-negative integer"; return 2 2>/dev/null || exit 2 ;; esac
if [ "$CADENCE" != 0 ] && [ "$STORE" != prod ]; then echo "CADENCE is for STORE=prod"; return 2 2>/dev/null || exit 2; fi
if [ "$CADENCE" != 0 ] && [ $((CADENCE % 2)) != 0 ]; then echo "CADENCE must be even: a gated dispatch is two appends"; return 2 2>/dev/null || exit 2; fi
case "$STORE" in
  large) ;;
  prod)
    [ -n "${GATE_INSTANCE:-}" ] || { echo "STORE=prod needs GATE_INSTANCE (and AG_HOME): a production-settings instance"; return 2 2>/dev/null || exit 2; }
    export GATE_PRODSHAPE=1
    ;;
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
  *) echo "STORE must be large, prod, small2k or small20k"; return 2 2>/dev/null || exit 2 ;;
esac

# decree's own toolchain (mise.toml at all commits: erlang 29.1.2, elixir 1.20.4-otp-29).
toolchain_path() { (cd "$WORK/fix" && mise exec -- bash -c 'echo $PATH'); }
