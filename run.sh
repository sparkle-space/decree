#!/usr/bin/env bash
# The #7752 read-index gate (sparkle-space/sparklespace#7752; rig lineage #7627): waits for the start rule, then runs.
set -uo pipefail
source "$(dirname "$0")/env.sh"
cd "$GATE_HOME"
PATH="$(toolchain_path)"
export PATH
# The arms must run decree's pinned toolchain. A python3 that is a version-manager shim (mise)
# re-resolves the toolchain for its children from the cwd, which purges the build, so the
# driver gets the absolute mix and this PATH and passes them on.
GATE_MIX="$(command -v mix)" GATE_PATH="$PATH"
export GATE_MIX GATE_PATH
echo "toolchain: $GATE_MIX; $(elixir --version | tail -1)"
# The four gated arms; in the large store also the span arms (each dispatch's own read_query
# time, on each side). SMALL_BEFORE=1 adds the FW-6 before-arms (not gated).
case "$STORE" in
  large)
    export ARMS="dec_main:$WORK/main:decision:main,dec_fix:$WORK/fix:decision:fix,rcold_main:$WORK/main:rule_cold:main,rcold_fix:$WORK/fix:rule_cold:fix,\
tel_dec_main:$WORK/main:decision_span:main,tel_dec_fix:$WORK/fix:decision_span:fix,\
tel_rcold_main:$WORK/main:rule_cold_span:main,tel_rcold_fix:$WORK/fix:rule_cold_span:fix"
    export PAIRS="dec_main-dec_fix,rcold_main-rcold_fix,tel_dec_main-tel_dec_fix,tel_rcold_main-tel_rcold_fix"
    # DECOMPOSE=1 is not the gate: nothing voids or stops the run, and every number prints. It adds
    # the index facts' read_tags call alone, the round trip FW-6's dispatch no longer sends.
    if [ "${DECOMPOSE:-0}" = 1 ]; then
      ARMS="$ARMS,rt_fix:$WORK/fix:read_tags:fix"
    fi
    export BLOCKS=${BLOCKS:-401} BENCH_N=${BENCH_N:-51} BENCH_WARM=${BENCH_WARM:-300}
    ;;
  *)
    if [ "${DECOMPOSE:-0}" = 1 ]; then echo "DECOMPOSE=1 is only for STORE=large"; exit 2; fi
    export ARMS="dec_main:$WORK/main:decision:main,dec_fix:$WORK/fix:decision:fix,rcold_main:$WORK/main:rule_cold:main,rcold_fix:$WORK/fix:rule_cold:fix"
    export PAIRS="dec_main-dec_fix,rcold_main-rcold_fix"
    if [ "${SMALL_BEFORE:-0}" = 1 ]; then
      ARMS="$ARMS,dec_fw6:$WORK/fw6:decision:fw6,rcold_fw6:$WORK/fw6:rule_cold:fw6"
      PAIRS="$PAIRS,dec_main-dec_fw6,rcold_main-rcold_fw6"
    fi
    ;;
esac
# SMOKE=1 checks the run path only: two tiny rounds, no start rule, numbers meaningless.
if [ "${SMOKE:-0}" = 1 ]; then export BLOCKS=2 BENCH_N=3 BENCH_WARM=5; fi

loads() { /usr/bin/env python3 -c 'import os; print(" ".join("%.2f" % l for l in os.getloadavg()))'; }
quiet() { /usr/bin/env python3 -c 'import os,sys; sys.exit(0 if all(l < 4 for l in os.getloadavg()) else 1)'; }

if [ "${SMOKE:-0}" = 1 ]; then echo "=== SMOKE RUN: checks the scripts only; its numbers mean nothing and are never reported"; fi
if [ "${DECOMPOSE:-0}" = 1 ]; then echo "=== DECOMPOSITION RUN, not the gate: no start rule, nothing voids it"; fi
# The start rule's first check: no BEAM or `mix` may be running. drive.py checks again just
# before it starts the arms. A decomposition run only lists them.
foreign=$(pgrep -f 'beam\.smp|(^|/)mix( |$)' || true)
if [ -n "$foreign" ] && { [ "${DECOMPOSE:-0}" = 1 ] || [ "${SMOKE:-0}" = 1 ]; }; then
  echo "note: BEAM or mix running (not void in a smoke or decomposition run)"
  for pid in $foreign; do echo "  pid $pid: $(ps -o command= -p "$pid" | cut -c1-160)"; done
elif [ -n "$foreign" ]; then
  echo "START RULE NOT MET: foreign BEAM"
  for pid in $foreign; do echo "  pid $pid: $(ps -o command= -p "$pid")"; done
  echo "Stop each one that is yours, or report them and stop."
  exit 2
fi
echo "=== 7752 gate run"
echo "store $STORE${SMALL_BEFORE:+ (SMALL_BEFORE=$SMALL_BEFORE)}"
echo "main (pre-FW-6) $MAIN_SHA"
echo "fix $BRANCH_SHA"
[ "${SMALL_BEFORE:-0}" = 1 ] && echo "fw6 $FW6_SHA"
echo "host $(uname -srm); cpus $(getconf _NPROCESSORS_ONLN)"
date
waited=0
until [ "${SMOKE:-0}" = 1 ] || [ "${DECOMPOSE:-0}" = 1 ] || quiet; do
  if [ $waited -ge 3600 ]; then echo "START RULE NOT MET within 60 min (loads $(loads)): not run"; exit 2; fi
  sleep 30; waited=$((waited + 30))
done
echo "start loads (1/5/15 min): $(loads)"
/usr/bin/env python3 "$GATE_HOME/drive.py"
echo "DRIVE_EXIT=$?"
date
echo "end loads (1/5/15 min): $(loads)"
