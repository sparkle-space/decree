#!/usr/bin/env bash
# The FW-6 four-arm gate (sparkle-space/sparklespace#7627): waits for the start rule, then runs.
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
export ARMS="dec_main:$WORK/main:decision:main,dec_fw6:$WORK/branch:decision:branch,rcold_main:$WORK/main:rule_cold:main,rcold_fw6:$WORK/branch:rule_cold:branch"
export PAIRS="dec_main-dec_fw6,rcold_main-rcold_fw6"
# DECOMPOSE=1 is not the gate: three more branch arms isolate the index facts' `read_tags` round
# trip, nothing voids or stops the run, and every number prints.
if [ "${DECOMPOSE:-0}" = 1 ]; then
  ARMS="$ARMS,dec_fw6_rt:$WORK/branch:decision_rt:branch,tel_fw6:$WORK/branch:decision_tel:branch,rt_fw6:$WORK/branch:read_tags:branch"
  PAIRS="$PAIRS,dec_fw6-dec_fw6_rt"
fi
export BLOCKS=${BLOCKS:-401} BENCH_N=${BENCH_N:-51} BENCH_WARM=${BENCH_WARM:-300}
# SMOKE=1 checks the run path only: two tiny rounds, no start rule, numbers meaningless.
if [ "${SMOKE:-0}" = 1 ]; then export BLOCKS=2 BENCH_N=3 BENCH_WARM=5; fi

loads() { /usr/bin/env python3 -c 'import os; print(" ".join("%.2f" % l for l in os.getloadavg()))'; }
quiet() { /usr/bin/env python3 -c 'import os,sys; sys.exit(0 if all(l < 4 for l in os.getloadavg()) else 1)'; }

if [ "${SMOKE:-0}" = 1 ]; then echo "=== SMOKE RUN: checks the scripts only; its numbers mean nothing and are never reported"; fi
if [ "${DECOMPOSE:-0}" = 1 ]; then echo "=== DECOMPOSITION RUN, not the gate: no start rule, nothing voids it"; fi
# The start rule's first check: no BEAM or `mix` may be running. drive.py checks again just
# before it starts the arms. A decomposition run only lists them.
foreign=$(pgrep -f 'beam\.smp|(^|/)mix( |$)' || true)
if [ -n "$foreign" ] && [ "${DECOMPOSE:-0}" = 1 ]; then
  echo "note: BEAM or mix running (not void in a decomposition run)"
  for pid in $foreign; do echo "  pid $pid: $(ps -o command= -p "$pid" | cut -c1-160)"; done
elif [ -n "$foreign" ]; then
  echo "START RULE NOT MET: foreign BEAM"
  for pid in $foreign; do echo "  pid $pid: $(ps -o command= -p "$pid")"; done
  echo "Stop each one that is yours, or report them and stop."
  exit 2
fi
echo "=== FW-6 gate run"
echo "main $MAIN_SHA"
echo "branch $BRANCH_SHA"
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
