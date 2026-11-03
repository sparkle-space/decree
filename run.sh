#!/usr/bin/env bash
# The FW-6 four-arm gate (sparkle-space/sparklespace#7627): waits for the start rule, then runs.
set -uo pipefail
source "$(dirname "$0")/env.sh"
cd "$GATE_HOME"
PATH="$(toolchain_path)"
export PATH
export ARMS="dec_main:$WORK/main:decision:main,dec_fw6:$WORK/branch:decision:branch,rcold_main:$WORK/main:rule_cold:main,rcold_fw6:$WORK/branch:rule_cold:branch"
export PAIRS="dec_main-dec_fw6,rcold_main-rcold_fw6"
export BLOCKS=${BLOCKS:-401} BENCH_N=${BENCH_N:-51} BENCH_WARM=${BENCH_WARM:-300}

loads() { /usr/bin/env python3 -c 'import os; print(" ".join("%.2f" % l for l in os.getloadavg()))'; }
quiet() { /usr/bin/env python3 -c 'import os,sys; sys.exit(0 if all(l < 4 for l in os.getloadavg()) else 1)'; }

echo "=== FW-6 gate run"
echo "main $MAIN_SHA"
echo "branch $BRANCH_SHA"
echo "host $(uname -srm); cpus $(getconf _NPROCESSORS_ONLN)"
date
waited=0
until quiet; do
  if [ $waited -ge 3600 ]; then echo "START RULE NOT MET within 60 min (loads $(loads)): not run"; exit 2; fi
  sleep 30; waited=$((waited + 30))
done
echo "start loads (1/5/15 min): $(loads)"
/usr/bin/env python3 "$GATE_HOME/drive.py"
echo "DRIVE_EXIT=$?"
date
echo "end loads (1/5/15 min): $(loads)"
