#!/usr/bin/env bash
# Builds both sides from this clone's git objects and makes a fresh store with the eight facts.
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$GATE_HOME"

git cat-file -e "$MAIN_SHA^{commit}" && git cat-file -e "$BRANCH_SHA^{commit}" || {
  echo "missing a commit: run 'git fetch origin main feature/fw6-index-facts' first"; exit 1; }

rm -rf "${WORK:?}"
mkdir -p "$WORK/main" "$WORK/branch"
git archive "$MAIN_SHA" | tar -x -C "$WORK/main"
git archive "$BRANCH_SHA" | tar -x -C "$WORK/branch"
cmp "$WORK/main/mix.lock" "$WORK/branch/mix.lock" && echo "mix.lock identical on both sides"

(cd "$WORK/branch" && mise trust --yes . >/dev/null 2>&1 || true; mise install)
PATH="$(toolchain_path)"
export PATH
erl -noshell -eval 'io:format("otp ~s~n", [erlang:system_info(otp_release)]), halt().'
elixir --version | tail -1
(cd "$WORK/branch" && mix local.hex --force --if-missing >/dev/null && mix local.rebar --force >/dev/null && mix deps.get)

for arm in main branch; do
  export DECREE_PATH=$WORK/$arm GATE_BUILD=$WORK/build-$arm BENCH_ARM=$arm
  mix deps.compile > "$WORK/$arm-deps.log" 2>&1 || { echo "deps.compile failed for $arm, see $WORK/$arm-deps.log"; exit 1; }
  mix compile > "$WORK/$arm-compile.log" 2>&1 || { echo "compile failed for $arm, see $WORK/$arm-compile.log"; exit 1; }
  echo "built $arm"
done

# A fresh store: main drops, creates and initialises; the branch side appends the facts.
DECREE_PATH=$WORK/main GATE_BUILD=$WORK/build-main BENCH_ARM=main GATE_RESET=1 GATE_FACTS=0 \
  mix run --no-compile --no-deps-check --no-start init.exs
DECREE_PATH=$WORK/branch GATE_BUILD=$WORK/build-branch BENCH_ARM=branch GATE_RESET=0 GATE_FACTS=1 \
  mix run --no-compile --no-deps-check --no-start init.exs
echo "SETUP OK"
