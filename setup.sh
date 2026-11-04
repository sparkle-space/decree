#!/usr/bin/env bash
# Builds both sides from this clone's git objects and makes a fresh store with the eight facts.
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$GATE_HOME"

git cat-file -e "$MAIN_SHA^{commit}" && git cat-file -e "$BRANCH_SHA^{commit}" && git cat-file -e "$FW6_SHA^{commit}" || {
  echo "missing a commit: run 'git fetch origin main fix/7752-read-index-lookups' first"; exit 1; }

rm -rf "${WORK:?}"
mkdir -p "$WORK/main" "$WORK/fix"
sides="main fix"
if [ "${SMALL_BEFORE:-0}" = 1 ]; then mkdir -p "$WORK/fw6"; sides="main fix fw6"; fi
# To a file first: stock macOS bsdtar can end a `git archive | tar -x` pipe early (SIGPIPE, 141).
git archive -o "$WORK/main.tar" "$MAIN_SHA"
git archive -o "$WORK/fix.tar" "$BRANCH_SHA"
tar -xf "$WORK/main.tar" -C "$WORK/main"
tar -xf "$WORK/fix.tar" -C "$WORK/fix"
cmp "$WORK/main/mix.lock" "$WORK/fix/mix.lock" && echo "mix.lock identical on main and fix"
if [ "${SMALL_BEFORE:-0}" = 1 ]; then
  git archive -o "$WORK/fw6.tar" "$FW6_SHA"
  tar -xf "$WORK/fw6.tar" -C "$WORK/fw6"
  cmp "$WORK/main/mix.lock" "$WORK/fw6/mix.lock" && echo "mix.lock identical on fw6"
fi

(cd "$WORK/fix" && mise trust --yes . >/dev/null 2>&1 || true; mise install)
PATH="$(toolchain_path)"
export PATH
erl -noshell -eval 'io:format("otp ~s~n", [erlang:system_info(otp_release)]), halt().'
elixir --version | tail -1
(cd "$WORK/fix" && mix local.hex --force --if-missing >/dev/null && mix local.rebar --force >/dev/null && mix deps.get)

for arm in $sides; do
  export DECREE_PATH=$WORK/$arm GATE_BUILD=$WORK/build-$arm BENCH_ARM=$arm
  mix deps.compile > "$WORK/$arm-deps.log" 2>&1 || { echo "deps.compile failed for $arm, see $WORK/$arm-deps.log"; exit 1; }
  mix compile > "$WORK/$arm-compile.log" 2>&1 || { echo "compile failed for $arm, see $WORK/$arm-compile.log"; exit 1; }
  echo "built $arm"
done

# A fresh store: main drops, creates and initialises; the fix side appends the facts and, for a
# small store, the filler (GATE_FILLER = the start size) and ANALYZEs every table.
DECREE_PATH=$WORK/main GATE_BUILD=$WORK/build-main BENCH_ARM=main GATE_RESET=1 GATE_FACTS=0 \
  mix run --no-compile --no-deps-check --no-start init.exs
GATE_FILLER=0
case "${STORE:-large}" in small2k|small20k) GATE_FILLER=$GATE_START_EVENTS ;; esac
export GATE_FILLER
DECREE_PATH=$WORK/fix GATE_BUILD=$WORK/build-fix BENCH_ARM=fix GATE_RESET=0 GATE_FACTS=1 \
  mix run --no-compile --no-deps-check --no-start init.exs
echo "SETUP OK"
