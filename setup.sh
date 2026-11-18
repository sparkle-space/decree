#!/usr/bin/env bash
# Builds both sides from this clone's git objects and makes a fresh store with the eight facts.
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$GATE_HOME"

git cat-file -e "$MAIN_SHA^{commit}" && git cat-file -e "$BRANCH_SHA^{commit}" && git cat-file -e "$FW6_SHA^{commit}" || {
  echo "missing a commit: run 'git fetch origin $MAIN_SHA $BRANCH_SHA $FW6_SHA' first (the fix and FW-6 branches were deleted on merge; GitHub serves the commits by SHA)"; exit 1; }

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
# STORE=prod: init.exs prefills W1's shape after the facts (GATE_PRODSHAPE=1, AG_HOME, AG_SCALE).
DECREE_PATH=$WORK/fix GATE_BUILD=$WORK/build-fix BENCH_ARM=fix GATE_RESET=0 GATE_FACTS=1 GATE_PRODSHAPE=${GATE_PRODSHAPE:-0} \
  mix run --no-compile --no-deps-check --no-start init.exs
# Method B (sparklespace#7787): one login role per arm, so pg_stat_statements keeps each arm's
# statements apart (it keys on the role). drive.py connects each arm as arm_<name>. The roles
# hold pg_checkpoint for the cadence arm, and the gate database's privileges only.
pgx() {
  if [ -n "${GATE_PG_CONTAINER:-}" ] && docker inspect "$GATE_PG_CONTAINER" >/dev/null 2>&1; then
    docker exec -i -e GATE_ROLE_PW "$GATE_PG_CONTAINER" psql -X -q -v ON_ERROR_STOP=1 -U "$GATE_PG_USER" -d "$GATE_DB" "$@"
  else
    PGPASSWORD="$GATE_PG_PASSWORD" psql -X -q -v ON_ERROR_STOP=1 -h "$GATE_PG_HOST" -p "$GATE_PG_PORT" -U "$GATE_PG_USER" -d "$GATE_DB" "$@"
  fi
}
# Only with method B on (AG_HOME set): without it the arms connect as GATE_PG_USER, as before, so
# a server the rig does not own gets no cluster-wide roles.
GATE_ROLE_PW="$GATE_PG_PASSWORD" ; export GATE_ROLE_PW
[ -n "${AG_HOME:-}" ] && for arm in dec_main dec_fix rcold_main rcold_fix tel_dec_main tel_dec_fix tel_rcold_main tel_rcold_fix rt_fix dec_fw6 rcold_fw6; do
  pgx -v role="arm_$arm" <<'SQL'
\getenv pw GATE_ROLE_PW
SELECT format('CREATE ROLE %I LOGIN', :'role') WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'role') \gexec
SELECT format('ALTER ROLE %I PASSWORD %L', :'role', :'pw') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'role') \gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'role') \gexec
SELECT format('GRANT ALL ON ALL TABLES IN SCHEMA public TO %I', :'role') \gexec
SELECT format('GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO %I', :'role') \gexec
SELECT format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO %I', :'role') \gexec
SELECT format('GRANT pg_checkpoint TO %I', :'role') \gexec
SQL
done
[ -n "${AG_HOME:-}" ] && echo "arm roles ready: arm_<arm name>, one per arm"
pgx -At -c "SELECT 'store events after setup: ' || stream_version FROM streams WHERE stream_id = 0" -c "SELECT 'server: shared_buffers ' || current_setting('shared_buffers') || ', data_checksums ' || current_setting('data_checksums') || ', wal_log_hints ' || current_setting('wal_log_hints') || ', pg_stat_statements ' || (current_setting('shared_preload_libraries') LIKE '%pg_stat_statements%')"
pgx -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" 2>/dev/null || echo "note: pg_stat_statements is not loaded on this server: method B is unavailable"
if [ "${GATE_PRODSHAPE:-0}" = 1 ] && [ -n "${AG_HOME:-}" ]; then
  python3 "$AG_HOME/shape_check.py" "$GATE_PG_CONTAINER" "$GATE_DB" public --scale "${AG_SCALE:-1.0}" || { echo "the production-shaped store is off its shape"; exit 1; }
fi
# The setup record drive.py checks before a STORE=prod run: this preset, scale and head, so a
# reused or grown store is refused.
head=$(pgx -At -c "SELECT stream_version FROM streams WHERE stream_id = 0")
python3 -c 'import json,sys; json.dump({"rig": sys.argv[1], "store": sys.argv[2], "scale": float(sys.argv[3]), "head": int(sys.argv[4])}, open(sys.argv[5], "w"))' \
  "$RIG" "${STORE:-large}" "${AG_SCALE:-1.0}" "$head" "$WORK/setup.json"
echo "SETUP OK"
