# Sourced by setup.sh and run.sh. Override any of these in the environment.
GATE_HOME=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export GATE_HOME
export MAIN_SHA=${MAIN_SHA:-0b41deec8a06882343db7cea97e004061bfd39ad}
export BRANCH_SHA=${BRANCH_SHA:-57cc85c27f207c955ceca5eb55b902962eb92bfc}
export GATE_PG_HOST=${GATE_PG_HOST:-127.0.0.1}
export GATE_PG_PORT=${GATE_PG_PORT:-5432}
export GATE_PG_USER=${GATE_PG_USER:-postgres}
export GATE_PG_PASSWORD=${GATE_PG_PASSWORD:-postgres}
# The Docker container running Postgres, if any; its CPU is expected and not foreign.
export GATE_PG_CONTAINER=${GATE_PG_CONTAINER:-fw6-gate-pg}
export GATE_DB=gate_fw6
export WORK=$GATE_HOME/work
export DECREE_DEPS=$WORK/branch/deps DECREE_LOCK=$WORK/branch/mix.lock MIX_ENV=prod
unset MIX_HOME MIX_ARCHIVES
# decree's own toolchain (mise.toml at both commits: erlang 29.1.2, elixir 1.20.4-otp-29).
toolchain_path() { (cd "$WORK/branch" && mise exec -- bash -c 'echo $PATH'); }
