#!/usr/bin/env bash
# Prove that axond's durable state can be restored, and that a restore lands at a
# point in time an operator chose.
#
# A backup procedure that has never been restored is a hypothesis. This drill is
# the executable form of the recovery objectives in
# `docs/operations/backup-and-recovery.md`, run against a real PostgreSQL of the
# supported version, with axond itself as the verifier at both ends: the state a
# recovery has to bring back is published through `axond admin` against a running
# replica, and the recovered database is only accepted if a *second* replica,
# booted on it, reads the same head revision and accepts a publication against
# it. Nothing here writes a control-plane row by hand — a drill that did would be
# restoring a database no replica ever produced.
#
# Two recoveries, because they fail differently:
#
#   1. a logical dump and restore — what a `pg_dump` in a nightly job gives you,
#      and what a migration to a new cluster uses;
#   2. point-in-time recovery from a base backup plus archived WAL to a target
#      time between two publications, which is the only recovery that answers
#      "undo the last twenty minutes". The assertion that matters is asymmetric:
#      everything published before the target is present, and the revision
#      published after it is gone. A restore that replayed to the end of the WAL
#      would pass a "the data is there" check and be useless for the incident it
#      exists for.
#
# This is the `restore-drill` lane of `qualification/recovery/manifest.toml`. It
# runs the eight stages the manifest gives it and writes their evidence to
# `target/recovery/` in the same schema the in-process lane writes, through
# `ops/recovery-evidence.py`. Conditions are *recorded* and then judged at the
# end of each stage rather than aborting it, so a stage that fails still leaves
# an artifact saying what it observed. `ops/check-recovery-evidence.py` then
# refuses a run whose stages left nothing.
#
# Redis is disposable hot state in this drill: the fixture provides the shared
# lease backend needed to enforce projected concurrency policy, but no Redis
# contents are recovered or counted as durable evidence. Reservations,
# rate-limit windows, and revocation caches are intentionally outside the
# restore boundary.
#
# Usage:
#     ops/restore-drill.sh              # the whole drill, ~2 minutes
#     AXOND_BIN=/path/to/axond ops/restore-drill.sh
#
# Needs Docker and a `cargo` build (or `AXOND_BIN`). Nothing outside the
# container is written except a temporary config directory and the evidence.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The supported backend version, from the single place that records it.
image="${AXOND_DRILL_POSTGRES_IMAGE:-$(
  sed -n 's/^ *image: *\(postgres:[^ ]*\) *$/\1/p' "${root}/.github/workflows/ci.yml" | head -n 1
)}"
redis_image="${AXOND_DRILL_REDIS_IMAGE:-redis:7.4.2-alpine}"
container="${AXOND_DRILL_CONTAINER:-axond-restore-drill}"
redis_container="${AXOND_DRILL_REDIS_CONTAINER:-axond-restore-drill-redis}"
live_port="${AXOND_DRILL_LIVE_PORT:-55442}"
restored_port="${AXOND_DRILL_RESTORED_PORT:-55443}"
redis_port="${AXOND_DRILL_REDIS_PORT:-56379}"
# One replica per database, because the point of the drill is that a *replica*
# reads the recovered journal, not that psql can select from it.
live_http="${AXOND_DRILL_LIVE_HTTP:-18442}"
logical_http="${AXOND_DRILL_LOGICAL_HTTP:-18443}"
recovered_http="${AXOND_DRILL_RECOVERED_HTTP:-18444}"
survivor_proxy_port="${AXOND_DRILL_SURVIVOR_PROXY_PORT:-55444}"
provider_port="${AXOND_DRILL_PROVIDER_PORT:-18445}"
password=drill
archive=/tmp/wal-archive
basebackup=/tmp/basebackup

workdir="$(mktemp -d)"
replicas=()
helper_processes=()
cleanup() {
  for pid in "${replicas[@]:-}"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${helper_processes[@]:-}"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  docker rm --force --volumes "$container" >/dev/null 2>&1 || true
  docker rm --force --volumes "$redis_container" >/dev/null 2>&1 || true
  if [[ "${AXOND_DRILL_KEEP_WORKDIR:-0}" == 1 ]]; then
    printf 'restore drill workdir retained at %s\n' "$workdir" >&2
  else
    rm -rf "$workdir"
  fi
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$1"; }
fail() {
  printf 'restore drill failed: %s\n' "$1" >&2
  exit 1
}

evidence="${root}/ops/recovery-evidence.py"
# The recorders read the manifest, which needs a TOML reader: `tomllib` from
# Python 3.11, or the `tomli` backport the deploy lockfile pins on the 3.10 ops
# floor. Resolve one now rather than half way through a drill, and prefer the
# venv `just ops-venv` builds when the ambient interpreter has neither.
reads_toml() {
  "$1" -c 'import importlib.util as u, sys
sys.exit(0 if u.find_spec("tomllib") or u.find_spec("tomli") else 1)' >/dev/null 2>&1
}
python_bin="${AXOND_PYTHON:-python3}"
if ! reads_toml "$python_bin"; then
  if reads_toml "${root}/target/ops-venv/bin/python"; then
    python_bin="${root}/target/ops-venv/bin/python"
  else
    fail "$python_bin has neither \`tomllib\` (Python 3.11+) nor the \`tomli\` backport; run \`just ops-venv\` or set AXOND_PYTHON"
  fi
fi
# The stage currently recording. Set by `stage`, read by the recorders, so a
# check reads as the condition it states rather than as plumbing.
log=""

# The checks the stage recorded, and whether each held. A gate is a summary of
# checks, so it is derived from these rather than asserted alongside them: an
# artifact whose gate says `met` next to the failed check it summarises is the
# kind of evidence this harness exists to make impossible.
check_held=()

stage() {
  log="${workdir}/$(printf '%s' "$1" | tr / .).log"
  check_held=()
  step "Stage $1"
  "$python_bin" "$evidence" start --log "$log" --stage "$1" \
    --schema "$2" --schema-identity "$3"
}
mark() { "$python_bin" "$evidence" mark --log "$log" --event "$1" --detail "$2"; }
observe() { "$python_bin" "$evidence" observe --log "$log" --key "$1" --value "$2" --type "${3:-text}"; }
gate() {
  "$python_bin" "$evidence" gate --log "$log" --gate "$1" --observed "$2" --met "$3" --detail "$4"
}
defer() { "$python_bin" "$evidence" defer --log "$log" --gate "$1" --why "$2"; }
# A condition the stage requires: recorded either way, judged by `close`.
require() {
  local check="$1" wanted="$2" got="$3" detail="$4"
  "$python_bin" "$evidence" require --log "$log" --check "$check" \
    --expected "$wanted" --observed "$got" --detail "$detail"
  if [[ "$got" == "$wanted" ]]; then
    check_held+=("$check")
    printf '  ok  %s = %s\n' "$check" "$got"
  else
    printf '  FAIL %s: expected %s, got %s\n' "$check" "$wanted" "$got"
  fi
}
# Whether every named check of this stage held. A name nothing recorded has not
# held, so a renamed or dropped check cannot leave a gate quietly passing.
held() {
  local check held_check
  for check in "$@"; do
    held_check=""
    for held_check in "${check_held[@]}"; do
      [[ "$held_check" == "$check" ]] && break
    done
    [[ "$held_check" == "$check" ]] || return 1
  done
}
# `true`/`false` for the gate recorder, from the checks that decide the gate.
verdict() { held "$@" && printf 'true' || printf 'false'; }
# Writes the artifact, then fails the drill if the stage failed. In that order,
# because an unexplained failure is the one an operator cannot act on.
close() {
  "$python_bin" "$evidence" finish --log "$log" || failed_stages+=("$(basename "$log" .log)")
}
failed_stages=()

# Setup failures happen before the normal restore stage starts. Retain a
# durable-inventory artifact for them rather than letting `set -e` turn a
# missing store table or refused lifecycle transition into an unexplained
# missing artifact.
record_durable_setup_failure() {
  local detail="$1"
  stage backup-restore/durable-inventory logical_restore "$schema_identity"
  mark "setup-failed" "$detail"
  require "the_secret_store_setup_succeeds" true false "$detail"
  defer readiness "setup failed before this stage could inspect a recovered replica"
  defer max_serving_error_fraction "setup failed before this stage could offer inference traffic"
  defer max_convergence_lag_seconds "setup failed before this stage could observe convergence"
  defer max_data_loss_revisions "the restore stage owns the revision loss boundary"
  defer admin_writes "setup failed before this stage could attempt an administrative write"
  defer max_unauthenticated_admin_successes "the administration stage owns restored-surface authentication"
  close
  fail "$detail"
}
# When this run began, so the checker can reject an artifact a previous run left
# behind: a stale file is indistinguishable from a stage that ran.
drill_started_ms=$(($(date +%s%N) / 1000000))

# psql inside the container: the client is the server's own, so no version skew,
# and `ON_ERROR_STOP` makes a failed statement a failed drill.
psql() {
  local database="$1" port="$2"
  shift 2
  docker exec -i -u postgres "$container" \
    psql -v ON_ERROR_STOP=1 -qtAX -p "$port" -d "$database" "$@"
}

command -v openssl >/dev/null 2>&1 || fail "openssl is required for this run's secrets and executable identity"

axond_bin="${AXOND_BIN:-${root}/target/release/axond}"
if [[ ! -x "$axond_bin" && "$axond_bin" == "${root}/target/release/axond" ]]; then
  step "Building axond, the drill's verifier"
  cargo build -p axond --locked --release --manifest-path "${root}/Cargo.toml"
fi
[[ -x "$axond_bin" ]] || fail "no axond binary at ${axond_bin}"

recovery_cargo_profile="release"
recovery_axond_sha256="$(openssl dgst -sha256 -r "$axond_bin" | awk '{print $1}')"
[[ "$recovery_axond_sha256" =~ ^[0-9a-f]{64}$ ]] ||
  fail "the release axond executable produced a malformed SHA-256 digest"
if [[ -n "${AXOND_RECOVERY_EXECUTABLE_SHA256:-}" &&
      "$AXOND_RECOVERY_EXECUTABLE_SHA256" != "$recovery_axond_sha256" ]]; then
  fail "AXOND_RECOVERY_EXECUTABLE_SHA256 does not identify the AXOND_BIN bytes"
fi
export AXOND_RECOVERY_EXECUTABLE_SHA256="$recovery_axond_sha256"
export AXOND_RECOVERY_CARGO_PROFILE="$recovery_cargo_profile"

command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v curl >/dev/null 2>&1 || fail "curl is required to probe the drill's replicas"
command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v nc >/dev/null 2>&1 || fail "nc is required to probe the loopback qualification helpers"

step "Starting ${image} with WAL archiving"
docker rm --force --volumes "$container" >/dev/null 2>&1 || true
docker rm --force --volumes "$redis_container" >/dev/null 2>&1 || true
# `archive_mode` is a restart-only setting, so it is given at startup rather than
# turned on afterwards; the archiver retries, so the directory can appear late.
docker run --detach --name "$container" \
  --env POSTGRES_PASSWORD="$password" \
  --publish "127.0.0.1:${live_port}:5432" \
  --publish "127.0.0.1:${restored_port}:5433" \
  "$image" \
  -c wal_level=replica \
  -c archive_mode=on \
  -c "archive_command=test ! -f ${archive}/%f && cp %p ${archive}/%f" \
  -c max_wal_senders=4 >/dev/null
docker exec -u postgres "$container" mkdir -p "$archive"

# Over TCP rather than the socket: the image's initdb-time server listens on the
# socket alone, so a socket probe can report ready before the real server exists
# and the first statement would race its shutdown.
for _ in $(seq 60); do
  if docker exec -u postgres "$container" pg_isready -q -h 127.0.0.1 -p 5432; then break; fi
  sleep 1
done
docker exec -u postgres "$container" pg_isready -h 127.0.0.1 -p 5432 >/dev/null ||
  fail "postgres did not become ready"

step "Starting ${redis_image} for shared lease enforcement"
docker run --detach --name "$redis_container" \
  --publish "127.0.0.1:${redis_port}:6379" \
  "$redis_image" >/dev/null
for _ in $(seq 30); do
  nc -z 127.0.0.1 "$redis_port" >/dev/null 2>&1 && break
  sleep 1
done
nc -z 127.0.0.1 "$redis_port" >/dev/null 2>&1 ||
  fail "redis did not become reachable"

# The provider and the switchable control-plane path are both loopback-only.
# The provider returns the committed compatibility fixture; the proxy changes
# only the PostgreSQL database/cluster behind one replica's stable DSN and closes
# active sockets on every switch, so the replica has to reconnect and converge.
PYTHONPATH="${root}/tests/compat" "$python_bin" -c \
  'from fake_upstream import serve_forever; import sys; serve_forever(int(sys.argv[1]))' \
  "$provider_port" >"${workdir}/provider.log" 2>&1 &
provider_pid="$!"
helper_processes+=("$provider_pid")
for _ in $(seq 30); do
  nc -z 127.0.0.1 "$provider_port" >/dev/null 2>&1 && break
  sleep 1
done
nc -z 127.0.0.1 "$provider_port" >/dev/null 2>&1 ||
  fail "the loopback provider fixture did not become reachable"

"$python_bin" "${root}/ops/recovery-proxy.py" \
  --listen-port "$survivor_proxy_port" \
  --live-port "$live_port" \
  --recovered-port "$restored_port" >"${workdir}/survivor-proxy.log" 2>&1 &
proxy_pid="$!"
helper_processes+=("$proxy_pid")
for _ in $(seq 30); do
  nc -z 127.0.0.1 "$survivor_proxy_port" >/dev/null 2>&1 && break
  sleep 1
done
nc -z 127.0.0.1 "$survivor_proxy_port" >/dev/null 2>&1 ||
  fail "the switchable survivor control-plane proxy did not become reachable"

# One database per recovery, so the logical restore is checked against its source
# rather than replacing it.
psql postgres 5432 -c 'CREATE DATABASE live' >/dev/null
psql postgres 5432 -c 'CREATE DATABASE logical_restore' >/dev/null

# Writes a stateful bootstrap config and points the environment at one database.
# Sets `drill_config` and `GW_DRILL_DSN` rather than printing them: the DSN is an
# export, and an export made inside a command substitution is lost with the
# subshell that made it.
config() {
  local database="$1" port="$2" http="${4:-0}"
  local catalog_source="${5:-none}" catalog_bootstrap="${6:-empty}"
  local catalog_create_table="${7:-false}"
  drill_config="${workdir}/${3}"
  cat >"$drill_config" <<EOF
mode = "stateful"

[server]
bind = "127.0.0.1:${http}"

[control_plane]
dsn_env = "GW_DRILL_DSN"

[secret_store]
kek_env = "GW_DRILL_KEK"

[catalog]
source = "${catalog_source}"
store = "postgres"
bootstrap = "${catalog_bootstrap}"
create_table = ${catalog_create_table}

[budget]
backend = "postgres"
dsn_env = "GW_DRILL_DSN"
table = "axond_budget"
create_table = false
namespace_scope = true

[rate_limit]
backend = "redis"
dsn_env = "GW_DRILL_REDIS_DSN"

[[usage_sink]]
kind = "postgres"
dsn_env = "GW_DRILL_DSN"
table = "axond_usage"
create_table = false

[usage_journal]
backend = "postgres"
dsn_env = "GW_DRILL_DSN"
create_schema = false
consumer = "restore-drill"

[[admin_breakglass]]
env = "GW_DRILL_BREAKGLASS"
EOF
  export GW_DRILL_DSN="postgres://postgres:${password}@127.0.0.1:${port}/${database}"
  export GW_DRILL_REDIS_DSN="redis://127.0.0.1:${redis_port}"
}

# Both secrets are generated per run and never printed: a drill that shipped a
# fixed credential in the repository is a credential someone eventually points at
# something real, and one that echoed the generated one would put it in a CI log.
# `check-recovery-evidence.py --forbid-env` is given both variable names — not
# their values, which would land in the process listing — so an artifact
# carrying either fails the run.
# The key-encryption key is base64 of 32 bytes, the shape `DeploymentKek::parse`
# accepts: hex of the same length decodes as base64 to 48 bytes and is refused.
GW_DRILL_KEK="$(openssl rand -base64 32)"
GW_DRILL_BREAKGLASS="$(openssl rand -hex 24)"
GW_DRILL_PROVIDER_KEY="$(openssl rand -hex 24)"
export GW_DRILL_KEK GW_DRILL_BREAKGLASS
# `axond admin` reads its credential from the environment rather than from a flag,
# which keeps it out of the process listing and out of this script's own output.
export AXOND_ADMIN_TOKEN="$GW_DRILL_BREAKGLASS"
# The shape, not the value: a key of the wrong length is refused by
# `DeploymentKek::parse` the moment a stage stages material, and a drill that
# only ever publishes secret *references* would not find out until then.
[[ "$(printf '%s' "$GW_DRILL_KEK" | base64 -d 2>/dev/null | wc -c | tr -d ' ')" == 32 ]] ||
  fail "the generated key-encryption key is not 32 bytes of base64"

# Boots a replica against a database and waits for its administrative surface.
# The replica is the verifier: every read and publication below goes through
# `/admin/v1` rather than through SQL.
serve() {
  local name="$1" http="$2" logfile="${workdir}/${1}.serve.log"
  AXOND_CONFIG="$drill_config" "$axond_bin" >"$logfile" 2>&1 &
  replicas+=("$!")
  for _ in $(seq 60); do
    if curl -fsS -m 2 "http://127.0.0.1:${http}/healthz" >/dev/null 2>&1; then
      endpoint="http://127.0.0.1:${http}"
      export AXOND_ADMIN_ENDPOINT="$endpoint"
      printf '  %s replica listening on %s\n' "$name" "$endpoint"
      return 0
    fi
    sleep 1
  done
  # A replica that will not boot on a recovered database is the regression this
  # drill exists to catch, so inside a stage it is recorded and the artifact is
  # written before the run stops.
  if [[ -n "$log" ]]; then
    require "the_${name}_replica_becomes_reachable" reachable unreachable \
      "a database no replica can boot on is not a recovered database"
    close
  fi
  fail "the ${name} replica did not become reachable: $(cat "$logfile")"
}

admin() {
  "$axond_bin" admin "$@" --operator drill --reason "restore drill"
}

# Publishes one resource document against the current head, and echoes the new
# head. Every revision this drill has to recover was made this way.
publish() {
  local resource="$1" file="$2" key="$3" expected="$4"
  admin apply --resource "$resource" -f "$file" \
    --idempotency-key "$key" --expected-revision "$expected" |
    jq -r .revision
}

head_revision() { admin state | jq -r .revision; }
revision_count() { admin history --limit 100 | jq '.revisions | length'; }
resource_count() { admin state | jq '.resources | length'; }

# Poll a replica's own convergence report after the proxy changes its database
# or cluster. The final report is returned even on timeout so the stage can
# retain the failure as evidence instead of aborting before the artifact closes.
wait_for_survivor() {
  local expected="$1" report=""
  for _ in $(seq 60); do
    report="$(AXOND_ADMIN_ENDPOINT="$live_endpoint" admin convergence 2>/dev/null || true)"
    if [[ -n "$report" ]] && jq -e --arg expected "$expected" \
      '.converged == true and .active == $expected and .loaded == $expected and .source == "control-plane"' \
      <<<"$report" >/dev/null 2>&1; then
      printf '%s' "$report"
      return 0
    fi
    sleep 1
  done
  printf '%s' "$report"
  return 1
}

probe_survivor_chat() {
  local body="$1"
  curl -sS -o "$body" -w '%{http_code}' \
    -H "Authorization: Bearer ${workload_key}" \
    -H 'content-type: application/json' \
    -d '{"model":"fixture-chat","messages":[{"role":"user","content":"recovery drill"}]}' \
    "${live_endpoint}/v1/chat/completions" 2>/dev/null || printf '000'
}

# Billing-grade usage is written to the outbox before the request is answered,
# but its Postgres sink is delivered asynchronously. Wait for the durable row
# before taking the recovery target so the drill measures a settled event rather
# than a request that was merely accepted by the journal.
usage_count() {
  psql "$1" "$2" -c 'SELECT count(*) FROM axond_usage'
}

latest_usage_row_id() {
  psql "$1" "$2" -c 'SELECT coalesce(max(id), 0) FROM axond_usage'
}

usage_rows_after_id() {
  local database="$1" port="$2" baseline_id="$3"
  psql "$database" "$port" -c \
    "SELECT count(*) FROM axond_usage WHERE id > ${baseline_id}"
}

usage_request_after_id() {
  local database="$1" port="$2" baseline_id="$3"
  psql "$database" "$port" -c \
    "SELECT CASE WHEN count(*) = 1 THEN min(request_id) ELSE '' END FROM axond_usage WHERE id > ${baseline_id}"
}

usage_identity_count() {
  local database="$1" port="$2" table="$3" request_id="$4"
  case "$table" in
    axond_usage|axond_usage_outbox) ;;
    *) printf '0'; return 0 ;;
  esac
  if [[ ! "$request_id" =~ ^req_[0-9a-f-]{36}$ ]]; then
    printf '0'
    return 0
  fi
  psql "$database" "$port" -c \
    "SELECT count(*) FROM ${table} WHERE request_id = '${request_id}'"
}

wait_for_usage_count() {
  local database="$1" port="$2" expected="$3" observed=0
  for _ in $(seq 60); do
    observed="$(usage_count "$database" "$port")"
    if [[ "$observed" -ge "$expected" ]]; then
      printf '%s' "$observed"
      return 0
    fi
    sleep 1
  done
  printf '%s' "$observed"
  return 1
}

# What an unauthenticated caller gets from the administrative surface. Two
# callers, because they fail at different places: no credential at all, and a
# wrong one.
unauthenticated_successes() {
  local base="$1" successes=0 status
  for header in "" "Authorization: Bearer not-the-drill-credential"; do
    if [[ -z "$header" ]]; then
      status="$(curl -s -o /dev/null -w '%{http_code}' "${base}/admin/v1/state")"
    else
      status="$(curl -s -o /dev/null -w '%{http_code}' -H "$header" "${base}/admin/v1/state")"
    fi
    [[ "$status" == "200" ]] && successes=$((successes + 1))
  done
  printf '%s' "$successes"
}

step "Installing the control-plane schema with axond migrate apply"
config live "$live_port" live.toml "$live_http" seed seed true
live_config="$drill_config"
"$axond_bin" migrate apply --config "$live_config"
schema_identity="$("$axond_bin" migrate status --config "$live_config")" ||
  fail "the freshly migrated schema is not current"
schema_identity="$(printf '%s' "$schema_identity" | tr '\n' ' ')"

step "Applying the usage, budget, and revocation schemas"
for sql in usage_v2 usage_v2_001_add_price_identity budget_v1 budget_v2 revocation_v1; do
  psql live 5432 -f - <"${root}/ops/postgres/${sql}.sql" >/dev/null
done
psql live 5432 -f - <"${root}/ops/postgres/usage_outbox_v1.sql" >/dev/null

step "Applying the encrypted secret-store schema"
psql live 5432 -f - <"${root}/ops/postgres/secret_store_v1.sql" >/dev/null

step "Building the deployment a recovery has to bring back, through axond admin"
config live "$survivor_proxy_port" survivor.toml "$live_http" seed seed true
serve live "$live_http"
live_endpoint="$endpoint"

# Catalogue import is asynchronous after healthz is reachable. Wait for its
# active pointer before reading the pointer or its snapshot metadata; a health
# check alone does not mean the seeded catalogue has been published.
catalog_content_id=""
for _ in $(seq 60); do
  catalog_content_id="$(psql live 5432 -c \
    'SELECT content_id FROM axond_catalog_active WHERE singleton' 2>/dev/null || true)"
  [[ -n "$catalog_content_id" ]] && break
  sleep 1
done
[[ "$catalog_content_id" == sha256:* ]] ||
  fail "catalogue import did not publish an active pointer within 60 seconds"
catalog_raw_digest="$(psql live 5432 -c \
  "SELECT raw_digest FROM axond_catalog_snapshot WHERE content_id = '${catalog_content_id}'")"
catalog_raw_bytes="$(psql live 5432 -c \
  "SELECT raw_bytes FROM axond_catalog_snapshot WHERE content_id = '${catalog_content_id}'")"
[[ "$catalog_raw_digest" == sha256:* && "$catalog_raw_bytes" -gt 0 ]] ||
  fail "the seeded catalogue did not retain a valid raw snapshot"
catalog_digest="$catalog_raw_digest"
catalog_size="$catalog_raw_bytes"
catalog_content="$catalog_content_id"

tenant=ten_01900000-0000-7000-8000-000000000001
provider=res_01900000-0000-7000-8000-000000000010
project=prj_01900000-0000-7000-8000-000000000002
principal=prn_01900000-0000-7000-8000-000000000003
catalog=res_01900000-0000-7000-8000-000000000013
enablement=res_01900000-0000-7000-8000-000000000012
alias=res_01900000-0000-7000-8000-000000000015
workload_key="axw1.$(printf '%064d' 0 | tr 0 d)"
workload_digest="sha256:$(printf '%s' "$workload_key" | openssl dgst -sha256 -r | awk '{print $1}')"
offering="$("$python_bin" -c '
import hashlib, struct
def string(value):
    encoded = value.encode()
    return b"\x03" + struct.pack(">Q", len(encoded)) + encoded
canonical = b"axond.desired-state\x00\x01" + b"\x07" + struct.pack(">Q", 2)
canonical += string("model") + string("gpt-4o")
canonical += string("provider") + string("openai")
print("off_" + hashlib.sha256(canonical).hexdigest())
')"
provider_url="http://127.0.0.1:${provider_port}"
cat >"${workdir}/tenant.json" <<EOF
{"summary":"onboard the drill tenant","mutation":"create","resource":{
  "tenant":"${tenant}","slug":"drill","display_name":"Drill"}}
EOF
cat >"${workdir}/project.json" <<EOF
{"summary":"add the drill project","mutation":"create","resource":{
  "project":"${project}","tenant":"${tenant}",
  "slug":"production","display_name":"Production"}}
EOF
cat >"${workdir}/principal.json" <<EOF
{"summary":"register the drill workload","mutation":"create","resource":{
  "principal":"${principal}","tenant":"${tenant}","project":"${project}",
  "slug":"drill-workload","display_name":"Drill workload",
  "key_digest":"${workload_digest}","roles":["operator"]}}
EOF
cat >"${workdir}/provider.json" <<EOF
{"summary":"connect the drill tenant to openai","mutation":"create","resource":{
  "provider":"${provider}","tenant":"${tenant}","slug":"openai",
  "display_name":"OpenAI","wire_family":"openai-chat","endpoint":"${provider_url}"}}
EOF
head=empty
head="$(publish tenants "${workdir}/tenant.json" "drill-tenants" "$head")"
printf '  published %-12s -> %s\n' tenants "$head"

# A credential is a *reference* to staged material, never the material. Publish
# the tenant first so the secret store's ownership check has a durable control-
# plane resource to authorize against before staging or activating the secret.
secret_stage_output="$(printf '%s' "$GW_DRILL_PROVIDER_KEY" |
  admin secret stage --tenant "$tenant" --material-file - 2>/dev/null || true)"
secret_ref="$(printf '%s' "$secret_stage_output" |
  jq -r '.reference // "missing"' 2>/dev/null || printf 'missing')"
secret_id="${secret_ref%@*}"
secret_reference="$secret_ref"
secret_version="${secret_ref##*@v}"
[[ "$secret_ref" == sct_*@v1 ]] ||
  record_durable_setup_failure "secret staging did not return a valid reference"
if ! admin secret lifecycle --tenant "$tenant" --reference "$secret_ref" \
  --state active >/dev/null 2>&1; then
  record_durable_setup_failure "secret lifecycle activation was refused"
fi
cat >"${workdir}/credential.json" <<EOF
{"summary":"stage the drill openai key","mutation":"create","resource":{
  "credential":"res_01900000-0000-7000-8000-000000000011","tenant":"${tenant}",
  "provider":"${provider}","slug":"openai-primary","display_name":"OpenAI primary",
  "secret":"${secret_id}","secret_version":${secret_version},"lifecycle":"active"}}
EOF
cat >"${workdir}/catalog.json" <<EOF
{"summary":"retain the seed catalogue","mutation":"create","resource":{
  "catalog":"${catalog}","slug":"seed",
  "digest":"${catalog_raw_digest}","size_bytes":${catalog_size}}}
EOF
cat >"${workdir}/model.json" <<EOF
{"summary":"enable the drill fixture model","mutation":"create","resource":{
  "enablement":"${enablement}","tenant":"${tenant}","project":"${project}",
  "slug":"fixture-chat","offering":"${offering}","catalog":"${catalog}",
  "snapshot":"${catalog_digest}","wire_family":"openai-chat","state":"enabled"}}
EOF
cat >"${workdir}/alias.json" <<EOF
{"summary":"publish the drill fixture alias","mutation":"create","resource":{
  "alias":"${alias}","tenant":"${tenant}","project":"${project}",
  "slug":"fixture-chat","wire_family":"openai-chat","state":"enabled",
  "targets":[{"enablement":"${enablement}"}]}}
EOF
cat >"${workdir}/price.json" <<EOF
{"summary":"approve the drill price book","mutation":"create","resource":{
  "price_book":"res_01900000-0000-7000-8000-000000000014","slug":"drill-prices",
  "catalog":"${catalog_content}","catalog_version":1,"state":"approved",
  "approved_at_millis":1,"approval_citation":"restore drill",
  "rules":[
    {"provider":"openai","model":"gpt-4o","precedence":"baseline",
      "from_millis":0,"until_millis":1000,
      "input_nano_dollars_per_million":2500000000,
      "output_nano_dollars_per_million":10000000000,"origin":"operator",
      "citation":"restore drill baseline"},
    {"provider":"openai","model":"gpt-4o","precedence":"override",
      "from_millis":1000,
      "input_nano_dollars_per_million":5000000000,
      "output_nano_dollars_per_million":15000000000,"origin":"operator",
      "citation":"restore drill override"}]}}
EOF
cat >"${workdir}/policy.json" <<EOF
{"summary":"cap the drill tenant","resource":{
  "tenant":"${tenant}","slug":"drill-limits","epoch":1,
  "subject_limit_microdollars":50000000,"namespace_limit_microdollars":500000000,
  "reservation_ttl_seconds":300,"max_in_flight_per_subject":8,"lease_ttl_seconds":60}}
EOF

for pair in projects:project principals:principal providers:provider \
  credentials:credential catalogs:catalog models:model aliases:alias \
  prices:price policies:policy; do
  resource="${pair%%:*}"
  head="$(publish "$resource" "${workdir}/${pair##*:}.json" "drill-${resource}" "$head")"
  printf '  published %-12s -> %s\n' "$resource" "$head"
done
live_head="$head"
live_revisions="$(revision_count)"
live_resources="$(resource_count)"
live_checksum="$(admin history --limit 1 | jq -r '.revisions[0].checksum')"
live_price_checksum="$(psql live 5432 -c \
  "SELECT content_checksum FROM axond_cp_resource_version \
   WHERE resource_kind = 'price' AND resource_id = 'res_01900000-0000-7000-8000-000000000014' \
   ORDER BY version DESC LIMIT 1")"

# Put one settled billing event on the durable side of the logical-backup
# boundary. Counting an empty source and an empty restore would prove only that
# two databases agree about having no usage history.
backup_usage_baseline="$(usage_count live 5432)"
backup_usage_baseline_id="$(latest_usage_row_id live 5432)"
backup_usage_status="$(probe_survivor_chat "${workdir}/usage-before-logical-backup.body")"
wait_for_usage_count live 5432 "$((backup_usage_baseline + 1))" >/dev/null || true
backup_usage_new_rows="$(usage_rows_after_id live 5432 "$backup_usage_baseline_id")"
backup_usage_request_id="$(usage_request_after_id live 5432 "$backup_usage_baseline_id")"
backup_usage_source_rows="$(usage_identity_count live 5432 axond_usage "$backup_usage_request_id")"
backup_usage_source_outbox_rows="$(usage_identity_count live 5432 axond_usage_outbox "$backup_usage_request_id")"
live_usage_rows="$(usage_count live 5432)"
live_usage_outbox_rows="$(psql live 5432 -c 'SELECT count(*) FROM axond_usage_outbox')"
psql live 5432 -c 'SELECT pg_switch_wal()' >/dev/null

# ---------------------------------------------------------------------------
stage backup-restore/restore logical_restore "$schema_identity"
mark "published" "a ${live_resources}-resource deployment at ${live_head}, published through /admin/v1"
observe live_head_revision "$live_head"
observe live_revisions "$live_revisions" count
observe live_resources "$live_resources" count

started=$(date +%s.%N)
docker exec -u postgres "$container" sh -c \
  'pg_dump -p 5432 -d live -Fc -f /tmp/live.dump'
mark "backed-up" "pg_dump of the live database taken while its replica was serving /admin/v1"
docker exec -u postgres "$container" sh -c \
  'pg_restore -p 5432 -d logical_restore --no-owner /tmp/live.dump'
restore_seconds="$(awk -v end="$(date +%s.%N)" -v start="$started" 'BEGIN { printf "%.3f", end - start }')"
mark "restored" "pg_restore into a database no replica ever wrote"
observe restore_duration_seconds "$restore_seconds" seconds

# Read the catalogue directly from the restored database before creating any
# recovered replica. The recovered config below is also non-repopulating, but
# this ordering makes the evidence independent of both boot and refresh code.
catalog_restore_content_id="$(psql logical_restore 5432 -c \
  'SELECT content_id FROM axond_catalog_active WHERE singleton' 2>/dev/null || true)"
catalog_restore_content_id="${catalog_restore_content_id:-missing}"
catalog_restore_raw_digest="$(psql logical_restore 5432 -c \
  "SELECT raw_digest FROM axond_catalog_snapshot WHERE content_id = '${catalog_restore_content_id}'" \
  2>/dev/null || true)"
catalog_restore_raw_digest="${catalog_restore_raw_digest:-missing}"
catalog_restore_raw_bytes="$(psql logical_restore 5432 -c \
  "SELECT raw_bytes FROM axond_catalog_snapshot WHERE content_id = '${catalog_restore_content_id}'" \
  2>/dev/null || true)"
catalog_restore_raw_bytes="${catalog_restore_raw_bytes:-0}"
catalog_restore_payload_bytes="$(psql logical_restore 5432 -c \
  "SELECT octet_length(payload) FROM axond_catalog_snapshot WHERE content_id = '${catalog_restore_content_id}'" \
  2>/dev/null || true)"
catalog_restore_payload_bytes="${catalog_restore_payload_bytes:-0}"
catalog_restore_rows="$(psql logical_restore 5432 -c \
  'SELECT count(*) FROM axond_catalog_snapshot' 2>/dev/null || true)"
catalog_restore_rows="${catalog_restore_rows:-0}"
mark "catalogue-preboot-read" "the restored catalogue pointer and payload metadata were read before any recovered replica booted"
observe catalogue_preboot_content_id "$catalog_restore_content_id"
observe catalogue_preboot_raw_digest "$catalog_restore_raw_digest"
observe catalogue_preboot_raw_bytes "$catalog_restore_raw_bytes" count
observe catalogue_preboot_payload_bytes "$catalog_restore_payload_bytes" count
observe catalogue_preboot_snapshot_rows "$catalog_restore_rows" count

config logical_restore "$live_port" logical.toml "$logical_http" seed empty false
restored_schema="$("$axond_bin" migrate status --config "$drill_config" 2>&1 | tr '\n' ' ')" &&
  restored_current=current || restored_current=stale
require "the_restored_schema_is_current" current "$restored_current" \
  "a replica refuses a database whose schema it does not recognise: ${restored_schema}"

serve logical_restore "$logical_http"
logical_endpoint="$endpoint"
mark "replica-booted" "a replica booted on the restored database and opened /admin/v1"

restored_head_revision="$(head_revision 2>/dev/null || printf 'unreadable')"
restored_revision_count="$(revision_count 2>/dev/null || printf '%s' -1)"
restored_resource_count="$(resource_count 2>/dev/null || printf '%s' -1)"
restored_head_checksum="$(admin history --limit 1 2>/dev/null | jq -r '.revisions[0].checksum // "unreadable"' 2>/dev/null || printf 'unreadable')"
observe restored_head_revision "$restored_head_revision"
observe restored_revision_count "$restored_revision_count" count
observe restored_resource_count "$restored_resource_count" count
observe live_head_checksum "$live_checksum"
observe restored_head_checksum "$restored_head_checksum"
require "the_restored_head_is_the_backed_up_head" "$live_head" "$restored_head_revision" \
  "the replica reads the deployment the backup was taken from"
require "the_restored_revision_chain_is_whole" "$live_revisions" "$restored_revision_count" \
  "no revision published before the backup was lost"
require "the_restored_deployment_is_whole" "$live_resources" "$restored_resource_count" \
  "tenancy, the provider connection, its credential reference, and the policy all came back"
require "the_restored_head_checksum_matches" "$live_checksum" \
  "$restored_head_checksum" \
  "the state the restored journal hydrates is byte-identical to the backed-up state"

# A restore that reads is half a restore: the journal has to accept the next
# change against the head it came back with. The document differs from the one
# already published, so the probe measures writability rather than whatever the
# journal decides to do with a candidate that changes nothing.
cat >"${workdir}/policy-after-restore.json" <<EOF
{"summary":"raise the cap after the restore","resource":{
  "tenant":"${tenant}","slug":"drill-limits","epoch":2,
  "subject_limit_microdollars":70000000,"namespace_limit_microdollars":700000000,
  "reservation_ttl_seconds":300,"max_in_flight_per_subject":8,"lease_ttl_seconds":60}}
EOF
after_restore="$(publish policies "${workdir}/policy-after-restore.json" \
  drill-after-restore "$live_head" || echo refused)"
require "a_publication_against_the_restored_head_is_accepted" accepted \
  "$([[ "$after_restore" == refused ]] && echo refused || echo accepted)" \
  "the restored journal is writable, not just readable"
observe revision_after_restore "$after_restore"

# The restored journal carries a complete projected workload, so this replica
# must become a real serving participant. Keep connection failure as a sentinel
# so the stage writes its artifact rather than aborting before it can retain the
# failed readiness or inference observation.
probe_status() {
  local body="$1" url="$2" status
  status="$(curl -sS -o "$body" -w '%{http_code}' "$url" 2>/dev/null || true)"
  printf '%s' "${status:-unreachable}"
}
readiness_status=unreachable
for _ in $(seq 60); do
  readiness_status="$(probe_status "${workdir}/logical-readyz.body" "${logical_endpoint}/readyz")"
  [[ "$readiness_status" == 200 ]] && break
  sleep 1
done
inference_status="$(probe_status "${workdir}/logical-inference.body" "${logical_endpoint}/v1/models")"
inference_error="$(jq -r '.error.type // "malformed"' "${workdir}/logical-inference.body" 2>/dev/null || printf 'malformed')"
observe readiness_probe "$readiness_status"
observe restored_readiness_status "$readiness_status"
observe restored_inference_status "$inference_status"
observe restored_inference_error "$inference_error"
require "the_restored_replica_becomes_ready" 200 "$readiness_status" \
  "the restored journal carries a complete projected serving snapshot"
require "the_restored_replica_authenticates_before_convergence" 401 "$inference_status" \
  "an unauthenticated restored-replica probe is refused before serving state is disclosed"
require "the_restored_replica_uses_the_unauthorized_envelope" unauthorized "$inference_error" \
  "the auth-first refusal hides the missing serving snapshot from anonymous callers"

restore_loss_checks=(the_restored_head_is_the_backed_up_head
  the_restored_revision_chain_is_whole the_restored_deployment_is_whole
  the_restored_head_checksum_matches)
gate max_data_loss_revisions \
  "$(held "${restore_loss_checks[@]}" && echo 0 || echo unknown)" \
  "$(verdict "${restore_loss_checks[@]}")" \
  "every revision the backup covered is present in the restored journal, and its head checksum matches"
gate admin_writes \
  "$(held a_publication_against_the_restored_head_is_accepted && echo accepted || echo refused)" \
  "$(verdict a_publication_against_the_restored_head_is_accepted)" \
  "a publication against the restored head was accepted by a replica booted on it"
defer readiness \
  "the reconvergence stage owns serving readiness; this stage retains the restored replica's preflight probe"
defer max_serving_error_fraction \
  "this stage offers no inference traffic, so the ceiling is vacuous by contract"
defer max_convergence_lag_seconds \
  "the reconvergence stage measures the survivor's convergence lag after the journal switch"
defer max_unauthenticated_admin_successes \
  "the \`administration\` stage measures the administrative surface's authentication"
close

# ---------------------------------------------------------------------------
stage backup-restore/reconvergence logical_restore "$schema_identity"
export AXOND_ADMIN_ENDPOINT="$logical_endpoint"
logical_revisions_after_restore="$(revision_count)"
mark "survivor-served-before-switch" \
  "the survivor replica retained its compiled snapshot while the restore replica accepted the recovery publication"
observe survivor_before_revision "$live_head"
observe restored_revision "$after_restore"
observe restored_revision_count "$logical_revisions_after_restore" count

# Change only the database name behind the survivor's stable DSN. The proxy
# closes its active sockets, so this is a real journal handoff rather than a
# restarted replica pointed at a new config.
kill -USR1 "$proxy_pid"
mark "journal-switched" "the survivor DSN was switched to the logical-restore database"
survivor_report="$(wait_for_survivor "$after_restore" || true)"
export AXOND_ADMIN_ENDPOINT="$live_endpoint"
survivor_converged="$(printf '%s' "$survivor_report" | jq -r '.converged // false' 2>/dev/null || printf 'false')"
[[ -n "$survivor_converged" ]] || survivor_converged=false
survivor_active="$(printf '%s' "$survivor_report" | jq -r '.active // "unreadable"' 2>/dev/null || printf 'unreadable')"
survivor_lag_seconds="$(printf '%s' "$survivor_report" | jq -r '(.lag_ms // -1) / 1000' 2>/dev/null || printf 'unknown')"
survivor_revision_count="$(revision_count 2>/dev/null || printf '%s' -1)"
survivor_readiness="$(curl -sS -o /dev/null -w '%{http_code}' \
  "${live_endpoint}/readyz" 2>/dev/null || printf '000')"
survivor_chat_status="$(probe_survivor_chat "${workdir}/survivor-logical-chat.body")"
survivor_unauthenticated="$(unauthenticated_successes "$live_endpoint")"
mark "survivor-converged" "the survivor loaded and activated the recovered logical-restore head"
observe survivor_active_revision "$survivor_active"
observe survivor_convergence_lag_seconds "$survivor_lag_seconds" seconds
observe survivor_readiness_status "$survivor_readiness"
observe survivor_inference_status "$survivor_chat_status"
observe survivor_revision_count "$survivor_revision_count" count
observe unauthenticated_admin_successes "$survivor_unauthenticated" count
require "the_survivor_converges_to_the_restored_head" true "$survivor_converged" \
  "the running survivor follows the journal after its stable DSN is switched"
require "the_survivor_active_revision_is_restored" "$after_restore" "$survivor_active" \
  "the survivor activates the revision accepted by the restored journal"
require "the_survivor_revision_chain_is_whole" "$logical_revisions_after_restore" "$survivor_revision_count" \
  "the survivor reads the complete restored revision chain"
require "the_survivor_is_ready_after_restore" 200 "$survivor_readiness" \
  "the survivor remains ready once the restored serving snapshot is active"
require "the_survivor_serves_inference_after_restore" 200 "$survivor_chat_status" \
  "traffic is answered after the survivor converges onto the restored journal"
survivor_lag_met="$(awk -v lag="$survivor_lag_seconds" \
  'BEGIN { print (lag >= 0 && lag <= 60) ? "true" : "false" }')"
gate max_serving_error_fraction \
  "$( [[ "$survivor_chat_status" == 200 ]] && echo 0 || echo 1 )" \
  "$( [[ "$survivor_chat_status" == 200 ]] && echo true || echo false )" \
  "the offered post-restore inference request was answered"
gate max_convergence_lag_seconds "$survivor_lag_seconds" "$survivor_lag_met" \
  "the survivor converged to the restored head inside the declared bound"
gate readiness \
  "$( [[ "$survivor_readiness" == 200 ]] && echo serves || echo refuses )" \
  "$( [[ "$survivor_readiness" == 200 ]] && echo true || echo false )" \
  "the survivor's readiness follows the restored serving snapshot"
defer max_data_loss_revisions \
  "the restore stage owns the durable revision-loss boundary"
defer admin_writes \
  "the restore stage owns the accepted recovered-journal publication"
defer max_unauthenticated_admin_successes \
  "the administration stage measures recovered administrative authentication"
close

# Return the survivor to the live database before the PITR publication window.
# This is another real handoff, but it is setup for the next independent
# recovery boundary and is not counted as evidence for either stage.
kill -HUP "$proxy_pid"
wait_for_survivor "$live_head" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
stage backup-restore/administration logical_restore "$schema_identity"
export AXOND_ADMIN_ENDPOINT="$logical_endpoint"
# A failing read is the regression this stage qualifies, so it is captured as a
# check rather than allowed to abort the drill before `close` writes evidence.
audit="$(admin audit --revision "$live_head" 2>/dev/null || printf '{"events":[]}')"
mark "audit-read" "the audit trail of ${live_head} read back through the restored replica"
observe audit_events_for_head "$(printf '%s' "$audit" | jq '.events | length')" count
require "the_audit_trail_survives_the_restore" true \
  "$(printf '%s' "$audit" | jq '(.events | length) > 0')" \
  "who changed what is recoverable, not only what the change was"
require "the_audit_trail_names_the_breakglass_actor" breakglass \
  "$(printf '%s' "$audit" | jq -r '.events[0].actor.kind')" \
  "the restored trail attributes the publication to the identity that made it"
# Do not add `-q`: with `pipefail`, an early grep exit can SIGPIPE printf and
# invert a matching scan into the clean branch. Draining to /dev/null keeps
# the producer's status independent of the audit payload size.
require "the_audit_trail_carries_no_secret_material" clean \
  "$(printf '%s' "$audit" | grep -F "$GW_DRILL_BREAKGLASS" >/dev/null && echo leaked || echo clean)" \
  "an audit read names the credential's env var, never its value"

successes="$(unauthenticated_successes "$logical_endpoint")"
mark "unauthenticated-refused" "two unauthenticated reads of /admin/v1/state on the restored replica"
observe unauthenticated_admin_successes "$successes" count
gate max_unauthenticated_admin_successes "$successes" \
  "$([[ "$successes" == "0" ]] && echo true || echo false)" \
  "a restored control plane does not come back with its administrative surface open"
defer admin_writes \
  "the restore stage owns the accepted recovered-journal publication; this stage measures authentication and audit reads"
defer readiness "this stage reads the administrative surface; it offers no inference traffic"
defer max_serving_error_fraction "no traffic is offered, so the ceiling is vacuous by contract"
defer max_convergence_lag_seconds "nothing converges in an audit read"
defer max_data_loss_revisions "the \`restore\` stage measures durable loss"
close

# ---------------------------------------------------------------------------
stage backup-restore/durable-inventory logical_restore "$schema_identity"
export AXOND_ADMIN_ENDPOINT="$logical_endpoint"

# A revision can restore perfectly while the rows it references are gone. This
# stage checks each durable class through the surface that owns it: the secret
# lifecycle API for wrapped material, the catalogue tables for imported bytes
# and their active pointer, and the control-plane rows for the approved price
# book. Only counts, identities, and lifecycle state enter the artifact.
secret_versions="$(admin secret versions --secret "$secret_id" --tenant "$tenant" \
  | jq -r --arg reference "$secret_reference" \
    '[.versions[] | select(.reference == $reference and .lifecycle == "active")] | length')"
secret_owner="$(admin secret versions --secret "$secret_id" --tenant "$tenant" \
  | jq -r '.versions[0].owner // "missing"')"
secret_ciphertext_rows="$(psql logical_restore 5432 -c \
  "SELECT count(*) FROM axond_secret WHERE secret_id = '${secret_id}' AND version = ${secret_version} AND lifecycle = 'active' AND wrapped_dek IS NOT NULL AND ciphertext IS NOT NULL")"
catalog_snapshot_rows="$(psql logical_restore 5432 -c \
  "SELECT count(*) FROM axond_catalog_snapshot WHERE content_id = '${catalog_content}' AND raw_digest = '${catalog_digest}'")"
catalog_active_content="$(psql logical_restore 5432 -c \
  "SELECT coalesce(content_id, 'missing') FROM axond_catalog_active WHERE singleton")"
price_book_rows="$(psql logical_restore 5432 -c \
  "SELECT count(*) FROM axond_cp_resource_version \
   WHERE resource_kind = 'price' AND resource_id = 'res_01900000-0000-7000-8000-000000000014' \
     AND body_form = 'inline' AND body_inline IS NOT NULL")"
restored_price_checksum="$(psql logical_restore 5432 -c \
  "SELECT coalesce(content_checksum, 'missing') FROM axond_cp_resource_version \
   WHERE resource_kind = 'price' AND resource_id = 'res_01900000-0000-7000-8000-000000000014' \
   ORDER BY version DESC LIMIT 1")"
restored_price_body="$(psql logical_restore 5432 -c \
  "SELECT encode(body_inline, 'hex') FROM axond_cp_resource_version \
   WHERE resource_kind = 'price' AND resource_id = 'res_01900000-0000-7000-8000-000000000014' \
     AND body_form = 'inline' AND body_inline IS NOT NULL \
   ORDER BY version DESC LIMIT 1" 2>/dev/null | \
  "$python_bin" "${root}/ops/decode-canonical-json.py" 2>/dev/null || printf '{}')"
expected_price_history="$(jq -cn '
  [
    {provider:"openai",published_model_id:"gpt-4o",precedence:"baseline",
      effective_from:0,effective_until:1000,
      rates:{input:2500000000,output:10000000000},
      provenance:{origin:"operator",citation:"restore drill baseline"}},
    {provider:"openai",published_model_id:"gpt-4o",precedence:"override",
      effective_from:1000,effective_until:null,
      rates:{input:5000000000,output:15000000000},
      provenance:{origin:"operator",citation:"restore drill override"}}
  ] | sort_by([.effective_from, .precedence])')"
restored_price_schema="$(printf '%s' "$restored_price_body" | \
  jq -r '.schema // "unreadable"' 2>/dev/null || printf 'unreadable')"
restored_price_catalog_version="$(printf '%s' "$restored_price_body" | \
  jq -r '.catalog_version // "unreadable"' 2>/dev/null || printf 'unreadable')"
restored_price_approval_state="$(printf '%s' "$restored_price_body" | \
  jq -r '.approval.state // "unreadable"' 2>/dev/null || printf 'unreadable')"
restored_price_approval_citation="$(printf '%s' "$restored_price_body" | \
  jq -r '.approval.citation // "unreadable"' 2>/dev/null || printf 'unreadable')"
restored_price_rule_count="$(printf '%s' "$restored_price_body" | \
  jq -r '.rules | length' 2>/dev/null || printf 'unreadable')"
restored_price_history="$(printf '%s' "$restored_price_body" | jq -c '
  [.rules[] | {
    provider,
    published_model_id,
    precedence,
    effective_from,
    effective_until: (.effective_until // null),
    rates: {input: .rates.input, output: .rates.output},
    provenance: {origin: .provenance.origin, citation: (.provenance.citation // null)}
  }] | sort_by([.effective_from, .precedence])' 2>/dev/null || printf 'unreadable')"
restored_usage_rows="$(usage_count logical_restore 5432)"
restored_usage_outbox_rows="$(psql logical_restore 5432 -c \
  'SELECT count(*) FROM axond_usage_outbox')"
backup_usage_restored_rows="$(usage_identity_count logical_restore 5432 axond_usage "$backup_usage_request_id")"
backup_usage_restored_outbox_rows="$(usage_identity_count logical_restore 5432 axond_usage_outbox "$backup_usage_request_id")"
mark "inventory-checked" "the restored secret, catalogue, and approved price-book records were inspected"
observe expected_secret_owner "$tenant"
observe restored_secret_versions "$secret_versions" count
observe restored_secret_owner "$secret_owner"
observe restored_secret_ciphertext_rows "$secret_ciphertext_rows" count
observe restored_catalog_snapshot_rows "$catalog_snapshot_rows" count
observe expected_catalog_active_content "$catalog_content"
observe restored_catalog_active_content "$catalog_active_content"
observe live_usage_rows "$live_usage_rows" count
observe restored_usage_rows "$restored_usage_rows" count
observe live_usage_outbox_rows "$live_usage_outbox_rows" count
observe restored_usage_outbox_rows "$restored_usage_outbox_rows" count
observe logical_backup_usage_request_id "$backup_usage_request_id"
observe logical_backup_usage_status "$backup_usage_status"
observe logical_backup_new_usage_rows "$backup_usage_new_rows" count
observe logical_backup_source_usage_identity_rows "$backup_usage_source_rows" count
observe logical_backup_source_outbox_identity_rows "$backup_usage_source_outbox_rows" count
observe logical_backup_restored_usage_identity_rows "$backup_usage_restored_rows" count
observe logical_backup_restored_outbox_identity_rows "$backup_usage_restored_outbox_rows" count
observe restored_price_book_rows "$price_book_rows" count
observe live_price_book_checksum "$live_price_checksum"
observe restored_price_book_checksum "$restored_price_checksum"
observe restored_price_book_schema "$restored_price_schema"
observe restored_price_book_catalog_version "$restored_price_catalog_version"
observe restored_price_book_approval_state "$restored_price_approval_state"
observe restored_price_book_approval_citation "$restored_price_approval_citation"
observe restored_price_book_rule_count "$restored_price_rule_count" count
observe expected_price_book_history "$expected_price_history"
observe restored_price_book_history "$restored_price_history"
require "the_restored_secret_version_is_active" 1 "$secret_versions" \
  "the credential's exact active version and lifecycle survived the restore"
require "the_restored_secret_version_owner_survives" "$tenant" "$secret_owner" \
  "the serialized secret-version owner remains the drill tenant"
require "the_restored_secret_material_is_encrypted" 1 "$secret_ciphertext_rows" \
  "the restored secret has wrapped ciphertext, not plaintext or a reference-only row"
require "the_restored_catalog_snapshot_survives" 1 "$catalog_snapshot_rows" \
  "the imported catalogue content and raw provenance survived the restore"
require "the_restored_catalog_active_pointer_survives" "$catalog_content" \
  "$catalog_active_content" \
  "the active catalogue pointer still names the retained content it confirmed"
require "the_logical_backup_usage_fixture_was_answered" 200 "$backup_usage_status" \
  "the backup includes a billing event produced by authenticated inference"
require "the_logical_backup_usage_fixture_is_nonempty" true \
  "$([[ "$live_usage_rows" -gt 0 && "$live_usage_outbox_rows" -gt 0 ]] && echo true || echo false)" \
  "the source has both the billing fact and its durable outbox event"
require "the_logical_backup_usage_fixture_is_exactly_one_new_row" 1 "$backup_usage_new_rows" \
  "the authenticated request creates one exact billing identity after the baseline boundary"
require "the_logical_backup_usage_identity_is_canonical" true \
  "$([[ "$backup_usage_request_id" =~ ^req_[0-9a-f-]{36}$ ]] && echo true || echo false)" \
  "the backup fixture is bound to its durable UUIDv7-shaped request identity"
require "the_logical_backup_usage_identity_is_in_the_source" 1 "$backup_usage_source_rows" \
  "the exact request answered before backup exists once in the source usage sink"
require "the_logical_backup_usage_outbox_identity_is_in_the_source" 1 "$backup_usage_source_outbox_rows" \
  "the exact request answered before backup exists once in the durable source outbox"
require "the_logical_backup_usage_identity_survives" 1 "$backup_usage_restored_rows" \
  "the logical restore retains the exact request identity created for this backup"
require "the_logical_backup_usage_outbox_identity_survives" 1 "$backup_usage_restored_outbox_rows" \
  "the logical restore retains the exact outbox identity created for this backup"
require "the_restored_usage_rows_match_backup" "$live_usage_rows" "$restored_usage_rows" \
  "the logical restore retains every billing fact present at the backup boundary"
require "the_restored_usage_outbox_matches_backup" "$live_usage_outbox_rows" "$restored_usage_outbox_rows" \
  "the logical restore retains every durable delivery event present at the backup boundary"
require "the_restored_price_book_survives" 1 "$price_book_rows" \
  "the approved effective-dated price book remains a durable resource body"
require "the_restored_price_book_checksum_matches" "$live_price_checksum" "$restored_price_checksum" \
  "the restored price-book bytes are the exact approved body, not merely a price row"
require "the_restored_price_book_schema_is_current" axond.price-book.v2 "$restored_price_schema" \
  "the restored body retains the current typed price-book schema"
require "the_restored_price_book_catalogue_version_survives" 1 "$restored_price_catalog_version" \
  "the approved rates remain pinned to the catalogue resource version they priced"
require "the_restored_price_book_approval_survives" approved "$restored_price_approval_state" \
  "the restored rates remain approved rather than silently becoming draft"
require "the_restored_price_book_approval_citation_survives" "restore drill" "$restored_price_approval_citation" \
  "the approval provenance remains attached to the recovered book"
require "the_restored_price_book_has_two_historical_rules" 2 "$restored_price_rule_count" \
  "the restore retains more than the current price and therefore preserves history"
require "the_restored_price_history_is_exact" "$expected_price_history" "$restored_price_history" \
  "effective intervals, rates, precedence, and per-rule provenance survive exactly"

defer max_data_loss_revisions "the restore stage owns the revision-loss boundary; this stage checks durable inventory classes"
defer readiness "the reconvergence stage owns serving after a restore"
defer admin_writes "the restore stage owns the accepted recovered-journal publication"
defer max_serving_error_fraction "this stage offers no inference traffic"
defer max_convergence_lag_seconds "this stage checks durable inventory, not replica convergence"
defer max_unauthenticated_admin_successes "the administration stage measures administrative authentication"
close

# ---------------------------------------------------------------------------
step "Recovery 2: point-in-time recovery to a chosen moment"
export AXOND_ADMIN_ENDPOINT="$live_endpoint"
docker exec -u postgres "$container" \
  pg_basebackup -p 5432 -D "$basebackup" -Fp -Xs -c fast
# The target is after the pre-target publication and before the post-target one.
# A second of separation on either side keeps the assertion about *what* was
# published rather than about clock resolution.
pre_target_head="$(head_revision)"
pre_target_revisions="$(revision_count)"
pre_target_usage_baseline="$(usage_count live 5432)"
pre_target_usage_baseline_id="$(latest_usage_row_id live 5432)"
pre_target_chat_status="$(probe_survivor_chat "${workdir}/usage-before-target.body")"
pre_target_usage_count="$(wait_for_usage_count live 5432 "$((pre_target_usage_baseline + 1))" || true)"
pre_target_usage_new_rows="$(usage_rows_after_id live 5432 "$pre_target_usage_baseline_id")"
pre_target_usage_id="$(usage_request_after_id live 5432 "$pre_target_usage_baseline_id")"
sleep 1
target="$(psql live 5432 -c 'SELECT now()')"
sleep 1

cat >"${workdir}/policy-after-target.json" <<EOF
{"summary":"raise the cap after the recovery target","resource":{
  "tenant":"${tenant}","slug":"drill-limits","epoch":2,
  "subject_limit_microdollars":90000000,"namespace_limit_microdollars":900000000,
  "reservation_ttl_seconds":300,"max_in_flight_per_subject":8,"lease_ttl_seconds":60}}
EOF
post_target_head="$(publish policies "${workdir}/policy-after-target.json" \
  drill-after-target "$pre_target_head")"
post_target_usage_baseline="$(usage_count live 5432)"
post_target_usage_baseline_id="$(latest_usage_row_id live 5432)"
post_target_chat_status="$(probe_survivor_chat "${workdir}/usage-after-target.body")"
post_target_usage_count="$(wait_for_usage_count live 5432 "$((post_target_usage_baseline + 1))" || true)"
post_target_usage_new_rows="$(usage_rows_after_id live 5432 "$post_target_usage_baseline_id")"
post_target_usage_id="$(usage_request_after_id live 5432 "$post_target_usage_baseline_id")"

# The segment holding the post-target write has to reach the archive before the
# restore reads it, so the switch's own LSN names the file to wait for. An early
# archiver failure is not fatal: the archive directory is created after startup,
# because `archive_mode` is restart-only, and the archiver retries.
switched="$(psql live 5432 -c 'SELECT pg_switch_wal()')"
archived=f
for _ in $(seq 60); do
  archived="$(psql live 5432 -c "SELECT coalesce(
      last_archived_wal >= pg_walfile_name('${switched}'::pg_lsn), false)
    FROM pg_stat_archiver")"
  [[ "$archived" == "t" ]] && break
  sleep 1
done
[[ "$archived" == "t" ]] || fail "WAL archiving did not reach ${switched}: last failure $(
  psql live 5432 -c "SELECT coalesce(last_failed_wal, 'none') FROM pg_stat_archiver"
)"

stage point-in-time-recovery/recovery live "$schema_identity"
mark "published-before-target" "the deployment reached ${pre_target_head} before the target was taken"
mark "target-taken" "recovery target ${target}"
mark "published-after-target" "${post_target_head} was published after the target and must not survive"
observe recovery_target "$target"
observe pre_target_head_revision "$pre_target_head"
observe post_target_head_revision "$post_target_head"
observe revisions_before_target "$pre_target_revisions" count

started=$(date +%s.%N)
docker exec -u postgres "$container" sh -c "cat >>${basebackup}/postgresql.auto.conf <<EOF
port = 5433
archive_mode = off
restore_command = 'cp ${archive}/%f %p'
recovery_target_time = '${target}'
recovery_target_action = 'promote'
EOF"
docker exec -u postgres "$container" touch "${basebackup}/recovery.signal"
docker exec -u postgres "$container" \
  pg_ctl -D "$basebackup" -l /tmp/restore.log -w -t 90 start ||
  fail "the restored cluster did not start: $(docker exec "$container" cat /tmp/restore.log)"

for _ in $(seq 60); do
  in_recovery="$(psql postgres 5433 -c 'SELECT pg_is_in_recovery()' 2>/dev/null || echo t)"
  [[ "$in_recovery" == "f" ]] && break
  sleep 1
done
restore_seconds="$(awk -v end="$(date +%s.%N)" -v start="$started" 'BEGIN { printf "%.3f", end - start }')"
observe recovery_in_progress "$in_recovery"
require "the_recovered_cluster_promotes" f "$in_recovery" \
  "a cluster still in recovery is not a cluster anyone can serve from"
mark "promoted" "the recovered cluster was promoted at the target"
observe restore_duration_seconds "$restore_seconds" seconds

# As with logical restore, read the PITR catalogue directly before creating a
# recovered replica. Missing tables become sentinel values and are judged by
# this stage's checks instead of aborting before its artifact can close.
pitr_catalog_content_id="$(psql live 5433 -c \
  'SELECT content_id FROM axond_catalog_active WHERE singleton' 2>/dev/null || true)"
pitr_catalog_content_id="${pitr_catalog_content_id:-missing}"
pitr_catalog_raw_digest="$(psql live 5433 -c \
  "SELECT raw_digest FROM axond_catalog_snapshot WHERE content_id = '${pitr_catalog_content_id}'" \
  2>/dev/null || true)"
pitr_catalog_raw_digest="${pitr_catalog_raw_digest:-missing}"
pitr_catalog_raw_bytes="$(psql live 5433 -c \
  "SELECT raw_bytes FROM axond_catalog_snapshot WHERE content_id = '${pitr_catalog_content_id}'" \
  2>/dev/null || true)"
pitr_catalog_raw_bytes="${pitr_catalog_raw_bytes:-0}"
pitr_catalog_payload_bytes="$(psql live 5433 -c \
  "SELECT octet_length(payload) FROM axond_catalog_snapshot WHERE content_id = '${pitr_catalog_content_id}'" \
  2>/dev/null || true)"
pitr_catalog_payload_bytes="${pitr_catalog_payload_bytes:-0}"
pitr_catalog_rows="$(psql live 5433 -c \
  'SELECT count(*) FROM axond_catalog_snapshot' 2>/dev/null || true)"
pitr_catalog_rows="${pitr_catalog_rows:-0}"
mark "catalogue-preboot-read" "the PITR catalogue pointer and payload metadata were read before any recovered replica booted"
observe pitr_catalogue_preboot_content_id "$pitr_catalog_content_id"
observe pitr_catalogue_preboot_raw_digest "$pitr_catalog_raw_digest"
observe pitr_catalogue_preboot_raw_bytes "$pitr_catalog_raw_bytes" count
observe pitr_catalogue_preboot_payload_bytes "$pitr_catalog_payload_bytes" count
observe pitr_catalogue_preboot_snapshot_rows "$pitr_catalog_rows" count

config live "$restored_port" restored.toml "$recovered_http" seed empty false
recovered_schema="$("$axond_bin" migrate status --config "$drill_config" 2>&1 | tr '\n' ' ')" &&
  recovered_current=current || recovered_current=stale
observe recovered_schema_status "$recovered_current"
require "the_recovered_schema_is_current" current "$recovered_current" \
  "a replica refuses a recovered database whose schema it does not recognise: ${recovered_schema}"

serve recovered "$recovered_http"
recovered_endpoint="$endpoint"
mark "replica-booted" "a replica booted on the promoted cluster and opened /admin/v1"

# PITR must restore the durable dependencies of the revision, not only the
# revision row itself. Read secret and catalogue metadata without material and
# without treating a post-target fixture as evidence. Catalogue values were
# captured above, before the recovered replica could boot.
pitr_secret_versions="$(admin secret versions --secret "$secret_id" --tenant "$tenant" \
  2>/dev/null || printf '{"versions":[]}')"
pitr_secret_owner="$(printf '%s' "$pitr_secret_versions" |
  jq -r '.versions[0].owner // "missing"')"
mark "durable-inventory-read" "PITR metadata reads for the tenant's secret and pre-boot catalogue snapshot"
observe pitr_secret_versions "$(printf '%s' "$pitr_secret_versions" | jq '.versions | length')" count
observe expected_secret_owner "$tenant"
observe pitr_secret_owner "$pitr_secret_owner"
observe pitr_secret_lifecycle "$(printf '%s' "$pitr_secret_versions" | jq -r '.versions[0].lifecycle // "missing"')"
observe expected_pitr_catalogue_content_id "$catalog_content_id"
observe pitr_catalogue_content_id "$pitr_catalog_content_id"
observe expected_pitr_catalogue_raw_digest "$catalog_raw_digest"
observe pitr_catalogue_raw_digest "$pitr_catalog_raw_digest"
observe expected_pitr_catalogue_raw_bytes "$catalog_raw_bytes" count
observe pitr_catalogue_raw_bytes "$pitr_catalog_raw_bytes" count
observe pitr_catalogue_payload_bytes "$pitr_catalog_payload_bytes" count
observe pitr_catalogue_snapshot_rows "$pitr_catalog_rows" count
require "the_pitr_secret_metadata_survives_the_target" 1 \
  "$(printf '%s' "$pitr_secret_versions" | jq '.versions | length')" \
  "the target preserves the secret version metadata referenced by the pre-target deployment"
require "the_pitr_secret_owner_survives_the_target" "$tenant" \
  "$pitr_secret_owner" \
  "the target preserves the serialized owner of the pre-target secret version"
require "the_pitr_secret_lifecycle_survives_the_target" active \
  "$(printf '%s' "$pitr_secret_versions" | jq -r '.versions[0].lifecycle // "missing"')" \
  "the target preserves the lifecycle needed by the pre-target credential"
require "the_pitr_catalogue_snapshot_survives_the_target" "$catalog_content_id" \
  "$pitr_catalog_content_id" \
  "the target preserves the active catalogue identity used by the pre-target state"
require "the_pitr_catalogue_raw_digest_survives_the_target" "$catalog_raw_digest" \
  "$pitr_catalog_raw_digest" \
  "the target preserves the raw catalogue blob identity"
require "the_pitr_catalogue_raw_bytes_survive_the_target" "$catalog_raw_bytes" \
  "$pitr_catalog_raw_bytes" \
  "the target preserves the raw catalogue byte count"
require "the_pitr_catalogue_payload_survives_the_target" true \
  "$([[ "$pitr_catalog_payload_bytes" -gt 0 ]] && echo true || echo false)" \
  "the target preserves accepted catalogue payload bytes"

# Sentinels rather than bare reads: a recovered head the replica cannot answer
# for has to become a failed check in this stage's artifact, not an abort before
# the artifact exists.
recovered_head="$(head_revision || echo unreadable)"
recovered_revisions="$(revision_count || echo -1)"
observe recovered_head_revision "$recovered_head"
observe revisions_after_recovery "$recovered_revisions" count
require "the_recovered_head_is_the_pre_target_revision" "$pre_target_head" "$recovered_head" \
  "the recovery landed at the moment the operator chose, not at the end of the WAL"
require "nothing_published_before_the_target_is_lost" "$pre_target_revisions" \
  "$recovered_revisions" \
  "the data-loss boundary is measured against the target, and on its safe side it is zero"
post_target_revision_presence="$(admin history --limit 100 |
  jq -r --arg r "$post_target_head" 'if any(.revisions[]; .revision == $r) then "present" else "absent" end')"
observe post_target_revision_presence "$post_target_revision_presence"
require "the_write_after_the_target_is_not_replayed" absent \
  "$post_target_revision_presence" \
  "a recovery that replayed past its target would be useless for the incident it exists for"

# A document the recovered journal has never held, for the same reason the
# restore stage publishes one: the probe measures writability, not what the
# journal makes of a candidate that changes nothing.
cat >"${workdir}/policy-after-recovery.json" <<EOF
{"summary":"raise the cap after the recovery","resource":{
  "tenant":"${tenant}","slug":"drill-limits","epoch":3,
  "subject_limit_microdollars":80000000,"namespace_limit_microdollars":800000000,
  "reservation_ttl_seconds":300,"max_in_flight_per_subject":8,"lease_ttl_seconds":60}}
EOF
after_recovery="$(publish policies "${workdir}/policy-after-recovery.json" \
  drill-after-recovery "$recovered_head" || echo refused)"
recovered_revisions_after_publication="$(revision_count || echo -1)"
require "a_publication_against_the_recovered_head_is_accepted" accepted \
  "$([[ "$after_recovery" == refused ]] && echo refused || echo accepted)" \
  "the recovered journal takes the next change rather than needing to be rebuilt"
observe revision_after_recovery "$after_recovery"
observe revisions_after_recovery_publication "$recovered_revisions_after_publication" count
observe readiness_probe "$(curl -s -o /dev/null -w '%{http_code}' "${recovered_endpoint}/readyz")"

for table in axond_usage axond_budget axond_revocation; do
  table_count="$(psql live 5433 -c "SELECT count(*) FROM pg_tables WHERE tablename = '${table}'")"
  observe "recovered_${table}_table_count" "$table_count" count
  require "the_${table}_schema_is_recovered" 1 "$table_count" \
    "the recovery brings back the whole durable schema, not only the journal's tables"
done

recovery_loss_checks=(the_recovered_head_is_the_pre_target_revision
  nothing_published_before_the_target_is_lost
  the_write_after_the_target_is_not_replayed)
gate max_data_loss_revisions \
  "$(held "${recovery_loss_checks[@]}" && echo 0 || echo unknown)" \
  "$(verdict "${recovery_loss_checks[@]}")" \
  "every revision published before the target survived, and the one published after it did not"
gate admin_writes \
  "$(held a_publication_against_the_recovered_head_is_accepted && echo accepted || echo refused)" \
  "$(verdict a_publication_against_the_recovered_head_is_accepted)" \
  "a publication against the recovered head was accepted by a replica booted on it"
defer readiness \
  "the blocked \`reconvergence\` stage owns serving across a recovery; the readiness_probe observation records what this replica answered"
defer max_serving_error_fraction \
  "this stage offers no inference traffic, so the ceiling is vacuous by contract"
defer max_convergence_lag_seconds \
  "the blocked \`reconvergence\` stage measures a fleet converging onto a recovered head"
defer max_unauthenticated_admin_successes \
  "the \`administration\` stage measures the administrative surface's authentication"
close

# ---------------------------------------------------------------------------
stage point-in-time-recovery/usage-boundary live "$schema_identity"
# Usage is journaled before the inference response is returned and then
# delivered to the durable sink. The two requests below straddle the chosen
# recovery target, so this stage measures the same asymmetric boundary as the
# revision stage while also proving that a replay has a stable, globally unique
# request identity.
recovered_pre_target_usage="$(usage_identity_count live 5433 axond_usage "$pre_target_usage_id" 2>/dev/null || printf '0')"
recovered_pre_target_outbox="$(usage_identity_count live 5433 axond_usage_outbox "$pre_target_usage_id" 2>/dev/null || printf '0')"
recovered_post_target_usage="$(usage_identity_count live 5433 axond_usage "$post_target_usage_id" 2>/dev/null || printf '0')"
recovered_post_target_outbox="$(usage_identity_count live 5433 axond_usage_outbox "$post_target_usage_id" 2>/dev/null || printf '0')"
mark "usage-boundary-measured" \
  "the usage rows and outbox events on either side of the PITR target were inspected"
observe pre_target_usage_request_id "$pre_target_usage_id"
observe post_target_usage_request_id "$post_target_usage_id"
observe pre_target_chat_status "$pre_target_chat_status"
observe post_target_chat_status "$post_target_chat_status"
observe pre_target_usage_count "$pre_target_usage_count" count
observe post_target_usage_count "$post_target_usage_count" count
observe pre_target_new_usage_rows "$pre_target_usage_new_rows" count
observe post_target_new_usage_rows "$post_target_usage_new_rows" count
observe recovered_pre_target_usage "$recovered_pre_target_usage" count
observe recovered_pre_target_outbox "$recovered_pre_target_outbox" count
observe recovered_post_target_usage "$recovered_post_target_usage" count
observe recovered_post_target_outbox "$recovered_post_target_outbox" count
require "the_pre_target_usage_request_is_answered" 200 "$pre_target_chat_status" \
  "the request whose usage identity is before the target was answered before recovery"
require "the_post_target_usage_request_is_answered" 200 "$post_target_chat_status" \
  "the request whose usage identity is after the target was answered before recovery"
require "the_pre_target_usage_request_creates_one_identity" 1 "$pre_target_usage_new_rows" \
  "the pre-target request is bound to the sole usage row created after its baseline"
require "the_post_target_usage_request_creates_one_identity" 1 "$post_target_usage_new_rows" \
  "the post-target request is bound to the sole usage row created after its baseline"
require "the_usage_request_ids_are_canonical" true \
  "$([[ "$pre_target_usage_id" =~ ^req_[0-9a-f-]{36}$ && "$post_target_usage_id" =~ ^req_[0-9a-f-]{36}$ ]] && echo true || echo false)" \
  "usage identities are UUIDv7-shaped request ids, not process-local counters"
require "the_usage_request_ids_are_globally_unique" true \
  "$([[ "$pre_target_usage_id" != "$post_target_usage_id" ]] && echo true || echo false)" \
  "a replayed request cannot be confused with the other request at the boundary"
require "the_pre_target_usage_record_survives" 1 "$recovered_pre_target_usage" \
  "the durable usage sink retains the request accepted before the target"
require "the_pre_target_usage_outbox_event_survives" 1 "$recovered_pre_target_outbox" \
  "the durable outbox retains the pre-target event that can be replayed"
require "the_post_target_usage_record_is_not_replayed" 0 "$recovered_post_target_usage" \
  "the usage sink does not contain a row from after the recovery target"
require "the_post_target_usage_outbox_event_is_not_replayed" 0 "$recovered_post_target_outbox" \
  "the recovered outbox does not replay an event beyond the target"
defer readiness "this stage reads recovered usage tables; the reconvergence stage owns serving"
defer max_serving_error_fraction "usage inspection offers no inference traffic"
defer max_convergence_lag_seconds "usage inspection does not converge a replica"
defer admin_writes "the recovery stage owns recovered-journal writes"
defer max_unauthenticated_admin_successes "the administration stage measures control-plane authentication"
close

# ---------------------------------------------------------------------------
stage point-in-time-recovery/reconvergence live "$schema_identity"
export AXOND_ADMIN_ENDPOINT="$recovered_endpoint"
mark "survivor-before-switch" \
  "the survivor retained its live serving snapshot while the recovered replica accepted the recovery publication"
observe survivor_before_revision "$live_head"
observe recovered_revision "$after_recovery"

# Point the same long-lived survivor at the promoted PITR cluster. The proxy
# closes its active sockets, so this exercises reconnection and convergence
# rather than a fresh process boot on recovered data.
kill -USR2 "$proxy_pid"
mark "journal-switched" "the survivor DSN was switched to the promoted PITR cluster"
survivor_report="$(wait_for_survivor "$after_recovery" || true)"
export AXOND_ADMIN_ENDPOINT="$live_endpoint"
survivor_converged="$(printf '%s' "$survivor_report" | jq -r '.converged // false' 2>/dev/null || printf 'false')"
[[ -n "$survivor_converged" ]] || survivor_converged=false
survivor_active="$(printf '%s' "$survivor_report" | jq -r '.active // "unreadable"' 2>/dev/null || printf 'unreadable')"
survivor_lag_seconds="$(printf '%s' "$survivor_report" | jq -r '(.lag_ms // -1) / 1000' 2>/dev/null || printf 'unknown')"
survivor_revision_count="$(revision_count 2>/dev/null || printf '%s' -1)"
survivor_readiness="$(curl -sS -o /dev/null -w '%{http_code}' \
  "${live_endpoint}/readyz" 2>/dev/null || printf '000')"
survivor_chat_status="$(probe_survivor_chat "${workdir}/survivor-pitr-chat.body")"
survivor_unauthenticated="$(unauthenticated_successes "$live_endpoint")"
mark "survivor-converged" "the survivor loaded and activated the recovered PITR head"
observe survivor_active_revision "$survivor_active"
observe survivor_convergence_lag_seconds "$survivor_lag_seconds" seconds
observe survivor_readiness_status "$survivor_readiness"
observe survivor_inference_status "$survivor_chat_status"
observe survivor_revision_count "$survivor_revision_count" count
observe unauthenticated_admin_successes "$survivor_unauthenticated" count
require "the_survivor_converges_to_the_recovered_head" true "$survivor_converged" \
  "the running survivor follows the journal after its stable DSN is switched to PITR"
require "the_survivor_active_revision_is_recovered" "$after_recovery" "$survivor_active" \
  "the survivor activates the publication accepted by the recovered journal"
require "the_survivor_revision_chain_is_recovered" "$recovered_revisions_after_publication" "$survivor_revision_count" \
  "the survivor reads the complete PITR revision chain"
require "the_survivor_is_ready_after_recovery" 200 "$survivor_readiness" \
  "the survivor remains ready once the recovered serving snapshot is active"
require "the_survivor_serves_inference_after_recovery" 200 "$survivor_chat_status" \
  "traffic is answered after the survivor converges onto the recovered journal"
survivor_lag_met="$(awk -v lag="$survivor_lag_seconds" \
  'BEGIN { print (lag >= 0 && lag <= 60) ? "true" : "false" }')"
gate max_serving_error_fraction \
  "$( [[ "$survivor_chat_status" == 200 ]] && echo 0 || echo 1 )" \
  "$( [[ "$survivor_chat_status" == 200 ]] && echo true || echo false )" \
  "the offered post-recovery inference request was answered"
gate max_convergence_lag_seconds "$survivor_lag_seconds" "$survivor_lag_met" \
  "the survivor converged to the recovered head inside the declared bound"
gate readiness \
  "$( [[ "$survivor_readiness" == 200 ]] && echo serves || echo refuses )" \
  "$( [[ "$survivor_readiness" == 200 ]] && echo true || echo false )" \
  "the survivor's readiness follows the recovered serving snapshot"
defer max_data_loss_revisions \
  "the recovery stage owns the durable revision-loss boundary"
defer admin_writes \
  "the recovery stage owns the accepted recovered-journal publication"
defer max_unauthenticated_admin_successes \
  "the administration stage measures recovered administrative authentication"
close

# ---------------------------------------------------------------------------
stage point-in-time-recovery/administration live "$schema_identity"
export AXOND_ADMIN_ENDPOINT="$recovered_endpoint"
audit="$(admin audit --revision "$pre_target_head" 2>/dev/null || printf '{"events":[]}')"
mark "audit-read" "the audit trail of ${pre_target_head} read back through the recovered replica"
observe audit_events_for_head "$(printf '%s' "$audit" | jq '.events | length')" count
require "the_audit_trail_survives_the_recovery" true \
  "$(printf '%s' "$audit" | jq '(.events | length) > 0')" \
  "the trail for a revision on the safe side of the target came back with it"
# Keep this scan non-quiet for the same pipefail/SIGPIPE reason as the restore
# audit above.
require "the_audit_trail_carries_no_secret_material" clean \
  "$(printf '%s' "$audit" | grep -F "$GW_DRILL_BREAKGLASS" >/dev/null && echo leaked || echo clean)" \
  "an audit read names the credential's env var, never its value"
require "the_audit_after_the_target_is_gone" refused \
  "$(admin audit --revision "$post_target_head" >/dev/null 2>&1 && echo readable || echo refused)" \
  "the boundary holds for the audit trail too, not only for revisions"

successes="$(unauthenticated_successes "$recovered_endpoint")"
mark "unauthenticated-refused" "two unauthenticated reads of /admin/v1/state on the recovered replica"
observe unauthenticated_admin_successes "$successes" count
gate max_unauthenticated_admin_successes "$successes" \
  "$([[ "$successes" == "0" ]] && echo true || echo false)" \
  "a recovered control plane does not come back with its administrative surface open"
defer admin_writes \
  "the recovery stage owns the accepted recovered-journal publication; this stage measures authentication and audit reads"
defer readiness "this stage reads the administrative surface; it offers no inference traffic"
defer max_serving_error_fraction "no traffic is offered, so the ceiling is vacuous by contract"
defer max_convergence_lag_seconds "nothing converges in an audit read"
defer max_data_loss_revisions "the \`recovery\` stage measures the boundary"
close

# ---------------------------------------------------------------------------
step "Checking the lane retained evidence for every stage it owes"
check_evidence() {
  GW_DRILL_PROVIDER_KEY="$GW_DRILL_PROVIDER_KEY" "$python_bin" "${root}/ops/check-recovery-evidence.py" --runner restore-drill \
    --since-unix-ms "$drill_started_ms" \
    --executable "$axond_bin" \
    --forbid-env GW_DRILL_BREAKGLASS --forbid-env GW_DRILL_KEK \
    --forbid-env GW_DRILL_PROVIDER_KEY
}
# A stage that failed is named before the checker's verdict, because the
# checker reads that stage's own failed check as incomplete evidence and would
# otherwise replace the diagnosis with a report that something is missing.
if ((${#failed_stages[@]})); then
  check_evidence || true
  fail "these stages failed: ${failed_stages[*]} (their artifacts are in target/recovery/)"
fi
check_evidence || fail "the evidence is incomplete"

printf '\nrestore drill passed: a deployment published through axond admin came back\n'
printf 'from a logical restore and from a point-in-time recovery, each read and\n'
printf 'extended by a replica booted on the recovered database, and the write after\n'
printf 'the recovery target was not replayed. Evidence: target/recovery/\n'
