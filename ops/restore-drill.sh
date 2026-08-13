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
# runs the four stages the manifest gives it and writes their evidence to
# `target/recovery/` in the same schema the in-process lane writes, through
# `ops/recovery-evidence.py`. Conditions are *recorded* and then judged at the
# end of each stage rather than aborting it, so a stage that fails still leaves
# an artifact saying what it observed. `ops/check-recovery-evidence.py` then
# refuses a run whose stages left nothing.
#
# Redis is deliberately absent. It holds hot state only — reservations, rate-limit
# windows, revocation caches — and losing it costs accuracy, not history, so this
# drill has nothing to restore for it.
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
container="${AXOND_DRILL_CONTAINER:-axond-restore-drill}"
live_port="${AXOND_DRILL_LIVE_PORT:-55442}"
restored_port="${AXOND_DRILL_RESTORED_PORT:-55443}"
# One replica per database, because the point of the drill is that a *replica*
# reads the recovered journal, not that psql can select from it.
live_http="${AXOND_DRILL_LIVE_HTTP:-18442}"
logical_http="${AXOND_DRILL_LOGICAL_HTTP:-18443}"
recovered_http="${AXOND_DRILL_RECOVERED_HTTP:-18444}"
password=drill
archive=/tmp/wal-archive
basebackup=/tmp/basebackup

workdir="$(mktemp -d)"
replicas=()
cleanup() {
  for pid in "${replicas[@]:-}"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  docker rm --force --volumes "$container" >/dev/null 2>&1 || true
  rm -rf "$workdir"
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
declare -A check_held=()

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
    check_held["$check"]=held
    printf '  ok  %s = %s\n' "$check" "$got"
  else
    check_held["$check"]=failed
    printf '  FAIL %s: expected %s, got %s\n' "$check" "$wanted" "$got"
  fi
}
# Whether every named check of this stage held. A name nothing recorded has not
# held, so a renamed or dropped check cannot leave a gate quietly passing.
held() {
  local check
  for check in "$@"; do
    [[ "${check_held[$check]:-missing}" == held ]] || return 1
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

axond_bin="${AXOND_BIN:-}"
if [[ -z "$axond_bin" ]]; then
  for candidate in "${root}/target/release/axond" "${root}/target/debug/axond"; do
    [[ -x "$candidate" ]] && axond_bin="$candidate" && break
  done
fi
if [[ -z "$axond_bin" ]]; then
  step "Building axond, the drill's verifier"
  cargo build -p axond --locked --manifest-path "${root}/Cargo.toml"
  axond_bin="${root}/target/debug/axond"
fi
[[ -x "$axond_bin" ]] || fail "no axond binary at ${axond_bin}"

command -v docker >/dev/null 2>&1 || fail "docker is required"
command -v curl >/dev/null 2>&1 || fail "curl is required to probe the drill's replicas"
command -v jq >/dev/null 2>&1 || fail "jq is required"
command -v openssl >/dev/null 2>&1 || fail "openssl is required for this run's secrets"

step "Starting ${image} with WAL archiving"
docker rm --force --volumes "$container" >/dev/null 2>&1 || true
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
  drill_config="${workdir}/${3}"
  cat >"$drill_config" <<EOF
mode = "stateful"

[server]
bind = "127.0.0.1:${http}"

[control_plane]
dsn_env = "GW_DRILL_DSN"

[secret_store]
kek_env = "GW_DRILL_KEK"

[[admin_breakglass]]
env = "GW_DRILL_BREAKGLASS"
EOF
  export GW_DRILL_DSN="postgres://postgres:${password}@127.0.0.1:${port}/${database}"
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
config live "$live_port" live.toml "$live_http"
live_config="$drill_config"
"$axond_bin" migrate apply --config "$live_config"
schema_identity="$("$axond_bin" migrate status --config "$live_config")" ||
  fail "the freshly migrated schema is not current"
schema_identity="$(printf '%s' "$schema_identity" | tr '\n' ' ')"

step "Applying the usage, budget, and revocation schemas"
for sql in usage_v2 budget_v1 budget_v2 revocation_v1; do
  psql live 5432 -f - <"${root}/ops/postgres/${sql}.sql" >/dev/null
done

step "Building the deployment a recovery has to bring back, through axond admin"
serve live "$live_http"
live_endpoint="$endpoint"

tenant=ten_01900000-0000-7000-8000-000000000001
provider=res_01900000-0000-7000-8000-000000000010
cat >"${workdir}/tenant.json" <<EOF
{"summary":"onboard the drill tenant","mutation":"create","resource":{
  "tenant":"${tenant}","slug":"drill","display_name":"Drill"}}
EOF
cat >"${workdir}/project.json" <<EOF
{"summary":"add the drill project","mutation":"create","resource":{
  "project":"prj_01900000-0000-7000-8000-000000000002","tenant":"${tenant}",
  "slug":"production","display_name":"Production"}}
EOF
cat >"${workdir}/provider.json" <<EOF
{"summary":"connect the drill tenant to openai","mutation":"create","resource":{
  "provider":"${provider}","tenant":"${tenant}","slug":"openai",
  "display_name":"OpenAI","wire_family":"openai-chat","endpoint":"https://api.openai.com"}}
EOF
# A credential is a *reference* to staged material, never the material: what a
# restore has to bring back here is the reference and its lifecycle.
cat >"${workdir}/credential.json" <<EOF
{"summary":"stage the drill openai key","mutation":"create","resource":{
  "credential":"res_01900000-0000-7000-8000-000000000011","tenant":"${tenant}",
  "provider":"${provider}","slug":"openai-primary","display_name":"OpenAI primary",
  "secret":"sct_01900000-0000-7000-8000-000000000012"}}
EOF
cat >"${workdir}/policy.json" <<EOF
{"summary":"cap the drill tenant","resource":{
  "tenant":"${tenant}","slug":"drill-limits","epoch":1,
  "subject_limit_microdollars":50000000,"namespace_limit_microdollars":500000000,
  "reservation_ttl_seconds":300,"max_in_flight_per_subject":8,"lease_ttl_seconds":60}}
EOF

head=empty
for pair in tenants:tenant projects:project providers:provider \
  credentials:credential policies:policy; do
  resource="${pair%%:*}"
  head="$(publish "$resource" "${workdir}/${pair##*:}.json" "drill-${resource}" "$head")"
  printf '  published %-12s -> %s\n' "$resource" "$head"
done
live_head="$head"
live_revisions="$(revision_count)"
live_resources="$(resource_count)"
live_checksum="$(admin history --limit 1 | jq -r '.revisions[0].checksum')"
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

config logical_restore "$live_port" logical.toml "$logical_http"
restored_schema="$("$axond_bin" migrate status --config "$drill_config" 2>&1 | tr '\n' ' ')" &&
  restored_current=current || restored_current=stale
require "the_restored_schema_is_current" current "$restored_current" \
  "a replica refuses a database whose schema it does not recognise: ${restored_schema}"

serve logical_restore "$logical_http"
logical_endpoint="$endpoint"
mark "replica-booted" "a replica booted on the restored database and opened /admin/v1"

require "the_restored_head_is_the_backed_up_head" "$live_head" "$(head_revision)" \
  "the replica reads the deployment the backup was taken from"
require "the_restored_revision_chain_is_whole" "$live_revisions" "$(revision_count)" \
  "no revision published before the backup was lost"
require "the_restored_deployment_is_whole" "$live_resources" "$(resource_count)" \
  "tenancy, the provider connection, its credential reference, and the policy all came back"
require "the_restored_head_checksum_matches" "$live_checksum" \
  "$(admin history --limit 1 | jq -r '.revisions[0].checksum')" \
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
observe readiness_probe "$(curl -s -o /dev/null -w '%{http_code}' "${logical_endpoint}/readyz")"

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
  "the blocked \`reconvergence\` stage owns what a restored replica serves; this replica answers /admin/v1 and refuses inference, which the readiness_probe observation records"
defer max_serving_error_fraction \
  "this stage offers no inference traffic, so the ceiling is vacuous by contract"
defer max_convergence_lag_seconds \
  "the blocked \`reconvergence\` stage measures replicas converging onto a restored journal"
defer max_unauthenticated_admin_successes \
  "the \`administration\` stage measures the administrative surface's authentication"
close

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
gate admin_writes \
  "$(held the_audit_trail_survives_the_restore && echo accepted || echo refused)" \
  "$(verdict the_audit_trail_survives_the_restore)" \
  "the authenticated surface answered the audit read the restore is qualified by"
defer readiness "this stage reads the administrative surface; it offers no inference traffic"
defer max_serving_error_fraction "no traffic is offered, so the ceiling is vacuous by contract"
defer max_convergence_lag_seconds "nothing converges in an audit read"
defer max_data_loss_revisions "the \`restore\` stage measures durable loss"
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
require "the_recovered_cluster_promotes" f "$in_recovery" \
  "a cluster still in recovery is not a cluster anyone can serve from"
mark "promoted" "the recovered cluster was promoted at the target"
observe restore_duration_seconds "$restore_seconds" seconds

config live "$restored_port" restored.toml "$recovered_http"
recovered_schema="$("$axond_bin" migrate status --config "$drill_config" 2>&1 | tr '\n' ' ')" &&
  recovered_current=current || recovered_current=stale
require "the_recovered_schema_is_current" current "$recovered_current" \
  "a replica refuses a recovered database whose schema it does not recognise: ${recovered_schema}"

serve recovered "$recovered_http"
recovered_endpoint="$endpoint"
mark "replica-booted" "a replica booted on the promoted cluster and opened /admin/v1"

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
require "the_write_after_the_target_is_not_replayed" absent \
  "$(admin history --limit 100 |
    jq -r --arg r "$post_target_head" 'if any(.revisions[]; .revision == $r) then "present" else "absent" end')" \
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
require "a_publication_against_the_recovered_head_is_accepted" accepted \
  "$([[ "$after_recovery" == refused ]] && echo refused || echo accepted)" \
  "the recovered journal takes the next change rather than needing to be rebuilt"
observe revision_after_recovery "$after_recovery"
observe readiness_probe "$(curl -s -o /dev/null -w '%{http_code}' "${recovered_endpoint}/readyz")"

for table in axond_usage axond_budget axond_revocation; do
  require "the_${table}_schema_is_recovered" 1 \
    "$(psql live 5433 -c "SELECT count(*) FROM pg_tables WHERE tablename = '${table}'")" \
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
gate admin_writes \
  "$(held the_audit_trail_survives_the_recovery the_audit_after_the_target_is_gone &&
    echo accepted || echo refused)" \
  "$(verdict the_audit_trail_survives_the_recovery the_audit_after_the_target_is_gone)" \
  "the authenticated surface answered the audit reads the boundary is measured with"
defer readiness "this stage reads the administrative surface; it offers no inference traffic"
defer max_serving_error_fraction "no traffic is offered, so the ceiling is vacuous by contract"
defer max_convergence_lag_seconds "nothing converges in an audit read"
defer max_data_loss_revisions "the \`recovery\` stage measures the boundary"
close

# ---------------------------------------------------------------------------
step "Checking the lane retained evidence for every stage it owes"
check_evidence() {
  "$python_bin" "${root}/ops/check-recovery-evidence.py" --runner restore-drill \
    --since-unix-ms "$drill_started_ms" \
    --forbid-env GW_DRILL_BREAKGLASS --forbid-env GW_DRILL_KEK
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
