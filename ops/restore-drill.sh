#!/usr/bin/env bash
# Prove that axond's durable state can be restored, and that a restore lands at a
# point in time an operator chose.
#
# A backup procedure that has never been restored is a hypothesis. This drill is
# the executable form of the recovery objectives in
# `docs/operations/backup-and-recovery.md`, run against a real PostgreSQL of the
# supported version, with axond's own commands as the verifier: the restored
# database is only accepted if `axond migrate status` reports the schema this
# build requires, which is the same check a replica makes before it serves.
#
# Two recoveries, because they fail differently:
#
#   1. a logical dump and restore — what a `pg_dump` in a nightly job gives you,
#      and what a migration to a new cluster uses;
#   2. point-in-time recovery from a base backup plus archived WAL to a target
#      time between two writes, which is the only recovery that answers "undo the
#      last twenty minutes". The assertion that matters is asymmetric: everything
#      committed before the target is present, and the write after it is gone.
#      A restore that replayed to the end of the WAL would pass a "the data is
#      there" check and be useless for the incident it exists for.
#
# Redis is deliberately absent. It holds hot state only — reservations, rate-limit
# windows, revocation caches — and losing it costs accuracy, not history, so this
# drill has nothing to restore for it.
#
# Usage:
#     ops/restore-drill.sh              # the whole drill, ~1 minute
#     AXOND_BIN=/path/to/axond ops/restore-drill.sh
#
# Needs Docker and a `cargo` build (or `AXOND_BIN`). Nothing outside the
# container is written except a temporary config directory.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# The supported backend version, from the single place that records it.
image="${AXOND_DRILL_POSTGRES_IMAGE:-$(
  sed -n 's/^ *image: *\(postgres:[^ ]*\) *$/\1/p' "${root}/.github/workflows/ci.yml" | head -n 1
)}"
container="${AXOND_DRILL_CONTAINER:-axond-restore-drill}"
live_port="${AXOND_DRILL_LIVE_PORT:-55442}"
restored_port="${AXOND_DRILL_RESTORED_PORT:-55443}"
password=drill
archive=/tmp/wal-archive
basebackup=/tmp/basebackup

workdir="$(mktemp -d)"
cleanup() {
  docker rm --force --volumes "$container" >/dev/null 2>&1 || true
  rm -rf "$workdir"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$1"; }
fail() {
  printf 'restore drill failed: %s\n' "$1" >&2
  exit 1
}

# psql inside the container: the client is the server's own, so no version skew,
# and `ON_ERROR_STOP` makes a failed statement a failed drill.
psql() {
  local database="$1" port="$2"
  shift 2
  docker exec -i -u postgres "$container" \
    psql -v ON_ERROR_STOP=1 -qtAX -p "$port" -d "$database" "$@"
}

expect() {
  local what="$1" wanted="$2" got="$3"
  [[ "$got" == "$wanted" ]] || fail "${what}: expected ${wanted}, got ${got}"
  printf '  ok  %s = %s\n' "$what" "$got"
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

for _ in $(seq 60); do
  if docker exec -u postgres "$container" pg_isready -q -p 5432; then break; fi
  sleep 1
done
docker exec -u postgres "$container" pg_isready -p 5432 >/dev/null ||
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
  local database="$1" port="$2"
  drill_config="${workdir}/${3}"
  cat >"$drill_config" <<EOF
mode = "stateful"

[server]
bind = "127.0.0.1:0"

[control_plane]
dsn_env = "GW_DRILL_DSN"

[secret_store]
kek_env = "GW_DRILL_KEK"

[[admin_breakglass]]
env = "GW_DRILL_BREAKGLASS"
EOF
  export GW_DRILL_DSN="postgres://postgres:${password}@127.0.0.1:${port}/${database}"
}

# The install path from docs/operations/control-plane-journal.md, not a hand-rolled
# one: the ledger row that `migrate status` reads is written by `migrate apply`,
# so a drill that applied the DDL with psql would be restoring a database no
# replica would accept.
export GW_DRILL_KEK="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
export GW_DRILL_BREAKGLASS="drill-breakglass-credential"

step "Installing the control-plane schema with axond migrate apply"
config live "$live_port" live.toml
live_config="$drill_config"
"$axond_bin" migrate apply --config "$live_config"
"$axond_bin" migrate status --config "$live_config" ||
  fail "the freshly migrated schema is not current"

step "Applying the usage, budget, and revocation schemas"
for sql in usage_v2 budget_v1 budget_v2 revocation_v1; do
  psql live 5432 -f - <"${root}/ops/postgres/${sql}.sql" >/dev/null
done

step "Writing the state a recovery has to bring back"
psql live 5432 >/dev/null <<'SQL'
INSERT INTO axond_cp_mutation (mutation_id, actor_kind, actor_issuer, actor_subject,
    mutation_kind, scope_kind, idempotency_key, submitted_at)
VALUES ('mut_00000000-0000-7000-8000-000000000001', 'human', 'https://issuer.example',
    'operator@example', 'publish', 'deployment', 'drill-1', now());

INSERT INTO axond_cp_resource_version (resource_kind, resource_id, version, scope_kind,
    slug, body_form, body_inline, content_checksum, serializer)
VALUES ('model_alias', 'res_00000000-0000-7000-8000-000000000001', 1, 'deployment',
    'gpt-4o', 'inline', '\x7b7d',
    'sha256:1111111111111111111111111111111111111111111111111111111111111111', 'json/v1');

INSERT INTO axond_cp_revision (revision_id, parent_id, mutation_id, serializer,
    state_checksum, created_at)
VALUES ('rev_00000000-0000-7000-8000-000000000001', NULL,
    'mut_00000000-0000-7000-8000-000000000001', 'json/v1',
    'sha256:2222222222222222222222222222222222222222222222222222222222222222', now());

INSERT INTO axond_cp_revision_entry (revision_id, resource_kind, resource_id, version)
VALUES ('rev_00000000-0000-7000-8000-000000000001', 'model_alias',
    'res_00000000-0000-7000-8000-000000000001', 1);

INSERT INTO axond_cp_audit_event (audit_event_id, revision_id, mutation_id, actor_kind,
    actor_issuer, actor_subject, event_kind, summary, recorded_at)
VALUES ('aud_00000000-0000-7000-8000-000000000001',
    'rev_00000000-0000-7000-8000-000000000001',
    'mut_00000000-0000-7000-8000-000000000001', 'human', 'https://issuer.example',
    'operator@example', 'revision.published', 'published the drill revision', now());

UPDATE axond_cp_head SET revision_id = 'rev_00000000-0000-7000-8000-000000000001';
SQL
psql live 5432 -c 'SELECT pg_switch_wal()' >/dev/null

step "Recovery 1: a logical dump and restore"
docker exec -u postgres "$container" sh -c \
  'pg_dump -p 5432 -d live -Fc -f /tmp/live.dump'
docker exec -u postgres "$container" sh -c \
  'pg_restore -p 5432 -d logical_restore --no-owner /tmp/live.dump'
config logical_restore "$live_port" logical.toml
"$axond_bin" migrate status --config "$drill_config" ||
  fail "the logically restored schema is not current"
expect "restored head" \
  "rev_00000000-0000-7000-8000-000000000001" \
  "$(psql logical_restore 5432 -c 'SELECT revision_id FROM axond_cp_head')"
expect "restored state checksum" \
  "$(psql live 5432 -c 'SELECT state_checksum FROM axond_cp_revision ORDER BY seq')" \
  "$(psql logical_restore 5432 -c 'SELECT state_checksum FROM axond_cp_revision ORDER BY seq')"
expect "restored audit trail" "1" \
  "$(psql logical_restore 5432 -c 'SELECT count(*) FROM axond_cp_audit_event')"

step "Recovery 2: point-in-time recovery to a chosen moment"
docker exec -u postgres "$container" \
  pg_basebackup -p 5432 -D "$basebackup" -Fp -Xs -c fast
# The target is after the first revision and before the second. A second of
# separation on either side keeps the assertion about *what* was committed rather
# than about clock resolution.
sleep 1
target="$(psql live 5432 -c 'SELECT now()')"
sleep 1

psql live 5432 >/dev/null <<'SQL'
INSERT INTO axond_cp_mutation (mutation_id, actor_kind, actor_component,
    mutation_kind, scope_kind, idempotency_key, submitted_at)
VALUES ('mut_00000000-0000-7000-8000-000000000002', 'system', 'drill',
    'publish', 'deployment', 'drill-2', now());

INSERT INTO axond_cp_revision (revision_id, parent_id, mutation_id, serializer,
    state_checksum, created_at)
VALUES ('rev_00000000-0000-7000-8000-000000000002',
    'rev_00000000-0000-7000-8000-000000000001',
    'mut_00000000-0000-7000-8000-000000000002', 'json/v1',
    'sha256:3333333333333333333333333333333333333333333333333333333333333333', now());

UPDATE axond_cp_head SET revision_id = 'rev_00000000-0000-7000-8000-000000000002';
SQL
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
expect "restored cluster promoted" "f" "$in_recovery"

config live "$restored_port" restored.toml
"$axond_bin" migrate status --config "$drill_config" ||
  fail "the point-in-time restored schema is not current"
expect "revisions recovered" "1" \
  "$(psql live 5433 -c 'SELECT count(*) FROM axond_cp_revision')"
expect "head at the recovery target" \
  "rev_00000000-0000-7000-8000-000000000001" \
  "$(psql live 5433 -c 'SELECT revision_id FROM axond_cp_head')"
expect "the write after the target is not replayed" "0" \
  "$(psql live 5433 -c "SELECT count(*) FROM axond_cp_revision
    WHERE revision_id = 'rev_00000000-0000-7000-8000-000000000002'")"
expect "audit trail recovered" "1" \
  "$(psql live 5433 -c 'SELECT count(*) FROM axond_cp_audit_event')"
expect "usage schema recovered" "1" \
  "$(psql live 5433 -c "SELECT count(*) FROM pg_tables WHERE tablename = 'axond_usage'")"
expect "budget schema recovered" "1" \
  "$(psql live 5433 -c "SELECT count(*) FROM pg_tables WHERE tablename = 'axond_budget'")"
expect "revocation schema recovered" "1" \
  "$(psql live 5433 -c "SELECT count(*) FROM pg_tables WHERE tablename = 'axond_revocation'")"

printf '\nrestore drill passed: logical restore and point-in-time recovery both\n'
printf 'produced a database axond migrate status accepts, and the write after the\n'
printf 'recovery target was not replayed.\n'
