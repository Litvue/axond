# Recovery qualification

What a stateful deployment is required to do when it loses its control plane,
boots without one, gets it back, rotates a credential, or restores its database
— and what evidence a run of the recovery harness has to leave behind. The
design is [ADR 0037](../adr/0037-recovery-qualification-harness.md); the
behaviour being qualified is
[revision convergence](./revision-convergence.md#during-a-control-plane-outage)
and the [control-plane journal](./control-plane-journal.md#during-a-postgres-outage).

This page is a **contract, and a partial report.** Twenty-two stages run today against
real PostgreSQL — fourteen in the combined stateful-tests lane and eight against a
backed-up and point-in-time-recovered database driven through `axond admin` — and
each writes evidence to `target/recovery/`; the usage boundary is measured over
the durable outbox and sink — see [the stages](#the-stages-and-what-runs-today).
The serving path
is now assembled in both lanes. The black-box stateful integration lane retains evidence for
cached cold-start serving, no-cache and invalid-cache refusal, the real
Postgres outage boundary, recovery convergence, and secret rotation. Restore
and point-in-time reconvergence retain survivor reconnection, convergence,
readiness, and inference evidence in the restore-drill lane.

For the stateless request path under load, read
[capacity qualification](./capacity.md) instead: it qualifies a Tier 0 process
and is not evidence about stateful serving.

## The committed manifest

`qualification/recovery/manifest.toml` is the input, in the same sense the
capacity manifest is: each scenario declares a driver capability, the evidence
it retains, the gate it fails on, and — while it is blocked — the slices it is
waiting on and what it needs from each. A scenario the driver has no capability
for cannot be written, so a result is always reproducible from the repository.

A scenario is split into **stages** — the control-plane half and the serving
half became executable at different times — and each stage carries its own
status, evidence, and blockers. A scenario is executable exactly when all of its
stages are. The separate stateful integration lane owns the process-level stages
marked `driver = "stateful-integration"`; the restore drill owns the durable
usage-boundary measurement. The recovery slice still lacks a retained fleet
record, even though all of its stages are executable.

An executable stage also names the **lane** that runs it, because the two halves
of the harness need different machinery:

| Lane | What it is | What it runs |
| --- | --- | --- |
| `stateful-tests` | The in-process driver in `crates/gateway/src/qualification/recovery.rs` plus the black-box stateful integration test lane. | An outage produced by cutting a loopback link to a real journal, process-level outage and recovery serving, the cold boots and convergence around it, cached process serving, and secret rotation. |
| `restore-drill` | `ops/restore-drill.sh`, run by `just restore-drill`. | A real PostgreSQL with WAL archiving in Docker, a deployment published through `axond admin` against a running replica, a logical restore, and a point-in-time recovery — each read and extended by a replica booted on the recovered database, with a long-lived survivor switched onto each recovered journal to prove reconvergence and serving. |

A stage that claims neither lane, and a lane with no stages, both fail the
contract test: a lane nothing runs is how a harness quietly stops running.

`crates/gateway/tests/recovery_contract.rs` keeps the file honest: it fails when
a scenario loses its gate, when the evidence classes below stop being covered,
when a dependency edge is dropped or invented, when this page and the manifest
disagree, and when a stage claims to be executable while still naming a blocker.
The complementary check lives next to the driver, in
`crates/gateway/src/qualification/recovery.rs`: the stages the manifest calls
executable and the stages the driver runs must be the same set, so a status can
never be upgraded by editing the manifest alone.

## The scenarios

| Scenario | What happens | Terminal readiness |
| --- | --- | --- |
| `control-plane-outage` | A converged replica loses Postgres: inference continues from the active snapshot, administrative writes fail retryably, lag grows, reason reads `unavailable`. | serves |
| `cold-boot-valid-cache` | A replica boots into the outage and restores the signed last-known-good cache, reporting `source = last-known-good`. | serves |
| `cold-boot-no-cache` | The same boot with no cache file: readiness is refused rather than an empty configuration served. | refuses |
| `cold-boot-invalid-cache` | The same boot against a cache that fails its authentication: readiness is refused and the cache's own failure reported. | refuses |
| `recovery-convergence` | Postgres returns holding revisions the fleet never saw; every replica converges without intervention. | serves |
| `secret-rotation` | A provider credential is rotated in the secret store and published; replicas pick it up by converging, with no restart or redeployment. | serves |
| `backup-restore` | The database is lost and restored from a backup: revisions, tenancy, secret metadata, the retained catalogue snapshot, and audit rows return together; approved price-book history remains an explicit qualification blocker. | serves |
| `point-in-time-recovery` | Recovery to a chosen target rather than to a backup, with the revision, secret metadata, catalogue pointer/payload, audit, and data-loss boundaries measured instead of assumed. | serves |

## The stages, and what runs today

| Stage | Status | What it does |
| --- | --- | --- |
| `control-plane-outage/journal-outage` | runs | Severs the link to a real journal under a converged replica: the active revision and its compiled snapshot are retained, an administrative publish is refused as `unavailable` without writing, and convergence reports the refusal. |
| `control-plane-outage/serving` | runs | A real TCP fault proxy severs Postgres beneath a converged process; inference is offered while the active snapshot remains available. |
| `control-plane-outage/administration` | runs | The authenticated state read and alias mutation refuse with `503`, anonymous administration remains `401`, and no outage write is accepted. |
| `cold-boot-valid-cache/cold-boot` | runs | A cache exported by a converged replica restores a boot with the journal unreachable, reporting `source = last-known-good`. |
| `cold-boot-valid-cache/serving` | runs | The restored snapshot answering requests, with readiness and authentication refusal retained by the stateful integration lane. |
| `cold-boot-no-cache/cold-boot` | runs | The same boot with no cache: bootstrap refuses with the control plane named, and publishes nothing. |
| `cold-boot-no-cache/readiness` | runs | A process booted without a cache remains reachable for authenticated administration but returns `503` readiness and `401` anonymous inference. |
| `cold-boot-invalid-cache/cold-boot` | runs | An edited record, a foreign signing key, and a truncated file each refuse the boot and name the cache. |
| `cold-boot-invalid-cache/readiness` | runs | A process with edited signed and compiled caches remains reachable for authenticated administration but returns `503` readiness and `401` anonymous inference. |
| `recovery-convergence/journal-recovery` | runs | The journal returns holding revisions the fleet never saw; both replicas converge to the head without intervention, writes are accepted again, and every revision is still readable. |
| `recovery-convergence/serving` | runs | The same process serves after the TCP path returns and converges to a revision published during the outage. |
| `recovery-convergence/administration` | runs | The recovered authenticated administrative surface reads the revision audit with its `breakglass` actor attribution. |
| `secret-rotation/rotation` | runs | Rotating material behind a credential reference without a redeployment. |
| `secret-rotation/serving` | runs | Requests authenticated with the rotated material and authenticated audit attribution. |
| `backup-restore/restore` | runs | A deployment published through `axond admin` is dumped, restored into a database no replica ever wrote, and read back by a replica booted on it: same head, same checksum, whole revision chain, whole resource set, a publication against the restored head accepted, and `/readyz` plus inference serve once the projected snapshot exists. |
| `backup-restore/administration` | runs | The audit trail read back through the authenticated surface of that replica, refused there without a credential, and checked to name a credential's reference rather than any material. |
| `backup-restore/durable-inventory` | runs | The encrypted secret material and lifecycle, retained catalogue snapshot and active pointer, and the approved price book's checksum, catalogue pin, approval, effective-dated rules, rates, and provenance beyond the journal and its tenancy. |
| `backup-restore/reconvergence` | runs | A long-lived survivor is switched to the logical-restore database through a stable DSN, reconnects, converges to the recovered head, remains ready, and answers authenticated inference traffic. |
| `point-in-time-recovery/recovery` | runs | A base backup plus archived WAL recovered to a target taken between two publications: everything before the target is present, the revision after it is absent, and a replica booted on the promoted cluster reads the pre-target head and accepts a publication against it. |
| `point-in-time-recovery/administration` | runs | The audit trail on the safe side of the target read back through the authenticated surface; the trail of the revision after it is gone with the revision. |
| `point-in-time-recovery/usage-boundary` | runs | The pre-target request's canonical ID and durable outbox/sink rows survive; the post-target request is absent and not replayed. |
| `point-in-time-recovery/reconvergence` | runs | The same survivor is switched to the promoted PITR cluster, reconnects, converges to the recovered head, remains ready, and answers authenticated inference traffic. |

## Running the stages that run

### The `stateful-tests` lane

The driver lives in the crate rather than in `tests/`, because a recovery stage
has to hold a replica's reconciler, its signed cache, and a real
`PostgresControlPlane` at once and then take the database away from underneath
them. The separate stateful integration lane owns process-level serving, outage,
recovery, and rotation; this driver owns the in-process severable-link evidence
and the durable recovery contract.

```sh
AXOND_TEST_POSTGRES_DSN=postgres://postgres:secret@127.0.0.1:5432/postgres \
  cargo test -p axond --bin axond qualification::recovery
```

Each stage creates its own schema, migrates it with this build, and reaches it
through a loopback link the harness cuts to produce the outage — so the replica
meets a dead socket and a refused reconnect, and the database keeps its rows.
Without a DSN the stages write nothing rather than falling back to an in-process
control plane: an outage of a fake is evidence about the fake. The process-level
serving and rotation path is exercised separately by
`stateful_revision_compiles_rotates_and_recovers`; the stages below remain the
durable recovery driver's evidence contract.

One JSON artifact per stage lands at
`target/recovery/<scenario>.<stage>.json`, carrying the build and schema it ran
against, the timeline, the observations, and a verdict per gate field. A field
the stage is not in a position to measure is recorded as `not_evaluated` with
the reason, never omitted — an artifact that listed only the gates it met would
read as a qualified scenario.

Under `AXOND_TEST_REQUIRE_SERVICES=1` a DSN the harness cannot cut — a
multi-host failover string, a Unix socket, an unresolvable host — fails the run
instead of skipping it, so a lane that quietly measured nothing cannot report
green.

### The `restore-drill` lane

```sh
just restore-drill              # builds the current gateway, then runs the drill
# or, after building target/debug/axond yourself:
bash ops/restore-drill.sh
```

It needs Docker, `psql`, `pg_dump`, `pg_restore`, `pg_basebackup`, `jq`,
`curl`, `openssl`, and `python3` 3.10 or newer — the ops floor, where the
lockfile's `tomli` stands in for `tomllib`. `just restore-drill` runs it on the
venv `just ops-venv` builds from `ops/deploy-requirements.txt`, so the backport
is pinned rather than assumed; running the script directly picks that venv up
too, and `AXOND_PYTHON` overrides both. It starts `postgres:17.6-alpine`
with WAL archiving, migrates it with
`axond migrate apply`, and then does everything else the way an operator would:
a live replica is started on the live database, a ten-resource deployment is
published through `axond admin apply --resource …`, and each recovered database
is handed to a replica of its own on its own port. A disposable
`redis:7.4.2-alpine` instance supplies the shared lease backend required by the
projected concurrency policy; its hot state is not part of the recovered
evidence. The ports
(`AXOND_DRILL_LIVE_HTTP`, `AXOND_DRILL_LOGICAL_HTTP`,
`AXOND_DRILL_RECOVERED_HTTP`) and the container name are overridable for a
machine that is already using them.

The drill's replicas authenticate with a breakglass credential taken from the
environment — the drill generates a throwaway one, and neither it nor the cache
signing key is ever printed or written to an artifact. The checker enforces
that: it refuses any artifact containing either value.

Both lanes record their conditions and judge them at the end of a stage rather
than aborting on the first one that fails, so a stage that fails still leaves an
artifact saying what it observed. A failure is deterministic: the stage names
the check, its bound, and what it saw, and the run exits non-zero after the
evidence is on disk.

### Checking that the evidence is actually there

```sh
ops/check-recovery-evidence.py --runner stateful-tests
ops/check-recovery-evidence.py --runner restore-drill
```

Each lane owes an artifact for every stage the manifest gives it. The checker
reads the manifest, then refuses a missing artifact, a wrong schema version, a
wrong scenario, stage, lane, capability or evidence set, a failed gate or check,
an empty timeline, an artifact an earlier run left behind (`--since-unix-ms`),
and any artifact carrying a string named by `--forbid-env`. CI runs it in
both lanes before uploading `target/recovery/`, so a lane that produced no
evidence fails the build instead of uploading nothing.

## What a run retains

Twelve evidence classes, and the committed scenarios have to cover all of them:

| Class | Holds |
| --- | --- |
| `outage_timeline` | When the control plane went away, when it returned, and what each replica did at every transition. |
| `serving_behavior` | Inference across the window: offered, answered, refused, and with which typed error. |
| `revisions` | Desired, loaded, and active revision per replica — the three that must be read together to say whether a fleet converged. |
| `convergence_lag` | How far behind desired state each replica was, sampled across the window. |
| `cold_start` | What a replica booting *into* the window did: restored from cache, refused readiness, or served. |
| `restore_duration` | Wall-clock restore time. Recorded, never asserted. |
| `data_loss_boundary` | What durable state did not survive, named rather than counted. |
| `usage_loss_boundary` | Which usage records and durable outbox events survived the recovery target, keyed by canonical request identity. |
| `durable_inventory` | Secret metadata/ownership and catalogue pointer, raw payload, and history that a recovered revision references. |
| `revision_loss_boundary` | Which revisions a recovery kept and which it left behind, which is what `max_data_loss_revisions` counts. |
| `fail_open_closed` | Which dependency failed open and which failed closed, per scenario. |
| `audit_auth` | Administrative authentication and audit outcomes across the window. |
| `pricing_history` | Whether approved, effective-dated price-book history survived with its catalogue identity and version. |

## What fails, and what is only recorded

Hard failures, declared per scenario, restricted to properties that do not move
with the machine the run happened on:

- **`max_serving_error_fraction`** — of the requests offered during the window,
  the fraction that may fail. Zero in every committed scenario: a control-plane
  outage degrades change, not serving. A scenario whose readiness gate is
  `refuses` offers no inference traffic at all — it carries no `serving_behavior`
  evidence — so its zero is vacuous rather than a promise that a replica which
  refused readiness still answers. Conversely a scenario whose readiness gate is
  `serves` must retain `serving_behavior`, so the ceiling is measured against
  offered requests instead of passing by default.
- **`max_convergence_lag_seconds`** — how long after the control plane returns a
  replica may still be behind desired state.
- **`max_data_loss_revisions`** — revisions committed before the recovery target
  that the recovered database no longer holds. For `point-in-time-recovery` the
  loss is measured *relative to the chosen target*: revisions published after it
  are expected to be gone, and that boundary is the point of the scenario.
- **`readiness`** — what readiness must say once the window closes. A replica
  that cannot serve says so; it never reports itself healthy while holding no
  configuration.
- **`admin_writes`** — `unavailable` while the control plane is gone (retryable,
  writing nothing), `accepted` once it is back.
- **`max_unauthenticated_admin_successes`** — zero, in every scenario, always.
  An outage may refuse a change; it may never admit a caller.

Recorded and never asserted: outage and restore durations, lag samples, and
throughput across the window. A shared CI runner cannot bound those without
flaking, and a flaky recovery gate is one that gets switched off.

## What is blocked, and on what

The process-level outage and returned-journal stages are now executable in the
stateful integration lane, alongside the cold-boot and secret-rotation stages.
The restore, usage-boundary, and point-in-time reconvergence stages are
executable in the restore-drill lane. All twenty-two recovery stages now have a
runner; the remaining gap is a retained fleet record, not an unimplemented
stage.
The durable half runs: the journal and its
forward-only migrations, the convergence reconciler, the signed last-known-good
cache, and — in the restore lane — the backup, the point-in-time recovery, the
durable inventory, and the administrative surface of a replica booted on the
result.

The restore lane's own boundary is worth stating plainly, because a restore that
brings back revisions reads as complete: it qualifies the revision journal, its
checksums, the tenancy and access projections, the encrypted secret material and
its lifecycle, retained catalogue snapshots, approved price-book identity and
content, effective-dated pricing history, approval citation, per-rule rates and
provenance, and the audit rows. Usage records are measured by the executable
`usage-boundary` stage: the pre-target request is identified in both the durable
sink and outbox, while the post-target request is absent from both after
promotion.

The former #155 dependency is resolved by that stage's explicit contract: usage
is measured through the Postgres outbox and sink, keyed by canonical globally
unique request IDs. The run remains harnessed until a clean fleet-level record is
retained alongside the stage artifacts.

The manifest carries the same map per scenario, so landing a slice tells you
which scenarios it unblocks.

### Slices this harness stopped waiting on

| Slice | Why no stage waits on it, and where the rest of it lives |
| --- | --- |
| #146 | Retired for recovery qualification: the restore lane exercises the existing Postgres-backed catalogue store and checks its retained active snapshot. The remaining import/source behavior stays on #146. |
| #159 | The part a recovery scenario needed — a database running with WAL archiving, and the evidence published as an artifact — is here: `ops/restore-drill.sh` runs that lane, and `ops/check-recovery-evidence.py` fails it when an executable stage leaves no artifact. The rest of #159 — disclosure, fuzzing, SDK compatibility — blocks no recovery stage, so it stays tracked on #159 itself rather than on a stage invented to wait on it. |

The manifest carries the same list in `[[retired_blocker]]`, and
`RETIRED_BLOCKERS` in the contract test holds the two together, so a slice can
only leave the map by saying what became of it.

## Related

- [ADR 0037](../adr/0037-recovery-qualification-harness.md) — the harness
  contract and why the scenarios are committed before the driver.
- [ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) — the mode
  boundary that makes an outage a change-freeze rather than an inference
  incident.
- [Revision convergence](./revision-convergence.md) — what a replica reports,
  the refusal reasons, and the signed last-known-good cache.
- [Control-plane revision journal](./control-plane-journal.md) — the schema,
  migrations, backup guidance, and the outage behaviour being qualified.
- [Capacity qualification](./capacity.md) — the stateless load harness this one
  is modelled on.
- [Qualification packet](./qualification.md) — where this contract sits in what
  production qualification has and has not measured.
