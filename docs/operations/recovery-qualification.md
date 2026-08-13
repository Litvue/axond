# Recovery qualification

What a stateful deployment is required to do when it loses its control plane,
boots without one, gets it back, rotates a credential, or restores its database
— and what evidence a run of the recovery harness has to leave behind. The
design is [ADR 0037](../adr/0037-recovery-qualification-harness.md); the
behaviour being qualified is
[revision convergence](./revision-convergence.md#during-a-control-plane-outage)
and the [control-plane journal](./control-plane-journal.md#during-a-postgres-outage).

This page is a **contract, and a partial report.** Five stages run today against
a real PostgreSQL journal and write evidence to `target/recovery/`; the rest are
declared and blocked — see [the stages](#the-stages-and-what-runs-today) and
[what is blocked, and on what](#what-is-blocked-and-on-what). No scenario is
qualified end to end, because none of them can serve a request yet.

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
stages are, which is why every scenario below is still incomplete.

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
| `backup-restore` | The database is lost and restored from a backup: revisions, tenancy, secret metadata, pricing, and audit rows return together. | serves |
| `point-in-time-recovery` | Recovery to a chosen target rather than to a backup, with the data-loss boundary measured instead of assumed. | serves |

## The stages, and what runs today

| Stage | Status | What it does |
| --- | --- | --- |
| `control-plane-outage/journal-outage` | runs | Severs the link to a real journal under a converged replica: the active revision and its compiled snapshot are retained, an administrative publish is refused as `unavailable` without writing, and convergence reports the refusal. |
| `control-plane-outage/serving` | blocked | Inference offered across the same window, so the serving-error ceiling is measured rather than asserted. |
| `control-plane-outage/administration` | blocked | The authenticated administrative surface refusing writes, and the audit trail of the refusal. |
| `cold-boot-valid-cache/cold-boot` | runs | A cache exported by a converged replica restores a boot with the journal unreachable, reporting `source = last-known-good`. |
| `cold-boot-valid-cache/serving` | blocked | The restored snapshot answering requests. |
| `cold-boot-no-cache/cold-boot` | runs | The same boot with no cache: bootstrap refuses with the control plane named, and publishes nothing. |
| `cold-boot-no-cache/readiness` | blocked | The readiness endpoint an operator's tooling probes. |
| `cold-boot-invalid-cache/cold-boot` | runs | An edited record, a foreign signing key, and a truncated file each refuse the boot and name the cache. |
| `cold-boot-invalid-cache/readiness` | blocked | The readiness endpoint and the operator-facing report of a cache that refused itself. |
| `recovery-convergence/journal-recovery` | runs | The journal returns holding revisions the fleet never saw; both replicas converge to the head without intervention, writes are accepted again, and every revision is still readable. |
| `recovery-convergence/serving` | blocked | Traffic across the recovery. |
| `recovery-convergence/administration` | blocked | The audit trail read through an authenticated surface. |
| `secret-rotation/rotation` | blocked | Rotating material behind a credential reference without a redeployment. |
| `secret-rotation/serving` | blocked | Requests authenticated with the rotated material. |
| `backup-restore/restore` | blocked | Backup, loss, restore, and what came back with it. |
| `backup-restore/reconvergence` | blocked | Replicas converging onto the restored journal. |
| `point-in-time-recovery/recovery` | blocked | Recovery to a chosen target, with the data-loss boundary measured. |
| `point-in-time-recovery/reconvergence` | blocked | Serving across the recovery and converging onto the recovered head. |

## Running the stages that run

The driver lives in the crate rather than in `tests/`, because a recovery stage
has to hold a replica's reconciler, its signed cache, and a real
`PostgresControlPlane` at once and then take the database away from underneath
them — none of which is reachable from outside the binary while stateful boot is
not wired to `serve`.

```sh
AXOND_TEST_POSTGRES_DSN=postgres://postgres:secret@127.0.0.1:5432/postgres \
  cargo test -p axond --bin axond qualification::recovery
```

Each stage creates its own schema, migrates it with this build, and reaches it
through a loopback link the harness cuts to produce the outage — so the replica
meets a dead socket and a refused reconnect, and the database keeps its rows.
Without a DSN the stages write nothing rather than falling back to an in-process
control plane: an outage of a fake is evidence about the fake.

One JSON artifact per stage lands at
`target/recovery/<scenario>.<stage>.json`, carrying the build and schema it ran
against, the timeline, the observations, and a verdict per gate field. A field
the stage is not in a position to measure is recorded as `not_evaluated` with
the reason, never omitted — an artifact that listed only the gates it met would
read as a qualified scenario.

CI runs the same stages in the stateful lane, which has a real Postgres service,
and keeps the artifacts under `recovery-evidence` on the run.

## What a run retains

Nine evidence classes, and the committed scenarios have to cover all of them:

| Class | Holds |
| --- | --- |
| `outage_timeline` | When the control plane went away, when it returned, and what each replica did at every transition. |
| `serving_behavior` | Inference across the window: offered, answered, refused, and with which typed error. |
| `revisions` | Desired, loaded, and active revision per replica — the three that must be read together to say whether a fleet converged. |
| `convergence_lag` | How far behind desired state each replica was, sampled across the window. |
| `cold_start` | What a replica booting *into* the window did: restored from cache, refused readiness, or served. |
| `restore_duration` | Wall-clock restore time. Recorded, never asserted. |
| `data_loss_boundary` | What durable state did not survive, named rather than counted. |
| `fail_open_closed` | Which dependency failed open and which failed closed, per scenario. |
| `audit_auth` | Administrative authentication and audit outcomes across the window. |

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

Every scenario is still `blocked`, because every scenario has at least one
blocked stage. The reason is upstream of the harness: a replica cannot yet serve
a projected revision, because the resource bodies a revision is made of —
tenancy, providers, catalogue, pricing, policy — are owned by slices that have
not landed. So the serving, restore, and rotation halves are specified and
waiting, while the control-plane halves run against
[the journal](./control-plane-journal.md) and its forward-only migrations, the
convergence reconciler, and the signed last-known-good cache.

| Slice | What the harness needs from it |
| --- | --- |
| #144 | Durable tenants, projects, principals, RBAC, and audit boundaries — the tenancy and audit state a restore is checked against. |
| #145 | The `SecretStore` and the zero-redeploy credential lifecycle that `secret-rotation` is the evidence for, and the secret metadata a restore must bring back. |
| #146 | Imported catalogue snapshots, so a restored revision's blob references point at content that has to survive with it. |
| #147 | Effective-dated price books, so a restore covers pricing state and not only routing state. |
| #148 | A revision projection a replica can serve — without it, serving behaviour is asserted about an empty snapshot. |
| #149 | Tenant model enablement and aliases, so requests offered during an outage resolve to a target. |
| #150 | Dynamically configurable budgets, rate limits, and revocation, so a converged revision changes policy a request can be observed under. |
| #155 | The explicit decision on the durable usage outbox and globally unique request ids: whether a recovery boundary is measured over usage records at all, and against which identity. |
| #158 | The operator restore and recovery procedures this harness rehearses, and the deployment overlay that provisions the cache signing key. |
| #159 | The hardened workflow the recovery lane runs in — it runs a database with archiving enabled and publishes its evidence as an artifact. |

The manifest carries the same map per scenario, so landing a slice tells you
which scenarios it unblocks.

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
