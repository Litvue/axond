# The production qualification packet

What has actually been measured about running Axond in production, what has only
been declared or harnessed, and what has not yet been retained — in one place, so the difference
between a merged harness and an answered question stays visible.

> **Architecture transition:**
> [ADR 0062](../adr/0062-blob-backed-flat-namespace-control-plane.md) accepts a
> blob-backed flat-namespace stateful-v2 topology. Do not dispatch the current
> PostgreSQL stateful-v1 cohort as production evidence for that target. The
> manifests and historical records below remain truthful for their source and
> topology; the
> [migration plan](../maintainers/namespace-control-plane-migration.md) defines
> how every slice must be re-cut before qualification restarts.
>
> The blob-backed stateful-v2 gates remain **pending**. A green `CI Success`
> proves the active software-change checks passed; it does not promote skipped
> PostgreSQL evidence into blob-backed qualification. Ordinary pull requests,
> pushes, merge queues, and schedules cannot start the legacy PostgreSQL cohort.

## Paused legacy PostgreSQL workflows

The PostgreSQL stateful-v1 lanes are retained only for deliberate compatibility
investigations. Each invocation must opt in explicitly; omitting the input or
setting it to false skips every legacy job:

```bash
gh workflow run ci.yml --ref <branch> \
  -f run_legacy_postgres_qualification=true
gh workflow run endurance.yml --ref <branch>
gh workflow run endurance.yml --ref <branch> \
  -f run_legacy_postgres_qualification=true
gh workflow run endurance.yml --ref <branch> \
  -f run_stateless_endurance_smoke=true
gh workflow run rollout.yml --ref <branch> \
  -f run_legacy_postgres_qualification=true
```

The `CI` opt-in covers the historical PostgreSQL stateful tests, restore/PITR,
recovery-record assembly, stateful endurance smoke, and both PostgreSQL-backed
Kubernetes stateful/PVC drills. The endurance and rollout workflows have no
schedule. The `Endurance smoke` workflow requires an explicit
`run_stateless_endurance_smoke=true` or `run_legacy_postgres_qualification=true`
input, exposes no duration input, and applies a hard 15-minute timeout to both
smoke jobs. Full stateless and target-topology long soaks run on dedicated
qualification infrastructure outside GitHub Actions.

Production qualification ([#156](https://github.com/Litvue/axond/issues/156))
decomposes into six slices. They landed, and will land, at different depths:

| Slice | Issue | Status | What exists today |
| --- | --- | --- | --- |
| `capacity` | [#217](https://github.com/Litvue/axond/issues/217) | `harnessed` | Driver and eight committed profiles — including multi-tenant isolation, admission shedding, a bounded stalling backend, and decoded production queue-depth telemetry. Its historical records predate raw-artifact-bound compact schema 2 and are not active evidence. |
| `endurance` | [#221](https://github.com/Litvue/axond/issues/221) | `harnessed` | Stateless mixed-workload driver and committed mix whose CI smoke tier is the ship gate. Promote a frozen-cohort smoke record to evidence it. The 12-hour soak is scheduled observational (per-hour drift), not a publication requirement. |
| `stateful-endurance` | [#221](https://github.com/Litvue/axond/issues/221) | `harnessed` | Multi-replica driver for revisions, backend faults, tenant isolation, durable accounting, and rolling restarts. The CI smoke tier is the ship gate; promote a frozen-cohort smoke record. The 12-hour soak is scheduled observational, not a publication requirement. |
| `recovery` | [#219](https://github.com/Litvue/axond/issues/219) | `harnessed` | Driver and twenty-two executable real-Postgres stages exist. The historical v0.3.39 debug record remains indexed for audit, but a release-profile v0.4.0 cohort rerun with raw schema 2, per-stage executable digests, and process-bound executed-binary identity is pending. |
| `fault` | [#218](https://github.com/Litvue/axond/issues/218) | `harnessed` | The provider, transport, Redis, and Postgres matrix is complete. Its historical v0.3.39 debug record remains indexed for audit, but raw-schema-1 release evidence from the v0.4.0 cohort is pending. |
| `rollout` | [#220](https://github.com/Litvue/axond/issues/220) | `harnessed` | Driver and committed reduced/heavy scenarios now require published v0.3.40 and candidate v0.4.0 serving one shared durable revision and alias through the real-Postgres migration/rollback matrix. A loopback OTLP receiver preserves exact caller traces for the retained executable. No raw-schema-5/compact-schema-4 record is retained. |

`qualification/packet.toml` is that table as data — question, inputs, lanes,
retained runs, and what each slice still owes; see
[ADR 0045](../adr/0045-qualification-packet-and-evidence-records.md) for why the
packet and the record are shaped this way. And
`crates/gateway/tests/qualification_packet.rs` checks it against the tree on
every run: a path that does not exist, a status a slice has not earned, a
scenario no slice covers, or a retained record that cannot be reproduced from
the manifest it names is a test failure.

## What the packet may not be read as

- **Not a claim that Axond is production-qualified.** All six slices are
  currently harnessed. #156 stays open until every slice has exactly one heavy
  release-profile record from the same clean v0.4.0 source cohort;
  `closure.satisfied` is derived from those records and cannot be set by hand.
- **Not yet a stateful endurance baseline.** The historical capacity records are Tier 0
  local runs; the historical fault and recovery records are v0.3.39 debug
  GitHub Actions runs against pinned Redis/Postgres lanes and are not active
  closure evidence. The stateful endurance harness has a ninety-second CI smoke
  tier as its ship gate; the 12–24 hour soak is scheduled observational.
- **Not a long-soak baseline.** The 12-hour soak is not a v0.4.0 publication
  gate. Rollout offers fleet load for under a minute; no current rollout record
  is retained.

## The status ladder

A slice climbs four rungs, and each is derived from what the slice has rather
than from what it says:

| Status | Reached when |
| --- | --- |
| `unbuilt` | The question is written down. Nothing runs. |
| `declared` | A committed manifest and contract page, kept honest by a `contract_test`, with no driver behind them. A contract test measures nothing, which is why it is not one. |
| `harnessed` | A driver a lane runs, with no retained run of its heavy tier. A short run may be retained here — it shows the harness produces records — but it does not promote the slice. |
| `evidenced` | A driver, and at least one retained run in the repository — including one from the packet's evidencing tier (`heavy` for capacity/rollout, `smoke` for endurance/stateful endurance, `serving` for recovery, `full` for fault). Endurance smoke is the ship gate (same leak/accounting assertions as the soak). The 12-hour soak remains scheduled observational for per-hour drift. |

## Qualification cohort

Packet manifest schema 2 names one immutable release cohort:
`v0.4.0-production-qualification`, candidate version `0.4.0`. Its exact
`source_commit` is truthfully `pending` until the candidate is frozen. Closure
requires replacing that sentinel with an exact Git object id and matching it
against every one of the six heavy records. Every candidate record must also be
clean, use `cargo_profile = "release"`, and identify v0.4.0 in both its source
and binary provenance. Rollout additionally fixes the published predecessor at
v0.3.40.

Freeze the cohort in this order:

1. Land every candidate code, manifest, workflow, and documentation change,
   set the workspace and shipped dependency versions to `0.4.0`, and commit.
   Keep the release-please PR unmerged.
2. Record that clean commit as `cohort.source_commit`; do not amend it.
3. Run capacity, fault, recovery, rollout, and the endurance **smoke** tiers
   from that exact Git object. Do not wait on a 12-hour soak. A rerun after any
   source change starts a new cohort rather than mixing records.
4. Promote and commit the six records, change each slice to `evidenced`, and set
   closure only when the packet test derives no error. These evidence-only
   commits may descend from the frozen source; their records must still name
   the frozen object that produced the candidate binaries.
5. Merge the release PR when the candidate is ready to ship. Qualification
   records may follow; they do not block the tag.

Historical PostgreSQL fault and recovery evidence can be reproduced only by a
manual `CI` workflow dispatch on the frozen source with
`run_legacy_postgres_qualification=true`. Pull-request CI still tests GitHub's
synthetic merge ref, which is the right software-change gate, but it neither
starts those legacy lanes nor produces target-topology evidence. The ADR 0062
cohort needs new blob-backed fault and recovery workflows before qualification
can restart.

The `axond-qualification` self-hosted runner label is also a provisioning
contract: it names a Linux x86_64 host with a running Docker Engine capable of
starting GitHub Actions service containers. Stateful endurance uses a pinned
PostgreSQL service, which GitHub creates before the first job step; a host with
the label but without Docker is misconfigured and cannot produce evidence.

## Retained evidence

A run's full artifacts (`target/<slice>/**/*.json`) are complete and
disposable. What the repository keeps is a *record*: the numbers or workload
observations, plus the
provenance that decides what may legitimately be compared with what — commit,
binary digest and cargo profile, manifest digest, fixture count, the machine,
and, per profile, the config the process actually booted. The manifest digest is
asserted against the committed manifest, so editing a profile's scale or
thresholds invalidates every record taken before the edit rather than leaving it
quietly describing a workload that no longer exists.

A record remains content-addressed by the binary, manifest, raw artifact, and
config digests. A historical branch commit can be a pre-squash provenance note;
an active v0.4.0 record is stricter and must name the packet cohort's exact
frozen source commit.

The packet currently has no active retained records. It separately indexes
these historical records so they remain reviewable without contributing to a
slice's status or to closure:

| Historical record | Tier | Runner | Version/profile | Binary `sha256` | Branch commit (pre-squash) |
| --- | --- | --- | --- | --- | --- |
| `qualification/faults/evidence/full-ci.toml` | full | github-actions | v0.3.39/debug | `c7c250314925` | `c18b7a5` |
| `qualification/recovery/evidence/serving-ci.toml` | serving | github-actions | v0.3.39/debug | `4c1789c306b9` | `c18b7a5` |

The earlier capacity and rollout files remain as historical measurements, not
active packet evidence: neither binds the raw artifacts required by the current
contract, and the rollout run used one binary on both sides. The indexed fault
and recovery records are likewise historical because v0.3.39 debug executables
cannot qualify the frozen v0.4.0 release cohort. The contract test requires a locally
recorded run to be disclosed here by path and by the digest of the binary that
produced it, so re-running a tier — which rebuilds the binary, and so changes
the digest — without rewriting this table is a test failure rather than a stale
paragraph.

Write a record from a run's artifacts:

```bash
just capacity   # or: AXOND_CAPACITY=1 cargo test --test capacity -- --test-threads=1
ops/qualification-evidence.py target/capacity/heavy \
  --runner local --note "8 vCPU cloud VM, debug build" \
  --out qualification/capacity/evidence/heavy-local.toml
```

A runner-recorded record comes from the `Capacity` workflow instead: dispatch it
and download its `qualification-record-capacity` artifact (the raw
`capacity-results` artifact remains beside it for detail). Locally, pass the
extracted results directory with `--runner github-actions`. Comparing two records is only meaningful when their
`[binary]`, `[inputs]`, and `[hardware]` blocks match; where they do, a moved
number is a regression rather than a different machine.

The other load-shaped slices use the same provenance envelope and a compact
`[[observation]]` per committed workload. The raw artifact remains the detailed
diagnosis; the observation binds its SHA-256 digest, elapsed time, verdict count,
and pass result to the manifest that produced it. The heavy rollout and
endurance workflows, including the first-class stateful-endurance slice, the
full service-backed fault lane, and the combined recovery job publish records
as `qualification-record-*` workflow artifacts:

```bash
ops/qualification-evidence.py target/faults \
  --slice fault --tier full --runner local \
  --note "local pinned Redis/Postgres matrix" \
  --out qualification/faults/evidence/full-local.toml

ops/qualification-evidence.py target/rollout/heavy \
  --slice rollout --tier heavy --runner local \
  --note "local heavy rollout" \
  --out qualification/rollout/evidence/heavy-local.toml

ops/qualification-evidence.py target/recovery \
  --slice recovery --tier serving --runner local \
  --binary target/release/axond \
  --note "local stateful-tests plus restore-drill" \
  --out target/qualification-records/recovery-serving.toml
```

Run `python3 ops/qualification-evidence.py --self-test` to exercise the
generic writer's refusal of missing workloads and failed verdicts. A generated
record is still not evidence until it passes the promotion boundary and its
path is added to the packet:

```bash
python3 ops/promote-qualification.py \
  target/qualification-records/rollout-heavy.toml \
  --artifacts target/rollout/heavy \
  --out qualification/rollout/evidence/heavy-ci.toml

python3 ops/promote-qualification.py \
  target/qualification-records/recovery-serving.toml \
  --artifacts target/recovery \
  --out qualification/recovery/evidence/serving-ci.toml
```

Promotion verifies the source tree, manifest, binary, and workload coverage as
before, and now also hashes every supplied raw JSON artifact. Both endurance
slices bind and re-hash request-identity and correlation ledgers, including
their file and byte counts. Sample sets are bound through `samples_sha256`,
`samples_files`, and `samples_bytes`. Stateless endurance binds exactly one
non-empty resource-sample JSONL file. Stateful endurance binds a non-empty set
with one file per replica incarnation and additionally binds its exact
per-request timing, durable, and outside-window identity ledgers.
Promotion independently reconstructs accounting and resource verdicts from
those retained bytes. A compact record
cannot be promoted from a TOML file alone: its claimed artifact digests must
match the complete raw-artifact directory from the same run.
Rollout observations also retain the previous and candidate executable versions
and digests plus the checksum-pinned release archive digest, so that provenance
survives the raw workflow artifact's retention window. Raw rollout schema 5
binds the evaluated migration matrix to the redacted control-plane environment
name, schema, and exact bootstrap digest. Compact rollout record schema 4
additionally preserves the shared durable revision, the `chat` alias,
successful serving probes from both v0.3.40 and v0.4.0, and the per-replica
exact caller-trace/OTLP witness, including the
separate reasoned ledger for typed-drain refusals that owe no usage row, the
exact ingress attempt behind every promotable drain exemption, and a settled
exporter snapshot. Reduced raw diagnostics may also record capability refusals,
but those are not accepted into a promotable compact record. Fault observations
bind raw artifact schema 1. Recovery stage rows bind raw artifact schema 2 and
the exact executable digest, which must equal the record's release binary
digest. The compact rollout row retains the OTLP caller-trace set's SHA-256 and
cardinality after the raw artifact's workflow-retention window.

Promotion refuses dirty provenance, stale manifest hashes, wrong heavy tiers,
partial workload sets, failed verdicts, and an endurance or stateful-endurance
record shorter than the committed duration of the packet evidencing tier
(CI `smoke`, not the 12-hour `soak`). A dispatched soak can still leave raw
diagnostic artifacts; it is not #156 ship-gate evidence. Recovery promotion additionally requires all 22 executable stages,
with the stateful-test and restore-drill lane attribution from the manifest.
Promotion does not edit the packet; the status and `retained` path remain a
reviewed change checked by the packet test.

The release workflow does not require packet closure to cut a tag.
`v0_4_0_release_candidate_requires_closed_production_qualification` remains in
the suite and runs only when `AXOND_REQUIRE_QUALIFICATION_CLOSURE=1`. Use it to
audit a cohort; do not hold production on it.

## Dependency retirements

The fault and rollout slices formerly named #158 in `blocked_on`. That edge is
retired rather than silently deleted: the stateful deployment, persistent
volume, and preflight work remains tracked on [#158](https://github.com/Litvue/axond/issues/158),
while the fault and rollout qualification contracts now own their executable
evidence. The packet header records the same retirement, and this paragraph is
checked by the qualification packet test.

## What each slice still owes

- **`endurance`** — a frozen-cohort CI smoke record binding exact request and
  correlation ledgers plus one sample JSONL. The 12-hour soak (per-hour drift)
  is scheduled observational and is not required to ship.
- **`stateful-endurance`** — a frozen-cohort CI smoke record with exact
  request, trace, per-request timing, and durable-identity shards plus the
  non-empty per-incarnation sample JSONL set. The 12-hour soak is scheduled
  observational and is not required to ship.
- **`capacity`** — a schema-2 heavy run that retains and binds every raw profile
  artifact, including decoded production queue-depth telemetry.
- **`recovery`** — re-run all executable real-Postgres stages from the frozen
  v0.4.0 source in release profile, retaining raw schema 2 and each stage's
  exact executable digest.
- **`fault`** — re-run the complete provider, transport, Redis, and Postgres
  matrix from the frozen v0.4.0 source in release profile, retaining raw schema
  1 on every compact observation.
- **`rollout`** — a release-profile raw-schema-5/compact-schema-4 run using published
  v0.3.40 and cohort v0.4.0 as distinct binaries, proving both serve one shared
  durable revision and `chat` alias through the migration and rollback ledger,
  with every exact caller trace exported through its replica-dedicated receiver
  and every non-usage trace explicitly justified.
- **Second reference tier** — rollout must supply the short fleet-under-load
  baseline; a long-duration soak remains scheduled observational work.

One question the packet is regularly asked for, and still cannot answer: a
**stateful fleet endurance baseline**. Durable workload-principal projection can
now compile a complete revision into a serving snapshot, and the persistent
StatefulSet/PVC overlay can restore its authenticated caches. The historical
single-process recovery run documents the #219 harness but does not qualify
v0.4.0. What remains outstanding is direct evidence: a stateful two-version rollout and the
stateful endurance smoke profile against multiple replicas, including
revisions, backend faults, and rolling restarts. Until those runs are
retained, no capacity profile claims stateful serving. The 12-hour soak is
not a publication gate.

## Related

- [Capacity qualification](./capacity.md) — the profiles, envelopes, candidate
  SLOs, and what a capacity run gates on.
- [Endurance qualification](./endurance.md) — the mixed-workload soak and the
  leak and accounting properties it gates on.
- [Recovery qualification](./recovery-qualification.md) — the stateful outage,
  cold-boot, convergence, rotation, and restore scenarios, the two lanes that
  run them, and what each retains.
- [Stateful endurance qualification](./stateful-endurance.md) — the same
  workload offered to a fleet whose catalogue, credentials, policy, provider,
  database, and processes change while it serves.
- [Upgrades and rollback](./upgrades.md) — the compatibility and rollback rules
  the rollout slice will qualify.
- [ADR 0033](../adr/0033-capacity-qualification-harness.md) — why a run is
  reproducible from a committed manifest, and why only stable properties gate.
- [ADR 0037](../adr/0037-recovery-qualification-harness.md) — why a scenario
  contract is committed before its driver.
- [ADR 0040](../adr/0040-endurance-qualification-harness.md) — why the endurance
  harness measures its own accumulators before it measures the gateway.
- [ADR 0057](../adr/0057-stateful-endurance-qualification.md) — why the stateful
  soak attributes every excused error and lost row to something the deployment
  itself emitted.
- [ADR 0045](../adr/0045-qualification-packet-and-evidence-records.md) — the
  packet and evidence-record schemas, and the ladder derived from them.
