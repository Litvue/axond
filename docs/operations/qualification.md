# The production qualification packet

What has actually been measured about running Axond in production, what has only
been declared or harnessed, and what has not yet been retained — in one place, so the difference
between a merged harness and an answered question stays visible.

Production qualification ([#156](https://github.com/Litvue/axond/issues/156))
decomposes into six slices. They landed, and will land, at different depths:

| Slice | Issue | Status | What exists today |
| --- | --- | --- | --- |
| `capacity` | [#217](https://github.com/Litvue/axond/issues/217) | `harnessed` | Driver and eight committed profiles — including multi-tenant isolation, admission shedding, a bounded stalling backend, and decoded production queue-depth telemetry. Its historical records predate raw-artifact-bound compact schema 2 and are not active evidence. |
| `endurance` | [#221](https://github.com/Litvue/axond/issues/221) | `harnessed` | Stateless mixed-workload driver and committed mix whose smoke tier runs in CI. Its 12-hour tier has not retained exact request/correlation ledgers and its sample JSONL under the current contract. |
| `stateful-endurance` | [#221](https://github.com/Litvue/axond/issues/221) | `harnessed` | Multi-replica driver for revisions, backend faults, tenant isolation, durable accounting, and rolling restarts. Its smoke lane is harness validation; no 12-hour record binds all five exact ledgers and the per-incarnation sample JSONL set. |
| `recovery` | [#219](https://github.com/Litvue/axond/issues/219) | `harnessed` | Driver and twenty-two executable real-Postgres stages exist. The historical v0.3.39 debug record remains indexed for audit, but a release-profile v0.4.0 cohort rerun with raw schema 2, per-stage executable digests, and process-bound executed-binary identity is pending. |
| `fault` | [#218](https://github.com/Litvue/axond/issues/218) | `harnessed` | The provider, transport, Redis, and Postgres matrix is complete. Its historical v0.3.39 debug record remains indexed for audit, but raw-schema-1 release evidence from the v0.4.0 cohort is pending. |
| `rollout` | [#220](https://github.com/Litvue/axond/issues/220) | `harnessed` | Driver and committed reduced/heavy scenarios now require published v0.3.40 and candidate v0.4.0 serving one shared durable revision and alias through the real-Postgres migration/rollback matrix. No raw/compact schema-3 record is retained. |

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
  closure evidence. The stateful endurance harness has run its
  ninety-second smoke tier, but the 12–24 hour tiers have not been dispatched.
- **Not a long-soak baseline.** The rollout scenario offers fleet load for under
  a minute, and no current rollout record is retained; it says nothing about
  resource drift over hours.

## The status ladder

A slice climbs four rungs, and each is derived from what the slice has rather
than from what it says:

| Status | Reached when |
| --- | --- |
| `unbuilt` | The question is written down. Nothing runs. |
| `declared` | A committed manifest and contract page, kept honest by a `contract_test`, with no driver behind them. A contract test measures nothing, which is why it is not one. |
| `harnessed` | A driver a lane runs, with no retained run of its heavy tier. A short run may be retained here — it shows the harness produces records — but it does not promote the slice. |
| `evidenced` | A driver, and at least one retained run in the repository — including one from the tier the slice's own manifest calls heavy (`heavy` for capacity/rollout, `soak` for endurance/stateful endurance), because a short run is a correctness check, not a measurement of what a replica does. |

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
3. Run capacity, fault, recovery, rollout, stateless endurance, and stateful
   endurance from that exact Git object. A rerun after any source change starts
   a new cohort rather than mixing records.
4. Promote and commit the six records, change each slice to `evidenced`, and set
   closure only when the packet test derives no error. These evidence-only
   commits may descend from the frozen source; their records must still name
   the frozen object that produced the candidate binaries.
5. Merge the release PR only after the pre-tag publication test is green.

Fault and recovery evidence comes from a manual `CI` workflow dispatch on the
frozen release branch. Pull-request CI still tests GitHub's synthetic merge ref,
which is the right merge gate but not the candidate head named by the cohort;
its generated records are diagnostic and cannot be promoted into that cohort.

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
survives the raw workflow artifact's retention window. Raw rollout schema 3 and
compact rollout record schema 3 additionally preserve the shared durable
revision, the `chat` alias, and successful serving probes from both v0.3.40 and
v0.4.0. Fault observations bind raw artifact schema 1. Recovery stage rows bind
raw artifact schema 2 and the exact executable digest, which must equal the
record's release binary digest.

Promotion refuses dirty provenance, stale manifest hashes, wrong heavy tiers,
partial workload sets, failed verdicts, and an endurance or stateful-endurance
`soak` run shorter than the committed 12-hour duration. A shortened dispatched
soak can still leave raw diagnostic artifacts, but the compact writer and
promoter both refuse it as #156
evidence. Recovery promotion additionally requires all 22 executable stages,
with the stateful-test and restore-drill lane attribution from the manifest.
Promotion does not edit the packet; the status and `retained` path remain a
reviewed change checked by the packet test.

The release workflow runs the packet's dedicated publication test before
release-please. While the workspace version is below `0.4.0`, the release PR may
remain open and absorb candidate work. Once the workspace identifies itself as
`0.4.0`, the workflow refuses to create the tag unless `closure.satisfied` is
true and the six independently validated records all belong to the frozen
cohort. This makes qualification a pre-tag gate rather than post-release prose.

## Dependency retirements

The fault and rollout slices formerly named #158 in `blocked_on`. That edge is
retired rather than silently deleted: the stateful deployment, persistent
volume, and preflight work remains tracked on [#158](https://github.com/Litvue/axond/issues/158),
while the fault and rollout qualification contracts now own their executable
evidence. The packet header records the same retirement, and this paragraph is
checked by the qualification packet test.

## What each slice still owes

- **`endurance`** — a dispatched 12–24 hour run binding exact request and
  correlation ledgers plus one sample JSONL. The drift thresholds that only
  hours can exercise (`max_rss_drift_kib_per_hour` and its neighbours) have no
  run behind them, so the soak tiers are declared bounds rather than measured
  ones.
- **`stateful-endurance`** — a complete 12-hour multi-replica run with exact
  request, trace, per-request timing, and durable-identity shards plus the
  non-empty per-incarnation sample JSONL set independently promoted from the
  raw artifact set.
- **`capacity`** — a schema-2 heavy run that retains and binds every raw profile
  artifact, including decoded production queue-depth telemetry.
- **`recovery`** — re-run all executable real-Postgres stages from the frozen
  v0.4.0 source in release profile, retaining raw schema 2 and each stage's
  exact executable digest.
- **`fault`** — re-run the complete provider, transport, Redis, and Postgres
  matrix from the frozen v0.4.0 source in release profile, retaining raw schema
  1 on every compact observation.
- **`rollout`** — a release-profile raw/compact schema-3 run using published
  v0.3.40 and cohort v0.4.0 as distinct binaries, proving both serve one shared
  durable revision and `chat` alias through the migration and rollback ledger.
- **Second reference tier** — rollout must supply the short fleet-under-load
  baseline; stateful endurance must supply the long-duration baseline.

One question the packet is regularly asked for, and still cannot answer: a
**stateful fleet endurance baseline**. Durable workload-principal projection can
now compile a complete revision into a serving snapshot, and the persistent
StatefulSet/PVC overlay can restore its authenticated caches. The historical
single-process recovery run documents the #219 harness but does not qualify
v0.4.0. What remains outstanding is direct evidence: a stateful two-version rollout and the
stateful endurance profile against multiple replicas for the committed 12
hours, including revisions, backend faults, and rolling restarts. Until those
runs are retained, no capacity profile claims stateful serving.

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
