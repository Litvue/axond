# The production qualification packet

What has actually been measured about running Axond in production, what has only
been declared or harnessed, and what has not yet been retained — in one place, so the difference
between a merged harness and an answered question stays visible.

Production qualification ([#156](https://github.com/Litvue/axond/issues/156))
decomposes into five slices. They landed, and will land, at different depths:

| Slice | Issue | Status | What exists today |
| --- | --- | --- | --- |
| `capacity` | [#217](https://github.com/Litvue/axond/issues/217) | `evidenced` | Driver, eight committed profiles — including multi-tenant isolation, admission shedding, and a bounded stalling backend — reduced tier on every change, heavy tier on demand, and two retained runs. |
| `endurance` | [#221](https://github.com/Litvue/axond/issues/221) | `harnessed` | Two drivers and committed mixes — one stateless, one against a fleet with a durable usage sink — whose smoke tiers run in CI. Neither 12–24 hour tier has been dispatched. |
| `recovery` | [#219](https://github.com/Litvue/axond/issues/219) | `evidenced` | Driver, committed scenarios, and twenty-two executable stages running against real PostgreSQL in two lanes — outage, cold boots, cached process serving, cache refusal, secret rotation, convergence, durable inventory, logical restore, point-in-time recovery, and the durable usage boundary — with a retained GitHub Actions record. A multi-replica fleet baseline remains open. |
| `fault` | [#218](https://github.com/Litvue/axond/issues/218) | `evidenced` | Committed provider, transport, Redis, and Postgres fault matrix plus a driver; the full pinned-service matrix has a clean retained GitHub Actions record. A second reference tier and integrated fleet evidence remain open. |
| `rollout` | [#220](https://github.com/Litvue/axond/issues/220) | `harnessed` | Driver, committed scenarios, reduced and heavy tiers; the local heavy run passed, but no retained record is committed, and mixed-version serving across a migration still needs a stateful multi-replica qualification. |

`qualification/packet.toml` is that table as data — question, inputs, lanes,
retained runs, and what each slice still owes; see
[ADR 0045](../adr/0045-qualification-packet-and-evidence-records.md) for why the
packet and the record are shaped this way. And
`crates/gateway/tests/qualification_packet.rs` checks it against the tree on
every run: a path that does not exist, a status a slice has not earned, a
scenario no slice covers, or a retained record that cannot be reproduced from
the manifest it names is a test failure.

## What the packet may not be read as

- **Not a claim that Axond is production-qualified.** Three of five slices are
  evidenced and two have a driver with no retained heavy run behind them; no
  slice has retained a run of a fleet. #156 stays open until every slice
  is `evidenced`; `closure.satisfied` in the packet is derived from the slices,
  so it cannot be set by hand.
- **Not a stateful fleet baseline.** The two capacity records are Tier 0 local
  runs; the retained fault and recovery records are process-level GitHub
  Actions runs against pinned Redis/Postgres lanes. None is a multi-replica
  load or soak measurement. The stateful endurance harness has run its
  ninety-second smoke tier, but the 12–24 hour tiers have not been dispatched.
- **Not a fleet baseline.** The retained CI records qualify process behavior,
  not offered load across replicas.

## The status ladder

A slice climbs four rungs, and each is derived from what the slice has rather
than from what it says:

| Status | Reached when |
| --- | --- |
| `unbuilt` | The question is written down. Nothing runs. |
| `declared` | A committed manifest and contract page, kept honest by a `contract_test`, with no driver behind them. A contract test measures nothing, which is why it is not one. |
| `harnessed` | A driver a lane runs, with no retained run of its heavy tier. A short run may be retained here — it shows the harness produces records — but it does not promote the slice. |
| `evidenced` | A driver, and at least one retained run in the repository — including one from the tier the slice's own manifest calls heavy (`heavy` for capacity, `soak` for endurance), because a short run is a correctness check, not a measurement of what a replica does. |

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

A record's identity is its digests, not its commit. The binary that produced
the numbers, the manifest it ran, and the config each profile booted are
content-addressed; they say exactly what was measured and they outlive any
rewriting of history. The branch commit is recorded too, but this repository
squash-merges, so read it as a note about the run rather than something to
check out.

| Record | Tier | Runner | Binary `sha256` | Branch commit (pre-squash) |
| --- | --- | --- | --- | --- |
| `qualification/capacity/evidence/reduced-local.toml` | reduced | local | `8e2cbb566e82` | `8ba8b96` |
| `qualification/capacity/evidence/heavy-local.toml` | heavy | local | `8e2cbb566e82` | `8ba8b96` |
| `qualification/faults/evidence/full-ci.toml` | full | github-actions | `c7c250314925` | `c18b7a5` |
| `qualification/recovery/evidence/serving-ci.toml` | serving | github-actions | `4c1789c306b9` | `c18b7a5` |

The two capacity records were produced on an 8 vCPU cloud VM from a **debug build**, which is what
`cargo test` builds. They are the first envelope, not a fleet baseline: a release
build on production-representative hardware will move every number in them, and
`runner = "local"` is in the record so that caveat travels with the data instead
of with this paragraph. The CI records are retained process-level fault and
recovery evidence; their GitHub Actions provenance and Redis/Postgres lanes do
not make them fleet-load measurements. The contract test requires a locally
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
endurance workflows, including the supplemental stateful-endurance lane, the
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
  --binary target/debug/axond \
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
before, and now also hashes every supplied raw JSON artifact. A compact record
cannot be promoted from a TOML file alone: its claimed artifact digests must
match the complete raw-artifact directory from the same run.

Promotion refuses dirty provenance, stale manifest hashes, wrong heavy tiers,
partial workload sets, failed verdicts, and an endurance `soak` run shorter than
the committed 12-hour duration. A shortened dispatched soak can still be
generated and uploaded for diagnosis, but it cannot be promoted as #156
evidence. Recovery promotion additionally requires all 22 executable stages,
with the stateful-test and restore-drill lane attribution from the manifest.
Promotion does not edit the packet; the status and `retained` path remain a
reviewed change checked by the packet test.

## Dependency retirements

The fault and rollout slices formerly named #158 in `blocked_on`. That edge is
retired rather than silently deleted: the stateful deployment, persistent
volume, and preflight work remains tracked on [#158](https://github.com/Litvue/axond/issues/158),
while the fault and rollout qualification contracts now own their executable
evidence. The packet header records the same retirement, and this paragraph is
checked by the qualification packet test.

## What each slice still owes

- **`endurance`** — a dispatched 12–24 hour run. The drift thresholds that only
  hours can exercise (`max_rss_drift_kib_per_hour` and its neighbours) have no
  run behind them, so the soak tiers are declared bounds rather than measured
  ones.
- **`recovery`** — a multi-replica fleet baseline: serving through the outage,
  from a restored cache, and across a recovery, plus rotation and restore under
  offered load. The retained process/recovery record covers all twenty-two
  executable stages today; the packet mirrors the manifest's dependency map and
  is tested against it so future dependencies cannot disappear silently.
- **`fault`** — a second reference tier and integrated fleet fault evidence. The
  full provider, transport, Redis, and Postgres matrix now has a clean retained
  GitHub Actions record.
- **`rollout`** — a manifest, a driver, and a stateful fleet: two or more
  replicas behind an ingress, with the artifact-digest, migration, and timeline
  metadata a rollout has to retain.
- **All of them** — a second reference tier. Every number retained so far is
  single-replica.

One question the packet is regularly asked for, and still cannot answer: a
**stateful fleet qualification baseline**. A stateful replica now compiles
durable tenant/project principals and serves after a complete snapshot is
published; without one it keeps readiness and inference fail-closed. The
single-process evidence belongs to #219, while a profile against multiple
replicas, offered load, and recovery remains outstanding. That is why #219
remains open: its twenty-two executable stages run against real PostgreSQL and
are retained, but no complete multi-replica fleet record is retained. It is also
why no capacity profile claims stateful serving.

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
