# The production qualification packet

What has actually been measured about running Axond in production, what has only
been declared, and what has not been built — in one place, so the difference
between a merged harness and an answered question stays visible.

Production qualification ([#156](https://github.com/Litvue/axond/issues/156))
decomposes into five slices. They landed, and will land, at different depths:

| Slice | Issue | Status | What exists today |
| --- | --- | --- | --- |
| `capacity` | [#217](https://github.com/Litvue/axond/issues/217) | `evidenced` | Driver, eight committed profiles — including multi-tenant isolation, admission shedding, and a bounded stalling backend — reduced tier on every change, heavy tier on demand, and two retained runs. |
| `endurance` | [#221](https://github.com/Litvue/axond/issues/221) | `harnessed` | Driver and committed mix; the smoke tier runs in CI. The 12–24 hour tier has never been dispatched. |
| `recovery` | [#219](https://github.com/Litvue/axond/issues/219) | `declared` | A committed scenario contract and a test that keeps it honest. No driver: stateful serving is not assembled yet. |
| `fault` | [#218](https://github.com/Litvue/axond/issues/218) | `unbuilt` | Nothing. The fake upstream already injects the provider faults a matrix would drive. |
| `rollout` | [#220](https://github.com/Litvue/axond/issues/220) | `harnessed` | Driver, committed scenarios, and a reduced tier in CI. The heavy tier has never been dispatched, and mixed-version serving waits on a stateful replica. |

`qualification/packet.toml` is that table as data — question, inputs, lanes,
retained runs, and what each slice still owes; see
[ADR 0045](../adr/0045-qualification-packet-and-evidence-records.md) for why the
packet and the record are shaped this way. And
`crates/gateway/tests/qualification_packet.rs` checks it against the tree on
every run: a path that does not exist, a status a slice has not earned, a
scenario no slice covers, or a retained record that cannot be reproduced from
the manifest it names is a test failure.

## What the packet may not be read as

- **Not a claim that Axond is production-qualified.** One of five slices has no
  driver, one has a contract and no driver, two have no heavy run behind them,
  and no slice has retained a run of a fleet. #156 stays open until every slice
  is `evidenced`; `closure.satisfied` in the packet is derived from the slices,
  so it cannot be set by hand.
- **Not evidence about stateful serving.** Everything measured so far is Tier 0:
  one process, no Redis, no Postgres, no control plane. See
  [recovery qualification](./recovery-qualification.md) for what stateful
  recovery will have to show.
- **Not a fleet baseline.** Both retained runs are local (see below).

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

A run's full artifacts (`target/capacity/**/*.json`) are complete and
disposable. What the repository keeps is a *record*: the numbers, plus the
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
| `qualification/capacity/evidence/reduced-local.toml` | reduced | local | `2895aa8bfc80` | `b5a19a5` |
| `qualification/capacity/evidence/heavy-local.toml` | heavy | local | `2895aa8bfc80` | `b5a19a5` |

Both were produced on an 8 vCPU cloud VM from a **debug build**, which is what
`cargo test` builds. They are the first envelope, not a fleet baseline: a release
build on production-representative hardware will move every number in them, and
`runner = "local"` is in the record so that caveat travels with the data instead
of with this paragraph. The contract test requires a locally recorded run to be
disclosed here by path and by the digest of the binary that produced it, so
re-running a tier — which rebuilds the binary, and so changes the digest —
without rewriting this table is a test failure rather than a stale paragraph.

Write a record from a run's artifacts:

```bash
just capacity   # or: AXOND_CAPACITY=1 cargo test --test capacity -- --test-threads=1
ops/qualification-evidence.py target/capacity/heavy \
  --runner local --note "8 vCPU cloud VM, debug build" \
  --out qualification/capacity/evidence/heavy-local.toml
```

A runner-recorded record comes from the `Capacity` workflow instead: dispatch it,
download the `capacity-results` artifact, and pass the extracted directory with
`--runner github-actions`. Comparing two records is only meaningful when their
`[binary]`, `[inputs]`, and `[hardware]` blocks match; where they do, a moved
number is a regression rather than a different machine.

## What each slice still owes

- **`endurance`** — a dispatched 12–24 hour run. The drift thresholds that only
  hours can exercise (`max_rss_drift_kib_per_hour` and its neighbours) have no
  run behind them, so the soak tier is a declared bound rather than a measured
  one.
- **`recovery`** — a driver, which waits on the resource slices that give a
  stateful replica something to serve. The packet mirrors the manifest's own
  dependency map and is tested against it, so landing a slice moves both.
- **`fault`** — a manifest and a driver. The provider rows are unblocked today;
  the backend rows need a stateful replica to fail.
- **`rollout`** — a manifest, a driver, and a fleet: two or more replicas behind
  an ingress, with the artifact-digest, migration, and timeline metadata a
  rollout has to retain.
- **All of them** — a second reference tier. Every number retained so far is
  single-replica.

One question the packet is regularly asked for, and cannot answer: **stateful
convergence**. `serve` still refuses a stateful config, so there is no stateful
replica to converge; a profile written against it today would measure the
refusal and retain it as evidence of something else. That is why the `recovery`
slice stays `declared` with a contract rather than a driver, and why no
capacity profile claims it.

## Related

- [Capacity qualification](./capacity.md) — the profiles, envelopes, candidate
  SLOs, and what a capacity run gates on.
- [Endurance qualification](./endurance.md) — the mixed-workload soak and the
  leak and accounting properties it gates on.
- [Recovery qualification](./recovery-qualification.md) — the declared stateful
  outage, cold-boot, convergence, rotation, and restore scenarios.
- [Upgrades and rollback](./upgrades.md) — the compatibility and rollback rules
  the rollout slice will qualify.
- [ADR 0033](../adr/0033-capacity-qualification-harness.md) — why a run is
  reproducible from a committed manifest, and why only stable properties gate.
- [ADR 0037](../adr/0037-recovery-qualification-harness.md) — why a scenario
  contract is committed before its driver.
- [ADR 0040](../adr/0040-endurance-qualification-harness.md) — why the endurance
  harness measures its own accumulators before it measures the gateway.
- [ADR 0045](../adr/0045-qualification-packet-and-evidence-records.md) — the
  packet and evidence-record schemas, and the ladder derived from them.
