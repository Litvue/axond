# The production qualification packet

What has actually been measured about running Axond on the request path, what
has only been harnessed, and what has not yet been retained — in one place, so
the difference between a merged harness and an answered question stays visible.

[ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md) retired the tier /
mode matrix. Recovery, rollout, and stateful-endurance harnesses that existed
only to prove that matrix are gone. Remaining evidence is SQLite +
`/ns/{ns}/v1`.

> **Architecture transition:**
> [ADR 0062](../adr/0062-blob-backed-flat-namespace-control-plane.md) is itself
> superseded by ADR 0063. Do not dispatch historical PostgreSQL stateful-v1
> overlay drills as production evidence for the namespaced gateway. Overlay
> drills remain behind `run_legacy_postgres_qualification=true`.
>
> The blob-backed stateful-v2 gates remain **pending**. A green `CI Success`
> proves the active software-change checks passed; it does not promote skipped
> overlay drills into request-path qualification.

## Retired harnesses

These trees were deleted rather than paused. They taught the removed tier
matrix as product evidence:

- `qualification/recovery` ([ADR 0037](../adr/0037-recovery-qualification-harness.md))
- `qualification/rollout` ([ADR 0041](../adr/0041-rollout-qualification-harness.md))
- `qualification/stateful-endurance` ([ADR 0057](../adr/0057-stateful-endurance-qualification.md))
- the ADR 0018 “no datastore” promise (the gate still boots; it now uses a temp
  SQLite file)

Kubernetes overlay drills (`stateful-deploy-drill`,
`stateful-persistent-drill`) are not those harnesses. They stay behind:

```bash
gh workflow run ci.yml --ref <branch> \
  -f run_legacy_postgres_qualification=true
```

Request-path qualification does not use that input.

## Remaining slices

Production qualification ([#156](https://github.com/Litvue/axond/issues/156))
now has three request-path slices:

| Slice | Issue | Status | What exists today |
| --- | --- | --- | --- |
| `capacity` | [#217](https://github.com/Litvue/axond/issues/217) | `harnessed` | Driver and committed profiles against SQLite + `/ns/{ns}/v1`. Historical records predate compact schema 2 and are not active evidence. |
| `endurance` | [#221](https://github.com/Litvue/axond/issues/221) | `harnessed` | Mixed-workload driver whose CI smoke tier is the ship gate. Promote a frozen-cohort smoke record to evidence it. The 12-hour soak is scheduled observational, not a publication requirement. |
| `fault` | [#218](https://github.com/Litvue/axond/issues/218) | `harnessed` | Provider and transport matrix on SQLite. Redis budget and rate-limit rows skip because those backends are withdrawn (ADR 0063), not because of a missing tier-matrix service. Historical v0.3.39 debug record remains indexed for audit. |

`qualification/packet.toml` is that table as data — question, inputs, lanes,
retained runs, and what each slice still owes; see
[ADR 0045](../adr/0045-qualification-packet-and-evidence-records.md) for why the
packet and the record are shaped this way. And
`crates/gateway/tests/qualification_packet.rs` checks it against the tree on
every run: a path that does not exist, a status a slice has not earned, a
scenario no slice covers, or a retained record that cannot be reproduced from
the manifest it names is a test failure.

## What the packet may not be read as

- **Not a claim that Axond is production-qualified.** The remaining slices are
  currently harnessed. #156 stays open until every remaining slice has exactly
  one heavy release-profile record from the same clean v0.4.0 source cohort;
  `closure.satisfied` is derived from those records and cannot be set by hand.
- **Not a no-datastore boot.** CI boots a temp SQLite file. That is the
  single-replica gate, not “no datastore”.
- **Not a long-soak baseline.** The 12-hour soak is not a v0.4.0 publication
  gate.

## The status ladder

A slice climbs four rungs, and each is derived from what the slice has rather
than from what it says:

| Status | Reached when |
| --- | --- |
| `unbuilt` | The question is written down. Nothing runs. |
| `declared` | A committed manifest and contract page, kept honest by a `contract_test`, with no driver behind them. A contract test measures nothing, which is why it is not one. |
| `harnessed` | A driver a lane runs, with no retained run of its heavy tier. A short run may be retained here — it shows the harness produces records — but it does not promote the slice. |
| `evidenced` | A driver, and at least one retained run in the repository — including one from the packet's evidencing tier (`heavy` for capacity, `smoke` for endurance, `full` for fault). Endurance smoke is the ship gate (same leak/accounting assertions as the soak). The 12-hour soak remains scheduled observational for per-hour drift. |

## Qualification cohort

Packet manifest schema 2 names one immutable release cohort:
`v0.4.0-production-qualification`, candidate version `0.4.0`. Its exact
`source_commit` is truthfully `pending` until the candidate is frozen. Closure
requires replacing that sentinel with an exact Git object id and matching it
against every remaining heavy record. Every candidate record must also be
clean, use `cargo_profile = "release"`, and identify v0.4.0 in both its source
and binary provenance.

## Retained evidence

A run's full artifacts (`target/<slice>/**/*.json`) are complete and
disposable. What the repository keeps is a *record*: the numbers or workload
observations, plus the provenance that decides what may legitimately be
compared with what.

The packet currently has no active retained records. It separately indexes
this historical record so it remains reviewable without contributing to a
slice's status or to closure:

| Historical record | Tier | Runner | Version/profile | Binary `sha256` | Branch commit (pre-squash) |
| --- | --- | --- | --- | --- | --- |
| `qualification/faults/evidence/full-ci.toml` | full | github-actions | v0.3.39/debug | `c7c250314925` | `c18b7a5` |

Write a record from a run's artifacts:

```bash
just capacity   # or: AXOND_CAPACITY=1 cargo test --test capacity -- --test-threads=1
ops/qualification-evidence.py target/capacity/heavy \
  --runner local --note "8 vCPU cloud VM, debug build" \
  --out qualification/capacity/evidence/heavy-local.toml
```

A runner-recorded record comes from the `Capacity` workflow instead. Comparing
two records is only meaningful when their `[binary]`, `[inputs]`, and
`[hardware]` blocks match.

```bash
ops/qualification-evidence.py target/faults \
  --slice fault --tier full --runner local \
  --note "local SQLite provider/transport matrix" \
  --out qualification/faults/evidence/full-local.toml
```

Run `python3 ops/qualification-evidence.py --self-test` to exercise the
generic writer's refusal of missing workloads and failed verdicts. A generated
record is still not evidence until it passes the promotion boundary and its
path is added to the packet.

The release workflow does not require packet closure to cut a tag.
`v0_4_0_release_candidate_requires_closed_production_qualification` remains in
the suite and runs only when `AXOND_REQUIRE_QUALIFICATION_CLOSURE=1`. Use it to
audit a cohort; do not hold production on it.

## What each slice still owes

- **`endurance`** — a frozen-cohort CI smoke record binding exact request and
  correlation ledgers plus one sample JSONL. The 12-hour soak (per-hour drift)
  is scheduled observational and is not required to ship.
- **`capacity`** — a schema-2 heavy run that retains and binds every raw profile
  artifact, including decoded production queue-depth telemetry.
- **`fault`** — re-run the provider and transport matrix from the frozen v0.4.0
  source in release profile on SQLite + `/ns/{ns}/v1`, retaining raw schema 1
  on every compact observation. Redis rows stay skipped (ADR 0063).

## Related

- [Capacity qualification](./capacity.md) — the profiles, envelopes, candidate
  SLOs, and what a capacity run gates on.
- [Endurance qualification](./endurance.md) — the mixed-workload soak and the
  leak and accounting properties it gates on.
- [Fault qualification](./fault-qualification.md) — the provider and transport
  matrix, and the evidence each row retains.
- [ADR 0014](../adr/0014-compatibility-and-soak-harness.md) — black-box soak.
- [ADR 0033](../adr/0033-capacity-qualification-harness.md) — capacity harness.
- [ADR 0040](../adr/0040-endurance-qualification-harness.md) — endurance harness.
- [ADR 0048](../adr/0048-fault-qualification-harness.md) — fault harness.
- [ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md) — stateful-only
  namespaced gateway; this page implements slice 7.
