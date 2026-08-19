# 45. The qualification packet and the retained evidence record

Date: 2026-08-13

## Status

Accepted

Reports on the qualification harnesses of
[ADR 0033](./0033-capacity-qualification-harness.md) (capacity and endurance)
and [ADR 0037](./0037-recovery-qualification-harness.md) (recovery), and commits
the two schemas that carry that report: the packet and the evidence record.

## Context

axond #156 asks for reproducible evidence in place of qualitative production
claims, and now decomposes into six qualification slices. Those slices land one at a
time, and each merge leaves the epic somewhere different: a harness with runs
behind it, a harness with none, a contract with no driver, an issue with
neither. Nothing in the tree distinguished those states, and the failure that
follows is not hypothetical — an audit of #156 read the capacity harness merging
as capacity being answered, when what had merged was the *measuring apparatus*
and no measurement.

Two things were missing, and they are different things. There was no statement
of how far each slice got that could be wrong in a way anything would notice.
And there was no artifact of a run: `target/capacity/**/*.json` is complete and
disposable, so the envelope table in the operations docs was numbers from a run
nobody could identify, on a host nobody recorded — by the time it was checked it
described a different CPU than the one the numbers came from.

## Decision

Commit two schema families, both read by `deny_unknown_fields`. Packet manifest
schema 2 adds a frozen release-candidate cohort. Compact records evolve per
slice: generic records remain schema 1, capacity is schema 2, and rollout is
schema 3 because it retains both executable identities and durable shared-state
serving proof.

**The packet** (`qualification/packet.toml`) states each slice's depth on a
four-rung ladder, and `crates/gateway/tests/qualification_packet.rs` *derives*
every rung from the slice's own fields rather than trusting the word:

| Rung | Requires |
| --- | --- |
| `unbuilt` | no manifest and no driver |
| `declared` | a manifest, a contract page, a `contract_test`, and no driver |
| `harnessed` | a manifest, a driver, a lane that runs it, and no retained run of its heavy tier |
| `evidenced` | a manifest, a driver, and a retained run of the slice's own `heavy_tier` |

Closure is derived from both the ladder and the release cohort, so #156 cannot
be closed by editing a flag. It requires exactly one heavy record for every
slice, all six built in release profile from a clean tree at the same exact
v0.4.0 source commit:

```rust
let errors = qualification_closure_errors(&packet, load_record);
assert_eq!(packet.closure.satisfied, errors.is_empty());
```

**The evidence record** (`qualification/<slice>/evidence/*.toml`, written by
`ops/qualification-evidence.py`) is a run's numbers or workload observations
plus the provenance that
decides what may legitimately be compared with what: commit and clean-tree flag,
binary digest, cargo profile and compiler, the manifest digest, the machine, and
per profile the config the process booted. Generic observations additionally
bind each workload to the digest of its raw artifact. Every retained record, whichever
slice retains it, is checked against the manifest it names — same workload set,
matching digest, all gates passed, nothing lost between offered and accounted
for. Endurance observations also retain the offered duration, the committed
duration, and the duration source; the promotion boundary refuses a `soak` run
that is shorter than the committed long tier. Both endurance slices bind exact
request-identity and correlation ledger digests, file counts, and byte counts,
plus sample claims through `samples_sha256`, `samples_files`, and
`samples_bytes`. Stateless endurance requires exactly one non-empty JSONL file;
stateful endurance requires a non-empty set with one file per replica
incarnation and adds its durable and outside-window identity ledgers.
Fault observations require raw artifact schema 1. Recovery records retain one
row per executable manifest stage, raw artifact schema 2, the digest of its raw
stage artifact, and the exact executable digest; active evidence requires that
stage digest to equal the record's release binary. Historical rows may omit the
new optional fields because they are indexed as history rather than retained as
closure evidence. Rollout raw and compact schema 3 preserve published v0.3.40,
candidate v0.4.0, one shared durable revision and `chat` alias, and successful
serving probes from both fleets.

Two consequences are deliberate. Editing a profile's scale or thresholds changes
the manifest digest and *invalidates every record taken before the edit*, which
fails the suite until the tier is re-run: a stale record is worse than none,
because it reads like coverage. And a record whose provenance the harness could
not determine is refused at write time rather than written with nulls.

`runner` is part of the record because a local debug-build run is evidence about
that machine. The packet may hold one — it is how a first envelope gets written
— but the contract test requires the operator page to name it, so nobody reads
it as a fleet baseline. The disclosure is checked by path *and* by the digest
below, so a re-run that rewrites a record without rewriting the page fails.

The heavy tier is named per slice (`heavy_tier`) rather than shared, because the
slices do not agree on the word: capacity's long tier is `heavy`, endurance's is
`soak`. A slice may retain a short run while still `harnessed` — that is how a
harness shows it produces records — but only a run of its own heavy tier moves
it to `evidenced`.

A record remains content-addressed by its binary, manifest, raw artifact, and
config digests. For ordinary historical records, `source.git_commit` can still
be a pre-squash provenance note. Release closure is deliberately stricter: the
packet's cohort starts with `source_commit = "pending"`, then freezes one exact
Git object id before any promotable run. Every one of the six heavy records must
name that exact source, v0.4.0 in source and binary provenance, a clean tree, and
the release Cargo profile. This separates useful historical measurements from
evidence that can qualify a particular release candidate.

### Stateful release boundary

The packet itself changes no deployment tier. Its closure contract does require
the heavy rollout lane to exercise real PostgreSQL state and prove that
published v0.3.40 and candidate v0.4.0 both serve the same durable revision and
alias. Recovery, fault, and both endurance slices must come from that same
frozen source cohort; a process-level or debug historical record cannot stand
in for the release candidate.

## Consequences

The report on #156 is now a file that can be wrong in a way CI notices, rather
than a claim in a comment. The operator page distinguishes active retained
records from indexed history, and drift between it and the packet is visible.

The cost is real: every manifest edit forces a re-run of the tiers that have
retained records, and rebuilding the binary changes the digest the operator page
discloses, so a re-run is two edits rather than one. That
friction is the point — it is what stops the numbers from quietly outliving
their inputs — but it makes the heavy tier something a change to the manifest
must budget for.

This ADR does not claim any slice is qualified. At the time of this amendment,
all six await heavy release-profile records from the frozen v0.4.0 cohort, and
the packet says so.
