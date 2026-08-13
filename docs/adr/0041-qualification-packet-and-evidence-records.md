# 41. The qualification packet and the retained evidence record

Date: 2026-08-13

## Status

Accepted

Reports on the qualification harnesses of
[ADR 0033](./0033-capacity-qualification-harness.md) (capacity and endurance)
and [ADR 0037](./0037-recovery-qualification-harness.md) (recovery), and commits
the two schemas that carry that report: the packet and the evidence record.

## Context

axond #156 asks for reproducible evidence in place of qualitative production
claims, and decomposes into five child issues. Those children merge one at a
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

Commit two schemas, both `schema_version = 1`, both read by
`deny_unknown_fields`.

**The packet** (`qualification/packet.toml`) states each slice's depth on a
four-rung ladder, and `crates/gateway/tests/qualification_packet.rs` *derives*
every rung from the slice's own fields rather than trusting the word:

| Rung | Requires |
| --- | --- |
| `unbuilt` | no manifest and no driver |
| `declared` | a manifest, a contract page, a `contract_test`, and no driver |
| `harnessed` | a manifest, a driver, a lane that runs it, and no retained run of its heavy tier |
| `evidenced` | a manifest, a driver, and a retained run of the slice's own `heavy_tier` |

Closure is derived the same way, so #156 cannot be closed by editing a flag:

```rust
let outstanding = slices.filter(|s| s.status != Evidenced).map(id);
assert_eq!(packet.closure.satisfied, outstanding.is_empty());
```

**The evidence record** (`qualification/<slice>/evidence/*.toml`, written by
`ops/qualification-evidence.py`) is a run's numbers plus the provenance that
decides what may legitimately be compared with what: commit and clean-tree flag,
binary digest, cargo profile and compiler, the manifest digest, the machine, and
per profile the config the process booted. Every retained record, whichever
slice retains it, is checked against the manifest it names — same workload set,
matching digest, all gates passed, nothing lost between offered and accounted
for.

Two consequences are deliberate. Editing a profile's scale or thresholds changes
the manifest digest and *invalidates every record taken before the edit*, which
fails the suite until the tier is re-run: a stale record is worse than none,
because it reads like coverage. And a record whose provenance the harness could
not determine is refused at write time rather than written with nulls.

`runner` is part of the record because a local debug-build run is evidence about
that machine. The packet may hold one — it is how a first envelope gets written
— but the contract test requires the operator page to name it, so nobody reads
it as a fleet baseline. The disclosure is checked by path *and* by commit, so a
re-run that rewrites a record without rewriting the page fails.

The heavy tier is named per slice (`heavy_tier`) rather than shared, because the
slices do not agree on the word: capacity's long tier is `heavy`, endurance's is
`soak`. A slice may retain a short run while still `harnessed` — that is how a
harness shows it produces records — but only a run of its own heavy tier moves
it to `evidenced`.

### State tier

Tier 0 (config-only). The packet, the records, and the writer are committed
files and a test binary: no Redis, no Postgres, no control plane, and no change
to the tier of any deployment. The evidence they carry is Tier 0 evidence too,
which is exactly why the packet has to say that the stateful slices are not
qualified.

## Consequences

The report on #156 is now a file that can be wrong in a way CI notices, rather
than a claim in a comment. The docs' envelope table is a retained record read in
operator units, and drift between them is visible.

A record's `git_commit` is the branch commit the run was taken at, and this
repository squash-merges, so once a packet PR lands that commit is not on
`main`. The record is still reproducible — the manifest digest, the binary
digest, and the config digests are the anchors a comparison actually needs, and
they survive the squash — but the hash is provenance rather than a checkout
instruction. Nothing resolves it against git, deliberately: a check that did
would fail on every squash and teach people to delete records.

The cost is real: every manifest edit forces a re-run of the tiers that have
retained records, and a rebase that changes the commit a record names leaves the
record pointing at a commit that was never pushed, so it wants re-taking. That
friction is the point — it is what stops the numbers from quietly outliving
their inputs — but it makes the heavy tier something a change to the manifest
must budget for.

This ADR does not claim any slice is qualified. At the time of writing four of
the five are not, and the packet says so.
