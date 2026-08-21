# 40. Endurance qualification: a long mixed workload, with bounded evidence

Date: 2026-08-13

## Status

Accepted

Extends [ADR 0033](./0033-capacity-qualification-harness.md) from *what one
replica costs at a fixed offered load* to *whether the same process is still the
same process after half a day of mixed traffic*. It reuses that ADR's
manifest-first shape, its provenance rules, and its refusal to gate on
throughput and latency.

## Context

Capacity answers a sizing question over a few minutes. It cannot answer the
question an operator actually asks before leaving a replica up for a month:
after hours of buffered and streamed traffic across tenants and providers, with
callers that hang up, upstreams that die mid-stream, and upstreams that refuse —
does resident memory come back down, are descriptors and sockets balanced, is
every dispatched request still accounted for exactly once?

Those are different failure modes from capacity's. A leak of one descriptor per
cancelled stream is invisible in a three-minute run at any load and fatal in a
twelve-hour one. So is a usage record dropped on a path that only opens when a
circuit trips. Both need duration, mixed endings, and accounting that is
reconciled rather than sampled.

Duration is also what makes the evidence hard. A twelve-hour run at the
committed concurrency settles millions of requests, and the obvious harness —
accumulate attempts, keep every usage record, hold every request id in a set to
find duplicates — grows without bound for exactly as long as the run it is
watching. A harness that leaks while measuring leaks either dies or, worse,
poisons the resident-memory reading it exists to produce.

Finally, a twelve-hour run cannot be the only way to find out the harness broke.
It does not fit a GitHub-hosted runner, it is dispatched monthly at best, and a
regression that lands on Tuesday should not wait for it.

## Decision

One driver, one manifest, one artifact schema, offered at two tiers, with every
accumulator bounded by something that is not the run length.

**The manifest is the whole input.** `qualification/endurance/manifest.toml`
commits the seed, the ending mix, and per-tier duration, concurrency, think
time, sample interval, segment length, and thresholds. The ending rotation is a
seeded permutation of one mix cycle, so a run offers exactly the committed
proportions in the same order on every host. Adding a workload means changing
committed data and the enum that admits it, as in ADR 0033.

**Two tiers, one binary.** `smoke` runs in the ordinary suite on every change;
`soak` is the twelve-hour tier, dispatched from the `Endurance` workflow. They
differ in duration, concurrency, and which *drift* gates apply — a slope stated
per hour is noise on a fifteen-second run. Everything that does not depend on
duration (losses, surplus identities, duplicates, leaks, unexpected statuses, socket balance) is
asserted at both tiers.

**A dispatched duration moves the soak tier only.** Both tiers share a test
binary, so an environment override read unconditionally would silently turn the
smoke tier into a second multi-hour run. The override applies at `Tier::Soak`
and nowhere else, the artifact records the effective duration, the committed
manifest duration, and which of the two it used, and a sequential regression
offers both tiers in one process with a soak duration dispatched over them to
prove it.

**What the driver holds is bounded by a tick, not by a segment.** Finished
attempts and parsed usage records are folded into running aggregates every 250
ms regardless of the segment length, so a fifteen-minute segment retains no more
than a 2.5-second one; the artifact records the interval and the number of
folds. Raw gateway output and fake-upstream request bodies are ring-buffered,
with exact counters kept separately, so a bounded history never becomes a
miscount.

**Duplicate detection is externalized, not weakened.** Request identities are
fingerprinted and appended to sixty-four sharded files rather than held in a
whole-run set. Equal identities always land in the same shard, so tallying one
shard at a time is *exact*, while the driver only ever holds one shard's worth.
The artifact records the shard count, the peak shard, and that the count was
exact.

**Reconciliation is against dispatched requests.** A planned fault can be
answered by an already-open circuit; a request that never reached an upstream
owes no usage record, and is classified as shed rather than lost. The run
asserts that at least one planned fault did reach upstream, so the fault path is
not quietly satisfied by the breaker.

**The idle tail is evidence, not a segment.** Load-tail segments are closed
before the settle, and the idle readings that follow are excluded from trend
fitting and from the segment count: a resident-memory slope must be fitted
through the run, not through the quiesce that follows it.

**Throughput and latency are recorded and never asserted**, as in ADR 0033. A
shared runner moves them, and a flaky gate is one that gets switched off.

### State tier

Tier 0. The harness drives a stateless replica against loopback fake upstreams,
needs no datastore, and changes no shipped code path. It raises no deployment's
tier.

## Consequences

- The soak's expensive property — hours — is spent on the failure modes that
  need hours, while the harness itself is exercised on every change by the
  smoke tier and the sequential regression.
- The evidence is reproducible from the manifest, the fixtures, the seed, and
  the recorded provenance, and a result states the duration it was actually
  offered rather than the one the manifest commits.
- Bounded retention costs some disk during a run (the fingerprint shards) and a
  second pass at the end. That is the cheaper direction: the alternative scales
  the harness's memory with the run and contaminates the reading.
- Packet promotion for v0.4.0 uses the CI **smoke** tier (same leak and
  accounting assertions as the soak). The twelve-hour soak remains a scheduled
  observational lane for per-hour drift and is not a publication gate. A
  shortened soak may still diagnose the harness; it is not ship-gate evidence.
- Gating socket and descriptor balance at zero tolerance makes the suite
  sensitive to a genuinely leaky change and to nothing else: the readings are
  taken after the driver's own client is dropped and the process has settled.
