# Endurance qualification

What one Axond replica looks like after hours of mixed traffic, rather than
after two minutes of it. Capacity qualification answers *how much, right now*
([capacity qualification](./capacity.md), [ADR 0033](../adr/0033-capacity-qualification-harness.md));
this page answers *and what is left behind afterwards*
([ADR 0038](../adr/0038-endurance-qualification-harness.md)) — the two failures that
only a long run makes visible:

- a resource that never comes back: memory, descriptors, sockets, or an upstream
  body still open after its caller is gone;
- an accounting row that goes missing or arrives twice, which is invisible until
  it is a bill.

This page qualifies the **stateless request path only** — a Tier 0 process with
no Redis, no Postgres, and no control plane.

## What the harness runs

`qualification/endurance/manifest.toml` is the committed input: one profile,
`mixed-endurance`, at two tiers. The driver boots the real `axond` binary
against the deterministic fake upstream over loopback and offers a closed-loop
mixed workload at a fixed concurrency, drawn from a seeded rotation, so the same
manifest produces the same sequence of requests on any host.

Every request is a point in four dimensions:

| Dimension | Values |
| --- | --- |
| Tenant | `platform` (operator credentials), `endurance-byok` (its own credentials), `endurance-fallback` (no credentials, `allow_platform_fallback`). |
| Provider and route | `fake-openai` over `/v1/chat/completions`, `/v1/embeddings`, `/v1/responses`; `fake-anthropic` over `/v1/messages`. |
| Wire | Buffered and streamed, interleaved. |
| Ending | `complete`, `cancelled` (the caller hangs up mid-answer), `dropped` (the upstream dies mid-stream), `faulted` (the upstream refuses before a byte). |

The endings are drawn in the committed proportion — 12 : 4 : 2 : 1 — from a
shuffled cycle, so a run offers exactly that mix rather than approximately it,
and the failures are interleaved with the successes rather than batched.

| Tier | Duration | Concurrency | Sample interval | Segment |
| --- | --- | --- | --- | --- |
| `smoke` | 15 s | 12 | 100 ms | 2.5 s |
| `soak` | 12 h | 48 | 1 s | 15 min |

The smoke tier runs in the ordinary test suite and in CI. It is the same code,
the same plan, and the same assertions as the soak — what differs is how long it
runs, and that the drift gates need a run long enough to have a slope.

## Run it

```bash
# The smoke tier, the sequential two-tier regression, and the deterministic
# checks. Part of the normal suite; also runs in CI.
cargo test --locked --all-features --test endurance -- \
  --nocapture --test-threads=1

# The soak tier: twelve hours, by name. The rest of the binary already ran in
# the suite, and offering its load again here would only contend with the soak.
just endurance

# A shorter dispatched run — forty minutes here. The override applies to the
# soak tier alone, so the smoke tier in the same binary keeps its committed
# fifteen seconds. Segments shrink to match, so the run still produces the
# segments the trend gates are evaluated over.
just endurance 2400000
```

The `Endurance` workflow runs the soak tier monthly and on dispatch (with an
optional duration override) and uploads both the result and its time series. It
invokes the soak test by name for the same reason `just endurance` does.

## What the harness holds while it runs

A twelve-hour run at the committed concurrency settles millions of requests, so
nothing the driver accumulates may scale with the run:

- finished attempts and parsed usage records are folded into running aggregates
  every 250 ms — `run.drain_interval_ms`, counted by `run.drains` — regardless
  of segment length, so a fifteen-minute segment retains no more than the
  smoke tier's 2.5-second one;
- request identities are fingerprinted and appended to sixty-four sharded files
  under `target/endurance/<tier>/<profile>-fingerprints/` rather than held in a
  whole-run set. Equal identities share a shard, so the duplicate count is
  exact while only one shard is ever in memory;
  `reconciliation.fingerprints` records the shard count, the peak shard, and
  that exactness;
- raw gateway output and fake-upstream request bodies are ring-buffered, with
  exact counters kept separately, so bounded history never becomes a miscount.

The fingerprint shards are working files, not evidence: they are rewritten by
the next run of the same tier and are not uploaded with the artifacts.

## What a run leaves behind

`target/endurance/<tier>/mixed-endurance.json` is the result, and
`mixed-endurance.samples.jsonl` beside it is every resource sample the run took,
written as it went rather than at the end — so a run that is killed at hour
eleven still has eleven hours of evidence.

The result carries the measurements (throughput, latency, TTFT, stream lifetime,
RSS, CPU, descriptors, sockets, occupancy, per-segment medians, usage-record
reconciliation) and the identity of everything that produced them: the SHA-256
of the binary, of the normalised config, of the manifest and every fixture; the
seed; the duration the run was actually offered (`run.requested_duration_ms`,
`profile.duration_ms`) next to the one the manifest commits
(`profile.manifest_duration_ms`) and which of the two it used
(`run.duration_source`); the fake upstream's address; the toolchain, git commit and dirty
flag, and the host's CPU, kernel, core count, and memory. **Numbers from
artifacts whose provenance differs are not comparable.**

## What fails, and what does not

Hard failures, asserted at both tiers, because none of them depends on how fast
the runner was:

- no failure the plan did not ask for (`max_unplanned_errors`), and every
  request the plan says succeeds, did (`min_accepted_fraction`);
- **exactly one usage record per dispatched request** — none missing
  (`max_missing_usage_records`), none repeated (`max_duplicate_usage_records`),
  and every record carrying a status the plan can account for
  (`max_unexpected_usage_statuses`). Identity is `request_id`;
- no upstream response body still open once every caller is gone
  (`max_leaked_upstream_streams`);
- descriptors and sockets returned after the load stops
  (`max_settled_socket_excess`), and bounded resident-memory growth over the run
  (`max_rss_growth_kib`);
- every axis of the plan actually offered (`workload_coverage`) over enough
  segments to fit a trend through (`min_segments`). A run that quietly covered
  one tenant and one ending would otherwise pass everything above it.

The soak tier adds the gates a short run cannot support: resident memory,
sockets, and descriptors are fitted over the per-segment medians and gated as a
slope per hour (`max_rss_drift_kib_per_hour`, `max_socket_drift_per_hour`,
`max_fd_drift_per_hour`). `trend.fitted` records whether the run was long enough
for those slopes to mean anything; the smoke tier declares no drift thresholds
because extrapolating an hourly figure from fifteen seconds would fail on noise,
and passing it would say nothing.

Recorded and **never** asserted: throughput, latency percentiles, TTFT, stream
lifetime, CPU. A shared runner cannot bound them without flaking, and a flaky
gate is one that gets disabled. A regression in those shows up as a moved number
between two artifacts with matching provenance.

Off Linux there is no `/proc`: RSS, CPU, sockets, and descriptors are recorded
as absent (`resources.procfs = false`) and the resource gates are not evaluated,
while everything else still gates. On a Linux host the same absence means the
sampler lost its subject, and the run fails `resource_sampling` — a measurement
that could not be taken must not read like one that passed.

## Planned faults and the open circuit

The fault alias points at a target that refuses every request, so its breaker
trips within the first few and stays tripped. After that the planned fault is
answered from the open circuit, typed as `all_provider_circuits_open`, without
an upstream attempt — and settles no usage record, because it spent nothing.
That is the gateway working ([ADR 0008](../adr/0008-target-failover-and-circuit-scope.md)),
so those requests are counted as `throughput.circuit_shed` and excluded from
what reconciliation expects. Dispatched faults are still asserted to happen: a
run whose faults were *all* shed never exercised the failure path.

## Measured envelope

Soak tier at a dispatched forty-five minutes, one replica, **debug build** —
`cargo test` builds unoptimized, so the throughput understates a release build
substantially. Host: 8 vCPU Intel Xeon Platinum 8175M @ 2.50 GHz, 31 GiB RAM,
Linux 5.15, rustc 1.97.1, `admission.queue_capacity = 0`, the fake upstream on
loopback.

<!-- ENVELOPE -->

## Reading an artifact

```bash
jq '{tier: .profile.tier, offered: .throughput.offered,
     usage: .reconciliation, rss: .resources.rss_kib, trend: .trend,
     failed: [.verdicts[] | select(.passed == false)]}' \
  target/endurance/soak/mixed-endurance.json
```

Fields worth knowing:

- `resources.*.baseline` is sampled before the load starts, `peak` over the
  whole run, and `settled` after the callers are gone and the process has been
  left idle. The gap between `peak` and `settled` is what the replica gives
  back; the gap between `settled` and `baseline` is what it did not.
- `trend.rss_kib_per_hour` is a least-squares fit through the per-segment
  medians, not the difference between the first and last sample: one segment
  that happened to be sampled during a GC pause cannot move it far.
  `trend.first_quarter_rss_kib` and `trend.last_quarter_rss_kib` are there to
  read the same question a second way.
- `reconciliation.expected` counts requests that reached an upstream attempt,
  which is not the same as requests offered — see the open-circuit note above.
- `latency_ms.stride` is above `1` when the run observed more requests than the
  artifact keeps: the retained sample is decimated across the whole run rather
  than truncated to its first minutes, so the percentiles still describe all of
  it.
- `profile.duration_ms` is the duration the run was offered and
  `profile.manifest_duration_ms` the tier's committed one; they differ whenever
  `run.duration_source` is `environment`. A dispatched duration is only ever
  read at the soak tier — the smoke tier shares the binary and keeps its
  committed seconds.
- `environment.config.normalized_toml` is the config the process actually
  booted, with the ephemeral ports and the per-run key directory replaced. It
  names environment variables and files, and carries no credential.
