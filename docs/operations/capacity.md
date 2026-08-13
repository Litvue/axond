# Capacity qualification

What one Axond replica costs to serve, how the numbers were produced, and which
of them a change is allowed to move. The design and its boundaries are
[ADR 0033](../adr/0033-capacity-qualification-harness.md); the bounds the
replica sheds at are [ADR 0030](../adr/0030-request-bounds-and-load-shedding.md).

What a replica costs over hours rather than minutes — leaks, descriptor balance,
and usage-record reconciliation — is
[endurance qualification](./endurance.md), which uses the same conventions.

This page qualifies the **stateless request path only** — a Tier 0 process with
no Redis, no Postgres, and no control plane. It is not evidence about stateful
serving, revision convergence, or any store-backed control. Where that leaves
production qualification as a whole, and which runs are retained as evidence, is
the [qualification packet](./qualification.md).

## What the harness runs

`qualification/capacity/manifest.toml` is the committed input. Each profile
declares a workload, a reduced and a heavy scale, and the thresholds the run is
gated on. The driver boots the real `axond` binary against the deterministic fake
upstream over loopback and offers the profile's load at a fixed concurrency:

| Profile | Workload |
| --- | --- |
| `buffered` | Buffered OpenAI chat completions. |
| `streaming` | Long-lived SSE streams, paced by the fake upstream, read to completion. |
| `mixed` | Both wire families, four routes, two providers, two credentials per provider, buffered and streamed interleaved. |
| `response-size` | Buffered answers rotating over 1 KiB, 32 KiB, and 256 KiB bodies. |
| `cancellation` | Streams where every second caller hangs up mid-answer. |

Each run writes `target/capacity/<tier>/<profile>.json`: throughput, latency
percentiles, TTFT and stream lifetime, RSS, CPU, sockets, occupancy, rejection
and error counts, usage-record counts and drops — and the SHA-256 of the binary,
the normalised config, the manifest, and every fixture, with the toolchain, git
commit, and host CPU, kernel, core count, and memory. **Numbers from artifacts
whose provenance differs are not comparable.**

## Run it

```bash
# The reduced tier. Runs as part of the normal suite; also runs in CI.
cargo test --locked --all-features --test capacity -- --nocapture

# The heavy tier. Same driver, same manifest, same assertions, larger scale.
# `--test-threads=1` keeps the two tiers from offering load at the same time.
AXOND_CAPACITY=1 cargo test --locked --all-features --test capacity -- \
  --nocapture --test-threads=1
```

The `Capacity` workflow runs the heavy tier on dispatch and weekly and uploads
the artifacts; the CI `tests` lane uploads the reduced ones. To reproduce a
stored result, check out its `environment.source.git_commit` (with
`git_dirty: false`), confirm the manifest hash matches, and run the tier its
`profile.tier` names on comparable hardware.

## Initial capacity envelopes

This table is the retained record
[`qualification/capacity/evidence/heavy-local.toml`](../../qualification/capacity/evidence/heavy-local.toml),
read in operator units; the record is the source, and a table that drifts from
it is a bug. Heavy tier, one replica, **debug build** — `cargo test` builds
unoptimized, so these understate release throughput substantially. Host: 8 vCPU
Intel Xeon Platinum 8559C, 31 GiB RAM, Linux 5.15.200, rustc 1.97.1, no queueing
(`admission.queue_capacity = 0`), the fake upstream on loopback.

| Profile | Concurrency | Requests | Accepted req/s | p50 | p95 | p99 | TTFT p95 | Peak RSS | Peak sockets | CPU cores used |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `buffered` | 128 | 40 000 | 7 675 | 15.9 ms | 25.0 ms | 30.6 ms | — | 44 MiB | 344 | 4.6 |
| `streaming` | 300 | 8 000 | 1 034 | 275 ms | 299 ms | 669 ms | 62 ms | 49 MiB | 733 | 3.1 |
| `mixed` | 128 | 12 000 | 1 360 | 2.7 ms | 279 ms | 293 ms | 51 ms | 38 MiB | 283 | 2.3 |
| `response-size` | 64 | 6 000 | 1 371 | 44.2 ms | 75.0 ms | 92.4 ms | — | 67 MiB | 156 | 4.1 |
| `cancellation` | 300 | 8 000 | 1 668 | 270 ms | 303 ms | 479 ms | 65 ms | 54 MiB | 720 | 3.5 |

Throughput and latency move 10–25% between runs on a shared host, while the
socket and memory columns barely move: read the first two as an order of
magnitude and the last two as the shape of a replica's resource use. Compare two
artifacts only when their `environment` blocks match.

Streamed latency is dominated by the fake upstream's pacing (a fixed ~40-chunk
answer), not by the gateway: read the stream rows as *concurrency the replica
holds*, not as model latency. The `mixed` p50/p95 gap is the interleave of
buffered and streamed requests in one distribution.

What the envelope says, in operator terms:

- **Sockets scale with concurrency, roughly two per in-flight stream** — one
  inbound, one upstream. 300 concurrent streams held ~750 descriptors. Size
  `ulimit -n` and `admission.max_in_flight_streams` together.
- **Resident memory is bounded by concurrency and body size, not by request
  count.** 40 000 buffered requests cost the same ~44 MiB as 400 would; 256 KiB
  bodies at 64 concurrent cost ~67 MiB. Bodies are buffered before dispatch
  (ADR 0030), so `admission.max_request_bytes` × concurrency is the term to
  reason about.
- **CPU saturates before memory.** Every profile used 2.3–4.6 cores of the 8
  available at these concurrencies. On this workload shape, a replica is
  CPU-bound; scale on CPU, and remember `[admission]` ceilings are *per replica*.

## Candidate SLOs

Candidates, not commitments — they are stated so a future qualification on
production-representative hardware and a release build has something to argue
with, and so an operator has a starting point for their own alert thresholds.
None of them is enforced by CI (see below).

| Candidate | Statement |
| --- | --- |
| Availability | ≥ 99.9% of admitted requests answered without a gateway-originated 5xx, measured per replica over 30 days, excluding upstream-attributed failures. |
| Buffered overhead | Gateway-added p95 latency ≤ 25 ms and p99 ≤ 50 ms for a buffered request, excluding upstream time. |
| Time to first token | Gateway-added p95 TTFT ≤ 50 ms over the upstream's own first byte. |
| Shedding verdict | 100% of requests refused for capacity carry a typed `429`/`503` rather than a timeout or a reset. |
| Accounting | ≥ 99.99% of admitted requests settle exactly one usage record, including cancelled streams; drops are counted and alertable (`axond.usage.records_dropped`). |
| Stream survival | ≥ 99.9% of streams that reach first byte either complete or end with a typed error, with no upstream socket outliving the caller. |

The accounting, shedding, and stream-survival candidates are the ones the harness
already gates on at every run; the latency and availability candidates need
release-build qualification on known hardware before they can be promised.

## What fails CI, and what does not

Hard failures, asserted on every reduced and heavy run, because none of them
depends on how fast the runner was:

- every offered request accepted (`min_accepted_fraction`),
- nothing shed (`max_rejections`) and no errors (`max_errors`),
- one usage record per admitted request (`max_missing_usage_records`), with
  cancelled streams settling as `client_cancelled`,
- no upstream response body still open once every client is gone
  (`max_leaked_upstream_streams`),
- bounded resident-memory growth over the run (`max_rss_growth_kib`), measured
  from the baseline to whichever is higher of the sampled peak and the settled
  reading taken after the load stops.

Recorded and **never** asserted: throughput, latency percentiles, TTFT, stream
lifetime, CPU, socket counts. A shared CI runner cannot bound them without
flaking, and a flaky gate is one that gets ignored. A performance regression
therefore shows up as a moved number between two artifacts with matching
provenance — compare heavy-tier runs, not CI runs.

Off Linux there is no `/proc`, so RSS, CPU, and sockets are recorded as absent
(`resources.procfs = false`) and the memory-growth threshold is not evaluated;
every other property still gates. On a Linux host the same absence means the
sampler lost its subject rather than never having one, and the run fails the
`resource_sampling` verdict — a measurement that could not be taken must not read
like one that passed. A run whose sampler never got a turn (`resources.samples =
0`) fails the same verdict: a span made only of its seeded baseline says nothing
about what the load cost.

## Reading an artifact

```bash
jq '{profile: .profile.id, tier: .profile.tier,
     accepted_rps: .throughput.accepted_rps, p99: .latency_ms.p99,
     rss: .resources.rss_kib, verdicts: [.verdicts[] | select(.passed == false)]}' \
  target/capacity/heavy/streaming.json
```

Fields worth knowing:

- `throughput.closed_loop` is always `true`: the driver holds a concurrency
  rather than pushing an arrival rate, so `offered_rps` is a *result* of service
  time, not an input.
- `occupancy.awaiting_first_byte_peak` is the driver's view of the queue the
  replica is holding — the client-side counterpart to
  `axond.admission.in_flight` (see [observability](../observability.md)). It
  counts a request until the first byte of its *answer* arrives: response headers
  for a buffered request, the first relayed chunk for a stream, whose headers can
  precede its first token by a long way.
- `resources.*.settled` is sampled when the last client byte arrives, before the
  usage-record settle wait, so idle-time cleanup is not reflected in it. Upstream
  socket cleanup is asserted separately through `upstream.streams_open_at_end`.
- `environment.config.normalized_toml` is the config the process actually booted,
  with the ephemeral ports replaced. It names environment variables and carries
  no credential.
