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
| `tenants` | Two namespaces served at once, each with its own inbound key and its own credential pool, platform fallback off. |
| `shedding` | More callers than the replica admits, each holding its slot while the upstream thinks. |
| `backend-limits` | One healthy upstream per two that stall — one before response headers, one mid-body. |

The last three profiles answer questions a throughput number cannot. `tenants`
records, per namespace, what it offered, what it was served, what it was
charged, and how many upstream calls carried *its own* credential — the
credential itself is never recorded, only whose it was and how often. `shedding`
boots a ceiling far below the load it offers, so what is measured is the refusal
rather than the throughput. `backend-limits` boots a short transport bound and
then stalls two upstreams out of three: every request has to end on the bound
the replica declares rather than on the upstream relenting, and once a stalling
target's circuit trips the rest are refused at once while the healthy target
keeps serving every request sent to it. Those two — the profiles that boot a
limit — also offer one more request after the load stops, because a ceiling that
keeps a permit or a bound that keeps a slot is invisible in every other number
on this page.

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
it is a bug — `ops/check-docs.py` reads both and fails when they disagree, so a
re-run that stops at the numbers below leaves the rules of thumb under them
failing too. Heavy tier, one replica, **debug build** — `cargo test` builds
unoptimized, so these understate release throughput substantially. Host: 8 vCPU
Intel Xeon Platinum 8559C, 31 GiB RAM, Linux 5.15.200, rustc 1.97.1, no queueing
(`admission.queue_capacity = 0`), the fake upstream on loopback.

| Profile | Concurrency | Requests | Accepted req/s | p50 | p95 | p99 | TTFT p95 | Peak RSS | Peak sockets | CPU cores used |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `buffered` | 128 | 40 000 | 7 679 | 16.0 ms | 25.1 ms | 30.8 ms | — | 46 MiB | 326 | 4.9 |
| `streaming` | 300 | 8 000 | 1 032 | 273 ms | 297 ms | 672 ms | 61 ms | 53 MiB | 738 | 3.3 |
| `mixed` | 128 | 12 000 | 1 370 | 2.6 ms | 277 ms | 286 ms | 51 ms | 42 MiB | 276 | 2.4 |
| `response-size` | 64 | 6 000 | 1 422 | 42.6 ms | 72.0 ms | 87.9 ms | — | 69 MiB | 153 | 4.3 |
| `cancellation` | 300 | 8 000 | 1 655 | 268 ms | 316 ms | 449 ms | 65 ms | 59 MiB | 702 | 3.7 |
| `tenants` | 128 | 12 000 | 951 | 254 ms | 264 ms | 270 ms | 51 ms | 42 MiB | 271 | 2.1 |
| `shedding` | 512 | 20 000 | 3.8 | 19.6 ms | 49.6 ms | 84.0 ms | — | 70 MiB | 1 994 | 1.4 |
| `backend-limits` | 64 | 1 200 | 99 | 3.4 ms | 2 004 ms | 2 060 ms | — | 35 MiB | 134 | 0.1 |

Throughput and latency move 10–25% between runs on a shared host, while the
socket and memory columns barely move: read the first two as an order of
magnitude and the last two as the shape of a replica's resource use. Compare two
artifacts only when their `environment` blocks match.

The last three rows are not throughput measurements and must not be read as
one. `shedding` offers 20 000 callers at a ceiling of 8, so its accepted rate is
the ceiling divided by how long one upstream answer takes; what it shows is the
1 994 descriptors 512 simultaneous callers cost a replica that is refusing
almost all of them, and that the refusal is cheap (1.4 cores). `backend-limits`
spends most of its wall clock waiting on upstreams that never answer, so its
latency columns are the 2 000 ms bound the replica declares rather than work it
did, and its CPU is near zero for the same reason. `tenants` is a like-for-like
interleave of `mixed` split across two namespaces, and costs what `mixed` costs.

Streamed latency is dominated by the fake upstream's pacing (a fixed ~40-chunk
answer), not by the gateway: read the stream rows as *concurrency the replica
holds*, not as model latency. The `mixed` p50/p95 gap is the interleave of
buffered and streamed requests in one distribution.

What the envelope says, in operator terms:

- **Sockets scale with concurrency, roughly two per in-flight stream** — one
  inbound, one upstream. 300 concurrent streams held ~738 descriptors. A
  refused caller costs an inbound descriptor too, until it is told no: the
  `shedding` row above holds four times the sockets of any other profile while
  admitting eight requests. Size `ulimit -n` for the load offered, not for the
  load admitted, and set it alongside `admission.max_in_flight_streams`.
- **Resident memory is bounded by concurrency and body size, not by request
  count.** 40 000 buffered requests cost the same ~47 MiB as 400 would; 256 KiB
  bodies at 64 concurrent cost ~68 MiB. Bodies are buffered before dispatch
  (ADR 0030), so `admission.max_request_bytes` × concurrency is the term to
  reason about.
- **CPU saturates before memory.** Every profile that served its load used
  2.1–4.8 cores of the 8 available at these concurrencies. On this workload
  shape, a replica is CPU-bound; scale on CPU, and remember `[admission]`
  ceilings are *per replica*.

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

- every offered request accepted (`min_accepted_fraction`) — except where the
  profile exists to be refused, where the refusal itself is bounded instead
  (`min_rejected_fraction`, `max_rejected_fraction`, `max_error_fraction`) and
  the floor on what was still served is a count (`min_accepted`), because a
  replica behind a ceiling serves what the ceiling allows however many callers
  arrive,
- nothing shed (`max_rejections`) and no errors (`max_errors`),
- no request outliving the bound the replica declares (`max_over_deadline`) and
  no untyped failure (`max_untyped_errors`, counting both an error answered
  without a typed body and a request that ended at the transport with no answer
  at all),
- no tenant served with a credential it does not own
  (`max_foreign_credential_uses`) and no charge filed against a namespace that
  did not send the request (`max_misattributed_usage_records`),
- the replica still serving one request after the load stops
  (`max_unserved_after_load`),
- one usage record per admitted request (`max_missing_usage_records`), with
  cancelled streams settling as `client_cancelled`,
- no upstream response body still open once every client is gone
  (`max_leaked_upstream_streams`),
- and, for any of these, the measurement itself: a threshold whose measurement
  block is absent from the artifact fails as `<threshold>_unmeasured` rather
  than passing as a zero,
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
