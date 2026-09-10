# Store latency baseline

How much of a request's cost is the Store, how that differs between SQLite and
Postgres, and how to measure a change against it. This is the evidence the
Store-contention epic ([#459](https://github.com/Litvue/axond/issues/459)) is
sequenced on: [#462](https://github.com/Litvue/axond/issues/462) produces it,
and #463–#466 are each measured with one of the workloads below.

The apparatus is the [capacity harness](./capacity.md)
([ADR 0033](../adr/0033-capacity-qualification-harness.md)) pointed at a
narrower subject. Capacity qualification asks what a replica costs on SQLite;
this page asks what the Store contributes to that cost, on both supported
backends, with the wait for a connection told apart from the query that ran on
it. Its rules are the same: the numbers carry their provenance, latency on a
shared runner is informational, and what fails is conservation, never speed.

## What the harness runs

`crates/gateway/tests/store_baseline.rs` drives
`crates/gateway/tests/support/capacity/store_baseline.rs`. The scenarios are
code rather than a manifest, so the artifact hashes that file as its input.
Every scenario boots the real `axond` binary against the deterministic fake
upstream over loopback, with an explicit `[storage]` section selecting the
backend under test, and exports metrics to a loopback OTLP receiver. Each
scenario gets a Store of its own — a fresh SQLite file, or a fresh schema in
the test database — so nothing carries over between them.

| Scenario | Workload | Subject |
| --- | --- | --- |
| `steady-buffered` | Closed-loop buffered chat completions at a fixed concurrency. | The inference Store path itself: one namespace+admission join before dispatch and one charge after the response (ADR 0064), plus the background usage-index append. |
| `steady-streamed` | Closed-loop paced SSE streams, read to completion. | The same path when the request holds no connection while streaming; TTFT is the number that moves. |
| `burst` | Waves of simultaneous requests with a 250 ms pause between waves. | What a pool does when demand arrives all at once and goes away: fills, drains to its idle cap, reconnects on the next wave (#465). |
| `summaries` | The steady buffered loop while management readers loop over `GET /api/v1/namespaces/platform/usage`. | Management reads sharing the connection or pool with inference, over a small index. |
| `slow-store` | The same, over a usage index pre-seeded with hundreds of thousands of rows. | The slow-Store case without a fault injector: each summary holds the connection for a while, and inference queues behind it (#463, #464). |
| `large-payload` | Buffered requests carrying a large native prompt and relaying a 256 KiB answer. | The payload the gateway copies and re-encodes, on the same Store path (#466). |

Two tiers offer the same scenarios at different scales:

| Scenario | Smoke: concurrency × requests | Full: concurrency × requests | Readers (smoke / full) | Seeded rows (smoke / full) | Payload (smoke / full) |
| --- | --- | --- | --- | --- | --- |
| `steady-buffered` | 4 × 24 | 32 × 3 000 | — | — | — |
| `steady-streamed` | 4 × 12 | 32 × 600 | — | — | — |
| `burst` | waves of 12 × 24 | waves of 128 × 1 024 | — | — | — |
| `summaries` | 4 × 24 | 32 × 2 000 | 1 / 4 | — | — |
| `slow-store` | 4 × 24 | 32 × 2 000 | 1 / 4 | 2 000 / 200 000 | — |
| `large-payload` | 4 × 12 | 16 × 600 | — | — | 32 KiB / 256 KiB |

Every scenario runs a **warmup** of the same shape first — 8–300 requests,
discarded — so the measured repetitions start from a process whose allocator,
pool, and prepared statements have been used, not from a cold boot. The smoke
tier then measures one repetition; the full tier measures three
(`AXOND_STORE_BASELINE_REPETITIONS` overrides either). The summary tables quote
the **median repetition by p50**, so one disturbed repetition does not become
the number.

Where a control is meaningful, the same load is then offered to the fake
upstream **directly**, after its own warmup, and the artifact records gateway
latency minus control latency at each percentile as `overhead_ms`. The two
scenarios whose subject is the management readers have no control: the upstream
has no summaries to serve, and a control there would measure the buffered loop
twice.

## What an artifact carries

Each run writes `target/store-baseline/<tier>/<backend>.json` and a
human-readable `<backend>.md` beside it (`AXOND_STORE_BASELINE_DIR` moves the
directory). The JSON is schema 1 of the `axond store latency baseline` harness
and holds, per scenario:

- the scale that was offered and the Store's identity at the end of the run —
  `sqlite_version()` or `version()`, usage-index row count, and the file size or
  `pg_total_relation_size` of the usage relation;
- the warmup's offered and accepted counts;
- every measured repetition: offered, accepted, shed, errors, transport
  failures, the `reconciled` flag, elapsed time, accepted req/s, latency and
  TTFT percentiles, status and typed-error tallies, CPU and RSS from `/proc`,
  and the usage-record settlement (expected, observed, missing);
- the summary readers' request count, success count, latency percentiles, and
  rate, when the scenario ran them;
- the control run and the derived overhead, when the scenario has one;
- `store_evidence`: the decoded `axond.store.*` and `axond.usage.index.queue.*`
  points, cumulative over the scenario's process (warmup included, and the
  artifact says so).

And once per artifact, the `environment` block the capacity harness records:
binary SHA-256 and version, normalised config SHA-256 and text, the SHA-256 of
the scenario source, host OS, kernel, CPU model and count, memory, whether the
process ran in a container, rustc version, cargo profile, and git commit with
its dirty flag. **Artifacts whose `environment` blocks differ are not
comparable.**

## The Store phases

The instrumentation this harness decodes is what the optimisation issues are
measured with. Every Store call records two histograms and one counter under a
closed vocabulary — `axond.store.backend` (`sqlite` | `postgres`) and
`axond.store.operation` (ten kinds of work) — and nothing else: no namespace,
period, or request identity ever becomes a label.

| Metric | What it separates |
| --- | --- |
| `axond.store.acquire_wait` | Time from the call to holding a connection. On SQLite: blocking-pool dispatch plus the one connection mutex. On Postgres: the pool permit, plus a fresh connect when nothing idle was available. Recorded for every call, including a saturated pool. |
| `axond.store.query_duration` | Time the connection was held: the statement or transaction itself. Recorded only for calls that reached a connection. |
| `axond.store.operations` | Calls by outcome: `ok`, `error`, or `saturated` (Postgres pool permit not granted within its wait bound). |
| `axond.store.connections_opened` | Postgres sessions opened, including reconnects after the pool shed idle sessions between bursts. |
| `axond.usage.index.queue.depth` | Exact depth of the bounded usage-index queue at each enqueue. |
| `axond.usage.index.queue.wait` | How long a usage record waited in that queue before the worker took it. |

Read them as a pair: an acquire wait that grows while query duration stays flat
is callers queueing on the Store, not the Store being slow. The full table with
alerting guidance is in [observability](../observability.md).

## Run it

```bash
# The smoke tier, SQLite. Runs as part of the normal suite; also runs in CI.
cargo test --locked --all-features --test store_baseline -- --nocapture

# The same, with the Postgres arm. Any database the harness may create schemas
# in; each scenario gets a schema of its own and drops it afterwards. The DSN
# never enters the config or the artifact — the process reads it from an
# environment variable the config names.
AXOND_TEST_POSTGRES_DSN='postgres://postgres:secret@127.0.0.1:5432/axond_test?sslmode=disable' \
  cargo test --locked --all-features --test store_baseline -- --nocapture

# The full tier: the baseline proper. Release build, both backends when the DSN
# is set, one tier at a time. `just store-baseline` is the same command.
AXOND_STORE_BASELINE=1 AXOND_TEST_POSTGRES_DSN='...' \
  cargo test --release --locked --all-features --test store_baseline -- \
    full_tier --nocapture --test-threads=1
```

A Postgres whose `ssl = on` presents a certificate webpki does not trust — a
distribution default — fails the boot with `error performing TLS handshake`
under the default `sslmode=prefer`. Say `sslmode=disable` for a loopback test
database, or `sslmode=require` against one whose certificate chains to a public
root.

Without `AXOND_TEST_POSTGRES_DSN` the Postgres arm skips and says so; without
`AXOND_STORE_BASELINE=1` the full tier skips and says so. The skip is printed,
never silent, so a run that produced only a SQLite artifact is recognisable as
one.

## What fails, and what does not

Asserted on every run, at both tiers, because none of it depends on how fast
the host was:

- **Reconciliation.** For every measured repetition,
  `offered == accepted + rejected + errors`, where `errors` counts typed
  failures, transport failures, and — since nothing hangs up in this harness —
  any cancelled attempt, so a driver bug is noticed rather than absorbed. The
  artifact's own `reconciled` flag must agree, the offered count must equal the
  scale that was asked for, and at least one request must have been accepted.
- **Accounting.** Every accepted request settles exactly one usage record
  before the repetition ends (`usage_records.missing == 0`), waited for with a
  bound on the sink rather than on the request path.
- **Controls.** The fake upstream accepted every control request, so an
  overhead is a difference between two complete measurements.
- **Coverage of the instrumentation.** For the scenario's backend, the process
  exported `axond.store.acquire_wait` *and* `axond.store.query_duration` points
  for `namespace_resolve`, `budget_charge`, and `usage_append` with non-zero
  counts and a matching `axond.store.operations` count; a `usage_summary` point
  when readers ran; and the two `axond.usage.index.queue.*` histograms. A
  future change that stops recording a phase fails here, not in a dashboard.
- **The scenario set is closed and ordered**, so two artifacts compare like
  with like.

Recorded and **never** asserted: latency percentiles, TTFT, throughput, CPU,
RSS, acquire-wait and query-duration values, queue depth and age, connections
opened. On a shared runner these measure the neighbours as much as the
gateway, and a gate on them would flake until it was ignored. The artifact's
`caveat` field says the same thing to a reader who skipped this page.

## Comparing two commits on a controlled runner

Shared-CI numbers say whether the harness still reconciles. A claim that a
change *moved* a number needs a runner nothing else is using, and the same
runner for both sides. The procedure:

1. **Reserve one Linux host** for the duration; nothing else scheduled on it.
   Note its identity — the artifact will record CPU model, core count, memory,
   kernel, and whether it ran containerized, and both artifacts must agree on
   all of them. A cloud VM is acceptable if it is the same instance for both
   sides; a burstable instance class is not.
2. **Build both sides in release**, with the same rustc, from clean trees
   (`git_dirty: false`). The baseline side is the merge-base of the change; the
   candidate is the change itself. `cargo_profile` must read `release` in both
   artifacts.
3. **Run the baseline side first, then the candidate, then the baseline
   again**, each with `just store-baseline` (or the full command above) and
   `AXOND_STORE_BASELINE_DIR` pointing at a directory per run. The second
   baseline run is the noise floor: any difference between the two baseline
   artifacts is what the host contributed, and a candidate-versus-baseline
   difference smaller than that is not a result.
4. **Give Postgres the same treatment.** The database should be on the same
   host over loopback for the phase numbers to be comparable across runs; if
   the question is about network round trips (#465), put it where production
   puts it and hold that constant across all three runs.
5. **Increase repetitions** when the noise floor is wide:
   `AXOND_STORE_BASELINE_REPETITIONS=5` or more. The summary quotes the median
   repetition; the JSON keeps all of them.
6. **Check the provenance before reading a number.** `environment.hardware`,
   `environment.toolchain`, and `environment.config.sha256` must be identical
   across the three artifacts; `environment.manifest.sha256` (the scenario
   source) will differ only if the change touched the harness, in which case the
   comparison is of two different experiments and must say so.
7. **Read the phase columns before the request columns.** A change to the Store
   should show in `store_evidence.operations.<op>.acquire_wait` or
   `query_duration` first; a request-path p95 that moved without either phase
   moving is a change somewhere other than the Store, or noise.
8. **Retain the raw JSON** of all three runs with the comparison, not the
   markdown alone.

```bash
jq '{backend, tier, commit: .environment.source.git_commit,
     profile: .environment.toolchain.cargo_profile, cpus: .environment.hardware.cpus,
     scenarios: [.scenarios[] | {id,
       p50: .repetitions[0].latency_ms.p50, p95: .repetitions[0].latency_ms.p95,
       overhead_p50: .overhead_ms.p50,
       resolve_wait_mean: .store_evidence.operations.namespace_resolve.acquire_wait.mean_ms,
       resolve_query_mean: .store_evidence.operations.namespace_resolve.query_duration.mean_ms,
       connections: .store_evidence.connections_opened}]}' \
  target/store-baseline/full/sqlite.json
```

## Baseline

The retained records are under
[`qualification/store-baseline/evidence/`](../../qualification/store-baseline/evidence/):
`full-local/sqlite.{json,md}` and `full-local/postgres.{json,md}`. They are the
source; the tables below are read from them. **This is a shared cloud VM with
4 vCPUs, not a controlled runner**: the repetitions' spread and the negative
overheads on some rows are what that costs, and the numbers are the shape of
the Store's contribution rather than a promise about any of them.

<!-- BASELINE_TABLES -->

## Recommended workloads and thresholds for the optimisation issues

Thresholds here are for a controlled-runner comparison as described above,
never for CI. Each is stated as the artifact field to read and the direction a
change must move it by more than the noise floor of the paired baseline runs.

| Issue | Workload | Read | Must hold |
| --- | --- | --- | --- |
| [#463](https://github.com/Litvue/axond/issues/463) bound SQLite work admission | `slow-store` and `burst`, SQLite | `store_evidence.operations.namespace_resolve.acquire_wait` (p95 ≤ and max), `budget_charge.acquire_wait`, `index_queue.wait`, and the request-path p99 | Acquire-wait p95 and max for `namespace_resolve` and `budget_charge` fall; `query_duration` for both does not rise; `usage_records.missing` stays 0; any new `saturated`-style outcome appears as a typed `503` counted under `rejected`, so the reconciliation still holds. A design that trades tail latency for dropped index writes fails on `usage_records.missing`. |
| [#464](https://github.com/Litvue/axond/issues/464) usage summaries at scale | `slow-store`, SQLite, with `seeded_usage_rows` raised (edit the scale or add a tier; the artifact records the row count and file size) | `operations.usage_summary.query_duration`, `summaries.latency_ms`, and — the interference — `operations.namespace_resolve.acquire_wait` while readers run, against the same scenario's `steady-buffered` sibling | `usage_summary.query_duration` mean and p95 fall at 200 000 rows and keep falling as rows grow; the `namespace_resolve.acquire_wait` gap between `slow-store` and `steady-buffered` narrows; summary results are byte-identical to the fold they replace on the parity fixtures the issue lists. |
| [#465](https://github.com/Litvue/axond/issues/465) Postgres burst connection churn | `burst`, Postgres, with the database at production network distance for the network-latency question | `store_evidence.connections_opened`, `operations.namespace_resolve.acquire_wait` p95 ≤, request-path p95/p99 per wave, and `axond.store.operations{outcome="saturated"}` | `connections_opened` per wave falls to the configured idle retention rather than tracking wave size; `namespace_resolve.acquire_wait` p95 no longer carries a connect; no `saturated` outcome appears at the scale the pool is sized for; the session cap the design states is not exceeded — read it from the database, not the artifact. |
| [#466](https://github.com/Litvue/axond/issues/466) payload clones across credential attempts | `large-payload`, either backend (the Store is the control here, not the subject), plus a variant with several credentials configured so an attempt retries | `overhead_ms.p50` and `.p95` against the control, `resources.cpu_utilization`, `resources.rss_kib.peak`, and TTFT on the streamed sibling | Overhead p50/p95 and CPU per accepted request fall at 256 KiB with the Store phases unchanged; the wire-compatibility tests the issue lists still pass; no phase in `store_evidence` moves, which proves the saving was in the transport and not the Store. |

What the baseline already says about the order of work, on this host:

- **On SQLite, the acquire wait is the Store's cost.** Across the inference
  operations, `query_duration` means stay well under a millisecond while
  `acquire_wait` means are several milliseconds and grow with concurrency and
  with readers present; the connection mutex behind `spawn_blocking` is the
  queue #463 bounds, and `slow-store` is the workload that shows it.
- **On Postgres, the acquire wait carries connects.** `burst` opens sessions
  in proportion to the wave rather than to the idle cap, and its
  `namespace_resolve.acquire_wait` is an order of magnitude above the steady
  scenarios'; that is #465's mechanism, observed rather than inferred.
- **Summaries interfere by holding the connection**, not by being many:
  `usage_summary.query_duration` grows with seeded rows and the inference
  operations' acquire wait grows with it, while summary request counts stay
  small. #464 is measured by whether that coupling breaks.
- **The large payload's cost is not in the Store.** `large-payload`'s Store
  phases match `steady-buffered`'s while its overhead against the control is
  the largest of the scenarios with one; #466 is measured against the control,
  and the Store phases are its invariant.
