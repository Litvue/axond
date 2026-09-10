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
| `burst` | Waves of simultaneous requests with a 250 ms pause between waves. | What a pool does when demand arrives all at once and goes away: fills, retains idle sessions up to the pool size, and reuses them on the next wave (#465). |
| `summaries` | The steady buffered loop while management readers loop over `GET /api/v1/namespaces/platform/usage`. | Management reads sharing the connection or pool with inference, over a small index. |
| `slow-store` | The same, over a usage index pre-seeded with hundreds of thousands of rows. | Management summaries over a large index. After #464 they run on a read-only SQLite connection and do not take the inference dispatch slot. |
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
| `axond.store.query_duration` | Time the connection was held: the statement or transaction itself, captured before the SQLite mutex is released (so runtime join delay is not counted as Store execution). Recorded only for calls that reached a connection. |
| `axond.store.operations` | Calls by outcome: `ok`, `error`, or `saturated` (Postgres pool permit not granted within its wait bound). |
| `axond.store.connections_opened` | Successful Postgres session opens, including reconnects after a dead session. Failed `connect()` attempts are not counted. After #465, a burst that fits in the pool should not increment this per wave. |
| `axond.store.connections_reused` | Checkouts served from idle. |
| `axond.store.connections_discarded` | Sessions dropped rather than returned idle. |
| `axond.store.pool.sessions` | Live and idle occupancy. The sum must stay at or below 32 per replica. |
| `axond.usage.index.queue.depth` | Occupied slots in the bounded usage-index queue at each accepted enqueue, from the channel's remaining capacity after `try_reserve`. |
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
the Store's contribution rather than a promise about any of them. Postgres ran
on the same host over loopback (`sslmode=disable`), so its phase numbers carry
no network round trip; other builds were running on the host during both arms.
The binary both arms booted is the one the artifacts hash
(`environment.binary.sha256`), built in release from the commit they name with
a clean tree.

- `sqlite`: commit `e3dd018807cf8d62fa833dd229d009974aaff678`, release profile, rustc 1.97.1 (8bab26f4f 2026-07-14), 4 vCPU Intel(R) Xeon(R) Processor, 16014 MiB, linux 6.12.94+; Store 3.46.0; 3 repetitions per scenario; run took 149 s.
- `postgres`: commit `e3dd018807cf8d62fa833dd229d009974aaff678`, release profile, rustc 1.97.1 (8bab26f4f 2026-07-14), 4 vCPU Intel(R) Xeon(R) Processor, 16014 MiB, linux 6.12.94+; Store PostgreSQL 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) on x86_64-pc-linux-gnu, compiled by gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0, 64-bit; 3 repetitions per scenario; run took 96 s.

### Request path (median repetition, release build)

| Scenario | Backend | Accepted req/s | p50 | p95 | p99 | TTFT p95 | Overhead p50 / p95 vs control | CPU cores | Peak RSS | Shed / errors / usage missing |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `steady-buffered` | sqlite | 581 | 53.2 ms | 89.8 ms | 105.6 ms | — | 52.9 ms / 89.2 ms | 0.30 | 28 MiB | 0 / 0 / 0 |
| `steady-buffered` | postgres | 816 | 28.1 ms | 112.1 ms | 182.5 ms | — | 27.7 ms / 111.5 ms | 0.43 | 26 MiB | 0 / 0 / 0 |
| `steady-streamed` | sqlite | 116 | 264.7 ms | 314.5 ms | 317.3 ms | 99.4 ms | 6.7 ms / 51.4 ms | 0.20 | 26 MiB | 0 / 0 / 0 |
| `steady-streamed` | postgres | 121 | 257.4 ms | 272.4 ms | 286.7 ms | 55.3 ms | -0.607 ms / 10.5 ms | 0.21 | 26 MiB | 0 / 0 / 0 |
| `burst` | sqlite | 282 | 96.5 ms | 192.9 ms | 262.2 ms | — | 95.2 ms / 191.1 ms | 0.13 | 36 MiB | 0 / 0 / 0 |
| `burst` | postgres | 341 | 91.2 ms | 150.5 ms | 168.5 ms | — | 90.0 ms / 148.7 ms | 0.18 | 35 MiB | 0 / 0 / 0 |
| `summaries` | sqlite | 554 | 53.7 ms | 99.7 ms | 121.7 ms | — | — | 0.31 | 27 MiB | 0 / 0 / 0 |
| `summaries` | postgres | 861 | 25.7 ms | 105.9 ms | 167.0 ms | — | — | 0.61 | 26 MiB | 0 / 0 / 0 |
| `slow-store` | sqlite | 124 | 250.9 ms | 390.8 ms | 472.8 ms | — | — | 0.79 | 29 MiB | 0 / 0 / 0 |
| `slow-store` | postgres | 783 | 30.5 ms | 110.9 ms | 164.8 ms | — | — | 0.29 | 28 MiB | 0 / 0 / 0 |
| `large-payload` | sqlite | 525 | 28.9 ms | 50.3 ms | 60.5 ms | — | 27.2 ms / 45.1 ms | 0.77 | 64 MiB | 0 / 0 / 0 |
| `large-payload` | postgres | 702 | 17.1 ms | 53.8 ms | 78.9 ms | — | 15.3 ms / 48.2 ms | 0.99 | 62 MiB | 0 / 0 / 0 |

### Store phases (cumulative over each scenario's process, warmup included)

| Scenario | Backend | `namespace_resolve` wait mean / p95 ≤ / max | `namespace_resolve` query mean | `budget_charge` wait mean / max | `budget_charge` query mean / max | `usage_summary` query mean / max | Connections opened |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `steady-buffered` | sqlite | 29.0 ms / 100.0 ms / 136.7 ms | 0.110 ms | 28.2 ms / 168.4 ms | 0.978 ms / 26.1 ms | — | 0 |
| `steady-buffered` | postgres | 0.596 ms / 2.5 ms / 41.8 ms | 0.588 ms | 0.300 ms / 6.4 ms | 36.9 ms / 360.1 ms | — | 111 |
| `steady-streamed` | sqlite | 12.8 ms / 50.0 ms / 79.3 ms | 0.232 ms | 9.8 ms / 77.8 ms | 1.5 ms / 7.1 ms | — | 0 |
| `steady-streamed` | postgres | 2.2 ms / 25.0 ms / 40.3 ms | 1.6 ms | 0.378 ms / 18.4 ms | 4.4 ms / 43.9 ms | — | 488 |
| `burst` | sqlite | 20.0 ms / 50.0 ms / 253.3 ms | 0.133 ms | 84.0 ms / 368.6 ms | 1.1 ms / 7.1 ms | — | 0 |
| `burst` | postgres | 19.6 ms / 50.0 ms / 47.9 ms | 4.9 ms | 40.2 ms / 106.4 ms | 25.2 ms / 144.2 ms | — | 619 |
| `summaries` | sqlite | 28.2 ms / 100.0 ms / 118.3 ms | 0.081 ms | 24.4 ms / 114.8 ms | 0.867 ms / 5.5 ms | 0.572 ms / 1.3 ms | 0 |
| `summaries` | postgres | 1.2 ms / 2.5 ms / 47.9 ms | 0.865 ms | 0.884 ms / 12.2 ms | 33.7 ms / 295.7 ms | 0.797 ms / 10.9 ms | 118 |
| `slow-store` | sqlite | 132.1 ms / 1000.0 ms / 516.7 ms | 0.144 ms | 116.1 ms / 671.9 ms | 1.5 ms / 7.6 ms | 38.7 ms / 48.6 ms | 0 |
| `slow-store` | postgres | 2.8 ms / 10.0 ms / 48.5 ms | 2.0 ms | 2.2 ms / 26.5 ms | 31.8 ms / 271.0 ms | 106.7 ms / 167.7 ms | 184 |
| `large-payload` | sqlite | 13.7 ms / 50.0 ms / 51.5 ms | 0.167 ms | 12.7 ms / 52.7 ms | 1.0 ms / 5.3 ms | — | 0 |
| `large-payload` | postgres | 0.214 ms / 0.050 ms / 12.9 ms | 1.0 ms | 0.027 ms / 6.8 ms | 17.7 ms / 117.7 ms | — | 53 |

### Management summaries and the background index

| Scenario | Backend | Summary req/s | Summary p50 / p95 | Usage rows at end | Store bytes at end | Accepted (incl. warmup) | Index enqueued / appended | Dropped at the queue bound | Index queue max depth | Index wait mean / max |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `steady-buffered` | sqlite | — | — | 9300 | 5906008 | 9300 | 9300 / 9300 | 0 | 62 | 14.7 ms / 84.8 ms |
| `steady-buffered` | postgres | — | — | 7214 | 1957888 | 9300 | 7214 / 7214 | 2086 | 256 | 388.0 ms / 486.6 ms |
| `steady-streamed` | sqlite | — | — | 1864 | 4554304 | 1864 | 1864 / 1864 | 0 | 31 | 7.3 ms / 58.9 ms |
| `steady-streamed` | postgres | — | — | 1864 | 573440 | 1864 | 1864 / 1864 | 0 | 18 | 2.4 ms / 30.0 ms |
| `burst` | sqlite | — | — | 3200 | 4820616 | 3200 | 3200 / 3200 | 0 | 119 | 65.4 ms / 215.1 ms |
| `burst` | postgres | — | — | 3200 | 884736 | 3200 | 3200 / 3200 | 0 | 202 | 106.8 ms / 258.2 ms |
| `summaries` | sqlite | 75.6 | 50.2 ms / 101.3 ms | 6200 | 5361216 | 6200 | 6200 / 6200 | 0 | 40 | 13.8 ms / 48.0 ms |
| `summaries` | postgres | 1227.2 | 3.2 ms / 4.5 ms | 3252 | 1130496 | 6200 | 3252 / 3252 | 2948 | 256 | 552.7 ms / 755.6 ms |
| `slow-store` | sqlite | 18.0 | 229.2 ms / 342.3 ms | 206200 | 29576864 | 6200 | 6200 / 6200 | 0 | 60 | 71.6 ms / 367.0 ms |
| `slow-store` | postgres | 36.0 | 110.9 ms / 141.2 ms | 202141 | 34496512 | 6200 | 2141 / 2141 | 4059 | 256 | 901.7 ms / 1426.1 ms |
| `large-payload` | sqlite | — | — | 1864 | 4578904 | 1864 | 1864 / 1864 | 0 | 23 | 8.3 ms / 31.0 ms |
| `large-payload` | postgres | — | — | 1759 | 532480 | 1864 | 1759 / 1759 | 105 | 256 | 280.4 ms / 447.3 ms |

### Reading it

- **Reconciliation held everywhere.** Every repetition of every scenario on
  both backends offered exactly the scale's count, accepted all of it, shed and
  errored nothing, and settled one usage record per accepted request
  (`usage_records.missing = 0`). The one `error=1` under `namespace_write` in
  `slow-store` is the second boot of that scenario's process hitting the
  insert-only `put_namespace` with a `Duplicate`, which boot ignores by design.
- **Repetitions agree to within the host's noise.** The three p50s per scenario
  sit within roughly ±15% on SQLite (`steady-buffered`: 49.0 / 63.0 / 53.2 ms)
  and ±5% on Postgres (28.5 / 27.8 / 28.1 ms). The negative overhead on
  Postgres `steady-streamed` (−0.6 ms at p50) is that noise: a stream's latency
  is the fake upstream's pacing, and the gateway's contribution there is below
  what this host resolves.
- **The controls are near zero.** The fake upstream answers a buffered request
  in well under a millisecond over loopback, so `overhead_ms` is, to within
  noise, the gateway's whole latency; where a control exists, read the
  request-path p50 as the overhead.

What the phases say, on this host:

- **On SQLite the acquire wait *is* the Store's cost, and it is most of the
  request.** In `steady-buffered` at 32 concurrency, `namespace_resolve` waits
  29.0 ms for the connection and runs in 0.11 ms; `budget_charge` waits 28.2 ms
  and runs in 0.98 ms. Two waits per request add to ~57 ms, which is the 53 ms
  p50 and the 52.9 ms overhead against the control. The connection mutex behind
  `spawn_blocking` is the queue [#463](https://github.com/Litvue/axond/issues/463)
  bounds; the wait, not the query, is what it must move.
- **On SQLite a large index turns that wait into a cliff.** `slow-store` (200 000
  seeded rows, 4 readers) puts `usage_summary` at 38.7 ms of held connection per
  call, and inference's `namespace_resolve` wait rises to 132 ms mean with a p95
  in the 250–1000 ms bucket: throughput falls from 581 to 124 req/s and p99
  from 106 to 473 ms. Over a small index (`summaries`) the same readers cost
  almost nothing (554 req/s, resolve wait 28 ms). The coupling is the held
  connection, not the number of summaries. [#464](https://github.com/Litvue/axond/issues/464)
  breaks that coupling: `summarize_usage` opens a second, read-only connection
  (`SQLITE_OPEN_READ_ONLY` plus `PRAGMA query_only=ON`) to the same WAL file
  and has its own dispatch slot. The reader attaches to the writer's file path
  from `PRAGMA database_list` as a `mode=ro` URI only when extra keys that stay
  valid on a second connection (`vfs`, `cache`, `psow`, `immutable`) are
  present. A writer URI `mode=rw` / `mode=rwc` cannot make the reader fail to
  boot. `nolock=1` and a non-WAL journal (`PRAGMA journal_mode` after the WAL
  request) keep summaries on the writer: SQLite forbids a second unlocked
  connection, and WAL is what lets a reader scan while the writer commits.
  `EXPLAIN QUERY PLAN` of the summary `SELECT` is `SEARCH axond_store_usage USING INDEX axond_store_usage_ns_period (namespace=? AND period=?)`.
  That is the `(namespace, period)` index, not a covering
  `(namespace, period, model, status)` index. A covering index would speed the
  fold (29 ms vs 38 ms at 200 000 rows in a release microbench) and would add a
  second b-tree insert on every usage append. The reader lane is what keeps
  the 38 ms fold off admits and charges. `:memory:` stores have no second
  connection; those summaries still share the writer. `Config::load` refuses
  `:memory:`, so a production store always has the reader. The unit test
  `file_store_summaries_run_beside_a_held_writer_dispatch` holds the writer slot
  and still completes a summary.
- **On Postgres the pool isolates inference from summaries, and the charge is
  the cost.** `slow-store` leaves `namespace_resolve` at 2.8 ms wait / 2.0 ms
  query and inference at 783 req/s while each summary holds a session for
  107 ms. What Postgres pays instead is `budget_charge`: 0.3 ms of wait and
  **36.9 ms of query** (max 360 ms) in `steady-buffered`, against 0.6 ms for
  the resolve. Every request in this harness charges the same namespace and
  period, so the 32 concurrent `UPDATE`s serialise on one budget row inside the
  database; the query time halves at 16 concurrency (`large-payload`, 17.7 ms).
  This is a real single-tenant-hot-row cost and **no subissue of #459 owns
  it** — it is recorded here so the epic can decide whether to.
- **On Postgres bursts paid for connects while idle retention was 8.**
  The #462 full-local `burst` run opened 619 sessions for 3 200 requests in
  waves of 128: the pool held 32 and kept 8 idle, so every wave reconnected ~24
  sessions, and `namespace_resolve` waited 19.6 ms mean (0.6 ms in
  `steady-buffered`) with the connect inside the wait. `steady-streamed` showed
  the same churn at low rate (488 connects for 1 864 requests): a stream holds
  no session while it streams, so the idle cap drained between the resolve and
  the charge. That was [#465](https://github.com/Litvue/axond/issues/465)'s
  mechanism.

  The pool unit test
  `store::postgres::tests::postgres_pool_reports_cold_warm_burst_and_recovery`
  repeats the same shape against loopback Postgres, 12 overlapping checkouts,
  three waves plus a 100 ms recovery pause. Before idle retention matched
  `POOL_SIZE`, each wave after the first opened 4 sessions (12 minus 8) and burst
  checkout p95 was 74.6 ms (the reconnects). After retaining 32 idle sessions,
  those waves open 0 extra sessions. One loopback run (Postgres 16,
  `sslmode=disable`) printed:

  - cold: p95 38.4 ms, p99 43.3 ms, opened 12, reused 1, live 12, idle 0
  - warm: p95 10.3 µs, p99 63.6 µs, opened 12, idle 12
  - burst (3 × 12): p95 57 µs, p99 73 µs, opened-per-wave `[0, 0, 0]`
  - recovery: p95 69 µs, p99 73 µs, opened 0, discarded 0, idle 12, available 32

  Cold p95 is connect time and moves with host load. Session counts and
  opened-per-wave are the invariant: discarded 0, live plus idle at most 32,
  available 32 after return. Burst p95 fell from 74.6 ms to microseconds.
  Recovery after the pause opened 0. The deployment connection budget is still
  32 per replica. Fleet budget is `N × 32`. A controlled-runner `burst` replay
  should now show `connections_opened` tracking pool fill, not wave size.
- **On Postgres the background index drops under steady load; on SQLite it does
  not.** The usage-index queue (bound 256) reached its bound in every buffered
  Postgres scenario and dropped 22% of `steady-buffered`'s records, 48% of
  `summaries`', and 65% of `slow-store`'s before they were indexed, with records
  waiting 0.4–0.9 s to be taken. SQLite's dedicated index thread never exceeded
  depth 119 and dropped nothing. Billing is unaffected — every accepted request
  settled its usage record, which is what the reconciliation asserts — but a
  Postgres deployment's management summaries under-count sustained load. The
  Postgres worker appends one row per event through the same pool the charges
  hold; at ~800 req/s it cannot keep pace. Whichever issue takes the index
  writer must be measured on `steady-buffered`/Postgres with
  *dropped at the queue bound* = 0 as the bar.
- **The large payload's cost is not visible in the Store phases.** At 16
  concurrency `large-payload`'s phases are what `steady-buffered` would be at
  that concurrency (SQLite resolve wait 13.7 ms; Postgres charge 17.7 ms), and
  its RSS is the payload (62–64 MiB against 26–28 MiB). Its overhead against the
  control (27 ms SQLite, 15 ms Postgres) is therefore mostly Store wait on this
  host; [#466](https://github.com/Litvue/axond/issues/466) is measured against
  the control with the Store phases held as the invariant, and needs the
  controlled runner to resolve its own effect above them.

## Recommended workloads and thresholds for the optimisation issues

Thresholds here are for a controlled-runner comparison as described above,
never for CI. Each is stated as the artifact field to read and the direction a
change must move it by more than the noise floor of the paired baseline runs.

| Issue | Workload | Read | Must hold |
| --- | --- | --- | --- |
| [#463](https://github.com/Litvue/axond/issues/463) bound SQLite work admission | `slow-store` and `burst`, SQLite | `store_evidence.operations.namespace_resolve.acquire_wait` (p95 ≤ and max), `budget_charge.acquire_wait`, `index_queue.wait`, and the request-path p99 | Acquire-wait p95 and max for `namespace_resolve` and `budget_charge` fall; `query_duration` for both does not rise; `usage_records.missing` stays 0; any new `saturated`-style outcome appears as a typed `503` counted under `rejected`, so the reconciliation still holds. A design that trades tail latency for dropped index writes fails on `usage_records.missing`. |
| [#464](https://github.com/Litvue/axond/issues/464) usage summaries at scale | `slow-store`, SQLite, with `seeded_usage_rows` raised (edit the scale or add a tier; the artifact records the row count and file size) | `operations.usage_summary.query_duration`, `summaries.latency_ms`, and — the interference — `operations.namespace_resolve.acquire_wait` while readers run, against the same scenario's `steady-buffered` sibling | `usage_summary.query_duration` mean and p95 fall at 200 000 rows and keep falling as rows grow; the `namespace_resolve.acquire_wait` gap between `slow-store` and `steady-buffered` narrows; summary results are byte-identical to the fold they replace on the parity fixtures the issue lists. |
| [#465](https://github.com/Litvue/axond/issues/465) Postgres burst connection churn | `burst`, Postgres, with the database at production network distance for the network-latency question; the pool unit test is the loopback mechanism check | `store_evidence.connections_opened`, `operations.namespace_resolve.acquire_wait` p95, request-path p95/p99 per wave, and `axond.store.operations{outcome="saturated"}`; pool unit test cold/warm/burst/recovery p95/p99 plus `opened`/`reused`/`discarded`/`live`/`idle` | `connections_opened` per wave falls to the configured idle retention rather than tracking wave size; `namespace_resolve.acquire_wait` p95 no longer carries a connect; no `saturated` outcome appears at the scale the pool is sized for; the session cap the design states is not exceeded (32 per replica, `N × 32` in the fleet, read live+idle from `axond.store.pool.sessions` and from `pg_stat_activity`). The pool unit test on this change: burst opened-per-wave `[0,0,0]`, discarded 0, burst p95 74.6 ms to microseconds. |
| [#466](https://github.com/Litvue/axond/issues/466) payload clones across credential attempts | `large-payload`, either backend (the Store is the control here, not the subject), plus a variant with several credentials configured so an attempt retries | `overhead_ms.p50` and `.p95` against the control, `resources.cpu_utilization`, `resources.rss_kib.peak`, and TTFT on the streamed sibling | Overhead p50/p95 and CPU per accepted request fall at 256 KiB with the Store phases unchanged; the wire-compatibility tests the issue lists still pass; no phase in `store_evidence` moves, which proves the saving was in the transport and not the Store. |
| Postgres index writer (no subissue yet; see the findings above) | `steady-buffered` and `slow-store`, Postgres | `store_evidence.index_queue.max_depth`, the *dropped at the queue bound* difference between accepted requests and `index_queue.depth_observations`, `index_queue.wait`, and `store.usage_rows` at the end against accepted requests | Dropped at the queue bound is 0 and `store.usage_rows` equals the accepted count; `index_queue.wait` mean falls below the request-path p50; `budget_charge.query_duration` does not rise, so the index did not buy its throughput by contending harder for the sessions the charges hold. |
