# 48. Fault qualification: one process per row, ceilings only, evidence per row

Date: 2026-08-13

## Status

Accepted

Amended by [ADR 0063](./0063-stateful-only-namespaced-gateway.md): provider and
transport rows run on SQLite + `/ns/{ns}/v1`. Redis budget and rate-limit rows
skip because those backends are withdrawn, not because of a missing
tier-matrix service.

Third harness in the qualification programme of axond #156, after the capacity
harness of [ADR 0033](./0033-capacity-qualification-harness.md) (*what does it
cost to serve*) and the recovery contract of
[ADR 0037](./0037-recovery-qualification-harness.md) (*what happens when the
control plane goes away*). It qualifies the behaviour the transport bounds of
[ADR 0028](./0028-transport-phase-bounds.md), the bounded status contract of
[ADR 0031](./0031-bounded-status-contract.md), and the state-tier outage posture
of [ADR 0027](./0027-stateless-and-stateful-operating-modes.md) promise when a
dependency misbehaves.

## Context

Axond's fault behaviour is the part an operator meets first and trusts least:
what the caller is told when a provider answers 429, when DNS does not resolve,
when headers never arrive, when a stream goes silent after the answer has begun,
when Redis disappears mid-request. Each of those paths has unit tests, and each
is documented, but the visible behaviour of the shipped binary under a real
injected fault has never been observed as a set.

That matters because the interesting claims are cross-cutting. "A provider
timeout is a 504 with `upstream_timeout`" is a routing claim; "and it releases
the upstream body, and settles exactly one usage record, and exports an attempt
span, and puts no provider endpoint in the caller's answer" is four more claims
in four other subsystems, and no unit test sees all five at once. A fault is
also where leakage happens: an error path is the code most likely to hand a
caller a URL, a DSN, or a credential, and the least likely to be read closely.

The instinct to assert this in the normal test suite fails the way ADR 0033
describes. Every one of these rows is a *timing* observation — a 600 ms header
bound, a severed pooled connection, an upstream released after the caller is
gone — and a shared CI runner that is running twenty other tests in parallel
will move all of them.

## Decision

A committed fault matrix, driven row by row against the real binary, writing a
machine-readable evidence artifact per row.

**The matrix is committed as data.** `qualification/faults/manifest.toml`
declares 22 rows across four families: provider (429 and 5xx, each with and
without a standby target), transport (DNS, connect refused, TLS handshake,
response-header and buffered-body timeouts, idle stream before and after the
first byte, mid-stream truncation, oversized success and oversized provider
error), Redis (latency, fail-closed outage, fail-open outage, recovery), and
Postgres (the same four). Each row carries the fault to inject, the deadline it
must answer within, and the status, error type, attempt count, upstream request
count, usage record count, usage status, and metrics it must produce. A fault
the driver has no injector for cannot be declared, and a declared row that the
driver does not cover fails the suite, so the matrix and the code cannot drift.

**Every row gets its own process.** A row boots the `axond` binary with its own
configuration, its own fake upstream, its own OTLP collector, and — for a
state-tier row — its own TCP fault proxy in front of the real datastore, then
stops it and reads its drained output. Sharing a process across rows would let a
parked credential, a tripped breaker, or a pooled connection from one fault
decide the next row's verdict.

**The faults are injected, never simulated.** The provider rows answer from a
fake upstream that really returns 429; the DNS row points at a reserved
`.invalid` name; the connect row points at a loopback port with no listener; the
TLS row points at a port that answers the handshake with bytes that are not TLS;
the state-tier rows carry the real Redis or Postgres connection through a
proxy that adds latency or severs the connection. Nothing in the matrix is
satisfied by an in-process fake tier, for the reason ADR 0037 gives.

**Timing is a ceiling, never a floor.** Every timing assertion is "within" — the
deadline, the upstream release settle time, the outage window covering the
request it explains. The one exception is a latency row, which asserts only that
the latency it injected is *observable* at all. Elapsed times, outage durations,
and settle times are recorded in the artifact and never asserted as ranges: a
shared runner moves them, and a flaky qualification gate is one that gets
switched off.

**A record belongs to the request whose identity it carries.** Usage settlement
is detached from the response, so a priming request's record can land after the
measured request's. Records are attributed by the mint time inside the
request ID rather than by position or by a quiet interval, and a record whose
identity cannot be parsed is counted and fails its own verdict rather than being
silently dropped. A row that must settle *no* record is judged against the whole
of the process's drained output after it has exited and flushed: no interval is
long enough to prove a record is not coming, so none is used.

**The leakage scan never serialises what it looks for.** Each row scans the
caller's answer, the usage records, the process output, and the telemetry
exports for the endpoints, credentials, connection strings, and inbound key that
row was given, and a finding names the *surface and the label* of what leaked —
never the value. A transport row's needles are the endpoint it was actually
pointed at, so its evidence is about the endpoint it used rather than one it
never reached. The caller-facing surfaces must be free of endpoints too; an
operator's own logs may name the provider the operator configured.

**The lane is the gate.** The matrix runs only when `AXOND_FAULT_MATRIX=1`, in a
CI lane that runs the `faults` binary alone with `--test-threads=1`; the default
suite skips it with a message saying why. `--test-threads=1` alone would not do:
the workspace suite runs other binaries concurrently, which is exactly the
contention these rows cannot tolerate.

### Store

Provider and transport rows boot SQLite and hit `/ns/{ns}/v1`. Redis budget and
rate-limit rows always skip: those backends are withdrawn (ADR 0063), and the
skip reason is that withdrawal. Postgres HA rows skip unless
`AXOND_TEST_POSTGRES_DSN` is set. Required CI does not need Redis.

## Consequences

- The fault contract becomes checkable rather than asserted: each row's artifact
  under `target/faults/<row>.json` states the injected fault and timing, the
  classification, the deadline and elapsed time, attempts and upstream requests,
  cleanup and shutdown, the usage outcome, the telemetry, and the leakage scan,
  with a verdict per claim carrying its expected and observed values.
- Leakage regressions on error paths are caught by construction, on every row,
  rather than by whoever remembers to read an error body. The first run of the
  transport needles found one: a transport failure's caller-facing message
  carried the endpoint `reqwest` rendered into it, so the caller was told the
  provider's host, port, and path. The caller now gets the type and a static
  message; the endpoint stays in the operator's logs and on the attempt span.
- The lane costs a few minutes of serialised wall time and a Redis and a
  Postgres container. It is the price of timing observations that mean anything;
  the default suite is unaffected.
- Expectations live in a committed file, so changing what a fault does to a
  caller is a reviewed change to that file rather than an edit inside a test.
- The rows are honest about what they do not cover: control-plane outage and
  convergence belong to ADR 0037, capacity belongs to ADR 0033, and rollout,
  rollback, and long-duration soak are still unclaimed by any harness.
