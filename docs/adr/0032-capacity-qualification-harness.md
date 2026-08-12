# 32. Deterministic capacity qualification and its result artifact

Date: 2026-08-12

## Status

Accepted

Extends the black-box harness of
[ADR 0014](./0014-compatibility-and-soak-harness.md) from *does it stay correct
under load* to *what does it cost to serve*, and gives the bounds of
[ADR 0030](./0030-request-bounds-and-load-shedding.md) numbers an operator can
size against.

## Context

Axond has a soak suite that proves a long stream does not leak and an admission
suite that proves a bound sheds. Neither answers the question an operator asks
first: how much traffic does one replica serve, at what latency, holding how much
memory, how many sockets, and how much CPU — and where is the point past which
those change. Every answer to that question so far has been anecdotal: a number
someone measured once, on a machine nobody recorded, from a build nobody hashed.

That is worse than having no number. A capacity figure with no provenance gets
quoted into a sizing decision, and the deployment it sizes is the one that finds
out the figure came from a debug build on a laptop.

The obvious instinct — assert throughput and latency in CI — fails for a
different reason. CI runs on shared, noisy, oversubscribed runners; a p99 bound
tight enough to catch a real regression flakes weekly, and a flaky gate is a gate
that gets deleted or, worse, ignored. So the measurements that matter for sizing
are exactly the measurements CI cannot enforce.

## Decision

A Rust-native capacity harness, driven by a committed manifest, producing a
machine-readable result artifact per profile run.

**The inputs are committed.** `qualification/capacity/manifest.toml` defines
every profile — workload, description, cancellation cadence, a reduced and a
heavy scale, and thresholds — as data. The driver implements the workloads;
the manifest chooses the scales. A run that cannot be described by the committed
manifest cannot be run, so a result can always be reproduced from the repository.

**The subject is a real process.** Each profile boots the `axond` binary against
the deterministic fake upstream of ADR 0014, over loopback, with the transport
and admission bounds written out in full rather than defaulted, so a later change
to a shipped default cannot silently move a qualification result. Five workloads
are covered: buffered, streaming, mixed (both wire families, four routes, two
providers, two credentials per provider), response-size (1 KiB / 32 KiB /
256 KiB bodies), and cancellation (every second caller hangs up mid-answer).

**The driver is closed-loop.** It holds a fixed concurrency and sends a fixed
*number* of requests rather than pushing a fixed arrival rate. An open-loop rate
above what the machine can serve measures the machine's collapse instead of the
gateway's behaviour, and it is not reproducible across runners. The offered rate
is therefore a *result*, recorded as such.

**The artifact carries its own provenance.** Every run writes JSON under
`target/capacity/<tier>/<profile>.json` containing offered and accepted
throughput, p50/p95/p99 latency, TTFT and stream lifetime for streamed requests,
RSS, CPU seconds, socket counts, driver-side occupancy, rejection and error
counts by status and typed error, usage-record counts and drops by status,
upstream request and stream counts — and the SHA-256 of the binary, the
normalised config, the manifest, and every fixture, plus the toolchain, git
commit and dirty flag, and the host's kernel, CPU model, core count, and memory.
A number without that identity may not be compared with a number that has a
different one.

**Only environment-independent properties are hard failures.** The thresholds a
run is gated on are: every offered request accepted, nothing shed, no errors, one
usage record per admitted request, no upstream body still open once every client
is gone, and bounded resident-memory growth. Throughput, latency, TTFT, and CPU
are recorded and never asserted. They are the sizing evidence; the gate is
correctness under load.

**Both tiers are the same code.** The reduced tier runs inside
`cargo test --workspace`, in the CI `tests` lane, and uploads its artifacts. The
heavy tier is the identical driver, manifest, and assertions at a scale that
wants a runner to itself: `AXOND_CAPACITY=1`, run by the `Capacity` workflow on
dispatch and weekly. There is no second implementation to drift.

**One profile offers load at a time.** Both tiers live in one test binary, and
two of them driving two gateways on one host would measure each other's
contention while still producing an artifact that reads as an envelope. The
driver holds a process-wide lock across a run, and the heavy invocations pass
`--test-threads=1`, so the subject under measurement is one replica.

### State tier

Tier 0. The harness boots a config-only process — no Redis, no Postgres, no
control plane — and needs no service container, which is why it can precede
stateful serving. It qualifies the stateless request path *only*, and it is not
evidence about stateful serving, revision convergence, or any store-backed
control. Those need their own profiles once the surfaces exist.

## Consequences

- Sizing conversations get evidence with an identity attached. The published
  envelopes in [capacity qualification](../operations/capacity.md) name the
  hardware, build profile, and manifest they came from, and a reader who cannot
  match that provenance is told the numbers do not transfer.
- CI catches the regressions it can catch reliably, and no others. A change that
  leaks a socket per cancelled stream, drops a usage record under concurrency, or
  grows memory with the load fails on the pull request. A change that makes the
  gateway 30% slower does not — that shows up as a moved number in the artifacts,
  read by a human comparing runs with matching provenance.
- The recorded numbers come from a **debug build** in both tiers, because that is
  what `cargo test` builds. They are useful for comparing runs and for
  establishing the shape of the envelope; they understate release throughput
  substantially and must not be published as production figures.
- The fake upstream paces its own streams, so stream lifetime and TTFT measure
  the gateway's relay against a fixed synthetic model, not a provider. A real
  provider's time-to-first-token dominates the number an operator sees.
- Adding a workload means changing the driver, not just the manifest. That is
  deliberate: a manifest that could describe arbitrary load would be a load
  generator with a config file, and its results would not be reproducible from
  the repository.
- `/proc` is the source for RSS, CPU, and sockets. Off Linux the run still
  qualifies every other property and records the resource fields as absent
  rather than as zero, so a non-Linux run cannot look like a passing memory
  bound it never measured.
