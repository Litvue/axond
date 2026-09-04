# Public beta release status

Axond is publicly released. This file records where to find current release
evidence and known limitations; the reusable process lives in the
[maintainer release runbook](./docs/maintainers/releasing.md).

## Current evidence

| Criterion | Status | Evidence |
| --- | --- | --- |
| Required routes, failover, credential pools, identity, controls, telemetry, and durable usage are implemented | Met | Compatibility contract, ADRs, unit/integration tests, SDK compatibility, stateful tests, Tier 0 gate, and stream soak. |
| Blob-backed flat-namespace stateful-v2 qualification | Pending | ADR 0062 is accepted but not implemented. PostgreSQL stateful-v1 records are historical compatibility evidence and cannot satisfy this gate. |
| Public source repository | Met | `https://github.com/Litvue/axond`. |
| Cross-platform release archives | Met | The [latest GitHub release](https://github.com/Litvue/axond/releases/latest) contains Linux x86-64 and ARM64 GNU/musl, Apple Silicon macOS, and Windows archives, checksums, SBOMs, and provenance. |
| Public OCI image | Met | The versioned `ghcr.io/litvue/axond` image for each release is public, published as a `linux/amd64` + `linux/arm64` index, smoke-tested per architecture, signed keylessly, and attested: provenance on every manifest including the index, SBOM on the per-architecture children. |
| crates.io package | Met | [`axond`](https://crates.io/crates/axond) is published as the gateway binary. `gateway-core` and `gateway-transport` stay unpublished workspace members. |
| Deployment/configuration/operator documentation | Met | Task-oriented documentation index, executable examples, references, and runbooks under `docs/`. |

Current artifacts and workflow evidence are available from the
[GitHub release](https://github.com/Litvue/axond/releases/latest) and
[Release workflow history](https://github.com/Litvue/axond/actions/workflows/release-please.yml).

## Verify as an adopter

```bash
cargo install axond --locked
```

Verify binaries and the OCI image with the commands in
[Installation and verification](./docs/installation.md).

The release workflow builds every artifact from the immutable tag, requires the
tagged commit's `CI Success`, smoke-tests the published image before signing,
and publishes crates.io last because registry versions are immutable.

## Known limitations

- Released platforms are the ones in
  [the supported platform matrix](./docs/compatibility.md#supported-platforms):
  Linux x86-64 and ARM64 (glibc and static musl), Apple Silicon macOS, and
  Windows x86-64, with `linux/amd64` and `linux/arm64` images. Other platforms
  build from source.
- Release archives are verified by checksum and GitHub attestation; keyless
  cosign signatures cover the published image manifests only.
- `/readyz` reports a serving, boot-validated process; it does not continuously
  probe providers, Redis, or Postgres.
- The implemented stateful control plane is PostgreSQL-backed and is not the
  accepted production target. [ADR 0062](./docs/adr/0062-blob-backed-flat-namespace-control-plane.md)
  selects a blob-backed flat-namespace stateful-v2 design; it is not implemented
  or qualified yet, and the current PostgreSQL qualification cohort must not be
  presented as evidence for it.
- Cross-provider request translation is intentionally not supported. OpenAI and
  Anthropic aliases must use their native wire families.
- Every `/v1/responses` request, initial calls included, pins to the alias's
  first configured target and first configured credential, so the route has no
  failover and no credential rotation. This keeps continuations reachable
  without response-ID-to-target state; the cost is that a first-target outage or
  exhausted first key is returned to the caller. Changing an alias's target
  order or credential pool order strands response IDs created under the previous
  order.
- Shutdown is bounded, not unlimited: `SIGTERM` fails readiness, keeps admitting
  for `shutdown.drain_grace_ms`, then gives admitted requests
  `shutdown.deadline_ms`. A stream still open at the deadline is cut — settled as
  `client_cancelled` up to the last relayed token, not silently completed — and
  the supervisor's stopping timeout must exceed the sum of the configured bounds
  or buffered usage records are lost to a `SIGKILL`.

None of these limitations is hidden by an optimistic health response or silent
fallback. The compatibility contract and deployment guides describe their
operational consequences.

## Release gates

`CI Success` is a software-change gate, not a production-qualification gate.
It can be green while the blob-backed stateful-v2 release gates above remain
pending; skipped PostgreSQL stateful-v1 lanes do not satisfy or replace them.

The required pull-request CI aggregate covers:

- formatting, clippy, workspace tests, and rustdoc with warnings denied;
- deterministic unit/integration tests and short SQLite request-path fault
  and endurance coverage;
- provider SDK compatibility;
- dependency and license policy;
- crates.io package/publish dry runs;
- static musl build and SQLite boot-and-serve gate;
- a boot-and-serve smoke of every released binary target, on a runner of that
  target's own platform;
- Docker image and Compose quickstart smoke tests.

Kubernetes stateful overlay drills are excluded from pull requests, pushes,
merge queues, and schedules. They remain manually available only when
`run_legacy_postgres_qualification=true` is explicitly supplied. Recovery,
rollout, and stateful-endurance qualification harnesses were retired with the
tier matrix (ADR 0063 / #427). Request-path qualification is SQLite +
`/ns/{ns}/v1` and does not require Redis.

Release jobs add archive/image SBOMs, provenance attestations, cosign signing,
native per-architecture archive smoke — the same binary smoke against the exact
archived binary, on a runner of that archive's own platform — published-image
smoke, multi-architecture index platform assertions, and crates.io upload.
Repairing an existing tag runs those gates against that tag's artifacts; which
paths the tag must contain, and how to repair a tag that predates one of them,
are in the
[runbook](./docs/maintainers/releasing.md#repair-an-existing-tag).
