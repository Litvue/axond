# Public beta release status

Axond is publicly released. This file records where to find current release
evidence and known limitations; the reusable process lives in the
[maintainer release runbook](./docs/maintainers/releasing.md).

## Current evidence

| Criterion | Status | Evidence |
| --- | --- | --- |
| Required routes, failover, credential pools, identity, controls, telemetry, and durable usage are implemented | Met | Compatibility contract, ADRs, unit/integration tests, SDK compatibility, Store tests, Tier 0 gate, and stream soak. |
| Public source repository | Met | `https://github.com/Litvue/axond`. |
| Cross-platform release archives | Met | The [latest GitHub release](https://github.com/Litvue/axond/releases/latest) contains Linux x86-64 and ARM64 GNU/musl, Apple Silicon macOS, and Windows archives, checksums, SBOMs, keyless `.sig`/`.pem` signatures, and provenance. |
| Public OCI image | Met | The TypeScript gateway image at `ghcr.io/litvue/axond`, mirrored with the same digest to `ghcr.io/litvue/axond-typescript`, for each release is public, published as a `linux/amd64` + `linux/arm64` index, smoke-tested per architecture, signed keylessly, and attested: provenance on every manifest including the index, SBOM on the per-architecture children. |
| Historical crates.io package | Retained | 0.7.1 was the last Rust version. crates.io no longer receives versions; existing versions are not yanked. |
| Deployment/configuration/operator documentation | Met | Task-oriented documentation index, executable examples, references, and runbooks under `docs/`. |

Current artifacts and workflow evidence are available from the
[GitHub release](https://github.com/Litvue/axond/releases/latest) and
[Release workflow history](https://github.com/Litvue/axond/actions/workflows/release-please.yml).

## Verify as an adopter

Release artifacts contain the Bun-compiled TypeScript gateway.

Verify binaries and the OCI image with the commands in
[Installation and verification](./docs/installation.md).

The release workflow builds every artifact from the immutable tag, requires the
tagged commit's `CI Success`, smoke-tests the published image before signing,
and never publishes to crates.io.

## Known limitations

- Released platforms are the ones in
  [the supported platform matrix](./docs/compatibility.md#supported-platforms):
  Linux x86-64 and ARM64 (glibc and musl), Apple Silicon macOS, and
  Windows x86-64, with `linux/amd64` and `linux/arm64` images. Other platforms
  build from source.
- Musl archives are not static; Alpine requires `libstdc++`/`libgcc`
  (`apk add --no-cache libstdc++ libgcc`).
- Release archives carry keyless cosign signatures; verify with the
  `cosign verify-blob` command in the installation guide.
- `/readyz` reports a serving, boot-validated process; it does not continuously
  probe providers or the Store.
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

The required pull-request CI aggregate covers:

- formatting, clippy, workspace tests, and rustdoc with warnings denied;
- deterministic unit/integration tests and short SQLite request-path fault
  and endurance coverage;
- provider SDK compatibility;
- dependency and license policy;
- static musl build and SQLite boot-and-serve gate;
- a boot-and-serve smoke of every released binary target, on a runner of that
  target's own platform;
- Docker image and Compose quickstart smoke tests.

Recovery, rollout, and stateful-endurance qualification harnesses were retired
with the tier matrix (ADR 0063 / #427). Request-path qualification is SQLite +
`/ns/{ns}/v1`.

Release jobs add archive/image SBOMs, provenance attestations, cosign signing,
native per-architecture archive smoke — the same binary smoke against the exact
archived binary, on a runner of that archive's own platform — published-image
smoke, and multi-architecture index platform assertions.
Repairing an existing tag runs those gates against that tag's artifacts; which
paths the tag must contain, and how to repair a tag that predates one of them,
are in the
[runbook](./docs/maintainers/releasing.md#repair-an-existing-tag).
