# Public beta release status

Axond is publicly released. This file records where to find current release
evidence and known limitations; the reusable process lives in the
[maintainer release runbook](./docs/maintainers/releasing.md).

## Current evidence

| Criterion | Status | Evidence |
| --- | --- | --- |
| Required routes, failover, credential pools, identity, controls, telemetry, and durable usage are implemented | Met | Compatibility contract, ADRs, unit/integration tests, SDK compatibility, stateful tests, Tier 0 gate, and stream soak. |
| Public source repository | Met | `https://github.com/Litvue/axond`. |
| Cross-platform release archives | Met | The [latest GitHub release](https://github.com/Litvue/axond/releases/latest) contains Linux GNU/musl, Apple Silicon macOS, and Windows archives, checksums, SBOMs, and provenance. |
| Public OCI image | Met | The versioned `ghcr.io/litvue/axond` image for each release is public, smoke-tested, signed keylessly, and carries provenance/SBOM attestations. |
| crates.io workspace | Met | [`gateway-core`](https://crates.io/crates/gateway-core), [`gateway-transport`](https://crates.io/crates/gateway-transport), and [`axond`](https://crates.io/crates/axond) are published in dependency order. |
| Deployment/configuration/operator documentation | Met | Task-oriented documentation index, executable examples, references, and runbooks under `docs/`. |

Current artifacts and workflow evidence are available from the
[GitHub release](https://github.com/Litvue/axond/releases/latest) and
[Release workflow history](https://github.com/Litvue/axond/actions/workflows/release-please.yml).

## Verify as an adopter

```bash
cargo install axond --locked
cargo add gateway-core gateway-transport
```

Verify binaries and the OCI image with the commands in
[Installation and verification](./docs/installation.md).

The release workflow builds every artifact from the immutable tag, requires the
tagged commit's `CI Success`, smoke-tests the published image before signing,
and publishes crates.io last because registry versions are immutable.

## Known limitations

- The OCI image is currently `linux/amd64` only. Release archives cover Linux
  x86_64, Apple Silicon macOS, and Windows x86_64.
- `/readyz` reports a serving, boot-validated process; it does not continuously
  probe providers, Redis, or Postgres.
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

The required CI aggregate covers:

- formatting, clippy, workspace tests, and rustdoc with warnings denied;
- Redis/Postgres integration tests;
- provider SDK compatibility;
- dependency and license policy;
- crates.io package/publish dry runs;
- static musl build and hermetic Tier 0 boot;
- a boot-and-serve smoke of every released binary target, on a runner of that
  target's own platform;
- Docker image and Compose quickstart smoke tests.

Release jobs add archive/image SBOMs, provenance attestations, cosign signing,
the same binary smoke against the exact archived binary, published-image smoke,
and crates.io upload. Repairing an existing tag runs those gates against that
tag's artifacts; which paths the tag must contain, and how to repair a tag that
predates one of them, are in the
[runbook](./docs/maintainers/releasing.md#repair-an-existing-tag).
