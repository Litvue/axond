# Public beta release status

Axond `v0.3.1` is publicly released. This file records the current release
evidence and known limitations; the reusable process lives in the
[maintainer release runbook](./docs/maintainers/releasing.md).

## Current evidence

| Criterion | Status | Evidence |
| --- | --- | --- |
| Required routes, failover, credential pools, identity, controls, telemetry, and durable usage are implemented | Met | Compatibility contract, ADRs, unit/integration tests, SDK compatibility, stateful tests, Tier 0 gate, and stream soak. |
| Public source repository | Met | `https://github.com/Litvue/axond`. |
| Cross-platform release archives | Met | `v0.3.1` release contains Linux GNU/musl, Apple Silicon macOS, and Windows archives, checksums, SBOMs, and provenance. |
| Public OCI image | Met | `ghcr.io/litvue/axond:0.3.1` is public, smoke-tested, signed keylessly, and carries provenance/SBOM attestations. |
| crates.io workspace | Met | `gateway-core`, `gateway-transport`, and `axond` `0.3.1` were published in dependency order. |
| Deployment/configuration/operator documentation | Met | Task-oriented documentation index, executable examples, references, and runbooks under `docs/`. |

The successful release repair run is
<https://github.com/Litvue/axond/actions/runs/31547811397>.

## Verify as an adopter

```bash
cargo install axond --version 0.3.1 --locked
cargo add gateway-core@0.3.1 gateway-transport@0.3.1
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
- Responses continuations pin to the first configured target. Axond does not
  persist response-ID-to-target ownership, so it cannot recover affinity for a
  continuation originally served by a later failover target.
- The server does not yet install an application-level SIGTERM drain; deployment
  infrastructure must remove replicas from traffic before termination.

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
- Docker image and Compose quickstart smoke tests.

Release jobs add archive/image SBOMs, provenance attestations, cosign signing,
published-image smoke, and crates.io upload.
