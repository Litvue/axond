# Axond

[![CI](https://github.com/Litvue/axond/actions/workflows/ci.yml/badge.svg)](https://github.com/Litvue/axond/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/axond.svg)](https://crates.io/crates/axond)
[![docs.rs](https://docs.rs/axond/badge.svg)](https://docs.rs/axond)
[![release](https://img.shields.io/github/v/release/Litvue/axond)](https://github.com/Litvue/axond/releases/latest)
[![license](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](#license)

Axond is a stateless, single-binary, self-hosted **AI gateway**. Point OpenAI
or Anthropic clients at Axond to centralize provider credentials, stable model
aliases, failover, usage metering, budgets, rate limits, and telemetry.

> **Status: beta.** The supported routes and stability promises are explicit in
> the [compatibility contract](./docs/compatibility.md). Patch releases are
> upgrade-safe within `0.x`; documented breaking configuration changes require a
> minor release.

## What it provides

| Area | Capabilities |
| --- | --- |
| Provider wires | OpenAI chat completions, Responses, and embeddings; native Anthropic Messages; buffered and streamed requests. |
| Routing | Stable aliases, ordered target failover, per-target circuits, and weighted or round-robin credential pools. |
| Tenancy | Namespace-isolated provider credentials, explicit platform fallback, and bring-your-own-key deployments. |
| Inbound identity | Required static gateway keys, scoped minted tokens, optional in-gateway minting, issuance epochs, and precise JTI revocation. |
| Controls | Per-subject budgets, exact namespace-wide Redis/Postgres caps, and local or Redis-backed in-flight rate limits. |
| Operations | Atomic config reload, replica-local credential status, JSON logs, OTLP traces/metrics/logs, and durable Postgres usage. |
| Distribution | crates.io packages, checksummed and attested release binaries, and public keyless-signed, attested OCI images. |

Axond is passthrough-first: it rewrites only `model`, then forwards the caller's
native wire. It does not translate OpenAI payloads into Anthropic payloads or
vice versa. A mismatched alias is rejected before dispatch with a typed
`unsupported_wire` error.

## Run it in two minutes

The default Compose path pulls the public release image and runs without Redis
or Postgres. Placeholder provider credentials are sufficient for health,
readiness, catalogue, authentication, and typed-error checks.

```bash
git clone https://github.com/Litvue/axond.git
cd axond
cp ops/compose/env.example .env
docker compose up -d

curl http://localhost:8080/healthz
curl -H 'Authorization: Bearer quickstart-platform-key' \
  http://localhost:8080/v1/models
```

Expected probes:

```text
ok
{"data":[{"id":"gpt-4o",...}],"object":"list"}
```

To make a real provider request, replace the corresponding placeholder in
`.env`, restart the container, and call the gateway:

```bash
curl http://localhost:8080/v1/chat/completions \
  -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"Say hello in one word."}]}'
```

With placeholders, the request deliberately returns a typed provider or
transport error; it still proves the complete authenticated gateway path.
Keep `.env` until after teardown because Compose validates its required values
before every command:

```bash
docker compose down -v
```

See [Getting started](./docs/getting-started.md) for source builds, the stateful
Compose profile, and SDK examples.

## Install

The recommended path installs the checksum-verified prebuilt binary; it does not
download the repository or invoke the Rust compiler:

```bash
# Linux x86-64 or macOS Apple Silicon
curl --proto '=https' --tlsv1.2 -LsSf \
  https://raw.githubusercontent.com/Litvue/axond/main/install.sh | sh
```

PowerShell on Windows x86-64:

```powershell
irm https://raw.githubusercontent.com/Litvue/axond/main/install.ps1 | iex
```

The installers select the latest release, verify its SHA-256 sidecar, and put
`axond` under the current user's local application directory. Download and
inspect either script first if your policy forbids piped installers.

Other distribution paths:

```bash
# Cargo installs from source and compiles locally by design.
cargo install axond --locked

# Pull the current release image. There is intentionally no `latest` tag.
AXOND_VERSION=0.3.27 # x-release-please-version
docker pull "ghcr.io/litvue/axond:${AXOND_VERSION}"
```

Checksummed, attested prebuilt archives are published for Linux (`x86_64` and `aarch64`, GNU and
static musl each), macOS (`aarch64`), and Windows (`x86_64`). The OCI image is a
multi-architecture index covering `linux/amd64` and `linux/arm64`, so one pinned
digest deploys on either architecture. Production deployments should verify the
attestations and pin an image digest. See
[Installation and verification](./docs/installation.md).

## Point a client at Axond

Use `http://localhost:8080/v1` as the OpenAI base URL and an Axond gateway key
as the API key:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="quickstart-platform-key",
)
response = client.chat.completions.create(
    model="gpt-4o",
    messages=[{"role": "user", "content": "hello"}],
)
```

Anthropic clients use the same host and their native `x-api-key` behavior.
Detailed Python, TypeScript, streaming, Responses, embeddings, and Messages
examples are in the [client guides](./docs/index.md#connect-a-client).

## Choose a deployment

| Environment | Start here |
| --- | --- |
| Local evaluation | [Getting started](./docs/getting-started.md) |
| Docker Compose | [Compose guide](./docs/deployment/docker-compose.md) |
| Docker or Podman | [Container guide](./docs/deployment/container.md) |
| Linux VM / bare metal | [systemd guide](./docs/deployment/systemd.md) |
| Kubernetes | [Kubernetes guide](./docs/deployment/kubernetes.md) |
| ECS, Cloud Run, Container Apps, Nomad | [Managed-container contract](./docs/deployment/managed-containers.md) |
| Redis/Postgres-backed fleet | [Stateful backends](./docs/deployment/stateful-backends.md) |

Axond does not terminate inbound TLS. Put it behind a trusted reverse proxy or
load balancer, preserve streaming responses, and disable response buffering.
Use the [production checklist](./docs/deployment/production-checklist.md) before
exposing it outside a development network.

## State tiers

State tiers describe Axond's own dependencies, not provider egress.

| Tier | What it adds | Operational consequence |
| --- | --- | --- |
| **0 — config only** | Namespaces, aliases, provider keys, failover, credential pools, static/minted identity, stdout usage, hot reload, and optional per-replica controls. | No datastore. Health, circuits, and in-memory limits are replica-local. |
| **1 — Redis** | Exact shared budgets, namespace caps, cross-replica in-flight limiting, and precise token revocation. | Redis participates in admission. The default outage policy fails closed. |
| **2 — Postgres** | Durable usage rows, shared budgets/namespace caps, and precise revocation. | Requires schema ownership, migrations, backups, and boot-time connectivity. |

Configuration-owned namespaces, providers, aliases, prices, and credentials are
never overridden by a datastore. See the
[stateful deployment guide](./docs/deployment/stateful-backends.md) and
[ADR 0017](./docs/adr/0017-state-tiers-and-optional-backends.md).

Tiers describe dependencies; *operating modes* describe ownership. Stateless
remains the default, where TOML is the authority. The design for the opt-in
stateful mode — durable resources in Postgres, `/admin/v1` administration, and
inference still served from one immutable in-memory
snapshot — is [ADR 0027](./docs/adr/0027-stateless-and-stateful-operating-modes.md).

A stateful replica boots and administers: it opens the control plane and serves
`/admin/v1` — authenticated by the `[[admin_breakglass]]` credential until OIDC
lands — which is how durable desired state is written at all. What it cannot do
yet is *serve inference*: a published revision does not compile into a runtime
snapshot until convergence lands, so `/readyz` and every inference path answer
`503` per request instead of the process refusing to start, and `axond check
preflight` reports that same refusal on its `serving` line.

## Security model

- Every route except `/healthz` and `/readyz` requires an Axond credential.
- Provider keys, inbound keys, and DSNs are referenced by environment-variable
  name or an explicitly supported mounted file; secret values do not belong in
  TOML.
- At least one static gateway key is mandatory as a breakglass path even when
  minted-token verification is enabled.
- Configuration and dependencies are validated before the listener binds.
- Release binaries carry checksums plus provenance and SBOM attestations; the
  per-architecture images add SBOM attestations, and every published manifest
  (children and the multi-architecture index) is signed keylessly, attested for
  provenance, and verified in the release workflow. Archives are not
  cosign-signed.

Read the [deployment security model](./docs/security/deployment-model.md),
[minted-token guide](./docs/minted-token-guide.md), and latest
[security review](./docs/security-review-2026-08-05.md).

To report a vulnerability, follow [`SECURITY.md`](./SECURITY.md) — privately,
not in an issue. It also states which releases receive fixes.

## Documentation

The [documentation index](./docs/index.md) is organized by task:

- [Getting started](./docs/getting-started.md)
- [Installation and supply-chain verification](./docs/installation.md)
- [Configuration reference](./docs/configuration.md)
- [Deployments](./docs/index.md#deploy)
- [Troubleshooting](./docs/operations/troubleshooting.md)
- [Upgrades and rollback](./docs/operations/upgrades.md)
- [Observability and runbook](./docs/observability.md)
- [Compatibility contract](./docs/compatibility.md)
- [Architecture decisions](./docs/adr)

## Development

The Rust toolchain is pinned in
[`rust-toolchain.toml`](./rust-toolchain.toml). Run the core code, package, and
supply-chain gate with:

```bash
just check
```

Useful focused checks:

```bash
just quickstart-smoke  # build current source and exercise the Compose path
just compat            # OpenAI and Anthropic SDKs against a real local axond
just tier0              # prove config-only boot in a network-denied namespace
just soak               # long streaming soak, also available on demand in CI
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for contribution and compatibility-lock
guidance.

## Releases

Release-please maintains the changelog and workspace version. A release builds
six binary targets, publishes the `linux/amd64` and `linux/arm64` images plus the
multi-architecture index they form — booted on both architectures before it takes
the `<version>` tag — signs and attests artifacts, and publishes
`gateway-core`, `gateway-transport`, and `axond` to crates.io in dependency
order. Maintainer procedures are in the
[release runbook](./docs/maintainers/releasing.md).

## License

Dual-licensed under either [Apache-2.0](./LICENSE-APACHE) or
[MIT](./LICENSE-MIT) at your option.
