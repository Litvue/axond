# Axond

[![CI](https://github.com/Litvue/axond/actions/workflows/ci.yml/badge.svg)](https://github.com/Litvue/axond/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/axond.svg)](https://crates.io/crates/axond)
[![docs.rs](https://docs.rs/axond/badge.svg)](https://docs.rs/axond)
[![release](https://img.shields.io/github/v/release/Litvue/axond)](https://github.com/Litvue/axond/releases/latest)
[![license](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](#license)

Axond is a store-backed, single-binary, self-hosted **AI gateway**. Point
OpenAI or Anthropic clients at a namespace URL to centralize provider
credentials, usage metering, period budgets, and telemetry.

> **Status: beta.** The supported routes and stability promises are explicit in
> the [compatibility contract](./docs/compatibility.md). Patch releases are
> upgrade-safe within `0.x`; documented breaking configuration changes require a
> minor release. Product shape is
> [ADR 0063](./docs/adr/0063-stateful-only-namespaced-gateway.md).

## What it provides

| Area | Capabilities |
| --- | --- |
| Provider wires | OpenAI chat completions, Responses, and embeddings; native Anthropic Messages; buffered and streamed requests. |
| Models | Request `provider-id/model-id`. No `[[model]]` table, no alias failover. |
| Tenancy | API-created namespaces (file rows seeded at boot), namespace-isolated credentials, optional platform fallback. |
| Inbound identity | Exactly one deployment-wide static gateway key for `/api/v1` and `/ns/...`. Minted `axt1.` tokens are `401`. |
| Controls | Store-backed per-namespace period budgets; credential-pool rotation; per-replica admission. |
| Operations | Required SQLite or Postgres Store, JSON logs, OTLP traces/metrics/logs, optional usage sinks. |
| Distribution | crates.io packages, checksummed and attested release binaries, and public keyless-signed, attested OCI images. |

Axond is passthrough-first: it rewrites only `model`, then forwards the caller's
native wire. It does not translate OpenAI payloads into Anthropic payloads or
vice versa. A mismatched provider kind is rejected before dispatch with a typed
`unsupported_wire` error.

## Run it in two minutes

The default Compose path pulls the public release image and uses a temp SQLite
file. Placeholder provider credentials are sufficient for health, readiness,
catalogue, authentication, and typed-error checks. Inference is fail-closed
until a period budget is published.

```bash
git clone https://github.com/Litvue/axond.git
cd axond
cp ops/compose/env.example .env
docker compose up -d

curl http://localhost:8080/healthz
curl -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"limit_microdollars":1000000000000}' \
  -X PUT http://localhost:8080/api/v1/namespaces/platform/budgets/quickstart
curl -H 'Authorization: Bearer quickstart-platform-key' \
  http://localhost:8080/ns/platform/v1/models
```

Expected probes:

```text
ok
{"data":[{"id":"openai/gpt-4o",...}],"object":"list"}
```

To make a real provider request, replace the corresponding placeholder in
`.env`, restart the container, and call the gateway:

```bash
curl http://localhost:8080/ns/platform/v1/chat/completions \
  -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"model":"openai/gpt-4o","messages":[{"role":"user","content":"Say hello in one word."}]}'
```

With placeholders, the request deliberately returns a typed provider or
transport error; it still proves the complete authenticated gateway path.
Keep `.env` until after teardown because Compose validates its required values
before every command:

```bash
docker compose down -v
```

See [Getting started](./docs/getting-started.md) for source builds, the Postgres
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
AXOND_VERSION=0.6.1 # x-release-please-version
docker pull "ghcr.io/litvue/axond:${AXOND_VERSION}"
```

Checksummed, attested prebuilt archives are published for Linux (`x86_64` and `aarch64`, GNU and
static musl each), macOS (`aarch64`), and Windows (`x86_64`). The OCI image is a
multi-architecture index covering `linux/amd64` and `linux/arm64`, so one pinned
digest deploys on either architecture. Production deployments should verify the
attestations and pin an image digest. See
[Installation and verification](./docs/installation.md).

## Point a client at Axond

Use `http://localhost:8080/ns/{namespace}/v1` as the OpenAI base URL and the
deployment gateway key as the API key. The request `model` is
`provider-id/model-id`:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/ns/platform/v1",
    api_key="quickstart-platform-key",
)
response = client.chat.completions.create(
    model="openai/gpt-4o",
    messages=[{"role": "user", "content": "hello"}],
)
```

Anthropic clients use `http://localhost:8080/ns/{namespace}` (they append
`/v1/messages`) and their native `x-api-key` behavior. Detailed Python,
TypeScript, streaming, Responses, embeddings, and Messages examples are in the
[client guides](./docs/index.md#connect-a-client).

## Choose a deployment

| Environment | Start here |
| --- | --- |
| Local evaluation | [Getting started](./docs/getting-started.md) |
| Azure Container Apps | [ACA production guide](./docs/deployment/azure-container-apps.md) |
| Kubernetes | [Kubernetes guide](./docs/deployment/kubernetes.md) |
| Docker / Compose | [Compose](./docs/deployment/docker-compose.md) · [container](./docs/deployment/container.md) |
| Linux VM / bare metal | [systemd guide](./docs/deployment/systemd.md) |
| ECS, Cloud Run, Nomad | [Managed-container contract](./docs/deployment/managed-containers.md) |
| Postgres HA Store | [Store backends](./docs/deployment/stateful-backends.md) |

Axond does not terminate inbound TLS. Pin a GHCR digest, keep keys in a secret
store, and mount TOML for structure. Follow the
[production checklist](./docs/deployment/production-checklist.md) before exposing
it.

## Store

Boot requires `[storage]`. SQLite WAL is the single-replica Store; Postgres is
the HA Store. Namespaces, period budgets, and the usage index live there.
Redis/in-memory budget backends, `mode`, `[control_plane]`, and `/admin/v1` are
withdrawn. See the [configuration reference](./docs/configuration.md#state-tiers)
and [ADR 0063](./docs/adr/0063-stateful-only-namespaced-gateway.md).

## Security model

- Every route except `/healthz` and `/readyz` requires the deployment gateway
  key.
- Provider keys, inbound keys, and DSNs are referenced by environment-variable
  name or an explicitly supported mounted file; secret values do not belong in
  TOML.
- Exactly one static `[[gateway_key]]` is required. Minted tokens are not
  inbound identity.
- Configuration and the Store connection are validated before the listener
  binds.
- Release binaries carry checksums plus provenance and SBOM attestations; the
  per-architecture images add SBOM attestations, and every published manifest
  (children and the multi-architecture index) is signed keylessly, attested for
  provenance, and verified in the release workflow. Archives are not
  cosign-signed.

Read the [deployment security model](./docs/security/deployment-model.md) and
latest [security review](./docs/security-review-2026-08-05.md).

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
just tier0              # prove a temp SQLite file boots and serves /ns/{ns}/v1
just soak               # long streaming soak, also available on demand in CI
```

See [CONTRIBUTING.md](./CONTRIBUTING.md) for contribution and compatibility-lock
guidance.

## Releases

Release-please maintains the changelog and workspace version. A release builds
six binary targets, publishes the `linux/amd64` and `linux/arm64` images plus the
multi-architecture index they form — booted on both architectures before it takes
the `<version>` tag — signs and attests artifacts, and publishes the `axond`
binary crate to crates.io. Maintainer procedures are in the
[release runbook](./docs/maintainers/releasing.md).

## License

Dual-licensed under either [Apache-2.0](./LICENSE-APACHE) or
[MIT](./LICENSE-MIT) at your option.
