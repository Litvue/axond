# Deployment overview

Axond is one stateless process. It reads TOML for structure, environment or
supported mounted files for secret material, and touches no datastore unless a
configured feature requires one.

## 5-minute quickstart

The public quickstart pulls the released OCI image:

```bash
git clone https://github.com/Litvue/axond.git
cd axond
cp ops/compose/env.example .env
docker compose up -d
curl --fail http://127.0.0.1:8080/healthz
curl --fail \
  -H 'Authorization: Bearer quickstart-platform-key' \
  http://127.0.0.1:8080/v1/models
docker compose down -v
```

For expected responses, a real provider call, the source-build overlay, and the
Redis/Postgres profile, follow [Getting started](./getting-started.md) and the
[Compose guide](./deployment/docker-compose.md).

## Choose an environment

| Environment | Guide | Best fit |
| --- | --- | --- |
| Docker Compose | [Compose](./deployment/docker-compose.md) | Evaluation, local integration, and reproducible stateful demos. |
| Docker or Podman | [Container](./deployment/container.md) | Existing container platforms and custom orchestration. |
| Linux VM / bare metal | [systemd](./deployment/systemd.md) | Static binary behind an existing proxy/load balancer. |
| Kubernetes | [Kubernetes](./deployment/kubernetes.md) | Horizontally scaled container deployment with ConfigMap/Secret delivery. |
| Managed containers | [Managed-container contract](./deployment/managed-containers.md) | ECS/Fargate, Cloud Run, Azure Container Apps, Nomad, and similar platforms. |

Review [Stateful backends](./deployment/stateful-backends.md) before enabling
Redis/Postgres and use the
[Production checklist](./deployment/production-checklist.md) before exposing a
deployment.

## What every environment must provide

- A readable TOML configuration selected by `AXOND_CONFIG` (default
  `axond.toml`).
- Every environment or file reference declared by the config.
- At least one static `[[gateway_key]]`; there is no keyless mode.
- Provider network egress.
- Port 8080 or a scalar `AXOND_SERVER__BIND` override.
- JSON stdout/stderr collection.
- TLS termination and streaming-compatible proxy behavior when exposed over a
  network.

Configuration, credential resolution, and initial backend connections complete
before the listener binds.

## Minimal working config

```toml
[server]
bind = "0.0.0.0:8080"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "platform"
provider = "openai"
env = "GW_PLATFORM_OPENAI_API_KEY"

[[gateway_key]]
env = "GW_INBOUND_PLATFORM_KEY"
namespace = "platform"

[[model]]
name = "gpt-4o"
targets = [
  { provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 2_500_000, output_microdollars_per_million = 10_000_000 } },
]
```

That is the serving floor. Credential pools, failover tuning, minted identity,
budgets, rate limits, revocation, usage sinks, telemetry, and reload watching
are optional.

## Running the static binary

Use [Installation](./installation.md#prebuilt-release-binary) to verify and
extract a release archive, then:

```bash
AXOND_CONFIG=/etc/axond/axond.toml \
GW_PLATFORM_OPENAI_API_KEY=sk-... \
GW_INBOUND_PLATFORM_KEY=replace-me \
  ./axond
```

For a managed Linux service, use the complete
[systemd guide](./deployment/systemd.md).

## Running the container image

The OCI image is public, distroless, non-root, signed, attested, and published as
a `linux/amd64` + `linux/arm64` index. It has no `latest` tag and ships no
config. Verify and pin a digest as described in the
[container guide](./deployment/container.md).

## Environment variables

| Variable | Required | Meaning |
| --- | --- | --- |
| `AXOND_CONFIG` | no | TOML path; defaults to `axond.toml`. |
| Names referenced by `env` / `dsn_env` | yes | Secret values selected by the config. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | no | OTLP/HTTP collector. Unset means JSON logs only. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | no | Only `http/protobuf` is supported. |
| `OTEL_EXPORTER_OTLP_HEADERS` | no | Standard exporter authentication headers. |
| `RUST_LOG` | no | `tracing` filter; defaults to `info,axond=info`. |
| `AXOND_<SECTION>__<KEY>` | no | Scalar override such as `AXOND_SERVER__BIND=0.0.0.0:9090`. |

TOML owns structure; scalar overrides are for deployment adaptation.

## Health and readiness

| Endpoint | Authentication | Meaning |
| --- | --- | --- |
| `GET /healthz` | none | Process is alive. Keeps returning `ok` through the shutdown drain. |
| `GET /readyz` | none | Process is serving a boot-validated snapshot; `503 draining` once termination begins. |
| `GET /v1/models` | gateway credential | Namespace-scoped alias catalogue. |
| `GET /v1/credentials` | gateway credential | Replica-local credential labels and circuit state. |

`/readyz` does not continuously probe providers or stateful backends. Runtime
dependency health is exposed by typed errors and metrics.

Point the load balancer at `/readyz` and liveness at `/healthz`: on `SIGTERM`
the replica fails readiness first, keeps serving for `shutdown.drain_grace_ms`,
then refuses new work with `503 draining` while admitted requests finish. Give
the supervisor a stopping timeout above
`drain_grace_ms + deadline_ms + flush_timeout_ms` so buffered usage can flush;
see [Upgrades and rollback](./operations/upgrades.md).

## Telemetry

Set an OTLP/HTTP endpoint to install traces, metrics, logs, and W3C propagation:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318
```

Unset is supported: Axond writes JSON logs and usage records to stdout. See the
[observability runbook](./observability.md).

## Hot-reload and rotation

`SIGHUP` and optional `[reload] watch = true` validate and atomically publish a
new snapshot. Invalid candidates leave the old snapshot serving. File-backed
key material can be replaced and re-read; new process environment variables
require a replacement process/container.

`[server]`, `[[usage_sink]]`, and `[budget]` changes require restart. Follow the
[minted-token guide](./minted-token-guide.md) for signer and revocation
operations.

## Sizing and stateful opt-ins

Tier 0 has no datastore and scales by adding replicas, but circuits, credential
health, and in-memory controls are replica-local. Redis and Postgres make
selected controls exact/durable and therefore become availability and migration
dependencies. The [stateful guide](./deployment/stateful-backends.md) contains
the capability matrix, failure policy, and namespace-cap migration sequence.

Every deployment described here runs the stateless operating mode, where TOML is
the authority. The accepted design for an opt-in, Postgres-backed control plane
(`/admin/v1`, immutable revisions, one snapshot per request) is
[ADR 0027](./adr/0027-stateless-and-stateful-operating-modes.md); it is not
implemented, and it changes nothing about the deployments above.

## Next steps

- [Configuration reference](./configuration.md)
- [Production checklist](./deployment/production-checklist.md)
- [Troubleshooting](./operations/troubleshooting.md)
- [Upgrades and rollback](./operations/upgrades.md)
- [Deployment security model](./security/deployment-model.md)
