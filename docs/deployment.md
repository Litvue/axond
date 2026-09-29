# Deployment overview

Axond is one store-backed process. It reads TOML for structure, environment or
supported mounted files for secret material, and requires `[storage]` (SQLite
WAL or Postgres).

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
  -H 'content-type: application/json' \
  -d '{"limit_microdollars":1000000000000}' \
  -X PUT http://127.0.0.1:8080/api/v1/namespaces/platform/budgets/quickstart
curl --fail \
  -H 'Authorization: Bearer quickstart-platform-key' \
  http://127.0.0.1:8080/ns/platform/v1/models
docker compose down -v
```

For expected responses, source builds, and the Postgres overlay, follow
[Getting started](./getting-started.md) and the
[Compose guide](./deployment/docker-compose.md).

## Choose an environment

| Environment | Guide | Best fit |
| --- | --- | --- |
| Docker Compose | [Compose](./deployment/docker-compose.md) | Evaluation, local integration, and reproducible demos. |
| Docker or Podman | [Container](./deployment/container.md) | Existing container platforms and custom orchestration. |
| Linux VM / bare metal | [systemd](./deployment/systemd.md) | Static binary behind an existing proxy/load balancer. |
| Kubernetes | [Kubernetes](./deployment/kubernetes.md) | Horizontally scaled container deployment with ConfigMap/Secret delivery. |
| Azure Container Apps | [ACA production](./deployment/azure-container-apps.md) | Worked production path: GHCR digest, Key Vault keys, TOML mount. |
| Managed containers | [Managed-container contract](./deployment/managed-containers.md) | ECS/Fargate, Cloud Run, Nomad, and other OCI hosts. |

Review [Store backends](./deployment/stateful-backends.md) before choosing
Postgres HA and use the
[Production checklist](./deployment/production-checklist.md) before exposing a
deployment.

## What every environment must provide

- A readable TOML configuration selected by `AXOND_CONFIG` (default
  `axond.toml`).
- A required `[storage]` section (SQLite path or Postgres `dsn_env`).
- Every environment or file reference declared by the config.
- Exactly one static `[[gateway_key]]`; there is no keyless mode.
- Provider network egress.
- Port 8080 or a scalar `AXOND_SERVER__BIND` override.
- JSON stdout/stderr collection.
- TLS termination and streaming-compatible proxy behavior when exposed over a
  network.

Configuration, credential resolution, and the Store connection complete
before the listener binds.

## Minimal working config

```toml
[server]
bind = "0.0.0.0:8080"

[storage]
backend = "sqlite"
path = "axond.sqlite"

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

[catalog]
source = "seed"
bootstrap = "seed"
```

That is the serving floor. Callers send `openai/gpt-4o` to
`/ns/platform/v1/...`. Charging uses imported models.dev rates (`models-dev` in
production; `bootstrap = "seed"` so the first boot has rates). `[[price]]` is
optional for custom ids the catalogue does not list. Publish a period budget
before inference: `PUT /api/v1/namespaces/{ns}/budgets/{period}`. Credential
pools, admission, usage sinks, and telemetry are optional. `[[model]]`, `mode`,
and a second `[[gateway_key]]` are boot errors.

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
a `linux/amd64` + `linux/arm64` index, so it runs natively on either
architecture. It has no `latest` tag and ships no config. Verify and pin a digest as described in the
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

TOML owns structure; scalar overrides are for deployment adaptation. Secret
*values* never belong in the file: `env = "NAME"` or, for inbound keys, `file =
"/run/secrets/..."`. Azure Key Vault (or any platform store) injects those
references at process start. Changing an env-injected secret is a new
revision. Provider `[[credential]]` is env-only
today. Do not look up Key Vault on the request path.

## Health and readiness

| Endpoint | Authentication | Meaning |
| --- | --- | --- |
| `GET /healthz` | none | Process is alive. Keeps returning `ok` through the shutdown drain. |
| `GET /readyz` | none | Process is serving a boot-validated snapshot; `503 draining` once termination begins. |
| `GET /ns/{ns}/v1/models` | gateway credential | Cached `provider-id/model-id` catalogue, minus blocklist. |
| `GET /ns/{ns}/v1/credentials` | gateway credential | Replica-local credential labels and circuit state. |

`/readyz` does not probe providers or the Store. Dependency health is typed
errors (`503 budget_unavailable`) and metrics.

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

## Configuration changes and rotation

Config changes take a restart; there is no hot reload. Rotate the static
gateway key or a provider credential by rolling replicas with the new value.
Replicas share nothing but the Store, so a rolling restart keeps serving.

## Sizing

Replicas scale horizontally against one Store. Circuits, credential health,
and admission ceilings are replica-local. SQLite serves one replica; use
Postgres for more. See [Store backends](./deployment/stateful-backends.md).

## Next steps

- [Configuration reference](./configuration.md)
- [Production checklist](./deployment/production-checklist.md)
- [Troubleshooting](./operations/troubleshooting.md)
- [Upgrades and rollback](./operations/upgrades.md)
- [Backup, restore, and PITR](./operations/backup-and-recovery.md)
- [Deployment security model](./security/deployment-model.md)
