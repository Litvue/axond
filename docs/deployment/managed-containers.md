# Managed-container platforms

Axond fits platforms that run an arbitrary OCI image with environment variables,
a mounted configuration file, TCP/HTTP ingress, and health probes. This includes
ECS/Fargate, Google Cloud Run, Azure Container Apps, and Nomad.

This guide defines the portable deployment contract. Platform consoles and IaC
syntax change frequently; translate each row into the provider's current
resource model rather than copying an unverified provider-specific template.

| Requirement | Axond value |
| --- | --- |
| Image | `ghcr.io/litvue/axond@sha256:<verified-digest>` |
| Architecture | `linux/amd64` |
| Container port | `8080` by default |
| Config | Read-only TOML mounted into the container |
| Config selector | `AXOND_CONFIG=/path/to/axond.toml` |
| Secrets | Environment variables named by the TOML; supported gateway key/verifier values may use files |
| Liveness | `GET /healthz`, no authentication |
| Readiness | `GET /readyz`, no authentication |
| Logs | JSON on stdout/stderr |
| Writable disk | Not required for serving |
| Inbound TLS | Terminate at the platform load balancer |

## Minimum deployment

1. Verify the release image and resolve it to a digest.
2. Store `axond.toml` in the platform's config mechanism or bake a separate,
   non-secret config artifact.
3. Bind every `env` and `dsn_env` reference to the platform secret manager.
4. Route private or authenticated ingress to port 8080.
5. Configure `/healthz` and `/readyz` probes.
6. Preserve streaming responses and choose an idle timeout suitable for model
   generations.
7. Send logs to the platform log sink and configure OTLP/HTTP when desired.

## Scaling choices

Tier 0 scales with no shared state, but each replica has independent credential
health, failover circuits, in-memory budgets, and in-memory rate limits. That is
often correct for evaluation or simple routing.

Use a managed Redis or Postgres endpoint when budgets, rate limits, revocation,
or usage must be exact/durable across instances. Axond connects configured
backends before listening; a bad DSN or unreachable service prevents a new
revision from becoming healthy.

Set platform concurrency and autoscaling limits from measured request and
stream duration. Scaling to zero may be acceptable at Tier 0, but cold starts
include configuration validation and backend connections. With fail-closed
shared controls, backend availability is part of admission.

## Configuration updates

- A mounted-file update can be applied by `[reload] watch = true` when the
  platform updates the file in place or swaps its projected symlink.
- Environment secret changes require a new task/revision/container.
- `[server]`, `[[usage_sink]]`, and `[budget]` changes require a replacement
  instance.
- A rejected reload leaves the prior snapshot serving; a rejected new revision
  never binds its listener.

## Platform checks

Before declaring a platform supported, test:

- a streamed chat or Messages request through the real ingress;
- authentication headers at the gateway;
- the platform's maximum request and idle durations;
- secret and config rotation behavior;
- revision draining and client retry behavior;
- OTLP egress and required proxy settings;
- Redis/Postgres TLS and DNS from the execution environment.

The binary drains on `SIGTERM` within
`drain_grace_ms + deadline_ms + flush_timeout_ms`. Confirm the platform sends
`SIGTERM` (not only a network-level revision cutover) and that its stop grace
period exceeds that sum; where the grace period is fixed and short, lower the
three bounds to fit rather than losing buffered usage to a `SIGKILL`. Keep
overlapping revisions and retry-capable clients: streams open at the deadline
are still cut.
