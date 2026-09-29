# Deployment security model

This guide summarizes the security boundary an operator must preserve. The
dated [security review](../security-review-2026-08-05.md) records the detailed
code audit and findings, and the
[threat-model review triggers](./threat-model-review.md) say which changes to
Axond require that reasoning to be re-examined.

## Trust boundaries

- Callers trust Axond with prompts, completions, and an Axond credential.
- Axond holds provider credentials and injects them only at transport dispatch.
- Provider credentials are never returned by `/ns/{ns}/v1/models`,
  `/ns/{ns}/v1/credentials`, logs, usage rows, spans, or metrics.
- Namespace configuration decides which credential pool a caller may use.
- The Store (`[storage]`) is an admission dependency for period budgets.

## Inbound authentication

Every route except `/healthz` and `/readyz` requires a configured static gateway
key. There is exactly one `[[gateway_key]]`, and it authenticates both `/api/v1`
and `/ns/{ns}/v1` inference. There is no anonymous or open-development mode.
Minted `axt1.` tokens were withdrawn with
[ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md) and are refused.
The namespace is selected from the authenticated path, and an unknown
namespace is refused before dispatch.

## Secret delivery

TOML stores structure and references, never secret values.

- Provider credentials and DSNs use environment-variable references.
- The static gateway key may use an environment reference or a mounted file.
  Gateway-key files are exact bytes.
- Do not put credentials in provider URLs, query strings, command arguments,
  container labels, or ConfigMaps.

Restrict files and environment access to the service identity. Rotate the
gateway key by rolling replicas with the new value and moving callers at the
same time; there is one key, so there is no overlap window.

## Network

Axond uses rustls for provider HTTP and TLS-enabled Postgres connections.
It deliberately does not terminate inbound TLS. A trusted reverse proxy or load
balancer must provide TLS, caller network policy, and streaming-compatible
timeouts without stripping authentication headers.

Provider `base_url` must be path-only. Never include userinfo, query strings,
fragments, or secrets.

## Supply chain

Production should deploy a release digest after verifying:

- SHA-256 sidecar for a binary archive;
- GitHub build provenance;
- SPDX SBOM attestation;
- cosign keyless signature for the OCI digest.

The release workflow verifies the published image before signing it. There is
no `latest` image tag.

## Logs and telemetry

Axond emits identifiers needed for operations—namespace, model, target,
credential label, status, token counts, and cost—but not credentials, prompts,
or completions. Treat logs and usage rows as tenant metadata and apply ordinary
access control and retention policy.

## Availability decisions

Shared controls default to fail closed. Changing `on_unavailable` to `allow`
trades enforcement for availability and must be an explicit risk decision.
`/readyz` is not a continuous provider/datastore probe; runtime dependency
health comes from typed errors and metrics.
