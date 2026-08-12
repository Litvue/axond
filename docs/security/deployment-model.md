# Deployment security model

This guide summarizes the security boundary an operator must preserve. The
dated [security review](../security-review-2026-08-05.md) records the detailed
code audit and findings.

## Trust boundaries

- Callers trust Axond with prompts, completions, and an Axond credential.
- Axond holds provider credentials and injects them only at transport dispatch.
- Provider credentials are never returned by `/v1/models`,
  `/v1/credentials`, logs, usage rows, spans, or metrics.
- Namespace configuration decides which credential pool a caller may use.
- Redis/Postgres become admission dependencies only for the features explicitly
  configured to use them.

## Inbound authentication

Every route except `/healthz` and `/readyz` requires a configured static gateway
key or valid minted token. There is no anonymous or open-development mode.

Keep a static default-namespace key as the operator breakglass path. Minted
tokens narrow authority through namespace, alias, scope, lifetime, audience,
and optional request-cost claims. They do not gain the all-namespace credential
operator view.

## Secret delivery

TOML stores structure and references, never secret values.

- Provider credentials and DSNs use environment-variable references.
- Static gateway keys and verifier material may use environment references or
  supported mounted files.
- Ed25519 public/private base64 tolerates surrounding whitespace where
  documented; static gateway-key and HS256 files are exact bytes.
- Do not put credentials in provider URLs, query strings, command arguments,
  container labels, or ConfigMaps.

Restrict files and environment access to the service identity. Rotate by
overlapping distinct keys, moving callers, then removing the old key. See the
[minted-token guide](../minted-token-guide.md) for signer rotation and JTI
revocation.

## Network

Axond uses rustls for provider HTTP and TLS-enabled Postgres/Redis connections.
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

Axond emits identifiers needed for operations—namespace, alias, target,
credential label, status, token counts, and cost—but not credentials, prompts,
or completions. Treat logs and usage rows as tenant metadata and apply ordinary
access control and retention policy.

## Availability decisions

Shared controls default to fail closed. Changing `on_unavailable` to `allow`
trades enforcement for availability and must be an explicit risk decision.
`/readyz` is not a continuous provider/datastore probe; runtime dependency
health comes from typed errors and metrics.
