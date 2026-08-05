# Security review — 2026-08-05

Scope: the beta qualification review of secret handling, tenant isolation, and
the release supply chain. Reviewed at commit `1dd3a14` on `main` plus the
changes in this PR. Line references are to that state of the tree.

**Outcome: one finding, fixed in this PR. No other secret-exposure path was
identified.** Two residual items are recorded below as accepted risk with
follow-ups.

## 1. Outbound provider credentials are write-only

Provider keys exist as `secrecy::SecretString` from the moment they leave the
environment until the instant they are written into an outbound header, and
nowhere else.

| Claim | Evidence |
| --- | --- |
| Loaded into `SecretString` at snapshot build, never a bare `String` | `crates/gateway/src/credentials.rs:246` (`SecretString::from(secret.clone())`), fields at `:39`, `:80` |
| The transport holds it as `SecretString` | `crates/gateway-transport/src/lib.rs:38` |
| It is unwrapped **only** at header injection | `rg -n expose_secret crates/` returns exactly six hits, all in `crates/gateway-transport/src/lib.rs` (`:122`, `:123`, `:208`, `:209`, `:242`, `:243`) — the bearer/`x-api-key` arms of the three dispatch shapes |
| No `Debug`/`Display`/`Serialize` derive can reach one | Neither `Upstream`, `CredentialLease`, `PoolEntry`, nor `ConfigSnapshot` derives `Debug`; `SecretString`'s own `Debug` prints `[REDACTED]` regardless |
| Errors name the *reference*, not the value | `CredentialError::MissingEnv` (`crates/gateway/src/credentials.rs:49-59`) interpolates namespace, provider, credential label, and env-var **name** |

## 2. Inbound gateway keys are write-only

Inbound tokens are resolved at boot into a lookup table keyed by the secret;
what travels anywhere else is the env-var **name**.

| Claim | Evidence |
| --- | --- |
| Only the env-var name is attributed | `InboundKey { namespace, subject }` (`crates/gateway/src/state.rs:61-64`) — `subject` is the variable's name, and that is what reaches usage records and spans |
| The table is unformattable | `ConfigSnapshot` (`:49-58`) and `InboundKey` derive no `Debug`/`Serialize` |
| Auth compares and discards | `authenticate` in `crates/gateway/src/routes.rs` reads the header, looks it up, and returns the `InboundKey`; the token is never bound to a logged variable |
| Missing/duplicate keys fail at boot naming references only | `SnapshotError` (`crates/gateway/src/state.rs:66-80`): `MissingGatewayKey { namespace, env }`, `DuplicateGatewayKey`, `NoInboundKeys` |
| Fail-closed: no keyless mode exists | `NoInboundKeys` is a boot error ([ADR 0013](./adr/0013-inbound-auth-fails-closed.md)) |

## 3. Logs, spans, metrics, and usage records

- **Spans** carry routing, status, token, cost, latency, and retry fields only
  (`crates/gateway/src/telemetry/spans.rs`, `http.rs:107-125`). No request body,
  no response body, no header value. The header map is never attached to a span:
  `rg -n 'header' crates/gateway/src/telemetry/http.rs` shows the layer reading
  only method, route, and inbound `traceparent`.
- **Metrics** are built from the canonical `UsageRecord`
  (`crates/gateway/src/telemetry/metrics.rs`) with fixed low-cardinality
  dimensions; no free-form caller input becomes an attribute.
- **Usage records** serialize `credential_id` (a non-secret label) and
  `credential_source` (`platform` | `byok`) — `crates/gateway/src/usage/mod.rs`.
  No field holds key material, prompts, or completions.
- **Logs**: all 19 `tracing::{info,warn,error,debug}!` call sites were read.
  Datastore errors are logged as `error = %e`; a probe of both clients confirms
  neither echoes the DSN — `redis` renders `Redis URL did not parse` /
  `Connection refused (os error 111)`, and `tokio_postgres::Config`'s own `Debug`
  prints `password: Some(_)`.
- **Reload diffs** log namespace ids, provider ids, alias names, credential
  labels, and gateway-key env-var names — references, never values
  (`crates/gateway/src/reload.rs`).

## 4. Finding: transport errors echoed the upstream URL — **fixed here**

`reqwest` renders the full request URL into its error message
(`error sending request for url (https://…?api-key=…)`), and the transport
propagated that message verbatim into `TransportError::Http`. That string is
logged, attached to the failure path, and returned to the caller in the error
body. axond never puts a credential in a URL — keys go in headers — but
`[[provider]] base_url` is operator-supplied, and endpoints that authenticate
by query parameter or signed URL are common enough that this was a real path
from config to an error body.

Severity: low (requires a misconfiguration), but it is a secret-exposure path
and the brief is not to leave one in place. Fix, in
`crates/gateway-transport/src/lib.rs`: every `reqwest` failure is now described
through `transport_failure`, which strips the URL's query, fragment, and
userinfo while keeping the host and path that make the error diagnosable.
Covered by `a_described_failure_keeps_the_endpoint_and_drops_its_secrets`.

The [configuration reference](./configuration.md) now also states plainly that
`base_url` must be path-only and must not carry a secret — a query string there
is doubly wrong, since the route's path is appended to it and would land inside
the query.

## 5. Accepted risk, with follow-ups

1. ~~**Inbound key material is a `String`, not a `SecretString`.**~~ **Resolved
   (#35).** Inbound keys are now held as `secrecy::SecretString` (redacted
   `Debug`, zeroized on drop), symmetric with the outbound path, behind a
   private `ConfigSnapshot` field that is resolved through
   `resolve_inbound(...)` rather than a public map. The lookup is a
   constant-time byte comparison, closing the timing-oracle gap as well.
2. **`GET /v1/models` is unauthenticated.** It discloses configured alias names
   — not secrets, not per-tenant data, and the same list for every caller — and
   the smoke test relies on it. Gating it behind the key table is a small
   behaviour change and therefore a follow-up rather than a change smuggled into
   a review PR.

Neither is a known secret exposure; both are recorded here so the posture is
stated rather than implied.

## 6. BYOK isolation

The tenancy boundary is the namespace, and it is enforced at credential
resolution rather than by convention
([ADR 0003](./adr/0003-namespaced-credentials-and-byok.md),
[ADR 0006](./adr/0006-credential-pools-per-namespace-provider.md)).

- A `[[credential]]` binds an env var to **one** `(namespace, provider)` pair.
  Resolution takes the caller's namespace from their gateway key and looks up
  that pair — there is no path that reaches another namespace's pool.
- A namespace with no credential for the resolved provider gets
  `502 no_credential`, **not** somebody else's key.
- `allow_platform_fallback` is the single, explicit, per-namespace, default-`false`
  exception: it permits *that* namespace to fall back to the `platform`
  namespace's pool when it has no credential of its own. It is one-directional
  (into `platform` only) and per-provider. It never lets one customer namespace
  reach another customer namespace.
- Attribution is unambiguous after the fact: every usage record carries
  `credential_source` (`platform` | `byok`) alongside the non-secret
  `credential_id`, so a fallback is visible in billing data rather than silent.
- Boot validation rejects a credential referencing an undefined namespace or
  provider, so an isolation boundary cannot be broken by a typo
  (`crates/gateway/src/config.rs`, `Config::validate`).

## 7. GitHub Actions posture

- **Default-deny at the top of every workflow.** `ci.yml`, `soak.yml`,
  `dependency-audit.yml`, and `release-please.yml` all declare
  `permissions: contents: read` at workflow level; `pr-title.yml` declares
  `pull-requests: read`. Elevated scopes are granted per job and nowhere else:
  `contents: write` + `pull-requests: write` for the release-please job only
  (`release-please.yml:39-41`), and `contents/packages/id-token/attestations`
  for the artifact jobs (`:163-167`, `:247-252`). `release-metadata` — the job
  that fans out — is `contents: read` (`:95-96`).
- **No long-lived registry or signing secret.** GHCR login uses
  `${{ github.token }}`; signing is keyless via the job's OIDC identity
  (`id-token: write` + `sigstore/cosign-installer@v3`,
  `cosign sign --yes …@<digest>`). There is no `COSIGN_PRIVATE_KEY`, no PAT, and
  no registry password in the repository.
- **The trusted signer is pinned narrowly.** `SIGNER_IDENTITY`
  (`release-please.yml:33`) is anchored at both ends and admits only this
  workflow file at `refs/heads/main` or a `refs/tags/v<semver>`, so a modified
  copy of the workflow on another branch cannot produce a signature that
  verifies.
- **The release verifies its own output.** After signing, the job runs
  `cosign verify` against that identity and `gh attestation verify` for SLSA
  provenance (`:318-330`), so a broken chain fails the release rather than
  shipping quietly.
- **Signing follows a smoke test.** The published image is exercised by
  `ops/docker-smoke.sh` *before* it is signed (`:291-292`) — the signature
  attests to an image that at least boots and serves.
- Actions are used at pinned major tags from first-party or well-known
  publishers. Moving to commit SHAs would be stricter; noted, not required for
  beta.
- **Supply chain.** `deny.toml` fails on any advisory, any yanked crate, any
  unlisted license, and any source outside crates.io, with no ignore entries;
  `dependency-audit.yml` runs it on a schedule so an advisory published after a
  merge is still caught. Nothing here was weakened.

## 8. Threat model notes

- **Multi-tenancy** is namespace-level, enforced in-process. Axond does not
  attempt to defend one tenant against another at the OS level; a shared
  gateway is a shared trust domain for availability (a noisy tenant can consume
  connections) even though credentials are isolated. Per-tenant budgets bound
  the *cost* of that, not the concurrency.
- **The gateway is not a WAF.** Request bodies are forwarded to the provider
  essentially untouched; content policy is the provider's.
- **Transport out is TLS** via `rustls` with webpki roots; `sslmode=require`
  turns on TLS to Postgres. Inbound TLS is deliberately not axond's job — put it
  behind an ingress or a service mesh.
- **A compromised config file is game over**, as for any daemon: it can point a
  namespace at an attacker's `base_url`. Treat it as it is treated here — a
  read-only mount that names env vars and holds no secrets.
