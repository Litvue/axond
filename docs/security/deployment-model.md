# Deployment security model

This guide summarizes the security boundary an operator must preserve. The
dated [security review](../security-review-2026-08-05.md) records the detailed
code audit and findings, and the
[threat-model review triggers](./threat-model-review.md) say which changes to
Axond require that reasoning to be re-examined.

## Trust boundaries

- Callers trust Axond with prompts, completions, and an Axond credential.
- Axond holds provider credentials and injects them only at transport dispatch.
- When a namespace activates `axond.redact`, its provider receives deterministic
  placeholders for matched content rather than the original values; restoration
  state exists only for that request and is never written to telemetry or a
  durable store.
- Provider credentials are never returned by `/v1/models`,
  `/v1/credentials`, logs, usage rows, spans, or metrics.
- Namespace configuration decides which credential pool a caller may use.
- Redis/Postgres become admission dependencies only for the features explicitly
  configured to use them.

`axond.redact` is deterministic pseudonymization, not protection against guessing
low-entropy secrets. Providers see its tokens; equal values produce equal tokens
within a namespace, permitting a same-namespace chosen-plaintext oracle. Derived
namespace keys prevent cross-namespace comparison. Use separate namespaces where
caller equality must not be shared. Restoration is bound to the authenticated
route and limited to its known display-text fields; structured tool arguments,
URL fields, identifiers, and other control data never receive declassification.
Display text remains untrusted provider output: a token embedded in Markdown,
HTML, URL-looking prose, or an instruction inside an allowed display string is
restored. Clients must not auto-fetch, execute, or render restored model text in
a privileged context. Use block-only policy or disable redaction if that trust
boundary is unacceptable. Matches in caller-controlled JSON keys, continuation
ids, forwarded native wire headers, or the canonical provider-visible text
sequence across split fields/parts refuse before provider dispatch because those
channels cannot be safely rewritten. Protected wire fragments are checked
against each other and the body, so a match split across a header and JSON is
also refused.

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
- Deterministic guardrail keys use versioned environment-variable references and
  canonical padded-base64 32-byte values. Rotate by publishing a policy that
  names a new reference; do not replace the value behind an existing reference.
- Static gateway keys and verifier material may use environment references or
  supported mounted files.
- Ed25519 public/private base64 tolerates surrounding whitespace where
  documented; static gateway-key and HS256 files are exact bytes.
- Do not put credentials in provider URLs, query strings, command arguments,
  container labels, or ConfigMaps.

In stateful mode a credential resource stores an opaque secret reference and the
material stays in the secret store; see
[secret material in the stateful control plane](./secret-material.md) for what
that guarantees and how it is tested.

The namespace-native blob format is a separate v2 envelope; the legacy
Postgres v1 envelope is unchanged and has a frozen opening vector. Schema 2
stores only `aes256-kw.aes256-gcm.envelope.v2`, the stable KEK id, a 40-byte RFC
3394 wrapped data key, one material nonce, and ciphertext. Authenticated desired
state supplies the environment id, namespace, and exact secret reference;
AES-256-GCM binds those values and the KEK id through binary length-prefixed
material AAD. RFC 3394 wraps the fixed 32-byte DEK without a nonce and does not
independently authenticate caller context. The parser accepts one bounded
six-element canonical-CBOR spelling, never a generic CBOR extension.
Bootstrap KEKs are exactly 32 bytes in zeroizing owned buffers. A publisher has
one active encryption key, while a serving decrypt-only ring admits no more
than eight retired keys; neither set may contain ids that alias the same raw
key. This codec is an off-request-path building block:
create-only publication, exact-reference uniqueness, and snapshot-time
resolution must be wired before it can serve a credential. Dropping keys is
best-effort memory hygiene, not proof against registers, crash dumps, swap, or
library-internal expanded-key copies; restart replicas to evict a retired KEK
from the process boundary.

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
