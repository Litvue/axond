# 24. Opt-in gateway token minting

Date: 2026-08-11

## Status

Accepted

## Context

ADR 0016 defines the minted inbound identity format and keeps offline
`axond mint` as the default issuance path. Some deployments cannot safely
place a minter beside every caller, however, and need an authenticated HTTP
endpoint for issuing short-lived tokens. Putting a signing key in the gateway
changes the compromise and operational model, so the endpoint needs explicit
boundaries around authorization, claims, reloads, and state.

## Decision

`POST /v1/tokens` is an opt-in feature enabled by `[gateway_minting]`. It is
absent from the default configuration; offline `axond mint` remains the
recommended default. Only a static gateway key with `can_mint = true` may
call the endpoint. Minted-token principals have `can_mint = false`
unconditionally, so a token claim can never confer permission to mint another
token.

The route is registered at boot based on whether minting is configured. This
is a deliberate, narrow exception to the delta-B3 rule documented in
`error.rs`, where proxy routes remain registered and return typed errors:
for issuance, absence is the security property, and an unconfigured route
must not look available to callers or intermediaries. Consequently, enabling
minting on reload is reported but requires a restart, while removing
`[gateway_minting]` takes effect immediately and returns a typed 404. This
asymmetry makes the safe shutdown direction immediate without making a
reload silently create a new privileged endpoint.

Every request is narrowed against ceilings owned by `[gateway_minting]`:
`ttl_seconds`, `scope`, `aliases`, and `max_request_microdollars` cannot
exceed the configured authority. An omitted field inherits its configured
ceiling; when `scope` has no configured ceiling, an omitted scope uses the
ordinary capability posture and operator-only capabilities must be explicitly
granted. Omission must never widen a token into an unrestricted one. The
namespace is derived from the authorized static caller and cannot be supplied
by the request.

Minting is stateless. The gateway writes no issuance registry, usage record,
or issuance log, consistent with ADR 0016. Short `max_ttl` values and
`[[gateway_token_epoch]]` `min_iat` remain the Tier 0 revocation controls.
Rate-limit permits and budget reservations are intentionally not acquired by
`POST /v1/tokens`; this is a known accepted gap and a follow-up decision,
rather than a reason to introduce state into the mint path.
Because the minted `sub` is caller-chosen and budgets and inbound rate limits
are keyed by `(namespace, subject)`, a minting key can rotate subjects to get
fresh budget ledgers and per-subject concurrency allowances. Per-subject
budgets are therefore not a namespace-wide spend ceiling, and
`max_request_microdollars` limits one request rather than cumulative spend.
Operators should isolate minting namespaces from namespaces that rely on those
per-subject controls, treat `can_mint` as trusted for the whole namespace, and
keep `max_ttl` short.

The gateway resolves and validates both sides of the signing relationship at
boot and reload. Signing material must be well formed and must match the
verifier material for the same `kid`; Ed25519 compares the derived public key,
and HS256 compares the secret in constant time. A mismatch is a typed,
redacted snapshot error, so issuance cannot succeed with a token that the
same gateway would reject.

In-gateway minting necessarily places a signing key in the gateway. A
compromised minting replica can therefore forge tokens, and an Ed25519
verification-only replica benefit is lost on replicas that mint. Deployments
should use a separately deployed minting replica set, keep all other replicas
verification-only, and keep the minting TTL short.

### State tier

This is Tier 0 / config-only. Authorization, claim ceilings, key pairing,
and revocation epochs are derived from the immutable config snapshot, while
the token carries the complete receipt. No Redis or Postgres state is added,
and this decision does not raise the state tier of an existing deployment.

## Consequences

- Offline minting remains the default and keeps ordinary replicas
  verification-only.
- A separately deployed minting replica set limits the blast radius of a
  signing-key compromise, at the cost of operating an additional deployment
  boundary.
- Boot and reload reject missing, malformed, or mismatched signing material
  before requests can observe it; the last good snapshot continues serving
  after a rejected reload.
- Claim ceilings are centralized in configuration, but operators must plan
  revocation edits carefully: removing the last `can_mint` flag alone is
  rejected while `[gateway_minting]` remains present. Removing both the
  section and the flags disables issuance on reload.
- Precise per-token revocation, issuance accounting, rate limiting, and
  budget reservation remain follow-up work outside this stateless endpoint.
- Per-subject budget and concurrency controls are not a namespace-wide
  ceiling when callers can choose fresh subjects; namespace isolation and
  short-lived tokens are the accepted operational mitigation.
