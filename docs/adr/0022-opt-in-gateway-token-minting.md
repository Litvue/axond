# 22. Opt-in gateway token minting

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
exceed the configured authority. An omitted scope or microdollar limit inherits
its configured ceiling; when `scope` has no configured ceiling, an omitted
scope uses the ordinary capability posture. An omitted alias ceiling permits
`*`, while dispatch still narrows aliases to those the namespace can already
reach. Operator-only capabilities are rejected in a configured minting
ceiling because they can never be minted by this endpoint. Every effective
capability, including inherited capabilities, must also be held by the minting
key itself. Omission must never widen a token into an unrestricted one. The
namespace is derived from the authorized static caller and cannot be supplied
by the request.

Minting is stateless. The gateway writes no issuance registry, usage record,
or issuance log, consistent with ADR 0016. Short `max_ttl` values and
`[[gateway_token_epoch]]` `min_iat` remain the Tier 0 revocation controls.
Epochs are the revocation mechanism for minted tokens, so every deployment
that enables gateway minting must configure the applicable namespace epochs.
Rate-limit permits and budget reservations are intentionally not acquired by
`POST /v1/tokens`; this is a known accepted gap and a follow-up decision,
rather than a reason to introduce state into the mint path.
Because the minted `sub` is caller-chosen and budgets and inbound rate limits
are keyed by `(namespace, subject)`, a minting key can rotate subjects to get
fresh budget ledgers and per-subject concurrency allowances. Per-subject
budgets are therefore not a namespace-wide spend ceiling:
`BudgetConfig::limit_microdollars` is a cap per `(namespace, subject)`, so
`can_mint` has unbounded namespace spend authority by construction while
minting is enabled. `can_mint` is operator-level trust and must not be handed
to a downstream service. The blocker this ADR recorded — namespace-level budget
capping — is now available:
`BudgetConfig::namespace_limit_microdollars` caps everything a namespace spends
across all its subjects, enforced exactly on the `redis` and `postgres` backends
([ADR 0010](./0010-shared-budget-backends-and-charging-policy.md)). Every
deployment that enables minting should set it in the minting namespaces, since it
is what converts subject rotation from spend authority into a shared ceiling; the
`in-memory` and `none` backends cannot enforce it and reject it at boot.
Request throttling is not a mitigation: minted tokens outlive the mint request,
so throttling only slows accumulation of fresh subjects and bounds nothing.
Operators should isolate
minting namespaces from namespaces that rely on per-subject controls and keep
`max_ttl` short.

The gateway resolves and validates both sides of the signing relationship at
boot and reload. Signing material must be well formed and must match the
verifier material for the same `kid`; Ed25519 compares the derived public key,
and HS256 compares the secret in constant time. A mismatch is a typed,
redacted snapshot error, so issuance cannot succeed with a token that the
same gateway would reject.

In-gateway minting necessarily places the private signing key in the published
config snapshot on every replica that enables it. A compromised minting
replica can therefore forge inbound identity for every namespace its verifier
permits. EdDSA otherwise permits verification-only replicas to retain only
public verification material; HS256 never had that property because each
verifier already holds the forging secret. Deployments enabling EdDSA minting
should use dedicated replicas that do not serve dispatch traffic, keep the
private signing key only in that deployment's config, keep all other replicas
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
- Claim ceilings are centralized in configuration. A configured section with
  no `can_mint = true` key remains valid but rejects every mint caller, and
  reload logs that no key is authorized.
- Precise per-token revocation, issuance accounting, rate limiting, and
  budget reservation remain follow-up work outside this stateless endpoint.
- Per-subject budget and concurrency controls are not a namespace-wide
  ceiling when callers can choose fresh subjects. The spend half of that gap is
  now closable with `namespace_limit_microdollars` on a shared budget backend,
  which minting deployments should set; namespace isolation and short-lived
  tokens remain the mitigation for the concurrency half.
