# 16. Minted inbound identity and layered principal stores

Date: 2026-08-09

## Status

Accepted

## Context

Inbound authentication currently maps a static secret held in configuration to
`InboundKey { namespace, subject }`. That keeps the default path simple and
fail-closed (ADR 0013), but it also makes caller cardinality, expiry, and
delegation configuration concerns. A new credential should be a signed,
short-lived delegation of authority that the gateway can verify without
remembering every caller.

The design must preserve the namespace and credential boundaries in ADR 0003,
the single-namespace credential-pool walk in ADR 0006, the stateless-by-default
promise in ADR 0002, and the atomic config/reload gate in ADR 0011. It must also
leave a migration path for stateful caller stores without making availability
of one a prerequisite for the operator's existing breakglass credential.

## Decision

**The gateway is a verifier, not a registry.** Configuration enumerates who may
vouch for callers, not every caller. The gateway verifies a credential and
intersects its claims with configuration authority; it does not look up a
caller in a default registry.

**Minted credentials use JWS compact form with an `axt1.` prefix.** The prefix
lets the request path select the verification scheme without probing multiple
credential formats. Static keys and minted tokens therefore coexist without an
ambiguous or timing-sensitive lookup.

The claim set is:

| Claim | Decision |
|---|---|
| `ns` | Namespace; it must resolve in configuration. An unknown namespace is a rejection. |
| `sub` | Caller subject; becomes `BudgetKey.subject` and the usage subject. |
| `iat` | Issued-at time. |
| `exp` | Expiry time; mandatory. An otherwise valid token without `exp` is rejected. |
| `nbf` | Optional not-before time, with a small fixed skew allowance. |
| `aud` | Deployment audience, preventing cross-environment replay. |
| `kid` | Verification-key identifier in the JWS header, supporting overlap during rotation. |
| `jti` | Mandatory at every tier, including Tier 0 where no denylist is configured. |
| `scope` | Optional route capabilities: `chat`, `messages`, `embeddings`, and `models`. |
| `aliases` | Optional alias allowlist/globs, intersected with the namespace catalogue. |
| `max_request_microdollars` | Optional per-request ceiling, checked against the existing estimate. |

Verification happens before provider work: the `axt1.` prefix, signature, time
claims and audience, namespace, signer authority, and route/alias narrowing are
all checked in that order.

**A token may narrow authority, never widen it.** Configuration remains the
sole source of truth for namespace authority. The `ns` claim must name a
configured namespace; an unknown namespace is rejected, never implicitly
created or granted authority. Token scopes and aliases can only be
intersections with what that namespace can already reach. This preserves the
explicit namespace model in ADR 0003 and the fail-closed posture in ADR 0013.

### Configured verifiers

Configuration enumerates signers with a new section:

```toml
[[gateway_verifier]]
kid        = "acme-2026-08"
alg        = "EdDSA"
env        = "GW_VERIFY_ACME_2026_08"
namespaces = ["acme"]
max_ttl    = "15m"
```

`kid`, algorithm, source reference, permitted namespaces, and maximum token
lifetime are configuration-owned. The source is exactly one of an environment
variable name (`env`) or a file path (`file`), and material is resolved by the
same boot/reload validation path as existing gateway keys. File material is
re-read during reload, so rotation is a reload rather than a restart. This
source choice applies to static `[[gateway_key]]` breakglass credentials too.
Public Ed25519 verification keys may be held as public key bytes; HS256 secrets
remain protected as secrets.

Each verifier's `max_ttl` is bounded by a 24-hour policy ceiling. This is not a
protocol limit: an unbounded value would let a signer mint credentials that
outlive any incident response, defeating the first rung of this ADR's
revocation ladder.

**Ed25519/EdDSA is the default asymmetric verifier. HS256 is a deliberate
escape hatch.** Ed25519 means a gateway replica holds only public verification
material and cannot mint inbound identity. HS256 is available when the gateway
is intentionally also a minter, but every verifier holding the shared secret
can forge tokens and must therefore opt into that tradeoff.

Verifier rotation may replace file material under the same `kid` and reload, or
follow the overlap dance from ADRs 0011 and 0013: add the new `kid`, reload,
move minting to it, then remove the old `kid` and reload. Applied reload
summaries include a short fingerprint of each resolved verifier and static-key
material, so same-`kid` changes are observable without logging secrets. The
fingerprints are salted per process and comparable only within one process
lifetime; they show that material changed at this reload, but are not stable
identifiers for a key. A signer is permitted only for its configured
`namespaces`.

### Minting and federation

**Offline minting is the default.** An `axond mint` subcommand emits a token
from a signing key and claims. The gateway only verifies it; no issuance record,
store, or request-path dependency is added. This is the default that preserves
ADR 0002.

The minter reads signing material from an environment variable named by the
command, never from argv. It emits only the `axt1.` token on stdout; diagnostics
go to stderr. `axond keygen` writes a base64 PKCS#8 Ed25519 private key to a new
`0600` file and prints only the base64 raw public key plus a ready-to-paste
verifier configuration snippet. The public key is therefore safe to install on
verification-only replicas while the private key remains with the minter.

Minting always enforces the 24-hour policy ceiling. When a matching verifier is
available through `--config` or `AXOND_CONFIG`, minting additionally enforces
that verifier's configured `max_ttl` and may default its audience. Without
that config, the minter cannot know the verifier-specific bound: a token may
mint successfully against the policy ceiling and then be rejected by the
gateway if it exceeds the configured `max_ttl`.

The current minter emits only claims the verifier enforces. It deliberately
does not emit `scope`, `aliases`, or `max_request_microdollars` until the
corresponding authorization controls exist; an unenforced narrowing claim
would create a misleading credential.

`POST /v1/tokens` is an opt-in alternative for deployments where callers cannot
run a minter. It is authenticated by a static gateway key authorized to mint
(or by a separately configured minting key), accepts only narrowing claims, and
returns the token and its expiry. It remains stateless, but holding a signing
key in the gateway makes compromise a forgery capability; it is not the
default.

OIDC/JWKS federation and direct third-party token acceptance are deferred.
They may later verify cached JWKS material and map issuer claims to namespaces,
but they introduce an outbound dependency and must fail closed without a
usable cached key.

### Revocation

**Precise single-token revocation is impossible statelessly.** The revocation
ladder is:

1. Short lifetimes, bounded by each verifier's `max_ttl`.
2. Kill a `kid` by removing it from config and reloading, revoking every token
   from that signer.
3. A per-namespace (optionally per-subject) `min_iat` epoch in configuration,
   revoking tokens issued before that time.
4. An opt-in `jti` denylist using the already optional Redis/Postgres stores,
   with fail-closed behavior when configured storage is unavailable.

The first three are coarse but stateless. The fourth is the deliberate
stateful exception for requirements that demand precise revocation. `jti` is
mandatory even at Tier 0, where no denylist exists; this unconditional contract
is what keeps the fourth rung usable when a deployment later moves to the
stateful tiers defined by ADR 0017. If `jti` were optional, enabling a denylist
would silently leave already-issued tokens without revocable identifiers.

### PrincipalStore seam

Authentication is abstracted behind a layered `PrincipalStore` trait. The
conceptual interface is:

```rust
trait PrincipalStore {
    fn name(&self) -> &'static str;
    async fn resolve(
        &self,
        presented: &Presented,
        snapshot: &ConfigSnapshot,
    ) -> Result<Option<Principal>, ResolveError>;
}
```

`Presented` carries the raw credential and enough context to route it,
including its header source and credential prefix. `Principal` is the superset
of identity: namespace and subject, with optional scopes, aliases, limits, and
`key_id`/`kid`.

The default implementation is `ConfigPrincipals`, preserving today's static
gateway-key behavior. `TokenVerifier` handles `axt1.` credentials. Future
`PostgresPrincipals` or `RedisPrincipals` implementations may add hashed
credentials, revocation, and self-serve caller lifecycle.

**Each principal layer declares the credential shapes it owns, and shapes have
one owner.** `TokenVerifier` owns `axt1.` credentials; a store-backed layer
owns credentials such as `axk_<key_id>_`. Two layers may not declare the same
shape, and a config that attempts to do so is rejected at boot, so ordering
never decides authority.

A credential matching an owned shape is resolved exclusively by that owner.
Both no-match and error are terminal for that credential; there is no
continuation to another layer in either case. A credential that an unavailable
store would have resolved is rejected rather than silently resolved by another
authority, because nothing about a store's state may change which authority
authorized a caller.

The only routing to `ConfigPrincipals` is a credential matching no declared
shape. It then gets today's constant-time table scan over the static keys.
This is shape-based routing, not ordered probing, and it keeps the operator
breakglass path working. Other callers — those presenting credentials owned by
unaffected layers — remain unaffected by a store outage because their
resolution never touches that store.

Any store-backed implementation must satisfy three request-path requirements:

1. Use a bounded in-process TTL cache keyed by the presented credential's hash,
   including short-lived negative caching. Both cache directions are bounded;
   negative TTLs remain short so new credentials and revocations do not remain
   hidden.
2. Use a bounded timeout and fail closed for that credential when its configured
   store is unavailable. Failure is terminal; there is no continuation to
   `ConfigPrincipals` or any other layer.
3. Store `(key_id, hash)`, never plaintext key material. A credential such as
   `axk_<key_id>_<secret>` is indexed by `key_id` and verified against the
   stored hash.

### Compatibility and authorization errors

**Static gateway keys remain unchanged.** They remain the bootstrap credential
and the authorization for minting. The existing config key table continues to
work through `ConfigPrincipals`.

**At least one `[[gateway_key]]` remains mandatory.** ADR 0013's fail-closed
rule is preserved: there is no keyless mode, a config with no
`[[gateway_key]]` fails `Config::validate`, and `ConfigSnapshot::build` refuses
to publish an empty resolved static-key table. Verifiers are strictly
additive; a declared gateway verifier whose source is missing, unreadable, or
empty is also fatal, and duplicate static secrets remain invalid. A failed
reload keeps the last valid snapshot serving.

The static key is the operator breakglass credential and the layer that cannot
fail for infrastructure or signing-key reasons. A verifier-only deployment is
therefore not legal: if its sole signing authority is lost or compromised,
the deployment would have no recovery path. Requiring a static key is an
additional constraint this ADR accepts deliberately to preserve availability.

Malformed, expired, not-yet-valid, wrongly-audienced, or badly signed tokens
are authentication failures and return `401`. A validly authenticated token
whose signer is not permitted for its namespace, whose namespace is unknown,
or whose scope/alias does not authorize the requested operation is an
authorization failure and returns `403`.

Logs and typed errors may name `kid`, `ns`, `sub`, namespace names, and counts.
They must never contain token bytes, signing key material, or static secret
values.

### Budgets and usage

**`BudgetKey { namespace, subject }` remains unchanged.** Minting changes the
cardinality of `subject`, not the budget identity shape. Because arbitrary
subjects can now arrive in tokens, the in-memory `HashMap<BudgetKey, Ledger>`
must gain idle eviction or a TTL; otherwise a long-running replica can retain
an unbounded number of inactive subjects.

Usage records must continue to attribute namespace and subject. They should
also carry `kid` and, when federation exists, `iss`, so delegated spend can
answer which signer vouched for the caller. Downstream consumers must not
assume that subjects belong to the static config key list.

## Consequences

- A caller can receive an expiring, scoped credential without an axond config
  edit, environment-variable rollout, or per-caller gateway registry.
- Gateway verifier and static breakglass material may come from files and is
  re-read on reload, making rotation a no-restart operation. Reload summaries
  expose only short salted SHA-256 fingerprints, never material; fingerprints
  are comparable only within one process lifetime and are not stable key
  identifiers.
- Ed25519 keeps verification-only replicas from becoming token minters.
- Offline minting preserves the zero-external-state default; token issuance
  through HTTP and precise revocation are explicit opt-ins.
- `sub` becomes high-cardinality, so memory accounting, usage schemas, and
  operational dashboards must handle arbitrary subjects.
- The layered resolver makes a stateful identity backend additive rather than a
  cutover: callers on other layers remain unaffected by its outage, while a
  credential owned by the failed layer is rejected rather than falling through
  to a different authority.
- A token is not a second namespace authority. A verifier misconfiguration
  remains capable of granting authority to every token it signs for its
  configured namespaces, so signer ownership and rotation remain operational
  responsibilities.
- `POST /v1/tokens`, OIDC/JWKS federation, cumulative token budgets, and
  precise revocation remain open implementation decisions. The gateway does not
  gain a runtime control plane from this ADR.
- New cryptographic dependencies must still pass the repository's crates.io,
  license, toolchain, and `cargo-deny` policy.
