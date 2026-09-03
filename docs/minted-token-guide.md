# Minted inbound identity: operator guide and rotation runbook

> **Withdrawn ([ADR 0063](./adr/0063-stateful-only-namespaced-gateway.md)).** Minted `axt1.` tokens are not verified and not issued. `POST /v1/tokens` is unmounted. Use the one static `[[gateway_key]]`. Historical record; do not follow as a runbook.

This guide covers the Tier 0, config-only minted identity path. It uses the
same `axond` binary for key generation, offline minting, and gateway
verification. The gateway does not keep an issuance registry and verification
does not add Redis, Postgres, or another runtime dependency.

## In-gateway minting (opt-in)

Offline `axond mint` remains the default. To expose `POST /v1/tokens`,
configure a matching verifier and signing source, then authorize a static
gateway key:

```toml
[gateway_minting]
kid = "acme-2026-08"
env = "GW_SIGN_ACME_2026_08"
max_ttl = "10m"
scope = ["chat", "models"]
aliases = ["gpt-*"]
max_request_microdollars = 1000

[[gateway_key]]
env = "GW_INBOUND_PLATFORM_KEY"
namespace = "platform"
can_mint = true
```

The endpoint uses the same authentication middleware as every other
non-liveness route. The body accepts `sub`, optional `ttl_seconds`, `scope`,
`aliases`, and `max_request_microdollars`; it rejects unknown fields. Omitted
scope and microdollar limits inherit their configured ceilings when those
ceilings are configured; with no configured scope ceiling, the ordinary
capability posture is inherited. An omitted alias ceiling permits `*`, while
dispatch still narrows aliases to those the namespace can already reach.
Every effective capability must also be held by the minting key itself.
Operator-only capabilities cannot be configured as a minting ceiling and
therefore cannot be minted by this endpoint.
Requested values must narrow their ceilings and TTL must be between one second
and the effective maximum. Minted tokens are never authorized to mint another
token.

`POST /v1/tokens` is intentionally outside the request rate limiter and usage
fanout: issuance is unthrottled and unrecorded in the gateway by design. The
available controls are a short `max_ttl`, removing the signing `kid` to revoke
all tokens for that verifier, and `[[gateway_token_epoch]]` `min_iat` to revoke
tokens issued before a configured time. Epochs are the revocation mechanism for
minted tokens, so every deployment that enables gateway minting must configure
the applicable namespace epochs. The token itself is the issuance receipt; this
Tier 0 path does not maintain an issuance registry.

Because `sub` is caller-chosen, a trusted minting key can rotate subjects and
give each new `(namespace, subject)` a fresh budget ledger and per-subject
`max_in_flight_per_subject` allowance. `BudgetConfig::limit_microdollars` is
the cap per `(namespace, subject)`, so on its own it does not bound what a
`can_mint` key can spend: it can always move to a subject that has not spent
anything. `max_request_microdollars` limits one request rather than cumulative
spend.

**Bound the namespace, not just the subject.** Set
`[budget] namespace_limit_microdollars` on a `redis` or `postgres` budget in
every namespace where minting is enabled ([configuration](./configuration.md)).
It caps everything the namespace spends across all its subjects, so rotating
subjects no longer creates spend authority, and it is enforced exactly across
replicas. Without it, a `can_mint` key still has unbounded namespace spend
authority by construction. `in-memory` is not sufficient here: it would enforce
one cap per replica.

Even with the namespace cap, treat `can_mint` as operator-level trust, never hand
it to a downstream service, and keep the subject cap set as well so one subject
cannot consume the whole namespace. Request throttling would only slow subject
accumulation, because minted tokens outlive the request that created them, and
would not bound total spend. Keep `max_ttl` short.

```bash
curl -s https://gateway.example/v1/tokens -H "Authorization: Bearer $GW_INBOUND_PLATFORM_KEY" -H 'content-type: application/json' -d '{"sub":"agent-7","ttl_seconds":300,"scope":["chat"],"aliases":["gpt-4o"],"max_request_microdollars":500}'
# {"token":"axt1.…","exp":...,"expires_in":300,"namespace":"platform","sub":"agent-7"}
```

Enabling this puts the **private signing** key in the published config
snapshot on every replica that has minting enabled. A compromised minting
replica becomes a forgery capability for every namespace its verifier permits.
EdDSA otherwise allows verification-only replicas to hold only public key
material; HS256 never had that property because every verifier already holds
the forging secret. Keep offline
`axond mint` as the default; if HTTP minting is necessary, run it on dedicated
replicas that do not serve dispatch traffic, and reference the private signing
key only in that deployment's config. Keep `max_ttl` short.

## 1. Keep a static breakglass key

Minted verifiers are additive to static gateway keys. At least one
`[[gateway_key]]` is mandatory, even when every normal caller uses a minted
token. Keep that static key available to the operator for recovery, catalogue
access, and operational changes:

```toml
[[gateway_key]]
env = "GW_INBOUND_PLATFORM_KEY"
namespace = "platform"
```

The value is supplied through the gateway process environment:

```bash
export GW_INBOUND_PLATFORM_KEY='operator-breakglass-value'
```

Alternatively, mount the exact secret bytes and reference the path (do not add
a trailing newline):

```toml
[[gateway_key]]
file = "/run/secrets/axond-inbound-platform"
namespace = "platform"
```

Use `printf %s 'operator-breakglass-value' > /run/secrets/axond-inbound-platform`.
A trailing newline makes a file-backed static key unusable because HTTP headers
cannot carry it. Exactly one of `env` and `file` is permitted.

There is no keyless mode. A config without a static gateway key fails closed at
boot, and a missing referenced environment variable is a fatal error.

## 2. Generate and install an Ed25519 signer

Run `keygen` once on the machine that will mint tokens, capturing its output:

```bash
KEYGEN_OUTPUT="$(axond keygen \
  --private-key ./acme-signing.key \
  --kid acme-2026-08 \
  --env GW_VERIFY_ACME_2026_08 \
  --namespace acme \
  --max-ttl 15m)"
printf '%s\n' "$KEYGEN_OUTPUT"
# Copy the `export GW_VERIFY_ACME_2026_08='...'` line from the output into
# the gateway's environment manager.
```

`keygen` creates the private-key file with `create_new`; it never overwrites an
existing path. On Unix the file mode is `0600`. On non-Unix platforms the
command warns on stderr because permissions are inherited and must be
restricted by the operator. The file contains one line of standard-base64
PKCS#8 Ed25519 private-key material, followed by a newline.

Stdout contains only the standard-base64 raw Ed25519 public key and a
paste-ready `[[gateway_verifier]]` snippet. The private key is never printed.
Install the public half in the gateway environment under the name printed by
`--env`; the `printf` above displays the line to copy.

Do not put the private file on verification-only replicas.

Add the matching verifier and deployment audience to the gateway config. Keep
the static gateway key above in the same file:

```toml
[gateway_token]
audience = "acme-production"

[[gateway_verifier]]
kid = "acme-2026-08"
alg = "EdDSA"
env = "GW_VERIFY_ACME_2026_08"
namespaces = ["acme"]
max_ttl = "15m"
```

The `kid`, algorithm, permitted namespaces, and `max_ttl` are config-owned.
The public key value is environment-owned. The verifier must name an existing
namespace, and every declared verifier requires `[gateway_token].audience`.

### The trailing-newline trap

The generated private file ends with `\n`. Shell command substitution strips a
trailing newline, so this common local workflow works:

```bash
export GW_SIGN_ACME="$(cat ./acme-signing.key)"
```

Kubernetes Secret mounts, systemd `EnvironmentFile`, and Docker
`--env-file` can preserve that newline. Ed25519 base64 material is therefore
trimmed before decoding on both the mint and verify sides. Base64 whitespace is
transport noise. Do **not** apply the same rule to HS256: an HS256 secret is
opaque bytes, so its whitespace is data and its minimum length is checked on
the value exactly as supplied.

## 3. Mint and present a token

Read signing material by environment-variable name; never pass key material in
argv:

```bash
export GW_SIGN_ACME="$(cat ./acme-signing.key)"
TOKEN="$(
  axond mint \
    --config ./axond.toml \
    --kid acme-2026-08 \
    --alg EdDSA \
    --key-env GW_SIGN_ACME \
    --namespace acme \
    --subject customer-agent-1 \
    --ttl 10m
)"
```

With a matching usable config, `mint` defaults the audience from
`[gateway_token]`, and enforces the matching verifier's algorithm, namespace
permission, and `max_ttl`. It always enforces the 24-hour policy ceiling and a
minimum TTL of one second. `--audience` may be supplied explicitly, but it must
match the configured audience.

`mint` writes only the bare `axt1.` token to stdout. Diagnostics, including an
unloadable ambient `AXOND_CONFIG` warning, go to stderr. An explicit
`--config` load failure is fatal. If no config source is selected, minting uses
the policy ceiling alone; a token above the gateway verifier's configured
`max_ttl` can then mint and will be rejected when the gateway verifies it.
`mint` does not automatically load a cwd `axond.toml`; if one exists it only
prints a hint to pass `--config axond.toml`.

Present the token using the normal bearer credential framing:

```bash
curl --fail-with-body http://127.0.0.1:8080/v1/models \
  -H "Authorization: Bearer $TOKEN"
```

`x-api-key: $TOKEN` is also accepted. `/healthz` and `/readyz` are the only
unauthenticated routes. A valid token is namespace-scoped, so the catalogue
only contains aliases the `acme` namespace can resolve.

HS256 is supported for deliberate shared-secret deployments. Add this verifier
alongside the EdDSA verifier in `axond.toml` before running the command:

```toml
[[gateway_verifier]]
kid = "local-minter"
alg = "HS256"
env = "GW_VERIFY_LOCAL_MINTER"
namespaces = ["platform"]
max_ttl = "15m"
```

```bash
export GW_SIGN_LOCAL='01234567890123456789012345678901'
export GW_VERIFY_LOCAL_MINTER="$GW_SIGN_LOCAL"
TOKEN="$(
  axond mint --config ./axond.toml \
    --kid local-minter --alg HS256 --key-env GW_SIGN_LOCAL \
    --namespace platform --subject local-agent --ttl 10m
)"
```

HS256 is symmetric: `GW_SIGN_LOCAL` and `GW_VERIFY_LOCAL_MINTER` must contain
the same secret bytes. Every verifier holding those bytes can forge tokens;
prefer EdDSA for `[gateway_minting]` when verification-only replicas must not
be able to mint. As with any verifier, `GW_VERIFY_LOCAL_MINTER` must be present in
the gateway's environment before it starts; the export above only equips the
minting shell.

## 4. Token contract and claims

The compact JWS is framed as `axt1.<compact-jws>`. The JWS header requires:

- `kid`, selecting the configured verifier;
- `alg`, matching that verifier (`EdDSA` or `HS256`).

The verifier requires these claims:

| Claim | Meaning |
| --- | --- |
| `exp` | Expiry; always present and required. |
| `iat` | Issued-at time; required and bounded against `exp` and the verifier clock. |
| `aud` | Deployment audience; must match `[gateway_token].audience`. |
| `jti` | Non-empty token identifier; required even in Tier 0. |
| `ns` | Existing namespace permitted by the selected verifier. |
| `sub` | Non-empty caller subject used for budgets and usage attribution. Caller-chosen, so the namespace cap is the spend boundary. |

`nbf` is optional and, when present, is checked with the verifier's fixed
five-second clock-skew allowance. Unknown claims are otherwise ignored by the
current verifier.

`max_request_microdollars` is an optional admission-time per-request ceiling
in microdollars. The gateway compares it with the pre-dispatch estimate before
reservation; a cumulative per-token cap is not stateless and belongs to the
Tier 1 work in ADR 0017. An unlimited token omits the claim: a ceiling of `0`
admits nothing, because any priced request estimates above it. Estimates are
whole microdollars, so a cheap request against a cheap alias can estimate `0`
and pass any ceiling. It is enforced at admission time and emitted by
`axond mint --max-request-microdollars`.

ADR 0016 describes three narrowing claims. `scope` is enforced by route
capability and can be emitted with repeatable `--scope` flags:
`chat`, `messages`, `embeddings`, `responses`, `models`, `credentials`, and
`status`. `status` names the authenticated dependency-status view of
[ADR 0031](./adr/0031-bounded-status-contract.md) and grants nothing else; it is
the only capability that does not require the namespace to have a model route,
and its route ships with the stateful slices. It is also the only capability
`POST /v1/tokens` does **not** grant when the request names no `scope`: that
route writes a concrete claim, and it now writes every route capability except
`status`, so a monitoring grant from the minting route has to be asked for rather
than inherited. A configured `[gateway_minting] scope` ceiling is still what a
scope-less request inherits, so a ceiling that lists `status` grants it: the
exclusion is the *default*, not an override of the deployment's own ceiling.
`axond mint` is not that route. Omitting `--scope` omits the
claim, and an unscoped token is unrestricted under
[ADR 0019](./adr/0019-scoped-route-capabilities.md) — it satisfies `status` like
every other capability. A monitoring token from the command line is therefore
`axond mint --scope status`, not a scope-less mint.
A minted token cannot reach the all-namespaces credential view
(`GET /v1/credentials?namespaces=all`) at all: that view follows direct
operator authority, so use a scope-less static `[[gateway_key]]` in the default
namespace for it. Signing a `credentials:all` claim yourself does not help: the
gateway denies it with `403 token_scope_insufficient`, and `POST /v1/tokens`
refuses to mint it ([ADR 0021](./adr/0021-credential-status-endpoint.md)).
The `aliases` claim is enforced
both before dispatch and in the caller's `/v1/models` view, and is emitted by
`axond mint --alias`. It is a repeatable, case-sensitive pattern restriction.

```bash
axond mint --kid acme-2026-08 --key-env SIGN_KEY --namespace acme \
  --subject alice --ttl 15m --config axond.toml \
  --alias "gpt-4o" --alias "claude-*"
```

The claim is omitted when no `--alias` is supplied. Its exact glob syntax,
including fail-closed empty-array and invalid-pattern behavior, is documented
under `[[gateway_verifier]]` in [configuration.md](./configuration.md).

## 4.5. Precise single-token revocation

With an optional denylist configured, revoke a token by its `jti` without
placing token or key material on the command line:

```bash
axond revoke --jti TOKEN_JTI --ttl 15m --config axond.toml
```

`axond revoke` also accepts `--expires-at` as Unix seconds or RFC3339 UTC.
Without either flag it uses the largest configured verifier lifetime plus the
clock-skew allowance. This command is the complete operator surface; a full
administrative/control-plane API is out of scope.

Choose an explicit `--ttl` or `--expires-at` at or beyond the token's `exp`;
the no-flag default is recommended because a shorter entry silently stops
revoking the token when it expires.

## 5. Rotation runbook

File-backed verifier material is re-read when a candidate snapshot is built, so
rotation can happen without a process restart. The static breakglass key remains
present throughout. Use `printf %s` for HS256 and static gateway-key files:
their bytes are exact and a trailing newline changes the secret. Ed25519
base64 is trimmed before decoding, so its generated trailing newline is fine.

### In-place replacement under the same `kid`

1. Provision the replacement public key or secret to a temporary file, then
   atomically replace the configured path:

   ```bash
   printf %s "$NEW_PUBLIC_KEY" > /run/secrets/acme-verifier.next
   mv /run/secrets/acme-verifier.next /run/secrets/acme-verifier
   ```

2. Keep the same `kid` and send the reload signal:

   ```bash
   kill -HUP "$AXOND_PID"
   ```

3. Confirm the applied reload log reports the `kid` in `gateway_verifiers`
   `changed` and includes a different short fingerprint. The log contains
   fingerprints, never key material. Fingerprints are comparable only within
   one process lifetime: they show that material changed at this reload, but
   are not stable identifiers for a key.

A failed candidate (for example, a deleted, empty, unreadable, or corrupted
file) is rejected atomically and the previous snapshot keeps serving. Restore
the file and send SIGHUP again. A corrupted Ed25519 base64 value is rejected
during snapshot construction; an HS256 value shorter than 32 bytes is rejected
the same way.

### Overlap rotation with a new `kid`

For a graceful overlap, provision a new signer and file, then add a second
verifier alongside the old one:

```toml
[[gateway_verifier]]
kid = "acme-2026-11"
alg = "EdDSA"
file = "/run/secrets/acme-2026-11-public"
namespaces = ["acme"]
max_ttl = "15m"
```

Send SIGHUP, confirm the new `kid` is active, and switch minting to it. Wait
for old tokens to expire, or remove the old verifier entry to revoke them, then
send SIGHUP again. Both the verifier and static `[[gateway_key]]` sections
support exactly one of `env` and `file`; file sources are re-read at every
reload.

## 6. Revocation ladder

Tier 0 has no precise per-token revocation state. Use these controls from
coarsest to most targeted:

1. **Short TTLs.** Limit the lifetime and wait for tokens to expire.
2. **Remove a `kid`.** Reloading without a verifier revokes every token signed
   by that `kid`.
3. **`min_iat` epoch (#63).** Set a namespace-wide or per-subject issuance
   epoch to reject tokens issued before that instant. This is a shipped Tier 0
   config control and applies on reload:

   ```toml
   [[gateway_token_epoch]]
   namespace = "acme"
   min_iat = "2026-08-10T12:00:00Z"
   ```

   Edit the config and send `SIGHUP`:

   ```console
   $ kill -HUP "$(pidof axond)"
   ```

   The next snapshot rejects older `acme` tokens with
   `token_issued_before_epoch` and leaves other namespaces unchanged. A
   namespace-wide epoch affects every subject in that namespace; to spare one
   subject, add a per-subject entry with an earlier `min_iat`, which overrides
   the namespace-wide entry for that subject.
   A future epoch also blocks in-gateway issuance for the affected namespace
   and subject, so the gateway does not mint tokens it would immediately
   reject with `token_issued_before_epoch`.

To revoke in-gateway issuance entirely, either remove `[gateway_minting]` or
set every gateway key's `can_mint` to `false`. Removing the section takes effect
on reload, leaves the boot-registered handler returning typed 404
`minting_disabled`, and makes any remaining `can_mint = true` flags inert;
reload logs name those keys. Clearing the final `can_mint` flag leaves the
section in place but makes the endpoint reject every caller with
`mint_not_authorized`; reload logs a warning when minting is configured without
an authorized key.

4. **`jti` denylist (#68).** The optional denylist can reject one token by its
   mandatory `jti`. It is separate from the stateless mint endpoint and uses
   the configured shared revocation backend; its availability and fail-closed
   behavior must be treated as part of the selected request path.

## 7. Delegation and attribution

To delegate access to a customer, create a signer whose verifier entry permits
only that customer's namespace:

```toml
[[gateway_verifier]]
kid = "customer-acme-2026-08"
alg = "EdDSA"
env = "GW_VERIFY_CUSTOMER_ACME_2026_08"
namespaces = ["acme"]
max_ttl = "15m"
```

Give the customer the private signing half, not the gateway's static key or
provider credentials. The customer can mint subjects within `acme`, but cannot
make the verifier authorize another namespace: both the `ns` claim and the
configured verifier namespace set are checked.

Usage records for minted callers include `signer_kid`, so spend and activity
can be attributed to the signer that vouched for the subject. Static
`[[gateway_key]]` callers have no JWS signer and therefore omit `signer_kid`.
The subject remains the caller-controlled `sub`; do not treat it as a
replacement for signer attribution.

## 8. State tier summary

- **Tier 0:** config-only static keys and minted-token verification; no runtime
  datastore dependency. This is the default and retains static breakglass.
- **Tier 1 (Redis):** exact shared budgets, inbound rate limits, and precise
  revocation.
- **Tier 2 (Postgres):** durable audit and self-serve identity/key lifecycle.

Namespaces, aliases, provider credentials, and routing remain config-owned.
Stateful identity features must not silently replace that authority.
