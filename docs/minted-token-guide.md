# Minted inbound identity: operator guide and rotation runbook

This guide covers the Tier 0, config-only minted identity path. It uses the
same `axond` binary for key generation, offline minting, and gateway
verification. The gateway does not keep an issuance registry and verification
does not add Redis, Postgres, or another runtime dependency.

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
the same secret bytes. Every verifier holding those bytes can forge tokens,
which is why Ed25519 is preferred when verification-only replicas must not be
able to mint.

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
| `sub` | Non-empty caller subject used for budgets and usage attribution. |

`nbf` is optional and, when present, is checked with the verifier's fixed
five-second clock-skew allowance. Unknown claims are otherwise ignored by the
current verifier.

ADR 0016 describes three future narrowing claims:
`scope`, `aliases`, and `max_request_microdollars`. They are **not enforced by
the current gateway and are not emitted by `axond mint`**. Do not add them by
hand and assume a token is narrowed: it is not. Their enforcement and minting
are tracked by #60, #61, and #62 respectively.

## 5. Rotation runbook

Minted verifier rotation is a restart operation today, not a zero-downtime
reload. The running process cannot gain a new environment variable: exporting
`GW_VERIFY_ACME_2026_11` in an operator shell does not add it to an already
running gateway. This limitation is tracked in [#86](https://github.com/Litvue/axond/issues/86).
The static breakglass key remains present throughout:

1. Generate or provision the new signer with a new `kid`, for example
   `acme-2026-11`, and provision its public key in the gateway's environment
   manager under a new variable such as `GW_VERIFY_ACME_2026_11`.
2. Add the new `[[gateway_verifier]]` entry alongside the old entry. Keep the
   same audience and namespace permissions unless the change is intentional.
3. Start or restart the gateway with the new environment variable and both
   verifier entries present. Confirm the new verifier is active.
4. Switch all minting over to the new `kid`.
5. Wait for tokens signed by the old `kid` to expire, or remove the old
   verifier entry immediately to revoke them, then reload:

   ```bash
   kill -HUP "$AXOND_PID"
   ```

The candidate passes the full boot validation before an atomic snapshot swap.
A failed reload leaves the previous config serving. The applied reload log
reports added, removed, and definition-changed verifier `kid` values, plus the
audience delta. It does not report public-key or secret values.

Important rotation trap: the verifier definition diff compares the configured
`kid`, algorithm, env-var **name**, permitted namespaces, and `max_ttl`; it does
not inspect key material. Changing the public-key value under the same `kid`
therefore takes effect only when the gateway starts or restarts, and a later
reload can still report `changed=false`. Same-name key-material rotation is
not visible in that summary; use a new `kid` for an observable, overlap-safe
rotation.

## 6. Revocation ladder

Tier 0 has no precise per-token revocation state. Use these controls from
coarsest to most targeted:

1. **Short TTLs.** Limit the lifetime and wait for tokens to expire.
2. **Remove a `kid`.** Reloading without a verifier revokes every token signed
   by that `kid`.
3. **`min_iat` epoch (#63).** A future config/state control can reject tokens
   issued before a namespace or subject epoch.
4. **`jti` denylist (#68).** A future opt-in denylist can reject one token by
   its mandatory `jti`.

The `min_iat` and denylist controls are not current Tier 0 configuration
features. Precise single-token revocation requires **Tier 1** shared state,
as defined by ADR 0017; in this design that means Redis-backed enforcement.
Tier 1 availability and its fail-closed behavior must be treated as part of
the selected request path.

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
