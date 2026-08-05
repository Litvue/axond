# 13. Inbound authentication fails closed

Date: 2026-08-05

## Status

Accepted

## Context

Inbound caller authentication was permissive in two places at once.

A declared `[[gateway_key]]` whose `env` var was unset or empty was **silently
dropped** while the snapshot was built, and the request path treated an *empty*
key table as "allow all": every caller was admitted, attributed to the default
namespace under the subject `anonymous`. The two behaviours compose into the
worst possible outcome. A deploy that rotated `GW_INBOUND_ACME_KEY` and mistyped
the new variable's name did not fail — it removed authentication from a running
gateway, opened every namespace's credentials and budget to anyone who could
reach the port, and said nothing beyond an attribution field changing value.

The rest of the config surface already takes the opposite stance: an alias
pointing at an undefined provider, an unpriced target, a `[[credential]]` whose
env var is missing, a budget that could not enforce anything — each refuses to
boot (delta B2), and since ADR 0011 a *reload* candidate is held to the same gate
and rejected-and-kept on any failure. Inbound auth was the one part of the graph
where a missing reference degraded quietly instead of failing loudly, and it was
the part where degrading meant *less* security rather than a broken route.

The keyless mode existed for local development convenience: `cargo run` against
the example config, `curl` without a header. That is a real convenience, but it
is bought with a posture that cannot be distinguished at runtime from a
misconfigured production deploy.

## Decision

**Inbound authentication always fails closed. There is no keyless mode, and no
flag that reintroduces one.** Concretely:

- A declared `[[gateway_key]]` whose `env` var is unset or empty is a **fatal**
  error while the config snapshot is built (`SnapshotError::MissingGatewayKey`),
  naming the offending env var and its namespace. It is never dropped.
- A config with **no** `[[gateway_key]]` fails `Config::validate` — a gateway
  nobody could authenticate against is a configuration mistake, not a mode.
  `ConfigSnapshot::build` additionally refuses to publish a snapshot whose
  resolved key table is empty, so the invariant holds for any future path that
  builds one.
- Two keys may not resolve to the same secret
  (`SnapshotError::DuplicateGatewayKey`). The key table is keyed by the secret, so
  a shared value would silently drop one declared key and serve its callers under
  another namespace's authority.
- `authenticate` has no empty-table branch: every request to a route that
  dispatches to a provider presents a configured key, as `Authorization: Bearer`
  or `x-api-key` (ADR 0012), or gets `401`. The operational endpoints —
  `/healthz`, `/readyz`, and the `/v1/models` alias catalogue — stay
  unauthenticated, as they were: they name no credential and reach no provider.
- Boot logs the enforced posture as a count (`inbound auth enforced`,
  `gateway_keys = N`). There is no "anonymous access enabled" line, because that
  state no longer exists.

Because reload re-runs exactly this validation (ADR 0011), a candidate whose
gateway-key env var cannot be resolved is **rejected and the running config keeps
serving** — a botched key rotation fails at reload with a named error rather than
disarming a live gateway.

Errors and logs name **references only**: env-var names, namespaces, and counts.
A secret's value never appears, so a fatal boot error is safe to page on.

Outbound provider-credential resolution is untouched: `[[credential]]` pools keep
their own resolution and `allow_platform_fallback` semantics (ADR 0003, ADR
0006). This decision is about who may call the gateway, not which key the gateway
calls a provider with.

## Consequences

- A local run now needs one env var and one header. `axond.example.toml` ships a
  `[[gateway_key]]` and the README's `curl` carries a bearer token, so the
  documented path stays copy-pasteable.
- Any config that relied on the implicit keyless mode fails at boot with a
  message that says what to add. This is a deliberate breaking change during
  beta, and the failure is loud, immediate, and non-secret.
- Usage records no longer carry the `anonymous` subject: every record's `subject`
  is the gateway key's label (its env-var name), so attribution is always to a
  declared identity.
- The gateway is worth exactly one key's worth of trust: keys are still coarse
  (one secret → one namespace) and there is no rotation window in which two
  values for the *same* declared key are accepted. Overlapping rotation is done
  the way the config already allows — declare the new key alongside the old
  (with a *different* value), reload, then drop the old one.
