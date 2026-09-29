# Tenant isolation: what is proven, and where

Isolation between tenants is the property axond is most expected to hold. This
page maps each part of it to the mechanism that enforces it and the test that
would fail if it stopped. The tenant unit is the namespace: an API-created row
in the store, selected by the canonical `/ns/{ns}/v1` path
([ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md)).

## The layers

| Layer | Mechanism | Where the evidence is |
| --- | --- | --- |
| Routing | The namespace comes from the authenticated `/ns/{ns}/v1` path, and an unknown namespace is refused before dispatch | `routes::auth`, `routes::mod` |
| Credentials | Pool lookup is keyed on the caller's namespace; platform fallback is explicit and attributed | `credentials` |
| Store | Namespace, budget, and usage rows are keyed on the namespace | `store::sqlite`, `store::postgres` |
| Runtime | Catalogue, credential selection, and accounting are keyed on the caller's namespace | `routes::mod` unit tests |

## Withdrawn layers

The control-plane layers this page used to map (tenant and project domain
scopes, the administrative authorization service, row-level security, and the
tenancy constraints in the control-plane SQL) were withdrawn with
[ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md) and have no
mechanism left to test. So was the binary-level isolation suite
(`crates/gateway/tests/tenant_isolation.rs`): it exercised per-key namespace
isolation, minted tokens, and per-namespace aliases, none of which the one
static gateway key has.

