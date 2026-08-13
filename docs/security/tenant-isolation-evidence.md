# Tenant isolation: what is proven, and where

Isolation between tenants is the property axond is most expected to hold and the
one least visible from any single test: it is enforced in three different places
by three different mechanisms, and a claim about it is only as good as the layer
it was checked at. This document is the map — for each part of the property, the
mechanism that enforces it and the test that would fail if it stopped.

It is also the honest half of the record. Some of the isolation surface issue
[#225] asks for does not exist in the runtime yet, and those rows say so, with
the change that would let them be written. A regression suite that asserted them
today would have to assert them against a runtime that has no such surface, and
a green test for an absent mechanism is worse than a missing one.

## The layers

| Layer | Mechanism | Where the evidence is |
| --- | --- | --- |
| Domain | A resource carries its scope, so a reference across a tenant boundary is refusable without reading a body | `desired_state::resource`, `desired_state::revision` |
| Domain (credentials) | A credential's secret and provider must be reachable from its own owner's scope | `desired_state::credentials` |
| Storage | Hydration re-checks every stored dependency edge with a join, so a row written around the domain is still refused | `backends::control_plane::hydration`, `backends::control_plane::postgres` |
| Projection | A project becomes a *tenant-qualified* runtime namespace (`acme/core`), so two tenants' identically named projects cannot collapse into one | `convergence::tenancy`, `config::validate_namespace_ids` |
| Runtime | Catalogue, credential selection, and accounting are keyed on the caller's namespace | `crates/gateway/tests/tenant_isolation.rs` |

## Runtime: the stateful isolation suite

[`crates/gateway/tests/tenant_isolation.rs`](../../crates/gateway/tests/tenant_isolation.rs)
boots the shipped binary with two tenants (`acme/core`, `globex/core`), a
namespace that owns no credential and opts in to platform fallback
(`initech/core`), and the platform's own pool. Every provider credential is a
fixture value pointing at the fake upstream, which records what it was sent, so
each assertion is made from outside the process.

| Property | Test |
| --- | --- |
| A tenant's catalogue is what its own credentials can serve, never the deployment's alias list | `a_tenant_sees_only_the_models_its_own_credentials_can_serve` |
| Naming another tenant's alias is refused, before the provider is reached, and the refusal names nothing of the other tenant's | `a_tenant_cannot_invoke_another_tenants_alias` |
| Every provider request carries the calling tenant's credential and no other, under interleaved traffic | `a_provider_request_carries_only_the_calling_tenants_credential` |
| Platform fallback serves only the namespace that opted in, and the usage record attributes the spend to the platform pool | `platform_fallback_is_explicit_and_attributed` |
| Durable usage rows are partitioned by namespace | `usage_rows_never_cross_a_namespace` |
| One tenant exhausting its namespace budget does not deny another | `one_tenants_exhausted_budget_does_not_deny_another` |

The last two need a Postgres. They skip when `AXOND_TEST_POSTGRES_DSN` is unset
and are mandatory in CI, which sets `AXOND_TEST_REQUIRE_SERVICES=1`; the
stateless cases run everywhere. Each stateful boot creates its own usage and
budget objects and drops them when the deployment is dropped — including after a
failed assertion — so concurrent runs share a database without sharing rows and
a long-lived one does not accumulate them.

## Not covered yet, and why

These are the parts of [#225] that cannot be written against the current
runtime. Each names the change that unblocks it.

| Assertion | Blocked on | Why |
| --- | --- | --- |
| A principal of one tenant is refused a project, credential, or alias of another at the service layer | [#144] — durable principals, roles, and `Directory::authorize` | There is no principal in the runtime today: an inbound caller is a gateway key bound to a namespace in the config, so there is nothing to authorize *as*, and no denial to record |
| A cross-tenant read is refused by the database itself, not only by the query that asked | [#144] — row-level security keyed on `axond.tenant_id` | The shipped control-plane schema has ownership constraints and the hydration join, but no RLS policy, so "the DB refuses it" cannot be asserted without asserting the absence of a policy that is not there |
| No cross-tenant visibility through the admin surface | [#143] — the `/admin/v1` resource handlers | [#200]'s protocol and service boundary has landed, but it is contract-only: the route table is empty and `serve` mounts nothing, so the runtime still answers on the inference surface alone and there is no administrative read to scope |
| Tenancy isolation holds over *durable* projects and credentials rather than configured namespaces | The convergence slice that wires desired state into `serve` | `desired_state` and `convergence` are contract-only: no revision is loaded on the request path, so the runtime's namespaces are the ones its config declares. The projection that makes a project a tenant-qualified namespace is tested where it lives (`convergence::tenancy`), and the runtime suite above exercises the same *shape* of namespace id, which is what keeps the two from drifting before they are joined |

When each lands, the corresponding row moves into the table above with the test
that proves it. The suite is structured for that: the harness in
[`crates/gateway/tests/support/tenancy.rs`](../../crates/gateway/tests/support/tenancy.rs)
describes the deployment rather than any one case, so a new assertion is a test
against the tenants already booted.

[#143]: https://github.com/Litvue/axond/issues/143
[#144]: https://github.com/Litvue/axond/issues/144
[#200]: https://github.com/Litvue/axond/issues/200
[#225]: https://github.com/Litvue/axond/issues/225
