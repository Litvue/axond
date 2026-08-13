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
| Service | A grant is a role at a scope, and scope containment is one-directional, so a tenant-scoped administrator's request against another tenant is a refusal with a recorded reason and an opaque answer | `desired_state::access`, `admin::service`, `tenant_isolation::control_plane` |
| Storage | Hydration re-checks every stored dependency edge with a join, so a row written around the domain is still refused | `backends::control_plane::hydration`, `backends::control_plane::postgres` |
| Database | Row-level security keyed on `axond.tenant_id`, so a session pinned to one tenant cannot read or write another's rows even with the service layer bypassed | `sql/control_plane_0002_tenancy_access.sql`, `tenant_isolation::database` |
| Database (constraints) | Ownership is a constraint, not a convention: a project, principal, or projected row must name the tenant that owns it, and a revision, mutation, or audit event must name a tenant this deployment has a row for, so a row written around every layer above is still refused | `sql/control_plane_0003_tenancy_constraints.sql`, `sql/control_plane_0004_journal_ownership.sql`, `backends::control_plane::postgres` |
| Catalogues | Credential, model, and policy lookups resolve within the asking tenant's own scopes, including when two tenants enable the same offering | `tenant_isolation::catalogue` |
| Projection | A project becomes a *tenant-qualified* runtime namespace (`acme/core`), so two tenants' identically named projects cannot collapse into one | `convergence::tenancy`, `config::validate_namespace_ids`, `tenant_isolation::projection` |
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
| Every event the billing-grade outbox journals is ordered under, and names, only the namespace that spent it | `journaled_usage_events_never_cross_a_namespace` |
| One tenant exhausting its namespace budget does not deny another | `one_tenants_exhausted_budget_does_not_deny_another` |

The outbox is asserted separately from the row sink because it is a second
durable usage path with its own key: an event is appended before the response and
claimed for delivery per `(namespace, subject)`, so a journal keyed on anything
coarser would order one tenant's events behind another's and hand a consumer a
billable fact attributed to the wrong tenant. That boot runs the outbox in a
schema of its own, dropped with the boot.

The last three need a Postgres. They skip when `AXOND_TEST_POSTGRES_DSN` is unset
and are mandatory in CI, which sets `AXOND_TEST_REQUIRE_SERVICES=1`; the
stateless cases run everywhere. Each stateful boot creates its own usage and
budget objects, owned by a value that exists before the process that creates them
and dropped with it — after a failed assertion, and after a boot that panicked
before it ever served, which
`a_boot_that_fails_after_starting_still_drops_what_it_created` arranges and
checks — so concurrent runs share a database without sharing rows and a
long-lived one does not accumulate them.

## Database, service, and projections: the stateful control-plane suite

[`crates/gateway/src/tenant_isolation/`](../../crates/gateway/src/tenant_isolation/mod.rs)
is the half a black-box suite cannot see. Every scenario publishes two tenants
who own a project, principals, credentials, aliases, policy, and a model
enablement of the same offering into a real PostgreSQL journal on a schema of its
own, and then asks a question of one layer.

Each is stated in both directions: the negative assertion names the other
tenant's *exact* identifiers, and the same scenario asserts the positive — the
unpinned publisher reads what the pinned session could not, the deployment-scoped
grant reads the projection the tenant-scoped one was refused, and the other
tenant's durable rows are compared before and after a refused write. An absence
that holds because the fixture never created the row is a test that cannot fail.

| Property | Layer | Test |
| --- | --- | --- |
| A session pinned to one tenant reads nothing of the other's, swept over every table and every stored resource body | Database (RLS) | `nothing_a_pinned_session_can_read_names_the_other_tenant` |
| The same session cannot insert rows owned by another tenant, and its updates and deletes against another tenant's rows match nothing | Database (RLS) | `a_pinned_session_cannot_write_another_tenants_rows` |
| A tenant-scoped administrator cannot publish into another tenant, or publish a candidate that reaches a foreign credential; the refusal is recorded per tenant and nothing durable moves | Service | `a_tenant_scoped_administrator_cannot_publish_into_another_tenant` |
| A cross-tenant reference is refused as a validation failure that names the caller's own resource, keeping what it reached for to the operator detail | Domain/service | `a_cross_tenant_reference_names_the_caller_and_not_what_it_reached_for` |
| Deployment-wide desired state, history, and audit reads require deployment authority, and the refusal names nothing of the deployment | Service | `a_tenant_scoped_administrator_reads_no_deployment_wide_projection` |
| A rehearsal is authorized like the mutation it rehearses, so a dry run is not a cheaper way across the boundary | Service | `a_rehearsed_cross_tenant_mutation_is_refused_like_a_real_one` |
| A tenant's credentials are its own, and a secret resolves as the owner the revision gives it rather than as whoever asked | Catalogues | `a_tenants_credentials_are_never_another_tenants` |
| Two tenants enabling the same offering hold two enablements, and each project's aliases resolve to its own tenant's | Catalogues | `the_same_offering_enabled_twice_is_two_tenants_models` |
| Policy fallback climbs to the *named* tenant, so naming another tenant's project does not carry that tenant's budget across | Catalogues | `a_tenants_policy_governs_only_its_own_scopes` |
| Each project projects to its own tenant-qualified namespace, carrying its durable identity, with platform fallback off | Projection | `each_tenants_project_is_its_own_namespace_and_borrows_nothing` |
| Adding a tenant changes nothing about an existing tenant's namespace | Projection | `a_neighbour_changes_nothing_about_a_tenants_own_namespace` |

Every scenario requires PostgreSQL and returns early without a DSN, which in CI
would be a hole: `AXOND_TEST_REQUIRE_SERVICES=1` turns a missing DSN into a
panic, so the stateful lane cannot report green having run none of them.

One current behaviour is asserted rather than assumed, because it reads like a
leak and is not one: a *tenant declaration* is stored in the journal as a
deployment-scoped row (`tenant_id IS NULL`), which migration 0002's policies
admit for a pinned session, so the raw journal does name the other tenant's id
and slug. Nothing tenant-owned — projects, principals, credentials, secrets,
policy — is visible, and enumerating tenants through an administrative read is
refused a layer above by `a_tenant_scoped_administrator_reads_no_deployment_wide_projection`.
That is where the property lives; the test says so instead of hiding it.

The same asymmetry holds for writes, and is asserted the same way: the policies
that admit an ownerless row on the way in admit a pinned session *appending* one,
so `a_pinned_session_cannot_write_another_tenants_rows` writes a deployment-scoped
resource version and then shows it buys nothing — a version is desired state only
once a revision carries it, the publication chain admits no pinned session at
all, and no ownership row appears for the forgery.

## Not covered yet, and why

These are the parts of [#225] that cannot be written against the current
runtime. Each names the change that unblocks it.

| Assertion | Blocked on | Why |
| --- | --- | --- |
| A *tenant-scoped human* administrator is refused another tenant's resources over an authenticated `/admin/v1` request | [#143] — tenant-scoped admin authentication on the served surface | The stateful runtime authenticates a deployment-scoped breakglass credential, and the projections `/admin/v1` serves are of the whole deployment by construction. The scenarios above therefore decide authorization at the grant seam a tenant-scoped authenticator will hand the service — which is the same seam the request path will use — rather than fabricating an authenticated HTTP flow that no deployment can currently make |
| Tenancy isolation holds over *durable* projects and credentials on the request path rather than configured namespaces | The convergence slice that wires desired state into `serve` — tenant-routed catalogue and alias inference is [#148] and [#149] | No revision is loaded on the request path, so a served namespace is one the config declared. The two ends are pinned instead: the projection from durable state is asserted here, and the runtime suite asserts the same *shape* of namespace id it produces, which is what keeps them from drifting before they are joined |
| A cross-tenant alias is refused indistinguishably from a model that does not exist | A runtime error-contract decision, not a test | Today an alias belonging to another tenant is refused `502 no_credential` while an unknown model is a 404-class `model_not_found`, so status alone is an alias-existence oracle across tenants. No identifier, credential, or key of the other tenant is disclosed and nothing is dispatched upstream; changing which refusal is returned is the security owner's call and is tracked on [#225] |

When each lands, the corresponding row moves into the table above with the test
that proves it. The suite is structured for that: the harness in
[`crates/gateway/tests/support/tenancy.rs`](../../crates/gateway/tests/support/tenancy.rs)
describes the deployment rather than any one case, and
[`crates/gateway/src/tenant_isolation/harness.rs`](../../crates/gateway/src/tenant_isolation/harness.rs)
describes the two tenants' durable state and the sessions that read it, so a new
assertion is a test against tenants that already exist.

[#143]: https://github.com/Litvue/axond/issues/143
[#148]: https://github.com/Litvue/axond/issues/148
[#149]: https://github.com/Litvue/axond/issues/149
[#225]: https://github.com/Litvue/axond/issues/225
