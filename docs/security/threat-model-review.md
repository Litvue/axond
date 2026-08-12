# Threat-model review triggers

Audience: maintainers and reviewers of a pull request. This page answers one
question — *does this change require a security review, and what does that review
owe?* [`SECURITY.md`](../../SECURITY.md) covers the other direction: what happens
when somebody reports a vulnerability to us. Neither replaces the other, and this
page adds no new disclosure process.

The reasoned baseline is the [security review](../security-review-2026-08-05.md)
and the trust boundaries in the
[deployment security model](./deployment-model.md). Those documents are true of a
particular tree. A change to the code they reason about either preserves their
conclusions or invalidates them, and the difference is not visible from a diff's
line count: a five-line change to claim validation moves a trust boundary, while
a thousand-line refactor of provider wire parsing may not touch one.

## What a fired trigger owes

Three things, in the same pull request:

1. **Regression tests.** A test that fails without the change's security
   property and passes with it. The named tests under each trigger are the
   existing floor — extend them or add beside them; do not weaken them to make a
   diff pass. A security-relevant change with no test is not finished, the same
   rule [`SECURITY.md`](../../SECURITY.md) applies to a fix for a report. The
   names below are checked mechanically: `ops/check-docs.py` fails if a test this
   page names no longer exists, so a rename updates the page rather than
   hollowing it out.
2. **Threat-model or ADR updates.** Either the reasoning in the security review
   and the deployment security model still holds and you say so in the PR, or it
   does not and you update it. A change to a boundary, an availability stance, or
   a state tier is a [significant decision](../../CONTRIBUTING.md#conventions)
   and gets an ADR from the [template](../adr/template.md) in the same PR.
3. **Release-impact review.** State whether the change alters what an operator
   must do or know at upgrade: a configuration key, a default, a typed error, a
   schema, a permission, or an artifact. That statement drives the
   [compatibility contract](../compatibility.md), the changelog, and the
   migration notes the [release runbook](../maintainers/releasing.md) reads.
   Pre-1.0, a break is a **minor** bump and cannot ride in a patch.

"No trigger fired" is a legitimate and common review outcome. Say it explicitly
rather than leaving it unstated — an unmentioned trigger looks the same as an
unnoticed one.

## Trigger index

| Change touches | Trigger |
| --- | --- |
| `routes.rs` authentication, `mint.rs`, `principals.rs`, `revocation/`, scopes, claims, epochs | [Authentication, claims, and authorization](#1-authentication-token-claims-and-authorization) |
| Namespace resolution, `credentials.rs` pool lookup, `allow_platform_fallback`, budget/rate-limit keys, operator views | [Tenant and namespace scoping](#2-tenant-and-namespace-scoping) |
| `backends/secrets.rs`, `key_material.rs`, credential injection, error and log text, rotation | [SecretStore, credential delivery, rotation, and redaction](#3-secretstore-credential-delivery-rotation-and-redaction) |
| `backends/catalog.rs`, `aliases.rs`, `/v1/models`, alias scope, wire families, pricing | [Catalogue and model entitlement](#4-catalogue-and-model-entitlement) |
| `ops/postgres/`, `crates/gateway/sql/`, `usage/`, `telemetry/`, control-plane journal | [Persistence, migrations, telemetry, and usage](#5-persistence-migrations-telemetry-and-usage) |
| `.github/workflows/`, `ops/publish-crates.sh`, `install.sh`, `install.ps1`, `Dockerfile`, `deny.toml` | [Actions, release permissions, attestations, and signing](#6-actions-release-permissions-attestations-and-signing) |

A change can fire more than one trigger; a credential-delivery change that also
adds a Postgres table fires two, and owes both sets.

## 1. Authentication, token claims, and authorization

**Fires on** any change to how a request is authenticated or what it is then
allowed to do: the `authenticate` path in `crates/gateway/src/routes.rs`; minting
and verification in `crates/gateway/src/mint.rs`; principal resolution and shape
ownership in `crates/gateway/src/principals.rs`; the capability set and its
mapping to routes; a new or renamed claim, scope, audience, or epoch rule; a new
authenticated route; anything that widens what a minted token can reach; and
`crates/gateway/src/revocation/` including its `on_unavailable` stance.

**Regression tests.** The fail-closed floor is
`every_authenticated_route_rejects_a_request_without_a_gateway_key` — a new route
belongs in it, not beside it. Scope narrowing is held by
`scoped_models_token_allows_models_and_denies_chat`,
`scoped_chat_token_allows_chat_and_denies_embeddings`,
`scoped_token_cannot_grant_a_route_the_namespace_lacks`, and
`scope_less_tokens_and_static_keys_reach_all_provider_routes`; privilege
separation by `static_minting_key_mints_but_minted_token_cannot_mint`. Issuance
policy lives in `mint.rs`: `mint_rejects_ttl_above_policy_ceiling`,
`mint_rejects_namespace_not_permitted_by_matching_verifier`,
`mint_rejects_unknown_scope_capability`, and
`mint_with_different_kid_is_rejected_by_verifier`. Revocation must stay
distinguishable per its typed error —
`an_epoch_rejected_token_returns_a_distinct_401_error_code`,
`denylisted_minted_token_returns_token_revoked`, and
`revocation_store_allow_admits_the_minted_token` for the deliberate fail-open.
A new claim needs a test that an *unknown or absent* value denies rather than
defaults open.

**Threat model and ADRs.** [ADR 0013](../adr/0013-inbound-auth-fails-closed.md)
(no keyless mode), [ADR 0016](../adr/0016-minted-inbound-identity-and-principal-stores.md),
[ADR 0019](../adr/0019-scoped-route-capabilities.md), and
[ADR 0022](../adr/0022-opt-in-gateway-token-minting.md) are the accepted
positions; a change that contradicts one supersedes it in a new ADR rather than
editing it. Sections 2 and 8 of the security review, and the inbound
authentication section of the [deployment security model](./deployment-model.md),
are the statements to re-confirm or amend.

**Release impact.** A claim, scope, or audience change is a token-format change:
tokens minted before the upgrade must still verify, or the migration note must
say they will not and the [minted-token guide](../minted-token-guide.md) must
document the rotation. A new typed `401`/`403` error code is part of the
[compatibility contract](../compatibility.md). Anything that could reject a
token an earlier release accepted is a minor bump with a rollback note.

## 2. Tenant and namespace scoping

**Fires on** any change to how a caller's namespace is derived or how it bounds
what they reach: credential pool resolution in
`crates/gateway/src/credentials.rs`, `allow_platform_fallback`, the shared keys
in `crates/gateway/src/budget/` and `crates/gateway/src/rate_limit/`, the
namespace filter on catalogue and credential-status responses, the all-namespace
operator view, and boot validation that rejects a credential naming an undefined
namespace or provider.

**Regression tests.** Isolation at credential resolution:
`byok_namespace_uses_its_own_pool_and_never_borrows_by_default` and
`platform_fallback_yields_the_whole_platform_pool_attributed_to_platform`.
Response scoping: `models_are_scoped_to_the_callers_namespace`,
`models_intersect_namespace_access_with_alias_scope`,
`credentials_status_isolated_between_tenant_namespaces`,
`credentials_status_scope_less_minted_token_keeps_own_namespace_view_only`, and
`credentials_status_operator_view_follows_authority_not_claims`. Shared-state
scoping: `one_namespace_cannot_consume_another_namespaces_floor`,
`namespaces_do_not_share_a_cap`,
`every_v2_key_in_a_namespace_shares_the_namespace_hash_tag`, and
`a_configured_namespace_that_is_only_a_prefix_does_not_claim_the_key` — key
derivation is an isolation boundary, so a prefix or separator change is a
security change. A new cross-namespace read needs a test proving it is
one-directional and off by default.

**Threat model and ADRs.** [ADR 0003](../adr/0003-namespaced-credentials-and-byok.md),
[ADR 0006](../adr/0006-credential-pools-per-namespace-provider.md), and
[ADR 0010](../adr/0010-shared-budget-backends-and-charging-policy.md) define the
boundary and its charging policy; section 6 of the security review is the BYOK
isolation argument, and section 8 records what the namespace boundary
deliberately does *not* defend (OS-level isolation, availability between
tenants). A second exception to one-directional fallback needs an ADR, not a
configuration key.

**Release impact.** A key-derivation change is a data migration for the shared
backends: say whether existing Redis keys or Postgres rows are still read, and
whether the fleet must be stopped, the way the namespace budget-cap migration is
recorded in [stateful backends](../deployment/stateful-backends.md) and the
[production checklist](../deployment/production-checklist.md). Any widening of a
response's scope is a disclosure change and belongs in the changelog even when it
is intended.

## 3. SecretStore, credential delivery, rotation, and redaction

**Fires on** any change to how secret material enters, is held, moves, or is
described: `crates/gateway/src/backends/secrets.rs` and its `SecretStore`
contract, `crates/gateway/src/key_material.rs`, the credential resolution in
`crates/gateway/src/credentials.rs`, header injection and failure description in
`crates/gateway-transport/src/lib.rs`, a new `expose_secret` call site, a new
`Debug`/`Display`/`Serialize` derive on a type that can reach one, rotation and
versioning, and the text of any error, log, span, or metric that could carry a
value rather than a reference.

**Regression tests.** Unformattability and reference opacity:
`material_is_not_debuggable`, `references_are_opaque_and_versioned`, and
`error_messages_never_carry_material`. Rotation and failure taxonomy:
`rotation_keeps_earlier_versions_resolvable`,
`an_unwrappable_secret_is_corrupt_not_unavailable`,
`a_missing_reference_is_distinguishable_from_an_outage`, and
`empty_material_is_rejected_rather_than_stored`. Delivery: the exact-bytes rules
in `resolves_env_without_trimming`,
`rejects_missing_empty_and_invalid_utf8_files`, and
`resolves_file_bytes_without_trimming`. Outbound description:
`a_described_failure_keeps_the_endpoint_and_drops_its_secrets`, the regression
for the one finding of the security review. Attribution without disclosure:
`fallback_status_hides_default_platform_label_but_keeps_explicit_id`.

A new `expose_secret` call site is a review item in its own right: the security
review counts them, so a PR that adds one says why the count changed. Boot- or
compile-time failures must name the *reference* — the env-var or file name — and
a test should assert that, not just that an error occurred.

**Threat model and ADRs.** Sections 1, 2, 4, and 5 of the security review are
the write-only argument for outbound and inbound material and must be amended,
not silently outgrown; the secret-delivery section of the
[deployment security model](./deployment-model.md) is the operator-facing
contract; `SecretStore`'s placement at `SnapshotCompilation` and off the request
path is in [backend contracts](../maintainers/backend-contracts.md) and
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md). Making secret
resolution reachable from the request path changes the availability argument and
needs an ADR.

**Release impact.** A rotation or wrapping change is an operator procedure
change: update the [minted-token guide](../minted-token-guide.md) for signer
material and the [configuration reference](../configuration.md) for references,
and say whether an overlapping-key rotation still works across the upgrade. A
new secret reference shape is a configuration surface addition — it belongs in
`axond.example.toml` or `axond.stateful.example.toml`, which `ops/check-docs.py`
enforces for stateful bootstrap keys.

## 4. Catalogue and model entitlement

**Fires on** any change to what a caller may discover or invoke: alias scope
patterns in `crates/gateway/src/aliases.rs`, alias-to-target mapping and wire
families, the `/v1/models` projection, catalogue ingestion in
`crates/gateway/src/backends/catalog.rs`, pricing metadata, and any new route
that exposes model or provider metadata.

**Regression tests.** Pattern semantics are the entitlement boundary:
`patterns_match_case_sensitively_and_union`, `prefix_does_not_subsume_other_globs`,
`an_empty_scope_permits_nothing`, and `invalid_patterns_are_rejected` — a glob
change that broadens a match is a privilege change. Projection:
`models_requires_a_gateway_key`, `models_lists_the_callers_aliases`, and the
namespace intersection tests in trigger 2. Ingestion must stay inert:
`observed_pricing_is_metadata_not_activation`,
`the_source_is_background_only_and_declares_incremental_refresh`, and
`an_unreachable_source_is_retryable_and_never_a_boot_failure` — upstream
catalogue data must never become an entitlement or an admission dependency. A
new route is also covered mechanically: `ops/check-docs.py` fails a registered
route that the [compatibility contract](../compatibility.md) does not document.

**Threat model and ADRs.** [ADR 0020](../adr/0020-alias-wire-family-validation.md)
and [ADR 0012](../adr/0012-native-provider-routes.md) bound wire families and
native routes; `CatalogSource`'s background-only placement is in
[backend contracts](../maintainers/backend-contracts.md). Item 2 of the security
review's accepted-risk section is why `/v1/models` is authenticated and scoped —
re-read it before changing that projection.

**Release impact.** Entitlement changes are visible to clients: a pattern
semantics change can silently grant or revoke access at upgrade, so it needs a
migration note saying which existing configurations change meaning. Route and
wire-family additions are compatibility-contract entries; pricing changes affect
budgets, which the [production checklist](../deployment/production-checklist.md)
already asks operators to review.

## 5. Persistence, migrations, telemetry, and usage

**Fires on** any change to durable shape or emitted data: files under
`ops/postgres/` or `crates/gateway/sql/`, the sinks and row shapes in
`crates/gateway/src/usage/`, the control-plane journal and revision schema, span
and metric attributes in `crates/gateway/src/telemetry/`, log call sites, the
retention or delivery guarantees of usage records, and any new `on_unavailable`
policy on a store.

**Regression tests.** The two copies of the shipped DDL are gated by
`every_shipped_ddl_file_exists_in_both_locations` and
`the_two_copies_of_each_shipped_ddl_file_are_byte_identical` — an operator
applying `ops/postgres/*.sql` by hand and a gateway applying its embedded copy
must produce the same table, and a row-shape change is a new `*_v<N>.sql` rather
than an edit. Row and statement safety: `the_row_shape_matches_the_shipped_ddl`,
`every_column_is_bound_once_per_row`, `a_batch_never_exceeds_the_parameter_limit`,
`table_names_that_could_carry_sql_are_rejected`,
`a_schema_qualified_table_keeps_its_index_names_unqualified`, and
`a_later_chunk_failure_rolls_back_the_whole_batch`. The Postgres- and
Redis-backed tests (`a_batch_lands_in_postgres` and the shared-store tests) skip
without services, so run them the way
[CONTRIBUTING](../../CONTRIBUTING.md#development) documents — CI's stateful lane
does. A new emitted field needs a test that it carries a non-secret identifier:
usage rows carry `credential_id` and `credential_source`, never material.

**Threat model and ADRs.** [ADR 0007](../adr/0007-telemetry-model.md),
[ADR 0009](../adr/0009-durable-usage-sinks.md),
[ADR 0017](../adr/0017-state-tiers-and-optional-backends.md), and
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) hold the
telemetry, durability, and tier positions; section 3 of the security review is
the argument that logs, spans, metrics, and usage rows carry references only.
Raising a deployment's state tier, or making an existing feature depend on a
store it did not need, is an ADR with the template's state-tier declaration.
Free-form caller input must not become a metric attribute — that is a
cardinality *and* a disclosure decision.

**Release impact.** Schema changes are ordered operator work: name the DDL that
must be applied before writers, whether mixed versions may run, and the rollback
limit, in the [upgrade guide](../operations/upgrades.md) and the
[control-plane journal](../operations/control-plane-journal.md) or
[usage schema](../usage-schema.md) as appropriate. A field removed or renamed in
a usage row breaks somebody's billing pipeline and is a documented contract
change, not an implementation detail.

## 6. Actions, release permissions, attestations, and signing

**Fires on** any change under `.github/workflows/`, to `ops/publish-crates.sh`,
`ops/docker-smoke.sh`, `ops/binary-smoke.py`, `ops/tier0-gate.sh`,
`ops/publish-image-index.sh`, `ops/verify-image-evidence.sh`,
`ops/msrv-gate.sh`, `ops/api-compat.py`, `install.sh`,
`install.ps1`, the `Dockerfile`, or `deny.toml`; a new workflow, job permission,
secret, or environment; a new or bumped third-party action; and any change to
what is attested, signed, or verified.

**Review checks.** Workflow-level `permissions: contents: read` with elevation
per job only, no long-lived registry or signing secret (GHCR login uses
`github.token`; signing is keyless through the job's OIDC identity), the
narrowly anchored `SIGNER_IDENTITY`, `cosign verify` plus
`gh attestation verify` after signing, and signing only after
`ops/docker-smoke.sh` has exercised the published image, and attesting a binary
only after `ops/binary-smoke.py` has booted the exact archived file. The
multi-architecture index inherits the same order: `ops/publish-image-index.sh`
assembles it from child digests that were each smoked and signed already and
asserts it carries exactly the supported platforms, then it is signed, and
`ops/verify-image-evidence.sh` re-verifies the signature and provenance of both
the children and the index afterwards. Section 7 of the
[security review](../security-review-2026-08-05.md) states that posture; a PR
that changes any part of it says which part and why. A new job that needs
`id-token: write`, `packages: write`, or attestation scopes justifies the scope
at the job, and a `pull_request_target` or workflow-run trigger on untrusted
input is an ADR-level decision, not a workflow tweak.

**Regression tests.** The release path is exercised on every change rather than
at the tag: `publish-dry-run` packages each crate from its own tarball in
dependency order, `docker-smoke` and `quickstart-smoke` boot what is shipped,
`static-binary` proves the musl build and runs `ops/tier0-gate.sh`, the
`binary-smoke` lanes boot and serve every released target on a runner of its own
platform and the release lanes repeat that against the archived binary, the `docs`
lane drives both installers in dry-run with `AXOND_REQUIRE_ATTESTATION` — including
a deliberately wrong `AXOND_REPOSITORY` and an invalid setting that must fail —
and `ops/check-installer-download.sh` holds the installer's failure diagnostics
apart, so a transport failure is never reported as a missing release asset —
`dependency-policy` runs `cargo deny` with no ignore entries, and `api-compat`
and `msrv` hold the published surface and the floor. Keep the installer
verification paths covered: an installer that can be made to skip attestation
verification is a supply-chain regression.

**Threat model and ADRs.** [ADR 0004](../adr/0004-ci-and-release-pipeline.md),
[ADR 0025](../adr/0025-crates-io-publication.md), and
[ADR 0026](../adr/0026-prebuilt-binary-installers.md) define the pipeline and its
artifacts; the supply-chain section of the
[deployment security model](./deployment-model.md) is what an operator verifies.
Removing or weakening an attestation, a signature, or a verification step
supersedes those decisions explicitly.

**Release impact.** Required GitHub configuration, artifact sets, and signer
identity are documented in the [release runbook](../maintainers/releasing.md) and
[installation and verification](../installation.md); a change to any of them
updates both, because operators pin and verify against them. Dropping or
renaming an artifact, or changing the signer identity, breaks existing
verification commands and is a documented, changelog-visible break — and an MSRV
or public-API change is a minor release per the
[compatibility contract](../compatibility.md).

## Recording the review

Put the outcome in the pull request body, not only in review comments — it is
what a future reader of the commit sees:

- which triggers fired, or that none did;
- the tests that hold the property, named;
- whether the security review, the deployment security model, or an ADR changed,
  and if not, why the existing reasoning still holds;
- the release impact in one line: none, documentation, configuration, schema, or
  a break.

If a trigger fires and the answer to any of the three obligations is "later",
say so in the PR and open the issue before merging. A security fix arriving
through [`SECURITY.md`](../../SECURITY.md) uses this same page: the trigger it
fires tells you which regression test the fix owes.
