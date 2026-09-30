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
| `routes/auth.rs`, `principals.rs`, `[[gateway_key]]`, the `/ns/{ns}/v1` and `/api/v1` authentication path | [Authentication and authorization](#1-authentication-and-authorization) |
| Namespace resolution, `namespace.rs`, `credentials.rs` pool lookup, `allow_platform_fallback`, `budget/`, `admission.rs`, the store's namespace rows | [Tenant and namespace scoping](#2-tenant-and-namespace-scoping) |
| `key_material.rs`, credential injection, error and log text | [Credential delivery and redaction](#3-credential-delivery-and-redaction) |
| `backends/catalog.rs`, `aliases.rs`, `pricing.rs`, `[[price]]`, `/v1/models`, alias scope, wire families | [Catalogue and model entitlement](#4-catalogue-and-model-entitlement) |
| `ops/postgres/`, `crates/gateway/sql/`, `store/`, `usage/`, `telemetry/` | [Persistence, migrations, telemetry, and usage](#5-persistence-migrations-telemetry-and-usage) |
| `.github/workflows/`, `ops/publish-crates.sh`, `install.sh`, `install.ps1`, `Dockerfile`, `deny.toml` | [Actions, release permissions, attestations, and signing](#6-actions-release-permissions-attestations-and-signing) |
| `ts/packages` extension loading, `AXOND_EXTENSIONS_DIR`, Worker bundling of middleware | [TypeScript extension trust](#8-typescript-extension-trust) |

A change can fire more than one trigger; a credential-delivery change that also
adds a Postgres table fires two, and owes both sets.

## 1. Authentication and authorization

**Fires on** any change to how a request is authenticated or what it is then
allowed to do: the `authenticate` path in `crates/gateway/src/routes/auth.rs`,
principal resolution in `crates/gateway/src/principals.rs`, the static
`[[gateway_key]]`, a new authenticated route, and anything that widens what the
key can reach.

**Regression tests.** The fail-closed floor is
`every_authenticated_route_rejects_a_request_without_a_gateway_key`; a new route
belongs in it, not beside it. Namespaced inference authenticates before the
namespace is read: `every_canonical_namespace_route_authenticates_first` and
`noncanonical_namespace_encoding_is_a_typed_refusal_after_authentication`.
`/api/v1` is authenticated the same way: `openapi_json_requires_the_gateway_key`
holds the spec document. Withdrawn surfaces
stay withdrawn: `admin_v1_is_unmounted` holds that `/admin/v1` is 404 even with
an operator bearer, and `the_withdrawn_commands_are_not_parsed` holds that the
removed CLI subcommands are unknown rather than accepted and inert.

**Threat model and ADRs.** [ADR 0013](../adr/0013-inbound-auth-fails-closed.md)
(no keyless mode) and
[ADR 0061](../adr/0061-authentication-remains-an-outer-boundary.md)
(authentication is the compiled outer boundary, before parsing, accounting, and
dispatch) are the accepted positions.
[ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md) withdrew minted
tokens, verifiers, epochs, and revocation: one deployment-wide static key
authenticates management and inference, and the namespace is selected from the
canonical `/ns/{ns}/v1` path after authentication. Sections 2 and 8 of the
security review and the inbound authentication section of the
[deployment security model](./deployment-model.md) are the statements to
re-confirm or amend.

**Release impact.** A new typed `401`/`403` error code is part of the
[compatibility contract](../compatibility.md). Anything that could reject a key
an earlier release accepted is a minor bump with a rollback note.

## 2. Tenant and namespace scoping

**Fires on** any change to how a caller's namespace is derived or how it bounds
what they reach: namespace rows in the store, credential pool resolution in
`crates/gateway/src/credentials.rs`, `allow_platform_fallback`, per-namespace
budgets in `crates/gateway/src/budget/`, per-tenant admission in
`crates/gateway/src/admission.rs`, the namespace filter on catalogue and
credential-status responses, and boot validation that rejects a credential
naming an undefined namespace or provider.

**Regression tests.** Isolation at credential resolution:
`byok_namespace_uses_its_own_pool_and_never_borrows_by_default`,
`platform_fallback_yields_the_whole_platform_pool_attributed_to_platform`, and
`api_created_namespace_inherits_platform_credentials`. Response scoping:
`models_intersect_namespace_access_with_alias_scope`. Shared-state scoping:
`a_saturated_tenant_leaves_other_tenants_their_capacity` and
`a_refused_tenant_does_not_consume_the_global_ceiling`. Namespace lifecycle:
`delete_namespace_is_idempotent_and_fail_closed_on_recreate`. Usage:
`usage_summary_matches_rows_for_namespace_and_period` holds that
`GET /api/v1/namespaces/{ns}/usage` totals only that namespace's rows for the
requested period; `usage_summary_requires_period_query` refuses a missing
period rather than summing across periods. A new
cross-namespace read needs a test proving it is one-directional and off by
default.

**Threat model and ADRs.** [ADR 0003](../adr/0003-namespaced-credentials-and-byok.md)
and [ADR 0006](../adr/0006-credential-pools-per-namespace-provider.md) define the
boundary; section 6 of the security review is the BYOK isolation argument, and
section 8 records what the namespace boundary deliberately does *not* defend
(OS-level isolation, availability between tenants).
[ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md) makes the
API-created namespace the tenant unit: path `/ns/{ns}` is authoritative, store
lookup fails closed (`404 unknown_namespace` vs `503 store_unavailable`), and
API-created namespaces inherit the platform credential pool until BYOK lands.
A second exception to one-directional fallback needs an ADR, not a
configuration key.

**Release impact.** A change to how rows are keyed by namespace is a data
migration: say whether existing rows are still read and whether the fleet must
be stopped, in the [upgrade guide](../operations/upgrades.md) and the
[production checklist](../deployment/production-checklist.md). Any widening of a
response's scope is a disclosure change and belongs in the changelog even when
it is intended.

## 3. Credential delivery and redaction

**Fires on** any change to how secret material enters, is held, moves, or is
described: `crates/gateway/src/key_material.rs`, credential resolution in
`crates/gateway/src/credentials.rs`, header injection and failure description in
`crates/gateway-transport/src/lib.rs`, a new `expose_secret` call site, a new
`Debug`/`Display`/`Serialize` derive on a type that can reach one, and the text
of any error, log, span, or metric that could carry a value rather than a
reference.

**Regression tests.** Delivery: the exact-bytes rules in
`resolves_env_without_trimming`, `rejects_missing_empty_and_invalid_utf8_files`,
and `resolves_file_bytes_without_trimming`. Outbound description:
`a_described_failure_keeps_the_endpoint_and_drops_its_secrets`, the regression
for the one finding of the security review. A provider error that echoes the
outbound credential is returned with that value replaced by `[REDACTED]`.
`provider_error_replaces_the_echoed_credential` covers the caller body for both
the Bearer key and the Anthropic `x-api-key`. Attribution without disclosure:
`fallback_status_hides_default_platform_label_but_keeps_explicit_id`.

A new `expose_secret` call site is a review item in its own right: the security
review counts them, so a PR that adds one says why the count changed. Boot
failures must name the *reference* (the env-var or file name), and a test should
assert that, not just that an error occurred.

The durable `SecretStore`, envelope-encrypted blob secrets, the credential
lifecycle, and request-content redaction (the `axond.redact` guardrail) were
withdrawn with [ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md).
Reintroducing any of them fires this trigger and needs a new ADR.

**Threat model and ADRs.** Sections 1, 2, 4, and 5 of the security review are
the write-only argument for outbound and inbound material and must be amended,
not silently outgrown; the secret-delivery section of the
[deployment security model](./deployment-model.md) is the operator-facing
contract. Making secret resolution reachable from the request path changes the
availability argument and needs an ADR.

**Release impact.** A new secret reference shape is a configuration surface
addition: it belongs in the [configuration reference](../configuration.md) and
`axond.example.toml`.

## 4. Catalogue and model entitlement

**Fires on** any change to what a caller may discover or invoke: alias scope
patterns in `crates/gateway/src/aliases.rs`, wire families, the `/v1/models`
projection, catalogue ingestion in `crates/gateway/src/backends/catalog.rs`,
request-path pricing in `crates/gateway/src/pricing.rs` and `[[price]]`, and any
new route that exposes model or provider metadata.

**Regression tests.** Pattern semantics are the entitlement boundary:
`patterns_match_case_sensitively_and_union`, `prefix_does_not_subsume_other_globs`,
`an_empty_scope_permits_nothing`, and `invalid_patterns_are_rejected`; a glob
change that broadens a match is a privilege change. Projection:
`models_requires_a_gateway_key`, `models_lists_the_callers_aliases`, and the
namespace intersection tests in trigger 2. Inference is `/ns/{ns}/v1`, the
namespace must exist in the store, and the models list is that path's
catalogue: `namespaced_completion_and_namespace_api`. Configured `[[model]]`
aliases were withdrawn in ADR 0063; routing is `provider-id/model-id`.

Ingestion must stay inert:
`observed_pricing_is_metadata_not_activation`,
`the_source_is_background_only_and_declares_incremental_refresh`, and
`an_unreachable_source_is_retryable_and_never_a_boot_failure`; upstream
catalogue data must never become an entitlement or an admission dependency. The
source a deployment reads must be the source it configured:
`a_catalogue_source_url_must_be_https`,
`a_catalogue_source_url_must_have_a_host_without_credentials`, and
`a_redirected_source_is_refused_rather_than_followed`. Provider streams are
untrusted input: `responses_stream_rejects_event_and_payload_type_disagreement`
and `native_stream_rejects_event_and_payload_type_disagreement`. A new route is
also covered mechanically: `ops/check-docs.py` fails a registered route that the
[compatibility contract](../compatibility.md) does not document.

Approved price books, pinned catalogue offerings, durable enablements, and the
availability projection were withdrawn with
[ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md).

**Threat model and ADRs.** [ADR 0020](../adr/0020-alias-wire-family-validation.md)
and [ADR 0012](../adr/0012-native-provider-routes.md) bound wire families and
native routes; [ADR 0043](../adr/0043-catalogue-source-imports.md) holds
observed rates as metadata and [ADR 0056](../adr/0056-request-path-pricing.md)
covers request-path pricing. `CatalogSource`'s background-only placement is in
[backend contracts](../maintainers/backend-contracts.md). Item 2 of the security
review's accepted-risk section is why `/v1/models` is authenticated and scoped;
re-read it before changing that projection.

**Release impact.** Entitlement changes are visible to clients: a pattern
semantics change can silently grant or revoke access at upgrade, so it needs a
migration note saying which existing configurations change meaning. Route and
wire-family additions are compatibility-contract entries; pricing changes affect
budgets, which the [production checklist](../deployment/production-checklist.md)
already asks operators to review.

## 5. Persistence, migrations, telemetry, and usage

**Fires on** any change to durable shape or emitted data: files under
`ops/postgres/` or `crates/gateway/sql/`, the store in `crates/gateway/src/store/`,
the sinks and row shapes in `crates/gateway/src/usage/`, span and metric
attributes in `crates/gateway/src/telemetry/`, log call sites, and the retention
or delivery guarantees of usage records.

Provider failure diagnostics on attempt spans are bounded to 4 KiB and omit
the known outbound API key before truncation. The TypeScript attempt span does
the same, and the caller body keeps that prefix plus `… [truncated]` when the
provider text is longer. `provider_diagnostics_keep_context_limits_and_a_bounded_message`
covers the context-limit vocabulary, that marker, and the span bound. The gateway does not attach
request or successful-response bodies, but provider error messages can echo
caller input; trace access is therefore diagnostic-data access. The regression
`provider_refusals_keep_their_class_and_export_bounded_attempt_diagnostics`
checks actual upstream status, error classification, OpenTelemetry error
status, the UTF-8 byte bound, and credential omission across buffered and
streamed OpenAI and Anthropic calls. The TypeScript test of that name records
attempt `axond.status` as `error` on the same matrix.

**Regression tests.** The two copies of the shipped DDL are gated by
`every_shipped_ddl_file_exists_in_both_locations` and
`the_two_copies_of_each_shipped_ddl_file_are_byte_identical`: an operator
applying `ops/postgres/*.sql` by hand and a gateway applying its embedded copy
must produce the same table, and a row-shape change is a new `*_v<N>.sql` rather
than an edit. `sqlite_usage_summary_groups_by_model_and_status` holds the store
usage index that `GET .../usage` reads: per-model per-status counts, null cost
as zero, and duplicate `request_id` ignored. Row and statement safety:
`the_row_shape_matches_the_shipped_ddl`, `every_column_is_bound_once_per_row`,
`a_batch_never_exceeds_the_parameter_limit`,
`table_names_that_could_carry_sql_are_rejected`,
`a_schema_qualified_table_keeps_its_index_names_unqualified`, and
`a_later_chunk_failure_rolls_back_the_whole_batch`. The Postgres-backed tests
(`a_batch_lands_in_postgres` and the store tests) skip without services, so run
them the way [CONTRIBUTING](../../CONTRIBUTING.md#development) documents. A new
emitted field needs a test that it carries a non-secret identifier: usage rows
carry `credential_id` and `credential_source`, never material.

**Threat model and ADRs.** [ADR 0007](../adr/0007-telemetry-model.md),
[ADR 0009](../adr/0009-durable-usage-sinks.md), and
[ADR 0049](../adr/0049-billing-grade-usage-outbox.md) hold the telemetry and
durability positions; section 3 of the security review is the argument that
logs, spans, metrics, and usage rows carry references only. Free-form caller
input must not become a metric attribute; that is a cardinality *and* a
disclosure decision.

A change to the *delivery guarantee* is part of this trigger too, not just a
change to the row. The billing-grade usage outbox
([ADR 0049](../adr/0049-billing-grade-usage-outbox.md)) puts a durable write on
the request path for deployments that opt in, so the review question is
availability as much as disclosure: with the defaults, an outbox that is full or
unreachable refuses requests, and the escapes from that (`capacity_policy =
"drop-oldest"`, `on_undurable = "serve"`) are accounted losses rather than silent
ones. Quarantined events are deliberately not prunable: an operator's evidence
must not be deleted to free capacity.

**Release impact.** Schema changes are ordered operator work: name the DDL that
must be applied before writers, whether mixed versions may run, and the rollback
limit, in the [upgrade guide](../operations/upgrades.md) and the
[usage schema](../usage-schema.md) or the [usage
outbox](../operations/usage-outbox.md) as appropriate. A field removed or renamed
in a usage row breaks somebody's billing pipeline and is a documented contract
change, not an implementation detail.

## 6. Actions, release permissions, attestations, and signing

**Fires on** any change under `.github/workflows/`, to `ops/publish-crates.sh`,
`ops/docker-smoke.sh`, `ops/binary-smoke.py`, `ops/tier0-gate.sh`,
`ops/install-musl-tools.sh`,
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
asserts it carries exactly the supported platforms, it is staged under a
non-operator-facing tag and booted by digest on both architectures, and only then
is it retagged as `<version>`/`sha-<short>`, signed, attested, and re-verified by
`ops/verify-image-evidence.sh`. Promotion retags the smoked digest itself and
runs every rejectable check before the first tag, because a registry tag cannot
be withdrawn by a later failure. Which of the two it is doing is stated by
`INDEX_MODE`, never inferred: promotion fails if the smoked digest is empty
instead of degrading into an assemble-then-tag run, and staging refuses the
operator-facing tags outright. A change that signs or tags the index before
that boot is the regression this ordering exists to prevent:
`ops/check-release-config.py` rejects the shape of it, and
`ops/check-index-promotion.sh` drives the script against a stubbed registry to
prove no tag is applied when a promotion check fails. Section 7 of the
[security review](../security-review-2026-08-05.md) states that posture; a PR
that changes any part of it says which part and why. A new job that needs
`id-token: write`, `packages: write`, or attestation scopes justifies the scope
at the job, and a `pull_request_target` or workflow-run trigger on untrusted
input is an ADR-level decision, not a workflow tweak.

**Regression tests.** The release path is exercised on every change rather than
at the tag: `publish-dry-run` stages a self-contained `axond` tarball (internals
are not crates.io packages) and verifies that package, `docker-smoke` and
`quickstart-smoke` boot what is shipped,
`static-binary` proves the musl build and runs `ops/tier0-gate.sh`, the
`openapi-smoke` dumps the generated OpenAPI 3.1 spec and typechecks a client from
it, the
`binary-smoke` lanes boot and serve every released target on a runner of its own
platform and the release lanes repeat that against the archived binary, the `docs`
lane drives both installers in dry-run with `AXOND_REQUIRE_ATTESTATION` — including
a deliberately wrong `AXOND_REPOSITORY` and an invalid setting that must fail —
and `ops/check-installer-download.sh` holds the installer's failure diagnostics
apart, so a transport failure is never reported as a missing release asset —
`dependency-policy` runs `cargo deny` with no ignore entries, `api-compat`
and `msrv` hold the published surface and the floor, and `fuzz-smoke` replays the
committed [fuzz corpora](./fuzzing.md) through the parsers reached before
authentication. Keep the installer
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

## 7. Control-plane tenancy, principals, and administrative authorization

Withdrawn with [ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md). The
stateful control plane, its principal directory, `/admin/v1`, breakglass, and
OIDC administration no longer exist, so this trigger has no mechanism to review.
Reintroducing an administrative surface or principal model needs a new ADR and a
new trigger here; `admin_v1_is_unmounted` holds that the old surface stays gone.

## 8. TypeScript extension trust

**Fires on** a change to how `ts/` loads or trusts an extension: the stage
order, `apiVersion`, migration prefix checks, the untrusted store scope, or
`AXOND_EXTENSIONS_DIR`.

**Regression tests.** The Rust authentication floor in section 1 still holds
for the process operators run today. The TypeScript suite names the same
floor: `unknown_gateway_key_is_rejected_before_namespace_lookup` rejects an
unknown gateway key before it reads a namespace;
`noncanonical_namespace_path_is_invalid_after_authentication` refuses a
percent-encoded namespace segment without echoing it;
`extension_file_loads_from_a_directory_without_a_rebuild` loads an extension
file from a directory without rebuilding the process;
`untrusted_extension_query_without_namespace_is_refused` refuses an untrusted
query that omits the request namespace;
`nested_quantifier_redaction_pattern_is_rejected` refuses a nested-quantifier
redaction pattern; `unsupported_extension_api_version_is_refused_at_mount` and
`unsupported_extension_api_version_is_refused_when_loaded_from_disk` refuse an
extension whose apiVersion is not 1;
`extension_migration_outside_its_prefix_is_refused` refuses a migration that
creates a table outside that extension's table prefix.
`sse_config_and_body_rewrite_are_stable_under_arbitrary_splits` holds that an
SSE identity transform is insensitive to chunk boundaries, a model rewrite
changes only top-level model strings, and loading one config text twice agrees
while omitting secret values.

**Threat model and ADRs.** [ADR 0066](../adr/0066-typescript-hono-extension-contract.md)
is the decision: extensions are operator and first-party code with no
third-party isolation boundary. Trusted extensions see the process store.
Untrusted ones see only the request namespace. The Rust binary's static-key
boundary is unchanged until a later minor ships the TypeScript artifact.

**Release impact.** None for the Rust release binary. The TypeScript process
is an additional artifact.

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
