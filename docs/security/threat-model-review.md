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
| `ts/packages/gateway/src/auth.ts`, `ts/packages/gateway/src/app.ts`, `[[gateway_key]]`, the `/ns/{ns}/v1` and `/api/v1` authentication path | [Authentication and authorization](#1-authentication-and-authorization) |
| Namespace resolution, `ts/packages/gateway/src/namespace.ts`, `ts/packages/gateway/src/dispatch.ts` pool lookup, `allow_platform_fallback`, `ts/packages/gateway/src/app.ts`, `ts/packages/gateway/src/admission.ts`, the store's namespace rows | [Tenant and namespace scoping](#2-tenant-and-namespace-scoping) |
| `ts/packages/gateway/src/config.ts`, credential injection, error and log text | [Credential delivery and redaction](#3-credential-delivery-and-redaction) |
| `ts/packages/cli/src/discovery.ts`, `ts/packages/gateway/src/glob.ts`, `ts/packages/gateway/src/pricing.ts`, `[[price]]`, `/v1/models`, alias scope, wire families | [Catalogue and model entitlement](#4-catalogue-and-model-entitlement) |
| `ops/postgres/`, `ts/packages/cli/src/postgres-store.ts`, `ts/packages/cli/src/sqlite-store.ts`, `ts/packages/cli/src/usage-delivery.ts`, `ts/packages/gateway/src/otel.ts` | [Persistence, migrations, telemetry, and usage](#5-persistence-migrations-telemetry-and-usage) |
| `.github/workflows/`, `ts/scripts/sign-artifact.sh`, `install.sh`, `install.ps1`, `ts/Dockerfile`, `ts/scripts/check-npm-policy.ts` | [Actions, release permissions, attestations, and signing](#6-actions-release-permissions-attestations-and-signing) |
| `ts/packages` extension loading, `AXOND_EXTENSIONS_DIR`, Worker bundling of middleware | [TypeScript extension trust](#8-typescript-extension-trust) |

A change can fire more than one trigger; a credential-delivery change that also
adds a Postgres table fires two, and owes both sets.

## 1. Authentication and authorization

**Fires on** any change to how a request is authenticated or what it is then
allowed to do: the `authenticate` path in `ts/packages/gateway/src/auth.ts`,
principal resolution in `ts/packages/gateway/src/auth.ts`, the static
`[[gateway_key]]`, a new authenticated route, and anything that widens what the
key can reach.

**Regression tests.** `unknown_gateway_key_is_rejected_before_namespace_lookup`
holds the invalid-key boundary before namespace lookup;
`noncanonical_namespace_path_is_invalid_after_authentication` holds the
canonical path refusal. `test("withdrawn routes stay unmounted")` covers the removed
admin and unprefixed inference surfaces.
`argv_matches_clap_before_config_load` refuses withdrawn CLI commands.
The former exhaustive missing-key, all-canonical-routes and OpenAPI-authentication
tests have no equivalent TS regression; this is a coverage gap, not a weaker
authentication contract.

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
`ts/packages/gateway/src/dispatch.ts`, `allow_platform_fallback`, per-namespace
budgets in `ts/packages/gateway/src/app.ts`, per-tenant admission in
`ts/packages/gateway/src/admission.ts`, the namespace filter on catalogue and
credential-status responses, and boot validation that rejects a credential
naming an undefined namespace or provider.

**Regression tests.** `test("fallback credential status hides an env-derived platform label")`
covers own BYOK selection, fallback attribution, API-created namespace fallback,
and closed namespaces in the credential-status view.
`usage_summary_matches_the_rust_grouping` holds namespace/period accounting;
`test("management routes match the compatibility contract")` refuses a missing usage
period and checks idempotent namespace deletion.
`test("delete then recreate bumps incarnation so a late settle does not charge")`
holds the SQLite recreation boundary.
Per-tenant admission is no longer in core. The optional rate-limit extension
has `test("store mode on postgres saturates one namespace and leaves another its capacity")`;
this does not establish the former global-concurrency admission properties.
Alias-scope intersection was withdrawn; routing uses provider/model IDs.
A new cross-namespace read needs a test proving it is one-directional and off
by default.

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
described: `ts/packages/gateway/src/config.ts`, credential resolution in
`ts/packages/gateway/src/dispatch.ts`, header injection and failure description in
`ts/packages/gateway/src/dispatch.ts`, a new secret access call site, a new
serialization or inspection path on a type that can reach one, and the text
of any error, log, span, or metric that could carry a value rather than a
reference.

**Regression tests.** `test("credential and gateway key graph matches the rust refusals")`
covers missing/empty/invalid-UTF-8 secret files and preserves a file's newline.
`file_gateway_key_keeps_exact_bytes` checks the real file reader.
`upstream_transport_log_names_the_reason_and_omits_the_address` holds bounded,
non-address diagnostics rather than retaining the endpoint.
`provider_error_replaces_the_echoed_credential` covers caller bodies for both
Bearer and Anthropic keys.
`messages_wire_headers_default_the_version_and_keep_the_caller_pin` holds
provider header injection. `test("fallback credential status hides an env-derived platform label")`
holds attribution without disclosure. There is no dedicated TS regression for
environment-secret whitespace preservation.

A new secret access call site is a review item in its own right: the security
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
patterns in `ts/packages/gateway/src/glob.ts`, wire families, the `/ns/{ns}/v1/models`
projection, catalogue ingestion in `ts/packages/cli/src/discovery.ts`,
request-path pricing in `ts/packages/gateway/src/pricing.ts` and `[[price]]`, and any
new route that exposes model or provider metadata.

**Regression tests.** `test("a buffered chat completion rewrites the model and forwards the provider credential")`
exercises namespaced inference; `test("management routes match the compatibility contract")`
exercises namespace management. `provider_model_cache_follows_the_rust_source` holds
source-bound model projection, and
`test("an absent namespace is unknown and an unprefixed model is refused")` holds
namespace existence and provider/model routing.
`test("catalogue boot matches the rust refusals")` rejects plaintext catalogue URLs
and URL credentials. `catalogue_and_listing_redirects_are_not_followed_and_omit_the_secret`
refuses redirects; `test("a failed discovery keeps the last catalogue and a fresh row rejects a different source")`
keeps prior metadata on failure.
`test("responses sequence names the broken event")` rejects Responses SSE event/type
disagreement. The equivalent dedicated native Messages disagreement regression
is absent. Alias entitlement globs were withdrawn; namespace blocklist validation
is covered by `namespace_attrs_and_blocklist_match_the_rust_limits`.
The old alias algebra, observed-pricing inertness, background/incremental-source,
and all-route auth assertions are not established by these tests.
A new registered route must also appear in the
[compatibility contract](../compatibility.md), enforced by `ops/check-docs.py`.

Approved price books, pinned catalogue offerings, durable enablements, and the
availability projection were withdrawn with
[ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md).

**Threat model and ADRs.** [ADR 0020](../adr/0020-alias-wire-family-validation.md)
and [ADR 0012](../adr/0012-native-provider-routes.md) bound wire families and
native routes; [ADR 0043](../adr/0043-catalogue-source-imports.md) holds
observed rates as metadata and [ADR 0056](../adr/0056-request-path-pricing.md)
covers request-path pricing. Background discovery ownership is in
[backend contracts](../maintainers/backend-contracts.md). Item 2 of the security
review's accepted-risk section is why `/ns/{ns}/v1/models` is authenticated and scoped;
re-read it before changing that projection.

**Release impact.** Entitlement changes are visible to clients: a pattern
semantics change can silently grant or revoke access at upgrade, so it needs a
migration note saying which existing configurations change meaning. Route and
wire-family additions are compatibility-contract entries; pricing changes affect
budgets, which the [production checklist](../deployment/production-checklist.md)
already asks operators to review.

## 5. Persistence, migrations, telemetry, and usage

**Fires on** any change to durable shape or emitted data: files under
`ops/postgres/` or `ts/packages/cli/src/postgres-store.ts`, the store in `ts/packages/cli/src/sqlite-store.ts`,
the sinks and row shapes in `ts/packages/cli/src/usage-delivery.ts`, span and metric
attributes in `ts/packages/gateway/src/otel.ts`, log call sites, and the retention
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
`malformed_responses_controls_never_reach_the_provider` refuses a non-boolean
`stream` and a non-string `previous_response_id` before the provider is called,
and the error omits the value.

**Regression tests.** `test("embedded usage ddl matches ops/postgres")` checks embedded
DDL against every shipped usage migration, schema-qualified index naming and the
row width. `test("sqlite usage summary groups in byte order and saturates at the signed cap")`
holds grouped usage; `usage_summary_matches_the_rust_grouping` checks the HTTP
projection. `usage_sink_replaces_stdout_and_postgres_inserts_one_row` writes
and reads a Postgres usage row when `AXOND_TEST_POSTGRES` is set.
`test("a postgres sink rejects a missing dsn, a bad table, and a batch that does not fit")`
covers configuration refusals, not exhaustive SQL injection or bind accounting.
There are no equivalent TS tests for every-column binding, the parameter ceiling,
exact full row shape or whole-batch rollback across chunks; SQL-carrying table-name rejection is
not exhaustively tested either. Do not infer those
guarantees from the DDL test.
A new emitted field needs a test proving it carries a non-secret identifier:
usage rows carry credential identifiers, never material.

Telemetry disclosure is held by `test("metrics drop secret and content sentinels")`,
`test("otlp json joins traceparent and omits the prompt")`, and
`request_log_carries_server_span_fields_and_omits_content`: emitted metrics,
traces and request logs must omit content and secret material. Accounting
idempotency is held by `test("ten settlements of one request_id charge once")`
and `test("sqlite settlement is exactly once per request_id and survives reopen")`;
duplicate execution must not double-charge, including after restart.

**Threat model and ADRs.** [ADR 0007](../adr/0007-telemetry-model.md),
[ADR 0009](../adr/0009-durable-usage-sinks.md), and
[ADR 0049](../adr/0049-billing-grade-usage-outbox.md) hold the telemetry and
durability positions; section 3 of the security review is the argument that
logs, spans, metrics, and usage rows carry references only. Free-form caller
input must not become a metric attribute; that is a cardinality *and* a
disclosure decision.

A change to the *delivery guarantee* fires this trigger too. The historical
billing-grade outbox in ADR 0049 is not implemented by the TS core. The Store
usage insert is the charge; external usage sinks remain buffered best-effort
delivery. Do not claim historical outbox qualification for the current process.

**Release impact.** Schema changes are ordered operator work: name the DDL that
must be applied before writers, whether mixed versions may run, and the rollback
limit, in the [upgrade guide](../operations/upgrades.md) and the
[usage schema](../usage-schema.md) or the [usage
outbox](../operations/usage-outbox.md) as appropriate. A field removed or renamed
in a usage row breaks somebody's billing pipeline and is a documented contract
change, not an implementation detail.

## 6. Actions, release permissions, attestations, and signing

**Fires on** any change under `.github/workflows/`, to `ts/scripts/sign-artifact.sh`,
`ops/docker-smoke.sh`, `ops/binary-smoke.py`,
`ops/publish-image-index.sh`, `ops/verify-image-evidence.sh`,
`ts/scripts/check-npm-policy.ts`, `install.sh`,
`install.ps1`, `ts/Dockerfile`; a new workflow, job permission,
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

**Regression tests.** The TypeScript gateway job runs workspace tests, SDK
compatibility and `check:npm` dependency policy. The compiled-binary smoke boots
the Bun executable. Release dry-runs build and smoke binaries and images without
publishing; `ops/binary-smoke.py` and `ops/docker-smoke.sh` exercise archived
binaries and images in the release path before signing and attestation.
The non-PR `quickstart-smoke` builds the TS image, and `openapi-smoke` generates
the spec from TS `OPENAPI` and typechecks its client. The docs lane drives both
installers in dry-run with `AXOND_REQUIRE_ATTESTATION`, including wrong-repository
and invalid-setting refusals. `ops/check-installer-download.sh` distinguishes
transport failures from missing release assets.
Rust API-compatibility, MSRV and fuzz lanes were retired with the Rust tree under
[ADR 0069](../adr/0069-retire-the-rust-gateway.md). TypeScript fuzz equivalents
remain an open question; parser regression and property tests are not a claim
of coverage-guided fuzzing.
Keep installer verification covered: a path that bypasses attestation is a
supply-chain regression.

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
new trigger here; `test("withdrawn routes stay unmounted")` holds that the old surface stays gone.

## 8. TypeScript extension trust

**Fires on** a change to how `ts/` loads or trusts an extension: the stage
order, `apiVersion`, migration prefix checks, the untrusted store scope, or
`AXOND_EXTENSIONS_DIR`.

**Regression tests.** The authentication floor in section 1 applies to the
release process: `unknown_gateway_key_is_rejected_before_namespace_lookup` rejects an
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
Untrusted ones see only the request namespace. The static-key boundary is the TypeScript gateway's compiled outer boundary.

**Release impact.** Extensions run inside the sole release process. Changes to
extension loading or trust affect the supported TypeScript artifact.

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
