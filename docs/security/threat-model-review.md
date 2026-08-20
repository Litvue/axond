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
| `backends/secrets.rs`, `key_material.rs`, `desired_state/secrets.rs`, `desired_state/credentials.rs`, credential injection, error and log text, rotation | [SecretStore, credential delivery, rotation, and redaction](#3-secretstore-credential-delivery-rotation-and-redaction) |
| `backends/catalog.rs`, `aliases.rs`, `availability/`, `admin/catalogue.rs`, `desired_state/models.rs`, `desired_state/pricing.rs`, `/v1/models`, alias scope and ownership, wire families, pricing | [Catalogue and model entitlement](#4-catalogue-and-model-entitlement) |
| `ops/postgres/`, `crates/gateway/sql/`, `usage/`, `telemetry/`, control-plane journal | [Persistence, migrations, telemetry, and usage](#5-persistence-migrations-telemetry-and-usage) |
| `.github/workflows/`, `ops/publish-crates.sh`, `install.sh`, `install.ps1`, `Dockerfile`, `deny.toml` | [Actions, release permissions, attestations, and signing](#6-actions-release-permissions-attestations-and-signing) |
| `desired_state/access.rs`, `desired_state/tenancy.rs`, control-plane tenancy/principal projection, `/admin/v1` authorization, denial records | [Control-plane tenancy, principals, and administrative authorization](#7-control-plane-tenancy-principals-and-administrative-authorization) |

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
`mint_rejects_unknown_scope_capability`,
`mint_with_different_kid_is_rejected_by_verifier`, and
`mint_saturates_rather_than_overflowing_an_extreme_issue_time` for claim
arithmetic. Verification is also fuzzed continuously — see
[fuzzing](./fuzzing.md) — so a claim check that a malformed token can panic past
fails the required smoke lane. Revocation must stay
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
editing it. [ADR 0061](../adr/0061-authentication-remains-an-outer-boundary.md)
owns authentication's compiled outer-boundary placement and its ordering before
convergence, parsing, configurable policy, accounting, and dispatch. Moving or
making that boundary operator-orderable amends ADR 0061 and must preserve its
closed-route, fail-closed, immutable-generation, and anonymous-before-
convergence regression floor. Sections 2 and 8 of the security review, and the
inbound authentication section of the
[deployment security model](./deployment-model.md), are the statements to
re-confirm or amend.

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
contract, the reference, ownership, and lifecycle types in
`crates/gateway/src/desired_state/secrets.rs`, the credential body and its
publication rules in `crates/gateway/src/desired_state/credentials.rs` — a new
*field* on that body is a disclosure change, since bodies are canonically encoded
into a checksum an operator reads — `crates/gateway/src/key_material.rs`, the
credential resolution in
`crates/gateway/src/credentials.rs`, header injection and failure description in
`crates/gateway-transport/src/lib.rs`, a new `expose_secret` call site, a new
`Debug`/`Display`/`Serialize` derive on a type that can reach one, rotation and
versioning, request-content redaction or restoration in
`gateway-core::guardrail` and `crates/gateway/src/middleware.rs`, and the text of
any error, log, span, or metric that could carry a value rather than a reference.

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
`fallback_status_hides_default_platform_label_but_keeps_explicit_id`. Durable
references: `a_body_carries_a_reference_and_nothing_derived_from_the_material`
(the body's field set is asserted, so adding one fails the test),
`no_refusal_can_carry_material`,
`a_reference_names_an_exact_version_and_prints_no_material`. Ownership and
lifecycle: `one_secret_belongs_to_one_owner`,
`a_credential_reaches_only_the_providers_its_owner_can_reach`,
`material_never_resolves_for_another_owner`,
`the_lifecycle_matrix_is_total_and_deterministic`,
`withdrawn_material_is_never_put_back_in_service`,
`only_staged_and_active_material_unwraps`,
`one_version_of_a_secret_is_in_service_and_it_is_not_ambiguous`, and
`a_revision_is_refused_before_publication_and_again_on_hydration`.

Request-content redaction is held by
`deterministic_redaction_round_trips_buffered_and_split_openai_sse_output`,
`production_guardrail_is_stable_across_replicas_and_renames_and_restores_output`,
`compiled_guardrail_policy_masks_real_routes_stably_across_turns_and_namespaces`,
and `tokens_are_stable_within_one_namespace_and_separated_between_namespaces`:
the provider receives a deterministic namespace-separated placeholder while the
caller receives its original value. Split-output and failure behavior are held
by `split_placeholder_is_restored_across_stream_events`,
`split_placeholders_never_cross_provider_semantic_channels`,
`native_transport_failure_discards_real_redaction_carry_and_buffered_content`,
`responses_incomplete_redaction_token_fails_before_original_bytes_are_released`,
`complete_nonterminal_eof_never_impersonates_successful_policy_finalization`,
and `responses_done_sentinel_cannot_finalize_validated_passthrough_policy`.
Route binding, unsafe control validation, and discriminator agreement are held
by `restoration_is_route_bound_and_display_text_is_the_explicit_trust_boundary`,
`malformed_responses_controls_never_reach_middleware_or_provider`,
`responses_stream_rejects_event_and_payload_type_disagreement`, and
`native_stream_rejects_event_and_payload_type_disagreement`.
Pre-dispatch refusal and non-disclosure are held by
`guardrail_refusal_dispatches_nothing_records_no_usage_and_echoes_no_match`.

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
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md); the durable
credential contract — opaque exactly-versioned references, exact ownership, and
the lifecycle states — is
[ADR 0034](../adr/0034-typed-provider-credentials-and-secret-lifecycle.md), and a
change to the lifecycle relation or to what a reference discloses amends it.
Making secret resolution reachable from the request path changes the availability
argument and needs an ADR.

[ADR 0060](../adr/0060-request-path-middleware.md) owns deterministic
request-content redaction: key material is resolved only while compiling a
snapshot, originals and stream carry are request-lifetime state that zeroizes on
drop, and Native Messages or Responses buffering is explicit and fail closed.
That state is bounded independently of body size: 4,096 distinct originals,
4,096 carry channels, 1 MiB of aggregate carry identity, and 64 KiB of carry
prefixes. Exceeding a bound refuses atomically; it does not release partially
restored content or retain a partial state update.
Changing the placeholder format, namespace derivation, routing-field exclusion,
or restoration/finalization rules amends that ADR and this trigger's regression
floor. The floor includes malicious token relocation into buffered and streamed
tool arguments, matches in JSON member names/continuation ids/forwarded wire
headers, split request fragments across fields or media parts, route-shape
confusion, semantic stream-channel confusion, SSE event/data discriminator
disagreement, Responses completion without `status: "completed"`, unbalanced or
out-of-order Native Messages lifecycles, Native block/delta type confusion,
malformed routing controls, and a
Responses `[DONE]` sentinel arriving without `response.completed`. Display text is the documented
declassification trust boundary: clients must treat restored prose as untrusted
and never auto-fetch or execute provider-authored Markdown, HTML, URLs, or
instructions.

**Release impact.** A rotation or wrapping change is an operator procedure
change: update the [minted-token guide](../minted-token-guide.md) for signer
material and the [configuration reference](../configuration.md) for references,
and say whether an overlapping-key rotation still works across the upgrade. A
new secret reference shape is a configuration surface addition — it belongs in
`axond.example.toml` or `axond.stateful.example.toml`, which `ops/check-docs.py`
enforces for stateful bootstrap keys. A change to a credential body's schema or
to its lifecycle vocabulary is a *compatibility* change: say which stored
revisions the new build stops reading, and record it in the
[journal runbook](../operations/control-plane-journal.md) and
[revision convergence](../operations/revision-convergence.md#resource-body-schemas)
rather than only in the changelog.

A guardrail key reference or placeholder-format change is also an operator
procedure change. Document key provisioning and rotation, mixed-version and
last-known-good behavior, and whether placeholders generated before the upgrade
remain restorable. A format or namespace-derivation break requires a minor
release even when no durable schema changes.

## 4. Catalogue and model entitlement

**Fires on** any change to what a caller may discover or invoke: alias scope
patterns in `crates/gateway/src/aliases.rs`, alias-to-target mapping and wire
families, the `/v1/models` projection, catalogue ingestion in
`crates/gateway/src/backends/catalog.rs`, derived availability and discovery
evaluation in `crates/gateway/src/availability/`, the durable enablement and
alias bodies and their publication rules in
`crates/gateway/src/desired_state/models.rs`, pricing metadata, approved pricing
in `crates/gateway/src/desired_state/pricing.rs`, alias ownership in
`crates/gateway/src/config.rs`, the administrative catalogue projection in
`crates/gateway/src/admin/catalogue.rs`, and any new route that exposes
model or provider metadata.

**Regression tests.** Pattern semantics are the entitlement boundary:
`patterns_match_case_sensitively_and_union`, `prefix_does_not_subsume_other_globs`,
`an_empty_scope_permits_nothing`, and `invalid_patterns_are_rejected` — a glob
change that broadens a match is a privilege change. Projection:
`models_requires_a_gateway_key`, `models_lists_the_callers_aliases`, and the
namespace intersection tests in trigger 2. An alias a namespace owns is that
namespace's alone, in the catalogue and in resolution:
`an_owned_alias_is_listed_and_routable_only_by_its_namespace`,
`an_owned_alias_is_its_namespaces_own_and_shadows_the_deployments`, and
`rejects_an_alias_owned_by_a_namespace_the_deployment_does_not_define` — a name
one tenant publishes may not be enumerated or invoked by another, and an owner it
cannot serve is a refused file rather than a deployment-wide alias. The
administrative catalogue answers within one scope and says what it could not
consult: `a_tenant_read_is_isolated_and_explains_each_entry`,
`a_tenant_read_does_not_enumerate_a_projects_overrides`,
`a_project_of_another_tenant_yields_nothing`,
`a_read_names_the_facts_it_could_not_consult`,
`the_management_catalogue_reports_what_a_tenant_published`,
`a_catalogue_read_outside_the_grant_is_forbidden`, and
`a_catalogue_filter_that_cannot_be_parsed_is_refused` — a filter this build
cannot evaluate is refused rather than silently ignored, because an answer a
caller believes was narrowed is an entitlement claim it did not make.

Ingestion must stay inert:
`observed_pricing_is_metadata_not_activation`,
`the_source_is_background_only_and_declares_incremental_refresh`, and
`an_unreachable_source_is_retryable_and_never_a_boot_failure` — upstream
catalogue data must never become an entitlement or an admission dependency. The
source a deployment reads must also be the source it approved:
`a_catalogue_source_url_must_be_https`,
`a_catalogue_source_url_must_have_a_host_without_credentials`, and
`a_redirected_source_is_refused_rather_than_followed` — metadata an operator
prices and enables against may not arrive over a transport that can be
substituted, nor from wherever an answer redirected the import. The
durable side of the same rule: `an_observed_catalogue_rate_is_not_an_approved_price`,
`an_enablement_is_pinned_to_a_snapshot_the_revision_declares`,
`an_alias_resolves_in_order_within_its_own_reach_and_one_wire_family`,
`an_alias_is_a_project_scoped_name_unique_within_its_project`, and
`an_untyped_enablement_is_refused_rather_than_skipped` — a published entitlement
names one catalogue, reaches only its own owner, and is never inferred from an
unreadable row. Activation must stay an operator decision, and a price the
runtime cannot bill must not become a cheap one:
`a_revision_without_a_price_book_carries_no_approved_pricing`,
`approving_an_observed_rate_preserves_it_exactly`,
`a_draft_book_activates_no_prices`,
`a_rate_finer_than_a_micro_dollar_is_refused_rather_than_rounded`,
`a_negative_rate_is_refused`,
`a_rate_beyond_the_units_range_is_refused_as_an_overflow`,
`a_context_tiered_schedule_is_refused_naming_the_threshold`,
`an_audio_rate_is_refused_because_no_usage_field_would_bill_it`, and
`an_approved_book_with_no_readable_approver_is_refused`. An unpriced target must
stay *unpriced* rather than free — `a_target_the_book_does_not_price_has_no_price`
— and a refused book must not disarm the prices already in force:
`a_price_book_this_build_cannot_bill_leaves_the_previous_pricing_active`. What a
request was billed against stays attributable through
`a_snapshot_records_the_book_and_catalogue_it_priced_from`. A
new route is also covered mechanically: `ops/check-docs.py` fails a registered
route that the [compatibility contract](../compatibility.md) does not document.

Derived availability is evidence, never entitlement, and its tests hold that
line: `evidence_never_crosses_a_tenant_or_a_project_boundary` (one tenant's
discovery evidence cannot decide another's verdict),
`unknown_and_stale_evidence_is_never_silently_upgraded` and
`incomplete_discovery_is_unknown_and_never_a_denial` (a partial look neither
grants nor revokes), `a_discovery_outage_preserves_the_last_known_good_state`
(an outage costs freshness, not access),
`observation_detail_never_reaches_a_verdict` and
`a_namespace_scoped_verdict_coarsens_operator_only_reasons` (a verdict carries no
provider body, credential, or discovery mechanism a tenant may read), and
`projecting_availability_leaves_the_config_untouched` (an index is projected
beside a snapshot and can never enlarge what is served). Uncertainty is routable
only where a scope chose it:
`a_permitted_target_awaiting_discovery_is_distinct_from_a_key_nothing_describes`
holds that an empty or incomplete index permits nothing, because no rung examined
the pair.

Deriving that evidence from a revision is where catalogue presence could quietly
become permission, so the projection is held by
`a_catalogued_offering_nobody_entitled_is_denied_rather_than_available`,
`a_credential_whose_material_did_not_resolve_is_unknown_rather_than_granted`
(entitlement is credential *readiness*, not credential existence), and
`a_published_revision_derives_availability_that_catalogue_presence_alone_cannot_grant`
(the same property through the publication seam). Durable evidence carries no
operator detail and no authority:
`discovery_evidence_survives_a_restart_without_the_probes_own_words` and
`restored_evidence_does_not_restore_the_authority_a_revision_withdrew` hold that a
restored row decides nothing until a revision supplies the dimensions, while
`restoring_a_stale_positive_cannot_resurrect_a_target_a_listing_dropped`,
`restoring_refuses_a_row_whose_evidence_names_another_scope`,
`saving_a_record_replaces_the_evidence_it_held`, and
`one_tenants_discovery_evidence_is_not_another_tenants` hold the ordering,
mis-filing, replacement, and tenant-isolation rules against the database itself.
The read surface is scoped and answered from memory:
`an_availability_read_is_confined_to_the_scope_the_grant_encloses`,
`an_availability_read_must_name_the_tenant_it_asks_about`,
`an_availability_read_reaches_no_control_plane`,
`an_availability_read_distinguishes_deriving_nothing_from_finding_nothing`, and
`an_availability_read_overlays_this_replicas_own_health`. A discovery outage costs
freshness and nothing else —
`a_discovery_outage_ages_a_verdict_without_touching_convergence` and
`discovery_evidence_survives_the_revisions_published_over_it`.

**Threat model and ADRs.** [ADR 0020](../adr/0020-alias-wire-family-validation.md)
and [ADR 0012](../adr/0012-native-provider-routes.md) bound wire families and
native routes; the durable entitlement contract — opaque snapshot-pinned offering
identities, observed versus approved pricing, tenant defaults against project
overrides, and ordered single-wire-family alias targets — is
[ADR 0042](../adr/0042-model-enablement-and-alias-contracts.md), and a change to
what an enablement pins or to which targets an alias may name amends it;
[ADR 0058](../adr/0058-tenant-owned-alias-names-and-the-management-catalogue.md)
makes an alias name a namespace's own and states that the management catalogue is
an administrative read of a published revision rather than anything the request
path consults, so a change to who may see an alias or to what the catalogue
reports amends it; `CatalogSource`'s background-only placement is in
[backend contracts](../maintainers/backend-contracts.md).
[ADR 0043](../adr/0043-catalogue-source-imports.md) holds observed rates as
metadata and [ADR 0046](../adr/0046-approved-price-books.md) makes approval the
only activation, with exact conversion and no request-path pricing lookup — a
change that prices from mutable or unapproved data supersedes 0044 rather than
relaxing it. Item 2 of the security
review's accepted-risk section is why `/v1/models` is authenticated and scoped —
re-read it before changing that projection. The availability stance is a decision
of its own and is written down as
[ADR 0038](../adr/0038-derived-availability-and-discovery-evaluation.md): five
states, the precedence ladder, expiry in both directions, last-known-good
retention, and the rule that uncertainty is routable only where a scope chose it.
It inherits rather than revises the snapshot reasoning of
[ADR 0002](../adr/0002-stateless-by-default-stateful-by-opt-in.md) and
[ADR 0011](../adr/0011-config-hot-reload.md), and its per-scope evaluation is
covered by trigger 2's isolation reasoning; the slice that wires evaluation into
admission owes this trigger again on its own merits.

The projection, its durable evidence, and the scoped read that fill that contract
are
[ADR 0053](../adr/0053-stateful-availability-projection-and-discovery-persistence.md):
five authorities read from five places, entitlement as resolved credential
readiness, catalogue version kept apart from availability, replica-local health
overlaid rather than stored, and evidence persisted without the probe's own words
or any dimension a revision states.

**Trigger 4 answer for PR #344.** This change fires the trigger because it
changes durable model-alias publication rules. The security property is that a
newly authored enabled alias cannot advertise a disabled enablement, while a
legacy published alias with that shape remains readable for history and
rollback. Regression coverage proves both sides, including legacy hydration,
strict candidate refusal, enabled-to-disabled restack, and the already-disabled
republish case. ADR 0042 is amended. Release impact is deliberately asymmetric:
no retained revision becomes unreadable, but new candidates are stricter and
must repair the legacy shape before publication. The journal and revision
convergence runbooks record this compatibility stance and the complete revision
diff remains the resource-level retirement record.

**Release impact.** Availability contracts are inert on their own: nothing
constructs an index, `/v1/models` and readiness are unchanged, and no request
reads a verdict, so a release carrying only the contracts changes no observable
behaviour and needs no migration note. The projection slice adds a forward-only
tenant-isolated control-plane migration for discovery observations and
one authenticated administrative read, `GET /admin/v1/availability`; inference,
`/v1/models`, and readiness are still unchanged, so the note a release owes is the
migration rather than a behaviour change. The slice that wires an evaluation into
admission does change behaviour, and it fires this trigger again.

Entitlement changes are visible to clients: a pattern
semantics change can silently grant or revoke access at upgrade, so it needs a
migration note saying which existing configurations change meaning. Route and
wire-family additions are compatibility-contract entries; pricing changes affect
budgets, which the [production checklist](../deployment/production-checklist.md)
already asks operators to review. A change to an enablement or alias body's schema,
to its state vocabulary, or to what it pins is a *compatibility* change: say which
stored revisions the new build stops reading, and record it in the
[journal runbook](../operations/control-plane-journal.md) and
[revision convergence](../operations/revision-convergence.md#resource-body-schemas)
rather than only in the changelog. A price-book schema change is the same kind of
entry, because a replica that cannot read a book keeps serving the prices it
already converged onto.

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
usage rows carry `credential_id` and `credential_source`, never material; the
pricing publication line emits a resource reference, a content checksum, and a
catalogue content id — digests and names, not bodies — and what it attributes is
held by `a_snapshot_records_the_book_and_catalogue_it_priced_from`. A reason code
or metric label is a closed vocabulary, and adding a value to one is gated
mechanically by `every_revision_rejection_reason_is_catalogued`,
`every_reason_code_is_bounded_and_distinct`, and
`operator_only_reasons_coarsen_and_tenant_safe_ones_do_not`.
The control-plane ledger is the record of what DDL ran, so the writes to it are
gated too: `an_empty_ledger_is_refused_rather_than_migrated_from_zero`,
`a_hand_applied_schema_is_adopted_as_the_baseline_its_objects_prove`,
`a_schema_hand_applied_only_as_far_as_v1_adopts_v1_and_leaves_v2_pending`,
`a_partly_applied_schema_is_refused_without_recording_anything`,
`adoption_refuses_every_schema_that_is_not_an_empty_ledger`,
`concurrent_adoptions_record_the_baseline_once`,
`objects_in_another_schema_on_the_path_are_not_evidence_of_an_applied_baseline`,
`a_hand_applied_schema_missing_its_seed_row_is_refused_rather_than_adopted`,
`a_statement_whose_effect_cannot_be_confirmed_makes_its_migration_unadoptable`,
`a_tenancy_effect_undone_by_hand_is_refused_and_named`, and
`a_migration_no_object_can_account_for_blocks_adoption_of_the_whole_history` — a
baseline may only be recorded for migrations whose every statement is confirmed in
this schema, never by executing shipped DDL over a database that already has it,
and never at all once a migration ships a statement adoption cannot confirm. The
tenancy migration's row-security effects are part of that evidence rather than
assumed: a hand-applied database missing one table's `FORCE ROW LEVEL SECURITY` or
one `..._isolation` policy is refused by name, so adoption cannot record a history
whose tenant isolation is not actually in place.

Blob publication formats are held by
`head_documents_are_deterministic_bounded_and_strict`,
`authenticated_heads_refuse_tamper_replay_wrong_keys_and_rollback`,
`committed_winner_returns_success_after_shared_guard_observes_a_newer_head`,
`only_the_current_fenced_head_can_cross_from_history_into_activation`,
`manifest_signature_binds_every_publication_decision`,
`idempotency_is_authorization_scoped_and_durable_metadata_is_redacted`,
`bounded_history_exhaustion_is_visible_and_fails_novel_writes_closed`, and the
`publication_parsers` committed fuzz corpus. Object-store bytes are untrusted on
read: unknown schemas, signing schemas, algorithms, and keys; missing or invalid
signatures; cross-environment replay; rollback below the observed head tuple;
same-sequence/different-revision equivocation;
malformed or non-canonical encodings; invalid integrity; oversized documents;
impossible sequence links; and excessive object counts are typed fail-closed
refusals rather than recovery defaults. The signed manifest binds actor/grant
fingerprints, mutation identity/kind, scoped idempotency, state identity, and the
complete object set. Raw attribution, idempotency strings, signing material, and
secret values are absent from durable publication metadata and error rendering.
`VerifiedRevisionManifest` is authenticated history traversal only. Hydration
requires `VerifiedActiveRevision`, which carries the strong-read `ObjectVersion`
for the exact selecting head, and activation requires the non-cloneable wrapper
returned by a final unchanged-head validation. Signed orphan manifests, crash
uploads, losing CAS bodies, and failed-condition payloads therefore remain
unreachable even if an intermediary retains and later replays their bytes. The
domain guard exports both sequence and active digest because sequence alone
would not detect equivocation, but this slice retains that tuple only in memory
and does not modify or claim integration with the production last-known-good
cache. Cross-restart rollback/equivocation resistance remains unqualified until
the authenticated blob LKG runtime slice persists and restores the bound tuple.
Publication trust is bounded to 64 keys.

**Threat model and ADRs.** [ADR 0007](../adr/0007-telemetry-model.md),
[ADR 0009](../adr/0009-durable-usage-sinks.md),
[ADR 0049](../adr/0049-billing-grade-usage-outbox.md),
[ADR 0017](../adr/0017-state-tiers-and-optional-backends.md), and
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) hold the
telemetry, durability, and tier positions; section 3 of the security review is
the argument that logs, spans, metrics, and usage rows carry references only.
Raising a deployment's state tier, or making an existing feature depend on a
store it did not need, is an ADR with the template's state-tier declaration.
Free-form caller input must not become a metric attribute — that is a
cardinality *and* a disclosure decision.

A change to the *delivery guarantee* is part of this trigger too, not just a
change to the row. The billing-grade usage outbox
([ADR 0049](../adr/0049-billing-grade-usage-outbox.md)) puts a durable write on
the request path for deployments that opt in, so the review question is
availability as much as disclosure: with the defaults, an outbox that is full or
unreachable refuses requests, and the escapes from that (`capacity_policy =
"drop-oldest"`, `on_undurable = "serve"`) are accounted losses rather than silent
ones. Anything that changes which of those a deployment gets — a default, a
refusal, a retention or capacity bound, what may be pruned to make room — owes
that argument. Quarantined events are deliberately not prunable: an operator's
evidence must not be deleted to free capacity.

**Release impact.** Schema changes are ordered operator work: name the DDL that
must be applied before writers, whether mixed versions may run, and the rollback
limit, in the [upgrade guide](../operations/upgrades.md) and the
[control-plane journal](../operations/control-plane-journal.md) or
[usage schema](../usage-schema.md) or the [usage
outbox](../operations/usage-outbox.md) as appropriate. A field removed or renamed in
a usage row breaks somebody's billing pipeline and is a documented contract
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
at the tag: `publish-dry-run` packages each crate from its own tarball in
dependency order, `docker-smoke` and `quickstart-smoke` boot what is shipped,
`static-binary` proves the musl build and runs `ops/tier0-gate.sh`, the
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

**Fires on** any change to who a stateful deployment's administrators are and
what they may reach: the identity and role vocabulary in
`crates/gateway/src/desired_state/access.rs`, tenant lifecycle and project
ownership in `crates/gateway/src/desired_state/tenancy.rs`, the authorization
decision an `/admin/v1` handler consumes, the tenancy and principal projection
written by `crates/gateway/src/backends/control_plane/postgres.rs`, the
row-level-security policies and tenant-scoped constraints in
`crates/gateway/sql/control_plane_0002_tenancy_access.sql` and the deferred rules
and attribution filters that replace some of them in
`crates/gateway/sql/control_plane_0003_tenancy_constraints.sql`, the journal
ownership keys in
`crates/gateway/sql/control_plane_0004_journal_ownership.sql`, and the
denied-action record. A new administrative surface or action fires this trigger even when the
handler is downstream work: the matrix is exhaustive, so a surface with no
decided action is a hole rather than an omission.

The three risks this area exists to bound, and where each is answered:

- **Confused deputy.** The control plane acts on a caller's behalf, so a request
  naming another tenant's resource must be refused *before* it is persisted, not
  filtered afterwards. Scope containment is one-directional — a project-scoped
  caller does not reach its tenant, and a tenant-scoped caller does not reach the
  deployment — and a role cannot be granted at a scope it does not mean. The
  database holds the same rule independently: a projected row cannot name a
  tenant nothing declared or a project another tenant owns, so a service-layer
  bug is a failed transaction rather than a cross-tenant write. Since 0004 that
  covers the journal too, with its coverage stated rather than assumed: the keys
  are `NOT VALID`, so every row written after they land names a tenant this
  deployment has a row for and rows stored before them keep 0002's exemption,
  because validating those means inventing an owner for a tenant no revision
  declared. Republishing history satisfies the key instead of being refused by it —
  the publishing transaction records an owner it names and no revision declares at
  `lifecycle = "deleted"`, which serves nobody and is never rewritten over a live
  tenant. Ownership stops at the tenant, the boundary row-level security is keyed
  on; a journal row's project stays a domain check, because a project has no
  lifecycle to publish and a synthesized project row would be indistinguishable
  from a declared one. What the keys state is that the tenant has a *row* —
  declared, retained, or recorded for history — which a retired tenant has satisfied
  since 0002; "the revision that stores this row declares its tenant" stays the
  service layer's. Two things keep a publication from widening that set to its own
  benefit: the recording step runs after the deferred keys settle, so it cannot
  supply the ownership its own principals need, and it covers the state's resource
  scopes only — the mutation scope and actor attribution travelling with a
  publication get no owner row, so a caller cannot leave one behind for an
  identifier it merely names.
- **Identifier enumeration.** A refusal tells the caller `forbidden` and nothing
  else. Whether a tenant exists, whether a principal resolved, and which half of
  an OIDC pair was wrong are recorded in the denial row and never returned, so a
  caller cannot distinguish "no such tenant" from "not yours" and cannot walk the
  id space with a well-formed request. Denials are read per tenant for the same
  reason: one tenant's refusals are not another tenant's reconnaissance. Which
  page a caller gets is not a parameter — a `DenialPage` is built from an
  authorization, so the deployment page, the one place another tenant's workload
  appears as an actor, requires a decision at deployment scope that a
  tenant-scoped principal cannot obtain. A failed publication is not an oracle
  either: a projection refusal names the constraint that refused it and never the
  colliding row, whose sign-in or key digest stays in the operator's log.
- **Noisy neighbour.** Tenancy is an *authorization* boundary here, not a
  capacity one. The namespace-level fairness the data plane already has is
  trigger 2's; per-tenant admission, budgets, and availability are deliberately
  not decided by this layer and remain the downstream work of dynamic limits and
  availability evaluation. Section 8 of the
  [security review](../security-review-2026-08-05.md) records that tenant
  availability is not defended by the scoping boundary, and this layer does not
  change that: nothing here may become an admission or request-path dependency,
  because a control-plane lookup on the hot path is exactly how one tenant's
  administrative load becomes another tenant's latency.

**Regression tests.** The matrix is pinned exactly rather than sampled:
`the_authorization_matrix_is_exactly_the_intended_one`,
`only_a_platform_admin_creates_tenants_and_only_admins_grant_roles`, and
`a_role_is_grantable_only_at_the_scopes_it_means` — a widened cell fails the
first of those, so a grant cannot be broadened silently. Isolation and
containment: `a_caller_of_one_tenant_cannot_reach_another`,
`a_project_scoped_caller_cannot_reach_its_tenant_and_a_narrow_role_cannot_widen`,
`scope_containment_is_one_way`, and
`a_directory_refuses_a_cross_tenant_or_unscoped_identity`. Non-disclosure:
`an_unresolvable_caller_is_refused_without_saying_which_half_was_wrong`,
`a_denial_is_recorded_with_its_reason_and_no_caller_supplied_bytes`, and
`a_denial_carries_no_secret_material`. Identity: `one_person_is_one_principal`,
`a_minted_key_is_shown_once_hashed_at_rest_and_verified_in_constant_time`, and
`a_workload_authenticates_by_digest_and_a_revoked_one_authenticates_with_nothing`
— key material is shown once and stored as a digest, so this trigger fires
trigger 3's obligations too. Lifecycle and the mode boundary:
`a_disabled_tenant_is_administrable_and_a_deleted_one_is_not`,
`breakglass_is_allowed_everything_and_recorded_as_itself`,
`breakglass_and_the_gateway_recover_the_deployment_not_a_deleted_tenant` — the
recovery path is deployment-scoped on purpose: an undeclared or deleted tenant
refuses every caller, breakglass included, and getting one back means publishing
a revision that declares it again rather than reaching into a tombstone with a
tenant-scoped call —
`the_gateways_own_work_reads_anywhere_and_writes_only_its_catalogues`, and
`a_deployment_with_no_published_directory_grants_nothing_but_breakglass` for the
fail-closed floor: an empty directory authorizes nobody but the static
breakglass operator. The durable half runs against Postgres and skips without it,
so run it the way [CONTRIBUTING](../../CONTRIBUTING.md#development) documents:
`publishing_a_revision_projects_the_owners_and_the_directory_it_declares`,
`a_tenant_lifecycle_transition_is_a_row_update_and_never_a_delete`,
`a_projected_row_cannot_name_an_absent_tenant_or_another_tenants_project`,
`a_journal_row_cannot_name_a_tenant_this_deployment_has_no_row_for` and
`a_revision_naming_an_undeclared_tenant_records_that_owner_as_deleted` for the
journal keys and the exemption they keep — the first asserts that the four keys are
present *and* unvalidated, so a later change that quietly validates them fails
here rather than in an operator's upgrade —
`denied_actions_are_recorded_and_read_back_per_tenant_newest_first`,
`denials_are_recorded_once_and_read_newest_first_per_tenant` for the in-memory
oracle's agreement with it, and
`a_session_pinned_to_one_tenant_reads_no_other_tenants_rows` for the row-level
security behind the service layer, asserted against a role that cannot bypass it
— including a change recorded against the pinned tenant by another tenant's
workload, which is that tenant's own history and stays readable. The two
non-disclosure boundaries above are pinned by
`only_a_deployment_scoped_decision_reads_the_unscoped_refusal_trail` and
`a_projection_refusal_names_the_constraint_and_not_the_row_it_hit`.

**Threat model and ADRs.**
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) owns the mode
boundary — a directory exists only in stateful mode, and stateless deployments
keep the namespace boundary of trigger 2 unchanged — and
[ADR 0017](../adr/0017-state-tiers-and-optional-backends.md) owns the tier a
durable directory implies. Sections 6 and 8 of the security review are the
isolation argument and the recorded non-goals; a change that makes the boundary
weaker, or that makes an inference request depend on a control-plane read,
contradicts both and needs an ADR rather than a configuration key. Row-level
security is defence in depth and never the only check: a change that moves an
authorization decision *into* SQL supersedes that position explicitly.

**Release impact.** A role, surface, or action rename is an administrative
contract change and a schema change at once: the projected role vocabulary is a
`CHECK` constraint, so name the DDL an operator applies and whether a mixed-
version fleet may run, per the [upgrade guide](../operations/upgrades.md) and the
[control-plane journal](../operations/control-plane-journal.md). New tenant-scoped
constraints on existing history are ordered work: a constraint added `NOT VALID`
either names the backfill that lets it be validated, or states — in the migration,
the runbook, and a test — that validation is not pending and why, as 0004 does for
the journal keys. Reporting an operator a validation step they can never complete
is worse than stating the boundary.

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
