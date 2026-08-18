#!/usr/bin/env python3
"""Shared semantic contract for schema-2 recovery evidence.

The lane checker, shell recorder, and promotion boundary all need to interpret
the same raw document.  Keep the policy here free of filesystem and CLI state so
the promoter can import it and apply the identical gate, verdict, observation,
and provenance rules after it has bound artifact hashes.
"""

from __future__ import annotations

import math
import re
from decimal import Decimal, InvalidOperation
from typing import Any


RECOVERY_RESULT_SCHEMA_VERSION = 2
RELEASE_CARGO_PROFILE = "release"
LOWER_SHA256 = re.compile(r"^[0-9a-f]{64}$")
VERDICT_OUTCOMES = frozenset({"met", "failed", "not_evaluated"})
VERDICT_FIELDS = frozenset({"gate", "bound", "observed", "outcome", "detail"})

REQUIRED_GATE_NAMES = (
    "max_serving_error_fraction",
    "max_convergence_lag_seconds",
    "max_data_loss_revisions",
    "readiness",
    "admin_writes",
    "max_unauthenticated_admin_successes",
)

# At least one of the named evidence classes is required before a stage may
# evaluate the gate.  A stage without one must retain an explicit
# `not_evaluated` verdict explaining where the measurement belongs.
GATE_EVIDENCE: dict[str, frozenset[str]] = {
    "max_serving_error_fraction": frozenset({"serving_behavior"}),
    "max_convergence_lag_seconds": frozenset({"convergence_lag"}),
    "max_data_loss_revisions": frozenset({"revision_loss_boundary"}),
    "readiness": frozenset({"serving_behavior", "fail_open_closed", "cold_start"}),
    "admin_writes": frozenset(
        {"audit_auth", "outage_timeline", "revisions", "fail_open_closed"}
    ),
    "max_unauthenticated_admin_successes": frozenset({"audit_auth"}),
}


# One executable stage owns each hard gate in every recovery scenario.  This is
# deliberately separate from the broader evidence-class grants above: evidence
# classes say which stages *could* measure a gate, while this table says which
# single stage must reduce that measurement to the scenario verdict.  Without
# the second relation, every stage can defer every gate and a scenario can pass
# without ever evaluating one of its committed bounds.
#
# The assignments follow the manifest's stage responsibilities.  Serving and
# readiness belong to the stage that offers traffic or probes the expected
# fail-closed state; convergence and revision loss belong to the lifecycle
# stage; administrative availability and anonymous access belong to the stage
# that exercises those surfaces.  `validate_gate_ownership_model` also proves
# that every owner exists, is executable, and has a manifest evidence class
# capable of supporting its gate.  Consequently a producer cannot make a new
# assignment effective merely by emitting a verdict the manifest did not grant.
GATE_OWNERS: dict[str, dict[str, str | None]] = {
    "control-plane-outage": {
        "max_serving_error_fraction": "serving",
        "max_convergence_lag_seconds": None,
        "max_data_loss_revisions": "journal-outage",
        "readiness": "serving",
        "admin_writes": "journal-outage",
        "max_unauthenticated_admin_successes": "administration",
    },
    "cold-boot-valid-cache": {
        "max_serving_error_fraction": "serving",
        "max_convergence_lag_seconds": None,
        "max_data_loss_revisions": "cold-boot",
        "readiness": "cold-boot",
        "admin_writes": "serving",
        "max_unauthenticated_admin_successes": "serving",
    },
    "cold-boot-no-cache": {
        "max_serving_error_fraction": None,
        "max_convergence_lag_seconds": None,
        "max_data_loss_revisions": None,
        "readiness": "cold-boot",
        "admin_writes": "readiness",
        "max_unauthenticated_admin_successes": "readiness",
    },
    "cold-boot-invalid-cache": {
        "max_serving_error_fraction": None,
        "max_convergence_lag_seconds": None,
        "max_data_loss_revisions": None,
        "readiness": "cold-boot",
        "admin_writes": "readiness",
        "max_unauthenticated_admin_successes": "readiness",
    },
    "recovery-convergence": {
        "max_serving_error_fraction": "serving",
        "max_convergence_lag_seconds": "journal-recovery",
        "max_data_loss_revisions": "journal-recovery",
        "readiness": "serving",
        "admin_writes": "journal-recovery",
        "max_unauthenticated_admin_successes": "administration",
    },
    "secret-rotation": {
        "max_serving_error_fraction": "serving",
        "max_convergence_lag_seconds": "rotation",
        "max_data_loss_revisions": "rotation",
        "readiness": "serving",
        "admin_writes": "rotation",
        "max_unauthenticated_admin_successes": "serving",
    },
    "backup-restore": {
        "max_serving_error_fraction": "reconvergence",
        "max_convergence_lag_seconds": "reconvergence",
        "max_data_loss_revisions": "restore",
        "readiness": "reconvergence",
        "admin_writes": "restore",
        "max_unauthenticated_admin_successes": "administration",
    },
    "point-in-time-recovery": {
        "max_serving_error_fraction": "reconvergence",
        "max_convergence_lag_seconds": "reconvergence",
        "max_data_loss_revisions": "recovery",
        "readiness": "reconvergence",
        "admin_writes": "recovery",
        "max_unauthenticated_admin_successes": "administration",
    },
}

# A present ``None`` owner is an explicit applicability decision, not a missing
# assignment. These gates describe a surface the scenario deliberately never
# reaches: outage/cached cold boots do not converge against an unavailable
# journal, and fail-closed cold boots publish no snapshot against which serving
# errors or revision loss could truthfully be measured.
GATE_NOT_APPLICABLE: dict[str, dict[str, str]] = {
    "control-plane-outage": {
        "max_convergence_lag_seconds": "the journal is intentionally unavailable, so the replica diverges rather than converges",
    },
    "cold-boot-valid-cache": {
        "max_convergence_lag_seconds": "the replica restores a cache while the journal is unavailable and performs no convergence",
    },
    "cold-boot-no-cache": {
        "max_serving_error_fraction": "no serving snapshot is published and no inference traffic is offered",
        "max_convergence_lag_seconds": "the journal is unavailable and the expected terminal state is refusal",
        "max_data_loss_revisions": "no serving revision is admitted, so there is no recovered revision boundary to compare",
    },
    "cold-boot-invalid-cache": {
        "max_serving_error_fraction": "no authenticated cache is admitted and no inference traffic is offered",
        "max_convergence_lag_seconds": "the journal is unavailable and the expected terminal state is cache refusal",
        "max_data_loss_revisions": "no serving revision is admitted, so there is no recovered revision boundary to compare",
    },
}


# Minimum stage-specific measurements needed to make every committed evidence
# class concrete.  These are deliberately raw observations, not prose timeline
# labels: deleting the values that establish a durable boundary must invalidate
# an otherwise passing artifact.
STAGE_REQUIRED_OBSERVATIONS: dict[str, frozenset[str]] = {
    "control-plane-outage/journal-outage": frozenset(
        {
            "revision",
            "active_revision",
            "convergence_rejection_reason",
            "convergence_lag_ms",
            "proxy_severed_connections",
            "admin_write_status",
            "admin_write_error",
        }
    ),
    "control-plane-outage/serving": frozenset(
        {"revision", "proxy_severed_connections", "inference_status", "ready_status"}
    ),
    "control-plane-outage/administration": frozenset(
        {"authenticated_state_status", "mutation_status", "anonymous_state_status"}
    ),
    "cold-boot-valid-cache/cold-boot": frozenset(
        {
            "boot_note",
            "cold_start_outcome",
            "cached_revision",
            "restored_revision",
            "active_revision",
            "snapshot_source",
            "ready_status",
        }
    ),
    "cold-boot-valid-cache/serving": frozenset(
        {
            "ready_status",
            "chat_status",
            "anonymous_models_status",
            "admin_catalogue_status",
            "admin_mutation_status",
            "anonymous_admin_status",
        }
    ),
    "cold-boot-no-cache/cold-boot": frozenset(
        {
            "boot_note",
            "cold_start_outcome",
            "refusal",
            "snapshot_generation_after_cold_boot",
            "ready_status",
            "active_revision",
            "anonymous_models_status",
        }
    ),
    "cold-boot-no-cache/readiness": frozenset(
        {
            "ready_status",
            "admin_state_status",
            "admin_mutation_status",
            "anonymous_admin_status",
            "anonymous_models_status",
        }
    ),
    "cold-boot-invalid-cache/cold-boot": frozenset(
        {
            "boot_note",
            "cold_start_outcome",
            "unauthentic_cache_variants_refused",
            "ready_status",
            "edited_record_refused",
            "truncated_file_refused",
            "foreign_signing_key_refused",
        }
    ),
    "cold-boot-invalid-cache/readiness": frozenset(
        {
            "ready_status",
            "admin_state_status",
            "admin_mutation_status",
            "anonymous_admin_status",
            "anonymous_models_status",
        }
    ),
    "recovery-convergence/journal-recovery": frozenset(
        {
            "outage_revision",
            "unseen_revision",
            "recovered_head_revision",
            "active_revision",
            "direct_replica_active_revision",
            "loaded_unseen_revision",
            "snapshot_source",
            "converged",
            "residual_lag_ms",
            "recovery_seconds",
            "recovered_history_revisions",
            "recovered_history_contains_required_revisions",
            "post_recovery_write_accepted",
        }
    ),
    "recovery-convergence/serving": frozenset(
        {"revision", "source", "converged", "chat_status", "ready_status"}
    ),
    "recovery-convergence/administration": frozenset(
        {"audit_status", "actor", "anonymous_admin_status"}
    ),
    "secret-rotation/rotation": frozenset(
        {
            "revision",
            "active_revision",
            "source",
            "converged",
            "rotation_seconds",
            "publication_accepted",
            "rotated_revision_published",
            "rotation_history_contains_required_revisions",
            "same_replica_before_and_after_rotation",
        }
    ),
    "secret-rotation/serving": frozenset(
        {
            "chat_status",
            "ready_status",
            "audit_status",
            "audit_actor",
            "credential",
            "anonymous_admin_status",
            "rotated_material_authenticated_upstream",
        }
    ),
    "backup-restore/restore": frozenset(
        {
            "live_head_revision",
            "live_revisions",
            "live_resources",
            "restored_head_revision",
            "restored_revision_count",
            "restored_resource_count",
            "live_head_checksum",
            "restored_head_checksum",
            "restore_duration_seconds",
            "catalogue_preboot_content_id",
            "catalogue_preboot_raw_digest",
            "catalogue_preboot_raw_bytes",
            "catalogue_preboot_payload_bytes",
            "catalogue_preboot_snapshot_rows",
            "revision_after_restore",
            "restored_readiness_status",
            "restored_inference_status",
            "restored_inference_error",
        }
    ),
    "backup-restore/administration": frozenset(
        {"audit_events_for_head", "unauthenticated_admin_successes"}
    ),
    "backup-restore/durable-inventory": frozenset(
        {
            "expected_secret_owner",
            "restored_secret_versions",
            "restored_secret_owner",
            "restored_secret_ciphertext_rows",
            "restored_catalog_snapshot_rows",
            "expected_catalog_active_content",
            "restored_catalog_active_content",
            "live_usage_rows",
            "restored_usage_rows",
            "live_usage_outbox_rows",
            "restored_usage_outbox_rows",
            "logical_backup_usage_request_id",
            "logical_backup_usage_status",
            "logical_backup_new_usage_rows",
            "logical_backup_source_usage_identity_rows",
            "logical_backup_source_outbox_identity_rows",
            "logical_backup_restored_usage_identity_rows",
            "logical_backup_restored_outbox_identity_rows",
            "restored_price_book_rows",
            "live_price_book_checksum",
            "restored_price_book_checksum",
            "restored_price_book_schema",
            "restored_price_book_catalog_version",
            "restored_price_book_approval_state",
            "restored_price_book_approval_citation",
            "restored_price_book_rule_count",
            "expected_price_book_history",
            "restored_price_book_history",
        }
    ),
    "backup-restore/reconvergence": frozenset(
        {
            "survivor_before_revision",
            "restored_revision",
            "restored_revision_count",
            "survivor_active_revision",
            "survivor_convergence_lag_seconds",
            "survivor_readiness_status",
            "survivor_inference_status",
            "survivor_revision_count",
            "unauthenticated_admin_successes",
        }
    ),
    "point-in-time-recovery/recovery": frozenset(
        {
            "recovery_target",
            "recovery_in_progress",
            "recovered_schema_status",
            "pre_target_head_revision",
            "post_target_head_revision",
            "revisions_before_target",
            "restore_duration_seconds",
            "pitr_catalogue_preboot_content_id",
            "pitr_catalogue_preboot_raw_digest",
            "pitr_catalogue_preboot_raw_bytes",
            "pitr_catalogue_preboot_payload_bytes",
            "pitr_catalogue_preboot_snapshot_rows",
            "pitr_secret_versions",
            "expected_secret_owner",
            "pitr_secret_owner",
            "pitr_secret_lifecycle",
            "expected_pitr_catalogue_content_id",
            "pitr_catalogue_content_id",
            "expected_pitr_catalogue_raw_digest",
            "pitr_catalogue_raw_digest",
            "expected_pitr_catalogue_raw_bytes",
            "pitr_catalogue_raw_bytes",
            "pitr_catalogue_payload_bytes",
            "pitr_catalogue_snapshot_rows",
            "recovered_head_revision",
            "revisions_after_recovery",
            "post_target_revision_presence",
            "revision_after_recovery",
            "revisions_after_recovery_publication",
            "recovered_axond_usage_table_count",
            "recovered_axond_budget_table_count",
            "recovered_axond_revocation_table_count",
            "readiness_probe",
        }
    ),
    "point-in-time-recovery/administration": frozenset(
        {"audit_events_for_head", "unauthenticated_admin_successes"}
    ),
    "point-in-time-recovery/usage-boundary": frozenset(
        {
            "pre_target_usage_request_id",
            "post_target_usage_request_id",
            "pre_target_chat_status",
            "post_target_chat_status",
            "pre_target_usage_count",
            "post_target_usage_count",
            "pre_target_new_usage_rows",
            "post_target_new_usage_rows",
            "recovered_pre_target_usage",
            "recovered_pre_target_outbox",
            "recovered_post_target_usage",
            "recovered_post_target_outbox",
        }
    ),
    "point-in-time-recovery/reconvergence": frozenset(
        {
            "survivor_before_revision",
            "recovered_revision",
            "survivor_active_revision",
            "survivor_convergence_lag_seconds",
            "survivor_readiness_status",
            "survivor_inference_status",
            "survivor_revision_count",
            "unauthenticated_admin_successes",
        }
    ),
}


# These stages carry especially important multi-field boundaries.  Requiring the
# named equality checks prevents a producer from retaining plausible-looking
# observations and replacing the actual durable assertions with one unrelated
# passing check.
STAGE_REQUIRED_CHECKS: dict[str, frozenset[str]] = {
    "control-plane-outage/journal-outage": frozenset(
        {
            "active_revision_survives_the_cut",
            "convergence_reports_unavailable",
            "administrative_write_is_typed",
        }
    ),
    "control-plane-outage/serving": frozenset(
        {"inference_remains_available", "postgres_path_was_severed"}
    ),
    "control-plane-outage/administration": frozenset(
        {
            "authenticated_administration_refused",
            "mutation_refused",
            "anonymous_administration_refused",
        }
    ),
    "recovery-convergence/journal-recovery": frozenset(
        {
            "unseen_revision_loaded",
            "recovered_head_active",
            "fleet_reaches_one_head",
            "recovered_history_is_whole",
        }
    ),
    "recovery-convergence/serving": frozenset(
        {"recovered_revision_loaded", "recovered_inference_served"}
    ),
    "recovery-convergence/administration": frozenset(
        {"recovered_audit_is_readable", "recovered_audit_is_authenticated"}
    ),
    "secret-rotation/rotation": frozenset(
        {"rotated_revision_published", "no_restart"}
    ),
    "secret-rotation/serving": frozenset(
        {
            "rotated_material_authenticated_upstream",
            "authenticated_audit_attribution",
        }
    ),
    "cold-boot-valid-cache/cold-boot": frozenset(
        {
            "restored_revision_is_cached_revision",
            "snapshot_source_is_last_known_good",
            "cold_process_is_ready",
        }
    ),
    "cold-boot-valid-cache/serving": frozenset(
        {
            "readiness_serves_cached_snapshot",
            "anonymous_inference_refused",
            "administrative_read_refused_without_control_plane",
        }
    ),
    "cold-boot-invalid-cache/cold-boot": frozenset(
        {
            "edited_record_refused",
            "truncated_file_refused",
            "foreign_signing_key_refused",
        }
    ),
    "cold-boot-no-cache/cold-boot": frozenset(
        {
            "no_active_revision",
            "readiness_refuses_without_cache",
            "authentication_remains_first",
        }
    ),
    "cold-boot-no-cache/readiness": frozenset(
        {
            "readiness_refuses_without_cache",
            "authenticated_administration_refuses_without_control_plane",
            "administration_requires_authentication",
        }
    ),
    "cold-boot-invalid-cache/readiness": frozenset(
        {
            "readiness_refuses_with_invalid_cache",
            "authenticated_administration_refuses_without_control_plane",
            "administration_requires_authentication",
        }
    ),
    "backup-restore/durable-inventory": frozenset(
        {
            "the_restored_secret_version_is_active",
            "the_restored_secret_version_owner_survives",
            "the_restored_secret_material_is_encrypted",
            "the_restored_catalog_snapshot_survives",
            "the_restored_catalog_active_pointer_survives",
            "the_restored_usage_rows_match_backup",
            "the_restored_usage_outbox_matches_backup",
            "the_logical_backup_usage_fixture_was_answered",
            "the_logical_backup_usage_fixture_is_nonempty",
            "the_logical_backup_usage_fixture_is_exactly_one_new_row",
            "the_logical_backup_usage_identity_is_canonical",
            "the_logical_backup_usage_identity_is_in_the_source",
            "the_logical_backup_usage_outbox_identity_is_in_the_source",
            "the_logical_backup_usage_identity_survives",
            "the_logical_backup_usage_outbox_identity_survives",
            "the_restored_price_book_survives",
            "the_restored_price_book_checksum_matches",
            "the_restored_price_book_schema_is_current",
            "the_restored_price_book_catalogue_version_survives",
            "the_restored_price_book_approval_survives",
            "the_restored_price_book_approval_citation_survives",
            "the_restored_price_book_has_two_historical_rules",
            "the_restored_price_history_is_exact",
        }
    ),
    "point-in-time-recovery/recovery": frozenset(
        {
            "the_recovered_cluster_promotes",
            "the_recovered_schema_is_current",
            "the_pitr_secret_metadata_survives_the_target",
            "the_pitr_secret_owner_survives_the_target",
            "the_pitr_secret_lifecycle_survives_the_target",
            "the_pitr_catalogue_snapshot_survives_the_target",
            "the_pitr_catalogue_raw_digest_survives_the_target",
            "the_pitr_catalogue_raw_bytes_survive_the_target",
            "the_pitr_catalogue_payload_survives_the_target",
            "the_recovered_head_is_the_pre_target_revision",
            "nothing_published_before_the_target_is_lost",
            "the_write_after_the_target_is_not_replayed",
            "a_publication_against_the_recovered_head_is_accepted",
            "the_axond_usage_schema_is_recovered",
            "the_axond_budget_schema_is_recovered",
            "the_axond_revocation_schema_is_recovered",
        }
    ),
    "point-in-time-recovery/usage-boundary": frozenset(
        {
            "the_pre_target_usage_request_is_answered",
            "the_post_target_usage_request_is_answered",
            "the_pre_target_usage_request_creates_one_identity",
            "the_post_target_usage_request_creates_one_identity",
            "the_usage_request_ids_are_canonical",
            "the_usage_request_ids_are_globally_unique",
            "the_pre_target_usage_record_survives",
            "the_pre_target_usage_outbox_event_survives",
            "the_post_target_usage_record_is_not_replayed",
            "the_post_target_usage_outbox_event_is_not_replayed",
        }
    ),
}


# Every required recovery check is reconstructed from retained raw
# observations and committed literals.  The recorded check tuple is never an
# input to this relation: changing both its bound and observed values therefore
# cannot turn a forged assertion into evidence.
#
# Operand forms are intentionally small and auditable:
#   ("literal", value)
#   ("observation", key)
#   ("all_positive", key, ...)
#   ("positive", key)
#   ("canonical_request_id", key)
#   ("canonical_request_id_pair", first_key, second_key)
#   ("distinct", first_key, second_key)
#   ("accepted_revision", key)
#   ("boolean_label", key, true_label, false_label)
#   ("positive_label", key, positive_label, nonpositive_label)
#   ("null_label", key, null_label, present_label)
CHECK_RECONSTRUCTIONS: dict[str, dict[str, tuple[tuple[str, ...], tuple[str, ...]]]] = {
    "control-plane-outage/journal-outage": {
        "active_revision_survives_the_cut": (("observation", "revision"), ("observation", "active_revision")),
        "convergence_reports_unavailable": (("literal", "unavailable"), ("observation", "convergence_rejection_reason")),
        "administrative_write_is_typed": (("literal", "control_plane_unavailable"), ("observation", "admin_write_error")),
    },
    "control-plane-outage/serving": {
        "inference_remains_available": (("literal", "200"), ("observation", "inference_status")),
        "postgres_path_was_severed": (("literal", "at-least-one"), ("positive_label", "proxy_severed_connections", "at-least-one", "none")),
    },
    "control-plane-outage/administration": {
        "authenticated_administration_refused": (("literal", "503"), ("observation", "authenticated_state_status")),
        "mutation_refused": (("literal", "503"), ("observation", "mutation_status")),
        "anonymous_administration_refused": (("literal", "401"), ("observation", "anonymous_state_status")),
    },
    "recovery-convergence/journal-recovery": {
        "unseen_revision_loaded": (("observation", "unseen_revision"), ("observation", "loaded_unseen_revision")),
        "recovered_head_active": (("observation", "recovered_head_revision"), ("observation", "active_revision")),
        "fleet_reaches_one_head": (("observation", "recovered_head_revision"), ("observation", "direct_replica_active_revision")),
        "recovered_history_is_whole": (("literal", "three-required-revisions"), ("boolean_label", "recovered_history_contains_required_revisions", "three-required-revisions", "incomplete-history")),
    },
    "recovery-convergence/serving": {
        "recovered_revision_loaded": (("literal", "true"), ("boolean_label", "converged", "true", "false")),
        "recovered_inference_served": (("literal", "200"), ("observation", "chat_status")),
    },
    "recovery-convergence/administration": {
        "recovered_audit_is_readable": (("literal", "200"), ("observation", "audit_status")),
        "recovered_audit_is_authenticated": (("literal", "breakglass"), ("observation", "actor")),
    },
    "secret-rotation/rotation": {
        "rotated_revision_published": (("literal", "true"), ("boolean_label", "rotated_revision_published", "true", "false")),
        "no_restart": (("literal", "true"), ("boolean_label", "same_replica_before_and_after_rotation", "true", "false")),
    },
    "secret-rotation/serving": {
        "rotated_material_authenticated_upstream": (("literal", "true"), ("boolean_label", "rotated_material_authenticated_upstream", "true", "false")),
        "authenticated_audit_attribution": (("literal", "breakglass"), ("observation", "audit_actor")),
    },
    "cold-boot-valid-cache/cold-boot": {
        "restored_revision_is_cached_revision": (("observation", "cached_revision"), ("observation", "active_revision")),
        "snapshot_source_is_last_known_good": (("literal", "last-known-good"), ("observation", "snapshot_source")),
        "cold_process_is_ready": (("literal", "200"), ("observation", "ready_status")),
    },
    "cold-boot-valid-cache/serving": {
        "readiness_serves_cached_snapshot": (("literal", "200"), ("observation", "ready_status")),
        "anonymous_inference_refused": (("literal", "401"), ("observation", "anonymous_models_status")),
        "administrative_read_refused_without_control_plane": (("literal", "503"), ("observation", "admin_catalogue_status")),
    },
    "cold-boot-invalid-cache/cold-boot": {
        "edited_record_refused": (("literal", "refused"), ("boolean_label", "edited_record_refused", "refused", "accepted")),
        "truncated_file_refused": (("literal", "refused"), ("boolean_label", "truncated_file_refused", "refused", "accepted")),
        "foreign_signing_key_refused": (("literal", "refused"), ("boolean_label", "foreign_signing_key_refused", "refused", "accepted")),
    },
    "cold-boot-no-cache/cold-boot": {
        "no_active_revision": (("literal", "none"), ("null_label", "active_revision", "none", "present")),
        "readiness_refuses_without_cache": (("literal", "503"), ("observation", "ready_status")),
        "authentication_remains_first": (("literal", "401"), ("observation", "anonymous_models_status")),
    },
    "cold-boot-no-cache/readiness": {
        "readiness_refuses_without_cache": (("literal", "503"), ("observation", "ready_status")),
        "authenticated_administration_refuses_without_control_plane": (("literal", "503"), ("observation", "admin_state_status")),
        "administration_requires_authentication": (("literal", "401"), ("observation", "anonymous_admin_status")),
    },
    "cold-boot-invalid-cache/readiness": {
        "readiness_refuses_with_invalid_cache": (("literal", "503"), ("observation", "ready_status")),
        "authenticated_administration_refuses_without_control_plane": (("literal", "503"), ("observation", "admin_state_status")),
        "administration_requires_authentication": (("literal", "401"), ("observation", "anonymous_admin_status")),
    },
    "backup-restore/durable-inventory": {
        "the_restored_secret_version_is_active": (("literal", "1"), ("observation", "restored_secret_versions")),
        "the_restored_secret_version_owner_survives": (("observation", "expected_secret_owner"), ("observation", "restored_secret_owner")),
        "the_restored_secret_material_is_encrypted": (("literal", "1"), ("observation", "restored_secret_ciphertext_rows")),
        "the_restored_catalog_snapshot_survives": (("literal", "1"), ("observation", "restored_catalog_snapshot_rows")),
        "the_restored_catalog_active_pointer_survives": (("observation", "expected_catalog_active_content"), ("observation", "restored_catalog_active_content")),
        "the_restored_usage_rows_match_backup": (("observation", "live_usage_rows"), ("observation", "restored_usage_rows")),
        "the_restored_usage_outbox_matches_backup": (("observation", "live_usage_outbox_rows"), ("observation", "restored_usage_outbox_rows")),
        "the_logical_backup_usage_fixture_was_answered": (("literal", "200"), ("observation", "logical_backup_usage_status")),
        "the_logical_backup_usage_fixture_is_nonempty": (("literal", "true"), ("all_positive", "live_usage_rows", "live_usage_outbox_rows")),
        "the_logical_backup_usage_fixture_is_exactly_one_new_row": (("literal", "1"), ("observation", "logical_backup_new_usage_rows")),
        "the_logical_backup_usage_identity_is_canonical": (("literal", "true"), ("canonical_request_id", "logical_backup_usage_request_id")),
        "the_logical_backup_usage_identity_is_in_the_source": (("literal", "1"), ("observation", "logical_backup_source_usage_identity_rows")),
        "the_logical_backup_usage_outbox_identity_is_in_the_source": (("literal", "1"), ("observation", "logical_backup_source_outbox_identity_rows")),
        "the_logical_backup_usage_identity_survives": (("literal", "1"), ("observation", "logical_backup_restored_usage_identity_rows")),
        "the_logical_backup_usage_outbox_identity_survives": (("literal", "1"), ("observation", "logical_backup_restored_outbox_identity_rows")),
        "the_restored_price_book_survives": (("literal", "1"), ("observation", "restored_price_book_rows")),
        "the_restored_price_book_checksum_matches": (("observation", "live_price_book_checksum"), ("observation", "restored_price_book_checksum")),
        "the_restored_price_book_schema_is_current": (("literal", "axond.price-book.v2"), ("observation", "restored_price_book_schema")),
        "the_restored_price_book_catalogue_version_survives": (("literal", "1"), ("observation", "restored_price_book_catalog_version")),
        "the_restored_price_book_approval_survives": (("literal", "approved"), ("observation", "restored_price_book_approval_state")),
        "the_restored_price_book_approval_citation_survives": (("literal", "restore drill"), ("observation", "restored_price_book_approval_citation")),
        "the_restored_price_book_has_two_historical_rules": (("literal", "2"), ("observation", "restored_price_book_rule_count")),
        "the_restored_price_history_is_exact": (("observation", "expected_price_book_history"), ("observation", "restored_price_book_history")),
    },
    "point-in-time-recovery/recovery": {
        "the_recovered_cluster_promotes": (("literal", "f"), ("observation", "recovery_in_progress")),
        "the_recovered_schema_is_current": (("literal", "current"), ("observation", "recovered_schema_status")),
        "the_pitr_secret_metadata_survives_the_target": (("literal", "1"), ("observation", "pitr_secret_versions")),
        "the_pitr_secret_owner_survives_the_target": (("observation", "expected_secret_owner"), ("observation", "pitr_secret_owner")),
        "the_pitr_secret_lifecycle_survives_the_target": (("literal", "active"), ("observation", "pitr_secret_lifecycle")),
        "the_pitr_catalogue_snapshot_survives_the_target": (("observation", "expected_pitr_catalogue_content_id"), ("observation", "pitr_catalogue_content_id")),
        "the_pitr_catalogue_raw_digest_survives_the_target": (("observation", "expected_pitr_catalogue_raw_digest"), ("observation", "pitr_catalogue_raw_digest")),
        "the_pitr_catalogue_raw_bytes_survive_the_target": (("observation", "expected_pitr_catalogue_raw_bytes"), ("observation", "pitr_catalogue_raw_bytes")),
        "the_pitr_catalogue_payload_survives_the_target": (("literal", "true"), ("positive", "pitr_catalogue_payload_bytes")),
        "the_recovered_head_is_the_pre_target_revision": (("observation", "pre_target_head_revision"), ("observation", "recovered_head_revision")),
        "nothing_published_before_the_target_is_lost": (("observation", "revisions_before_target"), ("observation", "revisions_after_recovery")),
        "the_write_after_the_target_is_not_replayed": (("literal", "absent"), ("observation", "post_target_revision_presence")),
        "a_publication_against_the_recovered_head_is_accepted": (("literal", "accepted"), ("accepted_revision", "revision_after_recovery")),
        "the_axond_usage_schema_is_recovered": (("literal", "1"), ("observation", "recovered_axond_usage_table_count")),
        "the_axond_budget_schema_is_recovered": (("literal", "1"), ("observation", "recovered_axond_budget_table_count")),
        "the_axond_revocation_schema_is_recovered": (("literal", "1"), ("observation", "recovered_axond_revocation_table_count")),
    },
    "point-in-time-recovery/usage-boundary": {
        "the_pre_target_usage_request_is_answered": (("literal", "200"), ("observation", "pre_target_chat_status")),
        "the_post_target_usage_request_is_answered": (("literal", "200"), ("observation", "post_target_chat_status")),
        "the_pre_target_usage_request_creates_one_identity": (("literal", "1"), ("observation", "pre_target_new_usage_rows")),
        "the_post_target_usage_request_creates_one_identity": (("literal", "1"), ("observation", "post_target_new_usage_rows")),
        "the_usage_request_ids_are_canonical": (("literal", "true"), ("canonical_request_id_pair", "pre_target_usage_request_id", "post_target_usage_request_id")),
        "the_usage_request_ids_are_globally_unique": (("literal", "true"), ("distinct", "pre_target_usage_request_id", "post_target_usage_request_id")),
        "the_pre_target_usage_record_survives": (("literal", "1"), ("observation", "recovered_pre_target_usage")),
        "the_pre_target_usage_outbox_event_survives": (("literal", "1"), ("observation", "recovered_pre_target_outbox")),
        "the_post_target_usage_record_is_not_replayed": (("literal", "0"), ("observation", "recovered_post_target_usage")),
        "the_post_target_usage_outbox_event_is_not_replayed": (("literal", "0"), ("observation", "recovered_post_target_outbox")),
    },
}


# Every applicable scenario gate is reduced from retained observations by its
# designated owner. The artifact's recorded observed value and outcome are
# assertions about this relation, never independent evidence.
GATE_RECONSTRUCTIONS: dict[str, dict[str, tuple[str, ...]]] = {
    "control-plane-outage/journal-outage": {
        "max_data_loss_revisions": ("zero_if_equal_pairs", "revision", "active_revision"),
        "admin_writes": ("http_administration", "admin_write_status"),
    },
    "control-plane-outage/serving": {
        "max_serving_error_fraction": ("http_error_fraction", "inference_status"),
        "readiness": ("http_readiness", "ready_status"),
    },
    "control-plane-outage/administration": {
        "max_unauthenticated_admin_successes": ("http_unauthenticated_successes", "anonymous_state_status"),
    },
    "recovery-convergence/journal-recovery": {
        "max_convergence_lag_seconds": ("observation", "recovery_seconds"),
        "max_data_loss_revisions": ("boolean_label", "recovered_history_contains_required_revisions", "0", "1"),
        "admin_writes": ("boolean_label", "post_recovery_write_accepted", "accepted", "unavailable"),
    },
    "recovery-convergence/serving": {
        "max_serving_error_fraction": ("http_error_fraction", "chat_status"),
        "readiness": ("http_readiness", "ready_status"),
    },
    "recovery-convergence/administration": {
        "max_unauthenticated_admin_successes": ("http_unauthenticated_successes", "anonymous_admin_status"),
    },
    "secret-rotation/rotation": {
        "max_convergence_lag_seconds": ("observation", "rotation_seconds"),
        "max_data_loss_revisions": ("boolean_label", "rotation_history_contains_required_revisions", "0", "1"),
        "admin_writes": ("boolean_label", "publication_accepted", "accepted", "unavailable"),
    },
    "secret-rotation/serving": {
        "max_serving_error_fraction": ("http_error_fraction", "chat_status"),
        "readiness": ("http_readiness", "ready_status"),
        "max_unauthenticated_admin_successes": ("http_unauthenticated_successes", "anonymous_admin_status"),
    },
    "cold-boot-valid-cache/cold-boot": {
        "max_data_loss_revisions": ("zero_if_equal_pairs", "cached_revision", "active_revision"),
        "readiness": ("http_readiness", "ready_status"),
    },
    "cold-boot-valid-cache/serving": {
        "max_serving_error_fraction": ("http_error_fraction", "chat_status"),
        "admin_writes": ("http_administration", "admin_mutation_status"),
        "max_unauthenticated_admin_successes": ("http_unauthenticated_successes", "anonymous_admin_status"),
    },
    "cold-boot-no-cache/cold-boot": {
        "readiness": ("http_readiness", "ready_status"),
    },
    "cold-boot-no-cache/readiness": {
        "admin_writes": ("http_administration", "admin_mutation_status"),
        "max_unauthenticated_admin_successes": ("http_unauthenticated_successes", "anonymous_admin_status"),
    },
    "cold-boot-invalid-cache/cold-boot": {
        "readiness": ("http_readiness", "ready_status"),
    },
    "cold-boot-invalid-cache/readiness": {
        "admin_writes": ("http_administration", "admin_mutation_status"),
        "max_unauthenticated_admin_successes": ("http_unauthenticated_successes", "anonymous_admin_status"),
    },
    "backup-restore/restore": {
        "max_data_loss_revisions": (
            "zero_if_equal_pairs",
            "live_head_revision", "restored_head_revision",
            "live_revisions", "restored_revision_count",
            "live_resources", "restored_resource_count",
            "live_head_checksum", "restored_head_checksum",
        ),
        "admin_writes": ("accepted_revision", "revision_after_restore"),
    },
    "backup-restore/reconvergence": {
        "max_serving_error_fraction": ("http_error_count", "survivor_inference_status"),
        "max_convergence_lag_seconds": ("observation", "survivor_convergence_lag_seconds"),
        "readiness": ("http_readiness", "survivor_readiness_status"),
    },
    "backup-restore/administration": {
        "max_unauthenticated_admin_successes": ("observation", "unauthenticated_admin_successes"),
    },
    "point-in-time-recovery/recovery": {
        "max_data_loss_revisions": (
            "zero_if_equal_pairs_and_literal",
            "pre_target_head_revision", "recovered_head_revision",
            "revisions_before_target", "revisions_after_recovery",
            "post_target_revision_presence", "absent",
        ),
        "admin_writes": ("accepted_revision", "revision_after_recovery"),
    },
    "point-in-time-recovery/reconvergence": {
        "max_serving_error_fraction": ("http_error_count", "survivor_inference_status"),
        "max_convergence_lag_seconds": ("observation", "survivor_convergence_lag_seconds"),
        "readiness": ("http_readiness", "survivor_readiness_status"),
    },
    "point-in-time-recovery/administration": {
        "max_unauthenticated_admin_successes": ("observation", "unauthenticated_admin_successes"),
    },
}


def stage_key(scenario: dict[str, Any], stage: dict[str, Any]) -> str:
    """Return the stable manifest identity for one recovery stage."""
    return f"{scenario.get('id')}/{stage.get('id')}"


def gate_owner(scenario: dict[str, Any], gate: str) -> str | None:
    """Return the one stage committed to evaluating a scenario gate."""
    scenario_id = scenario.get("id")
    if not isinstance(scenario_id, str):
        return None
    return GATE_OWNERS.get(scenario_id, {}).get(gate)


def gate_not_applicable(scenario: dict[str, Any], gate: str) -> str | None:
    """Return the committed reason an otherwise-required gate does not apply."""
    scenario_id = scenario.get("id")
    if not isinstance(scenario_id, str):
        return None
    return GATE_NOT_APPLICABLE.get(scenario_id, {}).get(gate)


def validate_gate_ownership_model(scenarios: list[dict[str, Any]]) -> list[str]:
    """Prove ownership is total and grounded in executable manifest stages."""
    problems: list[str] = []
    required_gates = set(REQUIRED_GATE_NAMES)
    for scenario in scenarios:
        scenario_id = scenario.get("id")
        if not isinstance(scenario_id, str) or not scenario_id:
            problems.append("a recovery scenario has no stable id")
            continue
        owners = GATE_OWNERS.get(scenario_id)
        if owners is None:
            problems.append(f"{scenario_id}: no gate ownership model is committed")
            continue
        missing = required_gates - set(owners)
        unexpected = set(owners) - required_gates
        if missing:
            problems.append(f"{scenario_id}: gate owners are missing: {sorted(missing)}")
        if unexpected:
            problems.append(
                f"{scenario_id}: unknown gate owners are present: {sorted(unexpected)}"
            )

        raw_stages = scenario.get("stage")
        stages = raw_stages if isinstance(raw_stages, list) else []
        executable = {
            stage.get("id"): stage
            for stage in stages
            if isinstance(stage, dict)
            and stage.get("status") == "executable"
            and isinstance(stage.get("id"), str)
        }
        for gate in REQUIRED_GATE_NAMES:
            owner = owners.get(gate)
            if owner is None:
                reason = gate_not_applicable(scenario, gate)
                if not isinstance(reason, str) or not reason.strip():
                    problems.append(
                        f"{scenario_id}/{gate}: owner is missing without a committed non-applicability reason"
                    )
                continue
            if not isinstance(owner, str) or not owner:
                problems.append(f"{scenario_id}/{gate}: owner is malformed")
                continue
            owner_stage = executable.get(owner)
            if owner_stage is None:
                problems.append(
                    f"{scenario_id}/{gate}: owner {owner!r} is not an executable "
                    "manifest stage"
                )
                continue
            evidence = owner_stage.get("evidence")
            supporting = (
                GATE_EVIDENCE[gate].intersection(evidence)
                if isinstance(evidence, list)
                else frozenset()
            )
            if not supporting:
                problems.append(
                    f"{scenario_id}/{gate}: owner {owner!r} has no supporting manifest "
                    f"evidence class from {sorted(GATE_EVIDENCE[gate])}"
                )
            owner_key = f"{scenario_id}/{owner}"
            if gate not in GATE_RECONSTRUCTIONS.get(owner_key, {}):
                problems.append(
                    f"{scenario_id}/{gate}: owner {owner!r} has no shared gate "
                    "reconstruction"
                )
    return problems


def required_observations(scenario: dict[str, Any], stage: dict[str, Any]) -> frozenset[str]:
    """Return the committed raw observation contract for one executable stage."""
    return STAGE_REQUIRED_OBSERVATIONS.get(stage_key(scenario, stage), frozenset())


def required_checks(scenario: dict[str, Any], stage: dict[str, Any]) -> frozenset[str]:
    """Return checks whose exact presence is part of a stage's durable boundary."""
    return STAGE_REQUIRED_CHECKS.get(stage_key(scenario, stage), frozenset())


def _observation_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (str, int, float)) and not isinstance(value, bool):
        return str(value)
    raise ValueError("is not a scalar check-binding observation")


def _binding_observation(observations: dict[str, Any], key: str) -> Any:
    if key not in observations:
        raise ValueError(f"requires missing binding observation {key!r}")
    value = observations[key]
    if not _valid_observation(value):
        raise ValueError(f"requires invalid binding observation {key!r}")
    return value


def _resolve_check_operand(
    operand: tuple[str, ...], observations: dict[str, Any]
) -> str:
    operation, *arguments = operand
    if operation == "literal" and len(arguments) == 1:
        return arguments[0]
    if operation == "observation" and len(arguments) == 1:
        return _observation_text(_binding_observation(observations, arguments[0]))
    if operation == "all_positive" and arguments:
        values = [_decimal(_binding_observation(observations, key)) for key in arguments]
        return "true" if all(value is not None and value > 0 for value in values) else "false"
    if operation == "positive" and len(arguments) == 1:
        value = _decimal(_binding_observation(observations, arguments[0]))
        return "true" if value is not None and value > 0 else "false"
    if operation in {"canonical_request_id", "canonical_request_id_pair"}:
        expected_arguments = 1 if operation == "canonical_request_id" else 2
        if len(arguments) == expected_arguments:
            values = [
                _observation_text(_binding_observation(observations, key))
                for key in arguments
            ]
            canonical = all(
                re.fullmatch(r"req_[0-9a-f-]{36}", value) is not None
                for value in values
            )
            return "true" if canonical else "false"
    if operation == "distinct" and len(arguments) == 2:
        first = _observation_text(_binding_observation(observations, arguments[0]))
        second = _observation_text(_binding_observation(observations, arguments[1]))
        return "true" if first != second else "false"
    if operation == "accepted_revision" and len(arguments) == 1:
        revision = _observation_text(
            _binding_observation(observations, arguments[0])
        )
        return "refused" if revision == "refused" else "accepted"
    if operation == "boolean_label" and len(arguments) == 3:
        value = _binding_observation(observations, arguments[0])
        if not isinstance(value, bool):
            raise ValueError(
                f"requires boolean binding observation {arguments[0]!r}"
            )
        return arguments[1] if value else arguments[2]
    if operation == "positive_label" and len(arguments) == 3:
        value = _decimal(_binding_observation(observations, arguments[0]))
        if value is None:
            raise ValueError(
                f"requires numeric binding observation {arguments[0]!r}"
            )
        return arguments[1] if value > 0 else arguments[2]
    if operation == "null_label" and len(arguments) == 3:
        key = arguments[0]
        if key not in observations:
            raise ValueError(f"requires missing binding observation {key!r}")
        return arguments[1] if observations[key] is None else arguments[2]
    if operation == "zero_if_equal_pairs" and arguments and len(arguments) % 2 == 0:
        equal = all(
            _observation_text(_binding_observation(observations, arguments[index]))
            == _observation_text(
                _binding_observation(observations, arguments[index + 1])
            )
            for index in range(0, len(arguments), 2)
        )
        return "0" if equal else "1"
    if (
        operation == "zero_if_equal_pairs_and_literal"
        and len(arguments) >= 4
        and len(arguments) % 2 == 0
    ):
        pair_arguments = arguments[:-2]
        literal_key, literal_value = arguments[-2:]
        equal = all(
            _observation_text(_binding_observation(observations, pair_arguments[index]))
            == _observation_text(
                _binding_observation(observations, pair_arguments[index + 1])
            )
            for index in range(0, len(pair_arguments), 2)
        )
        literal_matches = (
            _observation_text(_binding_observation(observations, literal_key))
            == literal_value
        )
        return "0" if equal and literal_matches else "1"
    if operation in {
        "http_error_fraction",
        "http_error_count",
        "http_readiness",
        "http_administration",
        "http_unauthenticated_successes",
    } and len(arguments) == 1:
        status = _decimal(_binding_observation(observations, arguments[0]))
        if status is None or status != status.to_integral_value():
            raise ValueError(
                f"requires HTTP-status binding observation {arguments[0]!r}"
            )
        successful = Decimal(200) <= status < Decimal(300)
        if operation == "http_error_fraction":
            return "0.0" if successful else "1.0"
        if operation == "http_error_count":
            return "0" if successful else "1"
        if operation == "http_readiness":
            return "serves" if status == Decimal(200) else "refuses"
        if operation == "http_administration":
            return "accepted" if successful else "unavailable"
        return "1" if successful else "0"
    raise ValueError(f"uses unsupported check reconstruction {operand!r}")


def reconstruct_required_check(
    scenario: dict[str, Any],
    stage: dict[str, Any],
    check: str,
    observations: dict[str, Any],
) -> tuple[str, str]:
    """Rebuild a required check comparison without trusting its verdict tuple."""
    key = stage_key(scenario, stage)
    binding = CHECK_RECONSTRUCTIONS.get(key, {}).get(check)
    if binding is None:
        raise ValueError(f"required check {key}/{check} has no reconstruction")
    expected, observed = binding
    return (
        _resolve_check_operand(expected, observations),
        _resolve_check_operand(observed, observations),
    )


def reconstruct_required_gate(
    scenario: dict[str, Any],
    stage: dict[str, Any],
    gate: str,
    observations: dict[str, Any],
) -> str:
    """Rebuild an owner gate's observation from retained raw observations."""
    key = stage_key(scenario, stage)
    binding = GATE_RECONSTRUCTIONS.get(key, {}).get(gate)
    if binding is None:
        raise ValueError(f"required gate {key}/{gate} has no reconstruction")
    return _resolve_check_operand(binding, observations)


def deferred_gate_detail(
    gate: str, evidence: list[str] | tuple[str, ...], reason: str
) -> str:
    """Attach the gate's evidence-class boundary to a deferred explanation."""
    required = ", ".join(sorted(GATE_EVIDENCE[gate]))
    retained = ", ".join(evidence) if evidence else "none"
    return (
        f"requires evidence class [{required}]; this stage retains [{retained}]. "
        f"{reason.strip()}"
    )


def _has_evidence_class_justification(gate: str, detail: str) -> bool:
    normalized = detail.casefold()
    return "evidence class" in normalized and all(
        evidence_class.casefold() in normalized for evidence_class in GATE_EVIDENCE[gate]
    )


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def derive_verdict_outcome(kind: str, name: str, bound: str, observed: str) -> str | None:
    """Derive `met`/`failed` for comparison shapes the contract understands.

    Checks are equality comparisons. Numeric maximum gates are `observed <=
    bound`; readiness and administrative-write gates are exact enum equality.
    Descriptive legacy readiness observations are intentionally unsupported and
    return ``None`` rather than being guessed from prose.
    """
    if kind == "check":
        return "met" if bound == observed else "failed"
    if name.startswith("max_"):
        bound_number = _decimal(bound)
        observed_number = _decimal(observed)
        if bound_number is None or observed_number is None:
            return None
        return "met" if observed_number <= bound_number else "failed"
    if name in ("readiness", "admin_writes"):
        allowed = (
            frozenset({"serves", "refuses"})
            if name == "readiness"
            else frozenset({"accepted", "unavailable"})
        )
        if bound not in allowed or observed not in allowed:
            return None
        return "met" if bound == observed else "failed"
    return None


def _bound_matches(gate: str, recorded: str, expected: Any) -> bool:
    if gate.startswith("max_"):
        return _decimal(recorded) == _decimal(expected)
    return recorded == str(expected)


def _valid_observation(value: Any, *, allow_null: bool = False) -> bool:
    if value is None:
        return allow_null
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, bool):
        return True
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    return False


def _absolute_executable_path(value: str) -> bool:
    return (
        value.startswith("/")
        or value.startswith("\\\\")
        or re.match(r"^[A-Za-z]:[\\/]", value) is not None
    )


def validate_verdicts(
    artifact: Any,
    scenario: dict[str, Any],
    stage: dict[str, Any],
    *,
    reject_failed: bool = True,
) -> list[str]:
    """Validate complete gate coverage and check semantics for one raw artifact."""
    if not isinstance(artifact, dict):
        return ["artifact root is not a JSON object"]
    problems: list[str] = []
    gates = artifact.get("gates")
    checks = artifact.get("checks")
    if not isinstance(gates, list) or not isinstance(checks, list):
        return ["gates and checks must both be lists"]
    entries = [("gate", entry) for entry in gates] + [
        ("check", entry) for entry in checks
    ]
    names: dict[str, list[str]] = {"gate": [], "check": []}
    for index, (kind, entry) in enumerate(entries):
        label = f"{kind} verdict {index}"
        if not isinstance(entry, dict):
            problems.append(f"{label} is not an object")
            continue
        if set(entry) != VERDICT_FIELDS:
            problems.append(
                f"{label} fields are {sorted(entry)}, expected {sorted(VERDICT_FIELDS)}"
            )
            continue
        if any(
            not isinstance(entry[field], str) or not entry[field].strip()
            for field in VERDICT_FIELDS
        ):
            problems.append(f"{label} fields must be nonempty strings")
            continue
        name = entry["gate"]
        outcome = entry["outcome"]
        names[kind].append(name)
        if outcome not in VERDICT_OUTCOMES:
            problems.append(f"{label} has unsupported outcome {outcome!r}")
            continue
        if reject_failed and outcome == "failed":
            problems.append(
                f"{kind} {name!r} failed: expected {entry['bound']!r}, "
                f"observed {entry['observed']!r} ({entry['detail']})"
            )
        if kind == "check" and outcome == "not_evaluated":
            problems.append(f"check {name!r} may not be not_evaluated")
            continue
        if outcome == "not_evaluated":
            if entry["observed"] != "not measured":
                problems.append(
                    f"gate {name!r} is not_evaluated but observed is not 'not measured'"
                )
            continue
        derived = derive_verdict_outcome(kind, name, entry["bound"], entry["observed"])
        if derived is None:
            if outcome == "met":
                problems.append(
                    f"{kind} {name!r} claims met from an unsupported comparison "
                    f"{entry['observed']!r} against {entry['bound']!r}"
                )
        elif derived != outcome:
            problems.append(
                f"{kind} {name!r} outcome is {outcome!r}, independently derived {derived!r}"
            )

    gate_names = names["gate"]
    if len(gate_names) != len(set(gate_names)):
        problems.append("a scenario gate is recorded more than once")
    missing = set(REQUIRED_GATE_NAMES) - set(gate_names)
    unexpected = set(gate_names) - set(REQUIRED_GATE_NAMES)
    if missing:
        problems.append(f"required scenario gates are missing: {sorted(missing)}")
    if unexpected:
        problems.append(f"unknown scenario gates are present: {sorted(unexpected)}")

    scenario_gate = scenario.get("gate")
    evidence = stage.get("evidence")
    if not isinstance(scenario_gate, dict) or not isinstance(evidence, list):
        problems.append("manifest gate or stage evidence contract is malformed")
        return problems
    for entry in gates:
        if not isinstance(entry, dict) or set(entry) != VERDICT_FIELDS:
            continue
        name = entry["gate"]
        if name not in REQUIRED_GATE_NAMES:
            continue
        owner = gate_owner(scenario, name)
        stage_id = stage.get("id")
        if owner is None and gate_not_applicable(scenario, name) is None:
            problems.append(
                f"gate {name!r} has neither a committed owner nor a non-applicability reason"
            )
        elif owner is None and entry["outcome"] != "not_evaluated":
            problems.append(
                f"gate {name!r} is not applicable to this scenario and must be deferred"
            )
        elif stage_id == owner and entry["outcome"] == "not_evaluated":
            problems.append(
                f"gate {name!r} is owned by this stage and may not be not_evaluated"
            )
        elif stage_id != owner and entry["outcome"] != "not_evaluated":
            problems.append(
                f"gate {name!r} is owned by stage {owner!r}; non-owner stage "
                f"{stage_id!r} must defer"
            )
        if not _bound_matches(name, entry["bound"], scenario_gate.get(name)):
            problems.append(
                f"gate {name!r} bound {entry['bound']!r} does not match the manifest's "
                f"{scenario_gate.get(name)!r}"
            )
        supporting = GATE_EVIDENCE[name].intersection(evidence)
        if entry["outcome"] != "not_evaluated" and not supporting:
            problems.append(
                f"gate {name!r} is evaluated without any of its evidence classes "
                f"{sorted(GATE_EVIDENCE[name])}"
            )
        if entry["outcome"] == "not_evaluated" and not _has_evidence_class_justification(
            name, entry["detail"]
        ):
            problems.append(
                f"gate {name!r} has no complete evidence-class deferral justification"
            )

    check_names = names["check"]
    if len(check_names) != len(set(check_names)):
        problems.append("a stage check is recorded more than once")
    committed_checks = STAGE_REQUIRED_CHECKS.get(stage_key(scenario, stage))
    if committed_checks is not None:
        unexpected_checks = set(check_names) - committed_checks
        if unexpected_checks:
            problems.append(
                f"unexpected stage checks are present: {sorted(unexpected_checks)}"
            )
    passing = not any(
        isinstance(entry, dict) and entry.get("outcome") == "failed"
        for _, entry in entries
    )
    if passing:
        missing_checks = required_checks(scenario, stage) - set(check_names)
        if missing_checks:
            problems.append(f"required stage checks are missing: {sorted(missing_checks)}")
    return problems


def validate_recovery_artifact(
    artifact: Any,
    scenario: dict[str, Any],
    stage: dict[str, Any],
    *,
    require_executable_identity: bool = True,
    reject_failed: bool = True,
) -> list[str]:
    """Validate one raw recovery artifact independently of its retained hash."""
    if not isinstance(artifact, dict):
        return ["artifact root is not a JSON object"]
    problems: list[str] = []
    if artifact.get("schema_version") != RECOVERY_RESULT_SCHEMA_VERSION:
        problems.append(
            f"schema_version is not supported version {RECOVERY_RESULT_SCHEMA_VERSION}"
        )
    expected_identity = {
        "scenario": scenario.get("id"),
        "stage": stage.get("id"),
        "runner": stage.get("runner"),
        "capability": scenario.get("capability"),
        "evidence": stage.get("evidence"),
    }
    for field, expected in expected_identity.items():
        if artifact.get(field) != expected:
            problems.append(
                f"{field} {artifact.get(field)!r} does not match the manifest's {expected!r}"
            )
    run = artifact.get("run")
    if not isinstance(run, dict):
        problems.append("run provenance is missing or is not an object")
        run = {}
    if run.get("control_plane") != "postgres":
        problems.append("run.control_plane must be 'postgres'")
    for field in ("schema", "schema_identity", "axond_version"):
        if not isinstance(run.get(field), str) or not run[field].strip():
            problems.append(f"run.{field} is missing or empty")
    for field in ("started_at_unix_ms", "elapsed_ms"):
        value = run.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            problems.append(f"run.{field} must be a positive integer")
    if require_executable_identity:
        digest = run.get("axond_executable_sha256")
        if not isinstance(digest, str) or not LOWER_SHA256.fullmatch(digest):
            problems.append(
                "run.axond_executable_sha256 must be 64 lowercase hexadecimal characters"
            )
        if run.get("cargo_profile") != RELEASE_CARGO_PROFILE:
            problems.append("run.cargo_profile must be 'release'")
        if stage.get("driver") == "stateful-integration":
            executed_digest = run.get("axond_executed_sha256")
            if (
                not isinstance(executed_digest, str)
                or not LOWER_SHA256.fullmatch(executed_digest)
            ):
                problems.append(
                    "run.axond_executed_sha256 is required for process-backed evidence"
                )
            elif executed_digest != digest:
                problems.append(
                    "run.axond_executed_sha256 does not match axond_executable_sha256"
                )
            executable_path = run.get("axond_executable_path")
            if not isinstance(executable_path, str) or not executable_path.strip():
                problems.append(
                    "run.axond_executable_path is required for process-backed evidence"
                )
            elif not _absolute_executable_path(executable_path):
                problems.append(
                    "run.axond_executable_path must be an absolute execution identity"
                )
            elif "release" not in re.split(r"[\\/]", executable_path):
                problems.append(
                    "run.axond_executable_path does not identify a release-profile path"
                )
            if run.get("axond_execution_bound") is not True:
                problems.append(
                    "run.axond_execution_bound must be true for process-backed evidence"
                )
        elif any(
            run.get(field) is not None
            for field in (
                "axond_executed_sha256",
                "axond_executable_path",
                "axond_execution_bound",
            )
        ):
            problems.append(
                "non-process recovery evidence must not claim process execution provenance"
            )

    timeline = artifact.get("timeline")
    if not isinstance(timeline, list) or not timeline:
        problems.append("timeline is missing or empty")
    else:
        previous = -1
        for index, event in enumerate(timeline):
            if not isinstance(event, dict):
                problems.append(f"timeline event {index} is not an object")
                continue
            at = event.get("at_ms")
            if (
                not isinstance(at, int)
                or isinstance(at, bool)
                or at < previous
                or not isinstance(event.get("event"), str)
                or not event["event"].strip()
                or not isinstance(event.get("detail"), str)
                or not event["detail"].strip()
            ):
                problems.append(f"timeline event {index} is malformed or non-monotonic")
            elif isinstance(run.get("elapsed_ms"), int) and at > run["elapsed_ms"]:
                problems.append(f"timeline event {index} occurs after run.elapsed_ms")
            previous = at if isinstance(at, int) and not isinstance(at, bool) else previous

    observations = artifact.get("observations")
    if not isinstance(observations, dict):
        problems.append("observations is missing or is not an object")
        observations = {}
    contract_key = stage_key(scenario, stage)
    nullable_observation = (
        "active_revision"
        if contract_key == "cold-boot-no-cache/cold-boot"
        else None
    )
    invalid_observations = sorted(
        key
        for key, value in observations.items()
        if not isinstance(key, str)
        or not key
        or not _valid_observation(
            value, allow_null=key == nullable_observation
        )
    )
    if invalid_observations:
        problems.append(f"observations are empty or non-scalar: {invalid_observations}")
    if nullable_observation is not None and observations.get(nullable_observation) is not None:
        problems.append(
            "cold-boot-no-cache/cold-boot observation active_revision must be null"
        )

    verdict_problems = validate_verdicts(
        artifact, scenario, stage, reject_failed=reject_failed
    )
    problems.extend(verdict_problems)
    raw_gates = artifact.get("gates")
    raw_checks = artifact.get("checks")
    stage_required_checks = required_checks(scenario, stage)
    stage_reconstructions = CHECK_RECONSTRUCTIONS.get(
        stage_key(scenario, stage), {}
    )
    if set(stage_reconstructions) != set(stage_required_checks):
        missing_reconstructions = stage_required_checks - set(stage_reconstructions)
        unexpected_reconstructions = set(stage_reconstructions) - stage_required_checks
        if missing_reconstructions:
            problems.append(
                "required checks have no reconstruction: "
                f"{sorted(missing_reconstructions)}"
            )
        if unexpected_reconstructions:
            problems.append(
                "check reconstructions are not required by this stage: "
                f"{sorted(unexpected_reconstructions)}"
            )
    if isinstance(raw_checks, list):
        for check in raw_checks:
            if not isinstance(check, dict) or check.get("gate") not in stage_required_checks:
                continue
            name = check["gate"]
            try:
                expected_bound, expected_observed = reconstruct_required_check(
                    scenario, stage, name, observations
                )
            except ValueError as error:
                problems.append(str(error))
                continue
            if check.get("bound") != expected_bound:
                problems.append(
                    f"required check {name!r} bound {check.get('bound')!r} does not "
                    f"match reconstructed {expected_bound!r}"
                )
            if check.get("observed") != expected_observed:
                problems.append(
                    f"required check {name!r} observed {check.get('observed')!r} does "
                    f"not match reconstructed {expected_observed!r}"
                )
    owned_gates = {
        gate
        for gate in REQUIRED_GATE_NAMES
        if gate_owner(scenario, gate) == stage.get("id")
    }
    stage_gate_reconstructions = GATE_RECONSTRUCTIONS.get(contract_key, {})
    if set(stage_gate_reconstructions) != owned_gates:
        missing_reconstructions = owned_gates - set(stage_gate_reconstructions)
        unexpected_reconstructions = set(stage_gate_reconstructions) - owned_gates
        if missing_reconstructions:
            problems.append(
                "owned gates have no reconstruction: "
                f"{sorted(missing_reconstructions)}"
            )
        if unexpected_reconstructions:
            problems.append(
                "gate reconstructions are not owned by this stage: "
                f"{sorted(unexpected_reconstructions)}"
            )
    if isinstance(raw_gates, list):
        for gate_entry in raw_gates:
            if (
                not isinstance(gate_entry, dict)
                or gate_entry.get("gate") not in owned_gates
            ):
                continue
            name = gate_entry["gate"]
            try:
                expected_observed = reconstruct_required_gate(
                    scenario, stage, name, observations
                )
            except ValueError as error:
                problems.append(str(error))
                continue
            if gate_entry.get("observed") != expected_observed:
                problems.append(
                    f"owned gate {name!r} observed {gate_entry.get('observed')!r} "
                    f"does not match reconstructed {expected_observed!r}"
                )
            expected_outcome = derive_verdict_outcome(
                "gate", name, str(scenario.get("gate", {}).get(name)), expected_observed
            )
            if expected_outcome is None:
                problems.append(
                    f"owned gate {name!r} reconstruction cannot derive an outcome"
                )
            elif gate_entry.get("outcome") != expected_outcome:
                problems.append(
                    f"owned gate {name!r} outcome {gate_entry.get('outcome')!r} "
                    f"does not match reconstructed {expected_outcome!r}"
                )
    verdicts = (
        [*raw_gates, *raw_checks]
        if isinstance(raw_gates, list) and isinstance(raw_checks, list)
        else []
    )
    passing = bool(verdicts) and not any(
        isinstance(verdict, dict) and verdict.get("outcome") == "failed"
        for verdict in verdicts
    )
    if passing:
        required = required_observations(scenario, stage)
        if not required:
            problems.append(
                f"{stage_key(scenario, stage)} has no committed observation contract"
            )
        missing = required - set(observations)
        if missing:
            problems.append(f"required stage observations are missing: {sorted(missing)}")
    return problems


__all__ = [
    "GATE_EVIDENCE",
    "GATE_OWNERS",
    "GATE_NOT_APPLICABLE",
    "GATE_RECONSTRUCTIONS",
    "LOWER_SHA256",
    "RECOVERY_RESULT_SCHEMA_VERSION",
    "RELEASE_CARGO_PROFILE",
    "REQUIRED_GATE_NAMES",
    "STAGE_REQUIRED_CHECKS",
    "STAGE_REQUIRED_OBSERVATIONS",
    "CHECK_RECONSTRUCTIONS",
    "VERDICT_FIELDS",
    "VERDICT_OUTCOMES",
    "deferred_gate_detail",
    "derive_verdict_outcome",
    "gate_owner",
    "gate_not_applicable",
    "required_checks",
    "required_observations",
    "reconstruct_required_check",
    "reconstruct_required_gate",
    "stage_key",
    "validate_gate_ownership_model",
    "validate_recovery_artifact",
    "validate_verdicts",
]
