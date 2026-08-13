//! The `/admin/v1` error envelope: one closed vocabulary of machine-readable
//! codes, and nothing on the wire that a caller did not already know.
//!
//! Separate from [`GatewayError`](crate::error::GatewayError) on purpose. That
//! enum is the *inference* contract — its shape is what provider SDKs parse, and
//! its codes are part of the compatibility promise for `/v1` callers. An
//! administrative refusal answers different questions ("is my expected revision
//! current?", "does this deployment own durable state at all?") to a different
//! audience, and folding the two together would mean every new administrative
//! code widened the surface an inference SDK sees.
//!
//! Two properties are structural rather than conventional:
//!
//! **Nothing reaches the wire that was not already the caller's.** The
//! serialized body is built from [`AdminError::code`], a [`Display`] message
//! whose interpolated values are only revisions, resource references, and the
//! caller's own idempotency key, and an optional stable rule name. Backend text
//! — a DSN in a connection error, a driver's message, an identity provider's
//! response — is carried in [`AdminError::operator_detail`], which is never
//! serialized and exists to be logged. So redaction is a property of the type
//! rather than a filter someone has to remember to apply.
//!
//! **Every distinguishable outcome has its own code.** [`AdminError::CODES`] is
//! the whole vocabulary, asserted against the enum by a test, so a client can
//! branch on `stateful_mode_required` versus `revision_conflict` versus
//! `idempotency_key_reused` without parsing prose.
//!
//! [`Display`]: std::fmt::Display

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::auth::AdminAuthError;
use crate::backends::control_plane::ControlPlaneError;
use crate::backends::secrets::SecretError;
use crate::desired_state::secrets::SecretRef;
use crate::desired_state::{
    CanonicalError, ExpectedRevision, IdempotencyKey, InvalidIdempotencyKey, ResourceRef,
    RevisionId, ValidationError,
};

/// Why an administrative request was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdminError {
    /// The caller could not be established as an administrative identity. The
    /// cause is a [`AdminAuthError`], deliberately not rendered into the
    /// message: which credential was wrong is not something an unauthenticated
    /// caller is told.
    #[error("administrative authentication failed")]
    Unauthenticated(#[source] AdminAuthError),
    /// The caller is a known administrator, but not for this action or scope.
    #[error("administrative authorization failed")]
    Forbidden(#[source] AdminAuthError),
    /// Human administration is OIDC, so an identity provider outage is an
    /// availability failure of its own — and the reason a stateful deployment is
    /// required to configure a breakglass credential.
    #[error("the identity provider could not be consulted")]
    IdentityProviderUnavailable,
    /// This deployment does not own durable state, so there is nothing for
    /// `/admin/v1` to administer. Returned without consulting any control-plane
    /// backend: a stateless deployment has none to consult.
    #[error("this deployment is stateless; /admin/v1 administers durable state in stateful mode")]
    StatefulModeRequired,
    #[error("an administrative mutation requires an `Idempotency-Key` header")]
    IdempotencyKeyRequired,
    #[error("the `Idempotency-Key` header is not a usable key: {0}")]
    IdempotencyKeyInvalid(#[source] InvalidIdempotencyKey),
    /// The key was already used to publish *different* desired state. Replaying
    /// the earlier revision would report a change that never happened.
    #[error("idempotency key `{key}` already published revision {published} with other state")]
    IdempotencyKeyReused {
        key: IdempotencyKey,
        published: RevisionId,
    },
    #[error("an administrative mutation requires an `X-Axond-Expected-Revision` header")]
    ExpectedRevisionRequired,
    #[error("the `X-Axond-Expected-Revision` header is neither `empty` nor a revision id")]
    ExpectedRevisionInvalid,
    /// Another administrator published first. The caller re-reads and rebuilds;
    /// it does not replay the same candidate.
    #[error("expected {expected} to be current, but the newest is {actual:?}")]
    RevisionConflict {
        expected: ExpectedRevision,
        actual: Option<RevisionId>,
    },
    /// The complete candidate is not valid desired state. `rule` is the stable
    /// name of the invariant that refused it and `reference` the resource it is
    /// about; the domain's own prose stays in `detail`, which is logged rather
    /// than returned, because a validation message interpolates values from the
    /// state being validated.
    #[error("the candidate revision is not valid desired state: {rule}")]
    ValidationFailed {
        rule: &'static str,
        reference: Option<ResourceRef>,
        detail: String,
    },
    #[error("{reference} is already published with different content; publish a new version")]
    ImmutableResourceVersion { reference: ResourceRef },
    #[error("revision {0} is not retained")]
    RevisionNotFound(RevisionId),
    /// Stored state does not add up. An operator alert, never masked as an
    /// outage, and never retried into one.
    #[error("stored control-plane state is unreadable")]
    RevisionUnreadable {
        revision: Option<RevisionId>,
        detail: String,
    },
    /// Intact storage this build declines to interpret. Cleared by a deployment,
    /// not by a retry, and not an integrity alert.
    #[error("stored revision {revision} is not compatible with this build")]
    RevisionIncompatible {
        revision: RevisionId,
        detail: String,
    },
    #[error("stored revision {revision} exceeds what this build reads")]
    RevisionTooLarge {
        revision: RevisionId,
        detail: String,
    },
    /// The control plane is unreachable. Administration is degraded; inference
    /// is not, because no request path consults it.
    #[error("the control plane is unavailable")]
    ControlPlaneUnavailable { detail: String },
    /// The control plane refused this replica's own credential or a policy the
    /// caller cannot influence.
    #[error("the control plane refused the operation")]
    ControlPlaneDenied { detail: String },
    /// Two resources claim one name. The caller's fix is to rename, or to delete
    /// whatever still holds the name — so the name is in the response, and the
    /// status is a conflict rather than the `500` an opaque backend refusal
    /// would have produced.
    #[error("the {noun} name `{name}` is already taken")]
    NameTaken {
        noun: &'static str,
        name: String,
        detail: String,
    },
    #[error("the audit summary is empty, too long, or not printable")]
    AuditSummaryInvalid,
    #[error("the `X-Axond-Dry-Run` header must be `true` or `false`")]
    DryRunInvalid,
    #[error("a history request may ask for at most {max} revisions")]
    HistoryLimitInvalid { max: u32 },
    /// The request body is not the document the route reads. Separate from
    /// [`AdminError::ValidationFailed`], which is about a *candidate revision*
    /// the caller's edit produced: this one never got as far as an edit, so
    /// nothing was read from the control plane to refuse it.
    #[error("the request body is not a valid `{schema}` document: {detail}")]
    RequestInvalid {
        schema: &'static str,
        detail: String,
    },
    /// The request body is larger than this surface reads. Declared in the
    /// envelope rather than left to axum's bare `413`: a handler parses a
    /// document whole, so the bound is what stops an authenticated caller from
    /// making the process buffer an arbitrary body.
    #[error("the request body exceeds the {limit}-byte administrative limit")]
    RequestTooLarge { limit: usize },
    /// No such administrative route. Unlike `/v1`, where a `404` would be
    /// indistinguishable from a misconfigured `base_url`, an unknown
    /// `/admin/v1` path is a client error and says so in its own code.
    #[error("no such /admin/v1 route")]
    RouteNotFound,
    #[error("that method is not allowed on this /admin/v1 route")]
    MethodNotAllowed,
    /// The secret store is unreachable. Administration of material is degraded;
    /// inference is not, and neither is any revision already compiled — a
    /// snapshot holds the material it was published against.
    #[error("the secret store is unavailable")]
    SecretStoreUnavailable { detail: String },
    /// No such secret version *for this owner*. Material another owner holds is
    /// reported identically, which is the whole point: an administrator must not
    /// be able to probe this route to learn that another tenant's reference
    /// exists.
    #[error("{reference} is not stored")]
    SecretNotFound { reference: SecretRef },
    /// The version exists and its lifecycle state does not permit what was
    /// asked — rotating from a tombstoned version, resolving a revoked one, or a
    /// move the lifecycle matrix does not define. A move to the state a version
    /// is already in is not this: it succeeds, unchanged.
    #[error("{reference} cannot do that in its current state: {detail}")]
    SecretLifecycleRefused {
        reference: SecretRef,
        detail: String,
    },
    /// Destroying material the current desired state still pins. The caller's
    /// fix is to publish a credential that no longer references this version and
    /// then tombstone it, which is why the refusal is a conflict rather than a
    /// forbidden lifecycle move.
    #[error(
        "{reference} is still referenced by the current revision; publish a credential that \
             no longer pins it before destroying its material"
    )]
    SecretInUse { reference: SecretRef },
    /// The material presented is not storable — empty, or otherwise refused by
    /// the store before anything was sealed. The detail is logged rather than
    /// returned, for the reason every detail here is.
    #[error("the presented secret material was refused")]
    SecretMaterialRefused { detail: String },
    /// Stored material this replica cannot unwrap, or a store that refused the
    /// operation outright: a rotated or wrong deployment KEK, a damaged record,
    /// a schema this build does not read. An operator acts; a retry does not
    /// help.
    #[error("the secret store could not complete the operation")]
    SecretStoreUnusable { detail: String },
}

impl AdminError {
    /// Every code this surface can return, in the order the variants are
    /// declared. A test holds the two in step, so the vocabulary is reviewable
    /// as a list rather than by reading a `match`.
    pub const CODES: &'static [&'static str] = &[
        "admin_unauthenticated",
        "admin_forbidden",
        "identity_provider_unavailable",
        "stateful_mode_required",
        "idempotency_key_required",
        "idempotency_key_invalid",
        "idempotency_key_reused",
        "expected_revision_required",
        "expected_revision_invalid",
        "revision_conflict",
        "validation_failed",
        "immutable_resource_version",
        "revision_not_found",
        "revision_unreadable",
        "revision_incompatible",
        "revision_too_large",
        "control_plane_unavailable",
        "control_plane_denied",
        "name_taken",
        "audit_summary_invalid",
        "dry_run_invalid",
        "history_limit_invalid",
        "admin_request_invalid",
        "admin_request_too_large",
        "admin_route_not_found",
        "admin_method_not_allowed",
        "secret_store_unavailable",
        "secret_not_found",
        "secret_lifecycle_refused",
        "secret_in_use",
        "secret_material_refused",
        "secret_store_unusable",
    ];

    pub const fn code(&self) -> &'static str {
        match self {
            Self::Unauthenticated(_) => "admin_unauthenticated",
            Self::Forbidden(_) => "admin_forbidden",
            Self::IdentityProviderUnavailable => "identity_provider_unavailable",
            Self::StatefulModeRequired => "stateful_mode_required",
            Self::IdempotencyKeyRequired => "idempotency_key_required",
            Self::IdempotencyKeyInvalid(_) => "idempotency_key_invalid",
            Self::IdempotencyKeyReused { .. } => "idempotency_key_reused",
            Self::ExpectedRevisionRequired => "expected_revision_required",
            Self::ExpectedRevisionInvalid => "expected_revision_invalid",
            Self::RevisionConflict { .. } => "revision_conflict",
            Self::ValidationFailed { .. } => "validation_failed",
            Self::ImmutableResourceVersion { .. } => "immutable_resource_version",
            Self::RevisionNotFound(_) => "revision_not_found",
            Self::RevisionUnreadable { .. } => "revision_unreadable",
            Self::RevisionIncompatible { .. } => "revision_incompatible",
            Self::RevisionTooLarge { .. } => "revision_too_large",
            Self::ControlPlaneUnavailable { .. } => "control_plane_unavailable",
            Self::ControlPlaneDenied { .. } => "control_plane_denied",
            Self::NameTaken { .. } => "name_taken",
            Self::AuditSummaryInvalid => "audit_summary_invalid",
            Self::DryRunInvalid => "dry_run_invalid",
            Self::HistoryLimitInvalid { .. } => "history_limit_invalid",
            Self::RequestInvalid { .. } => "admin_request_invalid",
            Self::RequestTooLarge { .. } => "admin_request_too_large",
            Self::RouteNotFound => "admin_route_not_found",
            Self::MethodNotAllowed => "admin_method_not_allowed",
            Self::SecretStoreUnavailable { .. } => "secret_store_unavailable",
            Self::SecretNotFound { .. } => "secret_not_found",
            Self::SecretLifecycleRefused { .. } => "secret_lifecycle_refused",
            Self::SecretInUse { .. } => "secret_in_use",
            Self::SecretMaterialRefused { .. } => "secret_material_refused",
            Self::SecretStoreUnusable { .. } => "secret_store_unusable",
        }
    }

    pub const fn status(&self) -> StatusCode {
        match self {
            Self::Unauthenticated(_) => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            // A precondition the caller *omitted* is not a malformed one: `428`
            // says "state what you expected", which is the fix, where `400`
            // would read as "your header was wrong".
            Self::ExpectedRevisionRequired => StatusCode::PRECONDITION_REQUIRED,
            Self::IdempotencyKeyRequired
            | Self::IdempotencyKeyInvalid(_)
            | Self::ExpectedRevisionInvalid
            | Self::ValidationFailed { .. }
            | Self::AuditSummaryInvalid
            | Self::DryRunInvalid
            | Self::HistoryLimitInvalid { .. }
            | Self::RequestInvalid { .. }
            | Self::SecretMaterialRefused { .. } => StatusCode::BAD_REQUEST,
            Self::RevisionConflict { .. }
            | Self::IdempotencyKeyReused { .. }
            | Self::NameTaken { .. }
            | Self::ImmutableResourceVersion { .. }
            | Self::SecretLifecycleRefused { .. }
            | Self::SecretInUse { .. } => StatusCode::CONFLICT,
            Self::RequestTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RevisionNotFound(_) | Self::RouteNotFound | Self::SecretNotFound { .. } => {
                StatusCode::NOT_FOUND
            }
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            // Stateless mode is not a failure and not a misconfiguration: the
            // surface is unimplemented *for this deployment*, which is what
            // `501` means.
            Self::StatefulModeRequired => StatusCode::NOT_IMPLEMENTED,
            Self::ControlPlaneUnavailable { .. }
            | Self::IdentityProviderUnavailable
            | Self::SecretStoreUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            // Unreadable, incompatible, oversized, and refused storage are all
            // "this replica cannot serve the request, and retrying will not
            // change that": an operator acts, the caller does not.
            Self::RevisionUnreadable { .. }
            | Self::RevisionIncompatible { .. }
            | Self::RevisionTooLarge { .. }
            | Self::ControlPlaneDenied { .. }
            | Self::SecretStoreUnusable { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Whether repeating the identical request could succeed without the caller
    /// changing anything.
    ///
    /// Only the two outages qualify. A conflict is explicitly *not* retryable:
    /// the caller must re-read the head and rebuild, and a client that retried
    /// the same candidate would be racing rather than converging.
    pub const fn retryable(&self) -> bool {
        matches!(
            self,
            Self::ControlPlaneUnavailable { .. }
                | Self::IdentityProviderUnavailable
                | Self::SecretStoreUnavailable { .. }
        )
    }

    /// The operator-facing cause, for a log line. Never serialized: this is
    /// where a backend's own text — which may name a host, a DSN, or a driver
    /// internal — is kept out of a response.
    pub fn operator_detail(&self) -> Option<&str> {
        match self {
            Self::ValidationFailed { detail, .. }
            | Self::RevisionUnreadable { detail, .. }
            | Self::RevisionIncompatible { detail, .. }
            | Self::RevisionTooLarge { detail, .. }
            | Self::ControlPlaneUnavailable { detail }
            | Self::ControlPlaneDenied { detail }
            | Self::NameTaken { detail, .. }
            | Self::RequestInvalid { detail, .. }
            | Self::SecretStoreUnavailable { detail }
            | Self::SecretLifecycleRefused { detail, .. }
            | Self::SecretMaterialRefused { detail }
            | Self::SecretStoreUnusable { detail } => Some(detail),
            _ => None,
        }
    }

    /// The secret version this refusal is about, if it is about one.
    ///
    /// A reference, never material: that is the whole of what a secret refusal
    /// is allowed to carry, and [`SecretError`] has nothing else to give it.
    pub const fn secret(&self) -> Option<SecretRef> {
        match self {
            Self::SecretNotFound { reference }
            | Self::SecretLifecycleRefused { reference, .. }
            | Self::SecretInUse { reference } => Some(*reference),
            _ => None,
        }
    }

    /// Translate a secret-store failure into the administrative vocabulary.
    ///
    /// Exhaustive, like [`Self::from_control_plane`], and lossy in exactly one
    /// direction: [`SecretError::Ownership`] becomes the same
    /// [`Self::SecretNotFound`] an absent reference does, so this surface cannot
    /// be used to discover that another owner's version exists. The distinction
    /// survives in the log line the caller never sees.
    pub fn from_secret(error: SecretError) -> Self {
        match error {
            SecretError::Unavailable { backend, message } => Self::SecretStoreUnavailable {
                detail: format!("{backend}: {message}"),
            },
            SecretError::NotFound(reference) | SecretError::Ownership { reference, .. } => {
                Self::SecretNotFound { reference }
            }
            SecretError::Lifecycle { reference, state } => Self::SecretLifecycleRefused {
                reference,
                detail: format!("the material is {state}"),
            },
            SecretError::Transition { reference, source } => Self::SecretLifecycleRefused {
                reference,
                detail: source.to_string(),
            },
            SecretError::Invalid(detail) => Self::SecretMaterialRefused { detail },
            SecretError::Unwrap { reference, kek } => Self::SecretStoreUnusable {
                // The KEK *reference* is a configured name, not key material.
                detail: format!("{reference} could not be unwrapped under `{kek}`"),
            },
            SecretError::Denied { backend, message } => Self::SecretStoreUnusable {
                detail: format!("{backend}: {message}"),
            },
        }
    }

    /// The stable name of the invariant a candidate broke, if this is a
    /// validation refusal.
    pub const fn rule(&self) -> Option<&'static str> {
        match self {
            Self::ValidationFailed { rule, .. } => Some(rule),
            _ => None,
        }
    }

    /// The revision this refusal is about, if it is about one.
    pub const fn revision(&self) -> Option<RevisionId> {
        match self {
            Self::RevisionNotFound(revision)
            | Self::RevisionIncompatible { revision, .. }
            | Self::RevisionTooLarge { revision, .. } => Some(*revision),
            Self::IdempotencyKeyReused { published, .. } => Some(*published),
            Self::RevisionUnreadable { revision, .. } => *revision,
            // A conflict names the head the caller has to re-read, in the
            // structured field rather than only in the prose.
            Self::RevisionConflict { actual, .. } => *actual,
            _ => None,
        }
    }

    /// The resource this refusal names, if it names one.
    pub const fn reference(&self) -> Option<ResourceRef> {
        match self {
            Self::ImmutableResourceVersion { reference } => Some(*reference),
            Self::ValidationFailed { reference, .. } => *reference,
            _ => None,
        }
    }

    /// The body a caller receives.
    pub fn envelope(&self) -> AdminErrorEnvelope {
        AdminErrorEnvelope {
            error: AdminErrorBody {
                code: self.code(),
                message: self.to_string(),
                retryable: self.retryable(),
                rule: self.rule(),
                resource: self
                    .reference()
                    .map(|reference| reference.to_string())
                    // A secret refusal names the version it is about in the same
                    // field a resource refusal names its resource: both are
                    // opaque identifiers the caller already had.
                    .or_else(|| self.secret().map(|reference| reference.to_string())),
                revision: self.revision().map(|revision| revision.to_string()),
            },
        }
    }

    /// Translate a store failure into the administrative vocabulary.
    ///
    /// Exhaustive on purpose: a new [`ControlPlaneError`] variant must be given a
    /// code here rather than collapsing into a generic `500`, which is how
    /// "unreadable storage" and "unreachable storage" stop being
    /// distinguishable.
    pub fn from_control_plane(error: ControlPlaneError) -> Self {
        match error {
            ControlPlaneError::Unavailable { backend, message } => Self::ControlPlaneUnavailable {
                detail: format!("{backend}: {message}"),
            },
            ControlPlaneError::Conflict { expected, actual } => {
                Self::RevisionConflict { expected, actual }
            }
            ControlPlaneError::RevisionNotFound(revision) => Self::RevisionNotFound(revision),
            ControlPlaneError::Invalid(error) => Self::from(error),
            ControlPlaneError::ImmutableResourceVersion { reference } => {
                Self::ImmutableResourceVersion { reference }
            }
            ControlPlaneError::IdempotencyKeyReused { key, published } => {
                Self::IdempotencyKeyReused { key, published }
            }
            ControlPlaneError::Denied { backend, message } => Self::ControlPlaneDenied {
                detail: format!("{backend}: {message}"),
            },
            ControlPlaneError::NameTaken { noun, name, holder } => Self::NameTaken {
                noun,
                detail: holder.map_or_else(
                    || format!("the {noun} name `{name}` is already projected"),
                    |constraint| {
                        format!("the {noun} name `{name}` violates the unique index {constraint}")
                    },
                ),
                name,
            },
            ControlPlaneError::Corrupt { revision, source } => Self::RevisionUnreadable {
                revision: Some(revision),
                detail: source.to_string(),
            },
            ControlPlaneError::CorruptStorage { detail } => Self::RevisionUnreadable {
                revision: None,
                detail,
            },
            ControlPlaneError::Incompatible { revision, source } => Self::RevisionIncompatible {
                revision,
                detail: source.to_string(),
            },
            ControlPlaneError::TooLarge { revision, limit } => Self::RevisionTooLarge {
                revision,
                detail: limit.to_string(),
            },
        }
    }
}

/// The stable rule name and the resource a validation failure is about.
///
/// A separate function rather than a method on [`ValidationError`] because the
/// naming is an *administrative protocol* commitment: the domain is free to
/// reword its messages, and these strings are not allowed to move with them.
fn validation_rule(error: &ValidationError) -> (&'static str, Option<ResourceRef>) {
    match error {
        ValidationError::Empty => ("empty_revision", None),
        ValidationError::DuplicateResourceVersion { reference } => {
            ("duplicate_resource_version", Some(*reference))
        }
        ValidationError::MultipleVersions { first, .. } => ("multiple_versions", Some(*first)),
        ValidationError::VersionNotAdvanced { proposed, .. } => {
            ("version_not_advanced", Some(*proposed))
        }
        ValidationError::DuplicateSlug { first, .. } => ("duplicate_slug", Some(*first)),
        ValidationError::ScopeMismatch { reference, .. } => ("scope_mismatch", Some(*reference)),
        ValidationError::DanglingResourceReference { from, .. } => {
            ("dangling_resource_reference", Some(*from))
        }
        ValidationError::DanglingBlobReference { from, .. } => {
            ("dangling_blob_reference", Some(*from))
        }
        ValidationError::UnreferencedBlob { .. } => ("unreferenced_blob", None),
        ValidationError::PinnedSnapshotWithdrawn { enablement, .. } => {
            ("pinned_snapshot_withdrawn", Some(*enablement))
        }
        ValidationError::CrossTenantReference { from, .. } => {
            ("cross_tenant_reference", Some(*from))
        }
        ValidationError::TenantScopedDependency { from, .. } => {
            ("tenant_scoped_dependency", Some(*from))
        }
        ValidationError::Tenancy(_) => ("tenancy", None),
        // #243's credential records validate by their own rules; the resource is
        // named by the inner error's message, and the material never is.
        ValidationError::Credential(_) => ("provider_credential", None),
        ValidationError::CredentialTransition(_) => ("credential_transition", None),
        // #253's policy records and #255's model contracts validate by their own
        // rules, and name the resource they are about without quoting its body.
        ValidationError::Policy(policy) => ("policy", Some(policy.reference())),
        ValidationError::Provider(_) => ("provider_connection", None),
        ValidationError::Model(_) => ("model_contract", None),
        // #201's price books validate by their own rules, and name the book the
        // refusal is about without quoting the rates it states.
        ValidationError::Pricing(pricing) => ("price_book", Some(pricing.reference())),
        ValidationError::AuditMutationMismatch { .. } => ("audit_mutation_mismatch", None),
        ValidationError::Canonical(_) => ("not_canonical", None),
    }
}

impl From<ValidationError> for AdminError {
    fn from(error: ValidationError) -> Self {
        let (rule, reference) = validation_rule(&error);
        Self::ValidationFailed {
            rule,
            reference,
            detail: error.to_string(),
        }
    }
}

impl From<CanonicalError> for AdminError {
    /// State with no canonical form is invalid state, and the domain already says
    /// so: routing through [`ValidationError`] keeps one rule name for it rather
    /// than a second code that means the same thing.
    fn from(error: CanonicalError) -> Self {
        Self::from(ValidationError::from(error))
    }
}

impl From<ControlPlaneError> for AdminError {
    fn from(error: ControlPlaneError) -> Self {
        Self::from_control_plane(error)
    }
}

impl From<AdminAuthError> for AdminError {
    /// The authentication/authorization split is the error's own to make: only
    /// it knows whether the caller failed to establish an identity or failed to
    /// carry authority, and a `403` for the former would tell an anonymous
    /// caller that its credential was recognized.
    fn from(error: AdminAuthError) -> Self {
        if error.is_unavailable() {
            Self::IdentityProviderUnavailable
        } else if error.is_authorization() {
            Self::Forbidden(error)
        } else {
            Self::Unauthenticated(error)
        }
    }
}

/// The wire body of an administrative refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdminErrorEnvelope {
    pub error: AdminErrorBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdminErrorBody {
    /// The stable machine-readable code. Named `type` on the wire to match the
    /// inference envelope's shape, so one client-side error reader handles both.
    #[serde(rename = "type")]
    pub code: &'static str,
    pub message: String,
    /// Whether an identical retry could succeed. Explicit so a client does not
    /// infer it from the status code and retry a conflict.
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        (self.status(), Json(self.envelope())).into_response()
    }
}
