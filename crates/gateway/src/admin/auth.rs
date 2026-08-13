//! Administrative identity: who may change durable state, and how that is kept
//! disjoint from who may send inference traffic.
//!
//! ADR 0027 states the rule this module makes structural: **an inference
//! credential grants no administrative authority, and an administrative
//! credential grants no inference authority.** The two are separate credential
//! populations resolved by separate code, so "the gateway key also administers"
//! is not a configuration mistake anyone can make — there is no path from
//! [`InboundKey`](crate::state::InboundKey) or
//! [`PrincipalAuthority`](crate::principals::PrincipalAuthority) to an
//! [`AdminIdentity`], and nothing here consults the principal stores.
//!
//! Three positive properties:
//!
//! **A human is issuer-scoped.** [`AdminIdentity::Human`] carries the OIDC
//! issuer alongside the subject, because a subject is only unique within its
//! issuer: storing `alice` without saying which identity provider vouched for it
//! merges two people the moment a second provider is trusted. It maps to
//! [`Actor::Human`], which is what an audit row records.
//!
//! **Breakglass is attributed, not anonymous.** The static credential exists for
//! "the identity provider is down" and "the control plane rejected the last
//! change", so it cannot be removed — but a shared secret with no name attached
//! makes an audit trail unreadable. [`AdminIdentity::Breakglass`] therefore
//! requires a [`BreakglassAttribution`]: who is using it and why, both supplied
//! by the caller, both bounded and printable, and both carried into the audit
//! summary. The actor stays [`Actor::Breakglass`] rather than being disguised as
//! a human, because "someone used breakglass" is the thing an auditor searches
//! for.
//!
//! **A refusal never echoes the credential.** [`AdminCredential`] holds its
//! material in a [`SecretString`] and compares in constant time;
//! [`AdminAuthError`] has nowhere to put presented material, so no `401` body and
//! no log line can carry it.

use std::fmt;

use async_trait::async_trait;
use axum::http::HeaderMap;
use axum::http::header::AUTHORIZATION;
use secrecy::{ExposeSecret, SecretString};

use crate::desired_state::{Actor, MutationKind, ResourceScope};
use crate::principals::constant_time_eq;

/// The prefix every minted inference token carries. Presented to `/admin/v1` it
/// is refused by its own error: a caller reaching for the credential it already
/// has needs to be told the surfaces are disjoint, not that its token expired.
pub const INFERENCE_TOKEN_PREFIX: &str = "axt1.";

/// The inference API-key header. Never a credential on `/admin/v1`: it is not
/// read, and when it is the *only* thing presented the refusal names it, for the
/// same reason. Alongside a bearer token it is ignored rather than fatal — a
/// stray header from a shared client must not unauthenticate an administrator
/// who did present an administrative credential.
pub const INFERENCE_KEY_HEADER: &str = "x-api-key";

/// Who is using the breakglass credential.
pub const BREAKGLASS_OPERATOR_HEADER: &str = "x-axond-breakglass-operator";

/// Why the breakglass credential is being used instead of OIDC.
pub const BREAKGLASS_REASON_HEADER: &str = "x-axond-breakglass-reason";

/// An administrative credential as presented, held so it cannot be logged.
#[derive(Clone)]
pub struct AdminCredential(SecretString);

impl fmt::Debug for AdminCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AdminCredential(redacted)")
    }
}

impl AdminCredential {
    /// The bearer token an administrative request presented.
    ///
    /// Only `Authorization: Bearer`. A caller that presented nothing else but an
    /// inference `x-api-key`, or a bearer token that is a minted inference
    /// token, is refused as having offered an inference credential rather than
    /// none, so a caller that guessed the surfaces share credentials gets an
    /// explanation instead of a bare `401`.
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, AdminAuthError> {
        let bearer = headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .filter(|value| !value.is_empty());
        match bearer {
            Some(value) if value.starts_with(INFERENCE_TOKEN_PREFIX) => {
                Err(AdminAuthError::InferenceCredential)
            }
            Some(value) => Ok(Self(SecretString::from(value.to_owned()))),
            None if headers.contains_key(INFERENCE_KEY_HEADER) => {
                Err(AdminAuthError::InferenceCredential)
            }
            None => Err(AdminAuthError::MissingCredential),
        }
    }

    /// A credential from material already in hand — a configured breakglass
    /// secret, or a fixture.
    pub fn new(material: impl Into<String>) -> Self {
        Self(SecretString::from(material.into()))
    }

    /// Whether this credential is the expected one, compared in constant time so
    /// a wrong guess cannot be narrowed by timing.
    pub fn matches(&self, expected: &SecretString) -> bool {
        constant_time_eq(
            self.0.expose_secret().as_bytes(),
            expected.expose_secret().as_bytes(),
        )
    }

    /// The material, for an authenticator that has to verify it. Deliberately
    /// awkward to reach, and never rendered by [`fmt::Debug`].
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }
}

/// Who is using the breakglass credential, and why.
///
/// Caller-supplied and unverifiable — that is the nature of a shared static
/// secret — but *required*, bounded, and recorded. An unattributed breakglass
/// mutation is refused rather than published as "breakglass, unknown".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BreakglassAttribution {
    operator: String,
    reason: String,
}

/// Why an attribution was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidAttribution {
    #[error("breakglass use must name an operator and a reason")]
    Missing,
    #[error("breakglass attribution is over the {max}-character limit")]
    TooLong { max: usize },
    #[error("breakglass attribution must be printable ASCII")]
    Unprintable,
}

impl BreakglassAttribution {
    pub const MAX_LEN: usize = 200;

    /// Parse an operator and a reason.
    ///
    /// Bounded and printable-ASCII for the same reason an
    /// [`IdempotencyKey`](crate::desired_state::IdempotencyKey) is: both end up
    /// in a durable audit row and in log lines, and neither is a place to accept
    /// arbitrary bytes.
    pub fn parse(operator: &str, reason: &str) -> Result<Self, InvalidAttribution> {
        let operator = operator.trim();
        let reason = reason.trim();
        if operator.is_empty() || reason.is_empty() {
            return Err(InvalidAttribution::Missing);
        }
        for field in [operator, reason] {
            if field.len() > Self::MAX_LEN {
                return Err(InvalidAttribution::TooLong { max: Self::MAX_LEN });
            }
            if !field
                .bytes()
                .all(|byte| byte.is_ascii_graphic() || byte == b' ')
            {
                return Err(InvalidAttribution::Unprintable);
            }
        }
        Ok(Self {
            operator: operator.to_owned(),
            reason: reason.to_owned(),
        })
    }

    pub fn operator(&self) -> &str {
        &self.operator
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// An established administrative identity.
///
/// Constructible only by an [`AdminAuthenticator`] in practice, and by nothing
/// that resolves an inference credential in principle: there is no `From<InboundKey>`
/// and no variant that could hold one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminIdentity {
    /// An OIDC-authenticated human, identified by issuer-scoped subject.
    Human { issuer: String, subject: String },
    /// The static breakglass operator, with the attribution it is required to
    /// carry and the non-secret label of the configured credential it presented.
    Breakglass {
        attribution: BreakglassAttribution,
        /// [`AdminBreakglass::label`](crate::config::AdminBreakglass::label) —
        /// which configured credential, by name. Never material.
        credential: String,
    },
}

impl AdminIdentity {
    /// The audit actor this identity publishes as.
    ///
    /// Breakglass stays [`Actor::Breakglass`]: it is not rendered as a human even
    /// though it names one, because the distinction is exactly what an auditor
    /// filters on.
    pub fn actor(&self) -> Actor {
        match self {
            Self::Human { issuer, subject } => Actor::Human {
                issuer: issuer.clone(),
                subject: subject.clone(),
            },
            Self::Breakglass { .. } => Actor::Breakglass,
        }
    }

    /// The summary an audit event records, with breakglass attribution prefixed.
    ///
    /// The prefix is on the summary rather than in the actor because the actor is
    /// a closed vocabulary the durable schema stores; the attribution is prose an
    /// auditor reads next to it.
    pub fn audit_summary(&self, summary: &str) -> String {
        match self {
            Self::Human { .. } => summary.to_owned(),
            Self::Breakglass {
                attribution,
                credential,
            } => format!(
                "breakglass {} as `{}` ({}): {summary}",
                attribution.operator(),
                credential,
                attribution.reason()
            ),
        }
    }

    /// Whether this identity used the breakglass path.
    pub const fn is_breakglass(&self) -> bool {
        matches!(self, Self::Breakglass { .. })
    }
}

/// What an administrative caller is trying to do.
///
/// Coarse verbs over durable state rather than one variant per resource kind: a
/// new resource kind must not widen this enum, and an authorizer decides on
/// (action, scope) rather than on a URL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AdminAction {
    /// Read the complete desired state.
    ReadState,
    /// Read the bounded revision history.
    ReadHistory,
    /// Read a revision's audit trail.
    ReadAudit,
    /// Read what this replica has converged onto.
    ReadConvergence,
    /// Publish a new revision.
    Publish,
    /// Republish an earlier revision's desired state.
    Rollback,
}

impl AdminAction {
    pub const ALL: &'static [Self] = &[
        Self::ReadState,
        Self::ReadHistory,
        Self::ReadAudit,
        Self::ReadConvergence,
        Self::Publish,
        Self::Rollback,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadState => "read_state",
            Self::ReadHistory => "read_history",
            Self::ReadAudit => "read_audit",
            Self::ReadConvergence => "read_convergence",
            Self::Publish => "publish",
            Self::Rollback => "rollback",
        }
    }

    /// Whether this action publishes a revision, and therefore requires an
    /// idempotency key and an expected revision.
    pub const fn mutates(self) -> bool {
        matches!(self, Self::Publish | Self::Rollback)
    }

    /// The action that publishing `kind` requires authority for.
    ///
    /// Rollback is separate authority rather than a flavour of publication:
    /// republishing an earlier revision is the operation an incident responder
    /// may be trusted with while not being trusted to author new state, and the
    /// reverse. A grant is therefore only good for the verb it names.
    pub const fn for_mutation(kind: MutationKind) -> Self {
        match kind {
            MutationKind::Rollback => Self::Rollback,
            MutationKind::Create
            | MutationKind::Update
            | MutationKind::Delete
            | MutationKind::Rotate => Self::Publish,
        }
    }
}

/// Authority for one action at one scope, produced by an [`AdminAuthorizer`].
///
/// The service takes a grant rather than an identity, so a handler cannot reach
/// the publication path with an authenticated-but-unauthorized caller: there is
/// nothing to pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminGrant {
    identity: AdminIdentity,
    action: AdminAction,
    scope: ResourceScope,
}

impl AdminGrant {
    /// Record a decision an authorizer has already made. Only an
    /// [`AdminAuthorizer`] implementation should call this.
    pub fn granted(identity: AdminIdentity, action: AdminAction, scope: ResourceScope) -> Self {
        Self {
            identity,
            action,
            scope,
        }
    }

    pub const fn identity(&self) -> &AdminIdentity {
        &self.identity
    }

    pub const fn action(&self) -> AdminAction {
        self.action
    }

    pub const fn scope(&self) -> &ResourceScope {
        &self.scope
    }
}

/// Why an administrative caller was refused.
///
/// Has no variant carrying presented material, so neither a response nor a log
/// line built from it can leak a credential.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AdminAuthError {
    #[error("no administrative credential was presented")]
    MissingCredential,
    #[error("an inference credential carries no administrative authority")]
    InferenceCredential,
    #[error("the presented administrative credential was not recognized")]
    UnknownCredential,
    #[error("the presented OIDC token was not accepted")]
    TokenRejected,
    #[error("issuer `{issuer}` is not trusted for administration")]
    UntrustedIssuer { issuer: String },
    #[error("the identity provider could not be consulted")]
    IdentityProviderUnavailable,
    #[error(transparent)]
    Attribution(#[from] InvalidAttribution),
    #[error("this identity may not {}", action.as_str())]
    ActionNotPermitted { action: AdminAction },
    #[error("this identity may not act on that scope")]
    ScopeNotPermitted,
}

impl AdminAuthError {
    /// Whether this is a failure of *authority* rather than of identity: a `403`
    /// rather than a `401`.
    pub const fn is_authorization(&self) -> bool {
        matches!(
            self,
            Self::ActionNotPermitted { .. } | Self::ScopeNotPermitted
        )
    }

    /// Whether the identity provider itself could not be reached, which is an
    /// availability failure rather than a rejection.
    pub const fn is_unavailable(&self) -> bool {
        matches!(self, Self::IdentityProviderUnavailable)
    }
}

/// What the breakglass attribution headers amounted to.
///
/// Three states rather than two, and no refusal here, because only an
/// authenticator knows whether the presented credential is the breakglass one.
/// An OIDC administrator carrying a stray or half-filled attribution header is
/// not an unauthenticated caller, so a malformed attribution is *carried* to the
/// decision that cares about it instead of ending the request before it. Absent
/// and unreadable are kept apart for the same reason
/// [`MutationPreconditions`](super::protocol::MutationPreconditions) keeps them
/// apart: a header that could not be read is not a header that was not sent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresentedAttribution {
    /// Neither header was sent.
    Absent,
    /// Attribution was attempted and is not usable.
    Invalid(InvalidAttribution),
    Present(BreakglassAttribution),
}

impl PresentedAttribution {
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let field = |name: &str| headers.get(name).map(|value| value.to_str().ok());
        match (
            field(BREAKGLASS_OPERATOR_HEADER),
            field(BREAKGLASS_REASON_HEADER),
        ) {
            (None, None) => Self::Absent,
            // A header whose bytes are not text cannot be an attribution, and is
            // not the same thing as a header that was left out.
            (Some(None), _) | (_, Some(None)) => Self::Invalid(InvalidAttribution::Unprintable),
            (operator, reason) => {
                fn text(value: Option<Option<&str>>) -> &str {
                    value.flatten().unwrap_or("")
                }
                match BreakglassAttribution::parse(text(operator), text(reason)) {
                    Ok(attribution) => Self::Present(attribution),
                    Err(error) => Self::Invalid(error),
                }
            }
        }
    }

    /// The attribution breakglass requires, or why this request cannot use it.
    ///
    /// Called only once a credential has been recognized as the breakglass one:
    /// that is where an absent or malformed attribution becomes a refusal.
    pub fn require(&self) -> Result<BreakglassAttribution, AdminAuthError> {
        match self {
            Self::Present(attribution) => Ok(attribution.clone()),
            Self::Invalid(error) => Err(AdminAuthError::Attribution(*error)),
            Self::Absent => Err(AdminAuthError::Attribution(InvalidAttribution::Missing)),
        }
    }
}

/// What a request presented, before anything is decided about it.
#[derive(Debug, Clone)]
pub struct AdminPresented {
    pub credential: AdminCredential,
    pub attribution: PresentedAttribution,
}

impl AdminPresented {
    pub fn from_headers(headers: &HeaderMap) -> Result<Self, AdminAuthError> {
        Ok(Self {
            credential: AdminCredential::from_headers(headers)?,
            attribution: PresentedAttribution::from_headers(headers),
        })
    }
}

/// Establishes an administrative identity from what a request presented.
///
/// Async because an OIDC implementation resolves keys over the network; the
/// contract explicitly allows it to be slow, since no inference request waits on
/// it. The implementations land with the stateful runtime — an OIDC verifier and
/// the configured breakglass credential — and both produce the same
/// [`AdminIdentity`] type, so nothing downstream branches on how a caller
/// authenticated except where the audit trail deliberately does.
#[async_trait]
pub trait AdminAuthenticator: Send + Sync {
    /// A stable, low-cardinality name for logs and metrics.
    fn name(&self) -> &'static str;

    async fn authenticate(
        &self,
        presented: &AdminPresented,
    ) -> Result<AdminIdentity, AdminAuthError>;
}

/// Decides whether an established identity may perform an action at a scope.
///
/// Synchronous: an authorization decision that needed a backend read would put
/// an outage between an operator and breakglass, which is the situation
/// breakglass exists for. Policy resources are desired state, so a future
/// policy-aware authorizer reads them from the snapshot it was built with, not
/// from the control plane per request.
pub trait AdminAuthorizer: Send + Sync {
    fn name(&self) -> &'static str;

    fn authorize(
        &self,
        identity: &AdminIdentity,
        action: AdminAction,
        scope: &ResourceScope,
    ) -> Result<AdminGrant, AdminAuthError>;
}
