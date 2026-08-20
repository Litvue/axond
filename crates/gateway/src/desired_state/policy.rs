//! Policy bodies: what a tenant or a project *may spend and hold* (#208).
//!
//! [`tenancy`](super::tenancy) gives a tenant and a project a durable identity;
//! this module gives each of them one durable **policy document**: the budget
//! cap, the concurrency ceiling, and the token floor a revision wants enforced.
//! It is the typed contract the convergence/runtime slice activates against,
//! and nothing more — see *what this module deliberately does not do*, below.
//!
//! # A policy document is complete, never merged
//!
//! One document states a scope's policy in full:
//!
//! | Field | Meaning |
//! | --- | --- |
//! | `schema` | `axond.policy.v1` |
//! | `tenant_id` | the owning [`TenantId`] |
//! | `project_id` | the owning [`ProjectId`], absent for a tenant-wide document |
//! | `epoch` | the operator-advanced [`PolicyEpoch`] this content was published under |
//! | `budget_limit_microdollars` | the per-subject cap inside this scope |
//! | `namespace_budget_limit_microdollars` | the scope-wide cap, absent when there is none |
//! | `reservation_ttl_seconds` | how long a budget hold survives a replica that dies mid-request |
//! | `max_in_flight_per_subject` | the concurrency ceiling per subject |
//! | `lease_ttl_seconds` | how long an abandoned concurrency lease stays live |
//! | `minimum_token_epoch` | the mint epoch below which a token is refused |
//! | `content_middleware` | the optional ordered in-process content chain |
//! | `buffered_response_routes` | the optional normalized set of streaming surfaces allowed to buffer for response mutation |
//!
//! Two rules follow from *complete*, and they are the reason this is a document
//! rather than a bag of settings:
//!
//! - **Fields are never merged across revisions.** Reading policy takes the whole
//!   document of one revision or none of it. There is no "the new revision
//!   changed the cap, keep the old TTL": a revision is whole desired state, so a
//!   field absent from the newer document is not inherited from the older one.
//! - **Fields are never merged across scopes.** A project's policy does not
//!   inherit its tenant's field by field either. [`PolicySet::effective`] selects
//!   *one* document — the project's if it has one, otherwise its tenant's — so an
//!   effective policy is always a document some operator published as a unit and
//!   can read back verbatim. An absent optional field is therefore a complete
//!   statement ("this scope has no scope-wide cap"), never an inheritance.
//!
//! A scope with no document at all has no *published* policy, and the bootstrap
//! file's limits stand. That is the only fallback, and it is a fallback between
//! whole configurations rather than between fields.
//!
//! # Generation: an epoch plus the revision that published it
//!
//! A document declares an `epoch`; the revision that carries it has a
//! [`RevisionId`]. Together — with the scope they are the policy of, since an
//! epoch counts within one scope and says nothing across scopes — they are a
//! [`PolicyGeneration`], and that whole, not any part of it, is what a writer
//! holds and what a fence compares.
//!
//! The epoch alone is not enough: two publications can carry the same epoch (a
//! forked control plane, a restored backup), and a writer admitted on epoch
//! equality would be enforcing a document nobody currently serves. The revision
//! id alone is not enough either: it says which publication, but not whether the
//! change was a material one, and ordering revision ids is a storage detail
//! rather than a policy decision. The epoch is what an operator advances when the
//! content changes, and [`PolicyTransition`] refuses a material change that does
//! not advance it — which is what makes the epoch a usable order.
//!
//! A generation therefore carries the document's content, digested as a
//! [`PolicyContent`], as well. A revision is whole desired state, so every
//! revision restates every policy document it carries: a revision that changed an
//! unrelated resource still hands out a generation with a new revision id for a
//! document whose epoch and content never moved. That carry-forward is the
//! ordinary case, and it is exactly what distinguishes it from a fork — same
//! epoch, *different* content.
//!
//! # Stale writers fail closed
//!
//! [`PolicyFence`] admits a writer that holds the policy the fence is enforcing —
//! the active epoch and the active content, whichever revision carried it — and
//! refuses every other case: an older epoch, a newer epoch this replica has not
//! adopted, the same epoch stating a different policy, and any generation of
//! another scope. Refusing a seemingly newer generation is deliberate — a writer
//! that may enforce anything it can claim is newer is not fenced at all — and
//! refusing anything but the enforced policy means an unknown generation denies
//! instead of admitting unenforced. Adoption follows the same rule: it moves onto
//! a higher epoch of the same scope, and onto the active document as a later
//! revision restates it, never onto a different policy. That is the same posture
//! as an unreachable budget store
//! ([`UnavailablePolicy::Deny`](crate::budget::UnavailablePolicy)): an
//! unenforceable cap must not silently admit.
//!
//! # What this module deliberately does not do
//!
//! **Bootstrap keeps what bootstrap owns.** Which backend enforces a cap, the DSN
//! it connects with, the table or key prefix it lays state out under, and the
//! stance to take when that store cannot be reached are *not* policy. They are
//! local, operator-owned, and reviewed with the file they live in; a published
//! document that could flip `on_unavailable` to `allow` would turn a policy
//! publication into a way to switch enforcement off. Naming one of those fields in
//! a policy body is a typed refusal ([`PolicyError::BootstrapOwned`]) rather than
//! an unknown field, so the boundary is stated in the reader rather than only in
//! prose.
//!
//! **Nothing here is activated.** No request path reads a document, no store
//! writes one, and no snapshot is compiled from one: this slice is the contract,
//! and [`PolicyTransition`] classifies what activating a change would *require*
//! rather than performing it.
//!
//! # Where these rules are enforced
//!
//! [`PolicySet::of`] reads every policy body in a [`DesiredState`], and
//! [`DesiredState::validate`] calls it — so publication and hydration inherit it
//! exactly as they inherit tenancy, with no request path involved. Schema
//! strictness, and the compatibility-versus-damage distinction
//! ([`PolicyError::is_incompatible`]), follow the rules the
//! [tenancy module](super::tenancy) states for every body schema.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use gateway_core::{GuardrailAction, GuardrailRule, MiddlewareFailurePosture, MiddlewareScope};

use super::canonical::{Canonical, CanonicalValue, Checksum};
use super::ids::{InvalidId, ProjectId, ResourceId, RevisionId, Slug, TenantId};
use super::record::{
    BodyError, IdentifiedBody, PROJECT_ID_FIELD, Record, SCHEMA_FIELD, TENANT_ID_FIELD,
};
use super::resource::{
    ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
};
use super::revision::DesiredState;

/// The policy body schema this build reads and writes.
pub const POLICY_SCHEMA: &str = "axond.policy.v1";

const EPOCH_FIELD: &str = "epoch";
const BUDGET_LIMIT_FIELD: &str = "budget_limit_microdollars";
const NAMESPACE_BUDGET_LIMIT_FIELD: &str = "namespace_budget_limit_microdollars";
const RESERVATION_TTL_FIELD: &str = "reservation_ttl_seconds";
const MAX_IN_FLIGHT_FIELD: &str = "max_in_flight_per_subject";
const LEASE_TTL_FIELD: &str = "lease_ttl_seconds";
const MINIMUM_TOKEN_EPOCH_FIELD: &str = "minimum_token_epoch";
const CONTENT_MIDDLEWARE_FIELD: &str = "content_middleware";
const BUFFERED_RESPONSE_ROUTES_FIELD: &str = "buffered_response_routes";
const MIDDLEWARE_ID_FIELD: &str = "id";
const MIDDLEWARE_SCOPES_FIELD: &str = "scopes";
const MIDDLEWARE_FAILURE_POSTURE_FIELD: &str = "failure_posture";
const MIDDLEWARE_MAX_DURATION_FIELD: &str = "max_duration_milliseconds";
const MIDDLEWARE_GUARDRAIL_FIELD: &str = "guardrail";
const GUARDRAIL_KEY_ENV_FIELD: &str = "key_env";
const GUARDRAIL_RULES_FIELD: &str = "rules";
const GUARDRAIL_RULE_ID_FIELD: &str = "id";
const GUARDRAIL_RULE_PATTERN_FIELD: &str = "pattern";
const GUARDRAIL_RULE_ACTION_FIELD: &str = "action";

const CONTENT_MIDDLEWARE_FIELDS: &[&str] = &[
    MIDDLEWARE_ID_FIELD,
    MIDDLEWARE_SCOPES_FIELD,
    MIDDLEWARE_FAILURE_POSTURE_FIELD,
    MIDDLEWARE_MAX_DURATION_FIELD,
    MIDDLEWARE_GUARDRAIL_FIELD,
];
const GUARDRAIL_FIELDS: &[&str] = &[GUARDRAIL_KEY_ENV_FIELD, GUARDRAIL_RULES_FIELD];
const GUARDRAIL_RULE_FIELDS: &[&str] = &[
    GUARDRAIL_RULE_ID_FIELD,
    GUARDRAIL_RULE_PATTERN_FIELD,
    GUARDRAIL_RULE_ACTION_FIELD,
];
const MAX_CONTENT_MIDDLEWARE: usize = 32;
const MAX_MIDDLEWARE_DURATION_MILLISECONDS: u64 = 1_000;
const MAX_GUARDRAIL_RULES: usize = 64;
const MAX_GUARDRAIL_PATTERN_BYTES: usize = 4_096;
const REDACTION_MIDDLEWARE_ID: &str = "axond.redact";
const CORE_STAGE_IDS: &[&str] = &[
    "accounting",
    "admission",
    "authentication",
    "budget",
    "convergence",
    "diagnostic-ceiling",
    "pricing",
    "provider-failover",
    "rate-limit",
    "request-bounds",
    "settlement",
];

/// The settings a published document may never name, because the bootstrap file
/// owns them: which backend enforces policy, how it is reached, how it lays state
/// out, and what it does when it cannot be reached.
///
/// Held as data rather than as prose so the reader enforces the boundary and a
/// test can assert on it.
pub const BOOTSTRAP_OWNED_FIELDS: &[&str] = &[
    "backend",
    "create_table",
    "dsn_env",
    "key_prefix",
    "namespace_scope",
    "on_unavailable",
    "table",
];

/// Why a policy body, or the set of documents it belongs to, was refused.
///
/// Every arm names the resource it is about, so a refusal an operator reads
/// points at one row rather than at "the revision".
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("{reference} is a {} resource, not a {}", found.as_str(), expected.as_str())]
    Kind {
        reference: ResourceRef,
        expected: ResourceKind,
        found: ResourceKind,
    },
    #[error("{reference} is a blob body; a policy record is inline")]
    NotInline { reference: ResourceRef },
    #[error("{reference} is not a record")]
    NotARecord { reference: ResourceRef },
    #[error(
        "{reference} declares schema `{found}`, which this build does not read (expected `{expected}`)"
    )]
    Schema {
        reference: ResourceRef,
        expected: &'static str,
        found: String,
    },
    /// A `schema` that is present and is not text, so the identifier deciding how
    /// to read the rest of the body is itself unreadable. No release wrote one,
    /// so the row is damage rather than another release's writing.
    #[error(
        "{reference} carries a `schema` that is not an identifier, which no release wrote; \
         restore the row or republish the resource rather than changing build"
    )]
    DamagedSchema { reference: ResourceRef },
    #[error("{reference} has no `{field}`")]
    MissingField {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} carries `{field}`, which `{schema}` does not define")]
    UnknownField {
        reference: ResourceRef,
        schema: &'static str,
        field: String,
    },
    /// A field the bootstrap file owns, named by a published document.
    ///
    /// Its own arm rather than [`UnknownField`](Self::UnknownField): the field is
    /// not one a future schema may add, so the refusal states the boundary
    /// instead of reading as a version skew. These names are what this schema
    /// *reserves* in the shared reader.
    #[error(
        "{reference} carries `{field}`, which the bootstrap file owns and a published policy may not set"
    )]
    BootstrapOwned {
        reference: ResourceRef,
        field: String,
    },
    #[error(
        "{reference} field `{field}` is not the type `{}` defines",
        POLICY_SCHEMA
    )]
    FieldType {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} field `{field}` {source}")]
    MalformedId {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidId,
    },
    #[error("{reference} field `{field}` is out of range: {source}")]
    FieldRange {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidPolicy,
    },
    #[error("{reference} content middleware registration `{field}` is invalid: {source}")]
    InvalidMiddleware {
        reference: ResourceRef,
        field: String,
        #[source]
        source: InvalidContentMiddleware,
    },
    #[error("{reference} field `{field}` is invalid: {source}")]
    InvalidBufferedResponseRoutes {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidBufferedResponseRoutes,
    },
    #[error("{reference} carries {declared}, but its resource identity is {identity}")]
    IdentityMismatch {
        reference: ResourceRef,
        declared: String,
        identity: ResourceId,
    },
    #[error("{reference} declares the policy of {declared}, but is scoped to {scoped:?}")]
    ScopeMismatch {
        reference: ResourceRef,
        declared: PolicyScope,
        scoped: ResourceScope,
    },
}

impl PolicyError {
    /// Whether this refusal means *this build cannot read the body*, rather than
    /// *these rows do not agree with each other*.
    ///
    /// The rule is the one [`TenancyError::is_incompatible`] states, applied to
    /// one more schema: a schema identifier this build does not read, a field a
    /// newer release added, or a body with no identifier at all is a release skew
    /// and storage is intact; anything refused *inside* a body that declared
    /// `axond.policy.v1` is a rewrite, and points at storage.
    ///
    /// A bootstrap-owned field is damage rather than a skew on purpose: those
    /// field names are excluded from policy by design, so no future release adds
    /// one, and reporting it as a version skew would send an operator to upgrade
    /// a fleet over a document that should never have been authored.
    ///
    /// A *bound* is the exception, for the reason a display name is tenancy's: the
    /// rules a schema's values are held to can tighten within one identifier, so a
    /// value below a minimum this build enforces may be one an earlier build wrote
    /// and accepted, and storage is intact. A value that is not a counter at all —
    /// negative, or past what these fields count in — is not a bound but a shape,
    /// and the reader reports it as [`FieldType`](Self::FieldType): damage, since
    /// every release has written these fields as unsigned counters.
    ///
    /// [`TenancyError::is_incompatible`]: super::tenancy::TenancyError::is_incompatible
    pub fn is_incompatible(&self) -> bool {
        match self {
            Self::Schema { .. } | Self::UnknownField { .. } => true,
            // Absence of the schema identifier only: a body written before
            // policy had one at all is another release's writing, while a marker
            // that is present and unreadable is `DamagedSchema`.
            Self::MissingField { field, .. } => *field == SCHEMA_FIELD,
            Self::FieldType { .. } | Self::DamagedSchema { .. } => false,
            Self::InvalidMiddleware { source, .. } => matches!(
                source,
                InvalidContentMiddleware::Scope(_)
                    | InvalidContentMiddleware::FailurePosture(_)
                    | InvalidContentMiddleware::ZeroBound
                    | InvalidContentMiddleware::BoundTooLarge
                    | InvalidContentMiddleware::CoreStage(_)
                    | InvalidContentMiddleware::TooMany
                    | InvalidContentMiddleware::GuardrailTooManyRules
                    | InvalidContentMiddleware::GuardrailAction(_)
            ),
            Self::InvalidBufferedResponseRoutes { source, .. } => {
                matches!(source, InvalidBufferedResponseRoutes::Unsupported(_))
            }
            Self::FieldRange { .. } => true,
            Self::Kind { .. }
            | Self::NotInline { .. }
            | Self::NotARecord { .. }
            | Self::BootstrapOwned { .. }
            | Self::MalformedId { .. }
            | Self::IdentityMismatch { .. }
            | Self::ScopeMismatch { .. } => false,
        }
    }

    /// The resource this refusal is about.
    pub const fn reference(&self) -> ResourceRef {
        match self {
            Self::Kind { reference, .. }
            | Self::NotInline { reference }
            | Self::NotARecord { reference }
            | Self::Schema { reference, .. }
            | Self::DamagedSchema { reference }
            | Self::MissingField { reference, .. }
            | Self::UnknownField { reference, .. }
            | Self::BootstrapOwned { reference, .. }
            | Self::FieldType { reference, .. }
            | Self::MalformedId { reference, .. }
            | Self::FieldRange { reference, .. }
            | Self::InvalidMiddleware { reference, .. }
            | Self::InvalidBufferedResponseRoutes { reference, .. }
            | Self::IdentityMismatch { reference, .. }
            | Self::ScopeMismatch { reference, .. } => *reference,
        }
    }
}

/// Why a policy value is not one this build will enforce.
///
/// Checked when a body is *built* as well as when one is read, so an authored
/// document and a stored document obey the same bounds rather than two.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidPolicy {
    #[error("{value} is below the minimum of {min}")]
    TooSmall { value: u64, min: u64 },
}

/// Why a content-middleware registration is not a bounded, typed declaration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidContentMiddleware {
    #[error("id must be 1-64 lowercase ASCII letters, digits, `.`, `_`, or `-`")]
    Id,
    #[error("at least one scope is required")]
    NoScope,
    #[error("scope `{0}` is declared more than once")]
    DuplicateScope(&'static str),
    #[error("scope `{0}` is not supported")]
    Scope(String),
    #[error("failure posture `{0}` is not supported")]
    FailurePosture(String),
    #[error("max_duration_milliseconds must be at least 1")]
    ZeroBound,
    #[error("max_duration_milliseconds must not exceed {MAX_MIDDLEWARE_DURATION_MILLISECONDS}")]
    BoundTooLarge,
    #[error("`{0}` is a compiled core stage and cannot be registered by policy")]
    CoreStage(String),
    #[error("a policy may register at most {MAX_CONTENT_MIDDLEWARE} content middleware")]
    TooMany,
    #[error("middleware id `{0}` is registered more than once")]
    DuplicateId(String),
    #[error("middleware `axond.redact` requires failure posture `fail_closed`")]
    RedactionRequiresFailClosed,
    #[error("guardrail key_env must be a 1-128 byte environment-variable name")]
    GuardrailKeyEnv,
    #[error("a guardrail requires at least one rule")]
    GuardrailNoRules,
    #[error("a guardrail may declare at most {MAX_GUARDRAIL_RULES} rules")]
    GuardrailTooManyRules,
    #[error("guardrail rule id must be 1-64 lowercase ASCII letters, digits, `.`, `_`, or `-`")]
    GuardrailRuleId,
    #[error("guardrail rule id `{0}` is declared more than once")]
    DuplicateGuardrailRule(String),
    #[error("guardrail rule `{0}` must have a 1-{MAX_GUARDRAIL_PATTERN_BYTES} byte pattern")]
    GuardrailPattern(String),
    #[error("guardrail action `{0}` is not supported")]
    GuardrailAction(String),
}

/// Why a response route cannot be selected for policy-controlled buffering.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBufferedResponseRoutes {
    #[error("response route `{0}` is not supported")]
    Unsupported(String),
    #[error("response route `{0}` is selected more than once")]
    Duplicate(&'static str),
}

/// A streaming API surface whose response may be buffered explicitly by policy.
///
/// The closed enum keeps policy from silently accepting a route whose framing
/// the serving path does not yet know how to reconstruct.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BufferedResponseRoute {
    Messages,
    Responses,
}

impl BufferedResponseRoute {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Messages => "messages",
            Self::Responses => "responses",
        }
    }

    pub fn parse(value: &str) -> Result<Self, InvalidBufferedResponseRoutes> {
        match value {
            "messages" => Ok(Self::Messages),
            "responses" => Ok(Self::Responses),
            other => Err(InvalidBufferedResponseRoutes::Unsupported(other.to_owned())),
        }
    }
}

fn normalize_buffered_response_routes(
    routes: impl IntoIterator<Item = BufferedResponseRoute>,
) -> Result<Vec<BufferedResponseRoute>, InvalidBufferedResponseRoutes> {
    let mut routes = routes.into_iter().collect::<Vec<_>>();
    routes.sort_unstable();
    for pair in routes.windows(2) {
        if pair[0] == pair[1] {
            return Err(InvalidBufferedResponseRoutes::Duplicate(pair[0].as_str()));
        }
    }
    Ok(routes)
}

/// One ordered content-middleware registration in a typed policy document.
///
/// The identifier selects a compiled in-process implementation. The document
/// may select only the content scopes and failure/bound posture exposed here;
/// core authentication, admission, accounting, and failover stages have no
/// representation in this type and therefore cannot be reordered or removed by
/// desired state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentMiddlewareRegistration {
    id: String,
    scopes: Vec<MiddlewareScope>,
    failure_posture: MiddlewareFailurePosture,
    max_duration_milliseconds: u64,
    guardrail: Option<ContentGuardrailRegistration>,
}

impl ContentMiddlewareRegistration {
    pub fn new(
        id: impl Into<String>,
        scopes: impl IntoIterator<Item = MiddlewareScope>,
        failure_posture: MiddlewareFailurePosture,
        max_duration_milliseconds: u64,
    ) -> Result<Self, InvalidContentMiddleware> {
        let id = id.into();
        if id.is_empty()
            || id.len() > 64
            || !id.bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
            })
        {
            return Err(InvalidContentMiddleware::Id);
        }
        if CORE_STAGE_IDS.contains(&id.as_str()) {
            return Err(InvalidContentMiddleware::CoreStage(id));
        }
        if id == REDACTION_MIDDLEWARE_ID && failure_posture != MiddlewareFailurePosture::FailClosed
        {
            return Err(InvalidContentMiddleware::RedactionRequiresFailClosed);
        }
        let mut scopes = scopes.into_iter().collect::<Vec<_>>();
        if scopes.is_empty() {
            return Err(InvalidContentMiddleware::NoScope);
        }
        scopes.sort_unstable_by_key(|scope| middleware_scope_rank(*scope));
        for pair in scopes.windows(2) {
            if pair[0] == pair[1] {
                return Err(InvalidContentMiddleware::DuplicateScope(
                    middleware_scope_name(pair[0]),
                ));
            }
        }
        if max_duration_milliseconds == 0 {
            return Err(InvalidContentMiddleware::ZeroBound);
        }
        if max_duration_milliseconds > MAX_MIDDLEWARE_DURATION_MILLISECONDS {
            return Err(InvalidContentMiddleware::BoundTooLarge);
        }
        Ok(Self {
            id,
            scopes,
            failure_posture,
            max_duration_milliseconds,
            guardrail: None,
        })
    }

    pub fn with_guardrail(
        mut self,
        guardrail: ContentGuardrailRegistration,
    ) -> Result<Self, InvalidContentMiddleware> {
        self.guardrail = Some(guardrail);
        Ok(self)
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn scopes(&self) -> &[MiddlewareScope] {
        &self.scopes
    }

    pub const fn failure_posture(&self) -> MiddlewareFailurePosture {
        self.failure_posture
    }

    pub const fn max_duration_milliseconds(&self) -> u64 {
        self.max_duration_milliseconds
    }

    pub const fn guardrail(&self) -> Option<&ContentGuardrailRegistration> {
        self.guardrail.as_ref()
    }
}

/// Secret-reference and ordered rules for the built-in deterministic guardrail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentGuardrailRegistration {
    key_env: String,
    rules: Vec<GuardrailRule>,
}

impl ContentGuardrailRegistration {
    pub fn new(
        key_env: impl Into<String>,
        rules: Vec<GuardrailRule>,
    ) -> Result<Self, InvalidContentMiddleware> {
        let key_env = key_env.into();
        if key_env.is_empty()
            || key_env.len() > 128
            || !key_env.bytes().enumerate().all(|(index, byte)| {
                if index == 0 {
                    byte.is_ascii_alphabetic() || byte == b'_'
                } else {
                    byte.is_ascii_alphanumeric() || byte == b'_'
                }
            })
        {
            return Err(InvalidContentMiddleware::GuardrailKeyEnv);
        }
        if rules.is_empty() {
            return Err(InvalidContentMiddleware::GuardrailNoRules);
        }
        if rules.len() > MAX_GUARDRAIL_RULES {
            return Err(InvalidContentMiddleware::GuardrailTooManyRules);
        }
        let mut ids = BTreeSet::new();
        for rule in &rules {
            if rule.id.is_empty()
                || rule.id.len() > 64
                || !rule.id.bytes().enumerate().all(|(index, byte)| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
                })
            {
                return Err(InvalidContentMiddleware::GuardrailRuleId);
            }
            if !ids.insert(rule.id.clone()) {
                return Err(InvalidContentMiddleware::DuplicateGuardrailRule(
                    rule.id.clone(),
                ));
            }
            if rule.pattern.is_empty() || rule.pattern.len() > MAX_GUARDRAIL_PATTERN_BYTES {
                return Err(InvalidContentMiddleware::GuardrailPattern(rule.id.clone()));
            }
        }
        Ok(Self { key_env, rules })
    }

    pub fn key_env(&self) -> &str {
        &self.key_env
    }

    pub fn rules(&self) -> &[GuardrailRule] {
        &self.rules
    }
}

fn guardrail_action_name(action: GuardrailAction) -> &'static str {
    match action {
        GuardrailAction::Block => "block",
        GuardrailAction::Redact => "redact",
    }
}

fn parse_guardrail_action(value: &str) -> Result<GuardrailAction, InvalidContentMiddleware> {
    match value {
        "block" => Ok(GuardrailAction::Block),
        "redact" => Ok(GuardrailAction::Redact),
        other => Err(InvalidContentMiddleware::GuardrailAction(other.to_owned())),
    }
}

fn middleware_scope_rank(scope: MiddlewareScope) -> u8 {
    match scope {
        MiddlewareScope::Request => 0,
        MiddlewareScope::Response => 1,
        MiddlewareScope::StreamEvent => 2,
    }
}

fn middleware_scope_name(scope: MiddlewareScope) -> &'static str {
    match scope {
        MiddlewareScope::Request => "request",
        MiddlewareScope::Response => "response",
        MiddlewareScope::StreamEvent => "stream_event",
    }
}

fn parse_middleware_scope(value: &str) -> Result<MiddlewareScope, InvalidContentMiddleware> {
    match value {
        "request" => Ok(MiddlewareScope::Request),
        "response" => Ok(MiddlewareScope::Response),
        "stream_event" => Ok(MiddlewareScope::StreamEvent),
        other => Err(InvalidContentMiddleware::Scope(other.to_owned())),
    }
}

fn middleware_failure_posture_name(posture: MiddlewareFailurePosture) -> &'static str {
    match posture {
        MiddlewareFailurePosture::FailOpen => "fail_open",
        MiddlewareFailurePosture::FailClosed => "fail_closed",
    }
}

fn parse_middleware_failure_posture(
    value: &str,
) -> Result<MiddlewareFailurePosture, InvalidContentMiddleware> {
    match value {
        "fail_open" => Ok(MiddlewareFailurePosture::FailOpen),
        "fail_closed" => Ok(MiddlewareFailurePosture::FailClosed),
        other => Err(InvalidContentMiddleware::FailurePosture(other.to_owned())),
    }
}

/// A policy generation counter an operator advances when a document's content
/// changes.
///
/// Monotonic within a scope, and part of the body rather than derived from it: a
/// checksum tells you two documents differ, while an epoch tells you which one an
/// operator published *later*, which is what a fence needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PolicyEpoch(u64);

impl PolicyEpoch {
    /// The first epoch. Zero is not an epoch, so an unset field cannot read as a
    /// valid one.
    pub const FIRST: Self = Self(1);

    pub const fn new(value: u64) -> Result<Self, InvalidPolicy> {
        if value == 0 {
            return Err(InvalidPolicy::TooSmall { value, min: 1 });
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    /// The next epoch, for the publication that changes a document's content.
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for PolicyEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Which tenant or project a policy document is the policy *of*.
///
/// The tenancy model of #144/#191 exactly: a tenant, and optionally one of its
/// projects. There is no third level and no policy that belongs to a deployment —
/// deployment-wide limits are the bootstrap file's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PolicyScope {
    Namespace(ResourceId),
    Tenant(TenantId),
    Project {
        tenant: TenantId,
        project: ProjectId,
    },
}

impl PolicyScope {
    pub const fn tenant(self) -> Option<TenantId> {
        match self {
            Self::Namespace(_) => None,
            Self::Tenant(tenant) | Self::Project { tenant, .. } => Some(tenant),
        }
    }

    pub const fn project(self) -> Option<ProjectId> {
        match self {
            Self::Namespace(_) | Self::Tenant(_) => None,
            Self::Project { project, .. } => Some(project),
        }
    }

    /// The scope a document at this policy scope lives at on its envelope.
    ///
    /// A policy document is scoped to the thing it governs, so scope and subject
    /// are one fact rather than two that could disagree.
    pub const fn resource_scope(self) -> ResourceScope {
        match self {
            Self::Namespace(_) => ResourceScope::Deployment,
            Self::Tenant(tenant) => ResourceScope::Tenant(tenant),
            Self::Project { tenant, project } => ResourceScope::Project { tenant, project },
        }
    }

    /// The resource identity this scope's policy is written under.
    ///
    /// Derived from the governed object, so "the policy of project X" is one
    /// durable resource: a second document for the same scope is a second version
    /// of the same resource, which [`DesiredState::validate`] already refuses
    /// within one revision.
    pub const fn resource_id(self) -> ResourceId {
        match self {
            Self::Namespace(namespace) => namespace,
            Self::Tenant(tenant) => ResourceId::new(tenant.uuid()),
            Self::Project { project, .. } => ResourceId::new(project.uuid()),
        }
    }

    /// The scope whose document applies when this one has none: a project's
    /// tenant, and nothing above a tenant.
    pub const fn fallback(self) -> Option<Self> {
        match self {
            Self::Namespace(_) | Self::Tenant(_) => None,
            Self::Project { tenant, .. } => Some(Self::Tenant(tenant)),
        }
    }
}

impl fmt::Display for PolicyScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Namespace(namespace) => write!(f, "the policy of namespace resource {namespace}"),
            Self::Tenant(tenant) => write!(f, "the policy of tenant {tenant}"),
            Self::Project { tenant, project } => {
                write!(f, "the policy of project {project} in tenant {tenant}")
            }
        }
    }
}

/// What a scope may spend, and how long an unsettled hold survives.
///
/// What is *not* here: the backend that enforces it, where it stores ledgers, and
/// what it does when it cannot be reached. Those stay in the bootstrap file (see
/// the module docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BudgetPolicy {
    subject_limit_microdollars: u64,
    namespace_limit_microdollars: Option<u64>,
    reservation_ttl_seconds: u64,
}

/// The bound a [`BudgetPolicy`] triple broke, named by the surface that read
/// it: the stored document and the admin request spell the same three settings
/// differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetBound {
    /// The cap on one `(scope, subject)` pair.
    SubjectLimit,
    /// The cap on everything the scope spends.
    NamespaceLimit,
    /// How long a reservation is held.
    ReservationTtl,
}

impl BudgetBound {
    /// The name a stored `axond.policy.v1` document spells this bound with, so a
    /// refusal an operator reads names the field they would edit.
    pub const fn document_field(self) -> &'static str {
        match self {
            Self::SubjectLimit => BUDGET_LIMIT_FIELD,
            Self::NamespaceLimit => NAMESPACE_BUDGET_LIMIT_FIELD,
            Self::ReservationTtl => RESERVATION_TTL_FIELD,
        }
    }
}

impl BudgetPolicy {
    /// A cap of zero is refused, on either scope, exactly as the bootstrap file
    /// refuses one ([`Config::validate_budget`](crate::config::Config)): it
    /// denies every request for the scope, which is a state no *cap* expresses
    /// — the section's whole content is "spending here is finite and this is the
    /// bound", and zero says the scope is closed. Closing a scope is the
    /// tenancy layer's job (remove the projection, revoke the credentials), and
    /// routing it through a limit would make a fat-fingered document indistinguishable
    /// from a deliberate fleet-wide freeze.
    pub const fn new(
        subject_limit_microdollars: u64,
        namespace_limit_microdollars: Option<u64>,
        reservation_ttl_seconds: u64,
    ) -> Result<Self, InvalidPolicy> {
        if subject_limit_microdollars == 0 {
            return Err(InvalidPolicy::TooSmall {
                value: subject_limit_microdollars,
                min: 1,
            });
        }
        if let Some(0) = namespace_limit_microdollars {
            return Err(InvalidPolicy::TooSmall { value: 0, min: 1 });
        }
        if reservation_ttl_seconds == 0 {
            return Err(InvalidPolicy::TooSmall {
                value: reservation_ttl_seconds,
                min: 1,
            });
        }
        Ok(Self {
            subject_limit_microdollars,
            namespace_limit_microdollars,
            reservation_ttl_seconds,
        })
    }

    /// Read a triple a *stored* revision carries, which is a weaker rule than
    /// the one an author is held to.
    ///
    /// A bound can tighten inside a stable schema, and this one did: a build
    /// before this one accepted a zero cap through the admin API and wrote it
    /// into a revision. Refusing to read that row would take the whole revision
    /// out of service — and, since an administrative mutation builds its
    /// candidate from the head revision, would leave no in-band way to correct
    /// the number. So a stored zero cap reads back, is reported by
    /// [`unenforceable_cap`](Self::unenforceable_cap), and is refused at
    /// activation with the field named, while the replica keeps the policy it
    /// already had. A zero reservation TTL is not in that position — no build
    /// ever accepted one — so it stays a read refusal.
    pub const fn stored(
        subject_limit_microdollars: u64,
        namespace_limit_microdollars: Option<u64>,
        reservation_ttl_seconds: u64,
    ) -> Result<Self, InvalidPolicy> {
        if reservation_ttl_seconds == 0 {
            return Err(InvalidPolicy::TooSmall {
                value: reservation_ttl_seconds,
                min: 1,
            });
        }
        Ok(Self {
            subject_limit_microdollars,
            namespace_limit_microdollars,
            reservation_ttl_seconds,
        })
    }

    /// The cap this document states as zero, if it states one.
    ///
    /// Only a document [`stored`](Self::stored) by an earlier build can be in
    /// this state; [`new`](Self::new) refuses to build one.
    pub const fn unenforceable_cap(&self) -> Option<BudgetBound> {
        if self.subject_limit_microdollars == 0 {
            Some(BudgetBound::SubjectLimit)
        } else if let Some(0) = self.namespace_limit_microdollars {
            Some(BudgetBound::NamespaceLimit)
        } else {
            None
        }
    }

    /// Which of the three bounds a rejected triple broke. Every caller of
    /// [`BudgetPolicy::new`] names a field in its refusal, and the three share
    /// one error, so the choice lives here rather than being re-derived — and
    /// re-derived differently — at each surface.
    pub const fn unmet_bound(
        subject_limit_microdollars: u64,
        namespace_limit_microdollars: Option<u64>,
    ) -> BudgetBound {
        if subject_limit_microdollars == 0 {
            BudgetBound::SubjectLimit
        } else if let Some(0) = namespace_limit_microdollars {
            BudgetBound::NamespaceLimit
        } else {
            BudgetBound::ReservationTtl
        }
    }

    /// The cap on one `(scope, subject)` pair.
    pub const fn subject_limit_microdollars(&self) -> u64 {
        self.subject_limit_microdollars
    }

    /// The cap on everything the scope spends, or `None` when there is none.
    ///
    /// `None` is a complete statement rather than an omission: this scope has no
    /// scope-wide cap. Turning it on or off changes how a shared store composes
    /// the keys a reservation touches, which is why the transition is
    /// [`TransitionClass::MigrationRequired`] rather than live.
    pub const fn namespace_limit_microdollars(&self) -> Option<u64> {
        self.namespace_limit_microdollars
    }

    pub const fn reservation_ttl_seconds(&self) -> u64 {
        self.reservation_ttl_seconds
    }
}

/// How much a scope may have in flight at once, and how long an abandoned lease
/// stays live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConcurrencyPolicy {
    max_in_flight_per_subject: u64,
    lease_ttl_seconds: u64,
}

/// The bound a [`ConcurrencyPolicy`] pair broke.
///
/// Both settings share one error, so the choice of which one to name lives here
/// rather than being re-derived — and re-derived differently — at the document
/// reader and the admin surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyBound {
    /// How many admissions one `(scope, subject)` pair may hold at once.
    MaxInFlight,
    /// How long an abandoned lease stays live.
    LeaseTtl,
}

impl ConcurrencyBound {
    /// The name an `axond.policy.v1` document spells this bound with, which is
    /// also the name the admin request spells it with.
    pub const fn document_field(self) -> &'static str {
        match self {
            Self::MaxInFlight => MAX_IN_FLIGHT_FIELD,
            Self::LeaseTtl => LEASE_TTL_FIELD,
        }
    }
}

impl ConcurrencyPolicy {
    /// Which setting a refused pair broke, so a refusal names the one the
    /// caller would edit rather than the first one checked.
    pub const fn unmet_bound(max_in_flight_per_subject: u64) -> ConcurrencyBound {
        if max_in_flight_per_subject == 0 {
            ConcurrencyBound::MaxInFlight
        } else {
            ConcurrencyBound::LeaseTtl
        }
    }

    pub const fn new(
        max_in_flight_per_subject: u64,
        lease_ttl_seconds: u64,
    ) -> Result<Self, InvalidPolicy> {
        if max_in_flight_per_subject == 0 {
            return Err(InvalidPolicy::TooSmall {
                value: max_in_flight_per_subject,
                min: 1,
            });
        }
        if lease_ttl_seconds == 0 {
            return Err(InvalidPolicy::TooSmall {
                value: lease_ttl_seconds,
                min: 1,
            });
        }
        Ok(Self {
            max_in_flight_per_subject,
            lease_ttl_seconds,
        })
    }

    pub const fn max_in_flight_per_subject(&self) -> u64 {
        self.max_in_flight_per_subject
    }

    pub const fn lease_ttl_seconds(&self) -> u64 {
        self.lease_ttl_seconds
    }
}

/// The mint epoch below which a token issued for this scope is refused.
///
/// The epoch is a Unix timestamp in seconds, compared against a minted token's
/// `iat` claim just as a bootstrap `[[gateway_token_epoch]]` entry is — not an
/// opaque counter, so `3` revokes nothing and the current Unix time revokes
/// every token issued so far.
///
/// Revocation states a floor rather than a list, so a document stays a fixed
/// shape and a mass revocation is one advancing integer. Advancing it only ever
/// refuses more, which is why it is a live change; lowering it would un-revoke
/// tokens an operator already revoked, and is refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RevocationPolicy {
    minimum_token_epoch: u64,
}

impl RevocationPolicy {
    pub const fn new(minimum_token_epoch: u64) -> Self {
        Self {
            minimum_token_epoch,
        }
    }

    pub const fn minimum_token_epoch(&self) -> u64 {
        self.minimum_token_epoch
    }
}

/// The complete policy of one tenant or project, as a revision carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyBody {
    scope: PolicyScope,
    epoch: PolicyEpoch,
    budget: BudgetPolicy,
    concurrency: ConcurrencyPolicy,
    revocation: RevocationPolicy,
    content_middleware: Vec<ContentMiddlewareRegistration>,
    buffered_response_routes: Vec<BufferedResponseRoute>,
}

impl PolicyBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = POLICY_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[
        TENANT_ID_FIELD,
        PROJECT_ID_FIELD,
        EPOCH_FIELD,
        BUDGET_LIMIT_FIELD,
        NAMESPACE_BUDGET_LIMIT_FIELD,
        RESERVATION_TTL_FIELD,
        MAX_IN_FLIGHT_FIELD,
        LEASE_TTL_FIELD,
        MINIMUM_TOKEN_EPOCH_FIELD,
        CONTENT_MIDDLEWARE_FIELD,
        BUFFERED_RESPONSE_ROUTES_FIELD,
    ];

    pub fn new(
        scope: PolicyScope,
        epoch: PolicyEpoch,
        budget: BudgetPolicy,
        concurrency: ConcurrencyPolicy,
        revocation: RevocationPolicy,
    ) -> Self {
        Self {
            scope,
            epoch,
            budget,
            concurrency,
            revocation,
            content_middleware: Vec::new(),
            buffered_response_routes: Vec::new(),
        }
    }

    /// Attach the ordered content chain this document selects.
    ///
    /// Duplicate ids are refused here rather than left to chain compilation, so
    /// authored and stored documents have the same identity and ordering rules.
    pub fn with_content_middleware(
        mut self,
        content_middleware: Vec<ContentMiddlewareRegistration>,
    ) -> Result<Self, InvalidContentMiddleware> {
        if content_middleware.len() > MAX_CONTENT_MIDDLEWARE {
            return Err(InvalidContentMiddleware::TooMany);
        }
        let mut ids = BTreeSet::new();
        for registration in &content_middleware {
            if !ids.insert(registration.id().to_owned()) {
                return Err(InvalidContentMiddleware::DuplicateId(
                    registration.id().to_owned(),
                ));
            }
        }
        self.content_middleware = content_middleware;
        Ok(self)
    }

    /// Select streaming surfaces where response-mutating middleware may trade
    /// byte-for-byte passthrough for bounded response buffering.
    pub fn with_buffered_response_routes(
        mut self,
        routes: impl IntoIterator<Item = BufferedResponseRoute>,
    ) -> Result<Self, InvalidBufferedResponseRoutes> {
        self.buffered_response_routes = normalize_buffered_response_routes(routes)?;
        Ok(self)
    }

    pub const fn scope(&self) -> PolicyScope {
        self.scope
    }

    pub const fn epoch(&self) -> PolicyEpoch {
        self.epoch
    }

    pub const fn budget(&self) -> &BudgetPolicy {
        &self.budget
    }

    pub const fn concurrency(&self) -> &ConcurrencyPolicy {
        &self.concurrency
    }

    pub const fn revocation(&self) -> &RevocationPolicy {
        &self.revocation
    }

    pub fn content_middleware(&self) -> &[ContentMiddlewareRegistration] {
        &self.content_middleware
    }

    pub fn buffered_response_routes(&self) -> &[BufferedResponseRoute] {
        &self.buffered_response_routes
    }

    /// This document's generation, once the revision that publishes it is known.
    ///
    /// A body cannot carry its own revision id — the revision does not exist
    /// until the body is in it — so a generation is formed at the seam that knows
    /// both, and never guessed at inside the body.
    pub fn generation(&self, source: RevisionId) -> PolicyGeneration {
        PolicyGeneration {
            scope: self.scope,
            epoch: self.epoch,
            source,
            content: self.content(),
        }
    }

    /// Everything a request would see, without the epoch it was published under.
    ///
    /// What distinguishes "the same document, carried into a later revision" from
    /// "a different document claiming one epoch", which is the distinction
    /// [`PolicyFence`] turns on.
    pub fn content(&self) -> PolicyContent {
        PolicyContent::of(self)
    }

    /// The resource identity this document is written under: the governed
    /// object's.
    pub const fn resource_id(&self) -> ResourceId {
        self.scope.resource_id()
    }

    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    pub fn version(&self, slug: Slug) -> ResourceVersion {
        self.version_at(slug, ResourceVersionNumber::FIRST)
    }

    pub fn version_at(&self, slug: Slug, version: ResourceVersionNumber) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Policy, self.resource_id(), version),
            self.scope.resource_scope(),
            slug,
            self.body(),
        )
    }

    /// Read a policy resource's body, binding it to its envelope: identity to the
    /// reference, governed scope to the envelope's scope.
    pub fn read(resource: &ResourceVersion) -> Result<Self, PolicyError> {
        let record = Record::<PolicyError>::open_reserving(
            resource,
            ResourceKind::Policy,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
            BOOTSTRAP_OWNED_FIELDS,
        )?;
        let tenant = record.tenant()?;
        let scope = match record.optional_project()? {
            Some(project) => PolicyScope::Project { tenant, project },
            None => PolicyScope::Tenant(tenant),
        };
        record.identity(scope, scope.resource_id())?;
        if resource.scope != scope.resource_scope() {
            return Err(PolicyError::ScopeMismatch {
                reference: resource.reference,
                declared: scope,
                scoped: resource.scope.clone(),
            });
        }
        let bound = |field: &'static str, source: InvalidPolicy| PolicyError::FieldRange {
            reference: resource.reference,
            field,
            source,
        };
        let epoch = PolicyEpoch::new(record.integer(EPOCH_FIELD)?)
            .map_err(|source| bound(EPOCH_FIELD, source))?;
        let subject_limit = record.integer(BUDGET_LIMIT_FIELD)?;
        let namespace_limit = record.optional_integer(NAMESPACE_BUDGET_LIMIT_FIELD)?;
        let reservation_ttl = record.integer(RESERVATION_TTL_FIELD)?;
        // `stored`, not `new`: a zero cap an earlier build wrote reads back and
        // is refused at activation, so one bad number does not take the whole
        // revision — and the correction that would replace it — out of service.
        let budget = BudgetPolicy::stored(subject_limit, namespace_limit, reservation_ttl)
            .map_err(|source| bound(RESERVATION_TTL_FIELD, source))?;
        let max_in_flight = record.integer(MAX_IN_FLIGHT_FIELD)?;
        let lease_ttl = record.integer(LEASE_TTL_FIELD)?;
        let concurrency = ConcurrencyPolicy::new(max_in_flight, lease_ttl).map_err(|source| {
            // Two fields share one bound, so the refusal names the one that broke
            // it rather than the first one checked.
            bound(
                ConcurrencyPolicy::unmet_bound(max_in_flight).document_field(),
                source,
            )
        })?;
        let content_middleware = read_content_middleware(&record)?;
        let buffered_response_routes = read_buffered_response_routes(&record)?;
        Self {
            scope,
            epoch,
            budget,
            concurrency,
            revocation: RevocationPolicy::new(record.integer(MINIMUM_TOKEN_EPOCH_FIELD)?),
            content_middleware: Vec::new(),
            buffered_response_routes: Vec::new(),
        }
        .with_content_middleware(content_middleware)
        .map_err(|source| PolicyError::InvalidMiddleware {
            reference: resource.reference,
            field: CONTENT_MIDDLEWARE_FIELD.to_owned(),
            source,
        })
        .and_then(|body| {
            body.with_buffered_response_routes(buffered_response_routes)
                .map_err(|source| PolicyError::InvalidBufferedResponseRoutes {
                    reference: resource.reference,
                    field: BUFFERED_RESPONSE_ROUTES_FIELD,
                    source,
                })
        })
    }

    /// How a fleet may move from this document to `next`.
    ///
    /// The classification is a property of the two documents, so publication,
    /// review tooling, and a later activation slice reach the same answer.
    pub fn transition(&self, next: &Self) -> PolicyTransition {
        PolicyTransition::between(self, next)
    }

    /// How a fleet may move from this document to `next` when `next` *displaces*
    /// it for a namespace rather than succeeding it for a scope: a project
    /// publishing its own document over the tenant's, or dropping it again.
    ///
    /// Only the draining reasons. The two documents belong to different scopes,
    /// so neither the scope nor the epoch comparison means anything here — an
    /// epoch orders one scope's own publications, and a project's first epoch is
    /// not behind its tenant's tenth. What does carry over is the values: a
    /// namespace whose binding cap is cut still has holds that were admitted
    /// under the wider one.
    pub fn displaced_by(&self, next: &Self) -> PolicyTransition {
        PolicyTransition::displacing(self, next)
    }

    /// Whether two documents state the same policy, ignoring the epoch they were
    /// published under.
    ///
    /// The epoch is publication metadata, so a republication that advances only
    /// it changes nothing a request would see.
    fn same_content(&self, other: &Self) -> bool {
        self.content() == other.content()
    }
}

impl Canonical for PolicyBody {
    fn canonical(&self) -> CanonicalValue {
        // Absent rather than zero for an optional field: the canonical encoding
        // has no null, so "no scope-wide cap" is the absence of the key, and a
        // zero cap would mean a cap of zero.
        let mut fields = vec![
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
            (EPOCH_FIELD, CanonicalValue::integer(self.epoch.get())),
            (
                BUDGET_LIMIT_FIELD,
                CanonicalValue::integer(self.budget.subject_limit_microdollars),
            ),
            (
                RESERVATION_TTL_FIELD,
                CanonicalValue::integer(self.budget.reservation_ttl_seconds),
            ),
            (
                MAX_IN_FLIGHT_FIELD,
                CanonicalValue::integer(self.concurrency.max_in_flight_per_subject),
            ),
            (
                LEASE_TTL_FIELD,
                CanonicalValue::integer(self.concurrency.lease_ttl_seconds),
            ),
            (
                MINIMUM_TOKEN_EPOCH_FIELD,
                CanonicalValue::integer(self.revocation.minimum_token_epoch),
            ),
        ];
        match self.scope {
            PolicyScope::Namespace(namespace) => fields.push((
                "namespace_resource_id",
                CanonicalValue::string(namespace.to_string()),
            )),
            PolicyScope::Tenant(tenant) | PolicyScope::Project { tenant, .. } => {
                fields.push((TENANT_ID_FIELD, CanonicalValue::string(tenant.to_string())))
            }
        }
        if let Some(project) = self.scope.project() {
            fields.push((
                PROJECT_ID_FIELD,
                CanonicalValue::string(project.to_string()),
            ));
        }
        if let Some(limit) = self.budget.namespace_limit_microdollars {
            fields.push((NAMESPACE_BUDGET_LIMIT_FIELD, CanonicalValue::integer(limit)));
        }
        if !self.content_middleware.is_empty() {
            fields.push((
                CONTENT_MIDDLEWARE_FIELD,
                CanonicalValue::List(
                    self.content_middleware
                        .iter()
                        .map(|registration| {
                            let mut fields = vec![
                                (
                                    MIDDLEWARE_ID_FIELD,
                                    CanonicalValue::string(registration.id()),
                                ),
                                (
                                    MIDDLEWARE_SCOPES_FIELD,
                                    CanonicalValue::set(registration.scopes().iter().map(
                                        |scope| {
                                            CanonicalValue::string(middleware_scope_name(*scope))
                                        },
                                    )),
                                ),
                                (
                                    MIDDLEWARE_FAILURE_POSTURE_FIELD,
                                    CanonicalValue::string(middleware_failure_posture_name(
                                        registration.failure_posture(),
                                    )),
                                ),
                                (
                                    MIDDLEWARE_MAX_DURATION_FIELD,
                                    CanonicalValue::integer(
                                        registration.max_duration_milliseconds(),
                                    ),
                                ),
                            ];
                            if let Some(guardrail) = registration.guardrail() {
                                fields.push((
                                    MIDDLEWARE_GUARDRAIL_FIELD,
                                    CanonicalValue::map([
                                        (
                                            GUARDRAIL_KEY_ENV_FIELD,
                                            CanonicalValue::string(guardrail.key_env()),
                                        ),
                                        (
                                            GUARDRAIL_RULES_FIELD,
                                            CanonicalValue::List(
                                                guardrail
                                                    .rules()
                                                    .iter()
                                                    .map(|rule| {
                                                        CanonicalValue::map([
                                                            (
                                                                GUARDRAIL_RULE_ID_FIELD,
                                                                CanonicalValue::string(&rule.id),
                                                            ),
                                                            (
                                                                GUARDRAIL_RULE_PATTERN_FIELD,
                                                                CanonicalValue::string(
                                                                    &rule.pattern,
                                                                ),
                                                            ),
                                                            (
                                                                GUARDRAIL_RULE_ACTION_FIELD,
                                                                CanonicalValue::string(
                                                                    guardrail_action_name(
                                                                        rule.action,
                                                                    ),
                                                                ),
                                                            ),
                                                        ])
                                                    })
                                                    .collect(),
                                            ),
                                        ),
                                    ]),
                                ));
                            }
                            CanonicalValue::map(fields)
                        })
                        .collect(),
                ),
            ));
        }
        if !self.buffered_response_routes.is_empty() {
            fields.push((
                BUFFERED_RESPONSE_ROUTES_FIELD,
                CanonicalValue::set(
                    self.buffered_response_routes
                        .iter()
                        .map(|route| CanonicalValue::string(route.as_str())),
                ),
            ));
        }
        CanonicalValue::map(fields)
    }
}

fn read_buffered_response_routes(
    record: &Record<'_, PolicyError>,
) -> Result<Vec<BufferedResponseRoute>, PolicyError> {
    let Some(value) = record.optional_value(BUFFERED_RESPONSE_ROUTES_FIELD) else {
        return Ok(Vec::new());
    };
    let CanonicalValue::Set(values) = value else {
        return Err(PolicyError::FieldType {
            reference: record.reference(),
            field: BUFFERED_RESPONSE_ROUTES_FIELD,
        });
    };
    let routes = values
        .iter()
        .map(|value| {
            let CanonicalValue::String(value) = value else {
                return Err(PolicyError::FieldType {
                    reference: record.reference(),
                    field: BUFFERED_RESPONSE_ROUTES_FIELD,
                });
            };
            BufferedResponseRoute::parse(value).map_err(|source| {
                PolicyError::InvalidBufferedResponseRoutes {
                    reference: record.reference(),
                    field: BUFFERED_RESPONSE_ROUTES_FIELD,
                    source,
                }
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    normalize_buffered_response_routes(routes).map_err(|source| {
        PolicyError::InvalidBufferedResponseRoutes {
            reference: record.reference(),
            field: BUFFERED_RESPONSE_ROUTES_FIELD,
            source,
        }
    })
}

fn read_content_middleware(
    record: &Record<'_, PolicyError>,
) -> Result<Vec<ContentMiddlewareRegistration>, PolicyError> {
    let Some(value) = record.optional_value(CONTENT_MIDDLEWARE_FIELD) else {
        return Ok(Vec::new());
    };
    let CanonicalValue::List(values) = value else {
        return Err(PolicyError::FieldType {
            reference: record.reference(),
            field: CONTENT_MIDDLEWARE_FIELD,
        });
    };
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let entry = record.sub_record(
                value,
                CONTENT_MIDDLEWARE_FIELD,
                POLICY_SCHEMA,
                CONTENT_MIDDLEWARE_FIELDS,
            )?;
            let scopes = entry
                .string_set(MIDDLEWARE_SCOPES_FIELD)?
                .into_iter()
                .map(parse_middleware_scope)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|source| PolicyError::InvalidMiddleware {
                    reference: record.reference(),
                    field: format!("{CONTENT_MIDDLEWARE_FIELD}[{index}].{MIDDLEWARE_SCOPES_FIELD}"),
                    source,
                })?;
            let failure_posture =
                parse_middleware_failure_posture(entry.string(MIDDLEWARE_FAILURE_POSTURE_FIELD)?)
                    .map_err(|source| PolicyError::InvalidMiddleware {
                    reference: record.reference(),
                    field: format!(
                        "{CONTENT_MIDDLEWARE_FIELD}[{index}].{MIDDLEWARE_FAILURE_POSTURE_FIELD}"
                    ),
                    source,
                })?;
            let registration = ContentMiddlewareRegistration::new(
                entry.string(MIDDLEWARE_ID_FIELD)?,
                scopes,
                failure_posture,
                entry.integer(MIDDLEWARE_MAX_DURATION_FIELD)?,
            )
            .map_err(|source| PolicyError::InvalidMiddleware {
                reference: record.reference(),
                field: format!("{CONTENT_MIDDLEWARE_FIELD}[{index}]"),
                source,
            })?;
            let Some(value) = entry.optional_value(MIDDLEWARE_GUARDRAIL_FIELD) else {
                return Ok(registration);
            };
            let guardrail = entry.sub_record(
                value,
                MIDDLEWARE_GUARDRAIL_FIELD,
                POLICY_SCHEMA,
                GUARDRAIL_FIELDS,
            )?;
            let CanonicalValue::List(rule_values) = guardrail.value(GUARDRAIL_RULES_FIELD)? else {
                return Err(PolicyError::FieldType {
                    reference: record.reference(),
                    field: GUARDRAIL_RULES_FIELD,
                });
            };
            let rules = rule_values
                .iter()
                .map(|value| {
                    let rule = guardrail.sub_record(
                        value,
                        GUARDRAIL_RULES_FIELD,
                        POLICY_SCHEMA,
                        GUARDRAIL_RULE_FIELDS,
                    )?;
                    Ok(GuardrailRule {
                        id: rule.string(GUARDRAIL_RULE_ID_FIELD)?.to_owned(),
                        pattern: rule.string(GUARDRAIL_RULE_PATTERN_FIELD)?.to_owned(),
                        action: parse_guardrail_action(rule.string(GUARDRAIL_RULE_ACTION_FIELD)?)
                            .map_err(|source| PolicyError::InvalidMiddleware {
                            reference: record.reference(),
                            field: GUARDRAIL_RULE_ACTION_FIELD.to_owned(),
                            source,
                        })?,
                    })
                })
                .collect::<Result<Vec<_>, PolicyError>>()?;
            let guardrail = ContentGuardrailRegistration::new(
                guardrail.string(GUARDRAIL_KEY_ENV_FIELD)?,
                rules,
            )
            .map_err(|source| PolicyError::InvalidMiddleware {
                reference: record.reference(),
                field: MIDDLEWARE_GUARDRAIL_FIELD.to_owned(),
                source,
            })?;
            registration.with_guardrail(guardrail).map_err(|source| {
                PolicyError::InvalidMiddleware {
                    reference: record.reference(),
                    field: MIDDLEWARE_GUARDRAIL_FIELD.to_owned(),
                    source,
                }
            })
        })
        .collect()
}

/// What a policy document states, digested: the scope it governs and every value
/// it enforces, with no reference to when it was published.
///
/// A revision is whole desired state, so every revision restates every policy
/// document it carries, whether or not that document changed. Comparing content
/// is what lets a carried-forward document be recognized as the same policy
/// rather than as a second one claiming its epoch, and a digest is what lets a
/// [`PolicyGeneration`] carry that comparison without carrying a document.
///
/// Fixed-width and total: every field is length-prefixed or fixed-width, and an
/// absent optional cap is distinguished from any value it could hold, so two
/// documents digest alike only when they state the same policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyContent(Checksum);

impl PolicyContent {
    fn of(body: &PolicyBody) -> Self {
        let mut bytes = Vec::new();
        let (kind, owner, child) = match body.scope {
            PolicyScope::Namespace(namespace) => (0_u8, namespace.to_string(), String::new()),
            PolicyScope::Tenant(tenant) => (1, tenant.to_string(), String::new()),
            PolicyScope::Project { tenant, project } => {
                (2, tenant.to_string(), project.to_string())
            }
        };
        bytes.push(kind);
        for text in [owner, child] {
            bytes.extend_from_slice(&(text.len() as u64).to_be_bytes());
            bytes.extend_from_slice(text.as_bytes());
        }
        for number in [
            body.budget.subject_limit_microdollars,
            u64::from(body.budget.namespace_limit_microdollars.is_some()),
            body.budget.namespace_limit_microdollars.unwrap_or(0),
            body.budget.reservation_ttl_seconds,
            body.concurrency.max_in_flight_per_subject,
            body.concurrency.lease_ttl_seconds,
            body.revocation.minimum_token_epoch,
        ] {
            bytes.extend_from_slice(&number.to_be_bytes());
        }
        bytes.extend_from_slice(&(body.content_middleware.len() as u64).to_be_bytes());
        for registration in &body.content_middleware {
            bytes.extend_from_slice(&(registration.id.len() as u64).to_be_bytes());
            bytes.extend_from_slice(registration.id.as_bytes());
            bytes.extend_from_slice(&(registration.scopes.len() as u64).to_be_bytes());
            for scope in &registration.scopes {
                bytes.push(middleware_scope_rank(*scope));
            }
            bytes.push(match registration.failure_posture {
                MiddlewareFailurePosture::FailOpen => 0,
                MiddlewareFailurePosture::FailClosed => 1,
            });
            bytes.extend_from_slice(&registration.max_duration_milliseconds.to_be_bytes());
            match &registration.guardrail {
                Some(guardrail) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&(guardrail.key_env.len() as u64).to_be_bytes());
                    bytes.extend_from_slice(guardrail.key_env.as_bytes());
                    bytes.extend_from_slice(&(guardrail.rules.len() as u64).to_be_bytes());
                    for rule in &guardrail.rules {
                        for text in [&rule.id, &rule.pattern] {
                            bytes.extend_from_slice(&(text.len() as u64).to_be_bytes());
                            bytes.extend_from_slice(text.as_bytes());
                        }
                        bytes.push(match rule.action {
                            GuardrailAction::Block => 0,
                            GuardrailAction::Redact => 1,
                        });
                    }
                }
                None => bytes.push(0),
            }
        }
        bytes.extend_from_slice(&(body.buffered_response_routes.len() as u64).to_be_bytes());
        for route in &body.buffered_response_routes {
            bytes.push(match route {
                BufferedResponseRoute::Messages => 0,
                BufferedResponseRoute::Responses => 1,
            });
        }
        Self(Checksum::of(&bytes))
    }

    /// The digest itself, for a caller that stores or logs it.
    pub const fn digest(&self) -> Checksum {
        self.0
    }
}

impl fmt::Display for PolicyContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The identity of one published policy document: the scope it governs, the
/// epoch its content was published under, the policy that content *is*, and the
/// revision that carried it.
///
/// No part identifies a generation alone — see the module docs — so the four are
/// one type rather than fields a caller could compare separately. The scope is
/// what the epoch counts within: epochs are monotonic per scope and unrelated
/// across scopes, so a generation without its scope would compare as ordered
/// against a document governing something else. The revision is provenance: it
/// says which publication a writer read, and is what makes two documents claiming
/// one epoch distinguishable. The content is what says whether those two
/// documents are the same policy or a fork.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PolicyGeneration {
    scope: PolicyScope,
    epoch: PolicyEpoch,
    source: RevisionId,
    content: PolicyContent,
}

impl PolicyGeneration {
    pub const fn new(
        scope: PolicyScope,
        epoch: PolicyEpoch,
        source: RevisionId,
        content: PolicyContent,
    ) -> Self {
        Self {
            scope,
            epoch,
            source,
            content,
        }
    }

    /// The tenant or project whose policy this generation is.
    pub const fn scope(&self) -> PolicyScope {
        self.scope
    }

    pub const fn epoch(&self) -> PolicyEpoch {
        self.epoch
    }

    /// The revision that published this document.
    pub const fn source(&self) -> RevisionId {
        self.source
    }

    /// The policy this generation enforces, digested.
    pub const fn content(&self) -> PolicyContent {
        self.content
    }

    /// Whether this generation's content was published after `other`'s.
    ///
    /// Within one scope, and strictly by epoch: two generations of one epoch
    /// stating *different* policies are not ordered, they are a fork, and
    /// [`PolicyFence`] refuses one rather than picking a winner. Two generations
    /// of *different* scopes are not ordered either — an epoch counts within the
    /// scope that published it, so a higher one from another tenant says nothing
    /// about this one.
    pub fn supersedes(&self, other: &Self) -> bool {
        self.scope == other.scope && self.epoch > other.epoch
    }

    /// Whether both generations enforce the same policy under the same epoch,
    /// whichever revision carried them.
    ///
    /// True for a document restated verbatim by a later revision — the ordinary
    /// case, since a revision that changes an unrelated resource still republishes
    /// every policy document — and false for two different documents sharing an
    /// epoch, which is the fork a fence must refuse.
    pub fn same_policy(&self, other: &Self) -> bool {
        self.scope == other.scope && self.epoch == other.epoch && self.content == other.content
    }

    /// Whether this generation is `other` carried into a different revision:
    /// the same policy under the same epoch, published again.
    pub fn carries_forward(&self, other: &Self) -> bool {
        self.same_policy(other) && self.source != other.source
    }
}

impl fmt::Display for PolicyGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "epoch {} of {} from revision {}",
            self.epoch, self.scope, self.source
        )
    }
}

/// A generation a fence was offered, beside the generation it enforces.
///
/// One type for both refusals, and boxed in each, because a generation names its
/// scope and digests its content: two of them inline would pad out every `Result`
/// a fenced call site returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Offered {
    /// The generation offered: a writer's, or one to adopt.
    pub offered: PolicyGeneration,
    /// The generation the fence enforces.
    pub active: PolicyGeneration,
}

impl Offered {
    pub const fn new(offered: PolicyGeneration, active: PolicyGeneration) -> Self {
        Self { offered, active }
    }
}

/// Why a writer was fenced out.
///
/// Several arms rather than one, because they are different operational stories,
/// and only the first is the ordinary one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Fenced {
    /// The ordinary case: the writer holds a generation the fleet has moved past.
    #[error("writer holds {}, which the active {} has moved past", .0.offered, .0.active)]
    Stale(Box<Offered>),
    /// A writer claiming a generation this replica has not adopted. Refused
    /// rather than trusted: a writer that may enforce anything it can claim is
    /// newer than the active generation is not fenced at all.
    #[error("writer holds {}, which is ahead of the active {}", .0.offered, .0.active)]
    Ahead(Box<Offered>),
    /// The same epoch stating a *different* policy: two publications claiming one
    /// generation, which a restored backup or a forked control plane produces. A
    /// revision that merely restates the active document is not this — see
    /// [`PolicyGeneration::carries_forward`].
    #[error("writer holds {}, which claims the epoch of the active {}", .0.offered, .0.active)]
    Forked(Box<Offered>),
    /// A generation governing something else entirely: a fence enforces one
    /// scope's policy, and a writer holding another scope's document is not late
    /// or early but wired wrong.
    #[error(
        "writer holds {}, which is not the policy the active {} enforces",
        .0.offered,
        .0.active
    )]
    OtherScope(Box<Offered>),
}

impl Fenced {
    /// The generation the fenced-out writer held.
    pub fn writer(&self) -> PolicyGeneration {
        self.offered().offered
    }

    /// The generation the fence enforces.
    pub fn active(&self) -> PolicyGeneration {
        self.offered().active
    }

    fn offered(&self) -> &Offered {
        match self {
            Self::Stale(offered)
            | Self::Ahead(offered)
            | Self::Forked(offered)
            | Self::OtherScope(offered) => offered,
        }
    }
}

/// Why a fence refused to move.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("cannot adopt {} over the active {}", .0.offered, .0.active)]
pub struct NotAnAdvance(pub Box<Offered>);

impl NotAnAdvance {
    /// The generation that was not an advance.
    pub fn next(&self) -> PolicyGeneration {
        self.0.offered
    }

    /// The generation the fence enforces, and kept.
    pub fn active(&self) -> PolicyGeneration {
        self.0.active
    }
}

/// The generation a scope's policy is currently enforced under, and the gate every
/// writer passes.
///
/// Fail-closed by construction: [`admit`](Self::admit) returns `Ok` only for the
/// policy the fence is actively enforcing, so an older, forked, or newer
/// generation denies. Nothing here writes anything — it is the contract a later
/// activation slice enforces with.
///
/// One scope's, throughout: a generation names the scope it governs, so a fence
/// holding a tenant's policy refuses a project's document rather than adopting it
/// on a higher epoch and then denying every writer the scope actually has.
///
/// "The policy it is enforcing" rather than "the revision it came from": a
/// revision restates every policy document it carries, so a revision that changed
/// an unrelated resource hands out a generation with a new
/// [`source`](PolicyGeneration::source) for an unchanged document. Admitting and
/// adopting compare the epoch and the content, and treat the revision as
/// provenance — otherwise the ordinary carry-forward would read as a fork, and a
/// replica could never follow the fleet onto the revision it is serving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyFence {
    active: PolicyGeneration,
}

impl PolicyFence {
    pub const fn new(active: PolicyGeneration) -> Self {
        Self { active }
    }

    pub const fn active(&self) -> PolicyGeneration {
        self.active
    }

    /// Admit a writer holding `writer`, or say why it is fenced out.
    ///
    /// Admits the active generation, and the same policy under the same epoch from
    /// another revision — the same document, carried forward. Everything else
    /// denies, including a generation this replica has not adopted and one
    /// governing another scope.
    pub fn admit(&self, writer: PolicyGeneration) -> Result<(), Fenced> {
        if writer.same_policy(&self.active) {
            return Ok(());
        }
        let offered = Box::new(Offered::new(writer, self.active));
        Err(if writer.scope != self.active.scope {
            Fenced::OtherScope(offered)
        } else if writer.epoch < self.active.epoch {
            Fenced::Stale(offered)
        } else if writer.epoch > self.active.epoch {
            Fenced::Ahead(offered)
        } else {
            Fenced::Forked(offered)
        })
    }

    /// Move the fence onto a generation that supersedes the active one, or onto
    /// the active document as a later revision restates it.
    ///
    /// A generation that neither advances the epoch nor carries the active
    /// document forward is refused, so adopting is monotonic in what is
    /// *enforced*: a replica cannot be walked backwards onto an older policy, nor
    /// sideways onto a different policy claiming the active epoch, nor onto
    /// another scope's document however high its epoch, but it can follow the
    /// fleet onto the revision now serving the same policy.
    pub fn adopt(&mut self, next: PolicyGeneration) -> Result<(), NotAnAdvance> {
        if !next.supersedes(&self.active) && !next.carries_forward(&self.active) {
            return Err(NotAnAdvance(Box::new(Offered::new(next, self.active))));
        }
        self.active = next;
        Ok(())
    }
}

/// What activating a policy change requires of a fleet.
///
/// Ordered by severity, so the class of a change with several effects is the
/// maximum of theirs: a publication is as disruptive as its worst field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransitionClass {
    /// Safe to enforce on the next request. Nothing already admitted becomes
    /// invalid.
    Live,
    /// Safe once what was admitted under the old document has finished: holds and
    /// leases granted under a looser policy are honoured, and the new one binds
    /// from the next admission.
    Drain,
    /// Enforcement changes shape, not only its numbers: durable state laid out
    /// for the old document has to be reconciled before the new one is enforced.
    MigrationRequired,
    /// Not a transition this model performs. The publication is refused rather
    /// than applied.
    Refused,
}

impl TransitionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::Drain => "drain",
            Self::MigrationRequired => "migration-required",
            Self::Refused => "refused",
        }
    }
}

impl fmt::Display for TransitionClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One reason a transition is classified the way it is.
///
/// A change is described by every reason that applies, so an operator reads *why*
/// a publication needs a drain rather than only that it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TransitionReason {
    /// A document for a different tenant or project. Not a transition of this
    /// document at all.
    ScopeChanged,
    /// The epoch went backwards.
    EpochRegressed,
    /// The content changed without advancing the epoch, which would leave two
    /// documents claiming one generation and make the fence unable to tell a
    /// stale writer from a current one.
    EpochNotAdvanced,
    /// The epoch advanced with no change to what is enforced.
    Republished,
    BudgetRaised,
    /// A lower cap: what is already held was admitted under the higher one.
    BudgetLowered,
    /// A scope-wide cap where there was none: a shared store starts composing
    /// every reservation over a second key, so existing ledgers have to be
    /// reconciled before the cap means anything.
    ScopeCapEnabled,
    /// A scope-wide cap removed: reservations stop touching the composite key,
    /// and the ledgers it accumulated are left behind.
    ScopeCapDisabled,
    ScopeCapRaised,
    ScopeCapLowered,
    ReservationTtlExtended,
    /// A shorter hold lifetime: holds taken under the longer one would be
    /// reclaimed before the request that owns them settles.
    ReservationTtlShortened,
    ConcurrencyRaised,
    /// A lower ceiling: leases already granted exceed it.
    ConcurrencyLowered,
    LeaseTtlExtended,
    /// A shorter lease lifetime: leases held under the longer one would be
    /// reclaimed while their requests are still in flight.
    LeaseTtlShortened,
    /// The token floor rose: more tokens are refused, which is the fail-closed
    /// direction.
    TokenFloorRaised,
    /// The token floor fell, which would restore tokens an operator revoked.
    TokenFloorLowered,
    /// The ordered content chain changed. Existing requests retain the snapshot
    /// they started under; the new chain binds from the next request.
    ContentMiddlewareChanged,
    /// The set of streaming surfaces explicitly allowed to buffer for response
    /// mutation changed. Existing requests retain their captured policy.
    BufferedResponseRoutesChanged,
}

impl TransitionReason {
    /// The class this reason alone implies.
    pub const fn class(self) -> TransitionClass {
        match self {
            Self::ScopeChanged
            | Self::EpochRegressed
            | Self::EpochNotAdvanced
            | Self::TokenFloorLowered => TransitionClass::Refused,
            Self::ScopeCapEnabled | Self::ScopeCapDisabled => TransitionClass::MigrationRequired,
            Self::BudgetLowered
            | Self::ScopeCapLowered
            | Self::ReservationTtlShortened
            | Self::ConcurrencyLowered
            | Self::LeaseTtlShortened => TransitionClass::Drain,
            Self::Republished
            | Self::BudgetRaised
            | Self::ScopeCapRaised
            | Self::ReservationTtlExtended
            | Self::ConcurrencyRaised
            | Self::LeaseTtlExtended
            | Self::TokenFloorRaised
            | Self::ContentMiddlewareChanged
            | Self::BufferedResponseRoutesChanged => TransitionClass::Live,
        }
    }
}

/// How a fleet may move from one policy document to the next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyTransition {
    class: TransitionClass,
    reasons: Vec<TransitionReason>,
}

impl PolicyTransition {
    /// Classify the move from `from` to `to`.
    ///
    /// The refusing reasons are decided first and short-circuit the rest: a
    /// document for another scope, or one whose epoch does not carry its change,
    /// is not a document to compare field by field.
    fn between(from: &PolicyBody, to: &PolicyBody) -> Self {
        if from.scope != to.scope {
            return Self::of(vec![TransitionReason::ScopeChanged]);
        }
        if to.epoch < from.epoch {
            return Self::of(vec![TransitionReason::EpochRegressed]);
        }
        if to.epoch == from.epoch {
            return if from.same_content(to) {
                // The same document twice: no change to classify.
                Self::of(Vec::new())
            } else {
                Self::of(vec![TransitionReason::EpochNotAdvanced])
            };
        }
        if from.same_content(to) {
            return Self::of(vec![TransitionReason::Republished]);
        }
        Self::of(Self::fields(from, to))
    }

    /// See [`PolicyBody::displaced_by`]: the same field comparison, dropping
    /// only the reasons a handover cannot be judged by.
    ///
    /// Live changes go, because they strand nothing and the activation handover
    /// surface reports only draining reasons. Everything that constrains the move
    /// stays: a drain is still a drain when a different scope's document imposes
    /// it, and a refusing value — a token floor that falls — is still restoring
    /// tokens an operator revoked, whichever document lowers it.
    fn displacing(from: &PolicyBody, to: &PolicyBody) -> Self {
        Self::of(
            Self::fields(from, to)
                .into_iter()
                .filter(|reason| reason.class() != TransitionClass::Live)
                .collect(),
        )
    }

    /// Every reason the two documents' values differ, ignoring scope and epoch.
    fn fields(from: &PolicyBody, to: &PolicyBody) -> Vec<TransitionReason> {
        let mut reasons = Vec::new();
        let (old, new) = (&from.budget, &to.budget);
        push_ordered(
            &mut reasons,
            old.subject_limit_microdollars,
            new.subject_limit_microdollars,
            TransitionReason::BudgetRaised,
            TransitionReason::BudgetLowered,
        );
        match (
            old.namespace_limit_microdollars,
            new.namespace_limit_microdollars,
        ) {
            (None, Some(_)) => reasons.push(TransitionReason::ScopeCapEnabled),
            (Some(_), None) => reasons.push(TransitionReason::ScopeCapDisabled),
            (Some(old), Some(new)) => push_ordered(
                &mut reasons,
                old,
                new,
                TransitionReason::ScopeCapRaised,
                TransitionReason::ScopeCapLowered,
            ),
            (None, None) => {}
        }
        push_ordered(
            &mut reasons,
            old.reservation_ttl_seconds,
            new.reservation_ttl_seconds,
            TransitionReason::ReservationTtlExtended,
            TransitionReason::ReservationTtlShortened,
        );
        let (old, new) = (&from.concurrency, &to.concurrency);
        push_ordered(
            &mut reasons,
            old.max_in_flight_per_subject,
            new.max_in_flight_per_subject,
            TransitionReason::ConcurrencyRaised,
            TransitionReason::ConcurrencyLowered,
        );
        push_ordered(
            &mut reasons,
            old.lease_ttl_seconds,
            new.lease_ttl_seconds,
            TransitionReason::LeaseTtlExtended,
            TransitionReason::LeaseTtlShortened,
        );
        push_ordered(
            &mut reasons,
            from.revocation.minimum_token_epoch,
            to.revocation.minimum_token_epoch,
            TransitionReason::TokenFloorRaised,
            TransitionReason::TokenFloorLowered,
        );
        if from.content_middleware != to.content_middleware {
            reasons.push(TransitionReason::ContentMiddlewareChanged);
        }
        if from.buffered_response_routes != to.buffered_response_routes {
            reasons.push(TransitionReason::BufferedResponseRoutesChanged);
        }
        reasons
    }

    fn of(mut reasons: Vec<TransitionReason>) -> Self {
        reasons.sort_unstable();
        reasons.dedup();
        let class = reasons
            .iter()
            .map(|reason| reason.class())
            .max()
            .unwrap_or(TransitionClass::Live);
        Self { class, reasons }
    }

    /// The class of the whole change: the most disruptive of its reasons.
    pub const fn class(&self) -> TransitionClass {
        self.class
    }

    /// Every reason that applies, ordered so two replicas report one change the
    /// same way.
    pub fn reasons(&self) -> &[TransitionReason] {
        &self.reasons
    }

    /// Whether this change may be enforced on the next request.
    pub fn is_live(&self) -> bool {
        self.class == TransitionClass::Live
    }

    /// Whether this change is refused rather than applied.
    pub fn is_refused(&self) -> bool {
        self.class == TransitionClass::Refused
    }
}

fn push_ordered(
    reasons: &mut Vec<TransitionReason>,
    old: u64,
    new: u64,
    raised: TransitionReason,
    lowered: TransitionReason,
) {
    if new > old {
        reasons.push(raised);
    } else if new < old {
        reasons.push(lowered);
    }
}

/// A policy document as a revision holds it: its envelope, its name, and its
/// body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDocument {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: PolicyBody,
}

/// Every policy document of one revision, resolved once.
///
/// Built by [`PolicySet::of`], which is the single place policy bodies are
/// interpreted, so publication, hydration, and any later projection reach the
/// same conclusions. Ordering is by scope throughout, so two replicas iterate the
/// same documents in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PolicySet {
    documents: BTreeMap<PolicyScope, PolicyDocument>,
}

impl PolicySet {
    /// Read every policy body in a desired state.
    ///
    /// What is checked here is what no envelope-level rule can see: that each
    /// body is one this build reads, and that it is bound to its envelope —
    /// identity to the reference, governed scope to the envelope's scope.
    ///
    /// What is deliberately *not* checked is that a document's tenant or project
    /// row exists in the same revision. Tenancy already refuses a project-scoped
    /// resource that contradicts a declared project, and requiring the row itself
    /// would add a rule to revisions already published under the older one — the
    /// same reasoning [`Tenancy::of`](super::tenancy::Tenancy::of) states. A
    /// document whose scope names nothing is unroutable at the boundary that
    /// routes, not unreadable here.
    pub fn of(state: &DesiredState) -> Result<Self, PolicyError> {
        let mut set = Self::default();
        for resource in state.resources() {
            if resource.reference.kind != ResourceKind::Policy {
                continue;
            }
            let body = PolicyBody::read(resource)?;
            set.documents.insert(
                body.scope(),
                PolicyDocument {
                    reference: resource.reference,
                    slug: resource.slug.clone(),
                    body,
                },
            );
        }
        Ok(set)
    }

    /// Every document, ordered by scope.
    pub fn documents(&self) -> impl ExactSizeIterator<Item = &PolicyDocument> {
        self.documents.values()
    }

    /// The document published *for* this scope, if any.
    pub fn document(&self, scope: PolicyScope) -> Option<&PolicyDocument> {
        self.documents.get(&scope)
    }

    /// The complete document that governs `scope`: its own, or its tenant's.
    ///
    /// Whole-document selection, never a field-by-field merge — so what governs a
    /// project is always a document an operator published as a unit. A scope with
    /// neither has no published policy, and the bootstrap file's limits stand.
    pub fn effective(&self, scope: PolicyScope) -> Option<&PolicyDocument> {
        self.documents
            .get(&scope)
            .or_else(|| self.documents.get(&scope.fallback()?))
    }

    /// This set as the revision `source` published it.
    pub fn snapshot(&self, source: RevisionId) -> PolicySnapshot {
        PolicySnapshot {
            source,
            documents: self
                .documents
                .iter()
                .map(|(scope, document)| (*scope, document.body.clone()))
                .collect(),
        }
    }
}

/// The policy of one revision, with the generation each document is enforced
/// under.
///
/// A snapshot is what a later activation slice holds: complete documents, an
/// explicit generation per scope, and a fence per scope. It is built from a
/// revision that already validated, so there is no reading or refusing left in
/// it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicySnapshot {
    source: RevisionId,
    documents: BTreeMap<PolicyScope, PolicyBody>,
}

impl PolicySnapshot {
    /// The revision every generation in this snapshot came from.
    pub const fn source(&self) -> RevisionId {
        self.source
    }

    /// The complete policy governing `scope`, selected whole.
    pub fn effective(&self, scope: PolicyScope) -> Option<&PolicyBody> {
        self.documents
            .get(&scope)
            .or_else(|| self.documents.get(&scope.fallback()?))
    }

    /// The generation the policy governing `scope` is enforced under.
    ///
    /// The generation of the document that actually governs it: a project with no
    /// document of its own is fenced by its tenant's generation, because that is
    /// the document being enforced.
    ///
    /// So the generation a project is fenced by names its *tenant* while the
    /// project has no document, and names the project once it has one. Publishing a
    /// project's first own document therefore does not advance a generation, it
    /// replaces the governing document with a different one, and a fence built on
    /// the tenant's document refuses to adopt it
    /// ([`PolicyFence::adopt`] returns [`NotAnAdvance`]). That is the intended
    /// contract: a fence is a fence on one document, and a change of which document
    /// governs a scope is not an epoch advance any writer holding the old one may be
    /// carried across. An activation slice takes the new fence from the new snapshot
    /// rather than walking the old one forward, which is also why writers of a
    /// project and of its tenant hold *one* generation while the tenant's document
    /// governs both — they are enforcing one document, and there is nothing to tell
    /// apart.
    pub fn generation(&self, scope: PolicyScope) -> Option<PolicyGeneration> {
        Some(self.effective(scope)?.generation(self.source))
    }

    /// The fence a writer for `scope` must pass, from this snapshot.
    ///
    /// Taken per snapshot, not advanced across snapshots that change which document
    /// governs `scope` — see [`generation`](Self::generation).
    pub fn fence(&self, scope: PolicyScope) -> Option<PolicyFence> {
        Some(PolicyFence::new(self.generation(scope)?))
    }

    /// Every scope this snapshot publishes a document for, ordered.
    pub fn scopes(&self) -> impl ExactSizeIterator<Item = PolicyScope> + '_ {
        self.documents.keys().copied()
    }
}

impl BodyError for PolicyError {
    fn kind(reference: ResourceRef, expected: ResourceKind, found: ResourceKind) -> Self {
        Self::Kind {
            reference,
            expected,
            found,
        }
    }

    fn not_inline(reference: ResourceRef) -> Self {
        Self::NotInline { reference }
    }

    fn not_a_record(reference: ResourceRef) -> Self {
        Self::NotARecord { reference }
    }

    fn schema(reference: ResourceRef, expected: &'static str, found: String) -> Self {
        Self::Schema {
            reference,
            expected,
            found,
        }
    }

    fn damaged_schema(reference: ResourceRef) -> Self {
        Self::DamagedSchema { reference }
    }

    fn missing_field(reference: ResourceRef, field: &'static str) -> Self {
        Self::MissingField { reference, field }
    }

    fn unknown_field(reference: ResourceRef, schema: &'static str, field: String) -> Self {
        Self::UnknownField {
            reference,
            schema,
            field,
        }
    }

    /// The names in [`BOOTSTRAP_OWNED_FIELDS`], which this schema reserves.
    fn reserved_field(reference: ResourceRef, _schema: &'static str, field: String) -> Self {
        Self::BootstrapOwned { reference, field }
    }

    fn field_type(reference: ResourceRef, field: &'static str) -> Self {
        Self::FieldType { reference, field }
    }
}

impl IdentifiedBody for PolicyError {
    fn malformed_id(reference: ResourceRef, field: &'static str, source: InvalidId) -> Self {
        Self::MalformedId {
            reference,
            field,
            source,
        }
    }

    fn identity_mismatch(reference: ResourceRef, declared: String, identity: ResourceId) -> Self {
        Self::IdentityMismatch {
            reference,
            declared,
            identity,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::canonical::SerializerVersion;
    use super::super::fixtures::{
        DESIRED_STATE_RESOURCES, candidate, policy_body, project_id, project_policy,
        project_policy_body, revision_id, state, state_with_policy, tenant_id, tenant_policy,
        tenant_policy_body,
    };
    use super::super::mutation::ExpectedRevision;
    use super::super::revision::{
        BodySkew, IntegrityError, LoadedRevision, RevisionManifest, ValidationError,
    };
    use super::*;
    use std::time::SystemTime;

    fn tenant_scope() -> PolicyScope {
        PolicyScope::Tenant(tenant_id(1))
    }

    fn project_scope() -> PolicyScope {
        PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(2),
        }
    }

    fn slug() -> Slug {
        Slug::parse("limits").expect("test slug")
    }

    fn middleware(id: &str) -> ContentMiddlewareRegistration {
        ContentMiddlewareRegistration::new(
            id,
            [MiddlewareScope::Request],
            MiddlewareFailurePosture::FailClosed,
            25,
        )
        .expect("valid middleware registration")
    }

    fn with_fields(
        resource: &ResourceVersion,
        edit: impl FnOnce(&mut Vec<(String, CanonicalValue)>),
    ) -> ResourceVersion {
        let ResourceBody::Inline(CanonicalValue::Map(fields)) = &resource.body else {
            panic!("a policy fixture body is an inline record");
        };
        let mut fields = fields.clone();
        edit(&mut fields);
        ResourceVersion {
            body: ResourceBody::Inline(CanonicalValue::Map(fields)),
            ..resource.clone()
        }
    }

    fn set(fields: &mut Vec<(String, CanonicalValue)>, field: &str, value: CanonicalValue) {
        fields.retain(|(name, _)| name != field);
        fields.push((field.to_owned(), value));
    }

    #[test]
    fn content_middleware_round_trips_in_order_and_absence_is_compatible() {
        let plain = policy_body(tenant_scope(), 1);
        assert!(
            PolicyBody::read(&plain.version(slug()))
                .unwrap()
                .content_middleware()
                .is_empty()
        );

        let body = plain
            .with_content_middleware(vec![middleware("first"), middleware("second")])
            .unwrap();
        let read = PolicyBody::read(&body.version(slug())).expect("typed registration reads");
        assert_eq!(read, body);
        assert_eq!(
            read.content_middleware()
                .iter()
                .map(ContentMiddlewareRegistration::id)
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
    }

    #[test]
    fn guardrail_configuration_round_trips_and_changes_policy_content() {
        let plain = policy_body(tenant_scope(), 1);
        let middleware = ContentMiddlewareRegistration::new(
            REDACTION_MIDDLEWARE_ID,
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
            MiddlewareFailurePosture::FailClosed,
            25,
        )
        .unwrap()
        .with_guardrail(
            ContentGuardrailRegistration::new(
                "GW_GUARDRAIL_KEY",
                vec![
                    GuardrailRule {
                        id: "deny".to_owned(),
                        pattern: "forbidden".to_owned(),
                        action: GuardrailAction::Block,
                    },
                    GuardrailRule {
                        id: "email".to_owned(),
                        pattern: r"[a-z]+@example\.com".to_owned(),
                        action: GuardrailAction::Redact,
                    },
                ],
            )
            .unwrap(),
        )
        .unwrap();
        let configured = plain
            .clone()
            .with_content_middleware(vec![middleware])
            .unwrap();
        assert_ne!(configured.content(), plain.content());
        let read = PolicyBody::read(&configured.version(slug())).expect("guardrail reads");
        assert_eq!(read, configured);
        let guardrail = read.content_middleware()[0]
            .guardrail()
            .expect("guardrail configuration");
        assert_eq!(guardrail.key_env(), "GW_GUARDRAIL_KEY");
        assert_eq!(guardrail.rules().len(), 2);
        assert_eq!(guardrail.rules()[1].action, GuardrailAction::Redact);
    }

    #[test]
    fn redaction_middleware_requires_fail_closed_posture() {
        assert_eq!(
            ContentMiddlewareRegistration::new(
                REDACTION_MIDDLEWARE_ID,
                [
                    MiddlewareScope::Request,
                    MiddlewareScope::Response,
                    MiddlewareScope::StreamEvent,
                ],
                MiddlewareFailurePosture::FailOpen,
                25,
            ),
            Err(InvalidContentMiddleware::RedactionRequiresFailClosed)
        );
    }

    #[test]
    fn guardrail_registration_rejects_malformed_and_unbounded_configuration() {
        fn rule(id: &str, pattern: &str) -> GuardrailRule {
            GuardrailRule {
                id: id.to_owned(),
                pattern: pattern.to_owned(),
                action: GuardrailAction::Redact,
            }
        }

        assert_eq!(
            ContentGuardrailRegistration::new("", vec![rule("email", "secret")]),
            Err(InvalidContentMiddleware::GuardrailKeyEnv)
        );
        assert_eq!(
            ContentGuardrailRegistration::new("9INVALID", vec![rule("email", "secret")]),
            Err(InvalidContentMiddleware::GuardrailKeyEnv)
        );
        assert_eq!(
            ContentGuardrailRegistration::new("GW_GUARDRAIL_KEY", Vec::new()),
            Err(InvalidContentMiddleware::GuardrailNoRules)
        );
        assert_eq!(
            ContentGuardrailRegistration::new(
                "GW_GUARDRAIL_KEY",
                (0..=MAX_GUARDRAIL_RULES)
                    .map(|index| rule(&format!("rule-{index}"), "secret"))
                    .collect(),
            ),
            Err(InvalidContentMiddleware::GuardrailTooManyRules)
        );
        assert_eq!(
            ContentGuardrailRegistration::new("GW_GUARDRAIL_KEY", vec![rule("Email", "secret")],),
            Err(InvalidContentMiddleware::GuardrailRuleId)
        );
        assert_eq!(
            ContentGuardrailRegistration::new(
                "GW_GUARDRAIL_KEY",
                vec![rule("email", "one"), rule("email", "two")],
            ),
            Err(InvalidContentMiddleware::DuplicateGuardrailRule(
                "email".to_owned()
            ))
        );
        for pattern in [String::new(), "x".repeat(MAX_GUARDRAIL_PATTERN_BYTES + 1)] {
            assert_eq!(
                ContentGuardrailRegistration::new(
                    "GW_GUARDRAIL_KEY",
                    vec![rule("email", &pattern)],
                ),
                Err(InvalidContentMiddleware::GuardrailPattern(
                    "email".to_owned()
                ))
            );
        }
    }

    #[test]
    fn buffered_response_routes_are_typed_normalized_and_backward_compatible() {
        let plain = policy_body(tenant_scope(), 1);
        let old_content = plain.content();
        assert!(plain.buffered_response_routes().is_empty());
        assert!(
            PolicyBody::read(&plain.version(slug()))
                .unwrap()
                .buffered_response_routes()
                .is_empty()
        );

        let body = plain
            .with_buffered_response_routes([
                BufferedResponseRoute::Responses,
                BufferedResponseRoute::Messages,
            ])
            .unwrap();
        assert_eq!(
            body.buffered_response_routes(),
            [
                BufferedResponseRoute::Messages,
                BufferedResponseRoute::Responses,
            ]
        );
        assert_ne!(body.content(), old_content);
        assert_eq!(
            PolicyBody::read(&body.version(slug())).expect("typed route set reads"),
            body
        );
    }

    #[test]
    fn buffered_response_routes_reject_duplicates_and_unknown_values() {
        assert_eq!(
            policy_body(tenant_scope(), 1).with_buffered_response_routes([
                BufferedResponseRoute::Messages,
                BufferedResponseRoute::Messages,
            ]),
            Err(InvalidBufferedResponseRoutes::Duplicate("messages"))
        );

        let unknown = edited(|fields| {
            set(
                fields,
                BUFFERED_RESPONSE_ROUTES_FIELD,
                CanonicalValue::set([CanonicalValue::string("chat_completions")]),
            );
        });
        let error = PolicyBody::read(&unknown).expect_err("unknown routes fail closed");
        assert!(matches!(
            &error,
            PolicyError::InvalidBufferedResponseRoutes {
                source: InvalidBufferedResponseRoutes::Unsupported(route),
                ..
            } if route == "chat_completions"
        ));
        assert!(error.is_incompatible());
    }

    #[test]
    fn buffered_response_route_changes_require_an_epoch_and_activate_live() {
        let base = policy_body(tenant_scope(), 4);
        let same_epoch = base
            .clone()
            .with_buffered_response_routes([BufferedResponseRoute::Messages])
            .unwrap();
        assert_eq!(
            base.transition(&same_epoch).reasons(),
            &[TransitionReason::EpochNotAdvanced]
        );

        let changed = PolicyBody::new(
            base.scope(),
            base.epoch().next(),
            *base.budget(),
            *base.concurrency(),
            *base.revocation(),
        )
        .with_buffered_response_routes([BufferedResponseRoute::Messages])
        .unwrap();
        assert_eq!(
            base.transition(&changed).reasons(),
            &[TransitionReason::BufferedResponseRoutesChanged]
        );
        assert!(base.transition(&changed).is_live());
    }

    #[test]
    fn content_middleware_validation_reserves_core_and_bounds_the_chain() {
        assert!(matches!(
            ContentMiddlewareRegistration::new(
                "authentication",
                [MiddlewareScope::Request],
                MiddlewareFailurePosture::FailClosed,
                25,
            ),
            Err(InvalidContentMiddleware::CoreStage(_))
        ));
        assert!(matches!(
            ContentMiddlewareRegistration::new(
                "content",
                [MiddlewareScope::Request, MiddlewareScope::Request],
                MiddlewareFailurePosture::FailClosed,
                25,
            ),
            Err(InvalidContentMiddleware::DuplicateScope("request"))
        ));
        assert!(matches!(
            ContentMiddlewareRegistration::new(
                "content",
                [MiddlewareScope::Request],
                MiddlewareFailurePosture::FailClosed,
                MAX_MIDDLEWARE_DURATION_MILLISECONDS + 1,
            ),
            Err(InvalidContentMiddleware::BoundTooLarge)
        ));
        assert!(matches!(
            policy_body(tenant_scope(), 1).with_content_middleware(
                (0..=MAX_CONTENT_MIDDLEWARE)
                    .map(|index| middleware(&format!("content-{index}")))
                    .collect(),
            ),
            Err(InvalidContentMiddleware::TooMany)
        ));
    }

    #[test]
    fn middleware_changes_require_an_epoch_and_are_live_and_rollbackable() {
        let base = policy_body(tenant_scope(), 4);
        let same_epoch = base
            .clone()
            .with_content_middleware(vec![middleware("first")])
            .unwrap();
        assert_eq!(
            base.transition(&same_epoch).reasons(),
            &[TransitionReason::EpochNotAdvanced]
        );

        let added = PolicyBody::new(
            base.scope(),
            base.epoch().next(),
            *base.budget(),
            *base.concurrency(),
            *base.revocation(),
        )
        .with_content_middleware(vec![middleware("first")])
        .unwrap();
        assert_eq!(
            base.transition(&added).reasons(),
            &[TransitionReason::ContentMiddlewareChanged]
        );
        assert!(base.transition(&added).is_live());

        let removed = PolicyBody::new(
            added.scope(),
            added.epoch().next(),
            *added.budget(),
            *added.concurrency(),
            *added.revocation(),
        );
        assert!(added.transition(&removed).is_live());

        let rollback = PolicyBody::new(
            removed.scope(),
            removed.epoch().next(),
            *removed.budget(),
            *removed.concurrency(),
            *removed.revocation(),
        )
        .with_content_middleware(added.content_middleware().to_vec())
        .unwrap();
        assert!(removed.transition(&rollback).is_live());
        assert_eq!(rollback.content(), added.content());

        let project = PolicyBody::new(
            project_scope(),
            PolicyEpoch::FIRST,
            *base.budget(),
            *base.concurrency(),
            *base.revocation(),
        )
        .with_content_middleware(vec![middleware("project-chain")])
        .unwrap();
        let handover = base.displaced_by(&project);
        assert!(handover.is_live());
        assert!(
            handover.reasons().is_empty(),
            "a handover carries only reasons the activation surface reports"
        );
    }

    /// A tenant document with one field of the canonical record edited: how a
    /// strictness test names exactly what is wrong with a stored body.
    fn edited(edit: impl FnOnce(&mut Vec<(String, CanonicalValue)>)) -> ResourceVersion {
        with_fields(&tenant_policy(1, 1), edit)
    }

    #[test]
    fn a_document_round_trips_through_its_envelope_and_its_canonical_bytes() {
        let body = tenant_policy_body(1, 1);
        let resource = tenant_policy(1, 1);
        assert_eq!(PolicyBody::read(&resource).unwrap(), body);
        assert_eq!(
            resource.reference.id,
            ResourceId::new(tenant_id(1).uuid()),
            "a tenant's policy is written under the tenant it governs"
        );
        assert_eq!(resource.scope, ResourceScope::Tenant(tenant_id(1)));

        let project = project_policy_body(1, 2, 1);
        let resource = project_policy(1, 2, 1);
        assert_eq!(PolicyBody::read(&resource).unwrap(), project);
        assert_eq!(resource.reference.id, ResourceId::new(project_id(2).uuid()));

        // The bytes are the content's identity: the same document built twice is
        // one checksum, a different document is another, and the schema is inside
        // the bytes rather than beside them.
        assert_eq!(
            body.checksum().unwrap(),
            tenant_policy_body(1, 1).checksum().unwrap()
        );
        assert_ne!(
            body.checksum().unwrap(),
            tenant_policy_body(1, 2).checksum().unwrap(),
            "the epoch is part of the document, so republishing changes its bytes"
        );
        assert_ne!(body.checksum().unwrap(), project.checksum().unwrap());

        let bytes = SerializerVersion::V1.encode(&project.canonical()).unwrap();
        let decoded = SerializerVersion::V1
            .decode(&bytes)
            .expect("a policy body is canonical, so storage returns what it took");
        assert_eq!(
            SerializerVersion::V1.encode(&decoded).unwrap(),
            bytes,
            "the decoded body re-encodes to the bytes storage holds"
        );
        assert_eq!(
            PolicyBody::read(&ResourceVersion {
                body: ResourceBody::Inline(decoded),
                ..resource
            })
            .unwrap(),
            project,
            "and reads back as the same document"
        );
        assert!(
            matches!(
                body.canonical(),
                CanonicalValue::Map(ref fields)
                    if fields.iter().any(|(name, value)|
                        name == SCHEMA_FIELD && *value == CanonicalValue::string(POLICY_SCHEMA))
            ),
            "the schema identifier is inside the checksummed bytes"
        );
    }

    #[test]
    fn an_absent_optional_field_is_a_statement_rather_than_an_omission() {
        let uncapped = tenant_policy_body(1, 1);
        assert_eq!(uncapped.budget().namespace_limit_microdollars(), None);
        let CanonicalValue::Map(fields) = uncapped.canonical() else {
            panic!("a policy body is a record");
        };
        assert!(
            !fields
                .iter()
                .any(|(name, _)| name == NAMESPACE_BUDGET_LIMIT_FIELD),
            "no scope-wide cap is the absence of the key, not a cap of zero"
        );

        let capped = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::FIRST,
            BudgetPolicy::new(1_000_000, Some(1), 60).unwrap(),
            ConcurrencyPolicy::new(8, 30).unwrap(),
            RevocationPolicy::new(1),
        );
        assert_ne!(
            uncapped.checksum().unwrap(),
            capped.checksum().unwrap(),
            "a scope-wide cap is a different document from no cap at all"
        );
        assert_eq!(
            PolicyBody::read(&capped.version(slug())).unwrap(),
            capped,
            "and it reads back as the cap it is"
        );
    }

    #[test]
    fn a_body_is_bound_to_the_envelope_that_carries_it() {
        // Identity: the document names the object it governs, and the envelope is
        // written under that object's id. A body moved onto another envelope is
        // refused rather than silently governing whatever carried it.
        let moved = ResourceVersion {
            reference: ResourceRef::new(
                ResourceKind::Policy,
                ResourceId::new(project_id(9).uuid()),
                ResourceVersionNumber::FIRST,
            ),
            ..tenant_policy(1, 1)
        };
        assert!(
            matches!(
                PolicyBody::read(&moved),
                Err(PolicyError::IdentityMismatch { .. })
            ),
            "{:?}",
            PolicyBody::read(&moved)
        );

        // Scope: a tenant-wide document at a project's scope would be enforced
        // for a scope it does not describe.
        let rescoped = ResourceVersion {
            scope: ResourceScope::Project {
                tenant: tenant_id(1),
                project: project_id(2),
            },
            ..tenant_policy(1, 1)
        };
        assert_eq!(
            PolicyBody::read(&rescoped),
            Err(PolicyError::ScopeMismatch {
                reference: rescoped.reference,
                declared: tenant_scope(),
                scoped: rescoped.scope.clone(),
            })
        );

        // And a policy body is only ever read off a policy resource.
        let mistyped = ResourceVersion {
            reference: ResourceRef::new(
                ResourceKind::ProviderCredential,
                tenant_policy(1, 1).reference.id,
                ResourceVersionNumber::FIRST,
            ),
            ..tenant_policy(1, 1)
        };
        assert!(matches!(
            PolicyBody::read(&mistyped),
            Err(PolicyError::Kind { .. })
        ));
    }

    #[test]
    fn a_strict_reader_refuses_every_body_it_does_not_fully_understand() {
        let reference = tenant_policy(1, 1).reference;

        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(
                    fields,
                    SCHEMA_FIELD,
                    CanonicalValue::string("axond.policy.v2"),
                );
            })),
            Err(PolicyError::Schema {
                reference,
                expected: POLICY_SCHEMA,
                found: "axond.policy.v2".to_owned(),
            })
        );
        // A marker that is present and is not an identifier: no release wrote one,
        // so it is damage rather than a build to roll forward.
        for marker in [
            CanonicalValue::integer(1),
            CanonicalValue::List(vec![CanonicalValue::string(POLICY_SCHEMA)]),
            CanonicalValue::map([(SCHEMA_FIELD, CanonicalValue::string(POLICY_SCHEMA))]),
        ] {
            let error = PolicyBody::read(&edited(|fields| {
                set(fields, SCHEMA_FIELD, marker.clone());
            }))
            .expect_err("an unreadable marker");
            assert_eq!(error, PolicyError::DamagedSchema { reference });
            assert!(!error.is_incompatible(), "{error}");
            assert!(
                error.to_string().contains("restore the row"),
                "the alert must name the repair: {error}"
            );
        }
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, "burst_limit", CanonicalValue::integer(7u32));
            })),
            Err(PolicyError::UnknownField {
                reference,
                schema: POLICY_SCHEMA,
                field: "burst_limit".to_owned(),
            }),
            "a field a newer release added is refused rather than ignored"
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                fields.retain(|(name, _)| name != EPOCH_FIELD);
            })),
            Err(PolicyError::MissingField {
                reference,
                field: EPOCH_FIELD,
            })
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, EPOCH_FIELD, CanonicalValue::string("4"));
            })),
            Err(PolicyError::FieldType {
                reference,
                field: EPOCH_FIELD,
            })
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, EPOCH_FIELD, CanonicalValue::integer(0u32));
            })),
            Err(PolicyError::FieldRange {
                reference,
                field: EPOCH_FIELD,
                source: InvalidPolicy::TooSmall { value: 0, min: 1 },
            }),
            "zero is not an epoch, so an unset counter cannot read as a valid one"
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, LEASE_TTL_FIELD, CanonicalValue::integer(-30i32));
            })),
            Err(PolicyError::FieldType {
                reference,
                field: LEASE_TTL_FIELD,
            }),
            "a value that is not an unsigned counter is a shape refusal, not a bound"
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, LEASE_TTL_FIELD, CanonicalValue::integer(0u32));
            })),
            Err(PolicyError::FieldRange {
                reference,
                field: LEASE_TTL_FIELD,
                source: InvalidPolicy::TooSmall { value: 0, min: 1 },
            }),
            "a refusal names the field that broke a bound, not the one checked first"
        );
        assert_eq!(
            PolicyBody::read(&edited(|fields| {
                set(fields, MAX_IN_FLIGHT_FIELD, CanonicalValue::integer(0u32));
            })),
            Err(PolicyError::FieldRange {
                reference,
                field: MAX_IN_FLIGHT_FIELD,
                source: InvalidPolicy::TooSmall { value: 0, min: 1 },
            })
        );
        // A cap of zero is the one bound this build tightened over documents an
        // earlier one stored, so it reads back rather than taking the revision
        // out of service, and names itself for the refusal that follows at
        // activation.
        for (field, bound) in [
            (BUDGET_LIMIT_FIELD, BudgetBound::SubjectLimit),
            (NAMESPACE_BUDGET_LIMIT_FIELD, BudgetBound::NamespaceLimit),
        ] {
            let stored = PolicyBody::read(&edited(|fields| {
                set(fields, field, CanonicalValue::integer(0u32));
            }))
            .expect("a stored zero cap still hydrates");
            assert_eq!(stored.budget().unenforceable_cap(), Some(bound));
            assert_eq!(bound.document_field(), field);
        }
        assert!(matches!(
            PolicyBody::read(&edited(|fields| {
                set(
                    fields,
                    TENANT_ID_FIELD,
                    CanonicalValue::string("not-a-uuid"),
                );
            })),
            Err(PolicyError::MalformedId {
                field: TENANT_ID_FIELD,
                ..
            })
        ));
        assert_eq!(
            PolicyBody::read(&ResourceVersion {
                body: ResourceBody::Inline(CanonicalValue::string(POLICY_SCHEMA)),
                ..tenant_policy(1, 1)
            }),
            Err(PolicyError::NotARecord { reference })
        );

        // Every bound an authored document passes is the bound a stored one
        // passes, so the two cannot disagree.
        assert_eq!(
            ConcurrencyPolicy::new(0, 30),
            Err(InvalidPolicy::TooSmall { value: 0, min: 1 })
        );
        assert_eq!(
            BudgetPolicy::new(1, None, 0),
            Err(InvalidPolicy::TooSmall { value: 0, min: 1 })
        );
        // A cap of zero denies every request for the scope, which the bootstrap
        // file refuses for the same reason: it is not a bound on spending, it is
        // a closed scope wearing one. Refused on both scopes, so a published
        // document cannot express what an authored one may not.
        assert_eq!(
            BudgetPolicy::new(0, None, 30),
            Err(InvalidPolicy::TooSmall { value: 0, min: 1 })
        );
        assert_eq!(
            BudgetPolicy::new(1, Some(0), 30),
            Err(InvalidPolicy::TooSmall { value: 0, min: 1 })
        );
    }

    #[test]
    fn a_published_document_may_not_name_a_field_bootstrap_owns() {
        let reference = tenant_policy(1, 1).reference;
        for field in BOOTSTRAP_OWNED_FIELDS {
            assert_eq!(
                PolicyBody::read(&edited(|fields| {
                    set(fields, field, CanonicalValue::string("allow"));
                })),
                Err(PolicyError::BootstrapOwned {
                    reference,
                    field: (*field).to_owned(),
                }),
                "`{field}` is the bootstrap file's, and a publication may not set it"
            );
            assert!(
                !PolicyBody::KNOWN_FIELDS.contains(field),
                "`{field}` must not also be a field this schema defines"
            );
        }
        assert!(
            BOOTSTRAP_OWNED_FIELDS.contains(&"on_unavailable")
                && BOOTSTRAP_OWNED_FIELDS.contains(&"backend"),
            "backend identity and the unavailable stance are the two that must never be publishable"
        );
        assert!(
            !PolicyBody::read(&edited(|fields| {
                set(fields, "on_unavailable", CanonicalValue::string("allow"));
            }))
            .expect_err("a bootstrap-owned field is refused")
            .is_incompatible(),
            "a boundary this schema will never cross must not read as a version skew"
        );
    }

    #[test]
    fn a_body_this_build_cannot_read_is_a_skew_and_a_rewritten_one_is_damage() {
        let reference = tenant_policy(1, 1).reference;
        for error in [
            PolicyError::Schema {
                reference,
                expected: POLICY_SCHEMA,
                found: "axond.policy.v2".to_owned(),
            },
            PolicyError::UnknownField {
                reference,
                schema: POLICY_SCHEMA,
                field: "burst_limit".to_owned(),
            },
            PolicyError::MissingField {
                reference,
                field: SCHEMA_FIELD,
            },
        ] {
            assert!(error.is_incompatible(), "{error}");
            assert_eq!(error.reference(), reference);
        }
        for error in [
            PolicyError::MissingField {
                reference,
                field: EPOCH_FIELD,
            },
            PolicyError::FieldType {
                reference,
                field: EPOCH_FIELD,
            },
            PolicyError::IdentityMismatch {
                reference,
                declared: tenant_scope().to_string(),
                identity: reference.id,
            },
            PolicyError::FieldType {
                reference,
                field: LEASE_TTL_FIELD,
            },
        ] {
            assert!(
                !error.is_incompatible(),
                "a refusal inside a body that declared this schema points at storage: {error}"
            );
        }

        // A bound is the one rule that can tighten inside a stable schema — as a
        // display name's can in tenancy — so a value below one this build enforces
        // is a release skew rather than a rewritten row.
        let below_a_bound = PolicyError::FieldRange {
            reference,
            field: LEASE_TTL_FIELD,
            source: InvalidPolicy::TooSmall { value: 0, min: 1 },
        };
        assert!(below_a_bound.is_incompatible(), "{below_a_bound}");
    }

    #[test]
    fn content_middleware_skew_is_distinguished_from_damaged_registration_state() {
        let reference = tenant_policy(1, 1).reference;
        for source in [
            InvalidContentMiddleware::Scope("future_scope".to_owned()),
            InvalidContentMiddleware::FailurePosture("future_posture".to_owned()),
            InvalidContentMiddleware::ZeroBound,
            InvalidContentMiddleware::BoundTooLarge,
            InvalidContentMiddleware::CoreStage("future-core-stage".to_owned()),
            InvalidContentMiddleware::TooMany,
            InvalidContentMiddleware::GuardrailTooManyRules,
            InvalidContentMiddleware::GuardrailAction("future_action".to_owned()),
        ] {
            let error = PolicyError::InvalidMiddleware {
                reference,
                field: CONTENT_MIDDLEWARE_FIELD.to_owned(),
                source,
            };
            assert!(error.is_incompatible(), "{error}");
        }
        for source in [
            InvalidContentMiddleware::Id,
            InvalidContentMiddleware::NoScope,
            InvalidContentMiddleware::DuplicateScope("request"),
            InvalidContentMiddleware::DuplicateId("duplicate".to_owned()),
        ] {
            let error = PolicyError::InvalidMiddleware {
                reference,
                field: CONTENT_MIDDLEWARE_FIELD.to_owned(),
                source,
            };
            assert!(!error.is_incompatible(), "{error}");
        }
    }

    #[test]
    fn publication_and_hydration_read_policy_the_way_they_read_tenancy() {
        let state = state_with_policy();
        state.validate().expect("the fixture documents are valid");
        assert_eq!(
            state.resources().len(),
            DESIRED_STATE_RESOURCES + 2,
            "a document per scope, and no other resource added"
        );

        let mut broken = DesiredState::new();
        for resource in state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Policy
                && resource.scope == ResourceScope::Tenant(tenant_id(1))
            {
                edited(|fields| {
                    set(fields, EPOCH_FIELD, CanonicalValue::integer(0u32));
                })
            } else {
                resource.clone()
            };
            broken.insert(resource).expect("distinct references");
        }
        for blob in state.blobs() {
            broken.declare_blob(*blob);
        }
        assert!(
            matches!(
                broken.validate(),
                Err(ValidationError::Policy(PolicyError::FieldRange { .. }))
            ),
            "{:?}",
            broken.validate()
        );

        // Hydration classifies a body this build cannot read as a compatibility
        // refusal rather than as corruption — the same distinction tenancy makes,
        // reached through the same seam.
        let candidate = candidate(ExpectedRevision::Empty, "policy", state_with_policy());
        let manifest =
            RevisionManifest::of(revision_id(1), None, SystemTime::UNIX_EPOCH, &candidate)
                .expect("the fixture state is publishable");
        let mut newer = DesiredState::new();
        for resource in candidate.state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Policy
                && resource.scope == ResourceScope::Tenant(tenant_id(1))
            {
                edited(|fields| {
                    set(
                        fields,
                        SCHEMA_FIELD,
                        CanonicalValue::string("axond.policy.v2"),
                    );
                })
            } else {
                resource.clone()
            };
            newer.insert(resource).expect("distinct references");
        }
        for blob in candidate.state.blobs() {
            newer.declare_blob(*blob);
        }
        let error = LoadedRevision::assemble(manifest, newer)
            .expect_err("a policy schema from another release must not hydrate");
        assert!(
            matches!(
                error,
                IntegrityError::Incompatible(BodySkew::Policy(PolicyError::Schema { .. }))
            ),
            "{error}"
        );
        assert!(error.is_incompatible());
        assert_eq!(
            match &error {
                IntegrityError::Incompatible(skew) => skew.reference(),
                other => panic!("{other}"),
            },
            tenant_policy(1, 1).reference,
            "and it names the row an operator has to act on"
        );
    }

    #[test]
    fn an_effective_policy_is_one_whole_document_never_a_merge_of_two() {
        // A project document that differs from its tenant's in every field, and
        // that *drops* the tenant's scope-wide cap: if anything were merged, the
        // cap would survive.
        let tenant = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::FIRST,
            BudgetPolicy::new(9_000_000, Some(50_000_000), 900).unwrap(),
            ConcurrencyPolicy::new(64, 600).unwrap(),
            RevocationPolicy::new(7),
        );
        let project = PolicyBody::new(
            project_scope(),
            PolicyEpoch::FIRST,
            BudgetPolicy::new(1_000, None, 30).unwrap(),
            ConcurrencyPolicy::new(2, 15).unwrap(),
            RevocationPolicy::new(1),
        );
        let mut with_documents = state();
        with_documents
            .insert(tenant.version(slug()))
            .and_then(|state| state.insert(project.version(slug())))
            .expect("one document per scope");
        with_documents.validate().expect("both documents are valid");

        let set = PolicySet::of(&with_documents).unwrap();
        assert_eq!(set.documents().len(), 2);
        assert_eq!(set.document(project_scope()).unwrap().body, project);
        assert_eq!(
            set.effective(project_scope()).unwrap().body,
            project,
            "a scope with its own document is governed by that document verbatim"
        );
        assert_eq!(
            set.effective(project_scope())
                .unwrap()
                .body
                .budget()
                .namespace_limit_microdollars(),
            None,
            "the tenant's scope-wide cap is not inherited field by field"
        );

        // A sibling project with no document of its own takes its tenant's, whole.
        let sibling = PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(3),
        };
        assert_eq!(set.document(sibling), None);
        assert_eq!(set.effective(sibling).unwrap().body, tenant);

        // And a scope outside the revision has no published policy at all: the
        // bootstrap file's limits stand, rather than some partial document.
        assert_eq!(set.effective(PolicyScope::Tenant(tenant_id(4))), None);
        assert_eq!(PolicySet::of(&state()).unwrap().documents().len(), 0);
    }

    #[test]
    fn a_generation_is_an_epoch_and_the_revision_that_published_it() {
        let set = PolicySet::of(&state_with_policy()).unwrap();
        let (first, second) = (revision_id(1), revision_id(2));
        let snapshot = set.snapshot(first);
        assert_eq!(snapshot.source(), first);
        assert_eq!(snapshot.scopes().len(), 2);

        let content = tenant_policy_body(1, 1).content();
        let generation = snapshot.generation(tenant_scope()).unwrap();
        assert_eq!(generation.epoch(), PolicyEpoch::FIRST);
        assert_eq!(generation.source(), first);
        assert_eq!(generation.content(), content);
        assert_eq!(
            generation,
            PolicyGeneration::new(tenant_scope(), PolicyEpoch::FIRST, first, content)
        );

        let carried = set.snapshot(second).generation(tenant_scope()).unwrap();
        assert_ne!(
            generation, carried,
            "one epoch published by two revisions is two generations, not one"
        );
        assert!(
            carried.carries_forward(&generation) && generation.carries_forward(&carried),
            "but a revision that restates an unchanged document carries it forward, \
             rather than forking it"
        );
        assert!(
            carried.same_policy(&generation),
            "so the two generations enforce one policy"
        );

        // A project with no document of its own is fenced by the generation of
        // the document that actually governs it: its tenant's.
        let sibling = PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(3),
        };
        assert_eq!(
            snapshot.effective(sibling).unwrap(),
            &tenant_policy_body(1, 1)
        );
        assert_eq!(snapshot.generation(sibling), Some(generation));
        assert_eq!(snapshot.generation(PolicyScope::Tenant(tenant_id(4))), None);
        assert_eq!(snapshot.fence(PolicyScope::Tenant(tenant_id(4))), None);
    }

    #[test]
    fn a_writer_of_any_generation_but_the_active_one_fails_closed() {
        let (first, second) = (revision_id(1), revision_id(2));
        let content = tenant_policy_body(1, 4).content();
        let active =
            PolicyGeneration::new(tenant_scope(), PolicyEpoch::new(4).unwrap(), first, content);
        let fence = PolicyFence::new(active);
        assert_eq!(fence.active(), active);
        assert_eq!(fence.admit(active), Ok(()));

        let stale =
            PolicyGeneration::new(tenant_scope(), PolicyEpoch::new(3).unwrap(), first, content);
        assert_eq!(
            fence.admit(stale),
            Err(Fenced::Stale(Box::new(Offered::new(stale, active))))
        );

        // A generation this replica has not adopted is refused rather than
        // trusted: a writer that may enforce anything it claims is newer is not
        // fenced at all.
        let ahead = PolicyGeneration::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            second,
            content,
        );
        assert_eq!(
            fence.admit(ahead),
            Err(Fenced::Ahead(Box::new(Offered::new(ahead, active))))
        );

        // The same epoch stating a different policy — a restored backup, a forked
        // control plane — is refused rather than resolved in someone's favour.
        let forked = PolicyGeneration::new(
            tenant_scope(),
            PolicyEpoch::new(4).unwrap(),
            second,
            PolicyBody::new(
                tenant_scope(),
                PolicyEpoch::new(4).unwrap(),
                BudgetPolicy::new(9_000_000, None, 60).unwrap(),
                ConcurrencyPolicy::new(8, 30).unwrap(),
                RevocationPolicy::new(1),
            )
            .content(),
        );
        assert_eq!(
            fence.admit(forked),
            Err(Fenced::Forked(Box::new(Offered::new(forked, active))))
        );
        assert!(!forked.supersedes(&active) && !active.supersedes(&forked));
        assert!(!forked.carries_forward(&active));

        // Adoption is monotonic, so a replica cannot be walked back onto a
        // generation whose writers it already fenced out.
        let mut fence = PolicyFence::new(active);
        assert_eq!(fence.adopt(ahead), Ok(()));
        assert_eq!(fence.active(), ahead);
        assert_eq!(
            fence.admit(active),
            Err(Fenced::Stale(Box::new(Offered::new(active, ahead))))
        );
        assert_eq!(
            fence.adopt(active),
            Err(NotAnAdvance(Box::new(Offered::new(active, ahead))))
        );
        assert_eq!(
            fence.adopt(forked),
            Err(NotAnAdvance(Box::new(Offered::new(forked, ahead))))
        );
        assert_eq!(fence.active(), ahead, "a refused adoption changes nothing");
    }

    #[test]
    fn a_fence_cannot_be_walked_onto_another_scopes_policy() {
        // An epoch counts within the scope that published it, so another scope's
        // document is not a later publication of this one however high its epoch:
        // a miswired fence must refuse to move rather than adopt a policy none of
        // its writers hold and deny all of them.
        let elsewhere = project_policy_body(2, 9, 99).generation(revision_id(2));
        let active = tenant_policy_body(1, 4).generation(revision_id(1));
        assert!(!elsewhere.supersedes(&active) && !elsewhere.carries_forward(&active));
        assert!(!elsewhere.same_policy(&active));

        let mut fence = PolicyFence::new(active);
        assert_eq!(
            fence.adopt(elsewhere),
            Err(NotAnAdvance(Box::new(Offered::new(elsewhere, active))))
        );
        assert_eq!(fence.active(), active);
        assert_eq!(
            fence.admit(elsewhere),
            Err(Fenced::OtherScope(Box::new(Offered::new(
                elsewhere, active
            )))),
            "and a writer holding it is wired wrong rather than late or early"
        );
    }

    #[test]
    fn a_project_taking_its_tenants_document_is_fenced_on_that_document() {
        // A project with no document of its own is governed by its tenant's, so it
        // is fenced by the tenant's generation: one document, one fence, and
        // nothing to tell a project writer from a tenant writer while both enforce
        // it.
        let mut with_tenant_only = state();
        with_tenant_only
            .insert(tenant_policy_body(1, 1).version(slug()))
            .expect("a tenant document");
        let inherited = PolicySet::of(&with_tenant_only)
            .unwrap()
            .snapshot(revision_id(1));
        let fallback = inherited.generation(project_scope()).unwrap();
        assert_eq!(fallback.scope(), tenant_scope());
        assert_eq!(fallback, inherited.generation(tenant_scope()).unwrap());

        // Publishing the project's first own document does not advance that
        // generation, it replaces which document governs the project. So the
        // inherited fence refuses to move onto it, and an activation slice takes
        // the new fence from the new snapshot rather than walking the old one
        // forward.
        let mut with_project = state();
        with_project
            .insert(tenant_policy_body(1, 1).version(slug()))
            .and_then(|state| state.insert(project_policy_body(1, 2, 1).version(slug())))
            .expect("one document per scope");
        let own = PolicySet::of(&with_project)
            .unwrap()
            .snapshot(revision_id(2))
            .generation(project_scope())
            .unwrap();
        assert_eq!(own.scope(), project_scope());

        let mut fence = inherited.fence(project_scope()).unwrap();
        assert_eq!(
            fence.adopt(own),
            Err(NotAnAdvance(Box::new(Offered::new(own, fallback))))
        );
        assert_eq!(
            fence.active(),
            fallback,
            "and it keeps enforcing the document it has"
        );
    }

    #[test]
    fn an_unchanged_document_carried_into_a_later_revision_stays_the_same_policy() {
        // The ordinary case: a revision that changes an unrelated resource still
        // restates every policy document, so an unchanged document is republished
        // under a new revision id at the same epoch.
        let set = PolicySet::of(&state_with_policy()).unwrap();
        let (published, carried) = (
            set.snapshot(revision_id(1))
                .generation(tenant_scope())
                .unwrap(),
            set.snapshot(revision_id(2))
                .generation(tenant_scope())
                .unwrap(),
        );

        // A writer holding the earlier publication enforces the policy the fence
        // is enforcing, so it is admitted rather than fenced out as a fork.
        let mut fence = PolicyFence::new(carried);
        assert_eq!(fence.admit(published), Ok(()));
        assert_eq!(PolicyFence::new(published).admit(carried), Ok(()));

        // And a replica can follow the fleet onto the revision now serving it,
        // which strict epoch advance alone would never allow.
        let mut following = PolicyFence::new(published);
        assert_eq!(following.adopt(carried), Ok(()));
        assert_eq!(following.active(), carried);

        // Monotonic in what is enforced, not in the revision: the same document is
        // adoptable in either direction, while a different policy at that epoch is
        // not adoptable in either.
        assert_eq!(fence.adopt(published), Ok(()));
        let forked = PolicyGeneration::new(
            tenant_scope(),
            published.epoch(),
            revision_id(3),
            PolicyBody::new(
                tenant_scope(),
                published.epoch(),
                BudgetPolicy::new(9_000_000, None, 60).unwrap(),
                ConcurrencyPolicy::new(8, 30).unwrap(),
                RevocationPolicy::new(1),
            )
            .content(),
        );
        assert_eq!(
            fence.adopt(forked),
            Err(NotAnAdvance(Box::new(Offered::new(forked, published))))
        );
        assert!(matches!(fence.admit(forked), Err(Fenced::Forked(_))));

        // The transition model agrees: restating a document changes nothing.
        let body = tenant_policy_body(1, published.epoch().get());
        assert!(body.transition(&body).reasons().is_empty());
        assert_eq!(
            body.content(),
            PolicyBody::new(
                body.scope(),
                body.epoch().next(),
                *body.budget(),
                *body.concurrency(),
                *body.revocation(),
            )
            .content(),
            "advancing only the epoch restates the same policy"
        );

        // An absent optional cap is a statement of its own, so a document that
        // states one never digests like a document that has none, and neither
        // can be carried forward as the other.
        let capped = |limit| {
            PolicyBody::new(
                tenant_scope(),
                PolicyEpoch::FIRST,
                BudgetPolicy::new(1_000_000, limit, 60).unwrap(),
                ConcurrencyPolicy::new(8, 30).unwrap(),
                RevocationPolicy::new(1),
            )
            .content()
        };
        assert_ne!(capped(None), capped(Some(1)));
        assert_ne!(capped(Some(1)), capped(Some(2)));
    }

    #[test]
    fn a_transition_is_classified_by_what_activating_it_would_require() {
        let base = policy_body(tenant_scope(), 4);
        let next = |body: PolicyBody| {
            PolicyBody::new(
                body.scope(),
                body.epoch().next(),
                *body.budget(),
                *body.concurrency(),
                *body.revocation(),
            )
        };

        // Republication with no change to what is enforced.
        let republished = next(base.clone());
        assert_eq!(
            base.transition(&republished),
            PolicyTransition {
                class: TransitionClass::Live,
                reasons: vec![TransitionReason::Republished],
            }
        );
        assert!(base.transition(&republished).is_live());
        assert!(base.transition(&base).reasons().is_empty());

        // Looser limits and a higher token floor refuse more, or refuse nothing
        // new: enforceable on the next request.
        let raised = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            BudgetPolicy::new(2_000_000, None, 120).unwrap(),
            ConcurrencyPolicy::new(16, 60).unwrap(),
            RevocationPolicy::new(2),
        );
        assert_eq!(base.transition(&raised).class(), TransitionClass::Live);
        assert_eq!(
            base.transition(&raised).reasons(),
            [
                TransitionReason::BudgetRaised,
                TransitionReason::ReservationTtlExtended,
                TransitionReason::ConcurrencyRaised,
                TransitionReason::LeaseTtlExtended,
                TransitionReason::TokenFloorRaised,
            ]
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .as_slice(),
            "every reason that applies is reported, in one order"
        );

        // A tighter limit binds what has not been admitted yet, and honours what
        // has: a drain rather than a live change.
        let lowered = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            BudgetPolicy::new(500_000, None, 30).unwrap(),
            ConcurrencyPolicy::new(4, 15).unwrap(),
            RevocationPolicy::new(1),
        );
        let transition = base.transition(&lowered);
        assert_eq!(transition.class(), TransitionClass::Drain);
        assert!(
            transition
                .reasons()
                .contains(&TransitionReason::BudgetLowered)
        );
        assert!(
            transition
                .reasons()
                .contains(&TransitionReason::ReservationTtlShortened)
        );

        // Turning a scope-wide cap on or off changes the shape of what a shared
        // store keeps, not only its numbers.
        let capped = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            BudgetPolicy::new(1_000_000, Some(10_000_000), 60).unwrap(),
            *base.concurrency(),
            *base.revocation(),
        );
        assert_eq!(
            base.transition(&capped),
            PolicyTransition {
                class: TransitionClass::MigrationRequired,
                reasons: vec![TransitionReason::ScopeCapEnabled],
            }
        );
        assert_eq!(
            capped.transition(&next(capped.clone())).class(),
            TransitionClass::Live,
            "republishing a capped document is not itself a migration"
        );
        let uncapped = PolicyBody::new(
            capped.scope(),
            capped.epoch().next(),
            BudgetPolicy::new(1_000_000, None, 60).unwrap(),
            *capped.concurrency(),
            *capped.revocation(),
        );
        assert_eq!(
            capped.transition(&uncapped).reasons(),
            [TransitionReason::ScopeCapDisabled],
        );
        assert_eq!(
            capped.transition(&uncapped).class(),
            TransitionClass::MigrationRequired
        );

        // The most disruptive reason decides the class of the whole change.
        let mixed = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(5).unwrap(),
            BudgetPolicy::new(2_000_000, Some(1), 60).unwrap(),
            ConcurrencyPolicy::new(1, 15).unwrap(),
            *base.revocation(),
        );
        let transition = base.transition(&mixed);
        assert_eq!(transition.class(), TransitionClass::MigrationRequired);
        assert!(
            transition
                .reasons()
                .contains(&TransitionReason::BudgetRaised)
        );
        assert!(
            transition
                .reasons()
                .contains(&TransitionReason::ConcurrencyLowered)
        );
        assert!(TransitionClass::Live < TransitionClass::Drain);
        assert!(TransitionClass::Drain < TransitionClass::MigrationRequired);
        assert!(TransitionClass::MigrationRequired < TransitionClass::Refused);
        assert_eq!(
            TransitionClass::MigrationRequired.to_string(),
            "migration-required"
        );
    }

    #[test]
    fn a_change_the_epoch_does_not_carry_is_refused_rather_than_applied() {
        let base = policy_body(tenant_scope(), 4);

        // A material change under the same epoch would leave two documents
        // claiming one generation, and a fence unable to tell them apart.
        let same_epoch = PolicyBody::new(
            tenant_scope(),
            base.epoch(),
            BudgetPolicy::new(2_000_000, None, 60).unwrap(),
            *base.concurrency(),
            *base.revocation(),
        );
        let transition = base.transition(&same_epoch);
        assert!(transition.is_refused());
        assert_eq!(transition.reasons(), [TransitionReason::EpochNotAdvanced]);

        let regressed = PolicyBody::new(
            tenant_scope(),
            PolicyEpoch::new(3).unwrap(),
            *base.budget(),
            *base.concurrency(),
            *base.revocation(),
        );
        assert_eq!(
            base.transition(&regressed).reasons(),
            [TransitionReason::EpochRegressed]
        );

        // Lowering the token floor would restore tokens an operator revoked.
        let unrevoked = PolicyBody::new(
            tenant_scope(),
            base.epoch().next(),
            *base.budget(),
            *base.concurrency(),
            RevocationPolicy::new(base.revocation().minimum_token_epoch() - 1),
        );
        let transition = base.transition(&unrevoked);
        assert!(transition.is_refused());
        assert_eq!(transition.reasons(), [TransitionReason::TokenFloorLowered]);

        // And two documents for different scopes are not a transition at all.
        let elsewhere = policy_body(project_scope(), 5);
        assert_eq!(
            base.transition(&elsewhere).reasons(),
            [TransitionReason::ScopeChanged]
        );
        assert!(base.transition(&elsewhere).is_refused());
    }
}
