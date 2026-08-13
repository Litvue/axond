//! Model bodies: what a *tenant may use* and what a project *calls it* (#205).
//!
//! [`tenancy`](super::tenancy) types who a deployment serves. This module types
//! the two bodies that say what is servable: a [`ModelEnablementBody`], which
//! makes one catalogue offering usable by a tenant or by one of its projects, and
//! a [`ModelAliasBody`], which is the name a project's callers ask for and the
//! ordered list of enablements that name resolves to.
//!
//! Nothing here routes. This slice is the contract the availability, policy,
//! admin, and snapshot slices read; the request path is untouched, and a revision
//! carrying these bodies changes what desired state *means*, not what the running
//! gateway does.
//!
//! # What a body carries
//!
//! | Field | Enablement | Alias |
//! | --- | --- | --- |
//! | `schema` | `axond.model-enablement.v1` | `axond.model-alias.v1` |
//! | `enablement_id` / `alias_id` | its own [`ResourceId`], bound to the envelope's | same |
//! | `tenant_id` | the owning [`TenantId`] | the owning [`TenantId`] |
//! | `project_id` | the owning project, when the enablement is a project's override | required: an alias is a project's name |
//! | `offering_id` | the opaque [`OfferingId`] this enables | — |
//! | `catalog_snapshot` | the immutable snapshot that identity was read from | — |
//! | `wire_family` | the wire contract the offering speaks | the wire contract callers of this name get |
//! | `state` | `enabled` or `disabled` | `enabled` or `disabled` |
//! | `observed_price` | what the catalogue *published*, if recorded | — |
//! | `approved_price` | the price resource an operator *approved*, if any | — |
//! | `targets` | — | enablements in priority order |
//!
//! # An offering identity is opaque, and pinned
//!
//! [`OfferingId`] is a digest of the provider and model identifiers a catalogue
//! snapshot lists, not those identifiers themselves. So an enablement is stable
//! across catalogue refreshes that do not change which offering is meant, and a
//! body — which is canonically encoded into a checksum an operator reads in a
//! manifest — does not restate upstream vocabulary that upstream may re-spell.
//!
//! Stability is not the same as *provenance*, so an enablement also carries the
//! [`Checksum`] of the immutable catalogue snapshot the identity was derived from,
//! and [`Models::of`] requires the enablement's envelope to depend on the
//! catalogue resource version that carries exactly that snapshot
//! ([`ModelError::UnpinnedSnapshot`]). "Which catalogue said this model exists?"
//! is therefore answerable from the revision alone, and a refreshed catalogue
//! cannot retroactively change what an already-published revision enabled.
//!
//! # Observed prices are not approved prices
//!
//! [`ObservedPrice`] is a catalogue fact: the rate an upstream publishes. It is
//! recorded for an operator to look at, and it is *not* billable — there is no
//! conversion from it to an [`ApprovedPrice`], and
//! [`ModelEnablementBody::billable_price`] ignores it entirely. Putting a rate in
//! service is an explicit administrative act that publishes a `Price` resource
//! and references it, which is the boundary #201 owns; this body only points at
//! one.
//!
//! # Tenant defaults and project overrides
//!
//! An enablement's scope is what makes it a default or an override: a
//! tenant-scoped enablement applies to every project of that tenant, and a
//! project-scoped one for the same offering replaces it for that project.
//! [`Models::effective_for`] states the resolution once, so no consumer invents a
//! second precedence rule, and two *enabled* enablements at the same scope for
//! one offering are refused ([`ModelError::DuplicateOffering`]) rather than
//! resolved by iteration order. A disabled one resolves to nothing and so holds
//! no offering — which is what makes replacing an enablement reachable, since a
//! new snapshot is a new enablement and desired state never forgets the old.
//!
//! # What is checked, and where
//!
//! [`Models::of`] reads every model body in a [`DesiredState`], and
//! [`DesiredState::validate`] calls it — so publication and hydration inherit the
//! rules with no request path involved:
//!
//! - **identity and ownership** — a body's id is its envelope's
//!   ([`ModelError::IdentityMismatch`]) and its owner is its envelope's scope
//!   ([`ModelError::OwnerMismatch`]); an alias is project-scoped or it is not an
//!   alias ([`ModelError::NotProjectScoped`]);
//! - **references** — an approved price and every alias target must be declared
//!   dependencies of the envelope that names them
//!   ([`ModelError::UndeclaredTarget`]) and must exist in the revision
//!   ([`ModelError::DanglingTarget`]);
//! - **reach** — an alias resolves to its own tenant's enablements, and to its own
//!   project's or its tenant's defaults, never another project's
//!   ([`ModelError::ForeignTarget`]);
//! - **order and uniqueness** — targets are a priority list, so they are kept in
//!   the order they were authored and one enablement may not appear twice
//!   ([`ModelError::DuplicateTarget`]);
//! - **wire compatibility** — every target of an alias speaks the alias's wire
//!   family ([`ModelError::WireFamilyMismatch`]), so one name cannot mean two
//!   request shapes.
//!
//! Lifecycle moves are checked where a *pair* of versions exists rather than in a
//! single body: [`ModelEnablementBody::transition_from`] and
//! [`ModelAliasBody::transition_from`] permit a state change and refuse a version
//! that quietly re-pins its snapshot, re-points its offering, changes owner, or
//! changes wire family ([`ForbiddenModelTransition`]).
//!
//! # Bodies published before this slice
//!
//! An *alias* row whose body declares no `schema` at all is a row written before
//! these bodies were typed. It is skipped rather than refused, because hydration
//! runs these rules and refusing such a row would stop an existing revision from
//! loading on upgrade. The accommodation is that one shape only: a row that
//! carries the key is read strictly, so a `schema` that is not text is a refusal
//! rather than a row that skips the alias rules unreported.
//!
//! An *enablement* has no such history — no release ever wrote one — so an
//! untyped enablement body is refused rather than skipped. Skipping it would be
//! an entitlement hole and not an upgrade accommodation: a row nothing reads is
//! also a row nothing binds to a scope, pins to a snapshot, or holds to one
//! enablement per offering.
//!
//! A body that declares a schema is held to it exactly — an identifier this build
//! does not read is a typed compatibility refusal ([`ModelError::Schema`]), never
//! a field-by-field guess. Both bodies are read through the shared strict reader
//! in [`record`](super::record), so a model body cannot be read more loosely than
//! a tenancy or credential one; what a refusal *means* stays here, in
//! [`ModelError::is_incompatible`].
//!
//! The operator-facing statement of all of this is `docs/adr/0042-model-enablement-and-alias-contracts.md`,
//! with the schema table and the untyped-alias exception in
//! `docs/operations/revision-convergence.md`.

use std::collections::BTreeMap;
use std::fmt;

use super::canonical::{
    Canonical, CanonicalError, CanonicalValue, Checksum, InvalidChecksum, SerializerVersion,
};
use super::ids::{InvalidId, ProjectId, ResourceId, Slug, TenantId};
use super::record::{
    BodyError, IdentifiedBody, PROJECT_ID_FIELD, Record, SCHEMA_FIELD, TENANT_ID_FIELD,
};
use super::resource::{
    BlobKind, ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion,
    ResourceVersionNumber,
};
use super::revision::DesiredState;

/// The model-enablement body schema this build reads and writes.
pub const MODEL_ENABLEMENT_SCHEMA: &str = "axond.model-enablement.v1";

/// The model-alias body schema this build reads and writes.
pub const MODEL_ALIAS_SCHEMA: &str = "axond.model-alias.v1";

const ENABLEMENT_ID_FIELD: &str = "enablement_id";
const ALIAS_ID_FIELD: &str = "alias_id";
const OFFERING_ID_FIELD: &str = "offering_id";
const CATALOG_SNAPSHOT_FIELD: &str = "catalog_snapshot";
const WIRE_FAMILY_FIELD: &str = "wire_family";
const STATE_FIELD: &str = "state";
const OBSERVED_PRICE_FIELD: &str = "observed_price";
const APPROVED_PRICE_FIELD: &str = "approved_price";
const TARGETS_FIELD: &str = "targets";
const INPUT_MICROS_FIELD: &str = "input_micros_per_million";
const OUTPUT_MICROS_FIELD: &str = "output_micros_per_million";
const PRICE_ID_FIELD: &str = "price_id";
const VERSION_FIELD: &str = "version";

/// The field list of each nested record the two schemas define, so a sub-record
/// is held to its schema the way the body around it is.
const OBSERVED_PRICE_FIELDS: &[&str] = &[INPUT_MICROS_FIELD, OUTPUT_MICROS_FIELD];
const APPROVED_PRICE_FIELDS: &[&str] = &[PRICE_ID_FIELD, VERSION_FIELD];
const ALIAS_TARGET_FIELDS: &[&str] = &[ENABLEMENT_ID_FIELD, VERSION_FIELD];

/// A value inside a nested record is named by its path, the way an unknown key
/// inside one is, so a refusal names the value an operator has to go and fix
/// rather than the record it sits in — and so `version` says which of the two
/// records it belongs to.
const OBSERVED_INPUT_PATH: &str = "observed_price.input_micros_per_million";
const OBSERVED_OUTPUT_PATH: &str = "observed_price.output_micros_per_million";
const APPROVED_PRICE_ID_PATH: &str = "approved_price.price_id";
const APPROVED_VERSION_PATH: &str = "approved_price.version";
const TARGET_ENABLEMENT_ID_PATH: &str = "targets.enablement_id";
const TARGET_VERSION_PATH: &str = "targets.version";

/// Why an offering identity could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidOfferingId {
    #[error("offering id `{name}` is not prefixed `{}`", OfferingId::PREFIX)]
    Prefix { name: String },
    #[error("offering id `{name}` is not 64 lowercase hex digits")]
    Digits { name: String },
}

/// A stable, opaque identity for one catalogue offering: a provider's model as a
/// catalogue lists it.
///
/// Opaque on purpose. It is a digest of the two identifiers a normalized
/// catalogue entry is keyed by, so:
///
/// - it is **stable**: refreshing the catalogue re-derives the same id for the
///   same offering, and an enablement therefore survives a refresh without being
///   rewritten;
/// - it is **fixed-width and total**: every offering has an id of one shape, so
///   nothing downstream parses upstream naming, and no upstream re-spelling of a
///   display string changes an identity a revision was published against;
/// - it **carries no vocabulary**: a body, a manifest entry, and an error print an
///   id rather than a provider's product names.
///
/// Derivation is canonical ([`OfferingId::of`]), so two builds and two catalogue
/// importers agree on it byte for byte. The catalogue import slice owns the
/// normalized identifiers it is derived *from*; this contract owns only what an
/// enablement may name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OfferingId(Checksum);

impl OfferingId {
    /// The text form's prefix, so an id is recognizable in a log line and cannot
    /// be confused with a [`Checksum`] or a typed uuid.
    pub const PREFIX: &'static str = "off_";

    /// Derive the identity of the offering `model` of `provider`.
    ///
    /// Fallible because the derivation is a canonical encoding, and the canonical
    /// encoder refuses strings a checksum must never depend on the spelling of.
    pub fn of(provider: &str, model: &str) -> Result<Self, CanonicalError> {
        let key = CanonicalValue::map([
            ("provider", CanonicalValue::string(provider)),
            ("model", CanonicalValue::string(model)),
        ]);
        // Pinned rather than `default()`: this digest is embedded in published
        // bodies and re-derived by importers, so a future default encoding must
        // not re-spell an identity a revision was published against.
        Ok(Self(Checksum::of(&SerializerVersion::V1.encode(&key)?)))
    }

    /// Parse the text form. Total on arbitrary input: the digits are checked by
    /// [`Checksum::parse`], so operator- or storage-supplied text is refused
    /// rather than able to panic a reader, and there is one spelling per identity.
    pub fn parse(text: &str) -> Result<Self, InvalidOfferingId> {
        let digits = text
            .strip_prefix(Self::PREFIX)
            .ok_or_else(|| InvalidOfferingId::Prefix {
                name: text.to_owned(),
            })?;
        Checksum::parse(&format!("sha256:{digits}"))
            .map(Self)
            .map_err(|_| InvalidOfferingId::Digits {
                name: text.to_owned(),
            })
    }
}

impl fmt::Display for OfferingId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(Self::PREFIX)?;
        for byte in self.0.as_bytes() {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// An offering identity together with the immutable catalogue snapshot it was
/// read from.
///
/// The pair is the unit an enablement names, because either half alone is
/// ambiguous about a different question: the id alone does not say which
/// catalogue asserted the offering exists, and a snapshot alone does not say
/// which of its offerings is meant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CatalogOffering {
    pub offering: OfferingId,
    /// The digest of the catalogue snapshot blob this identity was derived from.
    pub snapshot: Checksum,
}

impl CatalogOffering {
    pub const fn new(offering: OfferingId, snapshot: Checksum) -> Self {
        Self { offering, snapshot }
    }

    /// Whether this offering was read from the snapshot `digest` names.
    pub fn is_pinned_to(self, digest: Checksum) -> bool {
        self.snapshot == digest
    }
}

impl fmt::Display for CatalogOffering {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.offering, self.snapshot)
    }
}

/// The request and response shape callers get: the wire contract, not a provider.
///
/// An alias is one name, so it is one wire contract; every enablement it resolves
/// to must speak the same one, which is what stops a fallback from changing the
/// shape of a response mid-incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WireFamily {
    /// OpenAI chat completions, and what the gateway's own surface speaks.
    OpenaiChat,
    /// Anthropic messages.
    AnthropicMessages,
}

impl WireFamily {
    /// Every family, iterated by the contract tests so a new one cannot be added
    /// without its identifier being stated.
    pub const ALL: &'static [Self] = &[Self::OpenaiChat, Self::AnthropicMessages];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenaiChat => "openai-chat",
            Self::AnthropicMessages => "anthropic-messages",
        }
    }

    /// The family a stored identifier names, or `None` for text no release wrote.
    pub fn parse(input: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|family| family.as_str() == input)
    }
}

impl fmt::Display for WireFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a model resource is in service.
///
/// Two states, and no third: an enablement or an alias an operator has taken out
/// of service still exists, still has a version history, and can be put back.
/// Withdrawal is *not* deletion — a disabled row is what makes "who could use this
/// last week?" answerable — and it is not a tombstone either, because the
/// catalogue and the price it points at are still perfectly readable facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum ModelLifecycle {
    /// In service: available to be resolved, subject to policy and availability.
    #[default]
    Enabled,
    /// Withheld, reversibly. Nothing resolves it; everything about it stays
    /// readable and auditable.
    Disabled,
}

impl ModelLifecycle {
    /// Both states, in lifecycle order.
    pub const ALL: &'static [Self] = &[Self::Enabled, Self::Disabled];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }

    /// The state a stored identifier names, or `None` for text no release wrote.
    pub fn parse(input: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|state| state.as_str() == input)
    }

    pub const fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }

    /// The move from this state to `next`.
    ///
    /// Total, and idempotent by construction: asking for the state a resource is
    /// already in is [`LifecycleChange::Unchanged`] rather than a conflict, so a
    /// retried administrative call is an answer instead of a refusal. Both
    /// directions are permitted, because disabling is reversible by definition —
    /// what a *version* may not change is everything else, and that is
    /// [`ModelEnablementBody::transition_from`].
    pub fn transition_to(self, next: Self) -> LifecycleChange {
        if self == next {
            LifecycleChange::Unchanged(self)
        } else {
            LifecycleChange::Moved {
                from: self,
                to: next,
            }
        }
    }
}

impl fmt::Display for ModelLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a permitted lifecycle move did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleChange {
    /// The resource was already in the requested state.
    Unchanged(ModelLifecycle),
    /// The resource moved.
    Moved {
        from: ModelLifecycle,
        to: ModelLifecycle,
    },
}

impl LifecycleChange {
    /// The state the resource is in afterwards, either way.
    pub const fn state(self) -> ModelLifecycle {
        match self {
            Self::Unchanged(state) => state,
            Self::Moved { to, .. } => to,
        }
    }

    /// Whether this move was the one that changed the state.
    pub const fn changed(self) -> bool {
        matches!(self, Self::Moved { .. })
    }
}

/// What a new version of a model resource may not change.
///
/// A resource's identity is durable, and so is what it *is*: an enablement of one
/// offering read from one snapshot, owned by one tenant or project, speaking one
/// wire contract. A version that changed any of those would keep a name and a
/// history while meaning something else, so it is a new resource instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ModelInvariant {
    Identity,
    Owner,
    Offering,
    Snapshot,
    WireFamily,
}

impl ModelInvariant {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::Owner => "owner",
            Self::Offering => "catalogue offering",
            Self::Snapshot => "catalogue snapshot",
            Self::WireFamily => "wire family",
        }
    }
}

impl fmt::Display for ModelInvariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A version that changes something a version may not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("a new version may not change the {invariant} of a model resource")]
pub struct ForbiddenModelTransition {
    pub invariant: ModelInvariant,
}

/// Who owns a model resource: a tenant, and optionally one of its projects.
///
/// Derived from the envelope's [`ResourceScope`] rather than authored beside it,
/// so the owner in the body and the scope the row is filed under cannot disagree.
/// The absent project is what makes an enablement a *tenant default*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModelOwner {
    pub tenant: TenantId,
    pub project: Option<ProjectId>,
}

impl ModelOwner {
    pub const fn tenant(tenant: TenantId) -> Self {
        Self {
            tenant,
            project: None,
        }
    }

    pub const fn project(tenant: TenantId, project: ProjectId) -> Self {
        Self {
            tenant,
            project: Some(project),
        }
    }

    /// The owner a scope implies, if it has one. [`ResourceScope::Deployment`]
    /// has none: a model nobody owns is not enabled for anybody.
    pub const fn from_scope(scope: &ResourceScope) -> Option<Self> {
        match scope {
            ResourceScope::Deployment => None,
            ResourceScope::Tenant(tenant) => Some(Self::tenant(*tenant)),
            ResourceScope::Project { tenant, project } => Some(Self::project(*tenant, *project)),
        }
    }

    /// The scope this owner is the owner of.
    pub const fn scope(self) -> ResourceScope {
        match self.project {
            None => ResourceScope::Tenant(self.tenant),
            Some(project) => ResourceScope::Project {
                tenant: self.tenant,
                project,
            },
        }
    }

    /// Whether a resource owned by `self` may reference one owned by `other`.
    ///
    /// A project reaches its own resources and its tenant's defaults; a tenant
    /// reaches only its own. Neither reaches another tenant's, and neither reaches
    /// a sibling project's — sharing across projects would be a delegation this
    /// contract does not make on an operator's behalf.
    pub fn reaches(self, other: Self) -> bool {
        self.tenant == other.tenant && (other.project.is_none() || other.project == self.project)
    }
}

impl fmt::Display for ModelOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.project {
            None => write!(f, "{}", self.tenant),
            Some(project) => write!(f, "{}/{project}", self.tenant),
        }
    }
}

/// A rate a catalogue *publishes*, in micro-dollars per million tokens.
///
/// Recorded so an operator can see what an upstream says a model costs, and
/// deliberately inert: nothing bills against it, no conversion turns it into an
/// [`ApprovedPrice`], and [`ModelEnablementBody::billable_price`] does not look at
/// it. A catalogue refresh may change what an upstream publishes at any time
/// without human action (ADR 0042), so treating an observed rate as an effective
/// one would let an upstream edit change what a deployment charges.
///
/// Integers, in micro-dollars, because desired state has no floating-point
/// representation at all (ADR 0010): a rate that entered a checksum as a float
/// would hash differently on two platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObservedPrice {
    pub input_micros_per_million: u64,
    pub output_micros_per_million: u64,
}

impl ObservedPrice {
    pub const fn new(input_micros_per_million: u64, output_micros_per_million: u64) -> Self {
        Self {
            input_micros_per_million,
            output_micros_per_million,
        }
    }
}

impl fmt::Display for ObservedPrice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{} µ$ per 1M tokens (observed)",
            self.input_micros_per_million, self.output_micros_per_million
        )
    }
}

/// A reference to the exact price resource version an operator approved.
///
/// A reference rather than a rate: the price itself is a versioned resource whose
/// approval, effective dating, and audit trail belong to the pricing slice
/// (#201). Exact rather than latest, so a revision pins the price it was published
/// against and a later re-approval cannot change what an already-published
/// revision charged.
///
/// The type can only name a `Price` resource — [`ApprovedPrice::of`] refuses
/// anything else — so "the approved price of this enablement" cannot come to point
/// at a policy or a catalogue row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ApprovedPrice(ResourceRef);

impl ApprovedPrice {
    /// The approved price a `Price` reference names, or `None` for a reference to
    /// anything else.
    pub fn of(reference: ResourceRef) -> Option<Self> {
        (reference.kind == ResourceKind::Price).then_some(Self(reference))
    }

    /// The exact price version, at the identity and version an operator approved.
    pub fn version(price: ResourceId, version: ResourceVersionNumber) -> Self {
        Self(ResourceRef::new(ResourceKind::Price, price, version))
    }

    pub const fn reference(self) -> ResourceRef {
        self.0
    }
}

impl fmt::Display for ApprovedPrice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// One enablement an alias resolves to, at an exact version.
///
/// Exact, for the same reason an approved price is: a published alias means the
/// enablements it was published against, not whatever those ids point at later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AliasTarget {
    pub enablement: ResourceId,
    pub version: ResourceVersionNumber,
}

impl AliasTarget {
    pub const fn new(enablement: ResourceId, version: ResourceVersionNumber) -> Self {
        Self {
            enablement,
            version,
        }
    }

    /// The first version of an enablement: what an alias authored alongside one
    /// names.
    pub const fn first(enablement: ResourceId) -> Self {
        Self::new(enablement, ResourceVersionNumber::FIRST)
    }

    /// This target as a resource reference, which is also the dependency edge the
    /// envelope must declare.
    pub const fn reference(self) -> ResourceRef {
        ResourceRef::new(ResourceKind::ModelEnablement, self.enablement, self.version)
    }

    fn canonical(self) -> CanonicalValue {
        CanonicalValue::map([
            (
                ENABLEMENT_ID_FIELD,
                CanonicalValue::string(self.enablement.to_string()),
            ),
            (
                VERSION_FIELD,
                CanonicalValue::integer(i128::from(self.version.get())),
            ),
        ])
    }
}

impl fmt::Display for AliasTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.reference())
    }
}

/// Why a model body, or the set of them in a revision, was refused.
///
/// Every arm names the resource it is about, so a refusal an operator reads points
/// at one row rather than at "the revision".
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ModelError {
    #[error("{reference} is a {} resource, not a {}", found.as_str(), expected.as_str())]
    Kind {
        reference: ResourceRef,
        expected: ResourceKind,
        found: ResourceKind,
    },
    #[error("{reference} is a blob body; a model record is inline")]
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
    #[error("{reference} field `{field}` is not the type its schema defines")]
    FieldType {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} field `{field}` is not an id: {source}")]
    MalformedId {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidId,
    },
    #[error("{reference} field `{field}` is not an offering id: {source}")]
    MalformedOffering {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidOfferingId,
    },
    #[error("{reference} field `{field}` is not a checksum: {source}")]
    MalformedChecksum {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidChecksum,
    },
    #[error("{reference} field `{field}` is {found}, which is not a rate this build can read")]
    PriceRange {
        reference: ResourceRef,
        field: &'static str,
        found: i128,
    },
    /// Version `0`, which no release ever wrote: resource versions start at one.
    #[error("{reference} names version {found} of a resource; versions start at 1")]
    VersionZero { reference: ResourceRef, found: i128 },
    #[error("{reference} declares wire family `{found}`, which this build does not know")]
    UnknownWireFamily {
        reference: ResourceRef,
        found: String,
    },
    #[error("{reference} declares state `{found}`, which this build does not know")]
    UnknownLifecycle {
        reference: ResourceRef,
        found: String,
    },
    #[error("{reference} carries {declared}, but its resource identity is {identity}")]
    IdentityMismatch {
        reference: ResourceRef,
        declared: String,
        identity: ResourceId,
    },
    #[error("{reference} declares owner {declared}, which is not the scope it is filed under")]
    OwnerMismatch {
        reference: ResourceRef,
        declared: ModelOwner,
    },
    /// An alias filed at a tenant or at the deployment. An alias is the name a
    /// project's callers ask for, so a tenant-wide one would be a name with no
    /// project to be unique within.
    #[error("{reference} is an alias filed outside a project")]
    NotProjectScoped { reference: ResourceRef },
    /// An enablement whose envelope does not depend on a catalogue resource
    /// carrying the snapshot its body pins.
    #[error("{reference} pins catalogue snapshot {snapshot}, which this revision does not declare")]
    UnpinnedSnapshot {
        reference: ResourceRef,
        snapshot: Checksum,
    },
    /// Two enablements of one offering at one scope: which one applied would
    /// depend on iteration order.
    #[error("{reference} enables {offering} at a scope {conflicting} already enables it at")]
    DuplicateOffering {
        reference: ResourceRef,
        offering: OfferingId,
        conflicting: ResourceRef,
    },
    /// A body naming a resource its envelope does not depend on. The envelope's
    /// edges are what publication, hydration, and storage check reachability and
    /// isolation on, so a body reference that is not one of them would be a
    /// reference none of those layers can see.
    #[error("{reference} names {target}, which its envelope does not declare as a dependency")]
    UndeclaredTarget {
        reference: ResourceRef,
        target: ResourceRef,
    },
    #[error("{reference} names {target}, which this revision does not declare")]
    DanglingTarget {
        reference: ResourceRef,
        target: ResourceRef,
    },
    /// A reference reaching outside its owner: another tenant's row, or a sibling
    /// project's.
    #[error("{reference} names {target}, which its owner cannot reach")]
    ForeignTarget {
        reference: ResourceRef,
        target: ResourceRef,
    },
    /// An alias with nothing to resolve to. Not the same thing as a disabled
    /// alias: withdrawal is a state, and an empty target list is a name that
    /// resolves to nothing at all.
    #[error("{reference} is an alias with no targets")]
    NoTargets { reference: ResourceRef },
    /// One enablement twice in a priority list, where the second occurrence could
    /// never be reached.
    #[error("{reference} names {target} more than once")]
    DuplicateTarget {
        reference: ResourceRef,
        target: ResourceRef,
    },
    /// A target speaking a different wire contract than the name promises.
    #[error("{reference} speaks {alias}, but {target} speaks {found}")]
    WireFamilyMismatch {
        reference: ResourceRef,
        target: ResourceRef,
        alias: WireFamily,
        found: WireFamily,
    },
}

impl ModelError {
    /// Whether this refusal means *this build cannot read the body*, rather than
    /// *these rows do not agree with each other*.
    ///
    /// The same division [`TenancyError::is_incompatible`] draws, and for the same
    /// reason: a compatibility refusal tells an operator that storage is intact
    /// and that the fix is a build or a revision, while everything else is real
    /// repair work. A body declaring a schema, a field, a wire family, or a state
    /// this release does not know is the newer-build case; a contradiction between
    /// two readable rows is not.
    ///
    /// [`TenancyError::is_incompatible`]: super::tenancy::TenancyError::is_incompatible
    pub fn is_incompatible(&self) -> bool {
        match self {
            Self::Schema { .. }
            | Self::UnknownField { .. }
            | Self::UnknownWireFamily { .. }
            | Self::UnknownLifecycle { .. } => true,
            // Absence of the schema identifier only: a body written before these
            // bodies had one at all is another release's writing, while a marker
            // that is present and unreadable is `DamagedSchema` below.
            Self::MissingField { field, .. } => *field == SCHEMA_FIELD,
            Self::FieldType { .. }
            | Self::DamagedSchema { .. }
            | Self::Kind { .. }
            | Self::NotInline { .. }
            | Self::NotARecord { .. }
            | Self::MalformedId { .. }
            | Self::MalformedOffering { .. }
            | Self::MalformedChecksum { .. }
            | Self::PriceRange { .. }
            | Self::VersionZero { .. }
            | Self::IdentityMismatch { .. }
            | Self::OwnerMismatch { .. }
            | Self::NotProjectScoped { .. }
            | Self::UnpinnedSnapshot { .. }
            | Self::DuplicateOffering { .. }
            | Self::UndeclaredTarget { .. }
            | Self::DanglingTarget { .. }
            | Self::ForeignTarget { .. }
            | Self::NoTargets { .. }
            | Self::DuplicateTarget { .. }
            | Self::WireFamilyMismatch { .. } => false,
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
            | Self::FieldType { reference, .. }
            | Self::MalformedId { reference, .. }
            | Self::MalformedOffering { reference, .. }
            | Self::MalformedChecksum { reference, .. }
            | Self::PriceRange { reference, .. }
            | Self::VersionZero { reference, .. }
            | Self::UnknownWireFamily { reference, .. }
            | Self::UnknownLifecycle { reference, .. }
            | Self::IdentityMismatch { reference, .. }
            | Self::OwnerMismatch { reference, .. }
            | Self::NotProjectScoped { reference }
            | Self::UnpinnedSnapshot { reference, .. }
            | Self::DuplicateOffering { reference, .. }
            | Self::UndeclaredTarget { reference, .. }
            | Self::DanglingTarget { reference, .. }
            | Self::ForeignTarget { reference, .. }
            | Self::NoTargets { reference }
            | Self::DuplicateTarget { reference, .. }
            | Self::WireFamilyMismatch { reference, .. } => *reference,
        }
    }
}

impl BodyError for ModelError {
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

    fn field_type(reference: ResourceRef, field: &'static str) -> Self {
        Self::FieldType { reference, field }
    }
}

impl IdentifiedBody for ModelError {
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

/// One model body, read through the shared strict reader.
type ModelRecord<'a> = Record<'a, ModelError>;

/// The wire family a body declares, refused by identifier rather than guessed.
fn wire_family(record: &ModelRecord<'_>) -> Result<WireFamily, ModelError> {
    let declared = record.string(WIRE_FAMILY_FIELD)?;
    WireFamily::parse(declared).ok_or_else(|| ModelError::UnknownWireFamily {
        reference: record.reference(),
        found: declared.to_owned(),
    })
}

/// The lifecycle state a body declares.
fn lifecycle(record: &ModelRecord<'_>) -> Result<ModelLifecycle, ModelError> {
    let declared = record.string(STATE_FIELD)?;
    ModelLifecycle::parse(declared).ok_or_else(|| ModelError::UnknownLifecycle {
        reference: record.reference(),
        found: declared.to_owned(),
    })
}

/// A required value of a nested record, named by its `path` — `approved_price.price_id`
/// rather than `approved_price` — so an absence names the value that is absent.
///
/// The sub-record itself is opened by [`Record::sub_record`], which holds it to its
/// own field list, so a key a newer release added inside it is an unknown field
/// rather than a value this build drops.
fn nested<'a>(
    sub: &ModelRecord<'a>,
    name: &'static str,
    path: &'static str,
) -> Result<&'a CanonicalValue, ModelError> {
    sub.optional_value(name).ok_or(ModelError::MissingField {
        reference: sub.reference(),
        field: path,
    })
}

fn integer(
    record: &ModelRecord<'_>,
    value: &CanonicalValue,
    field: &'static str,
) -> Result<i128, ModelError> {
    match value {
        CanonicalValue::Integer(number) => Ok(*number),
        _ => Err(ModelError::FieldType {
            reference: record.reference(),
            field,
        }),
    }
}

fn version_number(
    record: &ModelRecord<'_>,
    value: &CanonicalValue,
    field: &'static str,
) -> Result<ResourceVersionNumber, ModelError> {
    let number = integer(record, value, field)?;
    u64::try_from(number)
        .ok()
        .and_then(ResourceVersionNumber::new)
        .ok_or(ModelError::VersionZero {
            reference: record.reference(),
            found: number,
        })
}

fn micros(
    record: &ModelRecord<'_>,
    value: &CanonicalValue,
    field: &'static str,
) -> Result<u64, ModelError> {
    let number = integer(record, value, field)?;
    u64::try_from(number).map_err(|_| ModelError::PriceRange {
        reference: record.reference(),
        field,
        found: number,
    })
}

/// A tenant's — or one of its projects' — permission to use one catalogue
/// offering.
///
/// The unit everything downstream asks about: availability projects it, policy
/// constrains it, an alias targets it, and billing charges against the price it
/// points at. It is deliberately not a *model*: the catalogue owns what models
/// exist, and an enablement owns whether one is usable, by whom, at what approved
/// price, and read from which snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelEnablementBody {
    enablement: ResourceId,
    owner: ModelOwner,
    offering: CatalogOffering,
    wire_family: WireFamily,
    state: ModelLifecycle,
    observed: Option<ObservedPrice>,
    approved: Option<ApprovedPrice>,
}

impl ModelEnablementBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = MODEL_ENABLEMENT_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[
        ENABLEMENT_ID_FIELD,
        TENANT_ID_FIELD,
        PROJECT_ID_FIELD,
        OFFERING_ID_FIELD,
        CATALOG_SNAPSHOT_FIELD,
        WIRE_FAMILY_FIELD,
        STATE_FIELD,
        OBSERVED_PRICE_FIELD,
        APPROVED_PRICE_FIELD,
    ];

    /// An enabled enablement of `offering`, with no price approved yet.
    ///
    /// No approved price is the honest default: enabling a model and putting a
    /// rate in service are two administrative acts, and a body that invented one
    /// would be billing against a number nobody approved.
    pub const fn new(
        enablement: ResourceId,
        owner: ModelOwner,
        offering: CatalogOffering,
        wire_family: WireFamily,
    ) -> Self {
        Self {
            enablement,
            owner,
            offering,
            wire_family,
            state: ModelLifecycle::Enabled,
            observed: None,
            approved: None,
        }
    }

    /// The same enablement, recording what the catalogue published. Inert: see
    /// [`ObservedPrice`].
    #[must_use]
    pub fn observing(mut self, observed: ObservedPrice) -> Self {
        self.observed = Some(observed);
        self
    }

    /// The same enablement, pointing at the price version an operator approved.
    #[must_use]
    pub fn approving(mut self, approved: ApprovedPrice) -> Self {
        self.approved = Some(approved);
        self
    }

    /// The same enablement in state `state`: what a new version of it publishes.
    #[must_use]
    pub fn transitioned(mut self, state: ModelLifecycle) -> Self {
        self.state = state;
        self
    }

    pub const fn enablement(&self) -> ResourceId {
        self.enablement
    }

    pub const fn owner(&self) -> ModelOwner {
        self.owner
    }

    pub const fn offering(&self) -> CatalogOffering {
        self.offering
    }

    pub const fn wire_family(&self) -> WireFamily {
        self.wire_family
    }

    pub const fn state(&self) -> ModelLifecycle {
        self.state
    }

    pub const fn is_enabled(&self) -> bool {
        self.state.is_enabled()
    }

    /// What the catalogue published, if it was recorded. Never billable.
    pub const fn observed_price(&self) -> Option<ObservedPrice> {
        self.observed
    }

    /// The price this enablement bills against, if an operator approved one.
    ///
    /// Reads [`ApprovedPrice`] and nothing else: an observed catalogue rate is not
    /// a fallback, so an enablement with an observed price and no approved one has
    /// no billable price at all.
    pub const fn billable_price(&self) -> Option<ApprovedPrice> {
        self.approved
    }

    pub const fn resource_id(&self) -> ResourceId {
        self.enablement
    }

    /// The scope this enablement's versions live at: its owner's, and only that
    /// one.
    pub const fn scope(&self) -> ResourceScope {
        self.owner.scope()
    }

    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    /// The first version of this enablement, named `slug`, pinned to the catalogue
    /// resource version `catalog`.
    ///
    /// The pin and the approved price are declared as dependencies here rather
    /// than left to the caller, so an authored enablement cannot name a snapshot
    /// or a price the envelope does not depend on.
    pub fn version(&self, slug: Slug, catalog: ResourceRef) -> ResourceVersion {
        self.version_at(slug, ResourceVersionNumber::FIRST, catalog)
    }

    /// A specific version of this enablement, for a lifecycle move or a price
    /// approval.
    pub fn version_at(
        &self,
        slug: Slug,
        version: ResourceVersionNumber,
        catalog: ResourceRef,
    ) -> ResourceVersion {
        let dependencies = std::iter::once(catalog)
            .chain(self.approved.map(ApprovedPrice::reference))
            .collect::<Vec<_>>();
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::ModelEnablement, self.resource_id(), version),
            self.scope(),
            slug,
            self.body(),
        )
        .depending_on(dependencies)
    }

    /// The move from `previous` to this body, or why there is none.
    ///
    /// A version may change the lifecycle state, the observed rate, and the
    /// approved price. It may not change what the enablement *is*: its identity,
    /// its owner, the offering it enables, the snapshot that offering was read
    /// from, or the wire family it speaks. Re-pinning a snapshot in place is
    /// specifically refused — that is how a revision's meaning would change
    /// underneath a published alias — so a new snapshot is a new enablement.
    pub fn transition_from(
        &self,
        previous: &Self,
    ) -> Result<LifecycleChange, ForbiddenModelTransition> {
        for (invariant, unchanged) in [
            (
                ModelInvariant::Identity,
                previous.enablement == self.enablement,
            ),
            (ModelInvariant::Owner, previous.owner == self.owner),
            (
                ModelInvariant::Offering,
                previous.offering.offering == self.offering.offering,
            ),
            (
                ModelInvariant::Snapshot,
                previous.offering.snapshot == self.offering.snapshot,
            ),
            (
                ModelInvariant::WireFamily,
                previous.wire_family == self.wire_family,
            ),
        ] {
            if !unchanged {
                return Err(ForbiddenModelTransition { invariant });
            }
        }
        Ok(previous.state.transition_to(self.state))
    }

    /// Read an enablement resource's body, binding it to its envelope: identity to
    /// the reference, ownership to the scope.
    pub fn read(resource: &ResourceVersion) -> Result<Self, ModelError> {
        let record = ModelRecord::open(
            resource,
            ResourceKind::ModelEnablement,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let enablement = record.typed_id(ENABLEMENT_ID_FIELD, ResourceId::parse)?;
        record.identity(enablement, enablement)?;
        let owner = ModelOwner {
            tenant: record.tenant()?,
            project: record.optional_project()?,
        };
        if resource.scope != owner.scope() {
            return Err(ModelError::OwnerMismatch {
                reference: resource.reference,
                declared: owner,
            });
        }
        let offering = OfferingId::parse(record.string(OFFERING_ID_FIELD)?).map_err(|source| {
            ModelError::MalformedOffering {
                reference: resource.reference,
                field: OFFERING_ID_FIELD,
                source,
            }
        })?;
        let snapshot =
            Checksum::parse(record.string(CATALOG_SNAPSHOT_FIELD)?).map_err(|source| {
                ModelError::MalformedChecksum {
                    reference: resource.reference,
                    field: CATALOG_SNAPSHOT_FIELD,
                    source,
                }
            })?;
        let observed = match record.optional_value(OBSERVED_PRICE_FIELD) {
            None => None,
            Some(value) => {
                let sub = record.sub_record(
                    value,
                    OBSERVED_PRICE_FIELD,
                    MODEL_ENABLEMENT_SCHEMA,
                    OBSERVED_PRICE_FIELDS,
                )?;
                let input = nested(&sub, INPUT_MICROS_FIELD, OBSERVED_INPUT_PATH)?;
                let output = nested(&sub, OUTPUT_MICROS_FIELD, OBSERVED_OUTPUT_PATH)?;
                Some(ObservedPrice::new(
                    micros(&record, input, OBSERVED_INPUT_PATH)?,
                    micros(&record, output, OBSERVED_OUTPUT_PATH)?,
                ))
            }
        };
        let approved = match record.optional_value(APPROVED_PRICE_FIELD) {
            None => None,
            Some(value) => {
                let sub = record.sub_record(
                    value,
                    APPROVED_PRICE_FIELD,
                    MODEL_ENABLEMENT_SCHEMA,
                    APPROVED_PRICE_FIELDS,
                )?;
                let price = nested(&sub, PRICE_ID_FIELD, APPROVED_PRICE_ID_PATH)?;
                let version = nested(&sub, VERSION_FIELD, APPROVED_VERSION_PATH)?;
                let CanonicalValue::String(text) = price else {
                    return Err(ModelError::FieldType {
                        reference: resource.reference,
                        field: APPROVED_PRICE_ID_PATH,
                    });
                };
                let price = ResourceId::parse(text).map_err(|source| ModelError::MalformedId {
                    reference: resource.reference,
                    field: APPROVED_PRICE_ID_PATH,
                    source,
                })?;
                Some(ApprovedPrice::version(
                    price,
                    version_number(&record, version, APPROVED_VERSION_PATH)?,
                ))
            }
        };
        Ok(Self {
            enablement,
            owner,
            offering: CatalogOffering::new(offering, snapshot),
            wire_family: wire_family(&record)?,
            state: lifecycle(&record)?,
            observed,
            approved,
        })
    }
}

impl Canonical for ModelEnablementBody {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
            (
                ENABLEMENT_ID_FIELD,
                CanonicalValue::string(self.enablement.to_string()),
            ),
            (
                TENANT_ID_FIELD,
                CanonicalValue::string(self.owner.tenant.to_string()),
            ),
            (
                OFFERING_ID_FIELD,
                CanonicalValue::string(self.offering.offering.to_string()),
            ),
            (
                CATALOG_SNAPSHOT_FIELD,
                CanonicalValue::string(self.offering.snapshot.to_string()),
            ),
            (
                WIRE_FAMILY_FIELD,
                CanonicalValue::string(self.wire_family.as_str()),
            ),
            (STATE_FIELD, CanonicalValue::string(self.state.as_str())),
        ];
        // An absent field, not a null: there is one spelling of "no project", "no
        // observed rate", and "no approved price".
        if let Some(project) = self.owner.project {
            fields.push((
                PROJECT_ID_FIELD,
                CanonicalValue::string(project.to_string()),
            ));
        }
        if let Some(observed) = self.observed {
            fields.push((
                OBSERVED_PRICE_FIELD,
                CanonicalValue::map([
                    (
                        INPUT_MICROS_FIELD,
                        CanonicalValue::integer(i128::from(observed.input_micros_per_million)),
                    ),
                    (
                        OUTPUT_MICROS_FIELD,
                        CanonicalValue::integer(i128::from(observed.output_micros_per_million)),
                    ),
                ]),
            ));
        }
        if let Some(approved) = self.approved {
            let reference = approved.reference();
            fields.push((
                APPROVED_PRICE_FIELD,
                CanonicalValue::map([
                    (
                        PRICE_ID_FIELD,
                        CanonicalValue::string(reference.id.to_string()),
                    ),
                    (
                        VERSION_FIELD,
                        CanonicalValue::integer(i128::from(reference.version.get())),
                    ),
                ]),
            ));
        }
        CanonicalValue::map(fields)
    }
}

/// A project-scoped name, and the enablements it resolves to in order.
///
/// The alias is what a caller's `model` field says, and it is a project's: two
/// projects may both publish `fast`, and nothing global may treat those as one
/// name. Targets are an ordered *list* — position is the priority a later slice
/// falls back along — so re-ordering them is a different desired state and a
/// different checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelAliasBody {
    alias: ResourceId,
    tenant: TenantId,
    project: ProjectId,
    wire_family: WireFamily,
    state: ModelLifecycle,
    targets: Vec<AliasTarget>,
}

impl ModelAliasBody {
    /// The schema identifier this body encodes and reads.
    pub const SCHEMA: &'static str = MODEL_ALIAS_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[
        ALIAS_ID_FIELD,
        TENANT_ID_FIELD,
        PROJECT_ID_FIELD,
        WIRE_FAMILY_FIELD,
        STATE_FIELD,
        TARGETS_FIELD,
    ];

    /// An enabled alias resolving to `targets`, in the order given.
    pub fn new(
        alias: ResourceId,
        tenant: TenantId,
        project: ProjectId,
        wire_family: WireFamily,
        targets: impl IntoIterator<Item = AliasTarget>,
    ) -> Self {
        Self {
            alias,
            tenant,
            project,
            wire_family,
            state: ModelLifecycle::Enabled,
            targets: targets.into_iter().collect(),
        }
    }

    /// The same alias in state `state`: what a new version of it publishes.
    #[must_use]
    pub fn transitioned(mut self, state: ModelLifecycle) -> Self {
        self.state = state;
        self
    }

    /// The same alias resolving to `targets` instead: a re-prioritization.
    #[must_use]
    pub fn retargeted(mut self, targets: impl IntoIterator<Item = AliasTarget>) -> Self {
        self.targets = targets.into_iter().collect();
        self
    }

    pub const fn alias(&self) -> ResourceId {
        self.alias
    }

    pub const fn tenant(&self) -> TenantId {
        self.tenant
    }

    pub const fn project(&self) -> ProjectId {
        self.project
    }

    pub const fn owner(&self) -> ModelOwner {
        ModelOwner::project(self.tenant, self.project)
    }

    pub const fn wire_family(&self) -> WireFamily {
        self.wire_family
    }

    pub const fn state(&self) -> ModelLifecycle {
        self.state
    }

    pub const fn is_enabled(&self) -> bool {
        self.state.is_enabled()
    }

    /// The targets, in priority order.
    pub fn targets(&self) -> &[AliasTarget] {
        &self.targets
    }

    /// The first target, which is what a resolver would try first.
    pub fn primary(&self) -> Option<AliasTarget> {
        self.targets.first().copied()
    }

    pub const fn resource_id(&self) -> ResourceId {
        self.alias
    }

    /// The scope an alias lives at: always a project's.
    pub const fn scope(&self) -> ResourceScope {
        ResourceScope::Project {
            tenant: self.tenant,
            project: self.project,
        }
    }

    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    /// The first version of this alias, named `slug`.
    ///
    /// The targets are declared as the envelope's dependencies, so an authored
    /// alias cannot resolve to something its envelope does not depend on.
    pub fn version(&self, slug: Slug) -> ResourceVersion {
        self.version_at(slug, ResourceVersionNumber::FIRST)
    }

    /// A specific version of this alias, for a rename, a re-prioritization, or a
    /// lifecycle move.
    pub fn version_at(&self, slug: Slug, version: ResourceVersionNumber) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Alias, self.resource_id(), version),
            self.scope(),
            slug,
            self.body(),
        )
        .depending_on(self.targets.iter().map(|target| target.reference()))
    }

    /// The move from `previous` to this body, or why there is none.
    ///
    /// Targets and state may change — re-prioritizing and withdrawing a name are
    /// the two things an operator does to a published alias. Its identity, its
    /// owner, and its wire family may not: callers of a name have already been
    /// written against the request and response shape it promised.
    pub fn transition_from(
        &self,
        previous: &Self,
    ) -> Result<LifecycleChange, ForbiddenModelTransition> {
        for (invariant, unchanged) in [
            (ModelInvariant::Identity, previous.alias == self.alias),
            (ModelInvariant::Owner, previous.owner() == self.owner()),
            (
                ModelInvariant::WireFamily,
                previous.wire_family == self.wire_family,
            ),
        ] {
            if !unchanged {
                return Err(ForbiddenModelTransition { invariant });
            }
        }
        Ok(previous.state.transition_to(self.state))
    }

    /// Read an alias resource's body, binding it to its envelope.
    pub fn read(resource: &ResourceVersion) -> Result<Self, ModelError> {
        let record = ModelRecord::open(
            resource,
            ResourceKind::Alias,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let alias = record.typed_id(ALIAS_ID_FIELD, ResourceId::parse)?;
        record.identity(alias, alias)?;
        let tenant = record.tenant()?;
        let project = record.project()?;
        match &resource.scope {
            ResourceScope::Project { .. } => {}
            _ => {
                return Err(ModelError::NotProjectScoped {
                    reference: resource.reference,
                });
            }
        }
        if resource.scope != (ResourceScope::Project { tenant, project }) {
            return Err(ModelError::OwnerMismatch {
                reference: resource.reference,
                declared: ModelOwner::project(tenant, project),
            });
        }
        let CanonicalValue::List(targets) = record.value(TARGETS_FIELD)? else {
            return Err(ModelError::FieldType {
                reference: resource.reference,
                field: TARGETS_FIELD,
            });
        };
        let targets = targets
            .iter()
            .map(|target| {
                let sub = record.sub_record(
                    target,
                    TARGETS_FIELD,
                    MODEL_ALIAS_SCHEMA,
                    ALIAS_TARGET_FIELDS,
                )?;
                let enablement = nested(&sub, ENABLEMENT_ID_FIELD, TARGET_ENABLEMENT_ID_PATH)?;
                let version = nested(&sub, VERSION_FIELD, TARGET_VERSION_PATH)?;
                let CanonicalValue::String(text) = enablement else {
                    return Err(ModelError::FieldType {
                        reference: resource.reference,
                        field: TARGET_ENABLEMENT_ID_PATH,
                    });
                };
                let enablement =
                    ResourceId::parse(text).map_err(|source| ModelError::MalformedId {
                        reference: resource.reference,
                        field: TARGET_ENABLEMENT_ID_PATH,
                        source,
                    })?;
                Ok(AliasTarget::new(
                    enablement,
                    version_number(&record, version, TARGET_VERSION_PATH)?,
                ))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            alias,
            tenant,
            project,
            wire_family: wire_family(&record)?,
            state: lifecycle(&record)?,
            targets,
        })
    }
}

impl Canonical for ModelAliasBody {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
            (
                ALIAS_ID_FIELD,
                CanonicalValue::string(self.alias.to_string()),
            ),
            (
                TENANT_ID_FIELD,
                CanonicalValue::string(self.tenant.to_string()),
            ),
            (
                PROJECT_ID_FIELD,
                CanonicalValue::string(self.project.to_string()),
            ),
            (
                WIRE_FAMILY_FIELD,
                CanonicalValue::string(self.wire_family.as_str()),
            ),
            (STATE_FIELD, CanonicalValue::string(self.state.as_str())),
            // A list, not a set: position is priority, so two orders of the same
            // targets are two different desired states.
            (
                TARGETS_FIELD,
                CanonicalValue::List(
                    self.targets
                        .iter()
                        .map(|target| target.canonical())
                        .collect(),
                ),
            ),
        ])
    }
}

/// An enablement as a revision holds it: its envelope, its name, and its body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelEnablement {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: ModelEnablementBody,
}

/// An alias as a revision holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelAlias {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: ModelAliasBody,
}

/// The model contracts of one revision, resolved once.
///
/// Built by [`Models::of`], which is the single place these bodies are
/// interpreted: publication, hydration, and every later projection reach the same
/// conclusions because they all call it. Ordering is by id throughout, so two
/// replicas iterate the same enablements and aliases in the same order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Models {
    enablements: BTreeMap<ResourceId, ModelEnablement>,
    aliases: BTreeMap<ResourceId, ModelAlias>,
}

impl Models {
    /// Read and resolve the model contracts of a desired state.
    ///
    /// Bodies are read first, then the rules no single body can check: that a
    /// pinned snapshot is one this revision declares, that one offering is enabled
    /// once per scope, and that every alias resolves — in order, within its own
    /// reach, and in one wire family.
    ///
    /// An *alias* row whose body declares no `schema` is skipped rather than
    /// refused, because such rows predate this slice; an untyped *enablement* is
    /// refused, because none was ever published. See the module documentation.
    pub fn of(state: &DesiredState) -> Result<Self, ModelError> {
        let mut models = Self::default();
        for resource in state.resources() {
            match resource.reference.kind {
                ResourceKind::ModelEnablement => {
                    let body = ModelEnablementBody::read(resource)?;
                    models.enablements.insert(
                        body.enablement(),
                        ModelEnablement {
                            reference: resource.reference,
                            slug: resource.slug.clone(),
                            body,
                        },
                    );
                }
                ResourceKind::Alias if is_typed(resource) => {
                    let body = ModelAliasBody::read(resource)?;
                    models.aliases.insert(
                        body.alias(),
                        ModelAlias {
                            reference: resource.reference,
                            slug: resource.slug.clone(),
                            body,
                        },
                    );
                }
                _ => {}
            }
        }

        let mut offerings: BTreeMap<(ModelOwner, OfferingId), ResourceRef> = BTreeMap::new();
        for enablement in models.enablements.values() {
            let resource = state
                .get(&enablement.reference)
                .expect("the enablement was read from this state");
            check_snapshot_pin(state, resource, &enablement.body)?;
            if let Some(approved) = enablement.body.billable_price() {
                check_reference(
                    state,
                    resource,
                    approved.reference(),
                    enablement.body.owner(),
                )?;
            }
            // Only what resolves can be ambiguous. A disabled enablement is
            // never returned to anyone, so holding an offering it no longer
            // serves would make a refreshed snapshot unreachable: a new snapshot
            // is a new enablement, and the enablement it replaces can only be
            // disabled — this state has no way to forget a resource.
            if !enablement.body.is_enabled() {
                continue;
            }
            let owner = enablement.body.owner();
            let offering = enablement.body.offering().offering;
            if let Some(conflicting) = offerings.insert((owner, offering), enablement.reference) {
                return Err(ModelError::DuplicateOffering {
                    reference: enablement.reference,
                    offering,
                    conflicting,
                });
            }
        }

        for alias in models.aliases.values() {
            let resource = state
                .get(&alias.reference)
                .expect("the alias was read from this state");
            models.check_targets(state, resource, &alias.body)?;
        }
        Ok(models)
    }

    /// Every target of one alias: declared, reachable, unique, and speaking the
    /// alias's wire family.
    fn check_targets(
        &self,
        state: &DesiredState,
        resource: &ResourceVersion,
        body: &ModelAliasBody,
    ) -> Result<(), ModelError> {
        if body.targets().is_empty() {
            return Err(ModelError::NoTargets {
                reference: resource.reference,
            });
        }
        let mut seen: Vec<ResourceId> = Vec::with_capacity(body.targets().len());
        for target in body.targets() {
            if seen.contains(&target.enablement) {
                return Err(ModelError::DuplicateTarget {
                    reference: resource.reference,
                    target: target.reference(),
                });
            }
            seen.push(target.enablement);
            check_reference(state, resource, target.reference(), body.owner())?;
            // A target this build cannot read a body for is still a declared,
            // reachable enablement; its wire family is checked when the body is
            // one this build reads.
            if let Some(enabled) = self.enablements.get(&target.enablement)
                && enabled.body.wire_family() != body.wire_family()
            {
                return Err(ModelError::WireFamilyMismatch {
                    reference: resource.reference,
                    target: target.reference(),
                    alias: body.wire_family(),
                    found: enabled.body.wire_family(),
                });
            }
        }
        Ok(())
    }

    /// Every enablement, ordered by id.
    pub fn enablements(&self) -> impl ExactSizeIterator<Item = &ModelEnablement> {
        self.enablements.values()
    }

    /// Every alias, ordered by id.
    pub fn aliases(&self) -> impl ExactSizeIterator<Item = &ModelAlias> {
        self.aliases.values()
    }

    pub fn enablement(&self, id: ResourceId) -> Option<&ModelEnablement> {
        self.enablements.get(&id)
    }

    pub fn alias(&self, id: ResourceId) -> Option<&ModelAlias> {
        self.aliases.get(&id)
    }

    /// One project's aliases, ordered by id.
    pub fn aliases_of(&self, project: ProjectId) -> impl Iterator<Item = &ModelAlias> {
        self.aliases
            .values()
            .filter(move |alias| alias.body.project() == project)
    }

    /// The tenant-wide default enablement of `offering`, if there is one.
    pub fn default_for(&self, tenant: TenantId, offering: OfferingId) -> Option<&ModelEnablement> {
        self.enablement_at(ModelOwner::tenant(tenant), offering)
    }

    /// A project's own enablement of `offering`, if it has one.
    pub fn override_for(
        &self,
        tenant: TenantId,
        project: ProjectId,
        offering: OfferingId,
    ) -> Option<&ModelEnablement> {
        self.enablement_at(ModelOwner::project(tenant, project), offering)
    }

    /// The enablement of `offering` that applies inside `project`: the project's
    /// own if it has one, otherwise its tenant's default.
    ///
    /// The precedence rule, stated once. A project override *replaces* the tenant
    /// default rather than merging with it — including when the override is
    /// disabled, which is how a tenant-wide model is withdrawn from one project
    /// without being withdrawn from the rest.
    pub fn effective_for(
        &self,
        tenant: TenantId,
        project: ProjectId,
        offering: OfferingId,
    ) -> Option<&ModelEnablement> {
        self.override_for(tenant, project, offering)
            .or_else(|| self.default_for(tenant, offering))
    }

    fn enablement_at(&self, owner: ModelOwner, offering: OfferingId) -> Option<&ModelEnablement> {
        self.enablements.values().find(|enablement| {
            enablement.body.owner() == owner && enablement.body.offering().offering == offering
        })
    }
}

/// Whether an alias body declares a schema at all, and is therefore a body this
/// slice reads strictly rather than a row written before it.
fn is_typed(resource: &ResourceVersion) -> bool {
    let ResourceBody::Inline(CanonicalValue::Map(fields)) = &resource.body else {
        return false;
    };
    fields.iter().any(|(field, _)| field == SCHEMA_FIELD)
}

/// The snapshot an enablement pins must be a snapshot this revision carries, as a
/// declared dependency of the enablement itself.
///
/// The pin resolves structurally, so a storage path that reconstructs a catalogue
/// resource must keep its blob kind and digest intact rather than rematerializing
/// the body inline; see ADR 0042. An unresolvable pin is invalid rather than skew
/// on purpose: a revision whose enablements have lost the catalogue they were
/// approved against must not converge.
fn check_snapshot_pin(
    state: &DesiredState,
    resource: &ResourceVersion,
    body: &ModelEnablementBody,
) -> Result<(), ModelError> {
    let snapshot = body.offering().snapshot;
    let pinned = resource
        .depends_on
        .iter()
        .filter(|dependency| dependency.kind == ResourceKind::CatalogModel)
        .filter_map(|dependency| state.get(dependency))
        .filter_map(|catalog| catalog.body.blob())
        .any(|blob| blob.kind == BlobKind::CatalogSnapshot && blob.digest == snapshot);
    if pinned {
        Ok(())
    } else {
        Err(ModelError::UnpinnedSnapshot {
            reference: resource.reference,
            snapshot,
        })
    }
}

/// A reference a body names: declared by the envelope, present in the revision,
/// and inside the owner's reach.
fn check_reference(
    state: &DesiredState,
    resource: &ResourceVersion,
    target: ResourceRef,
    owner: ModelOwner,
) -> Result<(), ModelError> {
    if !resource.depends_on.contains(&target) {
        return Err(ModelError::UndeclaredTarget {
            reference: resource.reference,
            target,
        });
    }
    let Some(referenced) = state.get(&target) else {
        return Err(ModelError::DanglingTarget {
            reference: resource.reference,
            target,
        });
    };
    let reachable = ModelOwner::from_scope(&referenced.scope)
        .is_some_and(|referenced| owner.reaches(referenced));
    if reachable {
        Ok(())
    } else {
        Err(ModelError::ForeignTarget {
            reference: resource.reference,
            target,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::canonical::{Canonical as _, SerializerVersion};
    use super::super::fixtures::{
        alias_body, approved_price, blob_backed_catalog, candidate, catalog_offering,
        catalog_reference, catalog_snapshot, enablement_body, observed_price, offering_id, price,
        project, project_enablement, project_id, resource_id, revision_id,
        second_blob_backed_catalog, state, state_with_models, tenant, tenant_enablement, tenant_id,
        typed_alias,
    };
    use super::super::mutation::ExpectedRevision;
    use super::super::revision::{
        BodySkew, IntegrityError, LoadedRevision, RevisionManifest, ValidationError,
    };
    use super::*;
    use std::time::SystemTime;

    /// The model refusal behind a validation error, so a test asserts on the
    /// contract that refused rather than on the boxing that keeps
    /// [`ValidationError`] small.
    fn model_error(error: &ValidationError) -> Option<&ModelError> {
        match error {
            ValidationError::Model(model) => Some(model),
            _ => None,
        }
    }

    fn owner_tenant() -> ModelOwner {
        ModelOwner::tenant(tenant_id(1))
    }

    fn owner_project() -> ModelOwner {
        ModelOwner::project(tenant_id(1), project_id(2))
    }

    /// Rewrite a resource's inline record, which is how a body a caller could
    /// never author — or a newer build's body — is put in front of the reader.
    fn with_fields(
        resource: &ResourceVersion,
        edit: impl FnOnce(&mut Vec<(String, CanonicalValue)>),
    ) -> ResourceVersion {
        let ResourceBody::Inline(CanonicalValue::Map(fields)) = &resource.body else {
            panic!("a model fixture body is an inline record");
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

    /// [`state_with_models`] with one of its resources replaced, so a test drives
    /// validation from a realistic revision rather than from a single row.
    fn state_replacing(resource: ResourceVersion) -> DesiredState {
        let mut state = DesiredState::new();
        for blob in state_with_models().blobs() {
            state.declare_blob(*blob);
        }
        for existing in state_with_models().resources() {
            let existing = if existing.reference.same_resource(&resource.reference) {
                resource.clone()
            } else {
                existing.clone()
            };
            state.insert(existing).expect("distinct references");
        }
        state
    }

    #[test]
    fn an_offering_identity_is_opaque_stable_and_pinned_to_a_snapshot() {
        let offering = OfferingId::of("openai", "gpt-4o").unwrap();
        assert_eq!(
            offering,
            OfferingId::of("openai", "gpt-4o").unwrap(),
            "the same offering derives the same id, so a catalogue refresh does \
             not rewrite an enablement"
        );
        assert_ne!(offering, OfferingId::of("openai", "gpt-4o-mini").unwrap());
        assert_ne!(
            offering,
            OfferingId::of("azure", "gpt-4o").unwrap(),
            "one model name under two providers is two offerings"
        );
        // Length-prefixed derivation: no re-cutting of the two identifiers into
        // another pair can collide.
        assert_ne!(
            OfferingId::of("openai", "gpt-4o").unwrap(),
            OfferingId::of("openaigpt", "-4o").unwrap()
        );

        let text = offering.to_string();
        assert!(text.starts_with(OfferingId::PREFIX));
        assert_eq!(text.len(), OfferingId::PREFIX.len() + 64);
        assert_eq!(OfferingId::parse(&text).unwrap(), offering);
        assert!(
            !text.contains("gpt") && !text.contains("openai"),
            "an id carries no upstream vocabulary: {text}"
        );
        assert!(matches!(
            OfferingId::parse(&text.replace("off_", "sha256:")),
            Err(InvalidOfferingId::Prefix { .. })
        ));
        assert!(matches!(
            OfferingId::parse(&text[..text.len() - 1]),
            Err(InvalidOfferingId::Digits { .. })
        ));

        // The pin is part of what an enablement names, and the pair prints both.
        let pinned = CatalogOffering::new(offering, catalog_snapshot());
        assert!(pinned.is_pinned_to(catalog_snapshot()));
        assert!(
            !pinned.is_pinned_to(other_snapshot()),
            "a pin is to one snapshot, so a refreshed catalogue does not satisfy it"
        );
        assert_eq!(
            pinned.to_string(),
            format!("{offering}@{}", catalog_snapshot())
        );
    }

    #[test]
    fn a_body_round_trips_through_its_envelope_and_its_canonical_bytes() {
        let body = enablement_body(30, owner_tenant(), "gpt-4o")
            .observing(observed_price())
            .approving(approved_price(40));
        let resource = body.version(Slug::parse("gpt-4o").unwrap(), catalog_reference());
        assert_eq!(ModelEnablementBody::read(&resource).unwrap(), body);
        assert_eq!(resource.reference.id, resource_id(30));
        assert_eq!(resource.scope, ResourceScope::Tenant(tenant_id(1)));
        assert!(
            resource.depends_on.contains(&catalog_reference())
                && resource
                    .depends_on
                    .contains(&approved_price(40).reference()),
            "an authored enablement declares the snapshot it pins and the price it \
             bills against"
        );

        let alias = alias_body(
            &tenant_id(1),
            &project_id(2),
            32,
            &[reference_of(30), reference_of(31)],
        );
        let resource = alias.version(Slug::parse("fast").unwrap());
        assert_eq!(ModelAliasBody::read(&resource).unwrap(), alias);

        // The bytes are the identity of the content, and the schema is inside them.
        let bytes = SerializerVersion::V1.encode(&alias.canonical()).unwrap();
        let decoded = SerializerVersion::V1
            .decode(&bytes)
            .expect("a model body is canonical, so storage returns what it took");
        assert_eq!(SerializerVersion::V1.encode(&decoded).unwrap(), bytes);
        assert_eq!(
            ModelAliasBody::read(&ResourceVersion {
                body: ResourceBody::Inline(decoded),
                ..resource
            })
            .unwrap(),
            alias,
            "and reads back as the same body"
        );
        assert!(String::from_utf8_lossy(&bytes).contains(MODEL_ALIAS_SCHEMA));
    }

    /// The digest of a *different* catalogue snapshot: what a pin naming a
    /// catalogue this revision does not assert looks like.
    fn other_snapshot() -> Checksum {
        second_blob_backed_catalog(6)
            .body
            .blob()
            .expect("a blob body")
            .digest
    }

    fn reference_of(seed: u64) -> ResourceRef {
        ResourceRef::new(
            ResourceKind::ModelEnablement,
            resource_id(seed),
            ResourceVersionNumber::FIRST,
        )
    }

    #[test]
    fn target_order_is_priority_and_a_reordering_is_a_different_state() {
        let first = alias_body(
            &tenant_id(1),
            &project_id(2),
            32,
            &[reference_of(31), reference_of(30)],
        );
        let flipped = first
            .clone()
            .retargeted(first.targets().iter().rev().copied().collect::<Vec<_>>());
        assert_eq!(first.primary(), Some(AliasTarget::first(resource_id(31))));
        assert_eq!(flipped.primary(), Some(AliasTarget::first(resource_id(30))));
        assert_ne!(
            first.checksum().unwrap(),
            flipped.checksum().unwrap(),
            "priority is content: reordering targets is a different revision"
        );
        assert_eq!(
            first.targets(),
            &[
                AliasTarget::first(resource_id(31)),
                AliasTarget::first(resource_id(30))
            ],
            "the authored order is preserved exactly, not sorted"
        );
    }

    #[test]
    fn tenant_defaults_and_project_overrides_resolve_by_scope() {
        let state = state_with_models();
        state.validate().expect("the fixture revision is valid");
        let models = Models::of(&state).unwrap();
        let offering = offering_id("gpt-4o");

        let default = models
            .default_for(tenant_id(1), offering)
            .expect("the tenant default");
        assert_eq!(default.body.owner(), owner_tenant());
        let over = models
            .override_for(tenant_id(1), project_id(2), offering)
            .expect("the project override");
        assert_eq!(over.body.owner(), owner_project());
        assert_eq!(
            models
                .effective_for(tenant_id(1), project_id(2), offering)
                .map(|enablement| enablement.reference),
            Some(over.reference),
            "a project's own enablement replaces its tenant's default"
        );
        assert_eq!(
            models
                .effective_for(tenant_id(1), project_id(99), offering)
                .map(|enablement| enablement.reference),
            Some(default.reference),
            "and a project without one inherits the default"
        );
        assert_eq!(models.aliases_of(project_id(2)).count(), 1);
        assert_eq!(models.aliases_of(project_id(99)).count(), 0);

        // A disabled override withdraws one project without withdrawing the
        // tenant-wide default, and stays a versioned, auditable row.
        let withdrawn = enablement_body(31, owner_project(), "gpt-4o")
            .transitioned(ModelLifecycle::Disabled)
            .version_at(
                Slug::parse("gpt-4o").unwrap(),
                ResourceVersionNumber::FIRST,
                catalog_reference(),
            );
        let state = state_replacing(withdrawn);
        state
            .validate()
            .expect("a disabled enablement is valid desired state");
        let models = Models::of(&state).unwrap();
        assert!(
            !models
                .effective_for(tenant_id(1), project_id(2), offering)
                .unwrap()
                .body
                .is_enabled()
        );
        assert!(
            models
                .default_for(tenant_id(1), offering)
                .unwrap()
                .body
                .is_enabled()
        );
    }

    #[test]
    fn one_offering_is_enabled_once_per_scope() {
        let duplicate = enablement_body(33, owner_tenant(), "gpt-4o")
            .version(Slug::parse("gpt-4o-again").unwrap(), catalog_reference());
        let mut state = state_with_models();
        state.insert(duplicate).expect("a distinct reference");
        let error = state
            .validate()
            .expect_err("two tenant defaults for one offering are ambiguous");
        assert!(
            matches!(
                model_error(&error),
                Some(ModelError::DuplicateOffering { .. })
            ),
            "{error}"
        );
    }

    /// The replacement path: an offering a disabled enablement used to serve is
    /// free for the enablement that replaces it. Without this a refreshed
    /// snapshot could never be enabled, since a new snapshot is a new enablement
    /// and desired state has no way to drop the old one.
    #[test]
    fn a_disabled_enablement_does_not_hold_the_offering_that_replaces_it() {
        let replacement = enablement_body(33, owner_tenant(), "gpt-4o").version(
            Slug::parse("gpt-4o-refreshed").unwrap(),
            catalog_reference(),
        );
        let mut state = state_with_models();
        let retired = state
            .resources()
            .find(|resource| {
                ModelEnablementBody::read(resource)
                    .is_ok_and(|body| body.offering().offering == offering_id("gpt-4o"))
            })
            .cloned()
            .expect("the state enables gpt-4o");
        let body = ModelEnablementBody::read(&retired)
            .expect("an enablement body")
            .transitioned(ModelLifecycle::Disabled);
        let disabled = body.version_at(
            retired.slug.clone(),
            retired.reference.version.next(),
            catalog_reference(),
        );
        let disabled_reference = disabled.reference;
        state
            .supersede(disabled)
            .expect("disabling advances the enablement");
        // Every alias that named the enablement follows it to the version that
        // disabled it, exactly as an administrative edit carries dependents.
        let dependents: Vec<ResourceVersion> = state
            .resources()
            .filter(|resource| resource.depends_on.contains(&retired.reference))
            .cloned()
            .collect();
        for dependent in dependents {
            let alias = ModelAliasBody::read(&dependent).expect("an alias body");
            let targets: Vec<AliasTarget> = alias
                .targets()
                .iter()
                .map(|target| {
                    if target.enablement == disabled_reference.id {
                        AliasTarget::new(target.enablement, disabled_reference.version)
                    } else {
                        *target
                    }
                })
                .collect();
            state
                .supersede(
                    alias
                        .retargeted(targets)
                        .version_at(dependent.slug.clone(), dependent.reference.version.next()),
                )
                .expect("the alias follows its target");
        }
        state.insert(replacement).expect("a distinct reference");
        state
            .validate()
            .expect("only what resolves can be ambiguous");
    }

    #[test]
    fn an_enablement_is_pinned_to_a_snapshot_the_revision_declares() {
        // A snapshot no resource in the revision carries: the pin names a
        // catalogue nothing here asserts.
        let unpinned = ModelEnablementBody::new(
            resource_id(30),
            owner_tenant(),
            CatalogOffering::new(offering_id("gpt-4o"), other_snapshot()),
            WireFamily::OpenaiChat,
        )
        .version(Slug::parse("gpt-4o").unwrap(), catalog_reference());
        let error = state_replacing(unpinned)
            .validate()
            .expect_err("a pin naming an undeclared snapshot must be refused");
        assert!(
            matches!(
                model_error(&error),
                Some(ModelError::UnpinnedSnapshot { .. })
            ),
            "{error}"
        );

        // The same body without the dependency edge is refused too: the pin has to
        // be visible to the layers that check reachability.
        let undeclared = ResourceVersion::new(
            reference_of(30),
            ResourceScope::Tenant(tenant_id(1)),
            Slug::parse("gpt-4o").unwrap(),
            enablement_body(30, owner_tenant(), "gpt-4o").body(),
        );
        let error = state_replacing(undeclared)
            .validate()
            .expect_err("an unpinned enablement must be refused");
        assert!(
            matches!(
                model_error(&error),
                Some(ModelError::UnpinnedSnapshot { .. })
            ),
            "{error}"
        );
    }

    #[test]
    fn an_observed_catalogue_rate_is_not_an_approved_price() {
        let observed = enablement_body(30, owner_tenant(), "gpt-4o").observing(observed_price());
        assert_eq!(observed.observed_price(), Some(observed_price()));
        assert_eq!(
            observed.billable_price(),
            None,
            "what a catalogue publishes is not what a deployment charges"
        );

        let approved = observed.clone().approving(approved_price(40));
        assert_eq!(approved.billable_price(), Some(approved_price(40)));
        assert_ne!(
            observed.checksum().unwrap(),
            approved.checksum().unwrap(),
            "approving a price is a change to desired state"
        );
        assert_eq!(
            ApprovedPrice::of(catalog_reference()),
            None,
            "only a price resource can be an approved price"
        );

        // An approved price is a declared, present, reachable reference.
        let enablement = enablement_body(30, owner_tenant(), "gpt-4o")
            .approving(approved_price(40))
            .version(Slug::parse("gpt-4o").unwrap(), catalog_reference());
        let mut state = state_replacing(enablement.clone());
        state
            .insert(price(&tenant_id(1), 40, "gpt-4o-rate"))
            .expect("a distinct reference");
        state
            .validate()
            .expect("an approved price of the same tenant is reachable");

        // Another tenant's price is not.
        let mut state = state_replacing(enablement);
        state
            .insert(tenant(11, "globex"))
            .and_then(|state| state.insert(price(&tenant_id(11), 40, "foreign-rate")))
            .expect("a second tenant");
        let error = state
            .validate()
            .expect_err("a price belonging to another tenant is unreachable");
        assert!(
            matches!(error, ValidationError::CrossTenantReference { .. })
                || matches!(model_error(&error), Some(ModelError::ForeignTarget { .. })),
            "{error}"
        );
    }

    #[test]
    fn an_alias_resolves_in_order_within_its_own_reach_and_one_wire_family() {
        let (tenant, project) = (tenant_id(1), project_id(2));

        // A target this revision does not declare.
        let dangling = typed_alias(
            &tenant,
            &project,
            32,
            "fast",
            &[reference_of(31), reference_of(77)],
        );
        let error = state_replacing(dangling)
            .validate()
            .expect_err("an alias cannot resolve to a row that is not here");
        assert!(
            matches!(error, ValidationError::DanglingResourceReference { .. })
                || matches!(model_error(&error), Some(ModelError::DanglingTarget { .. })),
            "{error}"
        );

        // One enablement twice: the second occurrence could never be reached.
        let duplicated = typed_alias(
            &tenant,
            &project,
            32,
            "fast",
            &[reference_of(31), reference_of(31)],
        );
        let error = state_replacing(duplicated)
            .validate()
            .expect_err("a priority list may not repeat a target");
        assert!(
            matches!(
                model_error(&error),
                Some(ModelError::DuplicateTarget { .. })
            ),
            "{error}"
        );

        // A sibling project's enablement: same tenant, wrong scope.
        let sibling = project_enablement(&tenant, &project_id(9), 34, "gpt-4o-mini");
        let reaching = typed_alias(
            &tenant,
            &project,
            32,
            "fast",
            &[reference_of(31), sibling.reference],
        );
        let mut state = state_replacing(reaching);
        state
            .insert(super::super::fixtures::project(&tenant, 9, "other"))
            .and_then(|state| state.insert(sibling))
            .expect("a second project of the same tenant");
        let error = state
            .validate()
            .expect_err("an alias does not reach a sibling project's enablement");
        assert!(
            matches!(model_error(&error), Some(ModelError::ForeignTarget { .. })),
            "{error}"
        );

        // A target speaking another wire contract.
        let anthropic = ModelEnablementBody::new(
            resource_id(35),
            ModelOwner::tenant(tenant),
            catalog_offering("claude-sonnet"),
            WireFamily::AnthropicMessages,
        )
        .version(Slug::parse("claude-sonnet").unwrap(), catalog_reference());
        let mixed = typed_alias(
            &tenant,
            &project,
            32,
            "fast",
            &[reference_of(31), anthropic.reference],
        );
        let mut state = state_replacing(mixed);
        state.insert(anthropic).expect("a distinct reference");
        let error = state
            .validate()
            .expect_err("one name cannot mean two request shapes");
        assert!(
            matches!(
                model_error(&error),
                Some(ModelError::WireFamilyMismatch { .. })
            ),
            "{error}"
        );

        // A name with nothing behind it.
        let empty = typed_alias(&tenant, &project, 32, "fast", &[]);
        let error = state_replacing(empty)
            .validate()
            .expect_err("an alias with no targets resolves to nothing");
        assert!(
            matches!(model_error(&error), Some(ModelError::NoTargets { .. })),
            "{error}"
        );
    }

    #[test]
    fn an_alias_is_a_project_scoped_name_unique_within_its_project() {
        let tenant = tenant_id(1);
        // The same name in two projects of one tenant is two names.
        let sibling_project = project(&tenant, 9, "other");
        let sibling_enablement = project_enablement(&tenant, &project_id(9), 34, "gpt-4o");
        let sibling_alias = typed_alias(
            &tenant,
            &project_id(9),
            36,
            "fast",
            &[sibling_enablement.reference],
        );
        let mut state = state_with_models();
        state
            .insert(sibling_project)
            .and_then(|state| state.insert(sibling_enablement))
            .and_then(|state| state.insert(sibling_alias))
            .expect("distinct references");
        state
            .validate()
            .expect("`fast` in two projects is two aliases, not a collision");

        // The same name twice in one project is a collision, refused by the
        // envelope's own slug rule.
        let second = typed_alias(&tenant, &project_id(2), 37, "fast", &[reference_of(31)]);
        let mut state = state_with_models();
        state.insert(second).expect("a distinct reference");
        let error = state
            .validate()
            .expect_err("one project cannot publish one name twice");
        assert!(
            matches!(error, ValidationError::DuplicateSlug { .. }),
            "{error}"
        );

        // An alias filed outside a project is not an alias.
        let body = alias_body(&tenant, &project_id(2), 32, &[reference_of(31)]);
        let outside = ResourceVersion::new(
            ResourceRef::new(
                ResourceKind::Alias,
                resource_id(32),
                ResourceVersionNumber::FIRST,
            ),
            ResourceScope::Tenant(tenant),
            Slug::parse("fast").unwrap(),
            body.body(),
        );
        assert_eq!(
            ModelAliasBody::read(&outside),
            Err(ModelError::NotProjectScoped {
                reference: outside.reference
            })
        );
    }

    #[test]
    fn a_body_is_bound_to_the_envelope_it_is_filed_under() {
        let enablement = tenant_enablement(&tenant_id(1), 30, "gpt-4o");
        let renamed = ResourceVersion {
            reference: reference_of(99),
            ..enablement.clone()
        };
        assert!(matches!(
            ModelEnablementBody::read(&renamed),
            Err(ModelError::IdentityMismatch { .. })
        ));

        let misfiled = ResourceVersion {
            scope: ResourceScope::Project {
                tenant: tenant_id(1),
                project: project_id(2),
            },
            ..enablement
        };
        assert_eq!(
            ModelEnablementBody::read(&misfiled),
            Err(ModelError::OwnerMismatch {
                reference: misfiled.reference,
                declared: owner_tenant()
            }),
            "a tenant default filed inside a project would be an override nobody \
             authored"
        );
    }

    #[test]
    fn a_lifecycle_move_is_total_and_a_version_may_not_change_what_a_model_is() {
        for from in ModelLifecycle::ALL.iter().copied() {
            for to in ModelLifecycle::ALL.iter().copied() {
                let change = from.transition_to(to);
                assert_eq!(change.state(), to);
                assert_eq!(change.changed(), from != to);
                assert_eq!(
                    ModelLifecycle::parse(to.as_str()),
                    Some(to),
                    "every state has one identifier"
                );
            }
        }
        assert_eq!(ModelLifecycle::parse("retired"), None);
        assert_eq!(WireFamily::parse("openai-responses"), None);

        let enabled = enablement_body(30, owner_tenant(), "gpt-4o");
        let disabled = enabled.clone().transitioned(ModelLifecycle::Disabled);
        assert_eq!(
            disabled.transition_from(&enabled),
            Ok(LifecycleChange::Moved {
                from: ModelLifecycle::Enabled,
                to: ModelLifecycle::Disabled
            })
        );
        assert_eq!(
            enabled.transition_from(&enabled),
            Ok(LifecycleChange::Unchanged(ModelLifecycle::Enabled)),
            "a retried administrative call is an answer, not a conflict"
        );
        // Approving a price alongside a lifecycle move is permitted; changing what
        // the enablement *is* never is.
        assert!(
            disabled
                .clone()
                .approving(approved_price(40))
                .transition_from(&enabled)
                .is_ok()
        );
        for (next, invariant) in [
            (
                enablement_body(30, owner_project(), "gpt-4o"),
                ModelInvariant::Owner,
            ),
            (
                enablement_body(30, owner_tenant(), "gpt-4o-mini"),
                ModelInvariant::Offering,
            ),
            (
                enablement_body(31, owner_tenant(), "gpt-4o"),
                ModelInvariant::Identity,
            ),
        ] {
            assert_eq!(
                next.transition_from(&enabled),
                Err(ForbiddenModelTransition { invariant }),
                "{invariant} is durable across versions"
            );
        }
        let repinned = ModelEnablementBody::new(
            resource_id(30),
            owner_tenant(),
            CatalogOffering::new(offering_id("gpt-4o"), other_snapshot()),
            WireFamily::OpenaiChat,
        );
        assert_eq!(
            repinned.transition_from(&enabled),
            Err(ForbiddenModelTransition {
                invariant: ModelInvariant::Snapshot
            }),
            "a refreshed catalogue is a new enablement, not a re-pinned one"
        );

        let alias = alias_body(&tenant_id(1), &project_id(2), 32, &[reference_of(31)]);
        assert!(
            alias
                .clone()
                .retargeted([AliasTarget::first(resource_id(30))])
                .transition_from(&alias)
                .is_ok(),
            "re-prioritizing is what an operator does to a published name"
        );
        let other_family = ModelAliasBody::new(
            resource_id(32),
            tenant_id(1),
            project_id(2),
            WireFamily::AnthropicMessages,
            [AliasTarget::first(resource_id(31))],
        );
        assert_eq!(
            other_family.transition_from(&alias),
            Err(ForbiddenModelTransition {
                invariant: ModelInvariant::WireFamily
            }),
            "callers of a name were written against the shape it promised"
        );
    }

    #[test]
    fn a_schema_this_build_does_not_read_is_refused_rather_than_guessed_at() {
        let resource = tenant_enablement(&tenant_id(1), 30, "gpt-4o");
        let newer = with_fields(&resource, |fields| {
            set(
                fields,
                SCHEMA_FIELD,
                CanonicalValue::string("axond.model-enablement.v2"),
            );
        });
        assert_eq!(
            ModelEnablementBody::read(&newer),
            Err(ModelError::Schema {
                reference: newer.reference,
                expected: MODEL_ENABLEMENT_SCHEMA,
                found: "axond.model-enablement.v2".to_owned()
            })
        );
        let extended = with_fields(&resource, |fields| {
            set(fields, "residency", CanonicalValue::string("eu"));
        });
        assert_eq!(
            ModelEnablementBody::read(&extended),
            Err(ModelError::UnknownField {
                reference: extended.reference,
                schema: MODEL_ENABLEMENT_SCHEMA,
                field: "residency".to_owned()
            })
        );
        for error in [
            ModelEnablementBody::read(&newer).unwrap_err(),
            ModelEnablementBody::read(&extended).unwrap_err(),
            ModelEnablementBody::read(&with_fields(&resource, |fields| {
                set(fields, STATE_FIELD, CanonicalValue::string("retired"));
            }))
            .unwrap_err(),
            ModelEnablementBody::read(&with_fields(&resource, |fields| {
                set(
                    fields,
                    WIRE_FAMILY_FIELD,
                    CanonicalValue::string("openai-responses"),
                );
            }))
            .unwrap_err(),
        ] {
            assert!(
                error.is_incompatible(),
                "a body a newer release wrote is a compatibility refusal: {error}"
            );
            assert_eq!(error.reference(), resource.reference);
        }

        // A body that declares a schema this build reads and then is not one is
        // damage, not skew.
        let damaged = with_fields(&resource, |fields| {
            fields.retain(|(name, _)| name != WIRE_FAMILY_FIELD);
        });
        let error = ModelEnablementBody::read(&damaged).unwrap_err();
        assert_eq!(
            error,
            ModelError::MissingField {
                reference: damaged.reference,
                field: WIRE_FAMILY_FIELD
            }
        );
        assert!(!error.is_incompatible());

        // Malformed values of fields this build does read are typed refusals too.
        for (field, value, expected) in [
            (
                OFFERING_ID_FIELD,
                CanonicalValue::string("gpt-4o"),
                "offering",
            ),
            (
                CATALOG_SNAPSHOT_FIELD,
                CanonicalValue::string("sha512:0"),
                "checksum",
            ),
            (TENANT_ID_FIELD, CanonicalValue::string("acme"), "id"),
            (STATE_FIELD, CanonicalValue::integer(1), "type"),
        ] {
            let broken = with_fields(&resource, |fields| set(fields, field, value));
            let error = ModelEnablementBody::read(&broken)
                .expect_err("a malformed field is a typed refusal");
            assert!(
                error.to_string().contains(expected),
                "{error} should say what {field} is not"
            );
        }
    }

    /// Extend the nested record at `outer` with `field`, as a newer release that
    /// added a key inside a sub-record would have written it.
    fn extend_nested(resource: &ResourceVersion, outer: &str, field: &str) -> ResourceVersion {
        with_fields(resource, |fields| {
            let (_, value) = fields
                .iter_mut()
                .find(|(name, _)| name == outer)
                .expect("the fixture body carries the nested field");
            let CanonicalValue::Map(nested) = value else {
                panic!("{outer} is a nested record");
            };
            nested.push((field.to_owned(), CanonicalValue::string("later")));
        })
    }

    #[test]
    fn a_field_a_newer_release_added_inside_a_nested_record_is_refused_too() {
        // A sub-record is part of its schema, so extending one is the same skew as
        // extending the body around it: refused, never read past and dropped.
        let enablement = enablement_body(30, owner_tenant(), "gpt-4o")
            .observing(observed_price())
            .approving(approved_price(40))
            .version(Slug::parse("gpt-4o").unwrap(), catalog_reference());
        for (outer, field, schema) in [
            (
                OBSERVED_PRICE_FIELD,
                "cached_input_micros_per_million",
                MODEL_ENABLEMENT_SCHEMA,
            ),
            (
                APPROVED_PRICE_FIELD,
                "effective_from",
                MODEL_ENABLEMENT_SCHEMA,
            ),
        ] {
            let extended = extend_nested(&enablement, outer, field);
            let error = ModelEnablementBody::read(&extended).expect_err("an extended sub-record");
            assert_eq!(
                error,
                ModelError::UnknownField {
                    reference: extended.reference,
                    schema,
                    field: format!("{outer}.{field}")
                }
            );
            assert!(
                error.is_incompatible(),
                "a body a newer release wrote is a compatibility refusal: {error}"
            );
        }

        let alias = typed_alias(
            &tenant_id(1),
            &project_id(2),
            32,
            "fast",
            &[reference_of(30)],
        );
        let extended = with_first_target(&alias, |target| {
            target.push(("weight".to_owned(), CanonicalValue::integer(1)));
        });
        let error = ModelAliasBody::read(&extended).expect_err("an extended target");
        assert_eq!(
            error,
            ModelError::UnknownField {
                reference: extended.reference,
                schema: MODEL_ALIAS_SCHEMA,
                field: format!("{TARGETS_FIELD}.weight")
            }
        );
        assert!(error.is_incompatible());

        // The revision refuses to validate at all, so nothing converges on a body
        // it read only part of.
        let error = state_replacing(extended)
            .validate()
            .expect_err("a revision carrying an extended sub-record is not valid");
        assert!(
            matches!(model_error(&error), Some(ModelError::UnknownField { .. })),
            "{error}"
        );
    }

    /// Rewrite the first target of a typed alias, as a body carrying a damaged
    /// target would have been written.
    fn with_first_target(
        resource: &ResourceVersion,
        mutate: impl FnOnce(&mut Vec<(String, CanonicalValue)>),
    ) -> ResourceVersion {
        with_fields(resource, |fields| {
            let (_, value) = fields
                .iter_mut()
                .find(|(name, _)| name == TARGETS_FIELD)
                .expect("a typed alias carries targets");
            let CanonicalValue::List(targets) = value else {
                panic!("targets is a list");
            };
            let CanonicalValue::Map(target) = &mut targets[0] else {
                panic!("a target is a nested record");
            };
            mutate(target);
        })
    }

    #[test]
    fn a_value_missing_inside_a_nested_record_is_named_by_its_path() {
        // An operator repairing a refused revision is told which value to write,
        // so a sub-record's absence names the value rather than the record around
        // it — and `version` says which record it belongs to.
        let enablement = enablement_body(30, owner_tenant(), "gpt-4o")
            .observing(observed_price())
            .approving(approved_price(40))
            .version(Slug::parse("gpt-4o").unwrap(), catalog_reference());
        for (outer, field, path) in [
            (
                OBSERVED_PRICE_FIELD,
                INPUT_MICROS_FIELD,
                OBSERVED_INPUT_PATH,
            ),
            (
                OBSERVED_PRICE_FIELD,
                OUTPUT_MICROS_FIELD,
                OBSERVED_OUTPUT_PATH,
            ),
            (APPROVED_PRICE_FIELD, PRICE_ID_FIELD, APPROVED_PRICE_ID_PATH),
            (APPROVED_PRICE_FIELD, VERSION_FIELD, APPROVED_VERSION_PATH),
        ] {
            let damaged = with_fields(&enablement, |fields| {
                let (_, value) = fields
                    .iter_mut()
                    .find(|(name, _)| name == outer)
                    .expect("the fixture body carries the nested record");
                let CanonicalValue::Map(nested) = value else {
                    panic!("{outer} is a nested record");
                };
                nested.retain(|(name, _)| name != field);
            });
            let error = ModelEnablementBody::read(&damaged)
                .expect_err("a sub-record missing a value is refused");
            assert_eq!(
                error,
                ModelError::MissingField {
                    reference: damaged.reference,
                    field: path
                }
            );
            assert!(
                !error.is_incompatible(),
                "a value this build reads is damage, not skew: {error}"
            );
        }

        // A wrongly typed inner value is named by its path too, so the two
        // refusals point at the same place.
        let damaged = with_fields(&enablement, |fields| {
            let (_, value) = fields
                .iter_mut()
                .find(|(name, _)| name == APPROVED_PRICE_FIELD)
                .expect("the fixture body carries an approved price");
            let CanonicalValue::Map(nested) = value else {
                panic!("approved_price is a nested record");
            };
            set(nested, PRICE_ID_FIELD, CanonicalValue::integer(1));
        });
        assert_eq!(
            ModelEnablementBody::read(&damaged).expect_err("a price id that is not text"),
            ModelError::FieldType {
                reference: damaged.reference,
                field: APPROVED_PRICE_ID_PATH
            }
        );

        let alias = typed_alias(
            &tenant_id(1),
            &project_id(2),
            32,
            "fast",
            &[reference_of(30)],
        );
        for (field, path) in [
            (ENABLEMENT_ID_FIELD, TARGET_ENABLEMENT_ID_PATH),
            (VERSION_FIELD, TARGET_VERSION_PATH),
        ] {
            let damaged = with_first_target(&alias, |target| {
                target.retain(|(name, _)| name != field);
            });
            assert_eq!(
                ModelAliasBody::read(&damaged).expect_err("a target missing a value is refused"),
                ModelError::MissingField {
                    reference: damaged.reference,
                    field: path
                }
            );
        }
        let damaged = with_first_target(&alias, |target| {
            set(target, ENABLEMENT_ID_FIELD, CanonicalValue::integer(1));
        });
        assert_eq!(
            ModelAliasBody::read(&damaged).expect_err("a target id that is not text"),
            ModelError::FieldType {
                reference: damaged.reference,
                field: TARGET_ENABLEMENT_ID_PATH
            }
        );
    }

    #[test]
    fn an_alias_whose_schema_marker_is_damaged_is_refused_rather_than_skipped() {
        // The upgrade accommodation is a body with no `schema` key at all. A key
        // that is present but is not text is damage, so reading it as a row
        // predating this slice would let it skip the scope, target, reach, and
        // wire-family rules with nothing reported.
        //
        // And damage is what it is *called*, too: no release wrote a marker that
        // is not an identifier, so reporting it as skew would send an operator to
        // roll a build forward when the row needs restoring.
        let alias = typed_alias(
            &tenant_id(1),
            &project_id(2),
            32,
            "fast",
            &[reference_of(30)],
        );
        for marker in [
            CanonicalValue::integer(1),
            CanonicalValue::List(vec![CanonicalValue::string(MODEL_ALIAS_SCHEMA)]),
            CanonicalValue::map([(SCHEMA_FIELD, CanonicalValue::string(MODEL_ALIAS_SCHEMA))]),
        ] {
            let damaged = with_fields(&alias, |fields| {
                set(fields, SCHEMA_FIELD, marker.clone());
            });
            let state = state_replacing(damaged.clone());

            let error = Models::of(&state).expect_err("a damaged schema marker is refused");
            assert_eq!(
                error,
                ModelError::DamagedSchema {
                    reference: damaged.reference
                }
            );
            assert!(
                !error.is_incompatible(),
                "a marker no release wrote is corruption, not skew: {error}"
            );
            let detail = error.to_string();
            assert!(
                detail.contains("no release wrote") && detail.contains("restore"),
                "a corruption refusal must say what to do: {detail}"
            );
            let error = state
                .validate()
                .expect_err("a revision carrying a damaged alias is not valid");
            assert!(
                matches!(model_error(&error), Some(ModelError::DamagedSchema { .. })),
                "{error}"
            );
        }
    }

    #[test]
    fn a_damaged_alias_marker_hydrates_as_damage_not_skew() {
        // The companion to
        // `a_body_this_build_cannot_read_hydrates_as_an_incompatibility_not_corruption`:
        // a marker naming a schema this build does not read is another release's
        // revision, while a marker that is not an identifier at all is a row to
        // repair. Hydration is where the two stop being the same alert.
        let candidate = candidate(ExpectedRevision::Empty, "models", state_with_models());
        let manifest =
            RevisionManifest::of(revision_id(1), None, SystemTime::UNIX_EPOCH, &candidate)
                .expect("the fixture state is publishable");

        let mut damaged = DesiredState::new();
        for blob in candidate.state.blobs() {
            damaged.declare_blob(*blob);
        }
        for resource in candidate.state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Alias {
                with_fields(resource, |fields| {
                    set(fields, SCHEMA_FIELD, CanonicalValue::integer(1));
                })
            } else {
                resource.clone()
            };
            damaged.insert(resource).expect("distinct references");
        }
        let error = LoadedRevision::assemble(manifest, damaged)
            .expect_err("a damaged alias marker must not hydrate");
        assert!(
            matches!(
                error,
                IntegrityError::Invalid(ValidationError::Model(ref refusal))
                    if matches!(**refusal, ModelError::DamagedSchema { .. })
            ),
            "{error}"
        );
        assert!(
            !error.is_incompatible(),
            "damaged storage is not a build to roll forward: {error}"
        );
        assert!(
            error.to_string().contains("restore the row"),
            "the alert must name the repair: {error}"
        );
    }

    #[test]
    fn alias_rows_published_before_this_slice_still_load() {
        // `state` carries an untyped alias, as a build predating typed model
        // bodies wrote it: skipped rather than refused, so an existing revision
        // keeps hydrating on upgrade.
        let state = state();
        state
            .validate()
            .expect("an untyped alias body is not this build's to read");
        let models = Models::of(&state).unwrap();
        assert_eq!(models.aliases().len(), 0);
        assert_eq!(models.enablements().len(), 0);
    }

    #[test]
    fn an_untyped_enablement_is_refused_rather_than_skipped() {
        // No release ever wrote an untyped enablement, so skipping one would be an
        // entitlement hole: a row nothing reads is a row nothing binds to a scope
        // or pins to a snapshot.
        let typed = state_with_models();
        let mut state = DesiredState::new();
        for blob in typed.blobs() {
            state.declare_blob(*blob);
        }
        for resource in typed.resources() {
            let resource = if resource.reference.kind == ResourceKind::ModelEnablement {
                with_fields(resource, |fields| {
                    fields.retain(|(field, _)| field != SCHEMA_FIELD);
                })
            } else {
                resource.clone()
            };
            state.insert(resource).expect("distinct references");
        }

        let error = Models::of(&state).expect_err("an untyped enablement is refused");
        assert!(
            matches!(
                error,
                ModelError::MissingField {
                    field: SCHEMA_FIELD,
                    ..
                }
            ),
            "{error} should name the missing schema"
        );
        assert!(
            error.is_incompatible(),
            "a body with no schema is a compatibility refusal, not corruption"
        );
    }

    #[test]
    fn a_body_this_build_cannot_read_hydrates_as_an_incompatibility_not_corruption() {
        let candidate = candidate(ExpectedRevision::Empty, "models", state_with_models());
        let manifest =
            RevisionManifest::of(revision_id(1), None, SystemTime::UNIX_EPOCH, &candidate)
                .expect("the fixture state is publishable");

        let mut newer = DesiredState::new();
        for blob in candidate.state.blobs() {
            newer.declare_blob(*blob);
        }
        for resource in candidate.state.resources() {
            let resource = if resource.reference.kind == ResourceKind::Alias {
                with_fields(resource, |fields| {
                    set(
                        fields,
                        SCHEMA_FIELD,
                        CanonicalValue::string("axond.model-alias.v2"),
                    );
                })
            } else {
                resource.clone()
            };
            newer.insert(resource).expect("distinct references");
        }
        let error = LoadedRevision::assemble(manifest, newer)
            .expect_err("a newer alias schema must not hydrate");
        assert!(
            matches!(
                error,
                IntegrityError::Incompatible(BodySkew::Model(ref skew))
                    if matches!(**skew, ModelError::Schema { .. })
            ),
            "{error}"
        );
        assert!(error.is_incompatible());
        assert!(
            !error.to_string().contains("unreadable"),
            "intact storage must not be described as unreadable: {error}"
        );

        // A contradiction between two readable rows is repair work, not skew.
        assert!(
            !ModelError::DuplicateOffering {
                reference: reference_of(30),
                offering: offering_id("gpt-4o"),
                conflicting: reference_of(31),
            }
            .is_incompatible()
        );
    }

    #[test]
    fn an_owner_is_the_scope_it_came_from_and_reaches_only_what_it_owns() {
        let tenant = tenant_id(1);
        let project = project_id(2);
        assert_eq!(
            ModelOwner::from_scope(&ResourceScope::Tenant(tenant)),
            Some(ModelOwner::tenant(tenant))
        );
        assert_eq!(
            ModelOwner::from_scope(&ResourceScope::Project { tenant, project }),
            Some(ModelOwner::project(tenant, project))
        );
        assert_eq!(ModelOwner::from_scope(&ResourceScope::Deployment), None);
        for owner in [
            ModelOwner::tenant(tenant),
            ModelOwner::project(tenant, project),
        ] {
            assert_eq!(ModelOwner::from_scope(&owner.scope()), Some(owner));
            assert!(owner.reaches(ModelOwner::tenant(tenant)));
            assert!(!owner.reaches(ModelOwner::tenant(tenant_id(11))));
            assert!(!owner.reaches(ModelOwner::project(tenant, project_id(9))));
        }
        assert!(
            !ModelOwner::tenant(tenant).reaches(ModelOwner::project(tenant, project)),
            "a tenant default does not reach into one of its projects"
        );
        assert_eq!(
            ModelOwner::project(tenant, project).to_string(),
            format!("{tenant}/{project}")
        );
        // A catalogue snapshot is deployment-scoped, so it has no owner at all —
        // which is why the pin is checked as a declared blob rather than as reach.
        assert_eq!(ModelOwner::from_scope(&blob_backed_catalog(5).scope), None);
    }
}
