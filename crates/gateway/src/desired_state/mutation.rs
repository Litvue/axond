//! Mutations: who changed desired state, under what expectation, and the audit
//! event the change carries.
//!
//! A mutation is the unit an administrator submits and the unit an audit trail
//! records. Three properties are typed rather than conventional:
//!
//! - **Every change has an actor.** [`Actor`] has no "unknown" variant, so a
//!   durable mutation without attribution is unconstructible rather than
//!   discovered later in an audit review.
//! - **Every change states what it expected.** [`ExpectedRevision`] is required,
//!   so concurrent administrators get a typed conflict instead of last-write-wins
//!   (#141's "concurrent writers cannot lose updates").
//! - **Every change is safe to retry.** [`IdempotencyKey`] plus the candidate's
//!   checksum makes a retry replay its own outcome, and makes a *reused* key
//!   carrying different state a refusal — see
//!   [`RevisionCandidate`](super::revision::RevisionCandidate).
//!
//! The audit event is part of the mutation rather than a separate call, because
//! it has to commit in the mutation's own transaction: an audit trail that can be
//! half-written is not an audit trail.

use std::time::SystemTime;

use super::canonical::{Canonical, CanonicalValue};
use super::ids::{AuditEventId, InvalidId, MutationId, PrincipalId, RevisionId, TenantId};
use super::resource::{ResourceRef, ResourceScope};

/// Who performed a mutation.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Actor {
    /// An OIDC-authenticated human, identified by issuer-scoped subject: a
    /// subject is only unique within its issuer, so storing it without the
    /// issuer would merge two people from two identity providers.
    Human { issuer: String, subject: String },
    /// The static bootstrap breakglass operator. Distinct from a human on
    /// purpose: "someone used breakglass" is the thing an auditor looks for.
    Breakglass,
    /// A workload service account: an Axond-owned principal inside a tenant,
    /// authenticated by key material rather than by an identity provider (#144).
    ///
    /// Its owning tenant is carried rather than looked up, so an audit row is
    /// legible — and attributable to a tenant — without hydrating the revision
    /// that declared the principal, including after the principal is removed.
    Workload {
        tenant: TenantId,
        principal: PrincipalId,
    },
    /// The gateway itself — a background catalogue refresh, for example.
    ///
    /// Owned rather than `&'static str` because an audit row read back out of a
    /// durable store has to produce this without leaking.
    System { component: String },
}

impl Actor {
    /// The tenant this attribution belongs to, if the actor is one tenant's.
    ///
    /// `None` for a human, breakglass, or the gateway itself: those act on the
    /// deployment, and an administrator of two tenants is not two actors. What
    /// this exists for is the read side of a refusal — a workload's tenant is
    /// another tenant's identifier, so it filters what a tenant-scoped read
    /// returns exactly as it filters what a pinned session sees.
    pub const fn tenant(&self) -> Option<TenantId> {
        match self {
            Self::Human { .. } | Self::Breakglass | Self::System { .. } => None,
            Self::Workload { tenant, .. } => Some(*tenant),
        }
    }
}

impl Canonical for Actor {
    fn canonical(&self) -> CanonicalValue {
        match self {
            Self::Human { issuer, subject } => CanonicalValue::map([
                ("kind", CanonicalValue::string("human")),
                ("issuer", CanonicalValue::string(issuer.clone())),
                ("subject", CanonicalValue::string(subject.clone())),
            ]),
            Self::Breakglass => {
                CanonicalValue::map([("kind", CanonicalValue::string("breakglass"))])
            }
            Self::Workload { tenant, principal } => CanonicalValue::map([
                ("kind", CanonicalValue::string("workload")),
                ("tenant", CanonicalValue::string(tenant.to_string())),
                ("principal", CanonicalValue::string(principal.to_string())),
            ]),
            Self::System { component } => CanonicalValue::map([
                ("kind", CanonicalValue::string("system")),
                ("component", CanonicalValue::string(component.clone())),
            ]),
        }
    }
}

/// Why a canonical record does not describe an actor.
///
/// Reading is the inverse of [`Canonical`] and lives next to it, because a body
/// that records an actor — a price-book approval, for instance — must reach the
/// same conclusion the writer did rather than reinterpreting the record its own
/// way.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidActor {
    #[error("an actor is recorded as a record with a `kind`")]
    NotARecord,
    #[error("actor kind `{kind}` is not one this build knows")]
    UnknownKind { kind: String },
    #[error("an actor of kind `{kind}` is missing the `{field}` field, or it is not a string")]
    Field {
        kind: &'static str,
        field: &'static str,
    },
    #[error("an actor of kind `{kind}` does not have a `{field}` field in this build")]
    UnknownField { kind: &'static str, field: String },
    #[error("an actor of kind `{kind}` records a `{field}` that is not an id: {source}")]
    Id {
        kind: &'static str,
        field: &'static str,
        source: InvalidId,
    },
}

impl Actor {
    /// Read an actor back out of its canonical form.
    ///
    /// Strict for the same reason every body reader is: an actor this build cannot
    /// read is a refusal, never an anonymous fallback, because "who approved this"
    /// is the question the type exists to keep answerable.
    pub fn read(value: &CanonicalValue) -> Result<Self, InvalidActor> {
        let CanonicalValue::Map(fields) = value else {
            return Err(InvalidActor::NotARecord);
        };
        let string = |field: &'static str| match fields
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, value)| value)
        {
            Some(CanonicalValue::String(text)) => Some(text.clone()),
            _ => None,
        };
        // A field this build does not know is refused rather than dropped: an
        // approval read back in a reduced form would name a different approver
        // than the one who signed it, and re-canonicalizing it would publish a
        // checksum the stored bytes do not have.
        let only = |kind: &'static str, known: &[&str]| match fields
            .iter()
            .map(|(name, _)| name)
            .find(|name| !known.contains(&name.as_str()))
        {
            None => Ok(()),
            Some(field) => Err(InvalidActor::UnknownField {
                kind,
                field: field.clone(),
            }),
        };
        let kind = string("kind").ok_or(InvalidActor::NotARecord)?;
        match kind.as_str() {
            "human" => {
                only("human", &["kind", "issuer", "subject"])?;
                Ok(Self::Human {
                    issuer: string("issuer").ok_or(InvalidActor::Field {
                        kind: "human",
                        field: "issuer",
                    })?,
                    subject: string("subject").ok_or(InvalidActor::Field {
                        kind: "human",
                        field: "subject",
                    })?,
                })
            }
            "breakglass" => {
                only("breakglass", &["kind"])?;
                Ok(Self::Breakglass)
            }
            "workload" => {
                only("workload", &["kind", "tenant", "principal"])?;
                let id = |field: &'static str| {
                    string(field).ok_or(InvalidActor::Field {
                        kind: "workload",
                        field,
                    })
                };
                Ok(Self::Workload {
                    tenant: TenantId::parse(&id("tenant")?).map_err(|source| InvalidActor::Id {
                        kind: "workload",
                        field: "tenant",
                        source,
                    })?,
                    principal: PrincipalId::parse(&id("principal")?).map_err(|source| {
                        InvalidActor::Id {
                            kind: "workload",
                            field: "principal",
                            source,
                        }
                    })?,
                })
            }
            "system" => {
                only("system", &["kind", "component"])?;
                Ok(Self::System {
                    component: string("component").ok_or(InvalidActor::Field {
                        kind: "system",
                        field: "component",
                    })?,
                })
            }
            _ => Err(InvalidActor::UnknownKind { kind }),
        }
    }
}

impl std::fmt::Display for Actor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Human { issuer, subject } => write!(f, "human {subject} @ {issuer}"),
            Self::Breakglass => f.write_str("breakglass"),
            Self::Workload { tenant, principal } => write!(f, "workload {principal} of {tenant}"),
            Self::System { component } => write!(f, "system {component}"),
        }
    }
}

/// What a mutation did, independent of which resource kinds it touched.
///
/// Generic verbs rather than one variant per resource kind: a new resource kind
/// must not require a new mutation kind, and an audit reader should be able to
/// filter "every deletion" without enumerating kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MutationKind {
    Create,
    Update,
    Delete,
    /// Credential or key rotation: an update, but the one an auditor greps for.
    Rotate,
    /// Republication of an earlier revision's desired state.
    Rollback,
}

impl MutationKind {
    pub const ALL: &'static [Self] = &[
        Self::Create,
        Self::Update,
        Self::Delete,
        Self::Rotate,
        Self::Rollback,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
            Self::Rotate => "rotate",
            Self::Rollback => "rollback",
        }
    }
}

impl Canonical for MutationKind {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::string(self.as_str())
    }
}

/// A caller-supplied deduplication token.
///
/// A retry carrying the same key *and* the same desired state must return the
/// original outcome rather than publishing a second revision; the same key with
/// different desired state is a refusal, never a silent replay of a revision the
/// caller did not describe.
///
/// The token carries no scope of its own, so a durable implementation must dedupe
/// within the *authenticated caller's* scope and expire records rather than
/// retaining them forever: a global, immortal namespace would let one
/// administrator's `retry-1` replay or block another's. Two callers submitting the
/// same string are two independent writes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdempotencyKey(String);

/// Why an idempotency key was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidIdempotencyKey {
    #[error("an idempotency key must not be empty")]
    Empty,
    #[error("idempotency key is {length} characters, over the {max}-character limit")]
    TooLong { length: usize, max: usize },
    #[error("an idempotency key must be printable ASCII")]
    Unprintable,
}

impl IdempotencyKey {
    pub const MAX_LEN: usize = 200;

    /// Validate a caller-supplied key.
    ///
    /// Bounded and printable because it is a durable map key and appears in error
    /// messages and logs: an unbounded or control-character-bearing token is a
    /// storage and log-injection problem, not a client convenience.
    pub fn parse(input: &str) -> Result<Self, InvalidIdempotencyKey> {
        if input.is_empty() {
            return Err(InvalidIdempotencyKey::Empty);
        }
        if input.len() > Self::MAX_LEN {
            return Err(InvalidIdempotencyKey::TooLong {
                length: input.len(),
                max: Self::MAX_LEN,
            });
        }
        if !input
            .bytes()
            .all(|byte| byte.is_ascii_graphic() || byte == b' ')
        {
            return Err(InvalidIdempotencyKey::Unprintable);
        }
        Ok(Self(input.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The revision a writer believes is current.
///
/// Explicit rather than "whatever is current", so two administrators editing
/// concurrently get a typed conflict instead of a silent last-write-wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    /// The store has never published a revision.
    Empty,
    /// This exact revision must still be the newest.
    Exactly(RevisionId),
}

impl ExpectedRevision {
    /// Whether `newest` satisfies this expectation.
    ///
    /// One function so every implementation — the oracle, #165's Postgres
    /// `WHERE` clause — agrees on what "expected" means, including the case a
    /// store must never treat as a match: expecting an empty control plane when
    /// one already has revisions.
    pub fn matches(self, newest: Option<RevisionId>) -> bool {
        match (self, newest) {
            (Self::Empty, None) => true,
            (Self::Exactly(expected), Some(actual)) => expected == actual,
            _ => false,
        }
    }
}

impl std::fmt::Display for ExpectedRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("an empty control plane"),
            Self::Exactly(revision) => write!(f, "{revision}"),
        }
    }
}

/// One administrative change, whatever number of resources it touched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutation {
    pub id: MutationId,
    pub actor: Actor,
    pub kind: MutationKind,
    /// The narrowest scope the change applies to, for authorization and audit
    /// filtering.
    pub scope: ResourceScope,
    pub idempotency_key: IdempotencyKey,
    pub submitted_at: SystemTime,
}

impl Canonical for Mutation {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("id", CanonicalValue::string(self.id.to_string())),
            ("actor", self.actor.canonical()),
            ("kind", self.kind.canonical()),
            ("scope", self.scope.canonical()),
            (
                "idempotency_key",
                CanonicalValue::string(self.idempotency_key.as_str()),
            ),
        ])
    }
}

/// The audit event a mutation carries.
///
/// It records the mutation's *intent* — actor, verb, target, human summary — and
/// is written in the mutation's own transaction. The desired state itself is not
/// duplicated here: the revision is the state, and this event is why it changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    pub id: AuditEventId,
    pub mutation: MutationId,
    pub actor: Actor,
    pub kind: MutationKind,
    /// The resource version the change centred on, when there is one. A mutation
    /// that only deletes has no new version to point at.
    pub target: Option<ResourceRef>,
    pub summary: String,
    pub recorded_at: SystemTime,
}

impl Canonical for AuditEvent {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            ("id", CanonicalValue::string(self.id.to_string())),
            (
                "mutation",
                CanonicalValue::string(self.mutation.to_string()),
            ),
            ("actor", self.actor.canonical()),
            ("kind", self.kind.canonical()),
            ("summary", CanonicalValue::string(self.summary.clone())),
        ];
        // Absent means absent: there is no null, so an event without a target
        // omits the field rather than carrying a second spelling of "none".
        if let Some(target) = &self.target {
            fields.push(("target", target.canonical()));
        }
        CanonicalValue::map(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::super::ids::{ResourceId, Uuid7};
    use super::super::resource::{ResourceKind, ResourceVersionNumber};
    use super::*;

    fn revision(seed: u64) -> RevisionId {
        RevisionId::new(Uuid7::from_parts(seed, 0, seed).unwrap())
    }

    fn mutation_id(seed: u64) -> MutationId {
        MutationId::new(Uuid7::from_parts(seed, 0, seed).unwrap())
    }

    #[test]
    fn an_expectation_matches_only_the_state_it_describes() {
        let first = revision(1);
        let second = revision(2);

        assert!(ExpectedRevision::Empty.matches(None));
        assert!(!ExpectedRevision::Empty.matches(Some(first)));
        assert!(ExpectedRevision::Exactly(first).matches(Some(first)));
        assert!(!ExpectedRevision::Exactly(first).matches(Some(second)));
        assert!(
            !ExpectedRevision::Exactly(first).matches(None),
            "expecting a revision an empty control plane does not have must not match"
        );
    }

    #[test]
    fn an_expectation_explains_itself() {
        assert_eq!(
            ExpectedRevision::Empty.to_string(),
            "an empty control plane"
        );
        let first = revision(1);
        assert_eq!(
            ExpectedRevision::Exactly(first).to_string(),
            first.to_string()
        );
    }

    #[test]
    fn idempotency_keys_are_bounded_printable_tokens() {
        assert_eq!(
            IdempotencyKey::parse("retry-1").unwrap().as_str(),
            "retry-1"
        );
        assert_eq!(
            IdempotencyKey::parse("retry 1").unwrap().to_string(),
            "retry 1"
        );
        assert_eq!(IdempotencyKey::parse(""), Err(InvalidIdempotencyKey::Empty));
        assert_eq!(
            IdempotencyKey::parse(&"k".repeat(IdempotencyKey::MAX_LEN + 1)),
            Err(InvalidIdempotencyKey::TooLong {
                length: IdempotencyKey::MAX_LEN + 1,
                max: IdempotencyKey::MAX_LEN
            })
        );
        for input in ["retry\n1", "retry\t1", "retry\u{0}"] {
            assert_eq!(
                IdempotencyKey::parse(input),
                Err(InvalidIdempotencyKey::Unprintable)
            );
        }
        // Keys are compared exactly: no trimming, no case folding, because a
        // client's token is the client's.
        assert_ne!(
            IdempotencyKey::parse("retry-1").unwrap(),
            IdempotencyKey::parse("Retry-1").unwrap()
        );
    }

    #[test]
    fn actors_are_distinguishable_and_issuer_scoped() {
        let one = Actor::Human {
            issuer: "https://idp.example".to_owned(),
            subject: "u-1".to_owned(),
        };
        let other = Actor::Human {
            issuer: "https://other.example".to_owned(),
            subject: "u-1".to_owned(),
        };
        assert_ne!(one, other, "a subject is unique only within its issuer");
        assert_ne!(one.checksum().unwrap(), other.checksum().unwrap());
        assert_ne!(
            Actor::Breakglass.checksum().unwrap(),
            Actor::System {
                component: "breakglass".to_owned()
            }
            .checksum()
            .unwrap(),
            "breakglass is not a component that happens to be named breakglass"
        );
        assert_eq!(one.to_string(), "human u-1 @ https://idp.example");
    }

    #[test]
    fn mutation_kinds_have_distinct_canonical_forms() {
        let checksums: std::collections::BTreeSet<_> = MutationKind::ALL
            .iter()
            .map(|kind| kind.checksum().unwrap())
            .collect();
        assert_eq!(checksums.len(), MutationKind::ALL.len());
    }

    #[test]
    fn an_audit_event_omits_an_absent_target_rather_than_nulling_it() {
        let event = AuditEvent {
            id: AuditEventId::new(Uuid7::from_parts(5, 0, 5).unwrap()),
            mutation: mutation_id(4),
            actor: Actor::Breakglass,
            kind: MutationKind::Delete,
            target: None,
            summary: "retired the alias".to_owned(),
            recorded_at: SystemTime::UNIX_EPOCH,
        };
        let targeted = AuditEvent {
            target: Some(ResourceRef::new(
                ResourceKind::Alias,
                ResourceId::new(Uuid7::from_parts(6, 0, 6).unwrap()),
                ResourceVersionNumber::FIRST,
            )),
            ..event.clone()
        };
        assert_ne!(event.checksum().unwrap(), targeted.checksum().unwrap());
        // `recorded_at` is deliberately outside the canonical form: when a row
        // was written is not part of what was decided.
        let later = AuditEvent {
            recorded_at: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(60),
            ..event.clone()
        };
        assert_eq!(event.checksum().unwrap(), later.checksum().unwrap());
    }

    #[test]
    fn a_mutation_canonicalizes_its_attribution_but_not_its_clock() {
        let mutation = Mutation {
            id: mutation_id(3),
            actor: Actor::System {
                component: "catalog-refresh".to_owned(),
            },
            kind: MutationKind::Update,
            scope: ResourceScope::Deployment,
            idempotency_key: IdempotencyKey::parse("refresh-1").unwrap(),
            submitted_at: SystemTime::UNIX_EPOCH,
        };
        let later = Mutation {
            submitted_at: SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1),
            ..mutation.clone()
        };
        assert_eq!(mutation.checksum().unwrap(), later.checksum().unwrap());

        let other_actor = Mutation {
            actor: Actor::Breakglass,
            ..mutation.clone()
        };
        assert_ne!(
            mutation.checksum().unwrap(),
            other_actor.checksum().unwrap()
        );
    }
}
