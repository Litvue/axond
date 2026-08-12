//! Durable desired state: the [`ControlPlaneStore`] contract.
//!
//! The control plane owns what a stateful deployment *wants* to be serving.
//! Durable state is a chain of **immutable revisions**: publishing a change
//! creates new resource versions and a new manifest referencing them, and
//! nothing is edited in place. That is what makes "what was serving at 14:00"
//! answerable, rollback a matter of publishing an earlier manifest, and
//! hydration of any retained revision deterministic.
//!
//! Two rules shape every method here:
//!
//! - **Nothing on this trait is callable while an inference request is in
//!   flight.** A request reads the immutable snapshot it captured at start and
//!   never queries the control plane, so a `ControlPlaneStore` outage stalls
//!   convergence and administration while replicas keep serving. The contract
//!   is therefore allowed to be slow, and it is declared
//!   [`BackendPath::ControlPlane`](super::BackendPath::ControlPlane).
//! - **Redis cannot implement it.** [`ControlPlaneBackend`] has exactly one
//!   variant, and parsing rejects `redis` with a typed error instead of falling
//!   back, so "Redis is hot state only" is a compile- and boot-time property.
//!
//! ## Scope boundary with #164
//!
//! This is the *store* contract: identity of a revision, how a candidate is
//! published, and how conflicts and audit are expressed. The desired-state
//! domain itself — UUIDv7 typed ids, tenant-scoped slug rules, canonical
//! serialization and checksum rules, resource envelopes, content-addressed
//! blobs — is #164's, and the types here are deliberately thin placeholders
//! that #164 refines rather than replaces: [`ResourceVersionRef`] carries an id,
//! a slug, and a version and says nothing about a resource's schema, and
//! [`RevisionCandidate`] carries the payload as references plus a checksum
//! rather than embedding resource bodies.

use std::time::SystemTime;

use async_trait::async_trait;

use super::{BackendFailure, BackendKind, Capabilities, FailureCategory};

/// The durable implementations a deployment may select for the control plane.
///
/// One variant, on purpose. A second durable store is a new variant *and* a new
/// `durable_control_plane` implementation, which is a reviewable change rather
/// than a config string that happened to parse.
///
/// [`ControlPlaneBackend::parse`] is the only resolution path: deserialization
/// delegates to it, so a TOML value and a programmatic lookup accept exactly the
/// same names and produce exactly the same explanation when they do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ControlPlaneBackend {
    #[default]
    Postgres,
}

impl<'de> serde::Deserialize<'de> for ControlPlaneBackend {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        Self::parse(&name).map_err(serde::de::Error::custom)
    }
}

impl ControlPlaneBackend {
    pub const fn kind(self) -> BackendKind {
        match self {
            Self::Postgres => BackendKind::Postgres,
        }
    }

    /// Resolve an operator-supplied backend name.
    ///
    /// `redis` is rejected with its own arm rather than as an unknown name: the
    /// operator asked for something coherent-sounding and must be told why the
    /// answer is no, not that they made a typo.
    pub fn parse(name: &str) -> Result<Self, UnsupportedControlPlaneBackend> {
        match name {
            "postgres" => Ok(Self::Postgres),
            // A near miss on the one durable backend is a typo, not a request
            // for something else.
            "postgresql" | "pg" => Err(UnsupportedControlPlaneBackend::Unknown {
                name: name.to_owned(),
            }),
            "redis" => Err(UnsupportedControlPlaneBackend::HotStateOnly {
                name: "redis".to_owned(),
            }),
            "memory" | "in-memory" => Err(UnsupportedControlPlaneBackend::NotDurable {
                name: name.to_owned(),
            }),
            other => Err(UnsupportedControlPlaneBackend::Unknown {
                name: other.to_owned(),
            }),
        }
    }
}

/// Why a requested control-plane backend cannot be selected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UnsupportedControlPlaneBackend {
    #[error(
        "`{name}` holds loss-tolerant hot state and cannot own durable control-plane state; \
         the only durable control-plane backend is `postgres`"
    )]
    HotStateOnly { name: String },
    #[error("`{name}` is not durable and cannot own control-plane state")]
    NotDurable { name: String },
    #[error("unknown control-plane backend `{name}`; the only durable backend is `postgres`")]
    Unknown { name: String },
}

/// A published revision's identity. Monotonic per deployment, so a replica can
/// report desired, loaded, and active revisions as comparable numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RevisionId(pub u64);

impl std::fmt::Display for RevisionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "r{}", self.0)
    }
}

/// A durable resource's stable internal identifier. #164 replaces the inner
/// representation with a UUIDv7 type; callers only ever compare and carry it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResourceId(pub String);

/// The classes of durable resource a manifest may reference. Extended by later
/// slices; a store implementation must not interpret the variants beyond
/// storing and returning them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ResourceKind {
    Tenant,
    Project,
    Identity,
    Provider,
    ProviderCredential,
    CatalogModel,
    ModelEnablement,
    Price,
    Alias,
    Policy,
}

/// A manifest entry: one immutable version of one resource.
///
/// The slug is the tenant-scoped human-readable name; it may be renamed without
/// invalidating this reference, which is why the id is what a manifest joins on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceVersionRef {
    pub kind: ResourceKind,
    pub id: ResourceId,
    pub slug: String,
    pub version: u64,
}

/// A checksum over a candidate's canonically serialized desired state.
///
/// The canonicalization rules are #164's. The store treats this as an opaque
/// equality token: it persists it, returns it, and never recomputes it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RevisionChecksum(pub String);

/// Who performed a mutation, for the audit trail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Actor {
    /// An OIDC-authenticated human, identified by issuer-scoped subject.
    Human { issuer: String, subject: String },
    /// The static bootstrap breakglass operator.
    Breakglass,
    /// The gateway itself — a background catalogue refresh, for example.
    ///
    /// Owned rather than `&'static str` because an audit row read back out of a
    /// durable store has to produce this without leaking.
    System { component: String },
}

/// The audit event a mutation carries.
///
/// It is part of the candidate rather than a separate call because it must be
/// written in the mutation's own transaction: an audit trail that can be
/// half-written is not an audit trail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    pub actor: Actor,
    pub action: String,
    pub summary: String,
}

/// A caller-supplied deduplication token. A retry carrying the same key *and*
/// the same desired state must return the original outcome rather than
/// publishing a second revision; the same key with different desired state is
/// [`ControlPlaneError::IdempotencyKeyReused`], never a silent replay of the
/// revision the caller did not describe.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IdempotencyKey(pub String);

/// The revision a writer believes is current.
///
/// Explicit rather than "whatever is current" so two administrators editing
/// concurrently get a typed [`ControlPlaneError::Conflict`] instead of a
/// silent last-write-wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedRevision {
    /// The store has never published a revision.
    Empty,
    /// This exact revision must still be the newest.
    Exactly(RevisionId),
}

/// A revision offered for publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionCandidate {
    pub expected: ExpectedRevision,
    pub resources: Vec<ResourceVersionRef>,
    pub checksum: RevisionChecksum,
    pub audit: AuditEvent,
    pub idempotency_key: IdempotencyKey,
}

/// A published, immutable revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevisionManifest {
    pub id: RevisionId,
    pub parent: Option<RevisionId>,
    pub created_at: SystemTime,
    pub resources: Vec<ResourceVersionRef>,
    pub checksum: RevisionChecksum,
}

/// Why a control-plane operation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ControlPlaneError {
    #[error("control-plane store `{backend}` unavailable: {message}")]
    Unavailable {
        backend: &'static str,
        message: String,
    },
    /// Another writer published first. The caller re-reads and rebuilds; it does
    /// not replay the same candidate.
    #[error("expected revision {expected:?} but the newest is {actual:?}")]
    Conflict {
        expected: ExpectedRevision,
        actual: Option<RevisionId>,
    },
    #[error("revision {0} is not retained")]
    RevisionNotFound(RevisionId),
    #[error("invalid candidate revision: {0}")]
    Invalid(String),
    /// The key was already used to publish *different* desired state. Replaying
    /// the earlier revision would tell the caller their change was applied when
    /// it never was, so the write is refused instead.
    #[error(
        "idempotency key `{}` already published revision {published} with different desired state",
        key.0
    )]
    IdempotencyKeyReused {
        key: IdempotencyKey,
        published: RevisionId,
    },
    #[error("control-plane store `{backend}` refused the operation: {message}")]
    Denied {
        backend: &'static str,
        message: String,
    },
    /// A retained revision could not be interpreted. Never masked as
    /// "unavailable": an operator has to know that stored state is unreadable.
    #[error("stored revision {revision} is unreadable: {message}")]
    Corrupt {
        revision: RevisionId,
        message: String,
    },
}

impl BackendFailure for ControlPlaneError {
    fn category(&self) -> FailureCategory {
        match self {
            Self::Unavailable { .. } => FailureCategory::Unavailable,
            Self::Conflict { .. } => FailureCategory::Conflict,
            Self::RevisionNotFound(_) => FailureCategory::NotFound,
            Self::Invalid(_) | Self::IdempotencyKeyReused { .. } => FailureCategory::Invalid,
            Self::Denied { .. } => FailureCategory::Denied,
            Self::Corrupt { .. } => FailureCategory::Corrupt,
        }
    }
}

/// Durable desired state, read and written off the inference path.
///
/// An implementation must provide [`Capability::TransactionalWrites`],
/// [`Capability::OptimisticConcurrency`], [`Capability::IdempotentWrites`], and
/// [`Capability::TransactionalAudit`]; [`Capability::ChangeNotification`] is
/// optional and only decides whether convergence polls.
///
/// [`Capability::TransactionalWrites`]: super::Capability::TransactionalWrites
/// [`Capability::OptimisticConcurrency`]: super::Capability::OptimisticConcurrency
/// [`Capability::IdempotentWrites`]: super::Capability::IdempotentWrites
/// [`Capability::TransactionalAudit`]: super::Capability::TransactionalAudit
/// [`Capability::ChangeNotification`]: super::Capability::ChangeNotification
#[async_trait]
pub trait ControlPlaneStore: Send + Sync {
    fn name(&self) -> &'static str;

    fn capabilities(&self) -> Capabilities;

    /// Control-plane reachability, for administrative diagnostics.
    ///
    /// Never consulted by `/readyz`: readiness reflects whether the replica
    /// holds an active snapshot, not whether the control plane is reachable.
    async fn health(&self) -> Result<(), ControlPlaneError>;

    /// The newest published revision, or `None` before the first publication.
    async fn desired_revision(&self) -> Result<Option<RevisionId>, ControlPlaneError>;

    /// Load a retained revision. A revision is immutable, so a successful load
    /// is repeatable and cacheable forever.
    async fn load_revision(&self, id: RevisionId) -> Result<RevisionManifest, ControlPlaneError>;

    /// Publish a candidate as the new newest revision.
    ///
    /// Atomic with its audit event, conditioned on
    /// [`RevisionCandidate::expected`], and idempotent under
    /// [`RevisionCandidate::idempotency_key`]: a repeat of the same key carrying
    /// the same [`RevisionCandidate::checksum`] returns the revision the first
    /// call published, and a repeat carrying a different checksum is refused with
    /// [`ControlPlaneError::IdempotencyKeyReused`] rather than replaying an
    /// outcome the caller did not ask for. Publication does not validate
    /// desired state into a snapshot — compilation and rejection are the
    /// replica's job (#142); the store's job is to make the transition
    /// all-or-nothing.
    async fn publish_revision(
        &self,
        candidate: RevisionCandidate,
    ) -> Result<RevisionManifest, ControlPlaneError>;

    /// Audit events for a revision, newest-first, for `/admin/v1` reads.
    async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError>;
}

#[cfg(test)]
mod tests {
    use super::super::fakes::{InMemoryControlPlane, audit, candidate};
    use super::*;

    #[test]
    fn redis_cannot_be_selected_as_a_control_plane_backend() {
        let error = ControlPlaneBackend::parse("redis").expect_err("redis must be refused");
        assert!(matches!(
            error,
            UnsupportedControlPlaneBackend::HotStateOnly { .. }
        ));
        assert!(error.to_string().contains("hot state"));

        assert!(matches!(
            ControlPlaneBackend::parse("in-memory"),
            Err(UnsupportedControlPlaneBackend::NotDurable { .. })
        ));
        assert!(matches!(
            ControlPlaneBackend::parse("sqlite"),
            Err(UnsupportedControlPlaneBackend::Unknown { .. })
        ));
    }

    #[test]
    fn every_selectable_control_plane_backend_is_durable() {
        let backend = ControlPlaneBackend::parse("postgres").expect("durable backend");
        assert!(backend.kind().durable_control_plane());
        assert_eq!(
            ControlPlaneBackend::default(),
            ControlPlaneBackend::Postgres
        );
    }

    #[test]
    fn deserialization_resolves_through_parse() {
        assert_eq!(
            serde_json::from_str::<ControlPlaneBackend>("\"postgres\"").unwrap(),
            ControlPlaneBackend::Postgres
        );
        // One resolution path, so a configured value is refused with the same
        // explanation a programmatic lookup gets — including for a near miss.
        for name in ["redis", "in-memory", "postgresql", "sqlite"] {
            let refusal = serde_json::from_str::<ControlPlaneBackend>(&format!("\"{name}\""))
                .expect_err("only postgres is a durable control plane")
                .to_string();
            let expected = ControlPlaneBackend::parse(name).unwrap_err().to_string();
            assert!(
                refusal.contains(&expected),
                "`{name}` was refused as `{refusal}` instead of `{expected}`"
            );
        }
    }

    #[tokio::test]
    async fn publication_is_a_chain_of_immutable_revisions() {
        let store = InMemoryControlPlane::new();
        assert_eq!(store.desired_revision().await.unwrap(), None);

        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", "a"))
            .await
            .expect("first publication");
        assert_eq!(first.parent, None);
        assert_eq!(store.desired_revision().await.unwrap(), Some(first.id));

        let second = store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "second",
                "b",
            ))
            .await
            .expect("second publication");
        assert_eq!(second.parent, Some(first.id));
        assert!(second.id > first.id);

        // The earlier revision is unchanged by the later one.
        assert_eq!(store.load_revision(first.id).await.unwrap(), first);
    }

    #[tokio::test]
    async fn a_stale_expected_revision_conflicts_instead_of_overwriting() {
        let store = InMemoryControlPlane::new();
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", "a"))
            .await
            .unwrap();

        let error = store
            .publish_revision(candidate(ExpectedRevision::Empty, "racing", "c"))
            .await
            .expect_err("a stale expectation must not publish");
        assert_eq!(
            error,
            ControlPlaneError::Conflict {
                expected: ExpectedRevision::Empty,
                actual: Some(first.id),
            }
        );
        assert_eq!(error.category(), FailureCategory::Conflict);
        assert!(!error.retryable());
        assert_eq!(store.desired_revision().await.unwrap(), Some(first.id));
    }

    #[tokio::test]
    async fn a_retried_publication_applies_once() {
        let store = InMemoryControlPlane::new();
        let candidate = candidate(ExpectedRevision::Empty, "first", "a");
        let first = store.publish_revision(candidate.clone()).await.unwrap();
        let retried = store
            .publish_revision(candidate)
            .await
            .expect("a retry replays the original outcome");
        assert_eq!(first, retried);
        assert_eq!(store.published_revisions(), 1);
    }

    #[tokio::test]
    async fn a_reused_key_carrying_different_state_is_refused() {
        let store = InMemoryControlPlane::new();
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", "a"))
            .await
            .unwrap();

        // Same key, different desired state: replaying `first` would report a
        // change that was never published.
        let mut reused = candidate(ExpectedRevision::Exactly(first.id), "second", "a");
        reused.checksum = RevisionChecksum("sha256:b".to_owned());
        let error = store
            .publish_revision(reused)
            .await
            .expect_err("a reused key must not replay a different revision");
        assert_eq!(
            error,
            ControlPlaneError::IdempotencyKeyReused {
                key: IdempotencyKey("a".to_owned()),
                published: first.id,
            }
        );
        assert_eq!(error.category(), FailureCategory::Invalid);
        assert!(!error.retryable());
        assert_eq!(store.published_revisions(), 1);
        assert_eq!(store.desired_revision().await.unwrap(), Some(first.id));
    }

    #[tokio::test]
    async fn a_replay_survives_a_moved_expectation() {
        let store = InMemoryControlPlane::new();
        let first = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", "a"))
            .await
            .unwrap();
        store
            .publish_revision(candidate(
                ExpectedRevision::Exactly(first.id),
                "second",
                "b",
            ))
            .await
            .unwrap();

        // The original candidate's expectation is now stale, but its key and
        // desired state are unchanged: a retry replays rather than conflicts.
        let replayed = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", "a"))
            .await
            .expect("an unchanged retry replays its own outcome");
        assert_eq!(replayed, first);
        assert_eq!(store.published_revisions(), 2);
    }

    #[tokio::test]
    async fn audit_is_written_with_the_mutation() {
        let store = InMemoryControlPlane::new();
        let revision = store
            .publish_revision(candidate(ExpectedRevision::Empty, "first", "a"))
            .await
            .unwrap();
        assert_eq!(
            store.audit_trail(revision.id).await.unwrap(),
            vec![audit("first")]
        );
    }

    #[tokio::test]
    async fn an_audit_actor_round_trips_from_owned_data() {
        let store = InMemoryControlPlane::new();
        // What a durable store has when it reads an audit row back: owned bytes
        // with no static lifetime available to borrow from.
        let read_back = |column: &str| Actor::System {
            component: column.to_string(),
        };
        let mut candidate = candidate(ExpectedRevision::Empty, "refresh", "a");
        candidate.audit.actor = read_back(&String::from("catalog-refresh"));

        let revision = store.publish_revision(candidate).await.unwrap();
        let trail = store.audit_trail(revision.id).await.unwrap();
        assert_eq!(trail[0].actor, read_back("catalog-refresh"));
        assert_ne!(trail[0].actor, read_back("someone-else"));
    }

    #[tokio::test]
    async fn a_rejected_candidate_leaves_no_trace() {
        let store = InMemoryControlPlane::new();
        let mut invalid = candidate(ExpectedRevision::Empty, "invalid", "a");
        invalid.resources.clear();
        let error = store
            .publish_revision(invalid)
            .await
            .expect_err("an empty candidate is invalid");
        assert_eq!(error.category(), FailureCategory::Invalid);
        assert_eq!(store.desired_revision().await.unwrap(), None);
        assert_eq!(store.published_revisions(), 0);
    }

    #[tokio::test]
    async fn unknown_revisions_and_outages_are_distinguishable() {
        let store = InMemoryControlPlane::new();
        let missing = store
            .load_revision(RevisionId(7))
            .await
            .expect_err("unpublished revision");
        assert_eq!(missing.category(), FailureCategory::NotFound);
        assert!(!missing.retryable());

        store.set_unavailable(true);
        let outage = store
            .desired_revision()
            .await
            .expect_err("an unreachable store must not report an empty control plane");
        assert_eq!(outage.category(), FailureCategory::Unavailable);
        assert!(outage.retryable());
    }

    #[tokio::test]
    async fn the_store_declares_the_capabilities_publication_relies_on() {
        use super::super::Capability;

        let store = InMemoryControlPlane::new();
        for capability in [
            Capability::TransactionalWrites,
            Capability::OptimisticConcurrency,
            Capability::IdempotentWrites,
            Capability::TransactionalAudit,
        ] {
            assert!(
                store.capabilities().has(capability),
                "{capability:?} is required of every ControlPlaneStore"
            );
        }
        store.health().await.expect("a healthy fake");
    }
}
