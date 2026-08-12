//! Responsibility-specific internal backend contracts.
//!
//! Axond has no universal `StateBackend`. Backend selection is *per
//! responsibility*, because the seams legitimately differ: a spend cap needs a
//! millisecond request-path read with a fail-closed stance, while durable
//! desired state needs transactional multi-row writes with optimistic
//! concurrency and is allowed to be slow. One trait over both would force a
//! single error taxonomy, availability policy, and consistency model onto them
//! — and would imply that any backend can serve any responsibility, which is
//! how Redis ends up holding the system of record (ADR 0027, "stateless and
//! stateful operating modes"; `docs/maintainers/backend-contracts.md` maps these
//! contracts to it).
//!
//! Seven contracts exist. This module owns the three that are new and
//! control-plane-shaped, and it names the four request-path seams that already
//! ship so the boundary between them is reviewable in one place:
//!
//! | Contract | Responsibility | Path | Lives in |
//! | --- | --- | --- | --- |
//! | [`control_plane::ControlPlaneStore`] | Durable desired state: revisions, manifests, resource versions, audit | Control plane only | here |
//! | [`secrets::SecretStore`] | Wrapped secret material and unwrapping | Snapshot compilation only | here |
//! | [`catalog::CatalogSource`] | Model metadata ingestion | Background refresh only | here |
//! | [`crate::budget::BudgetStore`] | Spend caps | Request path (opt-in) | [`crate::budget`] |
//! | [`crate::rate_limit::RateLimiter`] | Inbound admission | Request path (opt-in) | [`crate::rate_limit`] |
//! | [`crate::revocation::RevocationStore`] | Precise `jti` revocation | Request path (opt-in) | [`crate::revocation`] |
//! | [`crate::usage::UsageSink`] | Durable usage rows | Off the request path | [`crate::usage`] |
//!
//! The four existing seams are deliberately *not* moved, re-exported, or given
//! a common supertrait here: each keeps its own error type, its own
//! `on_unavailable` policy, and its own tier declaration, so those stay
//! independently reviewable. [`RESPONSIBILITIES`] is the machine-checkable
//! version of the table above, and it is what the tests assert against.
//!
//! ## Scaffolding only
//!
//! These are contracts, not implementations. Nothing here is wired into
//! `serve`, so the running gateway is byte-for-byte the stateless gateway it
//! was: no new boot step, no new request-path work, no Postgres. Durable
//! implementations land in #141 (revisioned `ControlPlaneStore` on Postgres)
//! and #142 (revision → snapshot reconciliation); the shapes here are kept
//! minimal so those slices can extend them without a rewrite.

pub mod catalog;
pub mod control_plane;
pub mod secrets;

#[cfg(test)]
pub(crate) mod fakes;

/// Where a backend is allowed to be called from.
///
/// This is the property that keeps control-plane availability and data-plane
/// availability separate: an inference request reads one immutable snapshot and
/// never queries the control plane, so a contract declared
/// [`BackendPath::ControlPlane`], [`BackendPath::SnapshotCompilation`], or
/// [`BackendPath::Background`] appearing in a request handler is a bug rather
/// than a slow path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendPath {
    /// Called while an inference request is in flight. Latency and the
    /// `on_unavailable` stance are part of the contract.
    RequestPath,
    /// Called for an inference request's data, but never while it is in flight:
    /// buffered, batched, and unable to fail a response.
    OffRequestPath,
    /// Administrative reads and writes, revision publication, and convergence.
    /// Unavailability degrades change and cold start, never inference.
    ControlPlane,
    /// Called only while compiling a candidate revision into a snapshot, before
    /// that snapshot is publishable.
    SnapshotCompilation,
    /// Periodic maintenance with no request or boot dependency.
    Background,
}

impl BackendPath {
    /// Whether an inference request may call a backend on this path.
    pub const fn on_request_path(self) -> bool {
        matches!(self, Self::RequestPath)
    }
}

/// A backend implementation that some responsibility may select.
///
/// Naming the implementations as a closed set is what makes "Redis is hot state
/// only" checkable instead of aspirational: see
/// [`BackendKind::durable_control_plane`] and
/// [`control_plane::ControlPlaneBackend`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// No backend: the responsibility is unenforced or defaulted in-process.
    None,
    /// Process-local state. Per-replica, lost on restart.
    InMemory,
    /// Loss-tolerant hot state with expiry semantics.
    Redis,
    /// Transactional durable storage.
    Postgres,
    /// Line-oriented stdout, for usage records.
    Stdout,
    /// OTLP export, for usage records.
    Otlp,
    /// The models.dev public model-metadata catalogue.
    ModelsDev,
    /// An external secret manager (Vault, cloud KMS) behind
    /// [`secrets::SecretStore`]. A future adapter, named here so the contract's
    /// permitted set does not have to change when one arrives.
    ExternalSecretManager,
}

impl BackendKind {
    /// Whether this backend may hold durable control-plane state.
    ///
    /// Redis is excluded structurally, not by convention. Its data model is
    /// loss-tolerant hot state with expiry; durable desired state needs
    /// transactions, migrations, backup/restore, and referential integrity.
    /// Losing Redis loses hot enforcement precision, and losing durable state
    /// loses the deployment — those must not be the same store.
    pub const fn durable_control_plane(self) -> bool {
        matches!(self, Self::Postgres)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::InMemory => "in-memory",
            Self::Redis => "redis",
            Self::Postgres => "postgres",
            Self::Stdout => "stdout",
            Self::Otlp => "otlp",
            Self::ModelsDev => "models.dev",
            Self::ExternalSecretManager => "external-secret-manager",
        }
    }
}

/// One row of the responsibility table: which contract, where it may be called
/// from, and which implementations it may select.
#[derive(Debug, Clone, Copy)]
pub struct Responsibility {
    /// The contract's Rust trait name.
    pub contract: &'static str,
    /// One line on what the contract owns.
    pub responsibility: &'static str,
    pub path: BackendPath,
    pub permitted: &'static [BackendKind],
}

impl Responsibility {
    pub fn permits(&self, kind: BackendKind) -> bool {
        self.permitted.contains(&kind)
    }
}

/// Every backend responsibility in the gateway, including the four request-path
/// seams that predate the control-plane contracts.
///
/// Kept as data so the invariants — Redis is never durable, control-plane work
/// is never on the request path — are asserted by tests rather than by review.
pub const RESPONSIBILITIES: &[Responsibility] = &[
    Responsibility {
        contract: "ControlPlaneStore",
        responsibility: "durable desired state: revisions, manifests, resource versions, audit",
        path: BackendPath::ControlPlane,
        permitted: &[BackendKind::Postgres],
    },
    Responsibility {
        contract: "SecretStore",
        responsibility: "wrapped secret material and unwrapping",
        path: BackendPath::SnapshotCompilation,
        permitted: &[BackendKind::Postgres, BackendKind::ExternalSecretManager],
    },
    Responsibility {
        contract: "CatalogSource",
        responsibility: "model metadata ingestion",
        path: BackendPath::Background,
        permitted: &[BackendKind::ModelsDev],
    },
    Responsibility {
        contract: "BudgetStore",
        responsibility: "spend caps",
        path: BackendPath::RequestPath,
        permitted: &[
            BackendKind::None,
            BackendKind::InMemory,
            BackendKind::Redis,
            BackendKind::Postgres,
        ],
    },
    Responsibility {
        contract: "RateLimiter",
        responsibility: "inbound admission",
        path: BackendPath::RequestPath,
        permitted: &[BackendKind::None, BackendKind::InMemory, BackendKind::Redis],
    },
    Responsibility {
        contract: "RevocationStore",
        responsibility: "precise minted-token jti revocation",
        path: BackendPath::RequestPath,
        permitted: &[BackendKind::None, BackendKind::Redis, BackendKind::Postgres],
    },
    Responsibility {
        contract: "UsageSink",
        responsibility: "durable usage rows",
        path: BackendPath::OffRequestPath,
        permitted: &[
            BackendKind::Stdout,
            BackendKind::Otlp,
            BackendKind::Postgres,
        ],
    },
];

/// Look a responsibility up by its trait name.
pub fn responsibility(contract: &str) -> Option<&'static Responsibility> {
    RESPONSIBILITIES.iter().find(|r| r.contract == contract)
}

/// An optional behaviour an implementation may provide.
///
/// Capabilities are declared, not probed: a caller that needs transactional
/// audit asks the implementation whether it has it, instead of discovering the
/// answer from a half-applied write. Implementations of one contract may differ
/// (encrypted Postgres wraps under a local KEK; an external manager delegates
/// key management), and callers must degrade or refuse explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Capability {
    /// Multi-row writes commit atomically or not at all.
    TransactionalWrites,
    /// Writes can be conditioned on an expected current revision.
    OptimisticConcurrency,
    /// A repeated write carrying the same idempotency key applies once.
    IdempotentWrites,
    /// Mutations persist an audit event in the mutation's own transaction.
    TransactionalAudit,
    /// Changes can be observed without polling.
    ChangeNotification,
    /// Secret material is stored wrapped under a key-encryption key.
    EnvelopeEncryption,
    /// Key management, and therefore unwrapping, happens outside this process.
    ExternalKeyManagement,
    /// A refresh can ask for "changed since", not just the whole catalogue.
    IncrementalRefresh,
    /// The source carries price metadata alongside model metadata.
    PriceMetadata,
}

/// The capabilities one implementation declares.
///
/// A static slice rather than a set: implementations are known at compile time,
/// so this costs nothing and stays `const`-constructible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities(&'static [Capability]);

impl Capabilities {
    pub const NONE: Self = Self(&[]);

    pub const fn new(capabilities: &'static [Capability]) -> Self {
        Self(capabilities)
    }

    pub fn has(&self, capability: Capability) -> bool {
        self.0.contains(&capability)
    }

    pub fn iter(&self) -> impl Iterator<Item = Capability> + '_ {
        self.0.iter().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// What a caller must decide, independent of which backend failed.
///
/// Each contract keeps its own error enum — the arms and their messages are
/// contract-specific — and maps into this shared vocabulary so shared policy
/// (retry, surface as `503`, refuse a candidate revision) can be written once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureCategory {
    /// The backend could not be reached, or timed out. Retryable; the
    /// responsibility's `on_unavailable` policy decides what a caller does
    /// meanwhile.
    Unavailable,
    /// A concurrent writer won. The caller re-reads and retries with fresh
    /// expectations; retrying the same write unchanged loses again.
    Conflict,
    /// The referenced thing does not exist. Not retryable.
    NotFound,
    /// The request itself is wrong: malformed input, a dangling reference, a
    /// violated constraint. Not retryable.
    Invalid,
    /// The backend refused on authorization or policy grounds. Not retryable.
    Denied,
    /// Stored data could not be interpreted — a decryption failure, a corrupt
    /// or unknown-version record. Not retryable, and always an operator alert.
    Corrupt,
}

impl FailureCategory {
    /// Whether retrying the *same* operation can plausibly succeed.
    ///
    /// [`FailureCategory::Conflict`] is deliberately false: a conflicting write
    /// must be rebuilt against the current state, not replayed.
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Unavailable)
    }
}

/// A backend error that can be classified without knowing its contract.
pub trait BackendFailure: std::error::Error {
    fn category(&self) -> FailureCategory;

    fn retryable(&self) -> bool {
        self.category().retryable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redis_is_never_a_durable_control_plane_backend() {
        assert!(!BackendKind::Redis.durable_control_plane());
        for contract in ["ControlPlaneStore", "SecretStore", "CatalogSource"] {
            let responsibility = responsibility(contract).expect("declared responsibility");
            assert!(
                !responsibility.permits(BackendKind::Redis),
                "{contract} must not permit Redis"
            );
        }
    }

    #[test]
    fn durable_control_plane_backends_are_permitted_control_plane_implementations() {
        let control_plane = responsibility("ControlPlaneStore").expect("declared responsibility");
        for kind in control_plane.permitted {
            assert!(
                kind.durable_control_plane(),
                "{} may hold durable state, so it must be durable",
                kind.as_str()
            );
        }
    }

    #[test]
    fn control_plane_contracts_are_off_the_request_path() {
        for contract in ["ControlPlaneStore", "SecretStore", "CatalogSource"] {
            let responsibility = responsibility(contract).expect("declared responsibility");
            assert!(
                !responsibility.path.on_request_path(),
                "{contract} must not be reachable from an inference request"
            );
        }
    }

    #[test]
    fn request_path_seams_stay_declared_as_such() {
        for contract in ["BudgetStore", "RateLimiter", "RevocationStore"] {
            let responsibility = responsibility(contract).expect("declared responsibility");
            assert_eq!(responsibility.path, BackendPath::RequestPath);
            assert!(
                responsibility.permits(BackendKind::None),
                "{contract} must remain opt-in so Tier 0 stays stateless"
            );
        }
        let usage = responsibility("UsageSink").expect("declared responsibility");
        assert_eq!(usage.path, BackendPath::OffRequestPath);
    }

    #[test]
    fn responsibilities_are_unique_and_have_implementations() {
        let mut seen = std::collections::BTreeSet::new();
        for responsibility in RESPONSIBILITIES {
            assert!(
                seen.insert(responsibility.contract),
                "duplicate contract {}",
                responsibility.contract
            );
            assert!(
                !responsibility.permitted.is_empty(),
                "{} has no permitted implementation",
                responsibility.contract
            );
        }
        assert_eq!(seen.len(), 7, "the responsibility table is exhaustive");
    }

    #[test]
    fn only_unavailability_is_retryable() {
        assert!(FailureCategory::Unavailable.retryable());
        for category in [
            FailureCategory::Conflict,
            FailureCategory::NotFound,
            FailureCategory::Invalid,
            FailureCategory::Denied,
            FailureCategory::Corrupt,
        ] {
            assert!(!category.retryable(), "{category:?} must not be retried");
        }
    }

    #[test]
    fn capabilities_are_declared_not_probed() {
        const CAPS: Capabilities = Capabilities::new(&[Capability::TransactionalWrites]);
        assert!(CAPS.has(Capability::TransactionalWrites));
        assert!(!CAPS.has(Capability::ChangeNotification));
        assert!(Capabilities::NONE.is_empty());
        assert_eq!(CAPS.iter().count(), 1);
    }
}
