//! Wrapped secret material: the [`SecretStore`] contract.
//!
//! Encrypted Postgres is the first implementation: tenant provider credentials
//! are stored wrapped under a key-encryption key referenced from bootstrap, and
//! external managers (Vault, cloud KMS) are later adapters behind this same
//! contract.
//!
//! Four invariants, all enforced by the types rather than by convention:
//!
//! - **A request never touches this trait.** Secret material is unwrapped while
//!   a candidate revision is compiled, and a snapshot is only publishable once
//!   every credential it needs is already resolved in memory. So the contract
//!   is [`BackendPath::SnapshotCompilation`](super::BackendPath::SnapshotCompilation),
//!   a secret-store outage cannot fail an inference request, and there is no
//!   request-time fetch to cache, time out, or fail closed.
//! - **Everything except the compiled snapshot names a reference, never a
//!   value.** [`SecretRef`] is what manifests, audit events, `/admin/v1`
//!   responses, logs, and diagnostics carry. [`SecretMaterial`] holds the
//!   plaintext, has no `Debug` that prints it, no `Display`, no `Serialize`, and
//!   surrenders it only through [`SecretMaterial::expose`] — the same rule
//!   stateless mode already enforces for `env`-referenced keys.
//! - **Ownership is an argument, not a caller's responsibility.** Every method
//!   takes the [`SecretOwner`] the material is claimed for, and a store answers
//!   for material owned by somebody else exactly as it answers for material that
//!   does not exist. So no caller can resolve another tenant's key by holding a
//!   reference to it, and forgetting to check is not a thing a caller can do.
//! - **Reading material and administering it are different traits.**
//!   [`SecretResolver`] unwraps an exact version and nothing else;
//!   [`SecretStore`] stages, rotates, and moves material through its
//!   [`SecretLifecycle`] and never returns plaintext. Snapshot compilation only
//!   needs the first, so the component that holds plaintext is the one with no
//!   ability to change what a credential means.
//!
//! # Lifecycle
//!
//! The states, the transition matrix, and the idempotency rule are domain
//! contract, not storage policy: they live in
//! [`desired_state::secrets`](crate::desired_state::secrets), a revision's
//! credential body carries the state, and a store enforces the same relation the
//! domain defines. A store adds exactly two rules of its own:
//!
//! - only [`SecretLifecycle::permits_resolution`] material unwraps, so disabling
//!   or revoking a credential stops it being resolvable without deleting
//!   anything;
//! - tombstoning destroys material, which is why it is reachable only from
//!   `Revoked` and why nothing follows it.

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};

use super::{BackendFailure, BackendKind, Capabilities, FailureCategory};
use crate::desired_state::secrets::{
    ForbiddenTransition, LifecycleTransition, SecretLifecycle, SecretOwner, SecretRef,
};

/// The implementations a deployment may select for secret material.
///
/// Redis is absent, and so is any unencrypted store: material at rest is
/// wrapped or held by something whose whole job is holding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SecretBackend {
    /// Postgres rows holding material wrapped under a bootstrap-referenced KEK.
    #[default]
    EncryptedPostgres,
    /// An external secret manager that unwraps outside this process.
    External,
}

impl SecretBackend {
    pub const fn kind(self) -> BackendKind {
        match self {
            Self::EncryptedPostgres => BackendKind::Postgres,
            Self::External => BackendKind::ExternalSecretManager,
        }
    }
}

/// The key-encryption key a wrapped secret is sealed under.
///
/// A *reference* — an env var or external key name resolved at boot, the way
/// `[[credential]]` material already is. The KEK's own bytes never appear in a
/// manifest, a log line, or this type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KekRef(pub String);

impl std::fmt::Display for KekRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Unwrapped secret material, in memory, inside a compiled snapshot.
///
/// Deliberately not `Debug`-derivable, not `Display`, not `Serialize`, and not
/// convertible to `String`: the only way out is [`SecretMaterial::expose`], which
/// is easy to grep for in review. Wraps [`SecretString`] so the plaintext is
/// zeroized on drop, matching [`crate::credentials`].
#[derive(Clone)]
pub struct SecretMaterial(SecretString);

impl SecretMaterial {
    pub fn new(plaintext: String) -> Self {
        Self(SecretString::from(plaintext))
    }

    /// The plaintext. Callers are snapshot compilation and the transport layer
    /// that injects a provider credential — nothing else.
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }

    /// A non-secret property: whether the material is empty. Lets a caller
    /// reject unusable material without exposing it.
    pub fn is_empty(&self) -> bool {
        self.0.expose_secret().is_empty()
    }
}

impl From<SecretString> for SecretMaterial {
    fn from(secret: SecretString) -> Self {
        Self(secret)
    }
}

/// Prints a fixed marker. A secret that reaches a log through a struct's derived
/// `Debug` is the failure mode this exists to make impossible.
impl std::fmt::Debug for SecretMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretMaterial(<redacted>)")
    }
}

/// What a store may say about a secret version without unwrapping it.
///
/// The whole of a store's non-secret answer: which material, whose it is, and
/// what may be done with it. This is what an `/admin/v1` read, an audit summary,
/// and a pre-publication check are served from, so none of them needs plaintext
/// in order to describe a credential.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecretDescriptor {
    pub reference: SecretRef,
    pub owner: SecretOwner,
    pub lifecycle: SecretLifecycle,
}

impl SecretDescriptor {
    /// Whether this version's material may be unwrapped, lifecycle-wise.
    pub const fn permits_resolution(&self) -> bool {
        self.lifecycle.permits_resolution()
    }
}

impl std::fmt::Display for SecretDescriptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({}, {})", self.reference, self.owner, self.lifecycle)
    }
}

/// Why a secret operation failed.
///
/// No arm carries material, a KEK, or a ciphertext — only the reference, the
/// owner, the lifecycle state, and the backend, so every one of these is safe to
/// log verbatim.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecretError {
    #[error("secret store `{backend}` unavailable: {message}")]
    Unavailable {
        backend: &'static str,
        message: String,
    },
    #[error("secret {0} is not stored")]
    NotFound(SecretRef),
    /// The material exists but could not be unwrapped: a wrong, rotated, or
    /// revoked KEK, or a corrupt record. Distinct from `Unavailable` because
    /// retrying cannot help and a candidate revision must be rejected.
    #[error("secret {reference} could not be unwrapped under KEK `{kek}`")]
    Unwrap { reference: SecretRef, kek: KekRef },
    /// The material exists and belongs to somebody else.
    ///
    /// Reported as `NotFound` by [`FailureCategory`] on purpose: a caller
    /// learning that a reference it may not use *exists* is a disclosure, small
    /// but free to avoid. The distinct arm is for this process's own logs.
    #[error("secret {reference} does not belong to {owner}")]
    Ownership {
        reference: SecretRef,
        owner: SecretOwner,
    },
    /// The material exists, is this owner's, and its lifecycle state does not
    /// permit what was asked.
    #[error("secret {reference} is {state} and cannot be resolved")]
    Lifecycle {
        reference: SecretRef,
        state: SecretLifecycle,
    },
    /// A lifecycle move the contract does not define.
    #[error("secret {reference} cannot be moved: {source}")]
    Transition {
        reference: SecretRef,
        #[source]
        source: ForbiddenTransition,
    },
    #[error("invalid secret request: {0}")]
    Invalid(String),
    #[error("secret store `{backend}` refused the operation: {message}")]
    Denied {
        backend: &'static str,
        message: String,
    },
}

impl BackendFailure for SecretError {
    fn category(&self) -> FailureCategory {
        match self {
            Self::Unavailable { .. } => FailureCategory::Unavailable,
            // A reference somebody else owns is indistinguishable, to its
            // holder, from one that was never stored.
            Self::NotFound(_) | Self::Ownership { .. } => FailureCategory::NotFound,
            Self::Unwrap { .. } => FailureCategory::Corrupt,
            Self::Invalid(_) => FailureCategory::Invalid,
            Self::Lifecycle { .. } | Self::Transition { .. } | Self::Denied { .. } => {
                FailureCategory::Denied
            }
        }
    }
}

/// Exact-version resolution, and nothing else.
///
/// The half of the contract snapshot compilation needs: given the owner a
/// revision recorded and the exact version its credential body pinned, unwrap
/// that material. There is no "latest", no search, and no lifecycle mutation
/// here, so the component that holds plaintext cannot rotate, disable, or
/// re-point a credential, and a compilation cannot silently pick up material a
/// revision was never published against.
#[async_trait]
pub trait SecretResolver: Send + Sync {
    fn name(&self) -> &'static str;

    fn capabilities(&self) -> Capabilities;

    /// Unwrap the material `reference` names, as `owner`.
    ///
    /// **Never call this from a request handler.** A failure here rejects a
    /// candidate revision; it must never become a request-time error.
    ///
    /// Refuses material another owner holds ([`SecretError::Ownership`]) and
    /// material whose state does not permit resolution
    /// ([`SecretError::Lifecycle`]), both before any unwrapping happens.
    async fn resolve(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretMaterial, SecretError>;

    /// Whether a reference still resolves for this owner, without unwrapping it.
    ///
    /// For `/admin/v1` reads and pre-publication checks that have no business
    /// holding plaintext. Answers `false` for material this owner may not use, so
    /// probing is not a way to enumerate another tenant's secrets.
    async fn exists(&self, owner: SecretOwner, reference: &SecretRef) -> Result<bool, SecretError>;
}

/// Storing, rotating, and administering material.
///
/// Everything here is metadata in, metadata out: not one method returns
/// plaintext, and [`SecretStore::stage`] and [`SecretStore::rotate`] *consume*
/// the material they are given. Resolution is deliberately a supertrait rather
/// than a method, so a caller can be handed the ability to read material without
/// the ability to change what any credential points at.
#[async_trait]
pub trait SecretStore: SecretResolver {
    /// Store material for `owner` and return the reference that names it.
    ///
    /// The new secret starts [`SecretLifecycle::Staged`]: material can be proven
    /// by compiling a candidate revision against it before anything routes
    /// through it. The plaintext is consumed, so a caller cannot keep a copy by
    /// accident.
    ///
    /// Not idempotent, and deliberately not: deduplicating identical material
    /// would mean comparing plaintext across owners, which is exactly the
    /// comparison a secret store should not be doing.
    async fn stage(
        &self,
        owner: SecretOwner,
        material: SecretMaterial,
    ) -> Result<SecretDescriptor, SecretError>;

    /// Store the next version of an existing secret, staged.
    ///
    /// Previous versions keep their own state and stay resolvable while they are
    /// resolvable, so a revision compiled against version 2 keeps hydrating after
    /// version 3 is staged. Putting the new version in service is a separate
    /// [`SecretStore::transition`] call.
    async fn rotate(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        material: SecretMaterial,
    ) -> Result<SecretDescriptor, SecretError>;

    /// Move a version to `next`, or refuse.
    ///
    /// Idempotent: a request for the state the version already holds returns
    /// [`LifecycleTransition::Unchanged`] rather than an error, because an
    /// administrative call is retried by clients and operators and a retry must
    /// not look like a conflict. Any move the domain does not define is
    /// [`SecretError::Transition`], and no move reads or returns plaintext.
    async fn transition(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        next: SecretLifecycle,
    ) -> Result<LifecycleTransition, SecretError>;

    /// What a store may say about one version without unwrapping it.
    async fn describe(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
    ) -> Result<SecretDescriptor, SecretError>;
}

#[cfg(test)]
mod tests {
    use super::super::{Capability, fakes::InMemorySecrets};
    use super::*;
    use crate::desired_state::fixtures::{project_id, secret_id, tenant_id};

    fn owner() -> SecretOwner {
        SecretOwner::tenant(tenant_id(1))
    }

    /// Material staged and put in service, which is what most cases start from.
    async fn active(store: &InMemorySecrets, owner: SecretOwner, plaintext: &str) -> SecretRef {
        let staged = store
            .stage(owner, SecretMaterial::new(plaintext.to_owned()))
            .await
            .expect("store");
        store
            .transition(owner, &staged.reference, SecretLifecycle::Active)
            .await
            .expect("staged material can be put in service");
        staged.reference
    }

    #[test]
    fn material_is_not_debuggable() {
        let material = SecretMaterial::new("sk-live-do-not-log".to_owned());
        let rendered = format!("{material:?}");
        assert_eq!(rendered, "SecretMaterial(<redacted>)");
        assert!(!rendered.contains("sk-live"));

        // A holder's derived Debug inherits the redaction.
        #[derive(Debug)]
        struct Holder {
            #[allow(dead_code)]
            material: SecretMaterial,
        }
        assert!(!format!("{:?}", Holder { material }).contains("sk-live"));
    }

    #[test]
    fn references_are_opaque_and_versioned() {
        let secret = secret_id(1);
        let reference = SecretRef::first(secret);
        let rendered = reference.to_string();
        assert_eq!(rendered, format!("{secret}@v1"));
        assert!(format!("{reference:?}").contains(&secret.uuid().to_string()));
        assert!(!rendered.contains("sk-"));

        // A descriptor is the reference plus non-secret metadata, and nothing a
        // holder of material could recognize it by.
        let descriptor = SecretDescriptor {
            reference,
            owner: owner(),
            lifecycle: SecretLifecycle::Active,
        };
        assert_eq!(
            descriptor.to_string(),
            format!("{secret}@v1 ({}, active)", owner())
        );
        assert!(!format!("{descriptor:?}").contains("sk-"));
    }

    #[test]
    fn error_messages_never_carry_material() {
        let reference = SecretRef::first(secret_id(2));
        let errors = [
            SecretError::NotFound(reference),
            SecretError::Unwrap {
                reference,
                kek: KekRef("AXOND_KEK".to_owned()),
            },
            SecretError::Ownership {
                reference,
                owner: owner(),
            },
            SecretError::Lifecycle {
                reference,
                state: SecretLifecycle::Revoked,
            },
            SecretError::Transition {
                reference,
                source: ForbiddenTransition {
                    from: SecretLifecycle::Revoked,
                    to: SecretLifecycle::Active,
                },
            },
            SecretError::Invalid("empty material".to_owned()),
        ];
        for error in errors {
            let rendered = error.to_string();
            assert!(!rendered.contains("sk-live-do-not-log"));
            assert!(!rendered.contains("AXOND_KEK_SECRET_VALUE"));
        }
    }

    #[tokio::test]
    async fn stored_material_round_trips_through_a_reference() {
        let store = InMemorySecrets::new();
        let staged = store
            .stage(owner(), SecretMaterial::new("sk-live-1".to_owned()))
            .await
            .expect("store");
        // Material starts staged, resolvable, and not yet in service.
        assert_eq!(staged.lifecycle, SecretLifecycle::Staged);
        assert!(store.exists(owner(), &staged.reference).await.unwrap());
        assert_eq!(
            store
                .resolve(owner(), &staged.reference)
                .await
                .unwrap()
                .expose(),
            "sk-live-1"
        );
        assert_eq!(
            store.describe(owner(), &staged.reference).await.unwrap(),
            staged
        );
    }

    #[tokio::test]
    async fn rotation_keeps_earlier_versions_resolvable() {
        let store = InMemorySecrets::new();
        let first = active(&store, owner(), "sk-live-1").await;
        let second = store
            .rotate(owner(), &first, SecretMaterial::new("sk-live-2".to_owned()))
            .await
            .unwrap();

        assert_eq!(second.reference.secret, first.secret);
        assert_eq!(second.reference.version, first.version.next());
        // The new version is staged: rotating stores material, it does not put it
        // in service.
        assert_eq!(second.lifecycle, SecretLifecycle::Staged);
        assert_eq!(
            store.describe(owner(), &first).await.unwrap().lifecycle,
            SecretLifecycle::Active
        );
        // A revision compiled against the old version still hydrates.
        assert_eq!(
            store.resolve(owner(), &first).await.unwrap().expose(),
            "sk-live-1"
        );
        assert_eq!(
            store
                .resolve(owner(), &second.reference)
                .await
                .unwrap()
                .expose(),
            "sk-live-2"
        );
    }

    #[tokio::test]
    async fn empty_material_is_rejected_rather_than_stored() {
        let store = InMemorySecrets::new();
        let error = store
            .stage(owner(), SecretMaterial::new(String::new()))
            .await
            .expect_err("empty material is unusable");
        assert_eq!(error.category(), FailureCategory::Invalid);
    }

    #[tokio::test]
    async fn an_unwrappable_secret_is_corrupt_not_unavailable() {
        let store = InMemorySecrets::new();
        let reference = active(&store, owner(), "sk-live-1").await;
        store.break_kek();

        let error = store
            .resolve(owner(), &reference)
            .await
            .expect_err("KEK is wrong");
        assert_eq!(error.category(), FailureCategory::Corrupt);
        assert!(!error.retryable());
        // Presence is still answerable without unwrapping.
        assert!(store.exists(owner(), &reference).await.unwrap());
    }

    #[tokio::test]
    async fn a_missing_reference_is_distinguishable_from_an_outage() {
        let store = InMemorySecrets::new();
        let unknown = SecretRef::first(secret_id(3));
        assert_eq!(
            store
                .resolve(owner(), &unknown)
                .await
                .unwrap_err()
                .category(),
            FailureCategory::NotFound
        );

        store.set_unavailable(true);
        let outage = store.resolve(owner(), &unknown).await.expect_err("outage");
        assert_eq!(outage.category(), FailureCategory::Unavailable);
        assert!(outage.retryable());
    }

    #[tokio::test]
    async fn material_never_resolves_for_another_owner() {
        let store = InMemorySecrets::new();
        let intruder = SecretOwner::tenant(tenant_id(9));
        let sibling = SecretOwner::project(tenant_id(1), project_id(2));
        let reference = active(&store, owner(), "sk-live-1").await;

        for other in [intruder, sibling] {
            let error = store
                .resolve(other, &reference)
                .await
                .expect_err("material belongs to its owner alone");
            assert_eq!(
                error,
                SecretError::Ownership {
                    reference,
                    owner: other
                }
            );
            // Indistinguishable from material that was never stored: holding a
            // reference reveals nothing about whether it exists.
            assert_eq!(error.category(), FailureCategory::NotFound);
            assert!(!store.exists(other, &reference).await.unwrap());
            assert_eq!(
                store.describe(other, &reference).await.unwrap_err(),
                SecretError::Ownership {
                    reference,
                    owner: other
                }
            );
            // Nor may another owner administer it.
            assert!(
                store
                    .transition(other, &reference, SecretLifecycle::Revoked)
                    .await
                    .is_err()
            );
            assert!(
                store
                    .rotate(
                        other,
                        &reference,
                        SecretMaterial::new("sk-live-x".to_owned())
                    )
                    .await
                    .is_err()
            );
        }

        // The owner is unaffected by the attempts.
        assert_eq!(
            store.describe(owner(), &reference).await.unwrap().lifecycle,
            SecretLifecycle::Active
        );
    }

    #[tokio::test]
    async fn only_staged_and_active_material_unwraps() {
        let store = InMemorySecrets::new();
        let reference = active(&store, owner(), "sk-live-1").await;

        // Disabling withholds material without destroying it, and re-enabling
        // makes it resolvable again.
        store
            .transition(owner(), &reference, SecretLifecycle::Disabled)
            .await
            .unwrap();
        let error = store
            .resolve(owner(), &reference)
            .await
            .expect_err("disabled material does not unwrap");
        assert_eq!(
            error,
            SecretError::Lifecycle {
                reference,
                state: SecretLifecycle::Disabled
            }
        );
        assert_eq!(error.category(), FailureCategory::Denied);
        assert!(
            store.exists(owner(), &reference).await.unwrap(),
            "disabled material is still stored"
        );
        store
            .transition(owner(), &reference, SecretLifecycle::Active)
            .await
            .unwrap();
        assert_eq!(
            store.resolve(owner(), &reference).await.unwrap().expose(),
            "sk-live-1"
        );

        // Revoking is one-way, and tombstoning destroys the material.
        store
            .transition(owner(), &reference, SecretLifecycle::Revoked)
            .await
            .unwrap();
        assert!(store.resolve(owner(), &reference).await.is_err());
        assert_eq!(
            store
                .transition(owner(), &reference, SecretLifecycle::Active)
                .await
                .expect_err("revoked material is never put back in service"),
            SecretError::Transition {
                reference,
                source: ForbiddenTransition {
                    from: SecretLifecycle::Revoked,
                    to: SecretLifecycle::Active
                }
            }
        );
        store
            .transition(owner(), &reference, SecretLifecycle::Tombstoned)
            .await
            .unwrap();
        assert!(
            !store.holds_material(&reference),
            "tombstoning destroys the material, not just its state"
        );
        assert_eq!(
            store.describe(owner(), &reference).await.unwrap().lifecycle,
            SecretLifecycle::Tombstoned
        );
    }

    #[tokio::test]
    async fn a_repeated_lifecycle_request_is_a_no_op_not_a_conflict() {
        let store = InMemorySecrets::new();
        let staged = store
            .stage(owner(), SecretMaterial::new("sk-live-1".to_owned()))
            .await
            .unwrap();
        let reference = staged.reference;

        let first = store
            .transition(owner(), &reference, SecretLifecycle::Active)
            .await
            .unwrap();
        assert_eq!(
            first,
            LifecycleTransition::Moved {
                from: SecretLifecycle::Staged,
                to: SecretLifecycle::Active
            }
        );
        // The retry an operator, a proxy, or a client makes.
        for _ in 0..3 {
            assert_eq!(
                store
                    .transition(owner(), &reference, SecretLifecycle::Active)
                    .await
                    .unwrap(),
                LifecycleTransition::Unchanged(SecretLifecycle::Active)
            );
        }
        assert_eq!(
            store.describe(owner(), &reference).await.unwrap().lifecycle,
            SecretLifecycle::Active
        );
        // Terminal states are idempotent too, tombstoned included.
        store
            .transition(owner(), &reference, SecretLifecycle::Revoked)
            .await
            .unwrap();
        store
            .transition(owner(), &reference, SecretLifecycle::Tombstoned)
            .await
            .unwrap();
        assert_eq!(
            store
                .transition(owner(), &reference, SecretLifecycle::Tombstoned)
                .await
                .unwrap(),
            LifecycleTransition::Unchanged(SecretLifecycle::Tombstoned)
        );
    }

    #[tokio::test]
    async fn rotation_of_an_unknown_or_tombstoned_version_is_refused() {
        let store = InMemorySecrets::new();
        let unknown = SecretRef::first(secret_id(4));
        assert_eq!(
            store
                .rotate(owner(), &unknown, SecretMaterial::new("sk-live".to_owned()))
                .await
                .unwrap_err(),
            SecretError::NotFound(unknown)
        );

        let reference = active(&store, owner(), "sk-live-1").await;
        store
            .transition(owner(), &reference, SecretLifecycle::Revoked)
            .await
            .unwrap();
        store
            .transition(owner(), &reference, SecretLifecycle::Tombstoned)
            .await
            .unwrap();
        assert_eq!(
            store
                .rotate(
                    owner(),
                    &reference,
                    SecretMaterial::new("sk-live-2".to_owned())
                )
                .await
                .unwrap_err()
                .category(),
            FailureCategory::Denied,
            "tombstoned material has no next version"
        );
    }

    #[test]
    fn selectable_backends_wrap_or_delegate() {
        assert_eq!(SecretBackend::default(), SecretBackend::EncryptedPostgres);
        assert_eq!(
            SecretBackend::EncryptedPostgres.kind(),
            BackendKind::Postgres
        );
        assert_eq!(
            SecretBackend::External.kind(),
            BackendKind::ExternalSecretManager
        );
        let responsibility =
            super::super::responsibility("SecretStore").expect("declared responsibility");
        for backend in [SecretBackend::EncryptedPostgres, SecretBackend::External] {
            assert!(responsibility.permits(backend.kind()));
        }
        assert!(!responsibility.permits(BackendKind::Redis));
    }

    #[tokio::test]
    async fn encrypted_postgres_declares_envelope_encryption() {
        let store = InMemorySecrets::new();
        assert!(store.capabilities().has(Capability::EnvelopeEncryption));
        assert!(!store.capabilities().has(Capability::ExternalKeyManagement));
    }
}
