//! Wrapped secret material: the [`SecretStore`] contract.
//!
//! Encrypted Postgres is the first implementation: tenant provider credentials
//! are stored wrapped under a key-encryption key referenced from bootstrap, and
//! external managers (Vault, cloud KMS) are later adapters behind this same
//! contract.
//!
//! Two invariants, both enforced by the types rather than by convention:
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

use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};

use super::control_plane::ResourceId;
use super::{BackendFailure, BackendKind, Capabilities, FailureCategory};

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

/// An opaque handle to stored secret material.
///
/// This is the only secret-shaped thing that may be persisted in a revision,
/// returned from `/admin/v1`, or logged. It identifies material and reveals
/// nothing about it: two references are comparable, and neither is decodable.
/// Rotation produces a new `version` under the same `id`, so a manifest pins the
/// exact material a revision was compiled against.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SecretRef {
    pub id: ResourceId,
    pub version: u32,
}

impl std::fmt::Display for SecretRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "secret:{}@{}", self.id.0, self.version)
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

/// Why a secret operation failed.
///
/// No arm carries material, a KEK, or a ciphertext — only the reference and the
/// backend, so every one of these is safe to log verbatim.
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
            Self::NotFound(_) => FailureCategory::NotFound,
            Self::Unwrap { .. } => FailureCategory::Corrupt,
            Self::Invalid(_) => FailureCategory::Invalid,
            Self::Denied { .. } => FailureCategory::Denied,
        }
    }
}

/// Secret material, resolved during snapshot compilation only.
#[async_trait]
pub trait SecretStore: Send + Sync {
    fn name(&self) -> &'static str;

    fn capabilities(&self) -> Capabilities;

    /// Store material and return the reference that names it.
    ///
    /// The plaintext is consumed: a caller cannot keep a copy by accident.
    async fn store(&self, material: SecretMaterial) -> Result<SecretRef, SecretError>;

    /// Replace the material behind an existing reference, returning the new
    /// version. Previous versions remain resolvable so revisions compiled
    /// against them stay hydratable.
    async fn rotate(
        &self,
        reference: &SecretRef,
        material: SecretMaterial,
    ) -> Result<SecretRef, SecretError>;

    /// Unwrap material for snapshot compilation.
    ///
    /// **Never call this from a request handler.** A failure here rejects a
    /// candidate revision; it must never become a request-time error.
    async fn resolve(&self, reference: &SecretRef) -> Result<SecretMaterial, SecretError>;

    /// Whether a reference still resolves, without unwrapping it. For
    /// `/admin/v1` reads and pre-publication checks that have no business
    /// holding plaintext.
    async fn exists(&self, reference: &SecretRef) -> Result<bool, SecretError>;
}

#[cfg(test)]
mod tests {
    use super::super::{Capability, fakes::InMemorySecrets};
    use super::*;

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
        let reference = SecretRef {
            id: ResourceId("0191f0a1-credential".to_owned()),
            version: 3,
        };
        let rendered = reference.to_string();
        assert_eq!(rendered, "secret:0191f0a1-credential@3");
        assert!(format!("{reference:?}").contains("0191f0a1-credential"));
        assert!(!rendered.contains("sk-"));
    }

    #[test]
    fn error_messages_never_carry_material() {
        let reference = SecretRef {
            id: ResourceId("cred".to_owned()),
            version: 1,
        };
        let errors = [
            SecretError::NotFound(reference.clone()),
            SecretError::Unwrap {
                reference,
                kek: KekRef("AXOND_KEK".to_owned()),
            },
            SecretError::Invalid("empty material".to_owned()),
        ];
        for error in errors {
            assert!(!error.to_string().contains("sk-live-do-not-log"));
        }
    }

    #[tokio::test]
    async fn stored_material_round_trips_through_a_reference() {
        let store = InMemorySecrets::new();
        let reference = store
            .store(SecretMaterial::new("sk-live-1".to_owned()))
            .await
            .expect("store");
        assert!(store.exists(&reference).await.unwrap());
        assert_eq!(
            store.resolve(&reference).await.unwrap().expose(),
            "sk-live-1"
        );
    }

    #[tokio::test]
    async fn rotation_keeps_earlier_versions_resolvable() {
        let store = InMemorySecrets::new();
        let first = store
            .store(SecretMaterial::new("sk-live-1".to_owned()))
            .await
            .unwrap();
        let second = store
            .rotate(&first, SecretMaterial::new("sk-live-2".to_owned()))
            .await
            .unwrap();

        assert_eq!(second.id, first.id);
        assert_eq!(second.version, first.version + 1);
        // A revision compiled against the old version still hydrates.
        assert_eq!(store.resolve(&first).await.unwrap().expose(), "sk-live-1");
        assert_eq!(store.resolve(&second).await.unwrap().expose(), "sk-live-2");
    }

    #[tokio::test]
    async fn empty_material_is_rejected_rather_than_stored() {
        let store = InMemorySecrets::new();
        let error = store
            .store(SecretMaterial::new(String::new()))
            .await
            .expect_err("empty material is unusable");
        assert_eq!(error.category(), FailureCategory::Invalid);
    }

    #[tokio::test]
    async fn an_unwrappable_secret_is_corrupt_not_unavailable() {
        let store = InMemorySecrets::new();
        let reference = store
            .store(SecretMaterial::new("sk-live-1".to_owned()))
            .await
            .unwrap();
        store.break_kek();

        let error = store.resolve(&reference).await.expect_err("KEK is wrong");
        assert_eq!(error.category(), FailureCategory::Corrupt);
        assert!(!error.retryable());
        // Presence is still answerable without unwrapping.
        assert!(store.exists(&reference).await.unwrap());
    }

    #[tokio::test]
    async fn a_missing_reference_is_distinguishable_from_an_outage() {
        let store = InMemorySecrets::new();
        let unknown = SecretRef {
            id: ResourceId("absent".to_owned()),
            version: 1,
        };
        assert_eq!(
            store.resolve(&unknown).await.unwrap_err().category(),
            FailureCategory::NotFound
        );

        store.set_unavailable(true);
        let outage = store.resolve(&unknown).await.expect_err("outage");
        assert_eq!(outage.category(), FailureCategory::Unavailable);
        assert!(outage.retryable());
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
