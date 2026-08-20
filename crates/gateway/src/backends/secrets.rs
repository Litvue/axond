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

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use secrecy::zeroize::Zeroize;
use secrecy::{ExposeSecret, SecretString};

use super::{BackendFailure, BackendKind, Capabilities, Capability, FailureCategory};
use crate::desired_state::ids::SecretId;
use crate::desired_state::namespaces::NamespaceSecretRequest;
use crate::desired_state::secrets::{
    ForbiddenTransition, LifecycleTransition, SecretLifecycle, SecretOwner, SecretRef,
};

pub mod blob_envelope;
pub mod envelope;
pub mod postgres;

/// What the envelope-encrypted store can do: material is wrapped under a
/// deployment key-encryption key, and unwrapping happens in this process.
///
/// [`Capability::ExternalKeyManagement`] is deliberately absent — an external
/// manager is a second adapter behind this contract, and declaring the capability
/// here would tell a caller unwrapping happens somewhere it does not.
pub const ENVELOPE_CAPABILITIES: Capabilities =
    Capabilities::new(&[Capability::EnvelopeEncryption]);

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
    /// The version a rotation would mint is already stored.
    ///
    /// A version is immutable, so this is the outcome of rotating twice from one
    /// base reference — the second call's work was already done, by the first or
    /// by another administrator. Distinct from [`Self::Invalid`] because the
    /// material presented with it was never examined: reporting it as a bad key
    /// is how an operator comes to re-issue a good one.
    #[error("secret {reference} is already stored, so it cannot be minted again")]
    VersionExists { reference: SecretRef },
    /// Stored metadata cannot be interpreted. This is a store problem, not a
    /// refusal of material presented by the caller.
    #[error("secret store metadata is corrupt: {detail}")]
    Corrupt { detail: String },
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
            // The caller's request lost to a rotation that already happened,
            // which is what a conflict is: replaying it cannot win.
            Self::VersionExists { .. } => FailureCategory::Conflict,
            Self::Corrupt { .. } => FailureCategory::Corrupt,
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

    /// Whether this owner holds `reference` in a state that may be resolved,
    /// without unwrapping it.
    ///
    /// For `/admin/v1` reads and pre-publication checks that have no business
    /// holding plaintext. Ownership and lifecycle, both of which a store knows
    /// without touching the bytes: anything withdrawn — disabled, revoked, or
    /// tombstoned — answers `false`, so a check cannot approve a credential that
    /// will never authorize anything, and a caller that wants the *reason* asks
    /// [`SecretStore::describe`].
    ///
    /// It is not a substitute for [`SecretResolver::resolve`], and cannot be:
    /// whether the material still *unwraps* — under the current KEK, from an
    /// intact record — is only answerable by unwrapping it, so material a rotated
    /// or lost KEK has made unreadable answers `true` here and
    /// [`SecretError::Unwrap`] there. Compiling a candidate revision is what
    /// proves material, and that is deliberate: `exists` exists so a caller that
    /// must not hold plaintext does not have to.
    ///
    /// Another owner's material answers `false`, and identically to an absent
    /// reference, so probing is not a way to enumerate another tenant's secrets or
    /// to tell a foreign reference from one that was never stored.
    async fn exists(&self, owner: SecretOwner, reference: &SecretRef) -> Result<bool, SecretError>;

    /// Resolve one ADR 0062 ciphertext through its authoritative namespace
    /// binding.
    ///
    /// Existing tenant/project stores fail closed by default. Blob-backed
    /// implementations override this method and must verify the request's owner,
    /// exact reference, ciphertext digest, and lifecycle before unwrapping.
    async fn resolve_namespace(
        &self,
        request: &NamespaceSecretRequest,
    ) -> Result<SecretMaterial, SecretError> {
        Err(SecretError::Denied {
            backend: self.name(),
            message: format!(
                "namespace-bound secret {} for `{}` is not supported by this backend",
                request.reference(),
                request.owner()
            ),
        })
    }

    async fn exists_namespace(
        &self,
        _request: &NamespaceSecretRequest,
    ) -> Result<bool, SecretError> {
        Ok(false)
    }
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

    /// Every version of one secret this owner holds, oldest version first.
    ///
    /// The overlap a rotation creates is only administrable if it is visible: an
    /// operator activating a staged version, and then withdrawing the one it
    /// supersedes, is acting on two versions that exist at once. This is the read
    /// that shows both, and — like [`SecretStore::describe`] — it unwraps
    /// nothing.
    ///
    /// A secret another owner holds answers with no versions, exactly as one that
    /// was never stored does, so listing is not a way to learn that a foreign
    /// secret exists.
    async fn versions(
        &self,
        owner: SecretOwner,
        secret: SecretId,
    ) -> Result<Vec<SecretDescriptor>, SecretError>;
}

/// Build the configured store, resolving its DSN and its KEK from the
/// references bootstrap config names.
///
/// Connecting here means a misconfigured or unreachable secret store refuses to
/// boot, rather than failing every candidate revision later with an error that
/// points at the revision. The KEK is read at exactly this one point: the store
/// itself is handed the key, never the reference's contents, so there is a
/// single place in the process where key material is loaded.
pub async fn build(
    secret_store: &crate::config::SecretStore,
    control_plane: &crate::config::ControlPlane,
    env: &HashMap<String, String>,
) -> Result<Arc<dyn SecretStore>, SecretError> {
    build_with_mode(secret_store, control_plane, env, false).await
}

/// Build the secret store while allowing a previously authenticated compiled
/// snapshot to carry serving through an initial Postgres outage. Only an
/// unavailable connection is deferred; malformed configuration, missing KEK,
/// or a schema/permission refusal still fails boot.
pub async fn build_allow_unavailable(
    secret_store: &crate::config::SecretStore,
    control_plane: &crate::config::ControlPlane,
    env: &HashMap<String, String>,
) -> Result<Arc<dyn SecretStore>, SecretError> {
    build_with_mode(secret_store, control_plane, env, true).await
}

async fn build_with_mode(
    secret_store: &crate::config::SecretStore,
    control_plane: &crate::config::ControlPlane,
    env: &HashMap<String, String>,
    allow_unavailable: bool,
) -> Result<Arc<dyn SecretStore>, SecretError> {
    match secret_store.backend {
        crate::config::SecretStoreBackend::Postgres => {
            let dsn = dsn(secret_store, control_plane, env)?;
            let kek = deployment_kek(secret_store, env)?;
            let settings = postgres::SecretStoreSettings::from_config(secret_store, control_plane);
            if allow_unavailable {
                Ok(Arc::new(
                    postgres::PostgresSecrets::connect_or_defer(&dsn, settings, kek).await?,
                ))
            } else {
                Ok(Arc::new(
                    postgres::PostgresSecrets::connect(&dsn, settings, kek).await?,
                ))
            }
        }
    }
}

/// The store's connection string, from its own reference or inherited from the
/// control plane's. Only the *name* appears in the failure: the value is a DSN.
fn dsn(
    secret_store: &crate::config::SecretStore,
    control_plane: &crate::config::ControlPlane,
    env: &HashMap<String, String>,
) -> Result<String, SecretError> {
    fn named(name: &Option<String>) -> Option<&str> {
        name.as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
    }
    // Each candidate is trimmed before it is considered, so a blank
    // `[secret_store] dsn_env` inherits the control plane's — the same reading
    // configuration validation takes.
    let name = named(&secret_store.dsn_env)
        .or_else(|| named(&control_plane.dsn_env))
        .ok_or_else(|| {
            denied("no `dsn_env` names the secret store's connection string".to_owned())
        })?;
    env.get(name)
        .map(|dsn| dsn.trim().to_owned())
        .filter(|dsn| !dsn.is_empty())
        .ok_or_else(|| denied(format!("`{name}` is unset or empty")))
}

/// Read the deployment KEK from the env var or file bootstrap config names.
fn deployment_kek(
    secret_store: &crate::config::SecretStore,
    env: &HashMap<String, String>,
) -> Result<envelope::DeploymentKek, SecretError> {
    let (source, name) = secret_store.kek_reference().ok_or_else(|| {
        denied("`[secret_store]` names no key-encryption key to unwrap material with".to_owned())
    })?;
    let reference = KekRef(format!("{source}:{name}"));
    // The encoded key is zeroized on every path out of here, including the
    // failing ones: `DeploymentKek::parse` only owns what it decodes, not the
    // encoding this function read.
    let mut encoded = match source {
        "kek_env" => env
            .get(name)
            .cloned()
            .ok_or_else(|| denied(format!("`{name}` is unset")))?,
        _ => std::fs::read_to_string(name)
            .map_err(|error| denied(format!("`{name}` could not be read: {error}")))?,
    };
    // The failure names the reference and the reason, never the material.
    let kek = envelope::DeploymentKek::parse(reference, &encoded)
        .map_err(|error| denied(format!("the deployment KEK is unusable: {error}")));
    encoded.zeroize();
    kek
}

/// A refusal that names the backend, and never the value it was reading.
fn denied(message: String) -> SecretError {
    SecretError::Denied {
        backend: "encrypted-postgres",
        message,
    }
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

        // Rotating again from the *old* reference is a stale request, not a
        // second rotation: version 2 already exists, and overwriting it would
        // change what a credential body already pinning it resolves to.
        let error = store
            .rotate(owner(), &first, SecretMaterial::new("sk-live-3".to_owned()))
            .await
            .expect_err("a version is immutable once it is stored");
        // A conflict, not an invalid request: the presented material was never
        // examined, so the answer must not accuse it of anything.
        assert_eq!(
            error,
            SecretError::VersionExists {
                reference: second.reference
            }
        );
        assert_eq!(error.category(), FailureCategory::Conflict);
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
        // Ownership and lifecycle are still answerable without unwrapping, and
        // that is all `exists` claims: unwrappability is only provable by
        // unwrapping, which is what compiling a candidate revision does.
        assert!(store.exists(owner(), &reference).await.unwrap());
        assert!(
            store
                .describe(owner(), &reference)
                .await
                .unwrap()
                .permits_resolution()
        );
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
        // A presence check answers what resolution would do, so a
        // pre-publication check cannot approve withdrawn material. `describe`
        // still says *why*, without unwrapping anything.
        assert!(
            !store.exists(owner(), &reference).await.unwrap(),
            "disabled material would not resolve"
        );
        let descriptor = store.describe(owner(), &reference).await.unwrap();
        assert_eq!(descriptor.lifecycle, SecretLifecycle::Disabled);
        assert!(!descriptor.lifecycle.permits_resolution());
        store
            .transition(owner(), &reference, SecretLifecycle::Active)
            .await
            .unwrap();
        assert_eq!(
            store.resolve(owner(), &reference).await.unwrap().expose(),
            "sk-live-1"
        );
        assert!(store.exists(owner(), &reference).await.unwrap());

        // Revoking is one-way, and tombstoning destroys the material.
        store
            .transition(owner(), &reference, SecretLifecycle::Revoked)
            .await
            .unwrap();
        assert!(store.resolve(owner(), &reference).await.is_err());
        assert!(
            !store.exists(owner(), &reference).await.unwrap(),
            "revoked material would not resolve"
        );
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
        assert!(
            !store.exists(owner(), &reference).await.unwrap(),
            "destroyed material would not resolve"
        );
        assert!(store.resolve(owner(), &reference).await.is_err());
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
    /// A `[secret_store]` section naming the env vars this test controls.
    fn section() -> crate::config::SecretStore {
        crate::config::SecretStore {
            backend: crate::config::SecretStoreBackend::Postgres,
            dsn_env: Some("AXOND_SECRET_STORE_DSN".to_owned()),
            kek_env: Some("AXOND_SECRET_STORE_KEK".to_owned()),
            kek_file: None,
            schema: None,
            create_table: true,
        }
    }

    fn control_plane() -> crate::config::ControlPlane {
        crate::config::ControlPlane {
            dsn_env: Some("AXOND_CONTROL_PLANE_DSN".to_owned()),
            schema: None,
            migrate: false,
            connect_timeout_ms: 5_000,
            operation_timeout_ms: 30_000,
        }
    }

    /// A 32-byte key, base64, which is what an operator puts behind the
    /// reference.
    fn encoded_kek() -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode([7u8; envelope::KEY_LEN])
    }

    /// The store's own reference wins; the control plane's is what it falls back
    /// to, because encrypted Postgres is normally the same database.
    #[test]
    fn the_connection_string_is_the_store_s_reference_or_the_control_plane_s() {
        let env = HashMap::from([
            (
                "AXOND_SECRET_STORE_DSN".to_owned(),
                "postgres://own".to_owned(),
            ),
            (
                "AXOND_CONTROL_PLANE_DSN".to_owned(),
                "postgres://inherited".to_owned(),
            ),
        ]);
        assert_eq!(
            dsn(&section(), &control_plane(), &env).expect("its own reference"),
            "postgres://own"
        );
        let inheriting = crate::config::SecretStore {
            dsn_env: None,
            ..section()
        };
        assert_eq!(
            dsn(&inheriting, &control_plane(), &env).expect("the inherited reference"),
            "postgres://inherited"
        );
    }

    /// A blank reference is an absent one, the reading configuration validation
    /// takes — otherwise a section validation accepted would refuse to boot.
    #[test]
    fn a_blank_reference_inherits_the_control_plane_s_instead_of_refusing() {
        let env = HashMap::from([(
            "AXOND_CONTROL_PLANE_DSN".to_owned(),
            "postgres://inherited".to_owned(),
        )]);
        for blank in ["", "   ", "\n"] {
            let section = crate::config::SecretStore {
                dsn_env: Some(blank.to_owned()),
                ..section()
            };
            assert_eq!(
                dsn(&section, &control_plane(), &env).expect("the inherited reference"),
                "postgres://inherited",
                "a `dsn_env` of {blank:?} should inherit"
            );
        }
    }

    /// An unset reference is a refusal that names the variable and never a value.
    #[test]
    fn an_unresolvable_connection_string_is_refused_by_name() {
        let error = dsn(&section(), &control_plane(), &HashMap::new())
            .expect_err("nothing is set in this environment");
        assert_eq!(error.category(), FailureCategory::Denied);
        assert!(
            error.to_string().contains("AXOND_SECRET_STORE_DSN"),
            "{error}"
        );
        assert!(!error.to_string().contains("postgres://"), "{error}");
    }

    /// The KEK is read from whichever source the section names, and an unusable
    /// one is refused without the material appearing in the failure.
    #[test]
    fn the_kek_is_read_from_the_reference_and_never_echoed() {
        let encoded = encoded_kek();
        let env = HashMap::from([("AXOND_SECRET_STORE_KEK".to_owned(), encoded.clone())]);
        let kek = deployment_kek(&section(), &env).expect("a 32-byte key");
        assert_eq!(
            kek.reference().to_string(),
            "kek_env:AXOND_SECRET_STORE_KEK"
        );

        let short = HashMap::from([("AXOND_SECRET_STORE_KEK".to_owned(), {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode([7u8; 16])
        })]);
        let error = deployment_kek(&section(), &short).expect_err("half a key is not a key");
        assert_eq!(error.category(), FailureCategory::Denied);
        assert!(!error.to_string().contains(&encoded), "{error}");

        let missing = deployment_kek(&section(), &HashMap::new())
            .expect_err("an unset reference cannot be unwrapped with");
        assert!(
            missing.to_string().contains("AXOND_SECRET_STORE_KEK"),
            "{missing}"
        );
    }

    /// A file-referenced KEK is read from disk, and an unreadable path is a
    /// refusal rather than a boot that cannot decrypt anything.
    #[test]
    fn a_file_referenced_kek_is_read_from_disk() {
        let path = std::env::temp_dir().join(format!(
            "axond-kek-{}",
            crate::desired_state::Uuid7Generator::new().next()
        ));
        std::fs::write(&path, format!("{}\n", encoded_kek())).expect("write the key file");
        let section = crate::config::SecretStore {
            kek_env: None,
            kek_file: Some(path.to_string_lossy().into_owned()),
            ..section()
        };
        let kek =
            deployment_kek(&section, &HashMap::new()).expect("a trailing newline is tolerated");
        assert!(kek.reference().to_string().starts_with("kek_file:"));
        std::fs::remove_file(&path).expect("clean up");

        let error = deployment_kek(&section, &HashMap::new())
            .expect_err("the file is gone, so the key cannot be read");
        assert_eq!(error.category(), FailureCategory::Denied);
    }

    /// The configured store connects, applies its own schema, and serves
    /// material: the boot path an operator's `[secret_store]` section takes.
    #[tokio::test]
    async fn the_configured_store_boots_and_serves_material() {
        let Some(dsn) = crate::test_services::postgres_dsn() else {
            return;
        };
        let env = HashMap::from([
            ("AXOND_SECRET_STORE_DSN".to_owned(), dsn),
            ("AXOND_SECRET_STORE_KEK".to_owned(), encoded_kek()),
        ]);
        let schema = format!(
            "axond_secret_boot_{}",
            crate::desired_state::Uuid7Generator::new()
                .next()
                .to_string()
                .replace('-', "")
        );
        let section = crate::config::SecretStore {
            schema: Some(schema.clone()),
            ..section()
        };
        // The schema is the operator's; the store owns only its table in it.
        let (client, connection) = tokio_postgres::connect(
            env.get("AXOND_SECRET_STORE_DSN").expect("the test DSN"),
            crate::usage::tls_connector(),
        )
        .await
        .expect("connect to the test database");
        let cleanup = tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("create the test schema");

        let store = build(&section, &control_plane(), &env)
            .await
            .expect("the configured store boots");
        let staged = store
            .stage(owner(), SecretMaterial::new("sk-live-boot".to_owned()))
            .await
            .expect("staging through the built store");
        assert_eq!(
            store
                .resolve(owner(), &staged.reference)
                .await
                .expect("staged material resolves")
                .expose(),
            "sk-live-boot"
        );

        client
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .await
            .expect("drop the test schema");
        drop(client);
        cleanup.abort();
    }
}
