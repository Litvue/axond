//! Read-only hydration of one authenticated blob publication revision.
//!
//! This is deliberately not a [`ControlPlaneStore`](crate::backends::control_plane::ControlPlaneStore):
//! blob identity is the signed head's `(sequence, revision digest)`, and this
//! source neither fabricates a [`RevisionId`](super::RevisionId) nor offers an
//! administrative mutation surface. Runtime compilation, secret opening, and
//! process wiring belong to later slices; the authenticated candidate cache is
//! the durable boundary shared by those future consumers.

use std::collections::BTreeMap;

use crate::backends::object_store::ObjectStore;

use super::namespaces::{FlatNamespaces, NamespaceSecretRequest};
use super::secret_binding::AuthenticatedSecretBinding;
use super::{
    ActivationReadyRevision, BlobPublicationError, BlobReader, BlobRef, BlobResourceDocument,
    BlobResourceDocumentError, Canonical, CanonicalError, Checksum, DesiredState, EnvironmentId,
    ImmutableObjectKind, InvalidEnvironmentId, NamespaceStateError, ResourceRef, SecretLifecycle,
    ValidationError,
};

/// Resource bounds applied while hydrating one authenticated blob revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobHydrationLimits {
    pub max_resource_bytes: usize,
    pub max_blob_bytes: u64,
    pub max_state_bytes: usize,
}

impl Default for BlobHydrationLimits {
    fn default() -> Self {
        Self {
            max_resource_bytes: 256 * 1024 * 1024,
            max_blob_bytes: 4 * 1024 * 1024 * 1024,
            max_state_bytes: 512 * 1024 * 1024,
        }
    }
}

/// The bound an authenticated blob revision exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum BlobHydrationLimit {
    #[error(
        "immutable resource documents total {observed} bytes, over the {limit}-byte hydration limit"
    )]
    ResourceBytes { observed: usize, limit: usize },
    #[error(
        "resource documents declare {observed} blob bytes, over the {limit}-byte hydration limit"
    )]
    BlobBytes { observed: u64, limit: u64 },
    #[error("hydrated desired state is {observed} bytes, over the {limit}-byte hydration limit")]
    StateBytes { observed: usize, limit: usize },
}

/// Why an authenticated blob head could not become a complete candidate.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlobRevisionError {
    #[error(transparent)]
    Publication(#[from] BlobPublicationError),
    #[error("authenticated manifest contains an unexpected {kind:?} object")]
    UnexpectedObject { kind: ImmutableObjectKind },
    #[error("authenticated resource object {digest} is not a valid resource document: {source}")]
    ResourceDocument {
        digest: Checksum,
        source: BlobResourceDocumentError,
    },
    #[error("authenticated resource objects contain {reference} more than once")]
    DuplicateResource { reference: ResourceRef },
    #[error("authenticated resource objects disagree about blob {digest}")]
    ConflictingBlobDeclaration { digest: Checksum },
    #[error("hydrated desired state is invalid: {0}")]
    InvalidDesiredState(ValidationError),
    #[error("hydrated desired state has no canonical form: {0}")]
    Canonical(#[from] CanonicalError),
    #[error("authenticated manifest checksum is {expected}, but hydrated state hashes to {actual}")]
    ChecksumMismatch {
        expected: Checksum,
        actual: Checksum,
    },
    #[error(transparent)]
    Limit(#[from] BlobHydrationLimit),
}

/// Why a validated blob candidate could not become a namespace secret authority.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlobSecretAuthorityError {
    #[error(transparent)]
    Namespace(#[from] NamespaceStateError),
    #[error(transparent)]
    Environment(#[from] InvalidEnvironmentId),
}

/// Blob-native identity of one authenticated active revision.
///
/// This is intentionally distinct from the PostgreSQL journal's `RevisionId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobRevisionIdentity {
    sequence: u64,
    digest: Checksum,
}

impl BlobRevisionIdentity {
    pub(crate) const fn new(sequence: u64, digest: Checksum) -> Self {
        Self { sequence, digest }
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    pub const fn digest(self) -> Checksum {
        self.digest
    }
}

/// A complete desired state whose exact selecting head passed the final fence.
///
/// The activation evidence is single-use, so this candidate is not cloneable.
#[derive(Debug)]
pub struct BlobCandidate {
    identity: BlobRevisionIdentity,
    state: DesiredState,
    activation: ActivationReadyRevision,
}

impl BlobCandidate {
    pub const fn identity(&self) -> BlobRevisionIdentity {
        self.identity
    }

    pub const fn state(&self) -> &DesiredState {
        &self.state
    }

    /// The exact head fence that made this candidate eligible for activation.
    /// A blob-native cache writer uses this binding to ensure it cannot persist
    /// a candidate under a different environment than the one it authenticated.
    pub const fn activation(&self) -> &ActivationReadyRevision {
        &self.activation
    }

    pub fn into_parts(self) -> (BlobRevisionIdentity, DesiredState, ActivationReadyRevision) {
        (self.identity, self.state, self.activation)
    }

    /// Consume the candidate and retain the evidence required to bind one
    /// namespace-owned secret request. The activation witness remains owned by
    /// the authority, so later serving code cannot separate the secret
    /// binding from the exact head fence that authenticated the candidate.
    pub fn into_secret_authority(self) -> Result<BlobSecretAuthority, BlobSecretAuthorityError> {
        let namespaces = FlatNamespaces::of(&self.state)?;
        let environment = EnvironmentId::parse(
            self.activation
                .active_revision()
                .environment()
                .as_str()
                .to_owned(),
        )?;
        Ok(BlobSecretAuthority {
            identity: self.identity,
            state: self.state,
            namespaces,
            environment,
            activation: self.activation,
        })
    }
}

/// Non-cloneable authority derived only from a checksum-validated,
/// head-fenced blob candidate.
///
/// This type carries no secret material and has no constructor from raw
/// environment, owner, reference, or digest values. It is the only production
/// input accepted by the namespace binding minting path.
pub struct BlobSecretAuthority {
    identity: BlobRevisionIdentity,
    state: DesiredState,
    namespaces: FlatNamespaces,
    environment: EnvironmentId,
    activation: ActivationReadyRevision,
}

impl BlobSecretAuthority {
    pub const fn identity(&self) -> BlobRevisionIdentity {
        self.identity
    }

    pub const fn state(&self) -> &DesiredState {
        &self.state
    }

    pub const fn namespaces(&self) -> &FlatNamespaces {
        &self.namespaces
    }

    pub const fn activation(&self) -> &ActivationReadyRevision {
        &self.activation
    }

    pub(crate) fn bind(
        &self,
        request: &NamespaceSecretRequest,
    ) -> Result<AuthenticatedSecretBinding, BlobSecretBindingError> {
        super::secret_binding::mint_from_blob_authority(self, request)
    }

    pub(crate) fn environment(&self) -> &EnvironmentId {
        &self.environment
    }

    pub(crate) fn indexed_request(
        &self,
        request: &NamespaceSecretRequest,
    ) -> Option<NamespaceSecretRequest> {
        self.namespaces
            .secret_request(request.owner(), request.reference())
    }
}

/// Why a namespace secret request cannot be bound to a blob candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BlobSecretBindingError {
    #[error("secret request is not declared by the authenticated deployment secret index")]
    Undeclared,
    #[error("secret request is not active ({lifecycle:?})")]
    Inactive { lifecycle: SecretLifecycle },
    #[error("secret request does not match the authenticated deployment secret index")]
    Mismatch,
}

/// Read-only source for the current authenticated blob revision.
pub struct BlobRevisionSource<S> {
    reader: BlobReader<S>,
    limits: BlobHydrationLimits,
}

impl<S: ObjectStore> BlobRevisionSource<S> {
    pub const fn new(reader: BlobReader<S>, limits: BlobHydrationLimits) -> Self {
        Self { reader, limits }
    }

    /// Hydrate the current head, returning `None` only for a genuinely empty
    /// environment below the process-local sequence floor.
    ///
    /// Every manifest object is consumed exactly once. Only namespace and
    /// deployment resource documents belong to this slice; secret ciphertext is
    /// refused until the secret-index integration can authenticate its meaning.
    pub async fn candidate(&self) -> Result<Option<BlobCandidate>, BlobRevisionError> {
        let Some(active) = self.reader.read_active_revision().await? else {
            return Ok(None);
        };
        let expected_checksum = active.desired_state_checksum();
        let identity = BlobRevisionIdentity {
            sequence: active.sequence(),
            digest: active.revision(),
        };
        let objects: Vec<_> = active.objects().collect();

        let mut state = DesiredState::new();
        let mut blobs = BTreeMap::<Checksum, BlobRef>::new();
        let mut resource_bytes = 0usize;
        for (kind, digest) in objects {
            if kind == ImmutableObjectKind::Secret {
                return Err(BlobRevisionError::UnexpectedObject { kind });
            }
            let object = self.reader.read_immutable_object(kind, digest).await?;
            resource_bytes = resource_bytes.saturating_add(object.bytes.len());
            if resource_bytes > self.limits.max_resource_bytes {
                return Err(BlobHydrationLimit::ResourceBytes {
                    observed: resource_bytes,
                    limit: self.limits.max_resource_bytes,
                }
                .into());
            }

            let resource = BlobResourceDocument::decode(&object)
                .map_err(|source| BlobRevisionError::ResourceDocument { digest, source })?;
            if let Some(blob) = resource.body.blob().copied()
                && blobs
                    .insert(blob.digest, blob)
                    .is_some_and(|held| held != blob)
            {
                return Err(BlobRevisionError::ConflictingBlobDeclaration {
                    digest: blob.digest,
                });
            }
            let reference = resource.reference;
            state.insert(resource).map_err(|error| match error {
                ValidationError::DuplicateResourceVersion { reference } => {
                    BlobRevisionError::DuplicateResource { reference }
                }
                other => BlobRevisionError::InvalidDesiredState(other),
            })?;
            debug_assert!(state.get(&reference).is_some());
        }

        let blob_bytes = blobs
            .values()
            .fold(0u64, |total, blob| total.saturating_add(blob.size_bytes));
        if blob_bytes > self.limits.max_blob_bytes {
            return Err(BlobHydrationLimit::BlobBytes {
                observed: blob_bytes,
                limit: self.limits.max_blob_bytes,
            }
            .into());
        }
        for blob in blobs.into_values() {
            state.declare_blob(blob);
        }

        state
            .validate()
            .map_err(BlobRevisionError::InvalidDesiredState)?;
        let state_bytes = state.canonical().to_canonical_bytes()?;
        if state_bytes.len() > self.limits.max_state_bytes {
            return Err(BlobHydrationLimit::StateBytes {
                observed: state_bytes.len(),
                limit: self.limits.max_state_bytes,
            }
            .into());
        }
        let actual_checksum = Checksum::of(&state_bytes);
        if actual_checksum != expected_checksum {
            return Err(BlobRevisionError::ChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }

        // Keep this as the final I/O before returning. A head race after the
        // initial authenticated snapshot therefore cannot escape as a candidate.
        let activation = self.reader.fence_for_activation(active).await?;
        Ok(Some(BlobCandidate {
            identity,
            state,
            activation,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use bytes::Bytes;

    use crate::backends::object_store::{
        InMemoryObjectStore, ObjectKey, ObjectStore, ObjectStoreError, ObjectStoreLimits,
        ObjectValue, ObjectVersion,
    };
    use crate::convergence::LastKnownGood;
    use crate::convergence::lkg::testing::{KEY as CACHE_KEY, cache_path};
    use crate::desired_state::fixtures;
    use crate::desired_state::publication::{
        EnvironmentId, HeadDocumentError, deployment_resource_key, environment_head_key,
        namespace_resource_key, secret_key,
    };
    use crate::desired_state::{
        BlobPublication, BlobPublicationRequest, ExpectedHead, IdempotencyHistoryLimit,
        IdempotencyKey, ImmutableObject, MutationId, MutationKind, PublicationActorBinding,
        PublicationAuthorization, PublicationGrantBinding, PublicationKeyId, PublicationSigner,
        PublicationTrustStore, ResourceScope, Uuid7,
    };

    use super::*;

    const TEST_SIGNING_KEY_PKCS8_BASE64: &str = "MFMCAQEwBQYDK2VwBCIEIOn86WlkmKxquZ/ElW4lZfyxCVYnoaMnF56WoS4ICpKVoSMDIQDViT8X5LpD1A7O4sdlRada5GwjyvH2eAJ+ZiyfboLSBQ==";

    fn object_store_limits() -> ObjectStoreLimits {
        ObjectStoreLimits::for_max_object_bytes(
            NonZeroUsize::new(2 * 1024 * 1024).expect("non-zero object limit"),
        )
    }

    fn environment() -> EnvironmentId {
        EnvironmentId::parse("blob-reader-test").expect("valid environment")
    }

    fn signer() -> Arc<PublicationSigner> {
        let pkcs8 = BASE64_STANDARD
            .decode(TEST_SIGNING_KEY_PKCS8_BASE64)
            .expect("fixed test signing key");
        Arc::new(
            PublicationSigner::from_ed25519_pkcs8(
                PublicationKeyId::parse("blob-reader-test-key").expect("valid key id"),
                &pkcs8,
            )
            .expect("valid test signer"),
        )
    }

    fn trust(signer: &PublicationSigner) -> PublicationTrustStore {
        PublicationTrustStore::new([signer.trusted_key()]).expect("test trust")
    }

    fn object_key(kind: ImmutableObjectKind, digest: Checksum) -> ObjectKey {
        match kind {
            ImmutableObjectKind::NamespaceResource => namespace_resource_key(digest),
            ImmutableObjectKind::DeploymentResource => deployment_resource_key(digest),
            ImmutableObjectKind::Secret => secret_key(digest),
        }
    }

    fn resource_objects(state: &DesiredState) -> Vec<ImmutableObject> {
        state
            .resources()
            .map(|resource| ImmutableObject {
                kind: if resource.scope == ResourceScope::Deployment {
                    ImmutableObjectKind::DeploymentResource
                } else {
                    ImmutableObjectKind::NamespaceResource
                },
                bytes: Bytes::from(
                    resource
                        .canonical()
                        .to_canonical_bytes()
                        .expect("canonical resource"),
                ),
            })
            .collect()
    }

    async fn publish_state<S: ObjectStore>(
        store: Arc<S>,
        state: &DesiredState,
        idempotency_key: &str,
    ) -> (PublicationTrustStore, super::super::PublicationOutcome) {
        let signer = signer();
        let trust = trust(&signer);
        let publisher = BlobPublication::new(
            store,
            environment(),
            IdempotencyHistoryLimit::new(NonZeroUsize::new(8).expect("non-zero history")),
            signer,
            trust.clone(),
            None,
        )
        .expect("trusted publisher");
        let outcome = publisher
            .publish(BlobPublicationRequest {
                expected: ExpectedHead::Empty,
                authorization: PublicationAuthorization::new(
                    PublicationActorBinding::of(b"blob-reader-test-actor"),
                    PublicationGrantBinding::of(b"blob-reader-test-grant"),
                    MutationId::new(Uuid7::from_parts(42, 0, 42).expect("valid mutation id")),
                    MutationKind::Create,
                ),
                idempotency_key: IdempotencyKey::parse(idempotency_key)
                    .expect("valid idempotency key"),
                desired_state_checksum: state.checksum().expect("canonical desired state"),
                objects: resource_objects(state),
            })
            .await
            .expect("state publication");
        (trust, outcome)
    }

    #[tokio::test]
    async fn reader_constructs_without_signer_and_hydrates_authenticated_state() {
        let store = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let state = fixtures::state();
        let (trust, outcome) = publish_state(Arc::clone(&store), &state, "valid-reader").await;

        // This construction boundary deliberately has no signer argument or
        // private signing material.
        let reader = BlobReader::new(Arc::clone(&store), environment(), trust);
        let source = BlobRevisionSource::new(reader, BlobHydrationLimits::default());
        let candidate = source
            .candidate()
            .await
            .expect("authenticated hydration")
            .expect("active candidate");

        assert_eq!(candidate.identity().sequence(), outcome.sequence);
        assert_eq!(candidate.identity().digest(), outcome.revision);
        assert_eq!(candidate.state(), &state);
        let (identity, hydrated, activation) = candidate.into_parts();
        assert_eq!(identity.digest(), outcome.revision);
        assert_eq!(hydrated, state);
        assert_eq!(activation.active_revision().revision(), outcome.revision);
    }

    #[tokio::test]
    async fn blob_candidate_cache_recovers_cold_start_state_and_sequence_floor() {
        let store = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let state = fixtures::state();
        let (trust, outcome) = publish_state(Arc::clone(&store), &state, "blob-cache-cold").await;
        let candidate = BlobRevisionSource::new(
            BlobReader::new(Arc::clone(&store), environment(), trust.clone()),
            BlobHydrationLimits::default(),
        )
        .candidate()
        .await
        .expect("authenticated candidate")
        .expect("published candidate");

        let cache = LastKnownGood::new(cache_path("blob-cache-cold"), CACHE_KEY)
            .expect("cache key is valid");
        cache
            .export_blob_candidate(&environment(), &candidate)
            .expect("blob candidate cache writes atomically");
        let restored = cache
            .load_blob_candidate(&environment())
            .expect("blob candidate cache authenticates")
            .expect("blob candidate cache exists");

        assert_eq!(restored.sequence(), outcome.sequence);
        assert_eq!(restored.identity().digest(), outcome.revision);
        assert_eq!(restored.state(), &state);

        let reader = BlobReader::new_with_observed_state(
            Arc::clone(&store),
            environment(),
            trust,
            Some(restored.observed_head_state()),
        )
        .expect("cache environment binds to the reader");
        let recovered = BlobRevisionSource::new(reader, BlobHydrationLimits::default())
            .candidate()
            .await
            .expect("cold-start candidate remains readable")
            .expect("active candidate");
        assert_eq!(recovered.identity().sequence(), outcome.sequence);
        assert_eq!(recovered.state(), &state);

        let _ = fs::remove_file(cache.blob_path());
    }

    #[tokio::test]
    async fn restored_blob_floor_fences_a_lower_authenticated_head() {
        let store = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let signer = signer();
        let trust = trust(&signer);
        let publisher = BlobPublication::new(
            Arc::clone(&store),
            environment(),
            IdempotencyHistoryLimit::new(NonZeroUsize::new(8).expect("non-zero history")),
            signer,
            trust.clone(),
            None,
        )
        .expect("trusted publisher");
        let first_state = fixtures::state();
        let first = publisher
            .publish(BlobPublicationRequest {
                expected: ExpectedHead::Empty,
                authorization: PublicationAuthorization::new(
                    PublicationActorBinding::of(b"blob-cache-floor-actor"),
                    PublicationGrantBinding::of(b"blob-cache-floor-grant"),
                    MutationId::new(Uuid7::from_parts(45, 0, 45).expect("valid mutation id")),
                    MutationKind::Create,
                ),
                idempotency_key: IdempotencyKey::parse("blob-cache-floor-first")
                    .expect("valid key"),
                desired_state_checksum: first_state.checksum().expect("state checksum"),
                objects: resource_objects(&first_state),
            })
            .await
            .expect("first publication");
        let first_head = store
            .get(&environment_head_key(&environment()))
            .await
            .expect("first head");
        let first_candidate = BlobRevisionSource::new(
            BlobReader::new(Arc::clone(&store), environment(), trust.clone()),
            BlobHydrationLimits::default(),
        )
        .candidate()
        .await
        .expect("first candidate hydrates")
        .expect("first candidate exists");

        let second_state = fixtures::state_with_renamed_alias();
        let second = publisher
            .publish(BlobPublicationRequest {
                expected: ExpectedHead::Revision(first.revision),
                authorization: PublicationAuthorization::new(
                    PublicationActorBinding::of(b"blob-cache-floor-actor"),
                    PublicationGrantBinding::of(b"blob-cache-floor-grant"),
                    MutationId::new(Uuid7::from_parts(46, 0, 46).expect("valid mutation id")),
                    MutationKind::Update,
                ),
                idempotency_key: IdempotencyKey::parse("blob-cache-floor-second")
                    .expect("valid key"),
                desired_state_checksum: second_state.checksum().expect("state checksum"),
                objects: resource_objects(&second_state),
            })
            .await
            .expect("second publication");
        let second_candidate = BlobRevisionSource::new(
            BlobReader::new(Arc::clone(&store), environment(), trust.clone()),
            BlobHydrationLimits::default(),
        )
        .candidate()
        .await
        .expect("second candidate hydrates")
        .expect("second candidate exists");
        assert_eq!(second_candidate.identity().sequence(), second.sequence);

        let cache = LastKnownGood::new(cache_path("blob-cache-floor"), CACHE_KEY)
            .expect("cache key is valid");
        cache
            .export_blob_candidate(&environment(), &second_candidate)
            .expect("second candidate cache writes");
        assert!(matches!(
            cache.export_blob_candidate(&environment(), &first_candidate),
            Err(
                crate::convergence::LastKnownGoodError::BlobSequenceRollback {
                    minimum: 2,
                    actual: 1,
                    ..
                }
            )
        ));

        let current_head = store
            .get(&environment_head_key(&environment()))
            .await
            .expect("second head");
        store
            .replace_if_version(
                &environment_head_key(&environment()),
                first_head.bytes,
                &current_head.version,
            )
            .await
            .expect("test restores an older signed head");

        let restored = cache
            .load_blob_candidate(&environment())
            .expect("floor cache authenticates")
            .expect("floor cache exists");
        let reader = BlobReader::new_with_observed_state(
            store,
            environment(),
            trust,
            Some(restored.observed_head_state()),
        )
        .expect("cache environment binds to the reader");
        assert!(matches!(
            reader.read_active_revision().await,
            Err(BlobPublicationError::Head(HeadDocumentError::Rollback {
                minimum: 2,
                actual: 1,
            }))
        ));

        let _ = fs::remove_file(cache.blob_path());
    }

    #[tokio::test]
    async fn blob_candidate_cache_refuses_corruption_and_wrong_environment() {
        let store = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let state = fixtures::state();
        let (trust, _) = publish_state(Arc::clone(&store), &state, "blob-cache-integrity").await;
        let candidate = BlobRevisionSource::new(
            BlobReader::new(store, environment(), trust),
            BlobHydrationLimits::default(),
        )
        .candidate()
        .await
        .expect("authenticated candidate")
        .expect("published candidate");
        let cache = LastKnownGood::new(cache_path("blob-cache-integrity"), CACHE_KEY)
            .expect("cache key is valid");
        cache
            .export_blob_candidate(&environment(), &candidate)
            .expect("blob candidate cache writes");

        let mut bytes = fs::read(cache.blob_path()).expect("blob cache exists");
        *bytes.last_mut().expect("blob cache is non-empty") ^= 1;
        fs::write(cache.blob_path(), bytes).expect("test corrupts the blob cache");
        assert!(matches!(
            cache.load_blob_candidate(&environment()),
            Err(crate::convergence::LastKnownGoodError::Signature { .. })
        ));

        // The corrupted record is not repaired or replaced by a later cache
        // operation; remove it before testing the authenticated environment
        // binding with a fresh record.
        let _ = fs::remove_file(cache.blob_path());
        cache
            .export_blob_candidate(&environment(), &candidate)
            .expect("a fresh signed candidate can be written");
        let other_environment =
            EnvironmentId::parse("other-blob-environment").expect("valid other environment");
        assert!(matches!(
            cache.load_blob_candidate(&other_environment),
            Err(crate::convergence::LastKnownGoodError::BlobEnvironmentMismatch { .. })
        ));
        let _ = fs::remove_file(cache.blob_path());
    }

    #[tokio::test]
    async fn candidate_mints_only_active_indexed_secret_bindings() {
        let store = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let state = fixtures::flat_namespace_state_with_active_credential();
        let (trust, outcome) = publish_state(Arc::clone(&store), &state, "secret-authority").await;
        let source = BlobRevisionSource::new(
            BlobReader::new(store, environment(), trust),
            BlobHydrationLimits::default(),
        );
        let authority = source
            .candidate()
            .await
            .expect("authenticated hydration")
            .expect("active candidate")
            .into_secret_authority()
            .expect("flat projection");

        assert_eq!(authority.identity().digest(), outcome.revision);
        assert_eq!(
            authority.activation().active_revision().revision(),
            outcome.revision
        );
        let namespace = crate::namespace::NamespaceId::parse("acme").expect("fixture namespace");
        let request = authority
            .namespaces()
            .secret_request(&namespace, fixtures::secret_ref(953))
            .expect("indexed fixture secret");
        let binding = authority.bind(&request).expect("active binding");
        assert_eq!(binding.environment().as_str(), environment().as_str());
        assert_eq!(binding.owner(), &namespace);
        assert_eq!(binding.reference(), &fixtures::secret_ref(953));
        assert_eq!(
            binding.ciphertext_digest(),
            Checksum::of(b"fixture-ciphertext")
        );

        let staged = request.with_lifecycle(SecretLifecycle::Staged);
        assert!(matches!(
            authority.bind(&staged),
            Err(BlobSecretBindingError::Inactive {
                lifecycle: SecretLifecycle::Staged
            })
        ));
    }

    #[tokio::test]
    async fn hydration_refuses_tampered_and_missing_immutable_objects() {
        let tampered_store = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let state = fixtures::state();
        let objects = resource_objects(&state);
        let target = &objects[0];
        let digest = target.digest();
        let key = object_key(target.kind, digest);
        let (trust, _) =
            publish_state(Arc::clone(&tampered_store), &state, "tampered-reader").await;
        let stored = tampered_store.get(&key).await.expect("published object");
        tampered_store
            .replace_if_version(&key, Bytes::from_static(b"tampered"), &stored.version)
            .await
            .expect("test-only object corruption");
        let source = BlobRevisionSource::new(
            BlobReader::new(Arc::clone(&tampered_store), environment(), trust),
            BlobHydrationLimits::default(),
        );
        assert!(matches!(
            source.candidate().await,
            Err(BlobRevisionError::Publication(
                BlobPublicationError::ImmutableDigestMismatch { expected, .. }
            )) if expected == digest
        ));

        let missing_inner = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let (trust, _) = publish_state(Arc::clone(&missing_inner), &state, "missing-reader").await;
        let missing = Arc::new(MissingObjectStore {
            inner: missing_inner,
            missing: key,
        });
        let source = BlobRevisionSource::new(
            BlobReader::new(missing, environment(), trust),
            BlobHydrationLimits::default(),
        );
        assert!(matches!(
            source.candidate().await,
            Err(BlobRevisionError::Publication(BlobPublicationError::Store(
                ObjectStoreError::NotFound { .. }
            )))
        ));
    }

    #[derive(Clone)]
    struct MissingObjectStore {
        inner: Arc<InMemoryObjectStore>,
        missing: ObjectKey,
    }

    #[async_trait]
    impl ObjectStore for MissingObjectStore {
        fn name(&self) -> &'static str {
            "missing-object-test-store"
        }

        fn limits(&self) -> ObjectStoreLimits {
            self.inner.limits()
        }

        async fn get(&self, key: &ObjectKey) -> Result<ObjectValue, ObjectStoreError> {
            if key == &self.missing {
                Err(ObjectStoreError::NotFound { key: key.clone() })
            } else {
                self.inner.get(key).await
            }
        }

        async fn put_if_absent(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
        ) -> Result<ObjectVersion, ObjectStoreError> {
            self.inner.put_if_absent(key, bytes).await
        }

        async fn replace_if_version(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
            expected: &ObjectVersion,
        ) -> Result<ObjectVersion, ObjectStoreError> {
            self.inner.replace_if_version(key, bytes, expected).await
        }
    }

    #[derive(Clone)]
    struct HeadRaceStore {
        inner: Arc<InMemoryObjectStore>,
        stale_head: ObjectValue,
        head_reads: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl ObjectStore for HeadRaceStore {
        fn name(&self) -> &'static str {
            "head-race-test-store"
        }

        fn limits(&self) -> ObjectStoreLimits {
            self.inner.limits()
        }

        async fn get(&self, key: &ObjectKey) -> Result<ObjectValue, ObjectStoreError> {
            if key == &environment_head_key(&environment())
                && self.head_reads.fetch_add(1, Ordering::SeqCst) < 2
            {
                Ok(self.stale_head.clone())
            } else {
                self.inner.get(key).await
            }
        }

        async fn put_if_absent(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
        ) -> Result<ObjectVersion, ObjectStoreError> {
            self.inner.put_if_absent(key, bytes).await
        }

        async fn replace_if_version(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
            expected: &ObjectVersion,
        ) -> Result<ObjectVersion, ObjectStoreError> {
            self.inner.replace_if_version(key, bytes, expected).await
        }
    }

    #[tokio::test]
    async fn final_activation_fence_refuses_a_head_race() {
        let inner = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let state = fixtures::state();
        let signer = signer();
        let trust = trust(&signer);
        let publisher = BlobPublication::new(
            Arc::clone(&inner),
            environment(),
            IdempotencyHistoryLimit::new(NonZeroUsize::new(8).expect("non-zero history")),
            signer,
            trust.clone(),
            None,
        )
        .expect("trusted publisher");
        let first = publisher
            .publish(BlobPublicationRequest {
                expected: ExpectedHead::Empty,
                authorization: PublicationAuthorization::new(
                    PublicationActorBinding::of(b"blob-reader-test-actor"),
                    PublicationGrantBinding::of(b"blob-reader-test-grant"),
                    MutationId::new(Uuid7::from_parts(43, 0, 43).expect("valid mutation id")),
                    MutationKind::Create,
                ),
                idempotency_key: IdempotencyKey::parse("head-race-first").expect("valid key"),
                desired_state_checksum: state.checksum().expect("state checksum"),
                objects: resource_objects(&state),
            })
            .await
            .expect("first publication");
        let stale_head = inner
            .get(&environment_head_key(&environment()))
            .await
            .expect("first head");
        publisher
            .publish(BlobPublicationRequest {
                expected: ExpectedHead::Revision(first.revision),
                authorization: PublicationAuthorization::new(
                    PublicationActorBinding::of(b"blob-reader-test-actor"),
                    PublicationGrantBinding::of(b"blob-reader-test-grant"),
                    MutationId::new(Uuid7::from_parts(44, 0, 44).expect("valid mutation id")),
                    MutationKind::Update,
                ),
                idempotency_key: IdempotencyKey::parse("head-race-second").expect("valid key"),
                desired_state_checksum: state.checksum().expect("state checksum"),
                objects: resource_objects(&state),
            })
            .await
            .expect("second publication");

        let racing_store = Arc::new(HeadRaceStore {
            inner,
            stale_head,
            head_reads: Arc::new(AtomicUsize::new(0)),
        });
        let source = BlobRevisionSource::new(
            BlobReader::new(racing_store, environment(), trust),
            BlobHydrationLimits::default(),
        );
        assert_eq!(
            source.candidate().await.expect_err("head race must fail"),
            BlobRevisionError::Publication(BlobPublicationError::ActiveHeadChanged)
        );
    }

    #[tokio::test]
    async fn hydration_enforces_resource_blob_and_state_limits() {
        let store = Arc::new(InMemoryObjectStore::new(object_store_limits()));
        let state = fixtures::state();
        let (trust, _) = publish_state(Arc::clone(&store), &state, "bounded-reader").await;

        let resource_error = BlobRevisionSource::new(
            BlobReader::new(Arc::clone(&store), environment(), trust.clone()),
            BlobHydrationLimits {
                max_resource_bytes: 0,
                ..BlobHydrationLimits::default()
            },
        )
        .candidate()
        .await
        .expect_err("resource bytes must be bounded");
        assert!(matches!(
            resource_error,
            BlobRevisionError::Limit(BlobHydrationLimit::ResourceBytes { limit: 0, .. })
        ));

        let blob_error = BlobRevisionSource::new(
            BlobReader::new(Arc::clone(&store), environment(), trust.clone()),
            BlobHydrationLimits {
                max_blob_bytes: 0,
                ..BlobHydrationLimits::default()
            },
        )
        .candidate()
        .await
        .expect_err("declared blob bytes must be bounded");
        assert!(matches!(
            blob_error,
            BlobRevisionError::Limit(BlobHydrationLimit::BlobBytes { limit: 0, .. })
        ));

        let state_error = BlobRevisionSource::new(
            BlobReader::new(store, environment(), trust),
            BlobHydrationLimits {
                max_state_bytes: 0,
                ..BlobHydrationLimits::default()
            },
        )
        .candidate()
        .await
        .expect_err("canonical state bytes must be bounded");
        assert!(matches!(
            state_error,
            BlobRevisionError::Limit(BlobHydrationLimit::StateBytes { limit: 0, .. })
        ));
    }
}
