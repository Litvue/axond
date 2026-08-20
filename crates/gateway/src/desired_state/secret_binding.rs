//! Opaque authority handed to namespace-native secret cryptography.
//!
//! The blob codec must not let a backend caller assert an environment, owner,
//! or exact reference directly. This independent crypto slice therefore exposes
//! opaque binding types with **no raw production constructor**. The blob
//! revision integration wires [`AuthenticatedSecretBinding`] to a verified
//! signed active revision plus its validated deployment secret-index entry,
//! while [`BlobSecretPublicationBinding`] remains reserved for a later
//! create-only publication path. Tests and fuzzing get explicit cfg-only
//! synthetic constructors.

use super::namespaces::NamespaceSecretRequest;
use super::{
    BlobSecretAuthority, BlobSecretBindingError, Checksum, EnvironmentId, SecretLifecycle,
    SecretRef,
};
use crate::namespace::NamespaceId;

/// Single-use authenticated context for opening one immutable secret object.
///
/// It has no production constructor, formatter, or `Clone`. Opening consumes
/// it. The ciphertext digest is checked before key selection, while environment,
/// owner, and exact reference form the material AEAD AAD. The only production
/// minting path is beside signed publication verification; this type
/// deliberately cannot assert that provenance on its own.
pub struct AuthenticatedSecretBinding {
    environment: EnvironmentId,
    owner: NamespaceId,
    reference: SecretRef,
    ciphertext_digest: Checksum,
}

impl AuthenticatedSecretBinding {
    pub(crate) fn environment(&self) -> &EnvironmentId {
        &self.environment
    }

    pub(crate) fn owner(&self) -> &NamespaceId {
        &self.owner
    }

    pub(crate) const fn reference(&self) -> &SecretRef {
        &self.reference
    }

    pub(crate) const fn ciphertext_digest(&self) -> Checksum {
        self.ciphertext_digest
    }

    #[cfg(any(test, fuzzing))]
    pub(crate) fn synthetic(
        environment: &EnvironmentId,
        owner: &NamespaceId,
        reference: SecretRef,
        ciphertext_digest: Checksum,
    ) -> Self {
        Self {
            environment: environment.clone(),
            owner: owner.clone(),
            reference,
            ciphertext_digest,
        }
    }
}

/// Mint an authenticated binding only from the witness-owned deployment index.
///
/// Keeping this constructor in the binding module means the private fields of
/// [`AuthenticatedSecretBinding`] never become available to the blob reader,
/// resolver, or a caller supplying raw identity values.
pub(crate) fn mint_from_blob_authority(
    authority: &BlobSecretAuthority,
    request: &NamespaceSecretRequest,
) -> Result<AuthenticatedSecretBinding, BlobSecretBindingError> {
    let Some(indexed) = authority.indexed_request(request) else {
        return Err(BlobSecretBindingError::Undeclared);
    };
    if indexed.owner() != request.owner()
        || indexed.reference() != request.reference()
        || indexed.ciphertext_digest() != request.ciphertext_digest()
    {
        return Err(BlobSecretBindingError::Mismatch);
    }
    if request.lifecycle() != SecretLifecycle::Active {
        return Err(BlobSecretBindingError::Inactive {
            lifecycle: request.lifecycle(),
        });
    }
    if indexed.lifecycle() != request.lifecycle() {
        return Err(BlobSecretBindingError::Mismatch);
    }
    Ok(AuthenticatedSecretBinding {
        environment: authority.environment().clone(),
        owner: request.owner().clone(),
        reference: request.reference(),
        ciphertext_digest: request.ciphertext_digest(),
    })
}

/// Single-use publisher authority to seal one create-only secret reference.
///
/// It is a different type from [`AuthenticatedSecretBinding`] and likewise has
/// no production constructor in this slice. A serving resolver receives neither
/// this value nor a `BlobSecretSealer`.
pub struct BlobSecretPublicationBinding {
    environment: EnvironmentId,
    owner: NamespaceId,
    reference: SecretRef,
}

impl BlobSecretPublicationBinding {
    pub(crate) fn environment(&self) -> &EnvironmentId {
        &self.environment
    }

    pub(crate) fn owner(&self) -> &NamespaceId {
        &self.owner
    }

    pub(crate) const fn reference(&self) -> &SecretRef {
        &self.reference
    }

    #[cfg(any(test, fuzzing))]
    pub(crate) fn synthetic(
        environment: &EnvironmentId,
        owner: &NamespaceId,
        reference: SecretRef,
    ) -> Self {
        Self {
            environment: environment.clone(),
            owner: owner.clone(),
            reference,
        }
    }
}
