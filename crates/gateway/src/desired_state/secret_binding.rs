//! Opaque authority handed to namespace-native secret cryptography.
//!
//! The blob codec must not let a backend caller assert an environment, owner,
//! or exact reference directly. This independent crypto slice therefore exposes
//! opaque binding types with **no production constructor**. The integration
//! slice will wire [`AuthenticatedSecretBinding`] to a verified signed active
//! revision plus its validated deployment secret-index entry, and
//! [`BlobSecretPublicationBinding`] to a successful create-only reservation.
//! Until then, production code cannot fabricate either authority from raw
//! values. Tests and fuzzing get explicit cfg-only synthetic constructors.

use super::{Checksum, EnvironmentId, SecretRef};
use crate::namespace::NamespaceId;

/// Single-use authenticated context for opening one immutable secret object.
///
/// It has no production constructor, formatter, or `Clone`. Opening consumes
/// it. The ciphertext digest is checked before key selection, while environment,
/// owner, and exact reference form the material AEAD AAD. The integration slice
/// will add the only production minting path beside signed publication
/// verification; this crypto slice deliberately cannot assert that provenance.
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
