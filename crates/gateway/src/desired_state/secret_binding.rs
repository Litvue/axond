//! Opaque authority carried from signed publication hydration to secret open.
//!
//! The blob codec must not let a backend caller assert an environment, owner,
//! or exact reference directly. Publication hydration first verifies the signed
//! active revision, verifies that its deployment object is content-addressed by
//! that revision, and validates the deployment secret index. Only that path may
//! create [`VerifiedDeploymentSecretIndexProvenance`], which in turn is the only
//! production path that can mint an [`AuthenticatedSecretBinding`].

use super::{Checksum, EnvironmentId, SecretRef};
use crate::namespace::NamespaceId;

/// Evidence that an exact deployment secret-index entry came from a verified,
/// signed active revision.
///
/// This integration token is intentionally crate-private and not cloneable.
/// Its constructor is visible only inside `desired_state`, where publication
/// hydration and namespace projection meet. The independent publication and
/// projection slices should pass their already-verified environment and
/// content digests here rather than defining another authority wrapper.
#[allow(dead_code)] // Consumed when the publication/projection slices integrate.
pub(crate) struct VerifiedDeploymentSecretIndexProvenance<'a> {
    environment: &'a EnvironmentId,
    _revision_digest: Checksum,
    _deployment_digest: Checksum,
}

impl<'a> VerifiedDeploymentSecretIndexProvenance<'a> {
    /// Called only after signature, active-head fence, revision content address,
    /// deployment content address, and deployment secret-index validation.
    #[allow(dead_code)] // Integration constructor for the adjacent slices.
    pub(super) const fn from_verified_signed_index(
        environment: &'a EnvironmentId,
        revision_digest: Checksum,
        deployment_digest: Checksum,
    ) -> Self {
        Self {
            environment,
            _revision_digest: revision_digest,
            _deployment_digest: deployment_digest,
        }
    }

    /// Bind one already-validated index entry. Consuming provenance makes the
    /// authority handoff explicit at the point plaintext is requested.
    #[allow(dead_code)] // Integration method for the adjacent slices.
    pub(super) fn authenticate(
        self,
        owner: NamespaceId,
        reference: SecretRef,
        ciphertext_digest: Checksum,
    ) -> AuthenticatedSecretBinding {
        AuthenticatedSecretBinding {
            environment: self.environment.clone(),
            owner,
            reference,
            ciphertext_digest,
        }
    }
}

/// Single-use authenticated context for opening one immutable secret object.
///
/// It has no public or crate-wide constructor, no formatter, and no `Clone`.
/// Opening consumes it. The ciphertext digest is checked before key selection,
/// while environment, owner, and exact reference form the material AEAD AAD.
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
