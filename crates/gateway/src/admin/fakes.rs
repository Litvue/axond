//! Fake authorities and an instrumented store, for the contract tests.
//!
//! Deliberately minimal: the real implementations are an OIDC verifier and the
//! configured breakglass credential, and both land with the stateful runtime.
//! What the tests need from an authenticator is that *some* credential population
//! exists which is disjoint from the inference one, and that an identity-provider
//! outage is distinguishable from a rejection.
//!
//! [`CountingStore`] is the other half of the stateless argument: it wraps the
//! in-memory oracle and counts every call, so "stateless mode did not touch a
//! control-plane backend" is asserted against a number rather than inferred.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use secrecy::SecretString;

use super::auth::{
    AdminAction, AdminAuthError, AdminAuthenticator, AdminAuthorizer, AdminGrant, AdminIdentity,
    AdminPresented,
};
use crate::backends::Capabilities;
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::desired_state::oracle::InMemoryControlPlane;
use crate::desired_state::{
    AccessDenial, AuditEvent, DenialPage, LoadedRevision, ResourceScope, RevisionCandidate,
    RevisionId, RevisionManifest,
};

/// An authenticator over two hard-coded credential populations: OIDC-issued
/// human credentials, and one static breakglass secret.
pub(crate) struct FakeAdminAuthenticator {
    humans: Vec<(SecretString, String, String)>,
    breakglass: Option<(SecretString, String)>,
    unavailable: AtomicBool,
}

impl FakeAdminAuthenticator {
    pub(crate) fn new() -> Self {
        Self {
            humans: Vec::new(),
            breakglass: None,
            unavailable: AtomicBool::new(false),
        }
    }

    /// A human whose token `material` stands in for a verified OIDC assertion.
    pub(crate) fn with_human(mut self, material: &str, issuer: &str, subject: &str) -> Self {
        self.humans.push((
            SecretString::from(material.to_owned()),
            issuer.to_owned(),
            subject.to_owned(),
        ));
        self
    }

    pub(crate) fn with_breakglass(mut self, material: &str, label: &str) -> Self {
        self.breakglass = Some((SecretString::from(material.to_owned()), label.to_owned()));
        self
    }

    pub(crate) fn set_unavailable(&self, unavailable: bool) {
        self.unavailable.store(unavailable, Ordering::Relaxed);
    }
}

#[async_trait]
impl AdminAuthenticator for FakeAdminAuthenticator {
    fn name(&self) -> &'static str {
        "fake-admin-authenticator"
    }

    async fn authenticate(
        &self,
        presented: &AdminPresented,
    ) -> Result<AdminIdentity, AdminAuthError> {
        if let Some((secret, label)) = &self.breakglass
            && presented.credential.matches(secret)
        {
            // Breakglass works during an identity-provider outage: that is what
            // it is for, so the outage flag deliberately does not gate it.
            // Attribution becomes mandatory here, once the credential is known to
            // be the breakglass one, and not before.
            let attribution = presented.attribution.require()?;
            return Ok(AdminIdentity::Breakglass {
                attribution,
                credential: label.clone(),
            });
        }
        if self.unavailable.load(Ordering::Relaxed) {
            return Err(AdminAuthError::IdentityProviderUnavailable);
        }
        for (secret, issuer, subject) in &self.humans {
            if presented.credential.matches(secret) {
                return Ok(AdminIdentity::Human {
                    issuer: issuer.clone(),
                    subject: subject.clone(),
                });
            }
        }
        Err(AdminAuthError::UnknownCredential)
    }
}

/// An authorizer over an explicit set of actions and, optionally, an explicit set
/// of scopes.
pub(crate) struct FakeAdminAuthorizer {
    actions: BTreeSet<AdminAction>,
    scopes: Option<Vec<ResourceScope>>,
}

impl FakeAdminAuthorizer {
    /// Permits every action at every scope.
    pub(crate) fn permissive() -> Self {
        Self {
            actions: AdminAction::ALL.iter().copied().collect(),
            scopes: None,
        }
    }

    pub(crate) fn permitting(actions: &[AdminAction]) -> Self {
        Self {
            actions: actions.iter().copied().collect(),
            scopes: None,
        }
    }

    pub(crate) fn within(mut self, scopes: &[ResourceScope]) -> Self {
        self.scopes = Some(scopes.to_vec());
        self
    }
}

impl AdminAuthorizer for FakeAdminAuthorizer {
    fn name(&self) -> &'static str {
        "fake-admin-authorizer"
    }

    fn authorize(
        &self,
        identity: &AdminIdentity,
        action: AdminAction,
        scope: &ResourceScope,
    ) -> Result<AdminGrant, AdminAuthError> {
        if !self.actions.contains(&action) {
            return Err(AdminAuthError::ActionNotPermitted { action });
        }
        if let Some(scopes) = &self.scopes
            && !scopes.contains(scope)
        {
            return Err(AdminAuthError::ScopeNotPermitted);
        }
        Ok(AdminGrant::granted(identity.clone(), action, scope.clone()))
    }
}

/// The in-memory oracle, with manifest reads that start failing part-way
/// through a walk.
///
/// The history walk is the one read that makes several store calls to answer one
/// request, so it is the one place where a mid-read failure could be mistaken for
/// the end of the data. Failing the *n*-th manifest load is how that is provoked.
pub(crate) struct FlakyStore {
    inner: Arc<InMemoryControlPlane>,
    manifest_loads: AtomicUsize,
    fail_manifest_after: usize,
}

impl FlakyStore {
    pub(crate) fn failing_manifests_after(inner: Arc<InMemoryControlPlane>, loads: usize) -> Self {
        Self {
            inner,
            manifest_loads: AtomicUsize::new(0),
            fail_manifest_after: loads,
        }
    }
}

#[async_trait]
impl ControlPlaneStore for FlakyStore {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn health(&self) -> Result<(), ControlPlaneError> {
        self.inner.health().await
    }

    async fn desired_revision(&self) -> Result<Option<RevisionId>, ControlPlaneError> {
        self.inner.desired_revision().await
    }

    async fn load_manifest(&self, id: RevisionId) -> Result<RevisionManifest, ControlPlaneError> {
        if self.manifest_loads.fetch_add(1, Ordering::Relaxed) >= self.fail_manifest_after {
            return Err(ControlPlaneError::Unavailable {
                backend: self.inner.name(),
                message: "fake control plane went away mid-walk".to_owned(),
            });
        }
        self.inner.load_manifest(id).await
    }

    async fn load_revision(&self, id: RevisionId) -> Result<LoadedRevision, ControlPlaneError> {
        self.inner.load_revision(id).await
    }

    async fn publish_revision(
        &self,
        candidate: RevisionCandidate,
    ) -> Result<RevisionManifest, ControlPlaneError> {
        self.inner.publish_revision(candidate).await
    }

    async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError> {
        self.inner.audit_trail(id).await
    }

    async fn record_denial(&self, denial: &AccessDenial) -> Result<(), ControlPlaneError> {
        self.inner.record_denial(denial).await
    }

    async fn denials(
        &self,
        page: &DenialPage,
        limit: usize,
    ) -> Result<Vec<AccessDenial>, ControlPlaneError> {
        self.inner.denials(page, limit).await
    }
}

/// The in-memory oracle, plus a count of how many times it was consulted.
pub(crate) struct CountingStore {
    inner: Arc<InMemoryControlPlane>,
    calls: AtomicUsize,
}

impl CountingStore {
    pub(crate) fn new(inner: Arc<InMemoryControlPlane>) -> Self {
        Self {
            inner,
            calls: AtomicUsize::new(0),
        }
    }

    pub(crate) fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn count(&self) {
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl ControlPlaneStore for CountingStore {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn health(&self) -> Result<(), ControlPlaneError> {
        self.count();
        self.inner.health().await
    }

    async fn desired_revision(&self) -> Result<Option<RevisionId>, ControlPlaneError> {
        self.count();
        self.inner.desired_revision().await
    }

    async fn load_manifest(&self, id: RevisionId) -> Result<RevisionManifest, ControlPlaneError> {
        self.count();
        self.inner.load_manifest(id).await
    }

    async fn load_revision(&self, id: RevisionId) -> Result<LoadedRevision, ControlPlaneError> {
        self.count();
        self.inner.load_revision(id).await
    }

    async fn publish_revision(
        &self,
        candidate: RevisionCandidate,
    ) -> Result<RevisionManifest, ControlPlaneError> {
        self.count();
        self.inner.publish_revision(candidate).await
    }

    async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError> {
        self.count();
        self.inner.audit_trail(id).await
    }

    async fn record_denial(&self, denial: &AccessDenial) -> Result<(), ControlPlaneError> {
        self.count();
        self.inner.record_denial(denial).await
    }

    async fn denials(
        &self,
        page: &DenialPage,
        limit: usize,
    ) -> Result<Vec<AccessDenial>, ControlPlaneError> {
        self.count();
        self.inner.denials(page, limit).await
    }
}
