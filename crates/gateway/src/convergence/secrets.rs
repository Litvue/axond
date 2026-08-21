//! Resolving a candidate's secret material, and holding it for exactly as long
//! as a published snapshot needs it.
//!
//! This is the runtime half of #145: the step between "a revision's credential
//! bodies pin exact secret versions" and "a snapshot holds the material those
//! versions name". It runs during candidate compilation, off the request path, and
//! it is the *only* place material enters the runtime — a request never reaches
//! the [`SecretStore`](crate::backends::secrets::SecretStore), so a store outage
//! cannot fail an inference call, and rotation cannot change what a request in
//! flight is authenticated by.
//!
//! Three rules, each of which the types make structural:
//!
//! - **All of it, or none of it.** [`SecretMaterialization::resolve`] returns
//!   either every version the candidate requires or a
//!   [`ProjectionError::Secret`], so a partially resolved candidate is not a value
//!   that exists. Compilation cannot publish, so the previous snapshot keeps
//!   serving whatever fails here.
//! - **A version is live while a snapshot holds it.** [`RetainedMaterial`] is an
//!   `Arc`, and every published snapshot holds one clone. Overlapping versions
//!   during a rotation are therefore overlapping `Arc`s, and the last one to drop
//!   — which is the last request holding the old snapshot, not the administrator
//!   who rotated — is what releases the material.
//! - **Release means zeroize.** Dropping the last reference drops the
//!   [`SecretString`](secrecy::SecretString) inside
//!   [`SecretMaterial`], which zeroizes
//!   its buffer, and deregisters the version from the [`MaterialLedger`]. The
//!   ledger is what makes that observable — to a test, and to the status endpoint
//!   — without anything having to expose the material to observe it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::backends::object_store::ObjectStore;
use crate::backends::secrets::blob_envelope::{
    BlobSecretResolver, BlobSecretResolverConstructionError, KekDecryptRing,
};
use crate::backends::secrets::{SecretError, SecretMaterial, SecretResolver};
use crate::desired_state::credentials::Credentials;
use crate::desired_state::namespaces::NamespaceSecretRequest;
use crate::desired_state::secrets::{SecretOwner, SecretRef};
use crate::desired_state::{
    BlobCandidate, BlobReader, BlobSecretAuthorityError, DesiredState, ResourceRef,
};

use super::compile::ProjectionError;

/// Which secret versions unwrapped material currently exists for in this process.
///
/// A registry, not an owner: it counts references and holds no material, so it
/// can be consulted (by a status endpoint, by a test asserting destruction) with
/// no way to read what it is counting.
///
/// The count is what makes zeroization checkable at the right moment. A rotation
/// leaves two versions of one secret registered while the old snapshot is still
/// serving requests, and the old version disappears from here when the last
/// request holding that snapshot finishes — never when the administrator's call
/// returns.
#[derive(Debug, Default)]
pub struct MaterialLedger {
    held: Mutex<BTreeMap<SecretRef, usize>>,
}

impl MaterialLedger {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register `material` under `reference`, returning the handle whose last
    /// clone releases it.
    fn retain(
        self: &Arc<Self>,
        reference: SecretRef,
        material: SecretMaterial,
        binding: ResolvedSecretBinding,
    ) -> RetainedMaterial {
        *self
            .held
            .lock()
            .expect("not poisoned")
            .entry(reference)
            .or_insert(0) += 1;
        RetainedMaterial(Arc::new(Retained {
            reference,
            material,
            binding,
            ledger: Arc::clone(self),
        }))
    }

    fn release(&self, reference: SecretRef) {
        let mut held = self.held.lock().expect("not poisoned");
        match held.get_mut(&reference) {
            Some(count) if *count > 1 => *count -= 1,
            _ => {
                held.remove(&reference);
            }
        }
    }

    /// Whether unwrapped material for this exact version exists anywhere in the
    /// process.
    pub fn holds(&self, reference: SecretRef) -> bool {
        self.held
            .lock()
            .expect("not poisoned")
            .contains_key(&reference)
    }

    /// Every version currently held, ordered. What an operator sees when asking
    /// "which material is this replica holding" — references only.
    pub fn retained(&self) -> Vec<SecretRef> {
        self.held
            .lock()
            .expect("not poisoned")
            .keys()
            .copied()
            .collect()
    }

    /// How many versions are held. Zero once no snapshot references any.
    pub fn len(&self) -> usize {
        self.held.lock().expect("not poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Unwrapped material a published snapshot holds.
///
/// Cloning shares one buffer rather than copying it, so N snapshots referencing
/// one version keep one copy of the plaintext, and the buffer is zeroized when
/// the last of them drops. There is no way to construct one without registering
/// it in a [`MaterialLedger`], which is what stops material from being held by
/// something nothing accounts for.
#[derive(Clone)]
pub struct RetainedMaterial(Arc<Retained>);

struct Retained {
    reference: SecretRef,
    material: SecretMaterial,
    binding: ResolvedSecretBinding,
    ledger: Arc<MaterialLedger>,
}

/// The authority metadata retained beside plaintext and copied into encrypted
/// compiled recovery state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResolvedSecretBinding {
    Legacy,
    Namespace(NamespaceSecretRequest),
}

/// Deregistration happens here rather than at any call site, so a snapshot that
/// is dropped by a panic unwinding — or by the reconciler replacing it — accounts
/// for its material identically.
impl Drop for Retained {
    fn drop(&mut self) {
        self.ledger.release(self.reference);
    }
}

impl RetainedMaterial {
    /// The version this material is.
    pub fn reference(&self) -> SecretRef {
        self.0.reference
    }

    /// The plaintext, for the one caller that has to have it: building the
    /// credential pool a provider call authenticates with.
    pub fn expose(&self) -> &str {
        self.0.material.expose()
    }

    /// How many handles share this material. The retention property, in a form a
    /// test can assert.
    pub fn holders(&self) -> usize {
        Arc::strong_count(&self.0)
    }

    pub(crate) fn binding(&self) -> &ResolvedSecretBinding {
        &self.0.binding
    }
}

/// Prints the reference, never the material — the same discipline
/// [`SecretMaterial`]'s own `Debug` keeps.
impl std::fmt::Debug for RetainedMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedMaterial")
            .field("reference", &self.0.reference)
            .field("holders", &self.holders())
            .finish_non_exhaustive()
    }
}

/// Every secret version a candidate revision requires, unwrapped.
///
/// A snapshot owns one of these, which is what ties material's lifetime to the
/// snapshot's: the material a revision was published against stays resolvable for
/// as long as anything can still be serving that revision, and is released
/// afterwards without anybody scheduling the release.
#[derive(Clone, Debug, Default)]
pub struct ResolvedSecrets {
    materials: HashMap<SecretRef, RetainedMaterial>,
}

impl ResolvedSecrets {
    /// Rebuild the retained set from an authenticated compiled-serving cache.
    ///
    /// The cache reader is the only caller that has this path: material is
    /// decrypted in memory, registered with the same ledger as ordinary
    /// compilation, and then held by the published snapshot with identical
    /// zeroization semantics.
    pub(crate) fn from_cached(
        ledger: Arc<MaterialLedger>,
        materials: impl IntoIterator<Item = (SecretRef, SecretMaterial, ResolvedSecretBinding)>,
    ) -> Result<Self, String> {
        let mut resolved = Self::default();
        for (reference, material, binding) in materials {
            if resolved.materials.contains_key(&reference) {
                return Err(format!(
                    "compiled cache declares secret {reference} more than once"
                ));
            }
            resolved
                .materials
                .insert(reference, ledger.retain(reference, material, binding));
        }
        Ok(resolved)
    }

    /// The material for an exact version, or `None` if this candidate did not
    /// require it. Not a resolution: nothing here reaches a store.
    pub fn get(&self, reference: SecretRef) -> Option<&RetainedMaterial> {
        self.materials.get(&reference)
    }

    pub fn len(&self) -> usize {
        self.materials.len()
    }

    pub fn is_empty(&self) -> bool {
        self.materials.is_empty()
    }

    /// The versions this set holds, ordered.
    pub fn references(&self) -> Vec<SecretRef> {
        let mut references: Vec<SecretRef> = self.materials.keys().copied().collect();
        references.sort_unstable();
        references
    }
}

/// The compilation step that unwraps a candidate's material.
///
/// Holds the store as a [`SecretResolver`] rather than a
/// [`SecretStore`](crate::backends::secrets::SecretStore) on purpose: compilation
/// resolves exact versions and must not be able to stage, rotate, or transition
/// anything. The component that holds plaintext is therefore not a component that
/// can change what a credential points at.
pub struct SecretMaterialization {
    resolver: Option<Arc<dyn SecretResolver>>,
    ledger: Arc<MaterialLedger>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BlobCandidateSecretError {
    #[error("the verified blob candidate could not establish a secret authority: {0}")]
    Authority(#[from] BlobSecretAuthorityError),
    #[error("the blob candidate secret resolver could not be constructed: {0}")]
    Resolver(#[from] BlobSecretResolverConstructionError),
}

impl SecretMaterialization {
    /// A materialization backed by a store.
    pub fn new(resolver: Arc<dyn SecretResolver>, ledger: Arc<MaterialLedger>) -> Self {
        Self {
            resolver: Some(resolver),
            ledger,
        }
    }

    /// A materialization with no store: a stateless process, which has no desired
    /// state to hold typed credentials in.
    ///
    /// A revision that requires material is refused rather than compiled without
    /// it, because compiling it would produce a snapshot whose credentials silently
    /// do not exist.
    pub fn stateless(ledger: Arc<MaterialLedger>) -> Self {
        Self {
            resolver: None,
            ledger,
        }
    }

    /// Build the materialization for one verified blob candidate.
    ///
    /// The candidate is consumed into its non-cloneable authority before the
    /// resolver is created. The reader is passed through unchanged, so it keeps
    /// its trust-only surface and the resolver can only mint requests from the
    /// candidate's authenticated deployment secret index.
    pub(crate) fn from_blob_candidate<S: ObjectStore + 'static>(
        candidate: BlobCandidate,
        reader: BlobReader<S>,
        ring: KekDecryptRing,
        ledger: Arc<MaterialLedger>,
    ) -> Result<Self, BlobCandidateSecretError> {
        let authority = candidate.into_secret_authority()?;
        let resolver = BlobSecretResolver::new(authority, reader, ring)?;
        Ok(Self::new(Arc::new(resolver), ledger))
    }

    /// The store's name, for diagnostics; `None` in a stateless process.
    pub fn backend(&self) -> Option<&'static str> {
        self.resolver.as_ref().map(|resolver| resolver.name())
    }

    /// The ledger this materialization registers material in.
    pub fn ledger(&self) -> &Arc<MaterialLedger> {
        &self.ledger
    }

    /// Unwrap every version this revision's credentials pin.
    ///
    /// Resolution is by exact reference and scoped by the owner the *revision*
    /// records, never by an owner a caller passes in, so a credential body cannot
    /// be published that resolves another tenant's material — the store refuses
    /// the mismatch, and the candidate is rejected.
    ///
    /// Versions are deduplicated: two credentials pinning one version resolve it
    /// once and share the buffer.
    pub async fn resolve(&self, state: &DesiredState) -> Result<ResolvedSecrets, ProjectionError> {
        if state.is_flat_namespace_v2() {
            return self.resolve_flat(state).await;
        }
        let credentials = Credentials::of(state).map_err(|error| ProjectionError::Body {
            reference: error.reference(),
            detail: error.to_string(),
        })?;
        let mut resolved = ResolvedSecrets::default();
        for credential in credentials.all() {
            if !credential.body.permits_resolution() {
                // Disabled, revoked, and tombstoned credentials are published
                // *and* not resolvable: withdrawing material must not stop the
                // revision that records the withdrawal from compiling.
                continue;
            }
            let reference = credential.body.secret();
            if resolved.materials.contains_key(&reference) {
                continue;
            }
            let material = self
                .unwrap_one(credential.body.owner(), reference, credential.reference)
                .await?;
            resolved.materials.insert(
                reference,
                self.ledger
                    .retain(reference, material, ResolvedSecretBinding::Legacy),
            );
        }
        Ok(resolved)
    }

    async fn resolve_flat(&self, state: &DesiredState) -> Result<ResolvedSecrets, ProjectionError> {
        let flat = crate::desired_state::FlatNamespaces::of(state).map_err(|error| {
            ProjectionError::Incomplete {
                detail: error.to_string(),
            }
        })?;
        let mut resolved = ResolvedSecrets::default();
        for (holder, namespace) in flat.namespaces() {
            for credential in namespace.credentials() {
                let reference = credential.secret;
                if resolved.materials.contains_key(&reference) {
                    continue;
                }
                let Some(resolver) = &self.resolver else {
                    return Err(secret_error(
                        *holder,
                        reference,
                        "this process has no deployment-scoped secret resolver configured"
                            .to_owned(),
                    ));
                };
                let request = flat
                    .secret_request(namespace.namespace(), reference)
                    .expect("validated flat credentials have one exact secret binding");
                let material = resolver
                    .resolve_namespace(&request)
                    .await
                    .map_err(|error| secret_error(*holder, reference, error.to_string()))?;
                resolved.materials.insert(
                    reference,
                    self.ledger.retain(
                        reference,
                        material,
                        ResolvedSecretBinding::Namespace(request),
                    ),
                );
            }
        }
        Ok(resolved)
    }

    async fn unwrap_one(
        &self,
        owner: SecretOwner,
        reference: SecretRef,
        holder: ResourceRef,
    ) -> Result<SecretMaterial, ProjectionError> {
        let Some(resolver) = &self.resolver else {
            return Err(secret_error(
                holder,
                reference,
                "this process has no secret store configured, so typed provider credentials \
                 cannot be resolved: a stateful deployment needs a `[secret_store]` section"
                    .to_owned(),
            ));
        };
        resolver
            .resolve(owner, &reference)
            .await
            // `SecretError`'s Display carries the reference, the owner, and the
            // lifecycle state, and never material — so the refusal an operator
            // reads is the store's own words.
            .map_err(|error: SecretError| secret_error(holder, reference, error.to_string()))
    }
}

/// A resolution failure as the compiler's refusal: the reference by name, the
/// credential that pinned it, and the store's reason.
fn secret_error(holder: ResourceRef, reference: SecretRef, detail: String) -> ProjectionError {
    ProjectionError::Secret {
        holder,
        reference: reference.to_string(),
        detail,
    }
}

/// The materialization the convergence tests compile through.
///
/// A resolver rather than a store, and one that answers for any reference,
/// because the pipeline tests are about publication and last-known-good
/// behaviour: they need a revision's credentials to resolve, not to characterise
/// a store. The tests that characterise resolution use a real
/// [`InMemorySecrets`](crate::backends::fakes::InMemorySecrets) with material
/// seeded under exact references.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use async_trait::async_trait;

    use crate::backends::{Capabilities, Capability};

    /// Material for every reference asked of it, and no way to change what is
    /// stored: a resolver, so it cannot stand in for a store by accident.
    pub(crate) struct AnyMaterial;

    pub(crate) const MATERIAL: &str = "sk-test-material";

    #[async_trait]
    impl SecretResolver for AnyMaterial {
        fn name(&self) -> &'static str {
            "any-material"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::new(&[Capability::EnvelopeEncryption])
        }

        async fn resolve(
            &self,
            _owner: SecretOwner,
            _reference: &SecretRef,
        ) -> Result<SecretMaterial, SecretError> {
            Ok(SecretMaterial::new(MATERIAL.to_owned()))
        }

        async fn exists(
            &self,
            _owner: SecretOwner,
            _reference: &SecretRef,
        ) -> Result<bool, SecretError> {
            Ok(true)
        }

        async fn resolve_namespace(
            &self,
            _request: &NamespaceSecretRequest,
        ) -> Result<SecretMaterial, SecretError> {
            Ok(SecretMaterial::new(MATERIAL.to_owned()))
        }

        async fn exists_namespace(
            &self,
            _request: &NamespaceSecretRequest,
        ) -> Result<bool, SecretError> {
            Ok(true)
        }
    }

    /// A materialization every reference resolves through, with a fresh ledger.
    pub(crate) fn permissive() -> Arc<SecretMaterialization> {
        Arc::new(SecretMaterialization::new(
            Arc::new(AnyMaterial),
            MaterialLedger::new(),
        ))
    }

    /// A materialization backed by a store that is down: what a candidate hits
    /// when material it would otherwise resolve is momentarily unreachable.
    pub(crate) fn unavailable() -> Arc<SecretMaterialization> {
        let store = crate::backends::fakes::InMemorySecrets::new();
        store.set_unavailable(true);
        Arc::new(SecretMaterialization::new(
            Arc::new(store),
            MaterialLedger::new(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::fakes::InMemorySecrets;
    use crate::desired_state::credentials::ProviderCredentialBody;
    use crate::desired_state::secrets::SecretLifecycle;
    use crate::desired_state::{Slug, fixtures};

    const MATERIAL: &str = "sk-live-fixture";

    /// A state holding one tenant and the given credential bodies.
    fn state_with(bodies: Vec<(ProviderCredentialBody, &str)>) -> DesiredState {
        let mut state = DesiredState::new();
        state.insert(fixtures::tenant(1, "acme")).expect("a tenant");
        for (body, slug) in bodies {
            state
                .insert(body.version(Slug::parse(slug).expect("fixture slug")))
                .expect("a credential");
        }
        state
    }

    fn body(seed: u64, lifecycle: SecretLifecycle) -> ProviderCredentialBody {
        let staged = fixtures::credential_body(&fixtures::tenant_id(1), seed, "primary");
        staged
            .transitioned(lifecycle)
            .expect("a permitted lifecycle for a fixture")
    }

    /// A store holding every version the state's credentials pin, in the state
    /// each body declares.
    fn store(state: &DesiredState) -> Arc<InMemorySecrets> {
        let store = Arc::new(InMemorySecrets::new());
        for credential in Credentials::of(state)
            .expect("readable fixture credentials")
            .all()
        {
            store.seed(
                credential.body.owner(),
                credential.body.secret(),
                MATERIAL,
                credential.body.lifecycle(),
            );
        }
        store
    }

    #[tokio::test]
    async fn every_required_version_is_resolved_once_and_registered() {
        let state = state_with(vec![
            (body(3, SecretLifecycle::Active), "primary"),
            (body(4, SecretLifecycle::Staged), "next"),
        ]);
        let ledger = MaterialLedger::new();
        let materialization = SecretMaterialization::new(store(&state), Arc::clone(&ledger));
        assert_eq!(materialization.backend(), Some("in-memory"));

        let resolved = materialization
            .resolve(&state)
            .await
            .expect("the fixture's material is stored");
        let mut expected: Vec<SecretRef> = Credentials::of(&state)
            .unwrap()
            .required_secrets()
            .map(|(_, reference)| reference)
            .collect();
        expected.sort_unstable();
        assert_eq!(expected.len(), 2, "staged material resolves too");
        assert_eq!(resolved.references(), expected);
        assert_eq!(ledger.retained(), expected);
        for reference in expected {
            assert_eq!(
                resolved.get(reference).expect("resolved").expose(),
                MATERIAL
            );
        }
    }

    /// Dropping the last holder is what zeroizes, and nothing had to schedule it.
    #[tokio::test]
    async fn material_is_released_when_the_last_holder_drops() {
        let state = state_with(vec![(body(3, SecretLifecycle::Active), "primary")]);
        let ledger = MaterialLedger::new();
        let resolved = SecretMaterialization::new(store(&state), Arc::clone(&ledger))
            .resolve(&state)
            .await
            .expect("resolution");
        let reference = resolved.references()[0];
        assert!(ledger.holds(reference));

        // A second holder — the shape a rotation has while the previous snapshot
        // is still serving requests — keeps the material alive.
        let second = resolved.clone();
        assert_eq!(resolved.get(reference).unwrap().holders(), 2);
        drop(resolved);
        assert!(ledger.holds(reference), "a holder remains");
        drop(second);
        assert!(ledger.is_empty(), "the last holder releases the material");
        assert!(!ledger.holds(reference));
    }

    /// A store outage rejects the candidate, and the refusal names the reference
    /// and the credential rather than anything about the material.
    #[tokio::test]
    async fn an_unavailable_store_refuses_the_candidate_without_disclosure() {
        let state = state_with(vec![(body(3, SecretLifecycle::Active), "primary")]);
        let store = store(&state);
        store.set_unavailable(true);
        let ledger = MaterialLedger::new();
        let error = SecretMaterialization::new(store, Arc::clone(&ledger))
            .resolve(&state)
            .await
            .expect_err("an unavailable store cannot resolve");

        assert!(matches!(error, ProjectionError::Secret { .. }));
        let rendered = error.to_string();
        assert!(rendered.contains("sct_"), "{rendered}");
        assert!(!rendered.contains(MATERIAL), "{rendered}");
        // Nothing was retained: a partially resolved candidate is not a value.
        assert!(ledger.is_empty());
    }

    /// Material a store does not have rejects the candidate for the same reason,
    /// and names the credential that pinned it.
    #[tokio::test]
    async fn missing_material_refuses_the_candidate() {
        let state = state_with(vec![(body(3, SecretLifecycle::Active), "primary")]);
        let ledger = MaterialLedger::new();
        let error =
            SecretMaterialization::new(Arc::new(InMemorySecrets::new()), Arc::clone(&ledger))
                .resolve(&state)
                .await
                .expect_err("nothing is stored");
        assert!(error.to_string().contains("is not stored"), "{error}");
        assert!(ledger.is_empty());
    }

    /// Withdrawn material does not block the revision that withdraws it: the
    /// credential is published as disabled and simply not resolved.
    #[tokio::test]
    async fn a_disabled_credential_is_published_without_material() {
        let disabled = body(3, SecretLifecycle::Disabled);
        let state = state_with(vec![
            (disabled.clone(), "primary"),
            (body(4, SecretLifecycle::Active), "next"),
        ]);
        let ledger = MaterialLedger::new();
        let resolved = SecretMaterialization::new(store(&state), Arc::clone(&ledger))
            .resolve(&state)
            .await
            .expect("a disabled credential needs no material");
        assert_eq!(resolved.len(), 1);
        assert!(resolved.get(disabled.secret()).is_none());
        assert!(!ledger.holds(disabled.secret()));
    }

    /// A stateless process has no store, so a revision that needs material is
    /// refused with the section an operator has to add.
    #[tokio::test]
    async fn a_process_with_no_store_refuses_a_revision_that_needs_material() {
        let state = state_with(vec![(body(3, SecretLifecycle::Active), "primary")]);
        let ledger = MaterialLedger::new();
        let materialization = SecretMaterialization::stateless(Arc::clone(&ledger));
        assert_eq!(materialization.backend(), None);
        let error = materialization
            .resolve(&state)
            .await
            .expect_err("no store, no material");
        assert!(error.to_string().contains("[secret_store]"), "{error}");
        assert!(ledger.is_empty());

        // A revision with no typed credentials compiles in a stateless process:
        // file and env references are untouched by any of this.
        let empty = SecretMaterialization::stateless(Arc::clone(&ledger))
            .resolve(&state_with(Vec::new()))
            .await
            .expect("nothing to resolve");
        assert!(empty.is_empty());
    }
}
