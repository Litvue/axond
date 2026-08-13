//! Deriving an [`AvailabilityIndex`] from a published revision (#148).
//!
//! [`super`] defines what availability *means*; this module is the first thing
//! that produces one from facts a stateful deployment actually holds. It reads a
//! revision's catalogue pins, model enablements, provider connections,
//! credentials, and policy documents, folds in the material a candidate resolved
//! and this replica's own circuits, and files one record per
//! [`AvailabilityKey`].
//!
//! # Five authorities, read from five places
//!
//! | Dimension | Read from | Ignorant answer |
//! | --- | --- | --- |
//! | [`CataloguePresence`] | the catalogue listing an enablement pins, against the active one | `absent` |
//! | [`Enablement`] | [`ModelEnablementBody::state`] at the scope that owns it | `not_enabled` |
//! | [`Entitlement`] | the scope's provider connection, its credential, and whether that credential's exact material resolved | `missing` |
//! | [`PolicyDecision`] | the policy document governing the scope | `indeterminate` |
//! | [`RuntimeHealth`] | this replica's per-target circuit, overlaid when the question is asked | `unobserved` |
//!
//! The reason each has its own column is the property #148 exists for: a
//! deployment must be able to say *which* of them refused. Catalogue presence
//! alone never yields `available` — an offering the catalogue carries and nobody
//! enabled is `denied (not_enabled)`, one enabled with no credential is
//! `denied (entitlement_missing)`, and one with every authority permitting but no
//! discovery evidence is `unknown (no_evidence)`, because a listing is not proof
//! that a particular account can call a model.
//!
//! # Fail closed, in the one direction that is safe
//!
//! Every ignorant answer above is a refusal or an uncertainty, never a permit,
//! and the projection cannot invent a key: an enablement whose offering no
//! listing in hand carries produces *no record at all*, which
//! [`AvailabilityIndex::evaluate`] answers `unknown` with
//! [`DecidedBy::NoRecord`](super::DecidedBy::NoRecord) — a verdict that permits no attempt. Refusals like
//! that are counted ([`ProjectedAvailability::unnameable`]) rather than dropped
//! silently, because a projection that quietly stopped describing half a
//! catalogue would otherwise look identical to a tenant that enabled nothing.
//!
//! The same holds in the other direction, for a key an *earlier* revision
//! described and this one does not — a rollback that dropped an enablement, a
//! project that was deleted, a catalogue snapshot no longer in hand. Its record
//! keeps the evidence discovery paid for and loses every dimension, so it reads
//! `unavailable (catalogue_absent)` rather than the permit the previous revision
//! derived, and it is counted in [`ProjectedAvailability::undescribed`].
//!
//! # Four durable dimensions and one that is not
//!
//! The first four are facts of a *revision*, so they are derived once, when one
//! is projected. Runtime health is not: a circuit belongs to the replica and to
//! the snapshot it is serving, and a snapshot compiles with a breaker that has
//! attempted nothing. So a projected record carries
//! [`RuntimeHealth::Unobserved`] and health is overlaid at the instant a verdict
//! is asked for, through [`AvailabilityView`]. That keeps replica-local evidence
//! out of a value other replicas' verdicts would be read from, and it is the
//! only shape in which "this replica is skipping the target" and "the deployment
//! withdrew the target" stay distinguishable.
//!
//! # Evidence survives the projection
//!
//! A projection *re-derives the dimensions*; it does not re-derive evidence. It
//! starts from the previous index's evidence alone
//! ([`AvailabilityIndexBuilder::carrying_evidence`]), so
//! discovery evidence, the retained last-known-good look, and the definitive
//! watermark are carried across every publication — a revision that changes a
//! price does not reset what discovery established, and a discovery outage
//! during a rollout degrades to `available (last_known_good)` and then to
//! `stale`, exactly as [`super::index`] specifies. Stale positives are cleared
//! by later definitive conclusions there, not here.
//!
//! # What this does not do
//!
//! No provider is polled, no observation is persisted, and nothing here runs on
//! the request path: a projection is a pure function of a revision, a catalogue
//! listing, a resolved secret set, and a circuit snapshot, all already in hand.
//! Discovery adapters and their storage remain their own slice, and this module
//! only accepts the observations such a slice would produce.
//!
//! [`ModelEnablementBody::state`]: crate::desired_state::ModelEnablementBody::state

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::SystemTime;

use gateway_core::{CircuitState, FailoverTarget};

use crate::backends::catalog::CatalogContent;
use crate::convergence::ResolvedSecrets;
use crate::desired_state::credentials::{CredentialError, Credentials};
use crate::desired_state::models::{
    CatalogOffering, ModelEnablement, ModelError, ModelOwner, Models, OfferingId,
};
use crate::desired_state::policy::{PolicyError, PolicyScope, PolicySet};
use crate::desired_state::providers::{ProviderError, Providers};
use crate::desired_state::secrets::{SecretLifecycle, SecretRef};
use crate::desired_state::{Checksum, DesiredState};

use super::dimensions::{
    CataloguePresence, Enablement, Entitlement, PolicyDecision, RuntimeHealth,
};
use super::discovery::DiscoveryObservation;
use super::index::{AvailabilityIndex, AvailabilityIndexBuilder, AvailabilityRecord};
use super::refs::{AvailabilityKey, CredentialRef, ScopeRef, TargetRef};
use super::store::{self, EvidenceWrite, StoredObservation};
use super::verdict::Availability;

/// Why a revision could not be projected into availability at all.
///
/// Every arm is a body this build cannot read. They are the same refusals the
/// convergence pipeline already makes over the same bodies, surfaced here rather
/// than swallowed: an availability view derived from a revision this build only
/// half understands would be a confident answer about state nobody validated.
#[derive(Debug, thiserror::Error)]
pub enum AvailabilityProjectionError {
    #[error("the revision's model contracts could not be read: {0}")]
    Models(#[from] ModelError),
    #[error("the revision's provider connections could not be read: {0}")]
    Providers(#[from] ProviderError),
    #[error("the revision's credentials could not be read: {0}")]
    Credentials(#[from] CredentialError),
    #[error("the revision's policy documents could not be read: {0}")]
    Policy(#[from] PolicyError),
}

/// One catalogue snapshot, reduced to what availability needs: which offering
/// identities it carries, and what each one is called.
///
/// A listing rather than the catalogue itself. Availability names a target by a
/// bounded [`TargetRef`] token pair, and the reduction happens once, here, so no
/// verdict path ever re-parses provider vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogueListing {
    snapshot: Checksum,
    offerings: BTreeMap<OfferingId, TargetRef>,
    unnamed: usize,
}

impl CatalogueListing {
    /// Reduce `content`, as carried by the snapshot blob `snapshot` names.
    ///
    /// An offering whose provider or published id is not a valid availability
    /// token — over the length bound, or carrying a byte a log line must not
    /// take — is left out and counted in [`unnamed`](Self::unnamed) rather than
    /// failing the whole listing: one unprintable upstream name must not make a
    /// deployment blind to the rest of its catalogue.
    pub fn of(snapshot: Checksum, content: &CatalogContent) -> Self {
        let mut offerings = BTreeMap::new();
        let mut unnamed = 0;
        for model in content.models() {
            for offering in &model.offerings {
                let provider = offering.provider.as_str();
                let published = offering.published_model_id.as_str();
                let (Ok(identity), Ok(target)) = (
                    OfferingId::of(provider, published),
                    TargetRef::parse(provider, published),
                ) else {
                    unnamed += 1;
                    continue;
                };
                offerings.insert(identity, target);
            }
        }
        Self {
            snapshot,
            offerings,
            unnamed,
        }
    }

    /// The digest of the snapshot blob this listing was read from: the catalogue
    /// *version* an enablement pins against.
    pub const fn snapshot(&self) -> Checksum {
        self.snapshot
    }

    /// What this listing calls an offering, if it carries it.
    pub fn target(&self, offering: OfferingId) -> Option<&TargetRef> {
        self.offerings.get(&offering)
    }

    pub fn len(&self) -> usize {
        self.offerings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offerings.is_empty()
    }

    /// How many offerings this listing could not name.
    pub const fn unnamed(&self) -> usize {
        self.unnamed
    }
}

/// The catalogue a projection decides presence against: the listing in service,
/// and the superseded ones enablements may still be pinned to.
///
/// Both halves are needed, and for different questions. The *active* listing
/// answers presence — whether the deployment's current catalogue still carries
/// the offering. A *superseded* listing answers identity — what a target
/// published against an older catalogue is called, which is the only way a
/// withdrawal can be reported as
/// [`CataloguePresence::Withdrawn`] rather than as a target that silently stops
/// being described.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalogue {
    active: CatalogueListing,
    superseded: BTreeMap<Checksum, CatalogueListing>,
}

impl Catalogue {
    /// The catalogue currently in service.
    pub fn active(active: CatalogueListing) -> Self {
        Self {
            active,
            superseded: BTreeMap::new(),
        }
    }

    /// Keep an older snapshot for naming targets that were enabled against it.
    #[must_use]
    pub fn with_superseded(mut self, listing: CatalogueListing) -> Self {
        self.superseded.insert(listing.snapshot(), listing);
        self
    }

    /// What the catalogue says about a pinned offering: what it is called, and
    /// whether it is still carried.
    ///
    /// `None` when no listing in hand carries the identity at all — neither the
    /// active catalogue nor the snapshot the enablement pinned. Nothing can be
    /// said about a target that cannot be named, so nothing is: the caller counts
    /// the enablement as unnameable and files no record.
    fn presence(&self, pinned: CatalogOffering) -> Option<(TargetRef, CataloguePresence)> {
        if let Some(target) = self.active.target(pinned.offering) {
            return Some((target.clone(), CataloguePresence::Present));
        }
        let named = self
            .superseded
            .get(&pinned.snapshot)
            .and_then(|listing| listing.target(pinned.offering))?;
        // The catalogue carried it when the enablement was published and does not
        // now: a withdrawal, which is a different operator problem from a model
        // this deployment never imported.
        Some((named.clone(), CataloguePresence::Withdrawn))
    }

    /// Whether an enablement is pinned to the listing in service.
    fn is_current(&self, pinned: CatalogOffering) -> bool {
        pinned.is_pinned_to(self.active.snapshot())
    }
}

/// Which exact secret versions a candidate actually resolved.
///
/// References only — [`SecretRef`] is a version pointer, and nothing here holds
/// or can reach material. "This credential's material is in hand" is what
/// separates a credential a scope *has* from one it can *use*, and it is the
/// difference between [`Entitlement::Granted`] and [`Entitlement::Unknown`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CredentialReadiness {
    resolved: BTreeSet<SecretRef>,
}

impl CredentialReadiness {
    /// Nothing resolved: every credential's material is unproven.
    pub fn none() -> Self {
        Self::default()
    }

    /// The versions a compiled candidate holds.
    pub fn of(secrets: &ResolvedSecrets) -> Self {
        Self {
            resolved: secrets.references().into_iter().collect(),
        }
    }

    /// Declare one version resolved.
    #[must_use]
    pub fn holding(mut self, secret: SecretRef) -> Self {
        self.resolved.insert(secret);
        self
    }

    fn holds(&self, secret: SecretRef) -> bool {
        self.resolved.contains(&secret)
    }
}

/// This replica's own request outcomes, per target.
///
/// Replica-local, and derived from the per-target circuit breaker rather than
/// from anything durable: a bad afternoon on one replica lowers that replica's
/// verdicts and writes nothing back to a catalogue, an observation, or a
/// revision.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuntimeObservations {
    health: BTreeMap<String, RuntimeHealth>,
}

impl RuntimeObservations {
    /// No request has been made from this replica.
    pub fn none() -> Self {
        Self::default()
    }

    /// The circuits of a running snapshot, keyed as the request path keys them
    /// (`provider/model`).
    ///
    /// A closed circuit that exists is [`RuntimeHealth::Healthy`]: the breaker
    /// only holds a target it has attempted. A target it holds nothing for stays
    /// [`RuntimeHealth::Unobserved`], which is why this reads the snapshot of
    /// held circuits rather than asking the breaker per target — the breaker
    /// answers `closed` for a target nobody has ever called, and reporting that
    /// as health would turn silence into evidence.
    pub fn of_circuits(circuits: impl IntoIterator<Item = (String, CircuitState)>) -> Self {
        Self {
            health: circuits
                .into_iter()
                .map(|(target, state)| {
                    let health = match state {
                        CircuitState::Closed => RuntimeHealth::Healthy,
                        CircuitState::HalfOpen => RuntimeHealth::Impaired,
                        CircuitState::Open => RuntimeHealth::Unavailable,
                    };
                    (target, health)
                })
                .collect(),
        }
    }

    fn health(&self, target: &TargetRef) -> RuntimeHealth {
        self.health
            .get(&Self::circuit_key(target))
            .copied()
            .unwrap_or(RuntimeHealth::Unobserved)
    }

    /// The string the request path files this target's circuit under.
    ///
    /// Built with [`FailoverTarget::qualified_model`] — the same function
    /// `routes::target_key` uses — rather than by formatting the two components
    /// here, so the overlay cannot drift into looking health up under a spelling
    /// nothing writes. The two vocabularies meet because a projected record only
    /// exists where a connection's slug *is* the catalogue provider id; this pins
    /// the remaining half, and `a_targets_circuit_key_is_the_one_the_request_path_writes`
    /// fails the build if either side changes its mind.
    pub(crate) fn circuit_key(target: &TargetRef) -> String {
        FailoverTarget::new(target.provider.as_str(), target.model.as_str()).qualified_model()
    }
}

/// An index derived from a revision, with what the derivation could not describe.
///
/// The counters are part of the answer. A projection that names nothing is
/// indistinguishable from a deployment that enabled nothing unless it says so,
/// and every one of these is an operator-actionable discrepancy rather than a
/// statistic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedAvailability {
    index: AvailabilityIndex,
    unnameable: usize,
    undescribed: usize,
    skewed: usize,
    superseded: usize,
    misfiled: usize,
}

impl ProjectedAvailability {
    pub const fn index(&self) -> &AvailabilityIndex {
        &self.index
    }

    pub fn into_index(self) -> AvailabilityIndex {
        self.index
    }

    /// Enablements whose offering no listing in hand carries, and which therefore
    /// have no record. Non-zero means the deployment is serving a revision whose
    /// catalogue snapshots it has lost.
    pub const fn unnameable(&self) -> usize {
        self.unnameable
    }

    /// Keys the previous index held evidence for that this revision no longer
    /// describes. Their records survive for the evidence, under fail-closed
    /// dimensions: nothing a revision withdrew keeps reporting as permitted.
    pub const fn undescribed(&self) -> usize {
        self.undescribed
    }

    /// Enablements pinned to a superseded catalogue snapshot. Not a failure — an
    /// enablement is *meant* to survive a refresh — but it is what an operator
    /// looks at when a verdict and a catalogue page disagree.
    pub const fn skewed(&self) -> usize {
        self.skewed
    }

    /// Observations that arrived older than evidence already held.
    pub const fn superseded(&self) -> usize {
        self.superseded
    }

    /// Observations refused for naming a scope or target other than the record
    /// they were filed under. Always a projection bug when non-zero.
    pub const fn misfiled(&self) -> usize {
        self.misfiled
    }
}

/// The projection itself: a revision, a catalogue, and the material a candidate
/// resolved.
pub struct AvailabilityProjection<'a> {
    catalogue: &'a Catalogue,
    readiness: &'a CredentialReadiness,
}

impl<'a> AvailabilityProjection<'a> {
    pub const fn new(catalogue: &'a Catalogue, readiness: &'a CredentialReadiness) -> Self {
        Self {
            catalogue,
            readiness,
        }
    }

    /// Derive an index for `state`, keeping the evidence `previous` holds and
    /// folding in `observations`.
    ///
    /// The dimensions are re-derived from the revision every time; the evidence
    /// is not re-derived at all, because nothing in a revision is evidence about
    /// a provider. That split is what makes a publication cost freshness nothing:
    /// records keep their current look, their retained last-known-good one, and
    /// their definitive watermark across it.
    pub fn project(
        &self,
        state: &DesiredState,
        previous: &AvailabilityIndex,
        observations: impl IntoIterator<Item = DiscoveryObservation>,
    ) -> Result<ProjectedAvailability, AvailabilityProjectionError> {
        let models = Models::of(state)?;
        let providers = Providers::of(state)?;
        let credentials = Credentials::of(state)?;
        let policies = PolicySet::of(state)?;

        let mut builder = AvailabilityIndexBuilder::carrying_evidence(previous);
        let mut described = BTreeSet::new();
        let mut unnameable = 0;
        let mut skewed = 0;

        for enablement in models.enablements() {
            let pinned = enablement.body.offering();
            let Some((target, presence)) = self.catalogue.presence(pinned) else {
                unnameable += 1;
                continue;
            };
            if !self.catalogue.is_current(pinned) {
                skewed += 1;
            }
            let owner = enablement.body.owner();
            let scope = scope_of(owner);
            let (entitlement, credential) =
                self.entitlement(&providers, &credentials, owner, &target);
            let record = AvailabilityRecord {
                presence,
                enablement: enablement_of(enablement),
                entitlement,
                policy: policy_of(&policies, owner),
                credential,
                ..AvailabilityRecord::default()
            };
            let key = AvailabilityKey::new(scope, target);
            described.insert(key.clone());
            builder = builder.record(key, record);
        }

        for observation in observations {
            builder = builder.observe(observation);
        }

        // Keys the revision in hand does not describe kept their evidence and lost
        // their dimensions, so they read `unavailable` rather than the permit an
        // earlier revision derived. Counted, because a target that stops being
        // described is an operator-visible change — a rollback that dropped an
        // enablement, or a catalogue snapshot this deployment no longer holds.
        let undescribed = previous
            .records()
            .filter(|(key, record)| record.holds_evidence() && !described.contains(*key))
            .count();

        Ok(ProjectedAvailability {
            unnameable,
            undescribed,
            skewed,
            superseded: builder.superseded(),
            misfiled: builder.misfiled(),
            index: builder.build(),
        })
    }

    /// What the scope's own credential says about the target's provider.
    ///
    /// Three questions in order, because they fail differently: is there a
    /// connection to that provider the scope may use, does the scope hold a
    /// credential for it, and did that credential's exact material resolve.
    ///
    /// The best answer among the scope's credentials wins — one usable key
    /// entitles the scope however many revoked ones sit beside it — and the
    /// credential a decision was made against is named so an operator can
    /// correlate it. A reference, never material.
    fn entitlement(
        &self,
        providers: &Providers,
        credentials: &Credentials,
        owner: ModelOwner,
        target: &TargetRef,
    ) -> (Entitlement, Option<CredentialRef>) {
        // A connection is matched to the catalogue provider by its slug: the
        // connection is the deployment's own name for the upstream the catalogue
        // lists, and the slug is the only place the two vocabularies meet.
        let connections: BTreeSet<_> = providers
            .all()
            .filter(|provider| provider.slug.as_str() == target.provider.as_str())
            .filter(|provider| owner.reaches(owner_of_provider(provider)))
            .map(|provider| provider.body.provider())
            .collect();
        if connections.is_empty() {
            return (Entitlement::Missing, None);
        }

        let mut best: Option<(Entitlement, Option<CredentialRef>)> = None;
        for credential in credentials.all() {
            let body = &credential.body;
            if !connections.contains(&body.provider()) {
                continue;
            }
            let holder = ModelOwner {
                tenant: body.owner().tenant,
                project: body.owner().project,
            };
            if !owner.reaches(holder) {
                continue;
            }
            let entitlement = match body.lifecycle() {
                // In service, and its exact version is in hand: the only shape
                // that entitles anything.
                SecretLifecycle::Active if self.readiness.holds(body.secret()) => {
                    Entitlement::Granted
                }
                // In service but unresolved, or staged and not yet in service.
                // Neither is a grant and neither is a refusal: nothing has
                // established what this account may call.
                SecretLifecycle::Active | SecretLifecycle::Staged => Entitlement::Unknown,
                // Withheld or withdrawn. Both refuse; an operator repairs them
                // differently, and the credential reference is what says which
                // one to look at.
                SecretLifecycle::Disabled
                | SecretLifecycle::Revoked
                | SecretLifecycle::Tombstoned => Entitlement::Revoked,
            };
            let reference = CredentialRef::parse(credential.slug.as_str()).ok();
            best = Some(match best {
                Some(held) if rank(held.0) >= rank(entitlement) => held,
                _ => (entitlement, reference),
            });
        }
        best.unwrap_or((Entitlement::Missing, None))
    }
}

/// A replica's availability state across publications: the catalogue it decides
/// presence against, and the evidence it has accumulated.
///
/// The one mutable thing in this module, and deliberately *not* part of a
/// snapshot. A [`ConfigSnapshot`](crate::state::ConfigSnapshot) is immutable and
/// is replaced wholesale by every publication, so evidence held only there would
/// be lost by any revision — a price change would erase what discovery
/// established about a provider. Evidence is replica-local runtime state with a
/// lifetime of its own; a snapshot carries the *projection* of it that was true
/// when the snapshot compiled.
///
/// Discovery feeds [`observe`](Self::observe) from its own task, off the request
/// path. Compilation calls [`derive`](Self::derive), which folds the revision's
/// dimensions over the evidence already held and hands back an index to publish.
/// An outage of whatever feeds the observations changes nothing here: no
/// observation arrives, the previously retained evidence stays retained, and
/// verdicts age into `stale` on their own terms rather than a readiness probe
/// failing.
#[derive(Debug)]
pub struct AvailabilityEvidence {
    catalogue: Mutex<Arc<Catalogue>>,
    index: Mutex<Arc<AvailabilityIndex>>,
    pending: Mutex<Vec<DiscoveryObservation>>,
    /// What the last derivation was told, so a later look can be folded in
    /// without waiting for a revision that may never come.
    ///
    /// Kept here rather than reached for, because the alternative is worse than
    /// a clone per publication: the reconciler compiles only when desired state
    /// *changes*, so a steady-state deployment publishes nothing for hours, and a
    /// discovery loop with no way to re-derive would hold evidence no reader can
    /// see. Cloned off the request path, once per revision.
    derived_from: Mutex<Option<(Arc<DesiredState>, CredentialReadiness)>>,
    /// Held for the whole of a derivation, so that one is atomic with respect to
    /// any other.
    ///
    /// The queue and the index are separate values under separate locks, and a
    /// derivation reads the index, empties the queue, projects, and writes the
    /// index back. Two of them at once — compilation publishing a revision while
    /// the discovery loop re-projects a round of looks — could otherwise
    /// interleave so that the second wrote an index derived from a `previous`
    /// taken before the first, silently losing looks the first had already taken
    /// off the queue: evidence a replica paid a provider round trip for, gone
    /// from both the queue and every index. Nothing on the request path takes
    /// this.
    deriving: Mutex<()>,
}

impl AvailabilityEvidence {
    /// Start from a catalogue and no evidence at all.
    pub fn new(catalogue: Catalogue) -> Self {
        Self {
            catalogue: Mutex::new(Arc::new(catalogue)),
            index: Mutex::new(Arc::new(AvailabilityIndex::empty())),
            pending: Mutex::new(Vec::new()),
            derived_from: Mutex::new(None),
            deriving: Mutex::new(()),
        }
    }

    /// Replace the catalogue presence is decided against, as a catalogue import
    /// publishes a newer one. Evidence is untouched: what a provider listed is
    /// not invalidated by the deployment re-importing its price list.
    pub fn refresh(&self, catalogue: Catalogue) {
        *self.lock(&self.catalogue) = Arc::new(catalogue);
    }

    /// Record a discovery observation, to be folded into the next projection.
    ///
    /// Queued rather than applied: an index is immutable and a verdict is read
    /// from a published one, so evidence enters at the same seam a revision does
    /// — either the next [`derive`](Self::derive), or a [`reproject`](Self::reproject)
    /// the caller asks for once it has finished a round of looking.
    pub fn observe(&self, observation: DiscoveryObservation) {
        self.lock(&self.pending).push(observation);
    }

    /// The index published by the last derivation.
    pub fn index(&self) -> Arc<AvailabilityIndex> {
        Arc::clone(&self.lock(&self.index))
    }

    /// Fold stored evidence into what this replica holds, and report how many
    /// rows were refused as out of order.
    ///
    /// The boot half of [`persistable`](Self::persistable). Folded through the
    /// same declaration path a live observation takes, so a restart cannot
    /// believe something a running replica would have refused: a stored positive
    /// older than a conclusion the index has already reached is discredited, and
    /// a row naming another scope is refused rather than filed.
    pub fn restore(&self, rows: impl IntoIterator<Item = StoredObservation>) -> usize {
        let _deriving = self.lock(&self.deriving);
        let mut held = self.lock(&self.index);
        let mut builder = AvailabilityIndexBuilder::from_index(&held);
        for (key, record) in store::restored_records(rows) {
            builder = builder.record(key, record);
        }
        let refused = builder.superseded() + builder.misfiled();
        *held = Arc::new(builder.build());
        refused
    }

    /// The write that makes durable storage agree with the evidence this replica
    /// holds.
    ///
    /// Written by whatever owns discovery, off the request path. Carries no
    /// operator detail and no dimension a revision states, and names the keys it
    /// holds *no* evidence for as well as those it does — a look a definitive
    /// conclusion discredited must not outlive the process that discredited it.
    pub fn persistable(&self) -> EvidenceWrite {
        EvidenceWrite::of_index(&self.index())
    }

    /// Project `state` over the evidence held, publish the result, and return it.
    ///
    /// Serialised against every other derivation: the read of the index, the
    /// draining of the queue, and the write back are one step, so a concurrent
    /// caller cannot publish an index derived from evidence this one has already
    /// consumed.
    pub fn derive(
        &self,
        state: &DesiredState,
        readiness: &CredentialReadiness,
    ) -> Result<ProjectedAvailability, AvailabilityProjectionError> {
        let _deriving = self.lock(&self.deriving);
        let catalogue = Arc::clone(&self.lock(&self.catalogue));
        let previous = self.index();
        let pending: Vec<DiscoveryObservation> = self.lock(&self.pending).drain(..).collect();
        let projected = match AvailabilityProjection::new(&catalogue, readiness).project(
            state,
            &previous,
            pending.clone(),
        ) {
            Ok(projected) => projected,
            Err(error) => {
                // A refused projection applied nothing, so the looks are still the
                // newest evidence this replica holds and the next attempt needs them.
                // Ahead of anything queued since, so the queue stays in arrival order.
                let mut queued = self.lock(&self.pending);
                let since: Vec<DiscoveryObservation> = queued.drain(..).collect();
                queued.extend(pending);
                queued.extend(since);
                return Err(error);
            }
        };
        *self.lock(&self.index) = Arc::new(projected.index().clone());
        *self.lock(&self.derived_from) = Some((Arc::new(state.clone()), readiness.clone()));
        Ok(projected)
    }

    /// Fold whatever has been observed since into the revision already derived.
    ///
    /// The seam a discovery loop needs, and the reason it exists: convergence
    /// compiles only when desired state changes, so a deployment that publishes
    /// nothing for a day would otherwise keep every look taken that day queued
    /// and invisible. This applies them against the same revision the running
    /// index was derived from — no desired state is re-read, nothing is
    /// re-validated, and the caller publishes the returned index the same way
    /// compilation does.
    ///
    /// `None` before the first [`derive`](Self::derive): there is no revision to
    /// fold evidence into yet, and inventing one would mean answering with
    /// dimensions no revision stated. The looks stay queued for the derivation
    /// that does arrive.
    pub fn reproject(&self) -> Option<Result<ProjectedAvailability, AvailabilityProjectionError>> {
        let (state, readiness) = self.lock(&self.derived_from).clone()?;
        Some(self.derive(&state, &readiness))
    }

    /// A poisoned lock is recovered rather than propagated: the guarded values
    /// are whole values replaced under the lock, so a panic elsewhere cannot have
    /// left one half-written, and refusing to answer would turn someone else's
    /// panic into this replica's availability outage.
    fn lock<'a, T>(&self, guarded: &'a Mutex<T>) -> MutexGuard<'a, T> {
        guarded.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a reader needs to answer an availability question about *this* replica.
///
/// A seam rather than a direct reach into the running snapshot, so the
/// administrative surface depends on two small reads — the published index and
/// this replica's circuits — instead of on the whole of
/// [`AppState`](crate::state::AppState). Both are already in memory: answering
/// reaches no store, so the question is answerable during exactly the outages
/// that prompt it.
pub trait AvailabilityReader: Send + Sync {
    /// The index the snapshot this replica is serving carries and that same
    /// snapshot's circuits, or `None` when this replica derives no view at all.
    ///
    /// One call rather than two, because the two halves must describe the same
    /// instant: a publication landing between separate reads would pair one
    /// revision's targets with the next revision's breaker, which has attempted
    /// nothing and so reports every target [`RuntimeHealth::Unobserved`] — the
    /// overlay would silently vanish mid-incident.
    ///
    /// `None` rather than an empty index, and the distinction is the point: a
    /// replica that has derived nothing must not answer with an empty catalogue,
    /// which an operator reads as a tenant that has lost every entitlement.
    fn read(&self) -> Option<(Arc<AvailabilityIndex>, RuntimeObservations)>;
}

/// One replica's reading of a derived index: the index, and this replica's own
/// circuits.
///
/// Two things a projection deliberately keeps apart, joined only to answer a
/// question. The index is a value any replica could hold; the health is this
/// one's alone, and it is overlaid rather than stored so an operator asking two
/// replicas gets two honest answers rather than one stale one.
pub struct AvailabilityView<'a> {
    index: &'a AvailabilityIndex,
    runtime: &'a RuntimeObservations,
}

impl<'a> AvailabilityView<'a> {
    pub const fn new(index: &'a AvailabilityIndex, runtime: &'a RuntimeObservations) -> Self {
        Self { index, runtime }
    }

    /// The availability of one target in one scope, exactly as filed.
    pub fn evaluate(&self, key: &AvailabilityKey, now: SystemTime) -> Availability {
        self.index
            .evaluate_with(key, now, self.runtime.health(&key.target))
    }

    /// The availability of one target *inside a project*, falling back to the
    /// tenant default.
    ///
    /// The precedence a project override already has in desired state
    /// ([`Models::effective_for`]): a project's own enablement replaces its
    /// tenant's, including when it is a disabled one, so the fallback is only
    /// taken when the project has no record of its own. Nothing widens — the
    /// fallback reads the *tenant's* record, which is a record the project is
    /// entitled to inherit, and no sibling project's record is reachable from
    /// here.
    pub fn evaluate_effective(
        &self,
        scope: ScopeRef,
        target: &TargetRef,
        now: SystemTime,
    ) -> Availability {
        let own = AvailabilityKey::new(scope, target.clone());
        if scope.is_tenant_wide() || self.index.record(&own).is_some() {
            return self.evaluate(&own, now);
        }
        self.evaluate(
            &AvailabilityKey::new(ScopeRef::tenant(scope.tenant), target.clone()),
            now,
        )
    }

    /// Every target one scope may call, in target order: the ones filed under it
    /// and the ones it inherits.
    ///
    /// What an operator asking about a project means. A project is not a
    /// separate catalogue — it holds *overrides* of the tenant's enablements — so
    /// answering only from records filed under the project would report a
    /// project with no override of its own as a project that may call nothing.
    /// Each target is decided by [`evaluate_effective`](Self::evaluate_effective),
    /// so an override still replaces what it overrides, including a disabling
    /// one, and nothing outside the project's own tenant is reachable.
    pub fn evaluate_inherited_scope(
        &self,
        scope: ScopeRef,
        now: SystemTime,
    ) -> Vec<(TargetRef, Availability)> {
        if scope.is_tenant_wide() {
            return self.evaluate_scope(scope, now);
        }
        let inherited = ScopeRef::tenant(scope.tenant);
        let targets: BTreeSet<TargetRef> = self
            .index
            .evaluate_scope(&inherited, now)
            .into_iter()
            .chain(self.index.evaluate_scope(&scope, now))
            .map(|(target, _)| target)
            .collect();
        targets
            .into_iter()
            .map(|target| {
                let verdict = self.evaluate_effective(scope, &target, now);
                (target, verdict)
            })
            .collect()
    }

    /// Every target filed under one scope, in target order.
    pub fn evaluate_scope(
        &self,
        scope: ScopeRef,
        now: SystemTime,
    ) -> Vec<(TargetRef, Availability)> {
        self.index
            .evaluate_scope(&scope, now)
            .into_iter()
            .map(|(target, _)| {
                let verdict = self.evaluate(&AvailabilityKey::new(scope, target.clone()), now);
                (target, verdict)
            })
            .collect()
    }
}

/// How good an entitlement answer is, so the best of a scope's credentials
/// decides. A usable credential entitles the scope whatever else it holds, and
/// an unproven one is still better news than a revoked one.
const fn rank(entitlement: Entitlement) -> u8 {
    match entitlement {
        Entitlement::Granted => 3,
        Entitlement::Unknown => 2,
        Entitlement::Revoked => 1,
        Entitlement::Missing => 0,
    }
}

fn scope_of(owner: ModelOwner) -> ScopeRef {
    ScopeRef {
        tenant: owner.tenant,
        project: owner.project,
    }
}

fn owner_of_provider(provider: &crate::desired_state::providers::Provider) -> ModelOwner {
    ModelOwner {
        tenant: provider.body.tenant(),
        project: provider.body.project(),
    }
}

fn enablement_of(enablement: &ModelEnablement) -> Enablement {
    if enablement.body.state().is_enabled() {
        Enablement::Enabled
    } else {
        Enablement::NotEnabled
    }
}

/// What policy says about a scope.
///
/// A published document permits; the absence of one is
/// [`PolicyDecision::Indeterminate`], not a permit. A scope whose subject cap is
/// zero may spend nothing, which is a refusal an operator wrote down, so it is
/// reported as one rather than as a model that mysteriously produces no calls.
fn policy_of(policies: &PolicySet, owner: ModelOwner) -> PolicyDecision {
    let scope = match owner.project {
        None => PolicyScope::Tenant(owner.tenant),
        Some(project) => PolicyScope::Project {
            tenant: owner.tenant,
            project,
        },
    };
    match policies.effective(scope) {
        None => PolicyDecision::Indeterminate,
        Some(document) if document.body.budget().subject_limit_microdollars() == 0 => {
            PolicyDecision::Denied
        }
        Some(_) => PolicyDecision::Permitted,
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! A catalogue carrying one offering, for tests outside this module that
    //! need a projection to have something to name.

    use super::{Catalogue, CatalogueListing};
    use crate::backends::catalog::{
        CatalogContent, CatalogModelEntry, CatalogProvider, JsonPointer, ModelFacts, ModelId,
        ProviderEndpoint, ProviderId, ProviderOffering,
    };
    use crate::desired_state::Checksum;

    /// The listing `snapshot` describes: one provider, offering one model.
    pub(crate) fn listing(snapshot: Checksum, provider: &str, model: &str) -> CatalogueListing {
        let id = ProviderId::parse(provider).expect("a well-formed provider id");
        let content = CatalogContent::new(
            vec![CatalogProvider {
                id: id.clone(),
                display_name: None,
                doc_url: None,
                endpoint: ProviderEndpoint::default(),
                env_vars: Vec::new(),
                pointer: JsonPointer::new("").child("providers").child(provider),
            }],
            vec![CatalogModelEntry {
                id: ModelId::parse(model).expect("a well-formed model id"),
                neutral: None,
                offerings: vec![ProviderOffering {
                    provider: id,
                    model: ModelId::parse(model).expect("a well-formed model id"),
                    published_model_id: model.to_owned(),
                    facts: ModelFacts::default(),
                    overrides: Vec::new(),
                    price: None,
                    endpoint: ProviderEndpoint::default(),
                    pointer: JsonPointer::new("").child("models").child(model),
                }],
            }],
        )
        .expect("a catalogue with one offering");
        CatalogueListing::of(snapshot, &content)
    }

    pub(crate) fn catalogue(snapshot: Checksum, provider: &str, model: &str) -> Catalogue {
        Catalogue::active(listing(snapshot, provider, model))
    }
}
