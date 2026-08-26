//! The tenant-scoped management catalogue: what one tenant may enable, what it
//! has enabled, which aliases name those enablements, and what still stands
//! between an enablement and a routable name.
//!
//! The read a tenant administrator needs before publishing anything. `/v1/models`
//! answers "what can this key invoke right now" from an immutable snapshot, and
//! deliberately answers nothing else: it does not say that an offering exists but
//! is disabled, that no compiled price covers it, or that no alias points
//! at it. Those are administrative questions about *desired state*, so they are
//! answered here, on the administrative surface, by an administrative credential.
//!
//! Three properties this projection holds to.
//!
//! - **It is one tenant's view.** The scope is a request parameter and the grant
//!   must cover it, so a tenant-scoped administrator reads its own tenant and a
//!   project-scoped one its own project. Nothing here iterates another tenant's
//!   enablements, and an entry's scope is projected so an operator can tell a
//!   tenant default from a project override rather than inferring it.
//! - **It never becomes a request-path read.** This is `/admin/v1`, reached by an
//!   administrative identity against the control plane. No inference route
//!   consults it, and adding one would be the thing ADR 0002 forbids: the runtime
//!   reads published snapshots.
//! - **It reports reasons and provenance.** A model is unroutable for reasons
//!   this revision states ([`UnavailableReason`]) — disabled, shadowed by a
//!   project override, unpriced, or unaliased. When the pinned catalogue and
//!   availability readers are attached, the projection also includes imported
//!   provider/model metadata, price-book identity, and a scoped availability
//!   verdict. If either reader is unavailable, [`CatalogueView::pending`] names
//!   the missing fact so a caller is not misled by its absence.
//!
//! # The integration seam
//!
//! An enablement names a [`CatalogOffering`](crate::desired_state::CatalogOffering):
//! an opaque [`OfferingId`] plus the
//! digest of the catalogue snapshot it was read from. Both are projected verbatim,
//! which is everything a caller needs to correlate an entry with a catalogue it
//! fetched — and is all this build can honestly say, because resolving a digest
//! into provider names, capabilities, modalities, and context windows requires the
//! pinned-catalogue lookup that #146 owns. The contract that slice must supply is
//! exactly one function:
//!
//! ```text
//! fn offering(&self, snapshot: Checksum, offering: OfferingId) -> Option<CatalogueOffering>
//! ```
//!
//! keyed by the same `snapshot` digest an enablement pins. When it exists, the
//! provider, capability, modality, and catalogue-lifecycle filters become
//! predicates over its result, and `pending` shrinks. Nothing else here changes:
//! the scope rule, the reason vocabulary, and the response shape are independent
//! of where offering metadata comes from.

use std::collections::BTreeMap;
use std::time::SystemTime;

use serde::Serialize;

use super::diff::ScopeView;
use super::error::AdminError;
use crate::availability::{AvailabilityReader, AvailabilityView, ScopeRef, TargetRef};
use crate::backends::catalog::{
    CatalogSnapshot, Modality, ModelCapability, ModelLifecycle as CatalogLifecycle, ProviderId,
};
use crate::backends::catalog_pins::{PinnedCatalog, Resolution};
use crate::backends::catalog_store::{self, CatalogStore};
use crate::desired_state::pricing::{EffectiveInstant, PriceBooks, PricingSnapshot};
use crate::desired_state::{
    LoadedRevision, ModelAlias, ModelEnablement, ModelError, ModelLifecycle, ModelOwner, Models,
    OfferingId, ProjectId, ResourceId, ResourceScope, TenantId, WireFamily,
};
use crate::status::StatusScope;

/// What a catalogue read asks for: one scope, and filters over it.
///
/// The scope is not optional. There is no deployment-wide catalogue read, because
/// "every tenant's enablements" is not a question a tenant administrator has and
/// answering it would make one grant's blast radius the whole deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogueRequest {
    pub tenant: TenantId,
    /// The project whose effective catalogue is wanted, or `None` for the tenant's
    /// own defaults.
    pub project: Option<ProjectId>,
    pub filters: CatalogueFilters,
}

/// The filters a catalogue read may apply, all of them predicates over state this
/// revision holds.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CatalogueFilters {
    /// Enabled or disabled — the operator's decision, not an upstream's.
    pub state: Option<ModelLifecycle>,
    pub wire_family: Option<WireFamily>,
    /// One offering, by identity. Exact rather than a substring search: the id is
    /// an opaque digest, so a prefix of one means nothing.
    pub offering: Option<OfferingId>,
    /// `Some(true)` for entries a compiled price covers, `Some(false)` for those
    /// it does not.
    pub billable: Option<bool>,
    /// Provider and model facts from the pinned imported catalogue.
    pub provider: Option<String>,
    pub capability: Option<ModelCapability>,
    pub modality: Option<Modality>,
    pub catalog_lifecycle: Option<CatalogLifecycle>,
    /// The derived availability state for this replica and scope.
    pub availability: Option<crate::availability::AvailabilityState>,
}

impl CatalogueRequest {
    /// The scope a grant must cover to read this.
    pub const fn scope(&self) -> ResourceScope {
        match self.project {
            None => ResourceScope::Tenant(self.tenant),
            Some(project) => ResourceScope::Project {
                tenant: self.tenant,
                project,
            },
        }
    }

    /// Whether `enablement` is in the scope this read is about.
    ///
    /// A project read sees the project's own overrides *and* its tenant's
    /// defaults, because the effective catalogue inside a project is both. A
    /// tenant read sees only the tenant's defaults: another project's overrides
    /// are that project's business, and a tenant-wide listing of them would be a
    /// larger answer than the question.
    fn covers(&self, owner: ModelOwner) -> bool {
        if owner.tenant != self.tenant {
            return false;
        }
        match self.project {
            None => owner.project.is_none(),
            Some(project) => owner.project.is_none() || owner.project == Some(project),
        }
    }

    fn admits(
        &self,
        entry: &ModelEnablement,
        metadata: Option<&CatalogueMetadata>,
        availability: Option<&super::reads::AvailabilityTarget>,
        billable: bool,
    ) -> bool {
        let filters = &self.filters;
        let durable = filters
            .state
            .is_none_or(|state| state == entry.body.state())
            && filters
                .wire_family
                .is_none_or(|family| family == entry.body.wire_family())
            && filters
                .offering
                .is_none_or(|offering| offering == entry.body.offering().offering)
            && filters.billable.is_none_or(|wanted| wanted == billable);
        durable
            && filters.provider.as_deref().is_none_or(|provider| {
                metadata.is_some_and(|metadata| metadata.provider == provider)
            })
            && filters.capability.is_none_or(|capability| {
                metadata.is_some_and(|metadata| {
                    metadata
                        .capabilities
                        .iter()
                        .any(|candidate| candidate == capability.as_str())
                })
            })
            && filters.modality.is_none_or(|modality| {
                metadata.is_some_and(|metadata| {
                    metadata
                        .input_modalities
                        .iter()
                        .any(|candidate| candidate == modality.as_str())
                        || metadata
                            .output_modalities
                            .iter()
                            .any(|candidate| candidate == modality.as_str())
                })
            })
            && filters.catalog_lifecycle.is_none_or(|lifecycle| {
                metadata.is_some_and(|metadata| metadata.catalog_lifecycle == lifecycle.as_str())
            })
            && filters.availability.is_none_or(|state| {
                availability.is_some_and(|availability| availability.state == state.as_str())
            })
    }
}

/// Why an enabled-looking model is still not routable.
///
/// A closed vocabulary, because a caller branches on it. Every reason is derived
/// from the revision being read, so none of them can disagree with what a
/// convergence of that revision would do.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnavailableReason {
    /// The operator has withdrawn this enablement. Reversible, and its history is
    /// intact.
    Disabled,
    /// A tenant default that a project override replaces inside the project being
    /// read. Reported rather than hidden: "why is the tenant default not in
    /// effect here" is the question, and the answer is another row in the same
    /// response.
    Shadowed,
    /// Enabled and in effect, but no compiled price covers the offering, so it
    /// could be routed but not billed. An observed catalogue rate is not an
    /// approval (ADR 0042).
    Unpriced,
    /// Nothing routes to it: no alias in this scope names this enablement, so no
    /// caller has a name to send.
    Unaliased,
}

impl UnavailableReason {
    pub const ALL: &'static [Self] = &[
        Self::Disabled,
        Self::Shadowed,
        Self::Unpriced,
        Self::Unaliased,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Shadowed => "shadowed",
            Self::Unpriced => "unpriced",
            Self::Unaliased => "unaliased",
        }
    }
}

impl Serialize for UnavailableReason {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// A fact this build cannot consult, named so its absence is not read as an
/// all-clear.
///
/// The honest half of the integration seam: an operator asking "is this model
/// healthy" must be able to tell "no" from "this release does not know".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PendingFact {
    /// Provider, family, capabilities, modalities, and context limits, which live
    /// in the pinned catalogue snapshot an enablement's digest names.
    OfferingMetadata,
    /// Observed reachability and upstream lifecycle, which live in the
    /// availability index projected beside a snapshot.
    Availability,
}

impl PendingFact {
    pub const ALL: &'static [Self] = &[Self::OfferingMetadata, Self::Availability];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OfferingMetadata => "offering-metadata",
            Self::Availability => "availability",
        }
    }
}

impl Serialize for PendingFact {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// Metadata resolved from the exact catalogue snapshot an enablement pins.
///
/// The provider/model strings here are catalogue identities, not operator
/// connection ids or caller-facing aliases. They are included only after the
/// pinned snapshot resolves the opaque offering id unambiguously.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogueMetadata {
    pub provider: String,
    pub model: String,
    pub published_model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub family: Option<String>,
    pub capabilities: Vec<String>,
    pub input_modalities: Vec<String>,
    pub output_modalities: Vec<String>,
    pub catalog_lifecycle: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
}

/// The durable pricing identity currently covering an offering, if any.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CataloguePrice {
    pub book: String,
    pub book_version: u64,
    pub catalog: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_version: Option<u64>,
    pub source: &'static str,
}

/// One enablement, as the management catalogue shows it.
///
/// Identities, scope, lifecycle, and the pinned catalogue coordinates — never a
/// price amount, and never anything derived from secret material. An amount is
/// billing state that the price resource owns; what belongs here is whether a
/// compiled price covers the offering, which is what decides routability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogueEntry {
    /// The opaque offering identity, as an enablement names it.
    pub offering: String,
    /// The digest of the catalogue snapshot the identity was read from.
    pub catalog_snapshot: String,
    pub enablement: String,
    pub version: u64,
    pub slug: String,
    pub scope: ScopeView,
    pub wire_family: &'static str,
    pub state: &'static str,
    /// Whether this row is the one in effect for the scope that was read. False
    /// for a tenant default a project override replaces.
    pub effective: bool,
    /// Whether a caller could invoke it right now: in effect, enabled, priced,
    /// and named by at least one alias.
    pub routable: bool,
    /// Whether a compiled price covers this offering.
    pub billable: bool,
    /// The alias names, in this scope, that resolve to this enablement.
    pub aliases: Vec<String>,
    /// Why it is not routable, in a stable order. Empty when it is.
    pub unavailable: Vec<UnavailableReason>,
    /// Operator warnings that do not make the offering unroutable.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub notices: Vec<CatalogueNotice>,
    /// The exact provider offering resolved from the pinned imported snapshot.
    /// `None` means the catalogue store was unavailable, the pin was withdrawn,
    /// or the snapshot published more than one callable id for this opaque id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<CatalogueMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<CataloguePrice>,
    /// Replica-local entitlement, policy, discovery, and runtime health. This
    /// is deliberately a verdict rather than raw provider detail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub availability: Option<super::reads::AvailabilityTarget>,
}

/// A model alias as the management catalogue shows it.
///
/// Alias names and concrete enablements are different resources. Keeping the
/// alias as a first-class projection means an administrator can reconcile an
/// alias without reverse-engineering the offering rows, and can preserve the
/// target priority the request path will eventually use. Targets remain
/// enablement references here; resolving them to provider-local model ids is a
/// separate pinned-catalogue concern and is intentionally still represented by
/// [`PendingFact::OfferingMetadata`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogueAlias {
    /// The durable alias resource identity.
    pub alias: String,
    pub version: u64,
    /// The caller-facing name of this alias in the owning project.
    pub slug: String,
    pub scope: ScopeView,
    pub wire_family: &'static str,
    pub state: &'static str,
    /// Whether at least one exact ordered target is enabled and covered by a
    /// compiled price.
    pub routable: bool,
    /// Why this alias cannot currently route. Empty when `routable` is true.
    pub unavailable: Vec<AliasUnavailableReason>,
    /// Ordered enablement references. The order is the failover priority and is
    /// therefore part of the response contract rather than a set.
    pub targets: Vec<CatalogueAliasTarget>,
}

/// One ordered target of a [`CatalogueAlias`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogueAliasTarget {
    pub enablement: String,
    pub version: u64,
}

/// Why an enabled-looking alias cannot currently reach a usable target.
///
/// These reasons are derived from the same desired-state model used to validate
/// publication. A disabled fallback may remain in the ordered target list, so a
/// single unusable target does not make the alias unavailable when a later target
/// is still effective, enabled, and billable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AliasUnavailableReason {
    /// The alias itself is withdrawn.
    Disabled,
    /// An enabled alias has no target entries. This is defensive because desired
    /// state validation rejects this shape, but keeping the response vocabulary
    /// total makes damaged or legacy projections explicit.
    NoTargets,
    /// A target exists but its enablement is withdrawn.
    DisabledTarget,
    /// A target is enabled but no compiled price covers it, so it cannot be billed.
    UnpricedTarget,
}

impl AliasUnavailableReason {
    pub const ALL: &'static [Self] = &[
        Self::Disabled,
        Self::NoTargets,
        Self::DisabledTarget,
        Self::UnpricedTarget,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::NoTargets => "no-targets",
            Self::DisabledTarget => "disabled-target",
            Self::UnpricedTarget => "unpriced-target",
        }
    }
}

impl Serialize for AliasUnavailableReason {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// Closed vocabulary of operator warnings that do not make the offering unroutable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CatalogueNotice {}

impl Serialize for CatalogueNotice {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        match *self {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AliasStatus {
    routable: bool,
    unavailable: Vec<AliasUnavailableReason>,
}

/// One tenant's management catalogue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogueView {
    /// `None` before the first publication: an empty control plane is not an
    /// error, and neither is a tenant with nothing enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub scope: ScopeView,
    pub entries: Vec<CatalogueEntry>,
    /// Aliases in the requested tenant/project scope, in deterministic resource
    /// order. A tenant-wide read includes aliases owned by that tenant's
    /// projects; a project read narrows this to that project.
    pub aliases: Vec<CatalogueAlias>,
    /// Facts this build could not consult. Always projected, including when
    /// empty, so a client can tell an old build from a complete answer.
    pub pending: Vec<PendingFact>,
}

impl CatalogueView {
    pub fn of(
        revision: Option<&LoadedRevision>,
        request: &CatalogueRequest,
    ) -> Result<Self, AdminError> {
        Self::build(revision, request, &BTreeMap::new(), true, true)
    }

    /// Build the management view with the retained catalogue and replica-local
    /// availability context attached. Both are read before this synchronous
    /// projection is built: the request still never reaches a source, a store,
    /// or an upstream provider while it is being served.
    pub async fn of_with_context(
        revision: Option<&LoadedRevision>,
        request: &CatalogueRequest,
        catalogue: Option<&dyn CatalogStore>,
        availability: Option<&dyn AvailabilityReader>,
        disclosure: StatusScope,
        now: SystemTime,
    ) -> Result<Self, AdminError> {
        let Some(revision) = revision else {
            return Ok(Self::empty(None, request));
        };
        let models = Models::of(revision.state()).map_err(|error| unreadable(revision, &error))?;
        let snapshots = retained_catalogues(&models, catalogue).await;
        let pricing = PriceBooks::of(revision.state()).ok().and_then(|books| {
            EffectiveInstant::of(now)
                .ok()
                .and_then(|at| books.snapshot_at(at))
        });
        let book = PriceBooks::of(revision.state())
            .ok()
            .and_then(|books| books.book().cloned());
        let availability = availability_context(availability);
        let mut contexts = BTreeMap::new();
        for enablement in models.enablements() {
            let metadata = snapshots
                .get(&enablement.body.offering().snapshot)
                .and_then(|snapshot| offering_metadata(snapshot, enablement));
            let availability = metadata.as_ref().and_then(|metadata| {
                entry_availability(availability.as_ref(), request, metadata, disclosure, now)
            });
            let price = metadata.as_ref().and_then(|metadata| {
                price_metadata(book.as_ref(), pricing.as_ref(), metadata, now)
            });
            let billable = metadata
                .as_ref()
                .is_some_and(|metadata| offering_is_billable(pricing.as_ref(), metadata));
            contexts.insert(
                enablement.reference.id,
                EntryContext {
                    metadata,
                    price,
                    availability,
                    billable,
                },
            );
        }
        let metadata_pending = models.enablements().any(|enablement| {
            contexts
                .get(&enablement.reference.id)
                .is_none_or(|context| context.metadata.is_none())
        });
        let availability_pending = availability.is_none();
        Self::build(
            Some(revision),
            request,
            &contexts,
            metadata_pending,
            availability_pending,
        )
    }

    fn build(
        revision: Option<&LoadedRevision>,
        request: &CatalogueRequest,
        contexts: &BTreeMap<ResourceId, EntryContext>,
        metadata_pending: bool,
        availability_pending: bool,
    ) -> Result<Self, AdminError> {
        let Some(revision) = revision else {
            return Ok(Self::empty(None, request));
        };
        // The same resolution publication and hydration use: a projection that
        // read these bodies its own way could report a catalogue the runtime would
        // never serve.
        let models = Models::of(revision.state()).map_err(|error| unreadable(revision, &error))?;
        let alias_statuses: BTreeMap<ResourceId, AliasStatus> = models
            .aliases()
            .map(|alias| (alias.reference.id, alias_status(&models, alias, contexts)))
            .collect();
        let mut entries = Vec::new();
        for enablement in models.enablements() {
            let owner = enablement.body.owner();
            let context = contexts.get(&enablement.reference.id);
            let billable = context.is_some_and(|context| context.billable);
            if !request.covers(owner)
                || !request.admits(
                    enablement,
                    context.and_then(|context| context.metadata.as_ref()),
                    context.and_then(|context| context.availability.as_ref()),
                    billable,
                )
            {
                continue;
            }
            let aliases = aliases_naming(&models, request, enablement);
            let shadowed = request.project.is_some_and(|project| {
                owner.project.is_none()
                    && models
                        .override_for(request.tenant, project, enablement.body.offering().offering)
                        .is_some()
            });
            let mut unavailable = Vec::new();
            if !enablement.body.is_enabled() {
                unavailable.push(UnavailableReason::Disabled);
            }
            if shadowed {
                unavailable.push(UnavailableReason::Shadowed);
            }
            if enablement.body.is_enabled() && !shadowed && !billable {
                unavailable.push(UnavailableReason::Unpriced);
            }
            if aliases.is_empty() {
                unavailable.push(UnavailableReason::Unaliased);
            }
            entries.push(CatalogueEntry {
                offering: enablement.body.offering().offering.to_string(),
                catalog_snapshot: enablement.body.offering().snapshot.to_string(),
                enablement: enablement.reference.id.to_string(),
                version: enablement.reference.version.get(),
                slug: enablement.slug.as_str().to_owned(),
                scope: ScopeView::of(&enablement.body.scope()),
                wire_family: enablement.body.wire_family().as_str(),
                state: enablement.body.state().as_str(),
                effective: !shadowed,
                routable: unavailable.is_empty(),
                billable,
                aliases,
                unavailable,
                notices: Vec::new(),
                metadata: context.and_then(|context| context.metadata.clone()),
                price: context.and_then(|context| context.price.clone()),
                availability: context.and_then(|context| context.availability.clone()),
            });
        }
        let aliases = models
            .aliases()
            .filter(|alias| in_scope(request, alias))
            .map(|alias| {
                // `in_scope` has already established that this alias belongs
                // to the requested tenant/project. Status uses the alias's
                // exact target references; scope does not rewrite those
                // references through project override precedence.
                let status = alias_statuses
                    .get(&alias.reference.id)
                    .expect("every alias has a derived status");
                CatalogueAlias {
                    alias: alias.reference.id.to_string(),
                    version: alias.reference.version.get(),
                    slug: alias.slug.as_str().to_owned(),
                    scope: ScopeView::of(&alias.body.scope()),
                    wire_family: alias.body.wire_family().as_str(),
                    state: alias.body.state().as_str(),
                    routable: status.routable,
                    unavailable: status.unavailable.clone(),
                    targets: alias
                        .body
                        .targets()
                        .iter()
                        .map(|target| CatalogueAliasTarget {
                            enablement: target.enablement.to_string(),
                            version: target.version.get(),
                        })
                        .collect(),
                }
            })
            .collect();
        Ok(Self {
            revision: Some(revision.id().to_string()),
            scope: ScopeView::of(&request.scope()),
            entries,
            aliases,
            pending: pending_facts(metadata_pending, availability_pending),
        })
    }

    fn empty(revision: Option<String>, request: &CatalogueRequest) -> Self {
        Self {
            revision,
            scope: ScopeView::of(&request.scope()),
            entries: Vec::new(),
            aliases: Vec::new(),
            pending: PendingFact::ALL.to_vec(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct EntryContext {
    metadata: Option<CatalogueMetadata>,
    price: Option<CataloguePrice>,
    availability: Option<super::reads::AvailabilityTarget>,
    billable: bool,
}

fn pending_facts(metadata: bool, availability: bool) -> Vec<PendingFact> {
    PendingFact::ALL
        .iter()
        .copied()
        .filter(|fact| match fact {
            PendingFact::OfferingMetadata => metadata,
            PendingFact::Availability => availability,
        })
        .collect()
}

async fn retained_catalogues(
    models: &Models,
    catalogue: Option<&dyn CatalogStore>,
) -> BTreeMap<crate::desired_state::Checksum, CatalogSnapshot> {
    let Some(catalogue) = catalogue else {
        return BTreeMap::new();
    };
    let mut snapshots = BTreeMap::new();
    for enablement in models.enablements() {
        let digest = enablement.body.offering().snapshot;
        if snapshots.contains_key(&digest) {
            continue;
        }
        let Ok(Some(retained)) = catalogue.retained_by_raw_digest(digest).await else {
            continue;
        };
        let Ok(snapshot) = catalog_store::hydrate(&retained) else {
            continue;
        };
        snapshots.insert(digest, snapshot);
    }
    snapshots
}

fn offering_metadata(
    snapshot: &CatalogSnapshot,
    enablement: &ModelEnablement,
) -> Option<CatalogueMetadata> {
    let pinned = PinnedCatalog::of_snapshot(snapshot).ok()?;
    let Resolution::Callable(callable) = pinned.resolve(enablement.body.offering()) else {
        return None;
    };
    let facts = callable.facts();
    Some(CatalogueMetadata {
        provider: callable.provider().as_str().to_owned(),
        model: callable.model().as_str().to_owned(),
        published_model: callable.published_model_id().to_owned(),
        display_name: facts.display_name.clone(),
        family: facts.family.clone(),
        capabilities: facts
            .capabilities
            .iter()
            .map(|capability| capability.as_str().to_owned())
            .collect(),
        input_modalities: facts
            .input_modalities
            .iter()
            .map(|modality| modality.as_str().to_owned())
            .collect(),
        output_modalities: facts
            .output_modalities
            .iter()
            .map(|modality| modality.as_str().to_owned())
            .collect(),
        catalog_lifecycle: facts.lifecycle.as_str(),
        context_tokens: facts.limits.context_tokens,
        input_tokens: facts.limits.input_tokens,
        output_tokens: facts.limits.output_tokens,
    })
}

fn offering_is_billable(pricing: Option<&PricingSnapshot>, metadata: &CatalogueMetadata) -> bool {
    ProviderId::parse(&metadata.provider)
        .ok()
        .and_then(|provider| pricing?.price(&provider, &metadata.published_model))
        .is_some()
}

fn price_metadata(
    book: Option<&crate::desired_state::pricing::PriceBook>,
    pricing: Option<&PricingSnapshot>,
    metadata: &CatalogueMetadata,
    at: SystemTime,
) -> Option<CataloguePrice> {
    let provider = ProviderId::parse(&metadata.provider).ok()?;
    let pricing = pricing?;
    pricing.price(&provider, &metadata.published_model)?;
    let book = book?;
    let instant = EffectiveInstant::of(at).ok()?;
    let source = book
        .body
        .rules()
        .iter()
        .filter(|rule| rule.effective().contains(instant))
        .filter(|rule| {
            rule.target().provider == provider
                && rule.target().published_model_id == metadata.published_model
        })
        .max_by_key(|rule| rule.precedence())
        .map(|rule| rule.provenance().origin.as_str())?;
    Some(CataloguePrice {
        book: book.reference.to_string(),
        book_version: book.reference.version.get(),
        catalog: pricing.catalog().to_string(),
        catalog_version: pricing.catalog_version().map(|version| version.get()),
        source,
    })
}

type AvailabilityContext = (
    std::sync::Arc<crate::availability::AvailabilityIndex>,
    crate::availability::RuntimeObservations,
);

fn availability_context(reader: Option<&dyn AvailabilityReader>) -> Option<AvailabilityContext> {
    reader.and_then(AvailabilityReader::read)
}

fn entry_availability(
    context: Option<&AvailabilityContext>,
    request: &CatalogueRequest,
    metadata: &CatalogueMetadata,
    disclosure: StatusScope,
    now: SystemTime,
) -> Option<super::reads::AvailabilityTarget> {
    let (index, runtime) = context?;
    let scope = ScopeRef::of(&request.scope())?;
    let target = TargetRef::parse(&metadata.provider, &metadata.published_model).ok()?;
    let verdict = AvailabilityView::new(index, runtime).evaluate_effective(scope, &target, now);
    super::reads::AvailabilityResult::of(&request.scope(), disclosure, vec![(target, verdict)])
        .targets
        .into_iter()
        .next()
}

/// The alias names in the read's scope that resolve to `enablement`, ordered and
/// deduplicated.
///
/// Only *enabled* aliases count, because a disabled alias is not a name a caller
/// can send — which is why a model whose only alias is disabled reports
/// [`UnavailableReason::Unaliased`] rather than looking routable.
fn aliases_naming(
    models: &Models,
    request: &CatalogueRequest,
    enablement: &ModelEnablement,
) -> Vec<String> {
    let mut names: Vec<String> = models
        .aliases()
        .filter(|alias| in_scope(request, alias) && alias.body.is_enabled())
        .filter(|alias| {
            alias
                .body
                .targets()
                .iter()
                .any(|target| target.enablement == enablement.reference.id)
        })
        .map(|alias| alias.slug.as_str().to_owned())
        .collect();
    names.sort_unstable();
    names.dedup();
    names
}

/// Derive whether one alias has any usable target in its owning project.
///
/// The alias target is an exact enablement reference. A project override does not
/// rewrite an alias that explicitly targets a tenant default; the alias contract
/// permits either target and keeps the ordered references intact. Effective
/// enablement precedence belongs to unaliased model resolution, not to alias
/// target interpretation.
fn alias_status(
    models: &Models,
    alias: &ModelAlias,
    contexts: &BTreeMap<ResourceId, EntryContext>,
) -> AliasStatus {
    if !alias.body.is_enabled() {
        return AliasStatus {
            routable: false,
            unavailable: vec![AliasUnavailableReason::Disabled],
        };
    }

    let mut unavailable = Vec::new();
    let mut routable = false;
    if alias.body.targets().is_empty() {
        unavailable.push(AliasUnavailableReason::NoTargets);
    }

    for target in alias.body.targets() {
        let Some(enablement) = models.enablement(target.enablement) else {
            unavailable.push(AliasUnavailableReason::NoTargets);
            continue;
        };
        if !enablement.body.is_enabled() {
            unavailable.push(AliasUnavailableReason::DisabledTarget);
            continue;
        }
        if !contexts
            .get(&enablement.reference.id)
            .is_some_and(|context| context.billable)
        {
            unavailable.push(AliasUnavailableReason::UnpricedTarget);
            continue;
        }
        routable = true;
        break;
    }

    if routable {
        unavailable.clear();
    }
    unavailable.sort_unstable();
    unavailable.dedup();
    AliasStatus {
        routable,
        unavailable,
    }
}

/// Whether an alias belongs to the scope being read. Aliases are project-scoped,
/// so a tenant-wide read reports every project's aliases for a default it owns,
/// and a project read reports only that project's.
fn in_scope(request: &CatalogueRequest, alias: &ModelAlias) -> bool {
    alias.body.tenant() == request.tenant
        && request
            .project
            .is_none_or(|project| alias.body.project() == project)
}

/// Published state that no longer resolves is an operator alert, not an outage:
/// the detail is logged with the error rather than returned, because a model
/// rejection interpolates identifiers from the state it refused.
fn unreadable(revision: &LoadedRevision, error: &ModelError) -> AdminError {
    AdminError::RevisionUnreadable {
        revision: Some(revision.id()),
        detail: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::catalog::{RawPayload, SourceValidators};
    use crate::backends::catalog_store::{InMemoryCatalogStore, RetainedCatalog};
    use crate::backends::models_dev::ModelsDevAdapter;
    use crate::desired_state::fixtures::{
        actor, alias_body, approved_price, blob_backed_catalog, candidate, catalog_reference,
        enablement_body, offering_id, price, price_rule, priced_target, project,
        project_enablement, project_id, reference, resource_id, revision_id, tenant, tenant_id,
        typed_alias,
    };
    use crate::desired_state::{
        Approval, BlobKind, BlobRef, CatalogOffering, DesiredState, EffectiveInstant,
        EffectiveInterval, ExpectedRevision, ModelEnablementBody, OfferingId, PriceBookBody,
        ProjectId, ResourceBody, ResourceKind, ResourceScope, ResourceVersion,
        ResourceVersionNumber, RevisionManifest, RulePrecedence, Slug, TenantId,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    const CATALOG: &str = include_str!("../backends/fixtures/models_dev/catalog.identity.json");

    /// A revision two tenants publish into: one has aliased, shadowed, and
    /// withdrawn enablements; the other has an enablement of the *same* offering,
    /// so an isolation assertion cannot pass by the offering id being distinct.
    fn published() -> LoadedRevision {
        let acme = tenant_id(1);
        let core = project_id(2);
        let globex = tenant_id(3);
        let their_core = project_id(4);
        let catalog = blob_backed_catalog(5);
        let rate = price(&acme, 6, "gpt-4o-rate");
        let default = enablement_body(30, ModelOwner::tenant(acme), "gpt-4o")
            .approving(approved_price(6))
            .version(slug("gpt-4o"), catalog_reference());
        let over = project_enablement(&acme, &core, 31, "gpt-4o");
        let withdrawn = enablement_body(32, ModelOwner::tenant(acme), "gpt-4o-mini")
            .transitioned(ModelLifecycle::Disabled)
            .version(slug("gpt-4o-mini"), catalog_reference());
        let alias = typed_alias(&acme, &core, 33, "fast", &[default.reference]);
        let theirs = enablement_body(34, ModelOwner::tenant(globex), "gpt-4o")
            .version(slug("gpt-4o"), catalog_reference());
        let their_alias = typed_alias(&globex, &their_core, 35, "fast", &[theirs.reference]);

        let mut state = DesiredState::new();
        state.declare_blob(*catalog.body.blob().expect("a blob body"));
        state
            .insert(tenant(1, "acme"))
            .and_then(|state| state.insert(project(&acme, 2, "core")))
            .and_then(|state| state.insert(tenant(3, "globex")))
            .and_then(|state| state.insert(project(&globex, 4, "core")))
            .and_then(|state| state.insert(catalog))
            .and_then(|state| state.insert(rate))
            .and_then(|state| state.insert(default))
            .and_then(|state| state.insert(over))
            .and_then(|state| state.insert(withdrawn))
            .and_then(|state| state.insert(alias))
            .and_then(|state| state.insert(theirs))
            .and_then(|state| state.insert(their_alias))
            .expect("the fixture state is publishable");
        loaded(state)
    }

    fn loaded(state: DesiredState) -> LoadedRevision {
        let candidate = candidate(ExpectedRevision::Empty, "catalogue", state);
        let manifest = RevisionManifest::of(
            revision_id(9),
            None,
            std::time::SystemTime::UNIX_EPOCH,
            &candidate,
        )
        .expect("the fixture state is publishable");
        LoadedRevision::assemble(manifest, candidate.state).expect("a consistent revision")
    }

    fn slug(slug: &str) -> Slug {
        Slug::parse(slug).expect("fixture slug")
    }

    fn read(tenant: TenantId, project: Option<ProjectId>) -> CatalogueView {
        filtered(tenant, project, CatalogueFilters::default())
    }

    fn filtered(
        tenant: TenantId,
        project: Option<ProjectId>,
        filters: CatalogueFilters,
    ) -> CatalogueView {
        CatalogueView::of(
            Some(&published()),
            &CatalogueRequest {
                tenant,
                project,
                filters,
            },
        )
        .expect("the fixture revision is readable")
    }

    fn entry<'a>(view: &'a CatalogueView, slug: &str) -> &'a CatalogueEntry {
        view.entries
            .iter()
            .find(|entry| entry.slug == slug)
            .unwrap_or_else(|| panic!("an entry named {slug}, in {:?}", view.entries))
    }

    /// The acceptance gate (IG-10) in one test: a tenant reads its own catalogue
    /// and nobody else's, and every row that is not routable says why.
    ///
    /// Isolation is asserted against a *sibling with the same offering*: the other
    /// tenant enables `gpt-4o` too, so a projection that leaked would leak a row
    /// that looks identical to one this tenant may see, and an assertion on the
    /// offering alone would not notice.
    #[test]
    fn a_tenant_read_is_isolated_and_explains_each_entry() {
        let view = read(tenant_id(1), None);

        assert_eq!(view.entries.len(), 2, "{:?}", view.entries);
        assert!(
            view.entries
                .iter()
                .all(|entry| entry.scope.tenant.as_deref() == Some(&tenant_id(1).to_string())),
            "another tenant's enablement reached a tenant-scoped read: {:?}",
            view.entries
        );

        let enabled = entry(&view, "gpt-4o");
        assert!(!enabled.routable);
        assert!(!enabled.billable);
        assert_eq!(enabled.aliases, vec!["fast".to_owned()]);
        assert_eq!(enabled.unavailable, vec![UnavailableReason::Unpriced]);

        // Withdrawn and unnamed: unpriced is only reported when the row is
        // enabled and in effect.
        let withdrawn = entry(&view, "gpt-4o-mini");
        assert!(!withdrawn.routable);
        assert_eq!(withdrawn.state, "disabled");
        assert_eq!(
            withdrawn.unavailable,
            vec![UnavailableReason::Disabled, UnavailableReason::Unaliased,]
        );
    }

    /// A tenant-wide read does not enumerate one project's overrides: they are
    /// that project's business, and the tenant's question is about its defaults.
    #[test]
    fn a_tenant_read_does_not_enumerate_a_projects_overrides() {
        let view = read(tenant_id(1), None);
        assert!(
            view.entries
                .iter()
                .all(|entry| entry.scope.kind == "tenant"),
            "{:?}",
            view.entries
        );
    }

    /// Inside a project, the effective catalogue is the override *and* the default
    /// it replaces: "why is the tenant default not in effect here" is answered by
    /// the row beside it rather than by its absence.
    #[test]
    fn a_project_read_reports_the_override_beside_the_default_it_shadows() {
        let view = read(tenant_id(1), Some(project_id(2)));

        let shadowed = view
            .entries
            .iter()
            .find(|entry| entry.scope.kind == "tenant" && entry.slug == "gpt-4o")
            .expect("the tenant default");
        assert!(!shadowed.effective);
        assert!(!shadowed.routable);
        assert_eq!(shadowed.unavailable, vec![UnavailableReason::Shadowed]);

        let over = view
            .entries
            .iter()
            .find(|entry| entry.scope.kind == "project")
            .expect("the project override");
        assert!(over.effective);
        assert!(!over.billable);
        // An override is a fresh administrative decision: it inherits neither the
        // default's approved price nor the alias that named the default.
        assert_eq!(
            over.unavailable,
            vec![UnavailableReason::Unpriced, UnavailableReason::Unaliased]
        );
    }

    /// Another tenant's project cannot be read through a tenant it does own: the
    /// scope pair is validated as a pair.
    #[test]
    fn a_project_of_another_tenant_yields_nothing() {
        let view = read(tenant_id(1), Some(project_id(4)));
        assert!(
            view.entries
                .iter()
                .all(|entry| entry.scope.kind == "tenant"),
            "{:?}",
            view.entries
        );
    }

    #[test]
    fn a_read_filters_by_lifecycle_offering_and_billability() {
        let enabled = filtered(
            tenant_id(1),
            None,
            CatalogueFilters {
                state: Some(ModelLifecycle::Enabled),
                ..CatalogueFilters::default()
            },
        );
        assert_eq!(
            enabled
                .entries
                .iter()
                .map(|entry| entry.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["gpt-4o"]
        );

        let unbillable = filtered(
            tenant_id(1),
            None,
            CatalogueFilters {
                billable: Some(false),
                ..CatalogueFilters::default()
            },
        );
        assert_eq!(
            unbillable
                .entries
                .iter()
                .map(|entry| entry.slug.as_str())
                .collect::<Vec<_>>(),
            vec!["gpt-4o", "gpt-4o-mini"]
        );

        let one = filtered(
            tenant_id(1),
            None,
            CatalogueFilters {
                offering: Some(offering_id("gpt-4o")),
                ..CatalogueFilters::default()
            },
        );
        assert_eq!(one.entries.len(), 1);
        assert_eq!(one.entries[0].slug, "gpt-4o");
        assert_eq!(
            one.entries[0].wire_family,
            WireFamily::OpenaiChat.as_str(),
            "an entry carries the wire family its targets are held to"
        );
    }

    #[test]
    fn a_read_projects_aliases_as_first_class_ordered_resources() {
        let view = read(tenant_id(1), None);
        assert_eq!(view.aliases.len(), 1);

        let alias = &view.aliases[0];
        assert_eq!(
            alias.alias,
            crate::desired_state::fixtures::resource_id(33).to_string()
        );
        assert_eq!(alias.version, 1);
        assert_eq!(alias.slug, "fast");
        assert_eq!(alias.scope.kind, "project");
        assert_eq!(
            alias.scope.tenant.as_deref(),
            Some(tenant_id(1).to_string().as_str())
        );
        assert_eq!(
            alias.scope.project.as_deref(),
            Some(project_id(2).to_string().as_str())
        );
        assert_eq!(alias.wire_family, WireFamily::OpenaiChat.as_str());
        assert_eq!(alias.state, ModelLifecycle::Enabled.as_str());
        assert!(!alias.routable);
        assert_eq!(
            alias.unavailable,
            vec![AliasUnavailableReason::UnpricedTarget]
        );
        assert_eq!(
            alias.targets,
            vec![CatalogueAliasTarget {
                enablement: crate::desired_state::fixtures::resource_id(30).to_string(),
                version: 1,
            }]
        );
    }

    #[test]
    fn an_alias_explains_when_every_target_is_unusable() {
        let revision = published();
        let models = Models::of(revision.state()).expect("the fixture models are valid");
        let acme = tenant_id(1);
        let core = project_id(2);

        let compiled = BTreeMap::from([(
            crate::desired_state::fixtures::resource_id(30),
            EntryContext {
                billable: true,
                ..EntryContext::default()
            },
        )]);
        let exact_default = models.aliases().next().expect("the fixture alias");
        assert_eq!(
            alias_status(&models, exact_default, &compiled),
            AliasStatus {
                routable: true,
                unavailable: Vec::new(),
            },
            "an alias keeps its exact tenant-default target even when the project has an override"
        );

        let unpriced = ModelAlias {
            reference: reference(crate::desired_state::ResourceKind::Alias, 37),
            slug: slug("unpriced"),
            body: alias_body(
                &acme,
                &core,
                37,
                &[reference(
                    crate::desired_state::ResourceKind::ModelEnablement,
                    31,
                )],
            ),
        };
        assert_eq!(
            alias_status(&models, &unpriced, &compiled).unavailable,
            vec![AliasUnavailableReason::UnpricedTarget]
        );

        let withdrawn = ModelAlias {
            reference: reference(crate::desired_state::ResourceKind::Alias, 38),
            slug: slug("withdrawn"),
            body: alias_body(
                &acme,
                &core,
                38,
                &[reference(
                    crate::desired_state::ResourceKind::ModelEnablement,
                    32,
                )],
            ),
        };
        assert_eq!(
            alias_status(&models, &withdrawn, &compiled).unavailable,
            vec![AliasUnavailableReason::DisabledTarget]
        );

        let fallback = ModelAlias {
            reference: reference(crate::desired_state::ResourceKind::Alias, 39),
            slug: slug("fallback"),
            body: alias_body(
                &acme,
                &core,
                39,
                &[
                    reference(crate::desired_state::ResourceKind::ModelEnablement, 31),
                    reference(crate::desired_state::ResourceKind::ModelEnablement, 30),
                ],
            ),
        };
        assert_eq!(
            alias_status(&models, &fallback, &compiled),
            AliasStatus {
                routable: true,
                unavailable: Vec::new(),
            },
            "an unusable fallback does not make an alias unavailable when a later target works"
        );
    }

    #[test]
    fn a_project_read_only_projects_aliases_owned_by_that_project() {
        let view = read(tenant_id(1), Some(project_id(2)));
        assert_eq!(view.aliases.len(), 1);
        assert_eq!(view.aliases[0].slug, "fast");

        let other = read(tenant_id(1), Some(project_id(4)));
        assert!(other.aliases.is_empty());
    }

    /// The pinned coordinates are projected verbatim, because they are the whole
    /// correlation key a caller has until the catalogue slice can resolve them
    /// into provider metadata.
    #[test]
    fn an_entry_carries_the_catalogue_coordinates_it_was_enabled_against() {
        let view = read(tenant_id(1), None);
        let entry = entry(&view, "gpt-4o");
        assert_eq!(entry.offering, offering_id("gpt-4o").to_string());
        assert_eq!(
            entry.catalog_snapshot,
            crate::desired_state::fixtures::catalog_snapshot().to_string()
        );
    }

    /// A fact this build cannot consult is named rather than omitted: an operator
    /// must be able to tell "not healthy" from "this release does not know".
    #[test]
    fn a_read_names_the_facts_it_could_not_consult() {
        assert_eq!(read(tenant_id(1), None).pending, PendingFact::ALL.to_vec());
    }

    /// Before the first publication there is no revision, and a tenant with
    /// nothing enabled is not an error either: both answer an empty catalogue that
    /// still declares its scope.
    #[test]
    fn an_unpublished_control_plane_is_an_empty_catalogue() {
        let request = CatalogueRequest {
            tenant: tenant_id(1),
            project: None,
            filters: CatalogueFilters::default(),
        };
        let view = CatalogueView::of(None, &request).expect("an empty control plane is readable");
        assert!(view.revision.is_none());
        assert!(view.entries.is_empty());
        assert!(view.aliases.is_empty());
        assert_eq!(view.scope.kind, "tenant");
        assert_eq!(view.pending, PendingFact::ALL.to_vec());

        let stranger = read(tenant_id(7), None);
        assert!(stranger.revision.is_some());
        assert!(stranger.entries.is_empty());
    }

    /// Compiled `PricingSnapshot::price` decides `billable`, not `approved_price`.
    #[tokio::test]
    async fn a_covering_book_is_billable_without_an_enablement_price_pointer() {
        let (revision, store) = covering_book().await;
        let models = Models::of(revision.state()).expect("the fixture models are valid");
        let enablement = models
            .enablement(resource_id(30))
            .expect("the enablement is present");
        assert!(
            enablement.body.billable_price().is_none(),
            "the lie is a covering book with no approved_price pointer"
        );

        let view = CatalogueView::of_with_context(
            Some(&revision),
            &CatalogueRequest {
                tenant: tenant_id(1),
                project: None,
                filters: CatalogueFilters::default(),
            },
            Some(&store),
            None,
            StatusScope::Deployment,
            SystemTime::now(),
        )
        .await
        .expect("the covering-book revision is readable");
        let entry = entry(&view, "gpt-4o");
        assert!(entry.price.is_some(), "{entry:?}");
        assert!(entry.billable, "{entry:?}");
        assert!(
            !entry.unavailable.contains(&UnavailableReason::Unpriced),
            "{entry:?}"
        );
        assert!(entry.notices.is_empty());
        let encoded = serde_json::to_value(entry).expect("serializable");
        assert!(encoded.get("notices").is_none());
        assert_eq!(view.aliases.len(), 1);
        assert!(view.aliases[0].routable);
        assert!(view.aliases[0].unavailable.is_empty());
    }

    async fn covering_book() -> (LoadedRevision, InMemoryCatalogStore) {
        let snapshot = ModelsDevAdapter::default()
            .parse(CATALOG.as_bytes(), SourceValidators::default(), UNIX_EPOCH)
            .expect("the catalogue fixture parses");
        let acme = tenant_id(1);
        let core = project_id(2);
        let catalog_reference = reference(ResourceKind::CatalogModel, 5);
        let catalog = ResourceVersion::new(
            catalog_reference,
            ResourceScope::Deployment,
            slug("models-dev"),
            ResourceBody::Blob(BlobRef::of(BlobKind::CatalogSnapshot, CATALOG.as_bytes())),
        );
        let offering = CatalogOffering::new(
            OfferingId::of("openai", "openai/gpt-5.5").expect("an offering id"),
            snapshot.source.raw.digest,
        );
        let default = ModelEnablementBody::new(
            resource_id(30),
            ModelOwner::tenant(acme),
            offering,
            WireFamily::OpenaiChat,
        )
        .version(slug("gpt-4o"), catalog_reference);
        let book = PriceBookBody::new(
            snapshot.content.content_id(),
            ResourceVersionNumber::FIRST,
            Approval::Approved {
                by: actor(),
                at: EffectiveInstant::EPOCH,
                citation: None,
            },
        )
        .with_rule(price_rule(
            priced_target("openai", "openai/gpt-5.5"),
            RulePrecedence::Baseline,
            EffectiveInterval::from(EffectiveInstant::EPOCH),
            2_500_000,
            10_000_000,
        ))
        .version(resource_id(70), slug("baseline"));
        let alias = typed_alias(&acme, &core, 33, "default", &[default.reference]);

        let mut state = DesiredState::new();
        state.declare_blob(*catalog.body.blob().expect("a blob body"));
        state
            .insert(tenant(1, "acme"))
            .and_then(|state| state.insert(project(&acme, 2, "core")))
            .and_then(|state| state.insert(catalog))
            .and_then(|state| state.insert(default))
            .and_then(|state| state.insert(book))
            .and_then(|state| state.insert(alias))
            .expect("the covering-book state is publishable");

        let store = InMemoryCatalogStore::new();
        store
            .activate(
                &RetainedCatalog {
                    source: snapshot.source,
                    payload: RawPayload::new(CATALOG.as_bytes()),
                },
                SystemTime::now(),
            )
            .await
            .expect("the exact payload is retained");
        (loaded(state), store)
    }

    #[test]
    fn a_reason_and_a_pending_fact_serialize_as_their_wire_spelling() {
        assert_eq!(
            serde_json::to_value(UnavailableReason::ALL).expect("serializable"),
            serde_json::json!(["disabled", "shadowed", "unpriced", "unaliased"])
        );
        assert_eq!(
            serde_json::to_value(PendingFact::ALL).expect("serializable"),
            serde_json::json!(["offering-metadata", "availability"])
        );
        assert_eq!(
            serde_json::to_value(AliasUnavailableReason::ALL).expect("serializable"),
            serde_json::json!([
                "disabled",
                "no-targets",
                "disabled-target",
                "unpriced-target"
            ])
        );
    }
}
