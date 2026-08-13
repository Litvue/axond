//! The tenant-scoped management catalogue: what one tenant may enable, what it
//! has enabled, and what still stands between an enablement and a routable name.
//!
//! The read a tenant administrator needs before publishing anything. `/v1/models`
//! answers "what can this key invoke right now" from an immutable snapshot, and
//! deliberately answers nothing else: it does not say that an offering exists but
//! is disabled, that an enablement has no approved price, or that no alias points
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
//! - **It reports reasons, not verdicts it cannot reach.** A model is unroutable
//!   for reasons this revision states ([`UnavailableReason`]) — disabled,
//!   shadowed by a project override, unpriced, unaliased. Provider health,
//!   capability and modality metadata, and observed availability come from
//!   catalogue import (#146), pricing (#147), and the availability index (#148);
//!   until those land, this read is silent about them rather than guessing, and
//!   [`CatalogueView::pending`] names the facts it could not consult so a caller
//!   is not misled by their absence.
//!
//! # The integration seam
//!
//! An enablement names a [`CatalogOffering`]: an opaque [`OfferingId`] plus the
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
//! filters this module refuses today (`provider`, `capability`, `modality`) become
//! predicates over its result, and `pending` shrinks. Nothing else here changes:
//! the scope rule, the reason vocabulary, and the response shape are independent
//! of where offering metadata comes from.

use serde::Serialize;

use super::diff::ScopeView;
use super::error::AdminError;
use crate::desired_state::{
    LoadedRevision, ModelAlias, ModelEnablement, ModelError, ModelLifecycle, ModelOwner, Models,
    OfferingId, ProjectId, ResourceScope, TenantId, WireFamily,
};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CatalogueFilters {
    /// Enabled or disabled — the operator's decision, not an upstream's.
    pub state: Option<ModelLifecycle>,
    pub wire_family: Option<WireFamily>,
    /// One offering, by identity. Exact rather than a substring search: the id is
    /// an opaque digest, so a prefix of one means nothing.
    pub offering: Option<OfferingId>,
    /// `Some(true)` for entries with an approved price, `Some(false)` for those
    /// without.
    pub billable: Option<bool>,
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

    fn admits(&self, entry: &ModelEnablement) -> bool {
        let filters = &self.filters;
        filters
            .state
            .is_none_or(|state| state == entry.body.state())
            && filters
                .wire_family
                .is_none_or(|family| family == entry.body.wire_family())
            && filters
                .offering
                .is_none_or(|offering| offering == entry.body.offering().offering)
            && filters
                .billable
                .is_none_or(|billable| billable == entry.body.billable_price().is_some())
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
    /// No approved price, so the model could be routed but not billed. An observed
    /// price is not an approval (ADR 0042).
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

/// One enablement, as the management catalogue shows it.
///
/// Identities, scope, lifecycle, and the pinned catalogue coordinates — never a
/// price amount, and never anything derived from secret material. An amount is
/// billing state that the price resource owns; what belongs here is whether one
/// was *approved*, which is what decides routability.
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
    /// Whether an approved price makes it billable.
    pub billable: bool,
    /// The alias names, in this scope, that resolve to this enablement.
    pub aliases: Vec<String>,
    /// Why it is not routable, in a stable order. Empty when it is.
    pub unavailable: Vec<UnavailableReason>,
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
    /// Facts this build could not consult. Always projected, including when
    /// empty, so a client can tell an old build from a complete answer.
    pub pending: Vec<PendingFact>,
}

impl CatalogueView {
    pub fn of(
        revision: Option<&LoadedRevision>,
        request: &CatalogueRequest,
    ) -> Result<Self, AdminError> {
        let Some(revision) = revision else {
            return Ok(Self::empty(None, request));
        };
        // The same resolution publication and hydration use: a projection that
        // read these bodies its own way could report a catalogue the runtime would
        // never serve.
        let models = Models::of(revision.state()).map_err(|error| unreadable(revision, &error))?;
        let mut entries = Vec::new();
        for enablement in models.enablements() {
            let owner = enablement.body.owner();
            if !request.covers(owner) || !request.admits(enablement) {
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
            if enablement.body.billable_price().is_none() {
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
                billable: enablement.body.billable_price().is_some(),
                aliases,
                unavailable,
            });
        }
        Ok(Self {
            revision: Some(revision.id().to_string()),
            scope: ScopeView::of(&request.scope()),
            entries,
            pending: PendingFact::ALL.to_vec(),
        })
    }

    fn empty(revision: Option<String>, request: &CatalogueRequest) -> Self {
        Self {
            revision,
            scope: ScopeView::of(&request.scope()),
            entries: Vec::new(),
            pending: PendingFact::ALL.to_vec(),
        }
    }
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
    use crate::desired_state::fixtures::{
        approved_price, blob_backed_catalog, candidate, catalog_reference, enablement_body,
        offering_id, price, project, project_enablement, project_id, revision_id, tenant,
        tenant_id, typed_alias,
    };
    use crate::desired_state::{
        DesiredState, ExpectedRevision, ProjectId, RevisionManifest, Slug, TenantId,
    };

    /// A revision two tenants publish into: one has priced, aliased, shadowed, and
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

        let priced = entry(&view, "gpt-4o");
        assert!(priced.routable);
        assert!(priced.billable);
        assert_eq!(priced.aliases, vec!["fast".to_owned()]);
        assert!(priced.unavailable.is_empty());

        // Withdrawn, unpriced, and unnamed: three independent reasons, each one a
        // separate administrative act to clear, so the read states all three
        // rather than the first.
        let withdrawn = entry(&view, "gpt-4o-mini");
        assert!(!withdrawn.routable);
        assert_eq!(withdrawn.state, "disabled");
        assert_eq!(
            withdrawn.unavailable,
            vec![
                UnavailableReason::Disabled,
                UnavailableReason::Unpriced,
                UnavailableReason::Unaliased,
            ]
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
            vec!["gpt-4o-mini"]
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
        assert_eq!(view.scope.kind, "tenant");
        assert_eq!(view.pending, PendingFact::ALL.to_vec());

        let stranger = read(tenant_id(7), None);
        assert!(stranger.revision.is_some());
        assert!(stranger.entries.is_empty());
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
    }
}
