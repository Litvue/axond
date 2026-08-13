//! Deterministic tests for deriving availability from a revision (#148).
//!
//! Every one of them fixes `now`, and every one asserts on a *dimension* rather
//! than only on a state: the property the projection exists for is that a
//! deployment can say which authority refused, so a test that checked only
//! "not available" would pass against a projection that had lost the
//! distinction.

use std::time::{Duration, SystemTime};

use gateway_core::CircuitState;

use super::projection::testing;
use super::*;
use crate::backends::catalog::{CatalogContent, CatalogModelEntry, ModelId};
use crate::desired_state::credentials::ProviderCredentialBody;
use crate::desired_state::fixtures;
use crate::desired_state::models::{ModelLifecycle, ModelOwner, WireFamily};
use crate::desired_state::providers::ProviderBody;
use crate::desired_state::secrets::{SecretLifecycle, SecretOwner};
use crate::desired_state::{DesiredState, ProjectId, ResourceVersion, Slug, TenantId};

const MODEL: &str = "gpt-4o";
const PROVIDER: &str = "openai";

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn target() -> TargetRef {
    TargetRef::parse(PROVIDER, MODEL).expect("a well-formed target")
}

/// The catalogue every fixture enablement pins, reduced to a listing.
fn listing() -> CatalogueListing {
    testing::listing(fixtures::catalog_snapshot(), PROVIDER, MODEL)
}

fn catalogue() -> Catalogue {
    Catalogue::active(listing())
}

/// A catalogue that no longer carries the offering, but remembers the snapshot
/// the enablement pinned: the withdrawal shape.
fn withdrawn_catalogue() -> Catalogue {
    let empty = CatalogueListing::of(
        crate::desired_state::Checksum::of(b"a later catalogue import"),
        &CatalogContent::new(
            Vec::new(),
            vec![CatalogModelEntry {
                id: ModelId::parse("claude-3").expect("a well-formed model id"),
                neutral: None,
                offerings: Vec::new(),
            }],
        )
        .expect("a catalogue with one model"),
    );
    Catalogue::active(empty).with_superseded(listing())
}

/// A provider connection the tenant owns, named as the catalogue names the
/// upstream.
fn connection(tenant: TenantId, seed: u64) -> ResourceVersion {
    ProviderBody::for_tenant(
        fixtures::provider_id(seed),
        tenant,
        fixtures::display_name("OpenAI"),
        WireFamily::OpenaiChat,
        "https://api.openai.test",
    )
    .version(Slug::parse(PROVIDER).expect("a well-formed slug"))
}

/// A credential of the tenant's, in service, authenticating to that connection.
fn active_credential(tenant: TenantId, seed: u64, slug: &str) -> ResourceVersion {
    ProviderCredentialBody::staged(
        fixtures::resource_id(seed),
        SecretOwner::tenant(tenant),
        fixtures::provider_id(seed),
        fixtures::display_name("Key"),
        fixtures::secret_ref(seed),
    )
    .transitioned(SecretLifecycle::Active)
    .expect("staged material may enter service")
    .version(Slug::parse(slug).expect("a well-formed slug"))
}

/// A tenant that has everything: a catalogue pin, an enablement, a connection, a
/// credential in service, and a policy document.
///
/// Built up rather than taken whole, because every test below removes exactly
/// one of those and asserts on which dimension notices.
struct Deployment {
    state: DesiredState,
    tenant: TenantId,
    project: ProjectId,
}

impl Deployment {
    fn new() -> Self {
        let tenant = fixtures::tenant_id(1);
        let project = fixtures::project_id(2);
        let catalog = fixtures::blob_backed_catalog(5);
        let mut state = DesiredState::new();
        state.declare_blob(*catalog.body.blob().expect("a blob body"));
        state
            .insert(fixtures::tenant(1, "acme"))
            .and_then(|state| state.insert(fixtures::project(&tenant, 2, "core")))
            .and_then(|state| state.insert(catalog))
            .and_then(|state| state.insert(fixtures::tenant_enablement(&tenant, 30, MODEL)))
            .expect("the fixture revision is valid");
        Self {
            state,
            tenant,
            project,
        }
    }

    #[must_use]
    fn with(mut self, resource: ResourceVersion) -> Self {
        self.state
            .insert(resource)
            .expect("a fixture resource is a distinct reference");
        self
    }

    /// The connection and the credential together: what entitles a scope.
    #[must_use]
    fn entitled(self) -> Self {
        let tenant = self.tenant;
        self.with(connection(tenant, 40))
            .with(active_credential(tenant, 40, "openai-key"))
    }

    #[must_use]
    fn governed(self) -> Self {
        self.with(fixtures::tenant_policy(1, 1))
    }

    fn scope(&self) -> ScopeRef {
        ScopeRef::tenant(self.tenant)
    }

    fn key(&self) -> AvailabilityKey {
        AvailabilityKey::new(self.scope(), target())
    }
}

/// The material a candidate resolved for the credential seeded `seed`.
fn resolved(seed: u64) -> CredentialReadiness {
    CredentialReadiness::none().holding(fixtures::secret_ref(seed))
}

fn project(
    deployment: &Deployment,
    catalogue: &Catalogue,
    readiness: &CredentialReadiness,
    observations: impl IntoIterator<Item = DiscoveryObservation>,
) -> ProjectedAvailability {
    AvailabilityProjection::new(catalogue, readiness)
        .project(&deployment.state, &AvailabilityIndex::empty(), observations)
        .expect("the fixture revision projects")
}

fn verdict(projected: &ProjectedAvailability, key: &AvailabilityKey, now: u64) -> Availability {
    projected.index().evaluate(key, at(now))
}

/// The property #148 is named for. Every authority but the catalogue is silent,
/// and the answer is a refusal that names the missing one — not `available`, and
/// not a vague uncertainty either.
#[test]
fn a_catalogued_offering_nobody_entitled_is_denied_rather_than_available() {
    let deployment = Deployment::new().governed();
    let projected = project(
        &deployment,
        &catalogue(),
        &CredentialReadiness::none(),
        None,
    );

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Denied);
    assert_eq!(verdict.reason, AvailabilityReason::EntitlementMissing);
    assert_eq!(verdict.decided_by, DecidedBy::Entitlement);
    assert!(!verdict.permits_attempt());
}

/// Entitlement is credential *readiness*, not credential existence: a credential
/// in service whose exact version this candidate never resolved has proven
/// nothing.
#[test]
fn a_credential_whose_material_did_not_resolve_is_unknown_rather_than_granted() {
    let deployment = Deployment::new().entitled().governed();
    let projected = project(
        &deployment,
        &catalogue(),
        &CredentialReadiness::none(),
        None,
    );

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert_eq!(verdict.reason, AvailabilityReason::EntitlementUnknown);
    assert_eq!(verdict.decided_by, DecidedBy::Entitlement);
}

/// A credential taken out of service refuses, and the projection names the
/// credential so an operator knows which row to repair.
#[test]
fn a_revoked_credential_denies_and_names_itself() {
    let tenant = fixtures::tenant_id(1);
    let revoked = ProviderCredentialBody::staged(
        fixtures::resource_id(40),
        SecretOwner::tenant(tenant),
        fixtures::provider_id(40),
        fixtures::display_name("Key"),
        fixtures::secret_ref(40),
    )
    .transitioned(SecretLifecycle::Revoked)
    .expect("staged material may be revoked")
    .version(Slug::parse("openai-key").expect("a well-formed slug"));
    let deployment = Deployment::new()
        .with(connection(tenant, 40))
        .with(revoked)
        .governed();

    let projected = project(&deployment, &catalogue(), &resolved(40), None);

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Denied);
    assert_eq!(verdict.reason, AvailabilityReason::EntitlementRevoked);
    let record = projected
        .index()
        .record(&deployment.key())
        .expect("the enablement produced a record");
    assert_eq!(
        record.credential,
        Some(CredentialRef::parse("openai-key").expect("a well-formed reference"))
    );
}

/// Everything the deployment can decide, decided — and still not `available`,
/// because nothing has established that this account can call the model. That is
/// the whole difference between a catalogue and availability.
#[test]
fn a_fully_entitled_target_with_no_discovery_evidence_is_unknown() {
    let deployment = Deployment::new().entitled().governed();
    let projected = project(&deployment, &catalogue(), &resolved(40), None);

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert_eq!(verdict.reason, AvailabilityReason::NoEvidence);
    assert_eq!(verdict.decided_by, DecidedBy::Discovery);
}

/// The one path to `available`: every authority permits *and* a complete look
/// found the target. Its evidence travels with the verdict.
#[test]
fn discovery_evidence_over_a_permitting_revision_is_available() {
    let deployment = Deployment::new().entitled().governed();
    let observation = DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    )
    .expiring_at(at(200));
    let projected = project(&deployment, &catalogue(), &resolved(40), [observation]);

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert_eq!(verdict.reason, AvailabilityReason::Observed);
    assert_eq!(verdict.observed_at, Some(at(90)));
    assert_eq!(verdict.expires_at, Some(at(200)));
    assert!(!verdict.last_known_good);
    assert!(verdict.permits_attempt());
}

/// A policy document is an authority of its own: an entitled, catalogued,
/// enabled target whose scope nobody has governed is uncertain, and says which
/// dimension left it so.
#[test]
fn an_ungoverned_scope_is_unknown_by_policy() {
    let deployment = Deployment::new().entitled();
    let projected = project(&deployment, &catalogue(), &resolved(40), None);

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert_eq!(verdict.reason, AvailabilityReason::PolicyIndeterminate);
    assert_eq!(verdict.decided_by, DecidedBy::Policy);
}

/// A withdrawal is not an absence: the target keeps its name, and the refusal
/// says the catalogue dropped it rather than that nobody imported it.
#[test]
fn an_offering_the_active_catalogue_dropped_is_withdrawn() {
    let deployment = Deployment::new().entitled().governed();
    let projected = project(&deployment, &withdrawn_catalogue(), &resolved(40), None);

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Unavailable);
    assert_eq!(verdict.reason, AvailabilityReason::WithdrawnFromCatalogue);
    assert_eq!(verdict.decided_by, DecidedBy::Catalogue);
    assert_eq!(projected.skewed(), 1);
}

/// An enablement no listing in hand can name produces no record at all, and is
/// counted: a projection that silently described nothing would look exactly like
/// a tenant that enabled nothing.
#[test]
fn an_unnameable_enablement_is_counted_and_files_no_record() {
    let deployment = Deployment::new().entitled().governed();
    let empty = Catalogue::active(CatalogueListing::of(
        fixtures::catalog_snapshot(),
        &CatalogContent::new(
            Vec::new(),
            vec![CatalogModelEntry {
                id: ModelId::parse("claude-3").expect("a well-formed model id"),
                neutral: None,
                offerings: Vec::new(),
            }],
        )
        .expect("a catalogue with one model"),
    ));

    let projected = project(&deployment, &empty, &resolved(40), None);

    assert_eq!(projected.unnameable(), 1);
    assert!(projected.index().record(&deployment.key()).is_none());
    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.decided_by, DecidedBy::NoRecord);
    assert!(!verdict.permits_attempt());
}

/// One tenant's credential entitles nothing in another's scope, and one tenant's
/// enablement is not filed under another's key.
#[test]
fn another_tenants_credential_does_not_entitle_this_ones() {
    let other = fixtures::tenant_id(7);
    let deployment = Deployment::new()
        .with(fixtures::tenant(7, "globex"))
        .with(connection(other, 40))
        .with(active_credential(other, 40, "globex-key"))
        .governed();

    let projected = project(&deployment, &catalogue(), &resolved(40), None);

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Denied);
    assert_eq!(verdict.reason, AvailabilityReason::EntitlementMissing);
    assert!(
        projected
            .index()
            .record(&AvailabilityKey::new(ScopeRef::tenant(other), target()))
            .is_none()
    );
}

/// Evidence filed for one tenant never answers another's question, even when
/// both enable the same offering from the same provider.
#[test]
fn evidence_for_one_tenant_does_not_answer_for_another() {
    let other = fixtures::tenant_id(7);
    let deployment = Deployment::new()
        .entitled()
        .governed()
        .with(fixtures::tenant(7, "globex"))
        .with(fixtures::tenant_enablement(&other, 31, MODEL))
        .with(fixtures::tenant_policy(7, 1))
        .with(connection(other, 41))
        .with(active_credential(other, 41, "globex-key"));
    let readiness = resolved(40).holding(fixtures::secret_ref(41));
    let observation = DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    );

    let projected = project(&deployment, &catalogue(), &readiness, [observation]);

    assert_eq!(
        verdict(&projected, &deployment.key(), 100).state,
        AvailabilityState::Available
    );
    let theirs = AvailabilityKey::new(ScopeRef::tenant(other), target());
    let verdict = verdict(&projected, &theirs, 100);
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert_eq!(verdict.reason, AvailabilityReason::NoEvidence);
}

/// A project override replaces its tenant's default, including a disabled one:
/// the same precedence desired state already gives it, carried into availability
/// rather than re-decided here.
#[test]
fn a_disabled_project_override_denies_where_the_tenant_default_permits() {
    let deployment = Deployment::new().entitled().governed();
    let tenant = deployment.tenant;
    let owned_by = deployment.project;
    let disabled = fixtures::enablement_body(31, ModelOwner::project(tenant, owned_by), MODEL)
        .transitioned(ModelLifecycle::Disabled)
        .version(
            Slug::parse(MODEL).expect("a well-formed slug"),
            fixtures::catalog_reference(),
        );
    let deployment = deployment.with(disabled);

    let projected = project(&deployment, &catalogue(), &resolved(40), None);

    let overridden = AvailabilityKey::new(ScopeRef::project(tenant, owned_by), target());
    let verdict = verdict(&projected, &overridden, 100);
    assert_eq!(verdict.state, AvailabilityState::Denied);
    assert_eq!(verdict.reason, AvailabilityReason::NotEnabled);
    assert_eq!(verdict.decided_by, DecidedBy::Enablement);
}

/// Runtime health is this replica's, overlaid when the question is asked: the
/// derived index carries no circuit state, so what one replica's bad afternoon
/// changes is that replica's answer and nothing else.
#[test]
fn an_open_circuit_refuses_on_this_replica_without_touching_the_index() {
    let deployment = Deployment::new().entitled().governed();
    let observation = DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    );
    let projected = project(&deployment, &catalogue(), &resolved(40), [observation]);
    let index = projected.into_index();

    let healthy = RuntimeObservations::none();
    assert_eq!(
        AvailabilityView::new(&index, &healthy)
            .evaluate(&deployment.key(), at(100))
            .state,
        AvailabilityState::Available
    );

    let tripped =
        RuntimeObservations::of_circuits([(format!("{PROVIDER}/{MODEL}"), CircuitState::Open)]);
    let refused = AvailabilityView::new(&index, &tripped).evaluate(&deployment.key(), at(100));
    assert_eq!(refused.state, AvailabilityState::Unavailable);
    assert_eq!(refused.decided_by, DecidedBy::Runtime);
    assert_eq!(
        index
            .record(&deployment.key())
            .expect("the record is still filed")
            .runtime,
        RuntimeHealth::Unobserved
    );
}

/// A half-open circuit lowers certainty without discarding the evidence: the
/// verdict becomes `unknown`, and still says when the look was taken.
#[test]
fn a_half_open_circuit_lowers_a_positive_verdict_and_keeps_its_evidence() {
    let deployment = Deployment::new().entitled().governed();
    let observation = DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    );
    let index = project(&deployment, &catalogue(), &resolved(40), [observation]).into_index();
    let impaired =
        RuntimeObservations::of_circuits([(format!("{PROVIDER}/{MODEL}"), CircuitState::HalfOpen)]);

    let verdict = AvailabilityView::new(&index, &impaired).evaluate(&deployment.key(), at(100));
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert_eq!(verdict.decided_by, DecidedBy::Runtime);
    assert_eq!(verdict.observed_at, Some(at(90)));
    assert_eq!(verdict.source, Some(DiscoverySource::ProviderListing));
}

/// A publication re-derives the dimensions and keeps the evidence: a revision
/// that changes nothing about a provider must not cost the deployment its
/// freshness.
#[test]
fn evidence_survives_the_next_projection() {
    let deployment = Deployment::new().entitled().governed();
    let observation = DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    );
    let first = project(&deployment, &catalogue(), &resolved(40), [observation]).into_index();

    let again = AvailabilityProjection::new(&catalogue(), &resolved(40))
        .project(&deployment.state, &first, None)
        .expect("the revision projects again");

    let verdict = verdict(&again, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert_eq!(verdict.observed_at, Some(at(90)));
}

/// The other half of that rule: a key the revision in hand no longer describes
/// loses the permit the previous one derived. It keeps its evidence — the look
/// was still taken — but every dimension is re-stated by the revision or not at
/// all, so the answer falls to a refusal rather than outliving its authority.
#[test]
fn a_target_the_revision_stopped_describing_stops_being_permitted() {
    let deployment = Deployment::new().entitled().governed();
    let observation = DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    );
    let first = project(&deployment, &catalogue(), &resolved(40), [observation]).into_index();
    assert_eq!(
        first.evaluate(&deployment.key(), at(100)).state,
        AvailabilityState::Available
    );

    // The catalogue snapshot the enablement pinned is no longer in hand, so this
    // projection can name nothing: the same shape a rollback that dropped the
    // enablement produces.
    let unnameable = Catalogue::active(CatalogueListing::of(
        crate::desired_state::Checksum::of(b"a catalogue naming nothing"),
        &CatalogContent::new(
            Vec::new(),
            vec![CatalogModelEntry {
                id: ModelId::parse("claude-3").expect("a well-formed model id"),
                neutral: None,
                offerings: Vec::new(),
            }],
        )
        .expect("a catalogue with one model"),
    ));
    let again = AvailabilityProjection::new(&unnameable, &resolved(40))
        .project(&deployment.state, &first, None)
        .expect("the revision projects again");

    assert_eq!(again.undescribed(), 1);
    let verdict = verdict(&again, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Unavailable);
    assert_eq!(verdict.decided_by, DecidedBy::Catalogue);
    assert!(!verdict.permits_attempt());
    assert!(
        again
            .index()
            .record(&deployment.key())
            .expect("the evidence outlives the description")
            .discovery
            .is_some(),
        "the look was still taken, and a later revision may describe the target again"
    );
}

/// A key nobody ever learned anything about leaves nothing behind: only evidence
/// survives a re-derivation, so the index does not accumulate records for scopes
/// a revision has forgotten.
#[test]
fn a_forgotten_key_with_no_evidence_leaves_no_record() {
    let deployment = Deployment::new().entitled().governed();
    let first = project(&deployment, &catalogue(), &resolved(40), None).into_index();
    assert!(first.record(&deployment.key()).is_some());

    let empty = DesiredState::new();
    let again = AvailabilityProjection::new(&catalogue(), &resolved(40))
        .project(&empty, &first, None)
        .expect("an empty revision projects");

    assert_eq!(again.undescribed(), 0);
    assert!(again.index().record(&deployment.key()).is_none());
}

/// A refused projection applied nothing, so it must not consume anything either:
/// the looks it was handed are still the newest evidence this replica holds.
#[test]
fn a_refused_projection_keeps_the_looks_it_could_not_apply() {
    let deployment = Deployment::new().entitled().governed();
    let evidence = AvailabilityEvidence::new(catalogue());
    evidence.observe(DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    ));

    let mut unreadable = DesiredState::new();
    unreadable
        .insert(ResourceVersion::new(
            fixtures::reference(crate::desired_state::ResourceKind::ModelEnablement, 30),
            crate::desired_state::ResourceScope::Deployment,
            Slug::parse("unreadable").expect("a well-formed slug"),
            crate::desired_state::ResourceBody::Inline(
                crate::desired_state::CanonicalValue::map::<String>([]),
            ),
        ))
        .expect("a distinct reference");
    evidence
        .derive(&unreadable, &resolved(40))
        .expect_err("a revision this build cannot read refuses the projection");

    let projected = evidence
        .derive(&deployment.state, &resolved(40))
        .expect("the next revision projects");

    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert_eq!(verdict.observed_at, Some(at(90)));
}

/// A discovery outage degrades to last-known-good and then ages into `stale`. It
/// never becomes a refusal, and it never silently upgrades.
#[test]
fn a_discovery_outage_keeps_the_last_known_good_look() {
    let deployment = Deployment::new().entitled().governed();
    let evidence = AvailabilityEvidence::new(catalogue());
    evidence.observe(
        DiscoveryObservation::new(
            deployment.scope(),
            target(),
            DiscoveryResult::Present,
            DiscoveryCompleteness::Complete,
            DiscoverySource::ProviderListing,
            at(90),
        )
        .expiring_at(at(150)),
    );
    evidence
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects");

    evidence.observe(DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Indeterminate,
        DiscoveryCompleteness::Partial,
        DiscoverySource::ProviderListing,
        at(140),
    ));
    let outaged = evidence
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects again");

    let held = verdict(&outaged, &deployment.key(), 145);
    assert_eq!(held.state, AvailabilityState::Available);
    assert_eq!(held.reason, AvailabilityReason::LastKnownGood);
    assert!(held.last_known_good);

    let aged = verdict(&outaged, &deployment.key(), 200);
    assert_eq!(aged.state, AvailabilityState::Stale);
    assert_eq!(aged.observed_at, Some(at(90)));
}

/// The verdict a namespace sees carries no discovery source and no operator
/// detail: the evidence's `detail` can hold a provider's error body, and a
/// tenant asking what it may call is not asking about the deployment's plumbing.
#[test]
fn a_namespace_scoped_verdict_discloses_no_discovery_machinery() {
    use crate::status::StatusScope;

    let deployment = Deployment::new().entitled().governed();
    let observation = DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Indeterminate,
        DiscoveryCompleteness::Partial,
        DiscoverySource::ProviderProbe,
        at(90),
    )
    .detailed("HTTP 503 from https://api.example.test/v1/models?key=sk-live-never-printed");
    let projected = project(&deployment, &catalogue(), &resolved(40), [observation]);

    let verdict = verdict(&projected, &deployment.key(), 100).for_scope(StatusScope::Namespace);
    assert_eq!(verdict.source, None);
    let dumped = format!("{:?}", projected.index());
    assert!(!dumped.contains("sk-live"), "{dumped}");
    assert!(!dumped.contains("api.example.test"), "{dumped}");
}

/// The restart path, without a database in it: what a replica writes down is
/// exactly what it can fold back in, and folding it back in is not treated as
/// looks arriving out of order.
#[test]
fn evidence_written_down_and_read_back_is_the_evidence_that_was_held() {
    let deployment = Deployment::new().entitled().governed();
    let evidence = AvailabilityEvidence::new(catalogue());
    evidence.observe(
        DiscoveryObservation::new(
            deployment.scope(),
            target(),
            DiscoveryResult::Present,
            DiscoveryCompleteness::Complete,
            DiscoverySource::ProviderListing,
            at(90),
        )
        .expiring_at(at(300))
        .detailed("listed by https://api.example.test/v1/models?key=sk-live-never-stored"),
    );
    evidence
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects");
    // The refresh that failed: the positive moves to the fallback slot, so both
    // slots are occupied and a restart has both to restore.
    evidence.observe(DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Indeterminate,
        DiscoveryCompleteness::Partial,
        DiscoverySource::ProviderListing,
        at(120),
    ));
    evidence
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects again");

    let written = evidence.persistable();
    assert_eq!(written.rows().len(), 2, "both slots are written down");
    assert!(
        written
            .rows()
            .iter()
            .all(|row| row.observation.detail.is_none()),
        "a probe's own words do not cross the storage boundary"
    );
    assert_eq!(
        written
            .rows()
            .iter()
            .map(|row| row.slot)
            .collect::<Vec<ObservationSlot>>(),
        vec![ObservationSlot::Current, ObservationSlot::LastKnownGood]
    );

    // The replica that comes back: it restores the evidence, then derives the
    // revision over it, and reaches the verdict the first one held.
    let restarted = AvailabilityEvidence::new(catalogue());
    assert_eq!(
        restarted.restore(written.rows().to_vec()),
        0,
        "stored order is not disorder"
    );
    let projected = restarted
        .derive(&deployment.state, &resolved(40))
        .expect("the restored replica projects");
    assert_eq!(projected.superseded(), 0);

    let held = verdict(&projected, &deployment.key(), 200);
    assert_eq!(held.state, AvailabilityState::Available);
    assert_eq!(held.reason, AvailabilityReason::LastKnownGood);
    assert_eq!(held.observed_at, Some(at(90)));
    assert!(held.last_known_good);
}

/// Remembered evidence is not remembered *authority*. A restart restores looks
/// and nothing else, so a revision that revoked an entitlement is obeyed by the
/// replica that comes back even though its stored evidence says the model was
/// there.
#[test]
fn restored_evidence_does_not_restore_the_authority_a_revision_withdrew() {
    let deployment = Deployment::new().entitled().governed();
    let evidence = AvailabilityEvidence::new(catalogue());
    evidence.observe(DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    ));
    evidence
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects");
    let written = evidence.persistable();

    let restarted = AvailabilityEvidence::new(catalogue());
    restarted.restore(written.rows().to_vec());
    // Restored, but not yet derived: the dimensions are the fail-closed
    // defaults, and a verdict read now refuses rather than reporting the
    // remembered positive.
    let restored = restarted.index();
    let record = restored
        .record(&deployment.key())
        .expect("the key restored");
    assert_eq!(record.presence, CataloguePresence::Absent);
    assert_eq!(record.entitlement, Entitlement::Unknown);
    assert_eq!(
        restored.evaluate(&deployment.key(), at(100)).state,
        AvailabilityState::Unavailable
    );

    // And the revision that comes back with the credential withdrawn is obeyed.
    let withdrawn = Deployment::new().governed();
    let projected = restarted
        .derive(&withdrawn.state, &CredentialReadiness::none())
        .expect("the restored replica projects the current revision");
    let verdict = verdict(&projected, &deployment.key(), 100);
    assert_eq!(verdict.state, AvailabilityState::Denied);
    assert_eq!(verdict.decided_by, DecidedBy::Entitlement);
}

/// Stored evidence goes through the same ordering rule a live look does: a
/// positive older than a conclusion the receiving index has already reached is
/// refused rather than resurrecting a target a complete listing dropped.
#[test]
fn restoring_a_stale_positive_cannot_resurrect_a_target_a_listing_dropped() {
    let deployment = Deployment::new().entitled().governed();
    let stored = {
        let earlier = AvailabilityEvidence::new(catalogue());
        earlier.observe(DiscoveryObservation::new(
            deployment.scope(),
            target(),
            DiscoveryResult::Present,
            DiscoveryCompleteness::Complete,
            DiscoverySource::ProviderListing,
            at(90),
        ));
        earlier
            .derive(&deployment.state, &resolved(40))
            .expect("the revision projects");
        earlier.persistable().rows().to_vec()
    };

    let running = AvailabilityEvidence::new(catalogue());
    running.observe(DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Absent,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(200),
    ));
    running
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects");

    assert_eq!(running.restore(stored), 1, "the stale positive is refused");
    let projected = running
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects again");
    let verdict = verdict(&projected, &deployment.key(), 210);
    assert_eq!(verdict.state, AvailabilityState::Denied);
    assert_eq!(verdict.reason, AvailabilityReason::DiscoveryAbsent);
    assert_eq!(verdict.decided_by, DecidedBy::Discovery);
    assert!(!verdict.last_known_good);
}

/// A row that names another scope than its key is refused, so evidence that was
/// mis-filed — by a bug, or by a hand-edited row — cannot decide a tenant's
/// availability from another tenant's look.
#[test]
fn restoring_refuses_a_row_whose_evidence_names_another_scope() {
    let deployment = Deployment::new().entitled().governed();
    let other = ScopeRef::tenant(fixtures::tenant_id(11));
    let evidence = AvailabilityEvidence::new(catalogue());
    let refused = evidence.restore([StoredObservation {
        key: deployment.key(),
        slot: ObservationSlot::Current,
        observation: DiscoveryObservation::new(
            other,
            target(),
            DiscoveryResult::Present,
            DiscoveryCompleteness::Complete,
            DiscoverySource::ProviderListing,
            at(90),
        ),
        definitive_at: Some(at(90)),
    }]);

    assert_eq!(refused, 1);
    let record = evidence
        .index()
        .record(&deployment.key())
        .expect("the key exists")
        .clone();
    assert!(record.discovery.is_none());
    assert!(record.last_known_good.is_none());
    assert_eq!(record.definitive_at, None);
}

/// The overlay finds this replica's own trouble only if it looks the target up
/// under the string the request path files a circuit under. Both sides build
/// that string with `FailoverTarget::qualified_model`, and this is what fails if
/// either stops.
#[test]
fn a_targets_circuit_key_is_the_one_the_request_path_writes() {
    let routed = crate::config::Target {
        provider: PROVIDER.to_owned(),
        model: MODEL.to_owned(),
        price: gateway_core::ModelPrice {
            input_microdollars_per_million: 1,
            output_microdollars_per_million: 1,
            reasoning_microdollars_per_million: None,
            cache_read_microdollars_per_million: None,
            cache_write_microdollars_per_million: None,
        },
    };
    let written = crate::routes::target_key(&routed);
    assert_eq!(written, RuntimeObservations::circuit_key(&target()));

    // And read back through the overlay, so the agreement is exercised rather
    // than only asserted: a tripped circuit lowers the verdict for the target
    // whose key the request path wrote.
    let deployment = Deployment::new().entitled().governed();
    let evidence = AvailabilityEvidence::new(catalogue());
    evidence.observe(DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(90),
    ));
    let projected = evidence
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects");
    let runtime = RuntimeObservations::of_circuits([(written, CircuitState::Open)]);
    let verdict =
        AvailabilityView::new(projected.index(), &runtime).evaluate(&deployment.key(), at(100));
    assert_eq!(verdict.state, AvailabilityState::Unavailable);
    assert_eq!(verdict.decided_by, DecidedBy::Runtime);
}

/// Convergence compiles only when desired state changes, so a deployment that
/// publishes nothing all day must still be able to act on what it looked at. A
/// re-projection folds the queue into the revision already derived — and answers
/// nothing at all before there is one.
#[test]
fn a_look_taken_between_revisions_reaches_a_served_index() {
    let deployment = Deployment::new().entitled().governed();
    let evidence = AvailabilityEvidence::new(catalogue());

    assert!(
        evidence.reproject().is_none(),
        "there is no revision to fold a look into yet"
    );

    evidence
        .derive(&deployment.state, &resolved(40))
        .expect("the revision projects");
    assert_eq!(
        evidence.index().evaluate(&deployment.key(), at(100)).reason,
        AvailabilityReason::NoEvidence
    );

    // The discovery loop looks, and nothing publishes a revision afterwards.
    evidence.observe(DiscoveryObservation::new(
        deployment.scope(),
        target(),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(110),
    ));
    let projected = evidence
        .reproject()
        .expect("the revision it derived is still the one to fold into")
        .expect("the same revision projects again");

    let verdict = verdict(&projected, &deployment.key(), 120);
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert_eq!(verdict.decided_by, DecidedBy::Discovery);
    assert_eq!(
        evidence.index().evaluate(&deployment.key(), at(120)).state,
        AvailabilityState::Available,
        "and the replica holds it, so the next snapshot carries it"
    );
}
