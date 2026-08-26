//! `POST /admin/v1/bindings`: expand one imported or local model into one revision.
//!
//! Dedicated handler, not `publish::<R>`. Hydration of [`CatalogStore`] happens
//! here, before a synchronous [`DesiredStateEdit`]. Classification, probes, and
//! `request.scope` are computed from the expander delta against **expected**.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::SystemTime;

use axum::Json;
use axum::extract::State;
use axum::extract::rejection::BytesRejection;
use serde::Deserialize;

use super::auth::{AdminAction, AdminIdentity};
use super::diff::SemanticDiff;
use super::error::AdminError;
use super::protocol::{MutationPreconditions, MutationRequest};
use super::resources::{self, MutationEnvelope};
use super::router::AdminApi;
use super::service::{DesiredStateEdit, MutationOutcome, MutationResult, log_store};
use crate::backends::catalog::{CatalogSnapshot, ProviderId};
use crate::backends::catalog_pins::{PinnedCatalog, Resolution};
use crate::backends::catalog_projection::CallableId;
use crate::backends::catalog_store::{CatalogStore, RetainedCatalog, hydrate};
use crate::backends::local_catalog::LocalCatalogBuilder;
use crate::desired_state::{
    Actor, AliasTarget, Approval, ApprovedRate, ApprovedRates, BlobKind, BlobRef, CatalogOffering,
    Checksum, DesiredState, EffectiveInstant, EffectiveInterval, ExpectedRevision, ModelAliasBody,
    ModelEnablementBody, ModelLifecycle, ModelOwner, MutationKind, ObservedPrice, OfferingId,
    PriceBookBody, PriceBooks, PriceOrigin, PriceProvenance, PriceRule, PricedTarget, ProjectBody,
    ProjectId, ProviderBody, ResourceId, ResourceKind, ResourceRef, ResourceScope, ResourceVersion,
    ResourceVersionNumber, RulePrecedence, Slug, Surface, TenantId, Uuid7, ValidationError,
};
use crate::telemetry::metrics::{record_binding, record_binding_refusal};

const SCHEMA: &str = "binding";

const RULE_UNKNOWN_PROVIDER: &str = "unknown_provider";
const RULE_CATALOGUE_IDENTITY: &str = "catalogue_identity_required";
const RULE_NOT_IN_CATALOGUE: &str = "not_in_catalogue";
const RULE_AMBIGUOUS: &str = "ambiguous_callable";
const RULE_OBSERVED_UNBILLABLE: &str = "observed_unbillable";
const RULE_PRICE_REQUIRED: &str = "price_required";
const RULE_CATALOGUE_NOT_IMPORTED: &str = "catalogue_not_imported";
const RULE_PROJECT_REQUIRED: &str = "project_required";
const RULE_PIN_LOCKED: &str = "pin_locked";
const RULE_NOT_LOCAL: &str = "not_local";
const RULE_DRAFT_BOOK_NOT_APPROVED_BY_BINDING: &str = "draft_book_not_approved_by_binding";

/// One-model flattened form, or `models: [...]`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BindingResource {
    Many(BindingMany),
    One(BindingOne),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingMany {
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub pin: Option<String>,
    pub models: Vec<BindingModel>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingOne {
    pub tenant: String,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub pin: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    pub targets: Vec<BindingTarget>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingModel {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub pin: Option<String>,
    pub targets: Vec<BindingTarget>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingTarget {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub catalog: Option<BindingCatalog>,
    #[serde(default)]
    pub price: Option<BindingPrice>,
    #[serde(default)]
    pub source: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingCatalog {
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum BindingPrice {
    Stated {
        input_microdollars_per_million: u64,
        output_microdollars_per_million: u64,
    },
    Observed(String),
}

impl BindingResource {
    /// Parse-time path label; [`BindingPlan::path`] is unavailable until parse succeeds.
    fn path(&self) -> &'static str {
        let local = match self {
            Self::One(one) => one
                .targets
                .iter()
                .any(|target| target.source.as_deref() == Some("local")),
            Self::Many(many) => many.models.iter().any(|model| {
                model
                    .targets
                    .iter()
                    .any(|target| target.source.as_deref() == Some("local"))
            }),
        };
        if local { "local" } else { "imported" }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PinMode {
    Follow,
    Lock,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TargetSource {
    Imported,
    Local,
}

#[derive(Debug, Clone, Copy)]
enum PriceSpec {
    Stated {
        input_micros: u64,
        output_micros: u64,
    },
    Observed,
}

#[derive(Debug, Clone)]
struct PlannedTarget {
    provider_slug: String,
    model: String,
    catalog_provider: Option<String>,
    catalog_model: Option<String>,
    price: Option<PriceSpec>,
    source: TargetSource,
}

#[derive(Debug, Clone)]
struct BindingPlan {
    tenant: TenantId,
    project: Option<ProjectId>,
    name: String,
    pin: PinMode,
    lifecycle: ModelLifecycle,
    mutation: MutationKind,
    targets: Vec<PlannedTarget>,
}

impl BindingPlan {
    fn path(&self) -> &'static str {
        if self
            .targets
            .iter()
            .any(|target| target.source == TargetSource::Local)
        {
            "local"
        } else {
            "imported"
        }
    }

    fn has_imported(&self) -> bool {
        self.targets
            .iter()
            .any(|target| target.source == TargetSource::Imported)
    }
}

struct BindingEdit {
    plan: BindingPlan,
    imported: Option<CatalogSnapshot>,
    locals: BTreeMap<(String, String), CatalogSnapshot>,
    now: EffectiveInstant,
}

impl DesiredStateEdit for BindingEdit {
    fn edit(&self, state: &mut DesiredState, actor: &Actor) -> Result<(), AdminError> {
        expand(
            state,
            actor,
            &self.plan,
            self.imported.as_ref(),
            &self.locals,
            self.now,
        )
    }
}

pub(super) fn bindings_route() -> axum::routing::MethodRouter<Arc<AdminApi>> {
    axum::routing::post(publish_binding)
}

async fn publish_binding(
    State(api): State<Arc<AdminApi>>,
    identity: AdminIdentity,
    preconditions: MutationPreconditions,
    body: Result<axum::body::Bytes, BytesRejection>,
) -> Result<Json<MutationOutcome>, AdminError> {
    publish_binding_inner(api, identity, preconditions, body).await
}

fn record_binding_outcome(outcome: &Result<Json<MutationOutcome>, AdminError>, path: &'static str) {
    match outcome {
        Ok(json) => {
            let label = match &json.0.result {
                MutationResult::Published { .. } => "published",
                MutationResult::Replayed { .. } => "replayed",
                MutationResult::Unchanged { .. } => "unchanged",
                MutationResult::DryRun => "dry_run",
            };
            record_binding(label, path);
        }
        Err(error) => {
            if let AdminError::BindingRefused { rule, .. } = error {
                record_binding("refused", path);
                record_binding_refusal(rule);
            } else if matches!(error, AdminError::NameTaken { .. }) {
                record_binding("refused", path);
            }
        }
    }
}

async fn publish_binding_inner(
    api: Arc<AdminApi>,
    identity: AdminIdentity,
    preconditions: MutationPreconditions,
    body: Result<axum::body::Bytes, BytesRejection>,
) -> Result<Json<MutationOutcome>, AdminError> {
    let body = document_bytes(body)?;
    let envelope: MutationEnvelope<BindingResource> =
        serde_json::from_slice(&body).map_err(|error| AdminError::RequestInvalid {
            schema: SCHEMA,
            detail: error.to_string(),
        })?;
    if envelope.mutation.kind() == MutationKind::Delete {
        return Err(AdminError::RequestInvalid {
            schema: SCHEMA,
            detail: "`mutation: \"delete\"` is not accepted on /bindings; disable with \
                     `mutation: \"update\"` and `state: \"disabled\"`"
                .to_owned(),
        });
    }
    let summary = super::protocol::AuditSummary::parse(&envelope.summary)?;
    let parse_path = envelope.resource.path();
    let plan = match BindingPlan::from_resource(envelope.resource, envelope.mutation.kind()) {
        Ok(plan) => plan,
        Err(error) => {
            if let AdminError::BindingRefused { rule, .. } = &error {
                record_binding("refused", parse_path);
                record_binding_refusal(rule);
            }
            return Err(error);
        }
    };
    let path = plan.path();
    let outcome = publish_parsed_binding(api, identity, preconditions, summary, plan).await;
    record_binding_outcome(&outcome, path);
    outcome
}

async fn publish_parsed_binding(
    api: Arc<AdminApi>,
    identity: AdminIdentity,
    preconditions: MutationPreconditions,
    summary: super::protocol::AuditSummary,
    plan: BindingPlan,
) -> Result<Json<MutationOutcome>, AdminError> {
    let store = api.service.store()?;
    let head_id = store.desired_revision().await.map_err(log_store)?;
    let head_state = match head_id {
        Some(id) => store
            .load_revision(id)
            .await
            .map_err(log_store)?
            .state()
            .clone(),
        None => DesiredState::new(),
    };
    let expected = preconditions.expected;
    let expected_base = load_expected_base(store.as_ref(), expected, head_id).await?;
    // Scope and probes are the expected-delta's, not head's: a lost-response
    // retry of first-apply still has a pre-pin expected and must present a
    // Deployment grant to `apply`.
    let expected_state = match &expected_base {
        ExpectedBase::Missing => &head_state,
        ExpectedBase::State(state) => state,
    };
    let document_scope = plan.document_scope(expected_state)?;
    api.authorize(
        &identity,
        AdminAction::Publish,
        Surface::Model,
        &document_scope,
    )
    .await?;
    api.authorize(
        &identity,
        AdminAction::Publish,
        Surface::Alias,
        &document_scope,
    )
    .await?;

    let imported = load_active_snapshot(api.catalogue.as_deref()).await?;
    if plan.has_imported() && imported.is_none() {
        return Err(refused(
            RULE_CATALOGUE_NOT_IMPORTED,
            "no imported catalogue is active",
        ));
    }
    let locals = prepare_local_catalogs(&plan, imported.as_ref())?;
    let actor = identity.actor();
    let now = EffectiveInstant::of(SystemTime::now()).unwrap_or(EffectiveInstant::EPOCH);
    let local_snapshots: BTreeMap<(String, String), CatalogSnapshot> = locals
        .iter()
        .map(|(key, _, snapshot)| (key.clone(), snapshot.clone()))
        .collect();
    let edit = BindingEdit {
        plan: plan.clone(),
        imported: imported.clone(),
        locals: local_snapshots.clone(),
        now,
    };

    let expected_state = match expected_base {
        ExpectedBase::Missing => {
            retain_local_payloads(
                api.catalogue.as_deref(),
                &locals,
                preconditions.mode.is_dry_run(),
            )
            .await?;
            let grant = api
                .authorize(
                    &identity,
                    AdminAction::Publish,
                    Surface::Model,
                    &document_scope,
                )
                .await?;
            let request = MutationRequest {
                preconditions,
                kind: plan.mutation,
                surface: Surface::Model,
                scope: document_scope,
                summary,
            };
            let outcome = api.service.apply(&grant, &request, &edit).await?;
            return Ok(Json(outcome));
        }
        ExpectedBase::State(state) => state,
    };

    let mut expanded = expected_state.clone();
    expand(
        &mut expanded,
        &actor,
        &plan,
        imported.as_ref(),
        &local_snapshots,
        now,
    )?;
    let expected_diff = SemanticDiff::between(Some(&expected_state), &expanded)?;
    let classification = classify(&expected_state, &expanded, document_scope.clone());

    for (surface, scope) in &classification.probes {
        if *scope == document_scope && matches!(*surface, Surface::Model | Surface::Alias) {
            continue;
        }
        api.authorize(&identity, AdminAction::Publish, *surface, scope)
            .await?;
    }

    if expected.matches(head_id) && expected_diff.is_empty() {
        let checksum = expected_state.checksum()?;
        let Some(revision) = head_id else {
            return Err(AdminError::RequestInvalid {
                schema: SCHEMA,
                detail: "an empty control plane has no revision to leave unchanged".to_owned(),
            });
        };
        return Ok(Json(MutationOutcome {
            result: MutationResult::Unchanged {
                revision: revision.to_string(),
            },
            base: Some(revision.to_string()),
            checksum: checksum.to_string(),
            mode: preconditions.mode.as_str(),
            diff: expected_diff,
        }));
    }

    retain_local_payloads(
        api.catalogue.as_deref(),
        &locals,
        preconditions.mode.is_dry_run(),
    )
    .await?;

    let apply_scope = classification.request_scope.clone();
    let grant = api
        .authorize(
            &identity,
            AdminAction::Publish,
            Surface::Model,
            &apply_scope,
        )
        .await?;

    let request = MutationRequest {
        preconditions,
        kind: plan.mutation,
        surface: Surface::Model,
        scope: apply_scope,
        summary,
    };
    let outcome = api.service.apply(&grant, &request, &edit).await?;
    Ok(Json(outcome))
}

fn document_bytes(
    body: Result<axum::body::Bytes, BytesRejection>,
) -> Result<axum::body::Bytes, AdminError> {
    super::handlers::document(SCHEMA, body)
}

async fn load_active_snapshot(
    catalogue: Option<&dyn CatalogStore>,
) -> Result<Option<CatalogSnapshot>, AdminError> {
    let Some(store) = catalogue else {
        return Ok(None);
    };
    let loaded = store.load().await.map_err(AdminError::from_catalog_store)?;
    let Some(active) = loaded.active else {
        return Ok(None);
    };
    hydrate(&active)
        .map(Some)
        .map_err(AdminError::from_catalog_hydration)
}

type PreparedLocal = ((String, String), RetainedCatalog, CatalogSnapshot);

fn prepare_local_catalogs(
    plan: &BindingPlan,
    imported: Option<&CatalogSnapshot>,
) -> Result<Vec<PreparedLocal>, AdminError> {
    let imported_projection = imported
        .map(PinnedCatalog::of_snapshot)
        .transpose()
        .map_err(|error| {
            refused(
                RULE_CATALOGUE_NOT_IMPORTED,
                &format!("the active catalogue cannot be keyed: {error}"),
            )
        })?;
    let mut locals = Vec::new();
    for target in &plan.targets {
        if target.source != TargetSource::Local {
            continue;
        }
        if let Some(projection) = imported_projection.as_ref()
            && let Ok(provider) = ProviderId::parse(&target.provider_slug)
        {
            let published = target
                .catalog_model
                .as_deref()
                .unwrap_or(target.model.as_str());
            if projection
                .projection()
                .callable(&CallableId::new(provider, published))
                .is_some()
            {
                return Err(refused(
                    RULE_NOT_LOCAL,
                    &format!(
                        "`{}` / `{published}` exists in the imported catalogue",
                        target.provider_slug
                    ),
                ));
            }
        }
        let Some(PriceSpec::Stated {
            input_micros,
            output_micros,
        }) = target.price
        else {
            return Err(refused(
                RULE_PRICE_REQUIRED,
                "source: local requires stated micro-dollar rates",
            ));
        };
        let builder = LocalCatalogBuilder::new(&target.provider_slug, &target.model)
            .price(input_micros, output_micros);
        let retained = builder
            .retained(plan.tenant, SystemTime::now())
            .map_err(|error| malformed("source", &error.to_string()))?;
        let snapshot = hydrate(&retained).map_err(|error| {
            malformed(
                "source",
                &format!("the local catalogue could not be hydrated: {error}"),
            )
        })?;
        locals.push((
            (target.provider_slug.clone(), target.model.clone()),
            retained,
            snapshot,
        ));
    }
    Ok(locals)
}

async fn retain_local_payloads(
    catalogue: Option<&dyn CatalogStore>,
    locals: &[PreparedLocal],
    dry_run: bool,
) -> Result<(), AdminError> {
    if dry_run || locals.is_empty() {
        return Ok(());
    }
    let Some(store) = catalogue else {
        return Err(refused(
            RULE_CATALOGUE_NOT_IMPORTED,
            "no catalogue store is attached",
        ));
    };
    for (_, retained, _) in locals {
        store
            .retain(retained)
            .await
            .map_err(AdminError::from_catalog_store)?;
    }
    Ok(())
}

enum ExpectedBase {
    State(DesiredState),
    Missing,
}

async fn load_expected_base(
    store: &dyn crate::backends::control_plane::ControlPlaneStore,
    expected: ExpectedRevision,
    head: Option<crate::desired_state::RevisionId>,
) -> Result<ExpectedBase, AdminError> {
    match expected {
        ExpectedRevision::Empty => Ok(ExpectedBase::State(DesiredState::new())),
        ExpectedRevision::Exactly(id) => match store.load_revision(id).await {
            Ok(revision) => Ok(ExpectedBase::State(revision.state().clone())),
            Err(crate::backends::control_plane::ControlPlaneError::RevisionNotFound(_))
                if !expected.matches(head) =>
            {
                Ok(ExpectedBase::Missing)
            }
            Err(error) => Err(log_store(error)),
        },
    }
}

struct Classification {
    request_scope: ResourceScope,
    probes: BTreeSet<(Surface, ResourceScope)>,
}

fn classify(
    before: &DesiredState,
    after: &DesiredState,
    document_scope: ResourceScope,
) -> Classification {
    let mut probes = BTreeSet::new();
    let mut request_scope = document_scope;
    for resource in touched(before, after) {
        probes.insert((Surface::of(resource.reference.kind), resource.scope.clone()));
        request_scope = widen_scope(request_scope, &resource.scope);
    }
    Classification {
        request_scope,
        probes,
    }
}

fn widen_scope(current: ResourceScope, touched: &ResourceScope) -> ResourceScope {
    if matches!(current, ResourceScope::Deployment) || matches!(touched, ResourceScope::Deployment)
    {
        return ResourceScope::Deployment;
    }
    if let ResourceScope::Tenant(tenant) = current {
        return ResourceScope::Tenant(tenant);
    }
    if let ResourceScope::Tenant(tenant) = touched {
        return ResourceScope::Tenant(*tenant);
    }
    current
}

fn touched<'a>(
    before: &'a DesiredState,
    after: &'a DesiredState,
) -> impl Iterator<Item = &'a ResourceVersion> {
    before
        .resources()
        .filter(|resource| after.get(&resource.reference) != Some(resource))
        .chain(
            after
                .resources()
                .filter(|resource| before.get(&resource.reference) != Some(resource)),
        )
}

impl BindingPlan {
    fn from_resource(
        resource: BindingResource,
        mutation: MutationKind,
    ) -> Result<Self, AdminError> {
        match resource {
            BindingResource::Many(many) => {
                let [model] = many.models.as_slice() else {
                    return Err(malformed(
                        "models",
                        "this release expands exactly one imported model",
                    ));
                };
                Self::from_parts(
                    &many.tenant,
                    many.project.as_deref(),
                    many.pin.as_deref().or(model.pin.as_deref()),
                    model.name.as_deref(),
                    model.state.as_deref(),
                    &model.targets,
                    mutation,
                )
            }
            BindingResource::One(one) => Self::from_parts(
                &one.tenant,
                one.project.as_deref(),
                one.pin.as_deref(),
                one.name.as_deref(),
                one.state.as_deref(),
                &one.targets,
                mutation,
            ),
        }
    }

    fn from_parts(
        tenant: &str,
        project: Option<&str>,
        pin: Option<&str>,
        name: Option<&str>,
        state: Option<&str>,
        targets: &[BindingTarget],
        mutation: MutationKind,
    ) -> Result<Self, AdminError> {
        if targets.is_empty() {
            return Err(malformed("targets", "must not be empty"));
        }
        let planned = targets
            .iter()
            .map(parse_planned_target)
            .collect::<Result<Vec<_>, _>>()?;
        let tenant =
            TenantId::parse(tenant).map_err(|error| malformed("tenant", &error.to_string()))?;
        let project = project
            .map(ProjectId::parse)
            .transpose()
            .map_err(|error| malformed("project", &error.to_string()))?;
        let pin = match pin {
            None | Some("follow") => PinMode::Follow,
            Some("lock") => PinMode::Lock,
            Some(_) => {
                return Err(malformed(
                    "pin",
                    "is not a value this build knows; it accepts `follow`, `lock`",
                ));
            }
        };
        let lifecycle = match state {
            None | Some("enabled") => ModelLifecycle::Enabled,
            Some("disabled") => ModelLifecycle::Disabled,
            Some(_) => {
                return Err(malformed(
                    "state",
                    "is not a value this build knows; it accepts `enabled`, `disabled`",
                ));
            }
        };
        let first = &planned[0];
        let published = first
            .catalog_model
            .as_deref()
            .unwrap_or(first.model.as_str());
        let name = name.unwrap_or(published);
        if name.is_empty() {
            return Err(malformed("name", "must not be empty"));
        }
        let _ = Slug::parse_alias(name).map_err(|error| malformed("name", &error.to_string()))?;
        Ok(Self {
            tenant,
            project,
            name: name.to_owned(),
            pin,
            lifecycle,
            mutation,
            targets: planned,
        })
    }

    fn document_scope(&self, state: &DesiredState) -> Result<ResourceScope, AdminError> {
        let project = resolve_project(state, self.tenant, self.project)?;
        Ok(ResourceScope::Project {
            tenant: self.tenant,
            project,
        })
    }
}

fn parse_planned_target(target: &BindingTarget) -> Result<PlannedTarget, AdminError> {
    if target.provider.is_empty() {
        return Err(malformed("provider", "must not be empty"));
    }
    if target.model.is_empty() {
        return Err(malformed("model", "must not be empty"));
    }
    let source = match target.source.as_deref() {
        None => TargetSource::Imported,
        Some("local") => TargetSource::Local,
        Some(_) => {
            return Err(malformed(
                "source",
                "is not a value this build knows; it accepts `local`",
            ));
        }
    };
    if source == TargetSource::Local && target.catalog.is_some() {
        return Err(malformed("catalog", "must be omitted when source is local"));
    }
    if source == TargetSource::Local && matches!(target.price, Some(BindingPrice::Observed(_))) {
        return Err(refused(
            RULE_OBSERVED_UNBILLABLE,
            "source: local requires stated rates, not `observed`",
        ));
    }
    let price = match &target.price {
        None => None,
        Some(BindingPrice::Stated {
            input_microdollars_per_million,
            output_microdollars_per_million,
        }) => Some(PriceSpec::Stated {
            input_micros: *input_microdollars_per_million,
            output_micros: *output_microdollars_per_million,
        }),
        Some(BindingPrice::Observed(token)) => {
            if token != "observed" {
                return Err(malformed(
                    "price",
                    "must be a rate object or the token `observed`",
                ));
            }
            Some(PriceSpec::Observed)
        }
    };
    Ok(PlannedTarget {
        provider_slug: target.provider.clone(),
        model: target.model.clone(),
        catalog_provider: target
            .catalog
            .as_ref()
            .and_then(|catalog| catalog.provider.clone()),
        catalog_model: target
            .catalog
            .as_ref()
            .and_then(|catalog| catalog.model.clone()),
        price,
        source,
    })
}

fn expand(
    state: &mut DesiredState,
    actor: &Actor,
    plan: &BindingPlan,
    imported: Option<&CatalogSnapshot>,
    locals: &BTreeMap<(String, String), CatalogSnapshot>,
    now: EffectiveInstant,
) -> Result<(), AdminError> {
    let project = resolve_project(state, plan.tenant, plan.project)?;
    let owner = ModelOwner::project(plan.tenant, project);
    let alias_slug =
        Slug::parse_alias(&plan.name).map_err(|error| malformed("name", &error.to_string()))?;
    if plan.mutation == MutationKind::Create
        && alias_by_name(state, plan.tenant, project, &alias_slug).is_some()
    {
        return Err(AdminError::NameTaken {
            noun: "alias",
            name: plan.name.clone(),
            detail: format!("alias `{}` is already published in this project", plan.name),
        });
    }

    let mut wire_family = None;
    let mut enablements = Vec::new();
    for target in &plan.targets {
        let family = {
            let provider = provider_in_reach(state, owner, &target.provider_slug)?;
            ProviderBody::read(provider)
                .map_err(ValidationError::from)?
                .wire_family()
        };
        match wire_family {
            None => wire_family = Some(family),
            Some(held) if held != family => {
                return Err(malformed(
                    "targets",
                    "must share one wire family across an alias",
                ));
            }
            Some(_) => {}
        }
        let snapshot = match target.source {
            TargetSource::Imported => imported.ok_or_else(|| {
                refused(
                    RULE_CATALOGUE_NOT_IMPORTED,
                    "no imported catalogue is active",
                )
            })?,
            TargetSource::Local => locals
                .get(&(target.provider_slug.clone(), target.model.clone()))
                .ok_or_else(|| malformed("source", "local catalogue was not prepared"))?,
        };
        let catalog_scope = match target.source {
            TargetSource::Imported => ResourceScope::Deployment,
            TargetSource::Local => ResourceScope::Tenant(plan.tenant),
        };
        let write_book = target.source == TargetSource::Imported;
        let enablement = expand_target(
            state,
            actor,
            target,
            snapshot,
            owner,
            catalog_scope,
            write_book,
            family,
            plan.pin,
            plan.lifecycle,
            &alias_slug,
            now,
        )?;
        enablements.push(enablement);
    }
    let wire_family = wire_family.expect("targets is non-empty");
    ensure_alias(
        state,
        plan.tenant,
        project,
        alias_slug,
        wire_family,
        plan.lifecycle,
        &enablements,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn expand_target(
    state: &mut DesiredState,
    actor: &Actor,
    target: &PlannedTarget,
    snapshot: &CatalogSnapshot,
    owner: ModelOwner,
    catalog_scope: ResourceScope,
    write_book: bool,
    wire_family: crate::desired_state::WireFamily,
    pin: PinMode,
    lifecycle: ModelLifecycle,
    alias_slug: &Slug,
    now: EffectiveInstant,
) -> Result<ResourceRef, AdminError> {
    let pinned = PinnedCatalog::of_snapshot(snapshot).map_err(|error| {
        refused(
            RULE_CATALOGUE_NOT_IMPORTED,
            &format!("the catalogue cannot be keyed: {error}"),
        )
    })?;
    let callable = resolve_callable(target, pinned.projection())?;
    let offering_id = OfferingId::of(callable.provider().as_str(), callable.model().as_str())
        .map_err(|error| malformed("model", &error.to_string()))?;
    match pinned.resolve(CatalogOffering::new(
        offering_id,
        snapshot.source.raw.digest,
    )) {
        Resolution::Ambiguous { callables } => {
            let names = callables
                .iter()
                .map(|id| id.published_model_id().to_owned())
                .collect::<Vec<_>>()
                .join(", ");
            return Err(refused(
                RULE_AMBIGUOUS,
                &format!("offering is published as {names}"),
            ));
        }
        Resolution::Callable(_) => {}
        Resolution::Withdrawn | Resolution::OtherSnapshot { .. } => {
            return Err(refused(
                RULE_NOT_IN_CATALOGUE,
                &format!(
                    "`{}` / `{}` is not in the catalogue",
                    callable.provider(),
                    callable.published_model_id()
                ),
            ));
        }
    }

    let digest = snapshot.source.raw.digest;
    let size_bytes = snapshot.source.raw.size_bytes;
    let content_id = snapshot.content.content_id();
    let adopted_enablement = enabled_for(state, owner, offering_id).cloned();
    let catalog = ensure_catalog_model(
        state,
        digest,
        size_bytes,
        adopted_enablement.as_ref(),
        catalog_scope,
    )?;
    let catalog_version = catalog.reference.version;
    let priced = PricedTarget::new(callable.provider().clone(), callable.published_model_id());
    if write_book {
        let rates = resolve_rates(target, callable.price(), state, &priced, now)?;
        if let Some((rates, origin)) = rates {
            ensure_book(
                state,
                actor,
                content_id,
                catalog_version,
                priced,
                rates,
                origin,
                now,
            )?;
        }
    }
    ensure_enablement(
        state,
        owner,
        offering_id,
        digest,
        catalog.reference,
        wire_family,
        pin,
        lifecycle,
        callable.price(),
        alias_slug,
    )
}

fn resolve_callable<'a>(
    target: &PlannedTarget,
    projection: &'a crate::backends::catalog_projection::ModelProjection<'a>,
) -> Result<&'a crate::backends::catalog_projection::CallableOffering<'a>, AdminError> {
    if let Some(catalog_provider) = target.catalog_provider.as_deref()
        && catalog_provider != target.provider_slug
    {
        return Err(refused(
            RULE_CATALOGUE_IDENTITY,
            &format!(
                "catalog.provider `{catalog_provider}` must equal connection slug `{}`",
                target.provider_slug
            ),
        ));
    }
    let published = target
        .catalog_model
        .as_deref()
        .unwrap_or(target.model.as_str());
    let Ok(provider) = ProviderId::parse(&target.provider_slug) else {
        return Err(refused(
            RULE_CATALOGUE_IDENTITY,
            &format!(
                "connection slug `{}` is not a catalogue provider id",
                target.provider_slug
            ),
        ));
    };
    let id = CallableId::new(provider.clone(), published);
    if let Some(callable) = projection.callable(&id) {
        return Ok(callable);
    }
    if target.source == TargetSource::Local {
        return Err(refused(
            RULE_NOT_IN_CATALOGUE,
            &format!("`{provider}` / `{published}` is not in the local catalogue"),
        ));
    }
    let provider_known = projection
        .callables()
        .iter()
        .any(|callable| callable.provider() == &provider);
    if provider_known {
        Err(refused(
            RULE_NOT_IN_CATALOGUE,
            &format!("`{provider}` / `{published}` is not in the active catalogue"),
        ))
    } else {
        Err(refused(
            RULE_CATALOGUE_IDENTITY,
            &format!(
                "connection slug `{}` is not a unique CallableId provider",
                target.provider_slug
            ),
        ))
    }
}

fn resolve_rates(
    planned: &PlannedTarget,
    observed: Option<&crate::backends::catalog::ObservedPrice>,
    state: &DesiredState,
    target: &PricedTarget,
    now: EffectiveInstant,
) -> Result<Option<(ApprovedRates, PriceOrigin)>, AdminError> {
    match &planned.price {
        Some(PriceSpec::Stated {
            input_micros,
            output_micros,
        }) => Ok(Some((
            micros_to_rates(*input_micros, *output_micros)?,
            PriceOrigin::Operator,
        ))),
        Some(PriceSpec::Observed) => {
            let Some(price) = observed else {
                return Err(refused(
                    RULE_OBSERVED_UNBILLABLE,
                    "the catalogue publishes no observed rates for this callable",
                ));
            };
            let rates = ApprovedRates::new(
                ApprovedRate::approving(price.base.input),
                ApprovedRate::approving(price.base.output),
            );
            rates
                .to_model_price()
                .map_err(|error| refused(RULE_OBSERVED_UNBILLABLE, &error.to_string()))?;
            Ok(Some((rates, PriceOrigin::Catalogue)))
        }
        None => {
            if book_covers(state, target, now) {
                Ok(None)
            } else {
                Err(refused(
                    RULE_PRICE_REQUIRED,
                    "price is required unless the deployment book already covers this callable",
                ))
            }
        }
    }
}

fn micros_to_rates(input: u64, output: u64) -> Result<ApprovedRates, AdminError> {
    const PER_MICRO: u64 = 1_000;
    let input_nanos = input.checked_mul(PER_MICRO).ok_or_else(|| {
        malformed(
            "price.input_microdollars_per_million",
            "overflows nano-dollars",
        )
    })?;
    let output_nanos = output.checked_mul(PER_MICRO).ok_or_else(|| {
        malformed(
            "price.output_microdollars_per_million",
            "overflows nano-dollars",
        )
    })?;
    Ok(ApprovedRates::new(
        ApprovedRate::from_nanos(input_nanos),
        ApprovedRate::from_nanos(output_nanos),
    ))
}

fn book_covers(state: &DesiredState, target: &PricedTarget, now: EffectiveInstant) -> bool {
    let Ok(books) = PriceBooks::of(state) else {
        return false;
    };
    books
        .snapshot_at(now)
        .and_then(|snapshot| snapshot.price(&target.provider, &target.published_model_id))
        .is_some()
}

fn ensure_catalog_model(
    state: &mut DesiredState,
    digest: Checksum,
    size_bytes: u64,
    adopted_enablement: Option<&ResourceVersion>,
    scope: ResourceScope,
) -> Result<ResourceVersion, AdminError> {
    let mut matches: Vec<ResourceVersion> = state
        .resources()
        .filter(|resource| {
            resource.reference.kind == ResourceKind::CatalogModel
                && resource.scope == scope
                && resource
                    .body
                    .blob()
                    .is_some_and(|blob| blob.digest == digest)
        })
        .cloned()
        .collect();
    if !matches.is_empty() {
        if let Some(enablement) = adopted_enablement
            && let Some(pinned) = enablement.depends_on.iter().find_map(|reference| {
                matches
                    .iter()
                    .find(|row| row.reference.id == reference.id)
                    .cloned()
            })
        {
            return Ok(pinned);
        }
        matches.sort_by_key(|row| row.reference.id);
        return Ok(matches.into_iter().next().expect("matches is non-empty"));
    }
    let scope_key = match &scope {
        ResourceScope::Deployment => "deployment".to_owned(),
        ResourceScope::Tenant(tenant) => tenant.to_string(),
        ResourceScope::Project { tenant, project } => format!("{tenant}/{project}"),
    };
    let id = derived_resource_id(&[
        ResourceKind::CatalogModel.as_str(),
        &scope_key,
        &digest.to_string(),
    ]);
    let slug = catalog_insert_slug(state, digest, &scope);
    let blob = BlobRef {
        kind: BlobKind::CatalogSnapshot,
        digest,
        size_bytes,
    };
    state.declare_blob(blob);
    let version = resources::next_version(state, ResourceKind::CatalogModel, id);
    resources::publish(
        state,
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::CatalogModel, id, version),
            scope,
            slug,
            crate::desired_state::ResourceBody::Blob(blob),
        ),
    )?;
    state.retain_referenced_blobs();
    Ok(state
        .version_of(ResourceKind::CatalogModel, id)
        .expect("just published")
        .clone())
}

fn unique_enablement_slug(
    state: &DesiredState,
    owner: ModelOwner,
    preferred: &Slug,
    id: ResourceId,
    offering: OfferingId,
    digest: Checksum,
) -> Slug {
    let scope = owner.scope();
    let taken = |candidate: &Slug| {
        state.resources().any(|resource| {
            resource.reference.kind == ResourceKind::ModelEnablement
                && resource.scope == scope
                && resource.slug == *candidate
        })
    };
    if !taken(preferred) {
        return preferred.clone();
    }
    let digest_hex: String = digest
        .as_bytes()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let candidate = format!("{preferred}-{digest_hex}");
    if let Ok(slug) = Slug::parse_alias(&candidate)
        && !taken(&slug)
    {
        return slug;
    }
    let offering_hex = offering
        .to_string()
        .strip_prefix(OfferingId::PREFIX)
        .unwrap_or("")
        .to_owned();
    let id_hex: String = id
        .uuid()
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    for width in [8usize, 16, 32] {
        let mix = format!(
            "{}{}",
            &offering_hex[..width.min(offering_hex.len())],
            &id_hex[..width.min(id_hex.len())]
        );
        if let Ok(slug) = Slug::parse_alias(&mix)
            && !taken(&slug)
        {
            return slug;
        }
    }
    (0u32..)
        .find_map(|salt| {
            let candidate = format!("{id_hex}-{salt:x}");
            Slug::parse_alias(&candidate)
                .ok()
                .filter(|slug| !taken(slug))
        })
        .expect("uuid hex with a salt is a slug")
}

fn catalog_insert_slug(state: &DesiredState, digest: Checksum, scope: &ResourceScope) -> Slug {
    let preferred = if matches!(scope, ResourceScope::Deployment) {
        Slug::parse("imported").expect("imported is a slug")
    } else {
        Slug::parse("local").expect("local is a slug")
    };
    let taken = state.resources().any(|resource| {
        resource.reference.kind == ResourceKind::CatalogModel
            && resource.scope == *scope
            && resource.slug == preferred
    });
    if !taken {
        return preferred;
    }
    let hex: String = digest
        .as_bytes()
        .iter()
        .take(16)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Slug::parse(&hex).expect("hex is a slug")
}

#[allow(clippy::too_many_arguments)]
fn ensure_book(
    state: &mut DesiredState,
    actor: &Actor,
    catalog: crate::backends::catalog::CatalogContentId,
    catalog_version: ResourceVersionNumber,
    target: PricedTarget,
    rates: ApprovedRates,
    origin: PriceOrigin,
    at: EffectiveInstant,
) -> Result<(), AdminError> {
    let books = PriceBooks::of(state).map_err(|error| ValidationError::Pricing(Box::new(error)))?;
    let Some(existing) = books.book() else {
        let rule = baseline_rule(
            target,
            EffectiveInterval::from(EffectiveInstant::EPOCH),
            rates,
            origin,
        )?;
        let id = derived_resource_id(&[ResourceKind::Price.as_str(), "deployment", "approved"]);
        let slug = Slug::parse("approved").expect("approved is a slug");
        // Open interval from epoch so a lost-response retry rebuilds the same book.
        let body = PriceBookBody::new(
            catalog,
            catalog_version,
            Approval::Approved {
                by: actor.clone(),
                at: EffectiveInstant::EPOCH,
                citation: None,
            },
        )
        .with_rule(rule);
        let version = resources::next_version(state, ResourceKind::Price, id);
        resources::publish(state, body.version_at(id, slug, version))?;
        return Ok(());
    };

    let current = existing.body.rules();
    let in_force = current.iter().find(|rule| {
        rule.target() == &target
            && rule.precedence() == RulePrecedence::Baseline
            && rule.effective().contains(at)
    });
    if in_force.is_some_and(|rule| rule.rates() == rates) {
        return Ok(());
    }
    if matches!(existing.body.approval(), Approval::Draft) {
        return Err(refused(
            RULE_DRAFT_BOOK_NOT_APPROVED_BY_BINDING,
            "a binding does not approve a draft deployment price book; approve it on /admin/v1/prices first",
        ));
    }
    let has_history = current
        .iter()
        .any(|rule| rule.target() == &target && rule.precedence() == RulePrecedence::Baseline);
    // Greenfield coverage stays from epoch (retry identity). A rate change
    // closes the predecessor at `at` so consecutive baselines meet and the
    // existing pricing-boundary timer can arm from `PricingSnapshot::effective`.
    // A later binding that only appends a rule keeps the original approval.
    let (from, dated) = if in_force.is_some() || has_history {
        (at, true)
    } else {
        (EffectiveInstant::EPOCH, false)
    };
    let rules = replace_baseline_from(current, &target, rates, origin, from)?;
    let approval = if dated {
        Approval::Approved {
            by: actor.clone(),
            at: from,
            citation: None,
        }
    } else {
        existing.body.approval().clone()
    };
    let body = rules.into_iter().fold(
        PriceBookBody::new(catalog, catalog_version, approval),
        PriceBookBody::with_rule,
    );
    let version = resources::next_version(state, ResourceKind::Price, existing.reference.id);
    resources::publish(
        state,
        body.version_at(existing.reference.id, existing.slug.clone(), version),
    )?;
    Ok(())
}

fn baseline_rule(
    target: PricedTarget,
    effective: EffectiveInterval,
    rates: ApprovedRates,
    origin: PriceOrigin,
) -> Result<PriceRule, AdminError> {
    PriceRule::new(
        target,
        RulePrecedence::Baseline,
        effective,
        rates,
        PriceProvenance::stated(origin),
    )
    .map_err(|error| refused(RULE_OBSERVED_UNBILLABLE, &error.to_string()))
}

fn replace_baseline_from(
    current: &[PriceRule],
    target: &PricedTarget,
    rates: ApprovedRates,
    origin: PriceOrigin,
    from: EffectiveInstant,
) -> Result<Vec<PriceRule>, AdminError> {
    let mut next = Vec::with_capacity(current.len() + 1);
    for rule in current {
        if rule.target() != target || rule.precedence() != RulePrecedence::Baseline {
            next.push(rule.clone());
            continue;
        }
        if rule.effective().contains(from) {
            if rule.effective().starts() < from {
                let closed = EffectiveInterval::bounded(rule.effective().starts(), from)
                    .map_err(|error| malformed("effective_from", &error.to_string()))?;
                next.push(
                    PriceRule::new(
                        rule.target().clone(),
                        rule.precedence(),
                        closed,
                        rule.rates(),
                        rule.provenance().clone(),
                    )
                    .map_err(|error| refused(RULE_OBSERVED_UNBILLABLE, &error.to_string()))?,
                );
            }
            continue;
        }
        if rule.effective().starts() >= from {
            continue;
        }
        next.push(rule.clone());
    }
    next.push(baseline_rule(
        target.clone(),
        EffectiveInterval::from(from),
        rates,
        origin,
    )?);
    Ok(next)
}

#[allow(clippy::too_many_arguments)]
fn ensure_enablement(
    state: &mut DesiredState,
    owner: ModelOwner,
    offering: OfferingId,
    digest: Checksum,
    catalog: ResourceRef,
    wire_family: crate::desired_state::WireFamily,
    pin: PinMode,
    lifecycle: ModelLifecycle,
    observed: Option<&crate::backends::catalog::ObservedPrice>,
    slug: &Slug,
) -> Result<ResourceRef, AdminError> {
    let existing = enabled_for(state, owner, offering).cloned();
    if let Some(held) = existing {
        let body = ModelEnablementBody::read(&held).map_err(ValidationError::from)?;
        if body.offering().snapshot == digest {
            if body.state() == lifecycle && body.billable_price().is_none() {
                return Ok(held.reference);
            }
            let mut next =
                ModelEnablementBody::new(body.enablement(), owner, body.offering(), wire_family)
                    .transitioned(lifecycle);
            if let Some(observed) = observed_micros(observed) {
                next = next.observing(observed);
            }
            let version =
                resources::next_version(state, ResourceKind::ModelEnablement, held.reference.id);
            resources::publish(state, next.version_at(held.slug.clone(), version, catalog))?;
            return Ok(state
                .version_of(ResourceKind::ModelEnablement, held.reference.id)
                .expect("just published")
                .reference);
        }
        if pin == PinMode::Lock {
            return Err(refused(
                RULE_PIN_LOCKED,
                "pin=lock and the active catalogue digest has moved",
            ));
        }
        let disabled =
            ModelEnablementBody::new(body.enablement(), owner, body.offering(), wire_family)
                .transitioned(ModelLifecycle::Disabled);
        let version =
            resources::next_version(state, ResourceKind::ModelEnablement, held.reference.id);
        let old_catalog = held
            .depends_on
            .iter()
            .find(|reference| reference.kind == ResourceKind::CatalogModel)
            .copied()
            .unwrap_or(catalog);
        resources::publish(
            state,
            disabled.version_at(held.slug.clone(), version, old_catalog),
        )?;
    }

    let id = derived_resource_id(&[
        ResourceKind::ModelEnablement.as_str(),
        &owner.tenant.to_string(),
        &owner.project.map(|id| id.to_string()).unwrap_or_default(),
        &offering.to_string(),
        &digest.to_string(),
    ]);
    let mut body = ModelEnablementBody::new(
        id,
        owner,
        CatalogOffering::new(offering, digest),
        wire_family,
    )
    .transitioned(lifecycle);
    if let Some(observed) = observed_micros(observed) {
        body = body.observing(observed);
    }
    let version = resources::next_version(state, ResourceKind::ModelEnablement, id);
    let insert_slug = unique_enablement_slug(state, owner, slug, id, offering, digest);
    resources::publish(state, body.version_at(insert_slug, version, catalog))?;
    Ok(state
        .version_of(ResourceKind::ModelEnablement, id)
        .expect("just published")
        .reference)
}

fn ensure_alias(
    state: &mut DesiredState,
    tenant: TenantId,
    project: ProjectId,
    slug: Slug,
    wire_family: crate::desired_state::WireFamily,
    lifecycle: ModelLifecycle,
    enablements: &[ResourceRef],
) -> Result<(), AdminError> {
    let targets: Vec<AliasTarget> = enablements
        .iter()
        .map(|enablement| AliasTarget::new(enablement.id, enablement.version))
        .collect();
    if let Some(held) = alias_by_name(state, tenant, project, &slug).cloned() {
        let body = ModelAliasBody::read(&held).map_err(ValidationError::from)?;
        if body.targets() == targets.as_slice()
            && body.state() == lifecycle
            && body.wire_family() == wire_family
        {
            return Ok(());
        }
        let next = ModelAliasBody::new(body.alias(), tenant, project, wire_family, targets)
            .transitioned(lifecycle);
        let version = resources::next_version(state, ResourceKind::Alias, held.reference.id);
        resources::publish(state, next.version_at(held.slug.clone(), version))?;
        return Ok(());
    }
    let id = derived_resource_id(&[
        ResourceKind::Alias.as_str(),
        &tenant.to_string(),
        &project.to_string(),
        slug.as_str(),
    ]);
    let body =
        ModelAliasBody::new(id, tenant, project, wire_family, targets).transitioned(lifecycle);
    let version = resources::next_version(state, ResourceKind::Alias, id);
    resources::publish(state, body.version_at(slug, version))?;
    Ok(())
}

fn enabled_for(
    state: &DesiredState,
    owner: ModelOwner,
    offering: OfferingId,
) -> Option<&ResourceVersion> {
    state.resources().find(|resource| {
        if resource.reference.kind != ResourceKind::ModelEnablement {
            return false;
        }
        ModelEnablementBody::read(resource).is_ok_and(|body| {
            body.is_enabled() && body.owner() == owner && body.offering().offering == offering
        })
    })
}

fn alias_by_name<'a>(
    state: &'a DesiredState,
    tenant: TenantId,
    project: ProjectId,
    slug: &Slug,
) -> Option<&'a ResourceVersion> {
    let scope = ResourceScope::Project { tenant, project };
    state.resources().find(|resource| {
        resource.reference.kind == ResourceKind::Alias
            && resource.scope == scope
            && resource.slug == *slug
    })
}

fn provider_in_reach<'a>(
    state: &'a DesiredState,
    owner: ModelOwner,
    slug: &str,
) -> Result<&'a ResourceVersion, AdminError> {
    let wanted = Slug::parse(slug).map_err(|_| {
        refused(
            RULE_UNKNOWN_PROVIDER,
            &format!("`{slug}` is not a published provider in reach"),
        )
    })?;
    let mut found: Vec<&ResourceVersion> = state
        .resources()
        .filter(|resource| {
            resource.reference.kind == ResourceKind::Provider
                && resource.slug == wanted
                && ModelOwner::from_scope(&resource.scope).is_some_and(|other| owner.reaches(other))
        })
        .collect();
    found.sort_by_key(|resource| match resource.scope {
        ResourceScope::Project { .. } => 0u8,
        ResourceScope::Tenant(_) => 1,
        ResourceScope::Deployment => 2,
    });
    found.into_iter().next().ok_or_else(|| {
        refused(
            RULE_UNKNOWN_PROVIDER,
            &format!("`{slug}` is not a published provider in reach"),
        )
    })
}

fn resolve_project(
    state: &DesiredState,
    tenant: TenantId,
    project: Option<ProjectId>,
) -> Result<ProjectId, AdminError> {
    if let Some(project) = project {
        return Ok(project);
    }
    let mut projects: Vec<ProjectId> = state
        .resources()
        .filter(|resource| resource.reference.kind == ResourceKind::Project)
        .filter_map(|resource| ProjectBody::read(resource).ok())
        .filter(|body| body.tenant() == tenant)
        .map(|body| body.project())
        .collect();
    projects.sort();
    match projects.as_slice() {
        [only] => Ok(*only),
        _ => Err(refused(
            RULE_PROJECT_REQUIRED,
            "project is required unless the tenant has exactly one project",
        )),
    }
}

fn observed_micros(
    price: Option<&crate::backends::catalog::ObservedPrice>,
) -> Option<ObservedPrice> {
    let price = price?;
    const PER_MICRO: u64 = 1_000;
    let input = price.base.input.nanos();
    let output = price.base.output.nanos();
    if !input.is_multiple_of(PER_MICRO) || !output.is_multiple_of(PER_MICRO) {
        return None;
    }
    Some(ObservedPrice::new(input / PER_MICRO, output / PER_MICRO))
}

fn derived_resource_id(parts: &[&str]) -> ResourceId {
    let material = parts.join("\0");
    let digest = Checksum::of(material.as_bytes());
    let bytes = digest.as_bytes();
    let mut millis_bytes = [0u8; 8];
    millis_bytes[2..8].copy_from_slice(&bytes[0..6]);
    let millis = u64::from_be_bytes(millis_bytes);
    let sequence = u16::from_be_bytes([bytes[6], bytes[7]]) & 0x0fff;
    let mut entropy = [0u8; 8];
    entropy.copy_from_slice(&bytes[8..16]);
    ResourceId::new(
        Uuid7::from_parts(millis, sequence, u64::from_be_bytes(entropy))
            .expect("derived parts fit a uuid v7"),
    )
}

fn refused(rule: &'static str, detail: &str) -> AdminError {
    AdminError::BindingRefused {
        rule,
        detail: detail.to_owned(),
    }
}

fn malformed(field: &str, detail: &str) -> AdminError {
    AdminError::RequestInvalid {
        schema: SCHEMA,
        detail: format!("`{field}`: {detail}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::fixtures;
    use crate::desired_state::{BlobKind, ResourceBody, ResourceKind, ResourceScope};

    #[test]
    fn catalog_model_digest_collision_picks_enablement_pin_then_lowest_id() {
        let digest = Checksum::of(b"shared-catalogue");
        let blob = BlobRef {
            kind: BlobKind::CatalogSnapshot,
            digest,
            size_bytes: 4,
        };
        let high = fixtures::resource_id(90);
        let low = fixtures::resource_id(5);
        let high_row = ResourceVersion::new(
            ResourceRef::new(
                ResourceKind::CatalogModel,
                high,
                ResourceVersionNumber::FIRST,
            ),
            ResourceScope::Deployment,
            Slug::parse("high").unwrap(),
            ResourceBody::Blob(blob),
        );
        let low_row = ResourceVersion::new(
            ResourceRef::new(
                ResourceKind::CatalogModel,
                low,
                ResourceVersionNumber::FIRST,
            ),
            ResourceScope::Deployment,
            Slug::parse("low").unwrap(),
            ResourceBody::Blob(blob),
        );
        let owner = ModelOwner::project(fixtures::tenant_id(1), fixtures::project_id(2));
        let offering = fixtures::offering_id("gpt-4o");
        let enablement = ModelEnablementBody::new(
            fixtures::resource_id(14),
            owner,
            CatalogOffering::new(offering, digest),
            crate::desired_state::WireFamily::OpenaiChat,
        )
        .version(Slug::parse("gpt-4o").unwrap(), high_row.reference);

        let mut state = DesiredState::new();
        state.declare_blob(blob);
        state
            .insert(high_row.clone())
            .and_then(|state| state.insert(low_row.clone()))
            .and_then(|state| state.insert(enablement.clone()))
            .expect("two catalog rows of one digest are valid");

        let adopted = enabled_for(&state, owner, offering).cloned();
        let picked = ensure_catalog_model(
            &mut state,
            digest,
            4,
            adopted.as_ref(),
            ResourceScope::Deployment,
        )
        .expect("adopts");
        assert_eq!(picked.reference.id, high, "enablement pin wins");

        let mut without = DesiredState::new();
        without.declare_blob(blob);
        without
            .insert(high_row)
            .and_then(|state| state.insert(low_row))
            .expect("two rows");
        let picked = ensure_catalog_model(&mut without, digest, 4, None, ResourceScope::Deployment)
            .expect("lowest");
        assert_eq!(picked.reference.id, low);
    }

    fn occupy_enablement(state: &mut DesiredState, owner: ModelOwner, seed: u64, slug: Slug) {
        state
            .insert(ResourceVersion::new(
                ResourceRef::new(
                    ResourceKind::ModelEnablement,
                    fixtures::resource_id(seed),
                    ResourceVersionNumber::FIRST,
                ),
                owner.scope(),
                slug,
                ResourceBody::Inline(crate::desired_state::CanonicalValue::Bool(true)),
            ))
            .expect("distinct enablement");
    }

    #[test]
    fn unique_dotted_enablement_slugs_do_not_collapse_to_the_catalogue_digest() {
        let owner = ModelOwner::project(fixtures::tenant_id(1), fixtures::project_id(2));
        let digest = Checksum::of(b"shared-catalogue");
        let gpt = Slug::parse_alias("gpt-5.5").unwrap();
        let claude = Slug::parse_alias("claude-3.5-sonnet").unwrap();
        let mut state = DesiredState::new();
        occupy_enablement(&mut state, owner, 1, gpt.clone());
        occupy_enablement(&mut state, owner, 2, claude.clone());
        let first = unique_enablement_slug(
            &state,
            owner,
            &gpt,
            fixtures::resource_id(101),
            fixtures::offering_id("openai/gpt-5.5"),
            digest,
        );
        occupy_enablement(&mut state, owner, 101, first.clone());
        let second = unique_enablement_slug(
            &state,
            owner,
            &claude,
            fixtures::resource_id(102),
            fixtures::offering_id("openai/claude-3.5-sonnet"),
            digest,
        );
        let digest_only: String = digest
            .as_bytes()
            .iter()
            .take(8)
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_ne!(first, second);
        assert_ne!(first.as_str(), digest_only);
        assert_ne!(second.as_str(), digest_only);
        assert!(first.as_str().starts_with("gpt-5.5-"));
        assert!(second.as_str().starts_with("claude-3.5-sonnet-"));
    }

    #[test]
    fn a_too_long_dotted_suffix_falls_back_to_offering_and_id() {
        let owner = ModelOwner::project(fixtures::tenant_id(1), fixtures::project_id(2));
        let digest = Checksum::of(b"shared-catalogue");
        let long = Slug::parse_alias(&format!("a.{}", "b".repeat(Slug::MAX_LEN - 2))).unwrap();
        let mut state = DesiredState::new();
        occupy_enablement(&mut state, owner, 1, long.clone());
        let slug = unique_enablement_slug(
            &state,
            owner,
            &long,
            fixtures::resource_id(101),
            fixtures::offering_id("openai/gpt-5.5"),
            digest,
        );
        assert_ne!(slug, long);
        assert!(
            slug.as_str()
                .chars()
                .all(|character| character.is_ascii_hexdigit()),
            "length overflow must not reuse the catalogue digest hex, got {}",
            slug.as_str()
        );
        occupy_enablement(&mut state, owner, 101, slug.clone());
        let other = unique_enablement_slug(
            &state,
            owner,
            &long,
            fixtures::resource_id(102),
            fixtures::offering_id("openai/claude-3.5-sonnet"),
            digest,
        );
        assert_ne!(slug, other);
    }

    fn observed_target(source: Option<&str>) -> BindingTarget {
        BindingTarget {
            provider: "vllm".into(),
            model: "llama".into(),
            catalog: None,
            price: Some(BindingPrice::Observed("observed".into())),
            source: source.map(str::to_owned),
        }
    }

    #[test]
    fn parse_time_path_is_local_when_source_local_even_if_from_resource_refuses() {
        let one = BindingResource::One(BindingOne {
            tenant: "ten".into(),
            project: None,
            pin: None,
            name: None,
            state: None,
            targets: vec![observed_target(Some("local"))],
        });
        assert_eq!(one.path(), "local");
        match BindingPlan::from_resource(one, MutationKind::Create) {
            Err(AdminError::BindingRefused { rule, .. }) => {
                assert_eq!(rule, RULE_OBSERVED_UNBILLABLE);
            }
            other => panic!("expected observed_unbillable, got {other:?}"),
        }

        let many = BindingResource::Many(BindingMany {
            tenant: "ten".into(),
            project: None,
            pin: None,
            models: vec![BindingModel {
                name: None,
                state: None,
                pin: None,
                targets: vec![observed_target(Some("local"))],
            }],
        });
        assert_eq!(many.path(), "local");

        let imported = BindingResource::One(BindingOne {
            tenant: "ten".into(),
            project: None,
            pin: None,
            name: None,
            state: None,
            targets: vec![observed_target(None)],
        });
        assert_eq!(imported.path(), "imported");
    }
}
