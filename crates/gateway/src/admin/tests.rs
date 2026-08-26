//! Contract tests for the administrative boundary.
//!
//! Everything here runs against the in-memory store oracle and the fake
//! authorities, and every test states one of the properties this slice exists to
//! make structural rather than documented: stateless mode answers without a
//! backend, inference credentials carry no administrative authority, a mutation
//! cannot skip its preconditions, a dry run leaves nothing behind, and no
//! projection carries a secret.
//!
//! The router tests mount a synthetic route table, because
//! [`admin_route_specs`](super::router::admin_route_specs) is empty until #143:
//! the middleware's behaviour is asserted against a real request rather than
//! deferred until a handler exists to assert it with.

use std::sync::Arc;

use axum::Json;
use axum::body::Body;
use axum::extract::{Extension, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::routing::{get, post};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use super::auth::{
    AdminAction, AdminAuthError, AdminAuthenticator, AdminAuthorizer, AdminCredential, AdminGrant,
    AdminIdentity, AdminPresented, BREAKGLASS_OPERATOR_HEADER, BREAKGLASS_REASON_HEADER,
    BreakglassAttribution, INFERENCE_KEY_HEADER, InvalidAttribution, PresentedAttribution,
};
use super::diff::SemanticDiff;
use super::error::AdminError;
use super::fakes::{CountingStore, FakeAdminAuthenticator, FakeAdminAuthorizer, FlakyStore};
use super::protocol::{
    ADMIN_PREFIX, AuditSummary, DRY_RUN_HEADER, EXPECTED_REVISION_HEADER, IDEMPOTENCY_KEY_HEADER,
    MutationPreconditions, MutationRequest, WriteMode,
};
use super::reads::{HistoryLimit, HistoryRequest, StateView};
use super::router::{AdminApi, AdminRouteSpec, mount};
use super::service::{AdminService, MutationResult};
use crate::backends::secrets::SecretError;
use crate::config::Mode;
use crate::desired_state::oracle::InMemoryControlPlane;
use crate::desired_state::{
    CanonicalValue, DenialPage, DesiredState, ExpectedRevision, IdempotencyKey, MutationKind,
    ResourceBody, ResourceKind, ResourceScope, ResourceVersion, Slug, Surface, ValidationError,
    fixtures,
};

const HUMAN_TOKEN: &str = "oidc-assertion-for-alice";
const BREAKGLASS_SECRET: &str = "breakglass-static-secret";
const ISSUER: &str = "https://idp.example";
const SUBJECT: &str = "alice";
/// A value shaped like a credential, so a redaction assertion has something
/// unmistakable to look for.
const SECRET_LOOKING: &str = "sk-live-51H9xNEVERLOGME";

/// The fixture state contains tenants and a catalogue, which are
/// deployment-scoped, so the grant that publishes it is too.
fn scope() -> ResourceScope {
    ResourceScope::Deployment
}

fn authenticator() -> Arc<FakeAdminAuthenticator> {
    Arc::new(
        FakeAdminAuthenticator::new()
            .with_human(HUMAN_TOKEN, ISSUER, SUBJECT)
            .with_breakglass(BREAKGLASS_SECRET, "primary-breakglass"),
    )
}

fn human() -> AdminIdentity {
    AdminIdentity::Human {
        issuer: ISSUER.to_owned(),
        subject: SUBJECT.to_owned(),
    }
}

fn breakglass() -> AdminIdentity {
    AdminIdentity::Breakglass {
        attribution: BreakglassAttribution::parse("ops-oncall", "idp outage, ticket OPS-42")
            .expect("a valid attribution"),
        credential: "primary-breakglass".to_owned(),
    }
}

fn grant(action: AdminAction) -> AdminGrant {
    AdminGrant::granted(human(), action, scope())
}

fn request(key: &str, expected: ExpectedRevision, mode: WriteMode) -> MutationRequest {
    MutationRequest {
        preconditions: MutationPreconditions {
            expected,
            idempotency_key: IdempotencyKey::parse(key).expect("a valid key"),
            mode,
        },
        kind: MutationKind::Update,
        surface: Surface::Tenant,
        scope: scope(),
        summary: AuditSummary::parse("publish the fixture state").expect("a valid summary"),
    }
}

/// An edit that replaces the state wholesale — how a resource handler that
/// rewrites everything it owns behaves, without any resource schema being
/// involved.
fn replace_with(state: DesiredState) -> impl super::service::DesiredStateEdit {
    move |target: &mut DesiredState| {
        *target = state.clone();
        Ok(())
    }
}

fn service(store: &Arc<InMemoryControlPlane>) -> AdminService {
    AdminService::stateful(store.clone())
}

/// Publish the fixture state and return the revision it became.
async fn publish_fixture(service: &AdminService, key: &str) -> String {
    let outcome = service
        .apply(
            &grant(AdminAction::Publish),
            &request(key, ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state()),
        )
        .await
        .expect("the first publication");
    outcome.revision().expect("a published revision").to_owned()
}

// ---------------------------------------------------------------------------
// Stateless mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stateless_mode_is_typed_and_never_touches_a_control_plane_backend() {
    let inner = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(inner.clone()));
    // A store is available, and stateless mode still refuses to hold one: this is
    // the property, not the ordering of two checks inside a method.
    let service = AdminService::for_mode(Mode::Stateless, Some(counting.clone()));
    assert_eq!(service.mode(), Mode::Stateless);

    let refusals = vec![
        service
            .apply(
                &grant(AdminAction::Publish),
                &request("key-1", ExpectedRevision::Empty, WriteMode::Apply),
                &replace_with(fixtures::state()),
            )
            .await
            .expect_err("a stateless deployment administers nothing"),
        service
            .desired_state(&grant(AdminAction::ReadState))
            .await
            .expect_err("a stateless deployment has no desired state"),
        service
            .history(&grant(AdminAction::ReadHistory), HistoryRequest::default())
            .await
            .expect_err("a stateless deployment has no history"),
        service
            .audit(&grant(AdminAction::ReadAudit), fixtures::revision_id(1))
            .await
            .expect_err("a stateless deployment has no audit trail"),
    ];
    for refusal in refusals {
        assert_eq!(refusal.code(), "stateful_mode_required");
        assert_eq!(refusal.status(), StatusCode::NOT_IMPLEMENTED);
        assert!(!refusal.retryable());
    }
    assert_eq!(
        counting.calls(),
        0,
        "stateless mode consulted a control-plane backend"
    );
    assert_eq!(inner.published_revisions(), 0);
}

#[tokio::test]
async fn a_dry_run_in_stateless_mode_is_refused_before_any_validation() {
    let inner = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(inner));
    let service = AdminService::for_mode(Mode::Stateless, Some(counting.clone()));
    let error = service
        .apply(
            &grant(AdminAction::Publish),
            &request("key-1", ExpectedRevision::Empty, WriteMode::DryRun),
            // An edit that would fail validation: the mode refusal must come
            // first, so a stateless deployment answers the same way whatever the
            // request would have done.
            &(|_: &mut DesiredState| Err(ValidationError::Empty)),
        )
        .await
        .expect_err("stateless mode refuses a dry run too");
    assert_eq!(error.code(), "stateful_mode_required");
    assert_eq!(counting.calls(), 0);
}

// ---------------------------------------------------------------------------
// Identity: inference credentials are not administrative ones
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_inference_credential_carries_no_administrative_authority() {
    let authenticator = authenticator();
    // A minted inference token, and an inference API key: both are refused as
    // inference credentials rather than looked up in the administrative
    // population.
    for headers in [
        vec![(
            axum::http::header::AUTHORIZATION.as_str(),
            "Bearer axt1.eyJhbGciOiJFZERTQSJ9.payload.signature",
        )],
        vec![(INFERENCE_KEY_HEADER, "sk-gateway-inference-key")],
    ] {
        let mut map = axum::http::HeaderMap::new();
        for (name, value) in headers {
            map.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        let error = AdminPresented::from_headers(&map)
            .expect_err("an inference credential is not an administrative one");
        assert_eq!(error, AdminAuthError::InferenceCredential);
        let error = AdminError::from(error);
        assert_eq!(error.code(), "admin_unauthenticated");
        assert_eq!(error.status(), StatusCode::UNAUTHORIZED);
    }

    // And a static gateway key, which is not prefixed and so reaches the
    // authenticator, is simply not in the administrative population.
    let presented = AdminPresented {
        credential: AdminCredential::new("gateway-static-key"),
        attribution: PresentedAttribution::Absent,
    };
    assert_eq!(
        authenticator.authenticate(&presented).await,
        Err(AdminAuthError::UnknownCredential)
    );
}

#[tokio::test]
async fn an_authorized_oidc_human_publishes_and_is_recorded_by_issuer_scoped_subject() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);
    let revision = publish_fixture(&service, "key-human").await;

    let page = service
        .audit(
            &grant(AdminAction::ReadAudit),
            crate::desired_state::RevisionId::parse(&revision).expect("a revision id"),
        )
        .await
        .expect("an audit trail");
    let event = page.events.first().expect("one audit event");
    assert_eq!(event.actor.kind, "human");
    assert_eq!(event.actor.issuer.as_deref(), Some(ISSUER));
    assert_eq!(event.actor.subject.as_deref(), Some(SUBJECT));
    assert_eq!(event.summary, "publish the fixture state");
    assert!(!page.truncated);
}

#[tokio::test]
async fn breakglass_use_is_attributed_and_kept_distinguishable_from_a_human() {
    // Unattributed breakglass is refused rather than published as "someone".
    let authenticator = authenticator();
    let unattributed = AdminPresented {
        credential: AdminCredential::new(BREAKGLASS_SECRET),
        attribution: PresentedAttribution::Absent,
    };
    let error = authenticator
        .authenticate(&unattributed)
        .await
        .expect_err("breakglass must name an operator and a reason");
    assert_eq!(AdminError::from(error).code(), "admin_unauthenticated");

    // Attributed, it authenticates — even while the identity provider is down,
    // which is what it exists for.
    authenticator.set_unavailable(true);
    let identity = authenticator
        .authenticate(&AdminPresented {
            credential: AdminCredential::new(BREAKGLASS_SECRET),
            attribution: PresentedAttribution::Present(
                BreakglassAttribution::parse("ops-oncall", "idp outage, ticket OPS-42")
                    .expect("a valid attribution"),
            ),
        })
        .await
        .expect("breakglass works during an identity-provider outage");
    assert_eq!(identity, breakglass());

    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);
    let outcome = service
        .apply(
            &AdminGrant::granted(breakglass(), AdminAction::Publish, scope()),
            &request("key-breakglass", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state()),
        )
        .await
        .expect("an attributed breakglass publication");
    let revision =
        crate::desired_state::RevisionId::parse(outcome.revision().expect("a revision")).unwrap();
    let page = service
        .audit(&grant(AdminAction::ReadAudit), revision)
        .await
        .expect("an audit trail");
    let event = page.events.first().expect("one audit event");
    // Still `breakglass`, not disguised as the human it names: that distinction
    // is what an auditor filters on.
    assert_eq!(event.actor.kind, "breakglass");
    assert!(event.actor.subject.is_none());
    assert!(
        event.summary.contains("ops-oncall") && event.summary.contains("OPS-42"),
        "the attribution must reach the audit trail: {}",
        event.summary
    );
}

#[tokio::test]
async fn an_identity_provider_outage_is_not_a_rejection() {
    let authenticator = authenticator();
    authenticator.set_unavailable(true);
    let error = authenticator
        .authenticate(&AdminPresented {
            credential: AdminCredential::new(HUMAN_TOKEN),
            attribution: PresentedAttribution::Absent,
        })
        .await
        .expect_err("the identity provider is down");
    let error = AdminError::from(error);
    assert_eq!(error.code(), "identity_provider_unavailable");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(error.retryable());
}

#[test]
fn an_unpermitted_action_or_scope_is_forbidden_rather_than_unauthenticated() {
    let authorizer = FakeAdminAuthorizer::permitting(&[AdminAction::ReadState]).within(&[scope()]);
    let error = AdminError::from(
        authorizer
            .authorize(&human(), AdminAction::Publish, Surface::Tenant, &scope())
            .expect_err("a read-only identity may not publish"),
    );
    assert_eq!(error.code(), "admin_forbidden");
    assert_eq!(error.status(), StatusCode::FORBIDDEN);

    let elsewhere = ResourceScope::Tenant(fixtures::tenant_id(11));
    let error = AdminError::from(
        FakeAdminAuthorizer::permissive()
            .within(&[scope()])
            .authorize(
                &human(),
                AdminAction::ReadState,
                Surface::Tenant,
                &elsewhere,
            )
            .expect_err("that scope is not this identity's"),
    );
    assert_eq!(error.code(), "admin_forbidden");
}

#[tokio::test]
async fn a_grant_cannot_be_spent_on_another_action_or_another_scope() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);

    // A read grant cannot publish.
    let error = service
        .apply(
            &grant(AdminAction::ReadState),
            &request("key-1", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state()),
        )
        .await
        .expect_err("a read grant is not a write grant");
    assert_eq!(error.code(), "admin_forbidden");

    // Nor can a grant for one mutating verb be spent on another: rollback
    // authority is not publication authority.
    let error = service
        .apply(
            &grant(AdminAction::Rollback),
            &request("key-1", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state()),
        )
        .await
        .expect_err("a rollback grant does not author new state");
    assert_eq!(error.code(), "admin_forbidden");

    let mut rollback = request("key-1", ExpectedRevision::Empty, WriteMode::Apply);
    rollback.kind = MutationKind::Rollback;
    let error = service
        .apply(
            &grant(AdminAction::Publish),
            &rollback,
            &replace_with(fixtures::state()),
        )
        .await
        .expect_err("a publication grant does not republish an earlier revision");
    assert_eq!(error.code(), "admin_forbidden");
    assert_eq!(store.published_revisions(), 0);

    // A grant for one tenant cannot publish a mutation attributed to another.
    let mut elsewhere = request("key-2", ExpectedRevision::Empty, WriteMode::Apply);
    elsewhere.scope = ResourceScope::Tenant(fixtures::tenant_id(11));
    let error = service
        .apply(
            &grant(AdminAction::Publish),
            &elsewhere,
            &replace_with(fixtures::state()),
        )
        .await
        .expect_err("a grant is scoped");
    assert_eq!(error.code(), "admin_forbidden");
    assert_eq!(store.published_revisions(), 0);
}

#[tokio::test]
async fn a_refusal_of_authority_is_written_to_the_denial_trail() {
    let store = Arc::new(InMemoryControlPlane::new());
    let tenant = fixtures::tenant_id(1);
    let scoped = ResourceScope::Tenant(tenant);

    // (a) The authorizer refuses: recorded through the one path a handler has to
    // authority, so no route can refuse silently.
    let api = Arc::new(AdminApi::new(
        Arc::new(AdminService::stateful(store.clone())),
        authenticator(),
        Arc::new(FakeAdminAuthorizer::permitting(&[AdminAction::ReadState])),
    ));
    let error = api
        .authorize(&human(), AdminAction::Publish, Surface::Tenant, &scoped)
        .await
        .expect_err("a read-only identity may not publish");
    assert_eq!(error.code(), "admin_forbidden");

    // (b) The service refuses an edit that reached past the granted scope: the
    // authorizer never saw this one, which is exactly why it is recorded here.
    let service = service(&store);
    let mut request = request("key-1", ExpectedRevision::Empty, WriteMode::Apply);
    request.scope = scoped.clone();
    service
        .apply(
            &AdminGrant::granted(human(), AdminAction::Publish, scoped.clone()),
            &request,
            &replace_with(fixtures::state()),
        )
        .await
        .expect_err("a claimed scope is not the scope that was changed");

    let denials = crate::backends::control_plane::ControlPlaneStore::denials(
        store.as_ref(),
        &DenialPage::for_scope(Some(tenant)),
        10,
    )
    .await
    .expect("the denial trail");
    assert_eq!(denials.len(), 2, "both refusals are queryable: {denials:?}");
    assert!(
        denials.iter().all(|denial| denial.scope == scoped
            && denial.surface == Surface::Tenant
            && denial.actor == human().actor()),
        "a denial names who reached for what: {denials:?}"
    );
    assert!(
        denials
            .iter()
            .any(|denial| denial.reason == crate::desired_state::DenialReason::RoleLacksAction),
        "the action refusal names the role, not the scope: {denials:?}"
    );
    assert!(
        denials
            .iter()
            .any(|denial| denial.reason == crate::desired_state::DenialReason::OutOfScope),
        "the scope refusal names the scope: {denials:?}"
    );
    assert_eq!(store.published_revisions(), 0);
}

#[tokio::test]
async fn an_authenticated_refusal_survives_a_denial_trail_that_cannot_be_written() {
    // A control-plane outage must not turn a `403` into a `503`: the refusal has
    // already happened, and the caller learns the same thing either way.
    let store = Arc::new(InMemoryControlPlane::new());
    store.set_unavailable(true);
    let api = Arc::new(AdminApi::new(
        Arc::new(AdminService::stateful(store.clone())),
        authenticator(),
        Arc::new(FakeAdminAuthorizer::permitting(&[AdminAction::ReadState])),
    ));
    let error = api
        .authorize(
            &human(),
            AdminAction::Publish,
            Surface::Tenant,
            &ResourceScope::Tenant(fixtures::tenant_id(1)),
        )
        .await
        .expect_err("a read-only identity may not publish");
    assert_eq!(error.code(), "admin_forbidden");
    assert_eq!(error.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn a_scoped_grant_cannot_change_a_resource_outside_its_scope() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);

    // A tenant-scoped administrator, mutating within its own scope by
    // attribution, whose *edit* reaches a deployment-scoped resource.
    let tenant = ResourceScope::Tenant(fixtures::tenant_id(1));
    let mut scoped = request("key-1", ExpectedRevision::Empty, WriteMode::Apply);
    scoped.scope = tenant.clone();
    let error = service
        .apply(
            &AdminGrant::granted(human(), AdminAction::Publish, tenant.clone()),
            &scoped,
            &replace_with(fixtures::state()),
        )
        .await
        .expect_err("a claimed scope is not the scope that was changed");
    assert_eq!(error.code(), "admin_forbidden");
    assert_eq!(store.published_revisions(), 0);

    // The same grant may change what it does own: a rename inside the tenant.
    let deployment = service
        .apply(
            &grant(AdminAction::Publish),
            &request("key-2", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state()),
        )
        .await
        .expect("a deployment-scoped publication");
    let base = crate::desired_state::RevisionId::parse(deployment.revision().unwrap()).unwrap();
    let mut scoped = request("key-3", ExpectedRevision::Exactly(base), WriteMode::Apply);
    scoped.scope = tenant.clone();
    service
        .apply(
            &AdminGrant::granted(human(), AdminAction::Publish, tenant),
            &scoped,
            &replace_with(fixtures::state_with_renamed_alias()),
        )
        .await
        .expect("a tenant-scoped change to a tenant-scoped resource");
}

#[tokio::test]
async fn a_scoped_grant_cannot_read_the_deployment_wide_projections() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);
    let revision = publish_fixture(&service, "key-1").await;
    let revision = crate::desired_state::RevisionId::parse(&revision).expect("a revision id");
    let tenant = ResourceScope::Tenant(fixtures::tenant_id(1));
    let scoped = |action| AdminGrant::granted(human(), action, tenant.clone());

    // Every projection is of the whole deployment, so the right action at a
    // narrower scope is not authority to read it: otherwise a tenant
    // administrator sees every other tenant's resources, history and audit.
    for error in [
        service
            .desired_state(&scoped(AdminAction::ReadState))
            .await
            .expect_err("deployment-wide state needs deployment authority"),
        service
            .history(&scoped(AdminAction::ReadHistory), HistoryRequest::default())
            .await
            .expect_err("deployment-wide history needs deployment authority"),
        service
            .audit(&scoped(AdminAction::ReadAudit), revision)
            .await
            .expect_err("a deployment-wide audit trail needs deployment authority"),
    ] {
        assert_eq!(error.code(), "admin_forbidden");
        assert_eq!(error.status(), StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn a_stray_attribution_header_does_not_reject_an_administrator() {
    // One header, or an unreadable one, is not usable attribution — but it is
    // also not a reason to refuse a caller who is not using breakglass, because
    // nothing has yet decided which credential population it belongs to.
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {HUMAN_TOKEN}")).unwrap(),
    );
    headers.insert(
        BREAKGLASS_OPERATOR_HEADER,
        HeaderValue::from_static("ops-oncall"),
    );
    let presented = AdminPresented::from_headers(&headers).expect("a presented OIDC credential");
    assert_eq!(
        presented.attribution,
        PresentedAttribution::Invalid(InvalidAttribution::Missing)
    );

    headers.insert(
        BREAKGLASS_REASON_HEADER,
        HeaderValue::from_bytes(b"\xff not text").unwrap(),
    );
    let presented = AdminPresented::from_headers(&headers).expect("a presented OIDC credential");
    // Unreadable stays distinct from absent, as it does for the mutation
    // preconditions.
    assert_eq!(
        presented.attribution,
        PresentedAttribution::Invalid(InvalidAttribution::Unprintable)
    );

    let authenticator = authenticator();
    let identity = authenticator
        .authenticate(&presented)
        .await
        .expect("a stray attribution header does not unauthenticate a human");
    assert_eq!(identity, human());

    // The same half-filled attribution refuses breakglass, which is where
    // attribution is required.
    let error = authenticator
        .authenticate(&AdminPresented {
            credential: AdminCredential::new(BREAKGLASS_SECRET),
            attribution: PresentedAttribution::Invalid(InvalidAttribution::Missing),
        })
        .await
        .expect_err("breakglass must name an operator and a reason");
    assert_eq!(AdminError::from(error).code(), "admin_unauthenticated");
}

#[tokio::test]
async fn a_stray_inference_key_does_not_reject_an_administrator() {
    // `x-api-key` names the refusal when it is all a caller offered, because
    // then it is what the caller meant to authenticate with. Alongside an
    // administrative bearer token it is a stray header from a shared client, and
    // it is ignored rather than fatal — it is never read, so it grants nothing
    // either way.
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {HUMAN_TOKEN}")).unwrap(),
    );
    headers.insert(
        axum::http::HeaderName::from_static(INFERENCE_KEY_HEADER),
        HeaderValue::from_static("sk-gateway-inference-key"),
    );

    let presented = AdminPresented::from_headers(&headers).expect("a presented OIDC credential");
    let identity = authenticator()
        .authenticate(&presented)
        .await
        .expect("a stray inference key does not unauthenticate a human");
    assert_eq!(identity, human());
}

// ---------------------------------------------------------------------------
// Preconditions
// ---------------------------------------------------------------------------

#[test]
fn a_mutation_must_carry_an_idempotency_key_and_an_expected_revision() {
    let mut headers = axum::http::HeaderMap::new();
    assert_eq!(
        MutationPreconditions::from_headers(&headers)
            .expect_err("no key")
            .code(),
        "idempotency_key_required"
    );

    headers.insert(IDEMPOTENCY_KEY_HEADER, "".parse().unwrap());
    assert_eq!(
        MutationPreconditions::from_headers(&headers)
            .expect_err("an empty key is not a key")
            .code(),
        "idempotency_key_invalid"
    );

    headers.insert(IDEMPOTENCY_KEY_HEADER, "key-1".parse().unwrap());
    let error = MutationPreconditions::from_headers(&headers).expect_err("no expected revision");
    assert_eq!(error.code(), "expected_revision_required");
    assert_eq!(error.status(), StatusCode::PRECONDITION_REQUIRED);

    headers.insert(EXPECTED_REVISION_HEADER, "not-a-revision".parse().unwrap());
    assert_eq!(
        MutationPreconditions::from_headers(&headers)
            .expect_err("that is not a revision id")
            .code(),
        "expected_revision_invalid"
    );

    headers.insert(EXPECTED_REVISION_HEADER, "empty".parse().unwrap());
    let preconditions = MutationPreconditions::from_headers(&headers).expect("both preconditions");
    assert_eq!(preconditions.expected, ExpectedRevision::Empty);
    assert_eq!(preconditions.mode, WriteMode::Apply);

    // A named revision round-trips, so a caller can pass back exactly what a read
    // gave it.
    let revision = fixtures::revision_id(7);
    headers.insert(
        EXPECTED_REVISION_HEADER,
        revision.to_string().parse().unwrap(),
    );
    assert_eq!(
        MutationPreconditions::from_headers(&headers)
            .expect("a named revision")
            .expected,
        ExpectedRevision::Exactly(revision)
    );

    headers.insert(DRY_RUN_HEADER, "true".parse().unwrap());
    assert_eq!(
        MutationPreconditions::from_headers(&headers)
            .expect("a dry run")
            .mode,
        WriteMode::DryRun
    );
    headers.insert(DRY_RUN_HEADER, "yes".parse().unwrap());
    assert_eq!(
        MutationPreconditions::from_headers(&headers)
            .expect_err("`yes` is not a boolean")
            .code(),
        "dry_run_invalid"
    );
}

#[test]
fn an_audit_summary_is_bounded_and_printable() {
    assert!(AuditSummary::parse("rotate the primary credential").is_ok());
    for rejected in [
        String::new(),
        " ".repeat(4),
        "a".repeat(AuditSummary::MAX_LEN + 1),
        "line\nbreak".to_owned(),
    ] {
        assert_eq!(
            AuditSummary::parse(&rejected)
                .expect_err("an unusable summary")
                .code(),
            "audit_summary_invalid"
        );
    }
}

#[tokio::test]
async fn a_stale_expected_revision_conflicts_without_publishing() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);
    let head = publish_fixture(&service, "key-1").await;

    let error = service
        .apply(
            &grant(AdminAction::Publish),
            // The caller still believes nothing is published.
            &request("key-2", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state_with_renamed_alias()),
        )
        .await
        .expect_err("another administrator published first");
    assert_eq!(error.code(), "revision_conflict");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert!(
        !error.retryable(),
        "a conflict is resolved by re-reading, not by retrying"
    );
    assert_eq!(store.published_revisions(), 1);

    // A conflict names the newest revision, so the caller knows what to re-read.
    let envelope = serde_json::to_value(error.envelope()).expect("a serializable envelope");
    assert_eq!(envelope["error"]["type"], "revision_conflict");
    assert_eq!(
        envelope["error"]["revision"], head,
        "the head belongs in the structured field, not only in the message"
    );
}

#[tokio::test]
async fn an_expected_revision_the_store_no_longer_has_conflicts_rather_than_404s() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);
    let head = publish_fixture(&service, "key-1").await;

    // A base that is not the head and is not retained: the caller lost a race
    // against a publication old enough to have been pruned, and needs the head.
    let error = service
        .apply(
            &grant(AdminAction::Publish),
            &request(
                "key-2",
                ExpectedRevision::Exactly(fixtures::revision_id(99)),
                WriteMode::Apply,
            ),
            &replace_with(fixtures::state_with_renamed_alias()),
        )
        .await
        .expect_err("a base that is gone is not a base");
    assert_eq!(error.code(), "revision_conflict");
    let envelope = serde_json::to_value(error.envelope()).expect("a serializable envelope");
    assert_eq!(envelope["error"]["revision"], head);
    assert_eq!(store.published_revisions(), 1);
}

// ---------------------------------------------------------------------------
// Dry run
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_dry_run_validates_and_diffs_without_any_durable_side_effect() {
    let inner = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(inner.clone()));
    let service = AdminService::stateful(counting.clone());
    let revision = publish_fixture(&service, "key-1").await;
    let calls_after_publish = counting.calls();

    let outcome = service
        .apply(
            &grant(AdminAction::Publish),
            &request(
                "key-dry",
                ExpectedRevision::Exactly(
                    crate::desired_state::RevisionId::parse(&revision).unwrap(),
                ),
                WriteMode::DryRun,
            ),
            &replace_with(fixtures::state_with_renamed_alias()),
        )
        .await
        .expect("a dry run of a valid candidate");

    assert_eq!(outcome.result, MutationResult::DryRun);
    assert_eq!(outcome.mode, "dry-run");
    assert_eq!(outcome.base.as_deref(), Some(revision.as_str()));
    assert!(outcome.revision().is_none());
    // The rename is visible as an update of one resource, not as an add plus a
    // remove.
    assert_eq!(outcome.diff.summary.updated, 1);
    assert_eq!(outcome.diff.summary.added, 0);
    assert_eq!(outcome.diff.summary.removed, 0);
    assert!(outcome.diff.resources.iter().any(|delta| delta.renamed));

    // Nothing durable moved: no second revision, no new stored version, no audit
    // event, and the idempotency key is still spendable.
    assert_eq!(inner.published_revisions(), 1);
    let after = counting.calls();
    assert!(
        after > calls_after_publish,
        "a dry run reads the current state"
    );
    let outcome = service
        .apply(
            &grant(AdminAction::Publish),
            &request(
                "key-dry",
                ExpectedRevision::Exactly(
                    crate::desired_state::RevisionId::parse(&revision).unwrap(),
                ),
                WriteMode::Apply,
            ),
            &replace_with(fixtures::state_with_renamed_alias()),
        )
        .await
        .expect("the same key is still unspent after a dry run");
    assert!(matches!(outcome.result, MutationResult::Published { .. }));
    assert_eq!(inner.published_revisions(), 2);
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_retry_replays_and_a_reused_key_is_refused() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);
    let first = publish_fixture(&service, "key-retry").await;

    // The identical request again — a client that did not see the response.
    let replay = service
        .apply(
            &grant(AdminAction::Publish),
            &request("key-retry", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state()),
        )
        .await
        .expect("a retry of the same change replays");
    assert_eq!(
        replay.result,
        MutationResult::Replayed {
            revision: first.clone()
        }
    );
    assert_eq!(store.published_revisions(), 1);
    // A replay payload is the same bounded projection an apply returns: a
    // checksum and a diff, never state.
    let payload = serde_json::to_value(&replay).expect("a serializable outcome");
    assert_eq!(payload["result"], "replayed");
    assert!(payload.get("state").is_none());

    // The same key carrying *different* state is refused, not replayed: replaying
    // would report a change that never happened.
    let error = service
        .apply(
            &grant(AdminAction::Publish),
            &request("key-retry", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state_with_renamed_alias()),
        )
        .await
        .expect_err("a reused key is not a retry");
    assert_eq!(error.code(), "idempotency_key_reused");
    assert_eq!(error.status(), StatusCode::CONFLICT);
    assert_eq!(store.published_revisions(), 1);
}

// ---------------------------------------------------------------------------
// Validation before publication
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_complete_candidate_is_validated_before_anything_is_published() {
    let inner = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(inner.clone()));
    let service = AdminService::stateful(counting.clone());

    let error = service
        .apply(
            &grant(AdminAction::Publish),
            &request("key-invalid", ExpectedRevision::Empty, WriteMode::Apply),
            // A state whose alias depends on a resource version the candidate
            // does not contain: incomplete state, which only a *complete*
            // candidate check can catch.
            &(|state: &mut DesiredState| {
                let mut candidate = DesiredState::new();
                candidate.insert(fixtures::tenant(1, "acme"))?;
                candidate.insert(fixtures::alias(
                    &fixtures::tenant_id(1),
                    4,
                    "fast",
                    &[fixtures::reference(ResourceKind::ProviderCredential, 3)],
                ))?;
                *state = candidate;
                Ok(())
            }),
        )
        .await
        .expect_err("an incomplete candidate is not publishable");
    assert_eq!(error.code(), "validation_failed");
    assert_eq!(error.rule(), Some("dangling_resource_reference"));
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        inner.published_revisions(),
        0,
        "an invalid candidate must not reach the store"
    );
    assert_eq!(inner.stored_versions(), 0);

    // The domain's own prose is logged, not returned.
    assert!(error.operator_detail().is_some());
    let envelope = serde_json::to_value(error.envelope()).unwrap();
    assert_eq!(envelope["error"]["rule"], "dangling_resource_reference");
    assert!(envelope["error"].get("detail").is_none());
}

// ---------------------------------------------------------------------------
// Bounded reads
// ---------------------------------------------------------------------------

#[test]
fn a_history_request_cannot_ask_for_an_unbounded_page() {
    assert_eq!(HistoryLimit::default().get(), 20);
    assert_eq!(HistoryLimit::parse(1).expect("one revision").get(), 1);
    assert_eq!(
        HistoryLimit::parse(HistoryLimit::MAX)
            .expect("the maximum")
            .get(),
        HistoryLimit::MAX
    );
    for rejected in [0, HistoryLimit::MAX + 1, u32::MAX] {
        let error = HistoryLimit::parse(rejected).expect_err("an unbounded page");
        assert_eq!(error.code(), "history_limit_invalid");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn history_is_a_bounded_parent_walk_with_a_cursor() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);
    let mut published = vec![publish_fixture(&service, "key-0").await];
    // Alternate between two valid states, so each publication is a real change.
    for step in 1..5u32 {
        let expected = ExpectedRevision::Exactly(
            crate::desired_state::RevisionId::parse(published.last().unwrap()).unwrap(),
        );
        let state = if step % 2 == 1 {
            fixtures::state_with_renamed_alias()
        } else {
            fixtures::state()
        };
        let outcome = service
            .apply(
                &grant(AdminAction::Publish),
                &request(&format!("key-{step}"), expected, WriteMode::Apply),
                &replace_with(state),
            )
            .await
            .expect("a publication");
        published.push(outcome.revision().expect("a revision").to_owned());
    }
    assert_eq!(store.published_revisions(), 5);

    let page = service
        .history(
            &grant(AdminAction::ReadHistory),
            HistoryRequest {
                limit: HistoryLimit::parse(2).unwrap(),
                start: None,
            },
        )
        .await
        .expect("a bounded page");
    assert_eq!(page.revisions.len(), 2);
    assert_eq!(page.limit, 2);
    // Newest first, and each entry names its parent.
    assert_eq!(page.revisions[0].revision, published[4]);
    assert_eq!(
        page.revisions[0].parent.as_deref(),
        Some(published[3].as_str())
    );
    assert_eq!(page.revisions[1].revision, published[3]);
    let cursor = page.next_start.clone().expect("more revisions remain");
    assert_eq!(cursor, published[2]);

    let rest = service
        .history(
            &grant(AdminAction::ReadHistory),
            HistoryRequest {
                limit: HistoryLimit::parse(HistoryLimit::MAX).unwrap(),
                start: Some(crate::desired_state::RevisionId::parse(&cursor).unwrap()),
            },
        )
        .await
        .expect("the rest of the history");
    assert_eq!(rest.revisions.len(), 3);
    assert!(
        rest.next_start.is_none(),
        "the walk reached the first revision"
    );
    // A history entry describes a revision without shipping its state.
    let payload = serde_json::to_value(&rest).unwrap();
    assert!(payload["revisions"][0]["checksum"].is_string());
    assert!(payload["revisions"][0].get("resources").is_some());
    assert!(payload["revisions"][0].get("state").is_none());
}

#[tokio::test]
async fn a_control_plane_outage_mid_walk_is_reported_rather_than_served_as_a_short_page() {
    let oracle = Arc::new(InMemoryControlPlane::new());
    let service = service(&oracle);
    let first = publish_fixture(&service, "key-0").await;
    service
        .apply(
            &grant(AdminAction::Publish),
            &request(
                "key-1",
                ExpectedRevision::Exactly(crate::desired_state::RevisionId::parse(&first).unwrap()),
                WriteMode::Apply,
            ),
            &replace_with(fixtures::state_with_renamed_alias()),
        )
        .await
        .expect("a second publication");

    // The head loads, its parent does not: a truncated page here would report a
    // one-entry history that looks complete, and hide the outage.
    let flaky = AdminService::stateful(Arc::new(FlakyStore::failing_manifests_after(
        oracle.clone(),
        1,
    )));
    let error = flaky
        .history(
            &grant(AdminAction::ReadHistory),
            HistoryRequest {
                limit: HistoryLimit::parse(HistoryLimit::MAX).unwrap(),
                start: None,
            },
        )
        .await
        .expect_err("an outage is not the end of the history");
    assert_eq!(error.code(), "control_plane_unavailable");
    assert!(error.retryable());
}

#[tokio::test]
async fn an_audit_page_is_capped_and_says_when_it_truncated() {
    use crate::desired_state::{AuditEvent, AuditEventId, MutationId};
    use std::time::SystemTime;

    let ids = crate::desired_state::Uuid7Generator::new();
    let mutation = MutationId::new(ids.next());
    let events: Vec<AuditEvent> = (0..super::reads::AuditPage::MAX_EVENTS + 5)
        .map(|_| AuditEvent {
            id: AuditEventId::new(ids.next()),
            mutation,
            actor: human().actor(),
            kind: MutationKind::Update,
            target: None,
            summary: "published".to_owned(),
            recorded_at: SystemTime::UNIX_EPOCH,
        })
        .collect();
    let page = super::reads::AuditPage::of(fixtures::revision_id(1), &events);
    assert_eq!(page.events.len(), super::reads::AuditPage::MAX_EVENTS);
    assert!(page.truncated);
}

#[tokio::test]
async fn a_convergence_read_matches_while_only_its_elapsed_times_move() {
    use axum::response::IntoResponse;

    use super::conditional::Conditional;
    use super::reads::ConvergenceResult;

    let service = AdminService::stateful(Arc::new(InMemoryControlPlane::new()));
    let report = |lag_ms: u64, active| crate::convergence::RevisionReport {
        desired: Some(fixtures::revision_id(2)),
        loaded: Some(fixtures::revision_id(1)),
        active,
        source: Some(crate::convergence::SnapshotSource::ControlPlane),
        generation: 7,
        lag: std::time::Duration::from_millis(lag_ms),
        last_convergence: Some(std::time::Duration::from_millis(250)),
        consecutive_failures: 0,
        last_rejection: None,
    };
    let project = |report: &crate::convergence::RevisionReport| {
        service
            .convergence(&grant(AdminAction::ReadConvergence), Some(report))
            .expect("a convergence projection")
    };
    let answer = |result: ConvergenceResult, conditional: Option<&str>| {
        let mut headers = HeaderMap::new();
        if let Some(validator) = conditional {
            headers.insert(
                axum::http::header::IF_NONE_MATCH,
                HeaderValue::from_str(validator).expect("a conditional"),
            );
        }
        let identity = result.identity();
        Conditional::identified_by(&headers, result, &identity).into_response()
    };

    let behind = project(&report(3_000, Some(fixtures::revision_id(1))));
    let response = answer(behind, None);
    assert_eq!(response.status(), StatusCode::OK);
    let validator = response
        .headers()
        .get(axum::http::header::ETAG)
        .expect("a validator")
        .to_str()
        .expect("a readable validator")
        .to_owned();
    // Weak, because the body it labels is not byte-stable: `lag_ms` grows every
    // millisecond this replica stays behind.
    assert!(validator.starts_with("W/\""), "{validator}");
    // Per-caller, so no shared cache may reuse it for another administrator.
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("private, no-cache"),
    );
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::VARY)
            .and_then(|value| value.to_str().ok()),
        Some("authorization"),
    );

    // Still behind, three seconds later: the state a reconciler is waiting on has
    // not changed, so the poll it pays for is a header comparison. Validating by
    // the response bytes would answer `200` here forever.
    let later = project(&report(6_000, Some(fixtures::revision_id(1))));
    let response = answer(later, Some(&validator));
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);

    // The revision it was waiting for is now active: the validator must move.
    let converged = project(&report(0, Some(fixtures::revision_id(2))));
    let response = answer(converged, Some(&validator));
    assert_eq!(response.status(), StatusCode::OK);
    assert_ne!(
        response
            .headers()
            .get(axum::http::header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(validator.as_str()),
    );
}

#[tokio::test]
async fn convergence_is_projected_from_replica_state_without_reading_the_backend() {
    let inner = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(inner));
    let service = AdminService::stateful(counting.clone());
    let report = crate::convergence::RevisionReport {
        desired: Some(fixtures::revision_id(2)),
        loaded: Some(fixtures::revision_id(1)),
        active: Some(fixtures::revision_id(1)),
        source: Some(crate::convergence::SnapshotSource::LastKnownGood),
        generation: 7,
        lag: std::time::Duration::from_secs(3),
        last_convergence: Some(std::time::Duration::from_millis(250)),
        consecutive_failures: 2,
        last_rejection: Some(crate::convergence::Rejection {
            revision: Some(fixtures::revision_id(2)),
            reason: "unavailable",
            detail: format!("postgres://user:{SECRET_LOOKING}@db.internal/axond is unreachable"),
        }),
    };
    let result = service
        .convergence(&grant(AdminAction::ReadConvergence), Some(&report))
        .expect("a convergence projection");
    assert!(!result.converged);
    assert!(result.reconciling);
    assert_eq!(result.source, Some("last-known-good"));
    assert_eq!(result.lag_ms, 3_000);
    assert_eq!(result.last_rejection, Some("unavailable"));
    assert_eq!(
        counting.calls(),
        0,
        "convergence must answer during a control-plane outage"
    );
    // The rejection's operator detail is a log field, not a response field.
    let payload = serde_json::to_string(&result).unwrap();
    assert!(!payload.contains(SECRET_LOOKING));
    assert!(!payload.contains("db.internal"));

    // A replica with no reconciler has converged onto nothing. "Nothing desired
    // equals nothing active" is convergence for a reconciler, never an
    // all-clear for an operator gating a rollout on this read.
    let unreconciled = service
        .convergence(&grant(AdminAction::ReadConvergence), None)
        .expect("a convergence projection");
    assert!(!unreconciled.converged);
    assert!(!unreconciled.reconciling);
}

// ---------------------------------------------------------------------------
// Redaction
// ---------------------------------------------------------------------------

/// A state whose bodies contain unmistakably secret-looking values.
///
/// Deliberately not a provider credential (since #243 a credential body cannot
/// carry material at all) and not a policy (#253 types those too), so the body
/// belongs to a kind whose schema this slice knows nothing about. Redaction has
/// to hold for *any* body, because the projections never read one — they carry
/// its checksum.
fn state_with_secret_looking_bodies() -> DesiredState {
    let tenant = fixtures::tenant_id(1);
    let mut state = DesiredState::new();
    state
        .insert(fixtures::tenant(1, "acme"))
        .and_then(|state| {
            state.insert(ResourceVersion::new(
                fixtures::reference(ResourceKind::Price, 3),
                ResourceScope::Tenant(tenant),
                Slug::parse("primary").expect("a slug"),
                ResourceBody::Inline(CanonicalValue::map([(
                    "api_key",
                    CanonicalValue::string(SECRET_LOOKING),
                )])),
            ))
        })
        .expect("a valid state");
    state
}

#[tokio::test]
async fn no_secret_looking_value_reaches_a_diff_a_state_read_or_a_response() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);

    // Publish a state carrying a secret-looking body, then rotate it.
    let first = service
        .apply(
            &grant(AdminAction::Publish),
            &request("key-secret-1", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(state_with_secret_looking_bodies()),
        )
        .await
        .expect("a publication");
    let base = crate::desired_state::RevisionId::parse(first.revision().unwrap()).unwrap();

    // "Rotation" here is just a second version of the same resource.
    let rotated = {
        let tenant = fixtures::tenant_id(1);
        let mut state = DesiredState::new();
        state
            .insert(fixtures::tenant(1, "acme"))
            .and_then(|state| {
                state.insert(ResourceVersion::new(
                    fixtures::reference(ResourceKind::Price, 3)
                        .at(crate::desired_state::ResourceVersionNumber::FIRST.next()),
                    ResourceScope::Tenant(tenant),
                    Slug::parse("primary").expect("a slug"),
                    ResourceBody::Inline(CanonicalValue::map([(
                        "api_key",
                        CanonicalValue::string("sk-live-ROTATEDsecretVALUE"),
                    )])),
                ))
            })
            .expect("a valid state");
        state
    };
    let outcome = service
        .apply(
            &grant(AdminAction::Publish),
            &request(
                "key-secret-2",
                ExpectedRevision::Exactly(base),
                WriteMode::DryRun,
            ),
            &replace_with(rotated.clone()),
        )
        .await
        .expect("a dry run of the rotation");

    // The change is visible — one updated resource, with a changed content
    // checksum — and neither value is.
    assert_eq!(outcome.diff.summary.updated, 1);
    let delta = outcome
        .diff
        .resources
        .iter()
        .find(|delta| delta.kind == "price")
        .expect("the body-bearing resource changed");
    let before = delta.previous_body.as_ref().expect("a previous body");
    let after = delta.body.as_ref().expect("a new body");
    assert_ne!(before.content, after.content, "a rotation must be visible");

    let payloads = vec![
        serde_json::to_string(&outcome).expect("a serializable outcome"),
        serde_json::to_string(&outcome.diff).expect("a serializable diff"),
        serde_json::to_string(
            &service
                .desired_state(&grant(AdminAction::ReadState))
                .await
                .expect("a state read"),
        )
        .expect("a serializable state view"),
        serde_json::to_string(
            &service
                .audit(&grant(AdminAction::ReadAudit), base)
                .await
                .expect("an audit trail"),
        )
        .expect("a serializable audit page"),
    ];
    for payload in payloads {
        assert!(
            !payload.contains(SECRET_LOOKING) && !payload.contains("ROTATEDsecretVALUE"),
            "a secret-looking body value reached a response: {payload}"
        );
        assert!(!payload.contains("api_key"), "a body field name leaked");
    }

    // The credential itself is not renderable either, however hard a log line
    // tries.
    let credential = AdminCredential::new(SECRET_LOOKING);
    let rendered = format!("{credential:?}");
    assert!(!rendered.contains(SECRET_LOOKING), "{rendered}");
}

#[test]
fn a_diff_is_stable_complete_and_matches_resources_by_identity() {
    let base = fixtures::state();
    let renamed = fixtures::state_with_renamed_alias();

    let diff = SemanticDiff::between(Some(&base), &renamed).expect("a diff");
    assert_eq!(diff.summary.updated, 1);
    assert_eq!(diff.summary.added, 0);
    assert_eq!(diff.summary.removed, 0);
    assert_eq!(diff.summary.unchanged, 4);
    let delta = &diff.resources[0];
    assert_eq!(delta.change, "updated");
    assert!(delta.renamed);
    assert_eq!(delta.previous_slug.as_deref(), Some("fast"));
    assert_eq!(delta.slug.as_deref(), Some("quick"));
    assert_eq!(delta.previous_version, Some(1));
    assert_eq!(delta.version, Some(2));

    // Deterministic: the same pair of states diffs to the same bytes.
    let again = SemanticDiff::between(Some(&base), &renamed).expect("a diff");
    assert_eq!(
        serde_json::to_string(&diff).unwrap(),
        serde_json::to_string(&again).unwrap()
    );

    // The first publication is a diff against nothing, not a special case.
    let initial = SemanticDiff::between(None, &base).expect("a diff");
    assert_eq!(initial.summary.added, base.resources().len());
    assert_eq!(initial.summary.unchanged, 0);
    assert_eq!(initial.blobs.len(), 1);
    assert_eq!(initial.blobs[0].change, "added");
    assert!(!initial.is_empty());

    // A body is described by form and checksum, and a blob additionally by its
    // digest and size — never by content.
    let catalog = initial
        .resources
        .iter()
        .find(|delta| delta.kind == "catalog-model")
        .expect("the blob-backed catalogue");
    let body = catalog.body.as_ref().expect("a body");
    assert_eq!(body.form, "blob");
    assert!(body.size_bytes.is_some());

    // Identical states diff to nothing at all.
    let empty = SemanticDiff::between(Some(&base), &fixtures::state()).expect("a diff");
    assert!(empty.is_empty());
    assert_eq!(empty.summary.unchanged, base.resources().len());
}

#[test]
fn a_diff_shows_a_rewiring_that_nothing_else_about_the_resource_would_reveal() {
    // The alias keeps its slug and its body and only stops pointing at the
    // credential. Without the dependency edges the delta would be a bare
    // `updated` row with a version bump and an unchanged body checksum — the one
    // change a reviewer of a reference edit most needs to see.
    let tenant = fixtures::tenant_id(1);
    let catalog = fixtures::blob_backed_catalog(5);
    let credential = fixtures::credential(&tenant, 3, "primary");
    let base = fixtures::state();
    let alias = base
        .get(&fixtures::reference(ResourceKind::Alias, 4))
        .expect("the fixture alias");

    let mut rewired = DesiredState::new();
    rewired.declare_blob(*catalog.body.blob().expect("a blob body"));
    rewired
        .insert(fixtures::tenant(1, "acme"))
        .and_then(|state| state.insert(fixtures::project(&tenant, 2, "core")))
        .and_then(|state| state.insert(credential.clone()))
        .and_then(|state| state.insert(catalog.clone()))
        .and_then(|state| {
            state.insert(
                ResourceVersion::new(
                    alias.reference.at(alias.reference.version.next()),
                    alias.scope.clone(),
                    alias.slug.clone(),
                    alias.body.clone(),
                )
                .depending_on([catalog.reference]),
            )
        })
        .expect("dropping an edge leaves valid desired state");

    let diff = SemanticDiff::between(Some(&base), &rewired).expect("a diff");
    assert_eq!(diff.summary.updated, 1);
    let delta = &diff.resources[0];
    assert!(delta.rewired);
    assert!(!delta.renamed);
    assert_eq!(delta.slug, delta.previous_slug);
    assert_eq!(delta.body, delta.previous_body);
    let previous = delta
        .previous_depends_on
        .as_ref()
        .expect("the edges it had");
    let current = delta.depends_on.as_ref().expect("the edges it has");
    assert!(previous.contains(&credential.reference.to_string()));
    assert!(!current.contains(&credential.reference.to_string()));
    assert!(current.contains(&catalog.reference.to_string()));

    // An unchanged resource is still not rewired, so the flag means what it says.
    let unchanged = SemanticDiff::between(Some(&base), &fixtures::state()).expect("a diff");
    assert!(unchanged.is_empty());
}

#[test]
fn a_diff_shows_a_resource_changing_owner() {
    // Nothing pins a resource's scope across revisions, so an alias re-parented
    // from its tenant onto one of that tenant's projects is a legal publication
    // whose slug, body and edges are all untouched. Without the previous scope
    // the delta reads as a version bump, and "who owns this now" is the question
    // a reviewer of a re-parenting is there to answer.
    let tenant = fixtures::tenant_id(1);
    let project = fixtures::project_id(2);
    let base = fixtures::state();
    let alias = base
        .get(&fixtures::reference(ResourceKind::Alias, 4))
        .expect("the fixture alias");

    let mut moved = base.clone();
    moved
        .insert(
            ResourceVersion::new(
                alias.reference.at(alias.reference.version.next()),
                ResourceScope::Project { tenant, project },
                alias.slug.clone(),
                alias.body.clone(),
            )
            .depending_on(alias.depends_on.iter().copied()),
        )
        .expect("a project of the alias's own tenant is a valid owner");

    let diff = SemanticDiff::between(Some(&base), &moved).expect("a diff");
    assert_eq!(diff.summary.updated, 1);
    let delta = &diff.resources[0];
    assert!(delta.moved);
    assert!(!delta.renamed);
    assert!(!delta.rewired);
    assert_eq!(delta.slug, delta.previous_slug);
    assert_eq!(delta.body, delta.previous_body);
    assert_eq!(delta.depends_on, delta.previous_depends_on);
    assert_eq!(delta.scope.kind, "project");
    assert_eq!(
        delta.scope.project.as_deref(),
        Some(project.to_string()).as_deref()
    );
    let previous = delta.previous_scope.as_ref().expect("the owner it had");
    assert_eq!(previous.kind, "tenant");
    assert_eq!(previous.project, None);

    // An update that leaves the scope alone does not claim a move.
    let renamed = SemanticDiff::between(Some(&base), &fixtures::state()).expect("a diff");
    assert!(renamed.resources.iter().all(|delta| !delta.moved));
}

#[test]
fn a_diff_names_the_object_a_repointed_blob_body_now_addresses() {
    // Both snapshots stay declared, so no blob appears or disappears and the blob
    // section of the diff says nothing. The body view has to carry the digest, or
    // "which snapshot is this catalogue now serving" is unanswerable from the diff
    // a reviewer approves.
    let base = fixtures::state_with_two_blobs();
    let repointed_to = fixtures::second_blob_backed_catalog(6);
    let catalog = base
        .get(&fixtures::reference(ResourceKind::CatalogModel, 5))
        .expect("the fixture catalogue");

    let mut candidate = base.clone();
    candidate
        .insert(ResourceVersion::new(
            catalog.reference.at(catalog.reference.version.next()),
            catalog.scope.clone(),
            catalog.slug.clone(),
            repointed_to.body.clone(),
        ))
        .expect("repointing at an already declared blob is valid");

    let diff = SemanticDiff::between(Some(&base), &candidate).expect("a diff");
    assert!(
        diff.blobs.is_empty(),
        "neither snapshot entered or left the revision"
    );
    let delta = &diff.resources[0];
    let before = delta.previous_body.as_ref().expect("the body it had");
    let after = delta.body.as_ref().expect("the body it has");
    assert_eq!(
        before.digest.as_deref(),
        Some(
            catalog
                .body
                .blob()
                .expect("a blob body")
                .digest
                .to_string()
                .as_str()
        )
    );
    assert_eq!(
        after.digest.as_deref(),
        Some(
            repointed_to
                .body
                .blob()
                .expect("a blob body")
                .digest
                .to_string()
                .as_str()
        )
    );
    assert_ne!(before.digest, after.digest);
}

#[test]
fn a_state_view_describes_resources_without_their_bodies() {
    let view = StateView::of(None).expect("an empty state view");
    assert!(view.revision.is_none());
    assert!(view.resources.is_empty());
    assert!(view.blobs.is_empty());
}

// ---------------------------------------------------------------------------
// The error vocabulary
// ---------------------------------------------------------------------------

#[test]
fn every_declared_code_is_reachable_distinct_and_prose_free() {
    use crate::backends::control_plane::ControlPlaneError;
    use crate::backends::control_plane::hydration::HydrationLimit;

    let revision = fixtures::revision_id(1);
    let reference = fixtures::reference(ResourceKind::Alias, 4);
    let errors = vec![
        AdminError::Unauthenticated(AdminAuthError::UnknownCredential),
        AdminError::Forbidden(AdminAuthError::ScopeNotPermitted),
        AdminError::IdentityProviderUnavailable,
        AdminError::StatefulModeRequired,
        AdminError::IdempotencyKeyRequired,
        AdminError::IdempotencyKeyInvalid(
            IdempotencyKey::parse("").expect_err("an empty key is invalid"),
        ),
        AdminError::IdempotencyKeyReused {
            key: IdempotencyKey::parse("key-1").unwrap(),
            published: revision,
        },
        AdminError::ExpectedRevisionRequired,
        AdminError::ExpectedRevisionInvalid,
        AdminError::RevisionConflict {
            expected: ExpectedRevision::Empty,
            actual: Some(revision),
        },
        AdminError::from(ValidationError::Empty),
        AdminError::ImmutableResourceVersion { reference },
        AdminError::RevisionNotFound(revision),
        AdminError::RevisionUnreadable {
            revision: Some(revision),
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::RevisionIncompatible {
            revision,
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::RevisionTooLarge {
            revision,
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::ControlPlaneUnavailable {
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::ControlPlaneDenied {
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::NameTaken {
            noun: "tenant",
            name: "acme".to_owned(),
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::AuditSummaryInvalid,
        AdminError::DryRunInvalid,
        AdminError::HistoryLimitInvalid { max: 100 },
        AdminError::RequestInvalid {
            schema: "tenant",
            // The caller's own document, echoed to the caller: unlike an
            // operator detail, this is not the deployment's to keep.
            detail: "`slug`: a slug is lowercase".to_owned(),
        },
        AdminError::RequestTooLarge {
            limit: crate::admin::router::ADMIN_MAX_REQUEST_BYTES,
        },
        AdminError::RouteNotFound,
        AdminError::MethodNotAllowed,
        // The secret-store arms. Each holds a reference or a backend detail;
        // none of them can hold material, which is what the redaction assertion
        // below covers for the whole vocabulary at once.
        AdminError::SecretStoreUnavailable {
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::SecretNotFound {
            reference: crate::desired_state::fixtures::secret_ref_at(1, 1),
        },
        AdminError::SecretLifecycleRefused {
            reference: crate::desired_state::fixtures::secret_ref_at(1, 1),
            detail: "a revoked version is not resolvable".to_owned(),
        },
        AdminError::SecretInUse {
            reference: crate::desired_state::fixtures::secret_ref_at(1, 1),
        },
        AdminError::SecretVersionExists {
            reference: crate::desired_state::fixtures::secret_ref_at(1, 2),
        },
        AdminError::SecretMaterialRefused {
            detail: "material is empty".to_owned(),
        },
        AdminError::SecretStoreUnusable {
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::CatalogStoreUnavailable {
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::CatalogStoreUnusable {
            detail: SECRET_LOOKING.to_owned(),
        },
        AdminError::BindingRefused {
            rule: "catalogue_identity_required",
            detail: SECRET_LOOKING.to_owned(),
        },
    ];

    let codes: Vec<&'static str> = errors.iter().map(AdminError::code).collect();
    assert_eq!(
        codes,
        AdminError::CODES,
        "the declared vocabulary and the variants have diverged"
    );
    let unique: std::collections::BTreeSet<&&str> = codes.iter().collect();
    assert_eq!(unique.len(), codes.len(), "two variants share a code");

    for error in &errors {
        let payload = serde_json::to_string(&error.envelope()).expect("a serializable envelope");
        assert!(
            !payload.contains(SECRET_LOOKING),
            "an operator detail reached the wire: {payload}"
        );
    }

    // Every `ControlPlaneError` maps to a code rather than collapsing into a
    // generic failure, including the two that name no revision.
    let mapped = vec![
        ControlPlaneError::Unavailable {
            backend: "postgres",
            message: SECRET_LOOKING.to_owned(),
        },
        ControlPlaneError::Denied {
            backend: "postgres",
            message: SECRET_LOOKING.to_owned(),
        },
        ControlPlaneError::CorruptStorage {
            detail: SECRET_LOOKING.to_owned(),
        },
        ControlPlaneError::too_large(revision, HydrationLimit::Entries { limit: 10 }),
    ];
    for error in mapped {
        let admin = AdminError::from(error);
        assert!(AdminError::CODES.contains(&admin.code()));
        let payload = serde_json::to_string(&admin.envelope()).unwrap();
        assert!(
            !payload.contains(SECRET_LOOKING),
            "a backend message reached the wire: {payload}"
        );
        assert!(admin.operator_detail().is_some());
    }
}

#[test]
fn corrupt_secret_metadata_is_a_store_failure_not_a_material_refusal() {
    let detail = "secret sct_00000000-0000-7000-8000-000000000001 holds invalid metadata";
    let error = AdminError::from_secret(SecretError::Corrupt {
        detail: detail.to_owned(),
    });

    assert_eq!(error.code(), "secret_store_unusable");
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!error.to_string().contains("presented secret material"));
    assert_eq!(error.operator_detail(), Some(detail));
}

#[tokio::test]
async fn a_control_plane_outage_is_retryable_and_reveals_no_backend_detail() {
    let store = Arc::new(InMemoryControlPlane::new());
    let service = service(&store);
    store.set_unavailable(true);
    let error = service
        .apply(
            &grant(AdminAction::Publish),
            &request("key-1", ExpectedRevision::Empty, WriteMode::Apply),
            &replace_with(fixtures::state()),
        )
        .await
        .expect_err("the control plane is down");
    assert_eq!(error.code(), "control_plane_unavailable");
    assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(error.retryable());
    let payload = serde_json::to_string(&error.envelope()).unwrap();
    assert!(!payload.contains("postgres"), "{payload}");
}

// ---------------------------------------------------------------------------
// The router boundary
// ---------------------------------------------------------------------------

/// A synthetic table: one read route and one mutating route, standing in for the
/// resource handlers #143 will register.
fn test_specs() -> Vec<AdminRouteSpec> {
    vec![
        AdminRouteSpec {
            path: "/state",
            action: AdminAction::ReadState,
            router: || get(read_state),
        },
        AdminRouteSpec {
            path: "/publish",
            action: AdminAction::Publish,
            router: || post(publish),
        },
    ]
}

async fn read_state(
    State(api): State<Arc<AdminApi>>,
    Extension(identity): Extension<AdminIdentity>,
) -> Result<Json<Value>, AdminError> {
    let grant = api
        .authorize(
            &identity,
            AdminAction::ReadState,
            Surface::AuditTrail,
            &scope(),
        )
        .await?;
    let view = api.service.desired_state(&grant).await?;
    Ok(Json(serde_json::to_value(view).expect("serializable")))
}

async fn publish(
    State(api): State<Arc<AdminApi>>,
    Extension(identity): Extension<AdminIdentity>,
    Extension(preconditions): Extension<MutationPreconditions>,
) -> Result<Json<Value>, AdminError> {
    let grant = api
        .authorize(&identity, AdminAction::Publish, Surface::Tenant, &scope())
        .await?;
    let outcome = api
        .service
        .apply(
            &grant,
            &MutationRequest {
                preconditions,
                kind: MutationKind::Update,
                surface: Surface::Tenant,
                scope: scope(),
                summary: AuditSummary::parse("publish via the router")?,
            },
            &replace_with(fixtures::state()),
        )
        .await?;
    Ok(Json(serde_json::to_value(outcome).expect("serializable")))
}

fn api(store: Option<Arc<InMemoryControlPlane>>) -> Arc<AdminApi> {
    let service = match store {
        Some(store) => AdminService::stateful(store),
        None => AdminService::stateless(),
    };
    Arc::new(AdminApi::new(
        Arc::new(service),
        authenticator(),
        Arc::new(FakeAdminAuthorizer::permissive()),
    ))
}

async fn send(api: Arc<AdminApi>, request: Request<Body>) -> (StatusCode, Value) {
    let response = mount(api, test_specs())
        .oneshot(request)
        .await
        .expect("a response");
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

#[tokio::test]
async fn every_admin_route_rejects_a_request_without_an_administrative_credential() {
    let store = Arc::new(InMemoryControlPlane::new());
    for spec in test_specs() {
        let path = format!("{ADMIN_PREFIX}{}", spec.path);
        let builder = if spec.action.mutates() {
            Request::post(&path)
        } else {
            Request::get(&path)
        };
        let (status, body) = send(
            api(Some(store.clone())),
            builder.body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{path} answered without a credential"
        );
        assert_eq!(body["error"]["type"], "admin_unauthenticated");
        assert_eq!(body["error"]["retryable"], false);
    }
    assert_eq!(store.published_revisions(), 0);
}

#[tokio::test]
async fn an_inference_credential_is_rejected_at_the_admin_boundary() {
    let store = Arc::new(InMemoryControlPlane::new());
    for header in [
        (
            axum::http::header::AUTHORIZATION.as_str(),
            "Bearer axt1.token.signature",
        ),
        (INFERENCE_KEY_HEADER, "gateway-inference-key"),
    ] {
        let (status, body) = send(
            api(Some(store.clone())),
            Request::post(format!("{ADMIN_PREFIX}/publish"))
                .header(header.0, header.1)
                .header(IDEMPOTENCY_KEY_HEADER, "key-1")
                .header(EXPECTED_REVISION_HEADER, "empty")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["type"], "admin_unauthenticated");
    }
    assert_eq!(
        store.published_revisions(),
        0,
        "an inference credential published a revision"
    );
}

#[tokio::test]
async fn the_router_requires_preconditions_before_a_handler_runs() {
    let store = Arc::new(InMemoryControlPlane::new());
    let cases = vec![
        (vec![], "idempotency_key_required"),
        (
            vec![(IDEMPOTENCY_KEY_HEADER, "key-1")],
            "expected_revision_required",
        ),
        (
            vec![
                (IDEMPOTENCY_KEY_HEADER, "key-1"),
                (EXPECTED_REVISION_HEADER, "empty"),
                (DRY_RUN_HEADER, "perhaps"),
            ],
            "dry_run_invalid",
        ),
    ];
    for (headers, expected) in cases {
        let mut builder = Request::post(format!("{ADMIN_PREFIX}/publish")).header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {HUMAN_TOKEN}"),
        );
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        let (_, body) = send(
            api(Some(store.clone())),
            builder.body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(body["error"]["type"], expected);
    }
    assert_eq!(store.published_revisions(), 0);
}

/// One path, one spec per method: preconditions are keyed to a spec's action, so
/// a reader sharing a path with a writer must not inherit the writer's headers,
/// and the writer must not lose them.
#[tokio::test]
async fn a_path_that_answers_two_verbs_keeps_each_methods_preconditions() {
    let store = Arc::new(InMemoryControlPlane::new());
    let specs = || {
        vec![
            AdminRouteSpec {
                path: "/thing",
                action: AdminAction::ReadState,
                router: || get(read_state),
            },
            AdminRouteSpec {
                path: "/thing",
                action: AdminAction::Publish,
                router: || post(publish),
            },
        ]
    };
    let authorization = format!("Bearer {HUMAN_TOKEN}");

    let response = mount(api(Some(store.clone())), specs())
        .oneshot(
            Request::get(format!("{ADMIN_PREFIX}/thing"))
                .header(axum::http::header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("a response");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the read was asked for a mutation's headers"
    );

    let response = mount(api(Some(store.clone())), specs())
        .oneshot(
            Request::post(format!("{ADMIN_PREFIX}/thing"))
                .header(axum::http::header::AUTHORIZATION, &authorization)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("a response");
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["type"], "idempotency_key_required");
    assert_eq!(store.published_revisions(), 0);
}

#[tokio::test]
async fn an_authenticated_human_publishes_and_reads_through_the_router() {
    let store = Arc::new(InMemoryControlPlane::new());
    let (status, body) = send(
        api(Some(store.clone())),
        Request::post(format!("{ADMIN_PREFIX}/publish"))
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {HUMAN_TOKEN}"),
            )
            .header(IDEMPOTENCY_KEY_HEADER, "key-1")
            .header(EXPECTED_REVISION_HEADER, "empty")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "published");
    assert_eq!(store.published_revisions(), 1);

    let (status, body) = send(
        api(Some(store.clone())),
        Request::get(format!("{ADMIN_PREFIX}/state"))
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {HUMAN_TOKEN}"),
            )
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["resources"].as_array().map(Vec::len),
        Some(fixtures::DESIRED_STATE_RESOURCES)
    );
}

#[tokio::test]
async fn a_stateless_deployment_answers_the_admin_surface_without_a_backend() {
    let (status, body) = send(
        api(None),
        Request::get(format!("{ADMIN_PREFIX}/state"))
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {HUMAN_TOKEN}"),
            )
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["error"]["type"], "stateful_mode_required");
}

#[tokio::test]
async fn an_unknown_admin_path_answers_in_the_admin_envelope() {
    let (status, body) = send(
        api(Some(Arc::new(InMemoryControlPlane::new()))),
        Request::get(format!("{ADMIN_PREFIX}/nonexistent"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "admin_route_not_found");
}

#[tokio::test]
async fn a_known_admin_path_reached_with_the_wrong_method_answers_in_the_admin_envelope() {
    let store = Arc::new(InMemoryControlPlane::new());
    let (status, body) = send(
        api(Some(store.clone())),
        // A read route reached with a write method: the method fallback answers,
        // and it answers before authentication, so no credential is needed.
        Request::post(format!("{ADMIN_PREFIX}/state"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body["error"]["type"], "admin_method_not_allowed");
    assert_eq!(body["error"]["retryable"], false);
    assert_eq!(store.published_revisions(), 0);
}

#[tokio::test]
async fn a_method_refusal_still_names_the_methods_the_path_answers() {
    let store = Arc::new(InMemoryControlPlane::new());
    let response = mount(api(Some(store)), test_specs())
        .oneshot(
            Request::post(format!("{ADMIN_PREFIX}/state"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("a response");

    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let allow = response
        .headers()
        .get(axum::http::header::ALLOW)
        .and_then(|value| value.to_str().ok())
        .expect("a 405 carries Allow, custom envelope or not");
    assert!(allow.contains("GET"), "{allow}");
    assert!(!allow.contains("POST"), "{allow}");
}

#[tokio::test]
async fn the_unauthenticated_fallbacks_reveal_the_route_table_and_nothing_credentialed() {
    // Enumeration is the accepted cost: the route table is published surface, so
    // a registered path answers 405 and an unregistered one 404 without a
    // credential. What must not vary is anything a credential would decide — the
    // answer is identical anonymous, with a valid administrative credential, and
    // with an inference credential, and it names neither.
    let store = Arc::new(InMemoryControlPlane::new());
    let bearer = format!("Bearer {HUMAN_TOKEN}");
    let credentials: [Option<(&str, &str)>; 3] = [
        None,
        Some((axum::http::header::AUTHORIZATION.as_str(), &bearer)),
        Some((INFERENCE_KEY_HEADER, "gateway-static-key")),
    ];
    for credential in credentials {
        let mut request = Request::post(format!("{ADMIN_PREFIX}/state"));
        if let Some((header, value)) = credential {
            request = request.header(header, value);
        }
        let (status, body) = send(
            api(Some(store.clone())),
            request.body(Body::empty()).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{credential:?}");
        assert_eq!(body["error"]["type"], "admin_method_not_allowed");
        let rendered = body.to_string();
        for material in [
            HUMAN_TOKEN,
            BREAKGLASS_SECRET,
            "gateway-static-key",
            "unauthenticated",
        ] {
            assert!(
                !rendered.contains(material),
                "a method refusal says nothing about credentials: {rendered}"
            );
        }
    }

    // And an unregistered path stays a 404, which is the enumeration this
    // documents rather than hides.
    let (status, body) = send(
        api(Some(store.clone())),
        Request::post(format!("{ADMIN_PREFIX}/nonexistent"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "admin_route_not_found");
    assert_eq!(store.published_revisions(), 0);
}

#[tokio::test]
async fn a_precondition_header_that_is_not_readable_text_is_invalid_rather_than_absent() {
    // Bytes outside visible ASCII are a legal header value and an illegal
    // precondition. The dry-run case is the dangerous one: read as "absent", a
    // rehearsal would publish.
    let unreadable = HeaderValue::from_bytes(b"\xff\xfe").expect("a legal header value");
    let cases = [
        (IDEMPOTENCY_KEY_HEADER, "idempotency_key_invalid"),
        (EXPECTED_REVISION_HEADER, "expected_revision_invalid"),
        (DRY_RUN_HEADER, "dry_run_invalid"),
    ];
    for (header, expected) in cases {
        let mut headers = HeaderMap::new();
        headers.insert(IDEMPOTENCY_KEY_HEADER, "key-1".parse().unwrap());
        headers.insert(EXPECTED_REVISION_HEADER, "empty".parse().unwrap());
        headers.insert(header, unreadable.clone());
        let error = MutationPreconditions::from_headers(&headers)
            .expect_err("an unreadable precondition is refused");
        assert_eq!(error.code(), expected, "{header}");
    }
}

#[tokio::test]
async fn merging_the_admin_surface_into_the_inference_router_keeps_both_fallbacks() {
    // How `main` composes the two routers. Three things must survive the merge:
    // an inference route still answers, an unknown *inference* path still meets
    // the inference fallback rather than the administrative envelope, and an
    // unknown administrative path still meets the administrative envelope rather
    // than the inference fallback.
    let store = Arc::new(InMemoryControlPlane::new());
    let inference = axum::Router::new()
        .route("/v1/models", get(|| async { "inference" }))
        .fallback(|| async { (StatusCode::NOT_FOUND, "not an inference route") });
    let app = inference.merge(mount(api(Some(store.clone())), test_specs()));

    let answer = |request: Request<Body>| {
        let app = app.clone();
        async move {
            let response = app.oneshot(request).await.expect("a response");
            let status = response.status();
            let body = response.into_body().collect().await.unwrap().to_bytes();
            (status, String::from_utf8_lossy(&body).into_owned())
        }
    };

    let (status, body) = answer(Request::get("/v1/models").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "inference", "the merge shadowed an inference route");

    let (status, body) = answer(Request::get("/v1/nothing").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        body, "not an inference route",
        "the administrative fallback swallowed an inference path"
    );

    let (status, body) = answer(
        Request::get(format!("{ADMIN_PREFIX}/nonexistent"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let body: Value = serde_json::from_str(&body).expect("the administrative envelope");
    assert_eq!(body["error"]["type"], "admin_route_not_found");

    // And the merged surface is still authenticated: the inference router's
    // permissiveness is not inherited by an administrative route.
    let (status, body) = answer(
        Request::get(format!("{ADMIN_PREFIX}/state"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let body: Value = serde_json::from_str(&body).expect("the administrative envelope");
    assert_eq!(body["error"]["type"], "admin_unauthenticated");
    assert_eq!(store.published_revisions(), 0);
}

#[tokio::test]
async fn every_shipped_route_authenticates_before_it_answers_anything_about_state() {
    // The shipped table, not the synthetic one: a route added without the layer
    // would answer this loop with something other than `401`.
    let store = Arc::new(InMemoryControlPlane::new());
    for spec in super::router::admin_route_specs() {
        // A path parameter is filled with a syntactically plausible value, so a
        // `404` cannot stand in for the authentication this asserts.
        let path = format!("{ADMIN_PREFIX}{}", super::router::concrete_path(&spec));
        let builder = if spec.action.writes() {
            Request::post(&path)
        } else {
            Request::get(&path)
        };
        let response = super::router::router(api(Some(store.clone())))
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .expect("a response");
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path}");
        assert_eq!(body["error"]["type"], "admin_unauthenticated", "{path}");
    }
    assert_eq!(store.published_revisions(), 0);
}

#[test]
fn every_shipped_row_is_scoped_to_the_admin_prefix_and_declares_its_action() {
    for spec in super::router::admin_route_specs() {
        assert!(
            spec.path.starts_with('/') && !spec.path.starts_with(ADMIN_PREFIX),
            "a shipped path is relative to the prefix: {}",
            spec.path
        );
        // Every parameter of a shipped path is one the test helper fills, so a
        // loop over the table cannot quietly request a literal `{name}` segment.
        let concrete = super::router::concrete_path(&spec);
        assert!(
            !concrete.contains('{') && !concrete.contains('}'),
            "`concrete_path` left a parameter unfilled in {}: {concrete}",
            spec.path
        );
    }
    assert_eq!(ADMIN_PREFIX, "/admin/v1");
    for spec in test_specs() {
        assert!(
            spec.path.starts_with('/') && !spec.path.starts_with(ADMIN_PREFIX),
            "a spec path is relative to the prefix: {}",
            spec.path
        );
    }
}
