//! The `/admin/v1` surface, end to end over the mounted route table.
//!
//! Distinct from [`super::tests`], which characterises the service and the
//! router's layers against hand-written test routes: these drive the *real*
//! route table, the real documents, and the real handlers over the in-memory
//! control plane, so the thing under test is what a deployment serves.
//!
//! The through-line is that a document is the only thing a caller supplies: no
//! test here can publish without an idempotency key, an expected revision, and a
//! complete candidate that validates, because the surface offers no other way.

use std::sync::Arc;
use std::time::SystemTime;

use gateway_core::CircuitState;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::auth::{AdminAction, AdminAuthorizer, INFERENCE_KEY_HEADER};
use super::fakes::{
    CountingStore, FakeAdminAuthenticator, FakeAdminAuthorizer, RecordingAuthorizer,
};
use super::protocol::{
    ADMIN_PREFIX, DRY_RUN_HEADER, EXPECTED_REVISION_EMPTY, EXPECTED_REVISION_HEADER,
    IDEMPOTENCY_KEY_HEADER,
};
use super::router::{ADMIN_MAX_REQUEST_BYTES, AdminApi, refusing_router, router};
use super::service::AdminService;
use crate::availability::{
    AvailabilityIndex, AvailabilityKey, AvailabilityReader, AvailabilityRecord, CataloguePresence,
    DiscoveryCompleteness, DiscoveryObservation, DiscoveryResult, DiscoverySource, Enablement,
    Entitlement, PolicyDecision, RuntimeObservations, ScopeRef, TargetRef,
};
use crate::backends::catalog::{RawPayload, SourceValidators};
use crate::backends::catalog_store::{CatalogStore, InMemoryCatalogStore, RetainedCatalog};
use crate::backends::control_plane::ControlPlaneStore;
use crate::backends::fakes::InMemorySecrets;
use crate::backends::models_dev::{ModelsDevAdapter, SEED_PAYLOAD, seed_snapshot};
use crate::desired_state::oracle::InMemoryControlPlane;
use crate::desired_state::{
    Actor, DenialPage, ModelEnablementBody, OfferingId, PriceBookBody, ResourceKind, ResourceScope,
    Surface, fixtures,
};

const TOKEN: &str = "human-admin-token";
const ISSUER: &str = "https://idp.example";
const SUBJECT: &str = "operator@example";

/// One administrative deployment under test: the surface, and the store behind
/// it.
struct Deployment {
    api: Arc<AdminApi>,
    store: Arc<InMemoryControlPlane>,
    secrets: Arc<InMemorySecrets>,
}

impl Deployment {
    fn new() -> Self {
        Self::with_authorizer(FakeAdminAuthorizer::permissive())
    }

    fn with_catalogue(catalogue: Arc<dyn CatalogStore>) -> Self {
        Self::with_catalogue_authorizer(catalogue, Arc::new(FakeAdminAuthorizer::permissive()))
    }

    fn with_catalogue_authorizer(
        catalogue: Arc<dyn CatalogStore>,
        authorizer: Arc<dyn AdminAuthorizer>,
    ) -> Self {
        let store = Arc::new(InMemoryControlPlane::new());
        let secrets = Arc::new(InMemorySecrets::new());
        let api = Arc::new(
            AdminApi::new(
                Arc::new(AdminService::stateful(store.clone()).with_secrets(secrets.clone())),
                Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
                authorizer,
            )
            .with_catalogue(catalogue),
        );
        Self {
            api,
            store,
            secrets,
        }
    }

    fn with_catalog_handle(handle: crate::backends::catalog_runtime::CatalogHandle) -> Self {
        let store = Arc::new(InMemoryControlPlane::new());
        let secrets = Arc::new(InMemorySecrets::new());
        let api = Arc::new(
            AdminApi::new(
                Arc::new(AdminService::stateful(store.clone()).with_secrets(secrets.clone())),
                Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
                Arc::new(FakeAdminAuthorizer::permissive()),
            )
            .with_catalog_handle(handle),
        );
        Self {
            api,
            store,
            secrets,
        }
    }

    fn with_authorizer(authorizer: FakeAdminAuthorizer) -> Self {
        let store = Arc::new(InMemoryControlPlane::new());
        let secrets = Arc::new(InMemorySecrets::new());
        let api = Arc::new(AdminApi::new(
            Arc::new(AdminService::stateful(store.clone()).with_secrets(secrets.clone())),
            Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
            Arc::new(authorizer),
        ));
        Self {
            api,
            store,
            secrets,
        }
    }

    /// A call that carries material: no idempotency key and no expected
    /// revision, because storing material publishes no revision.
    async fn post_material(&self, path: &str, body: &Value) -> (StatusCode, Value) {
        self.send(
            Request::post(format!("{ADMIN_PREFIX}{path}"))
                .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::from(body.to_string()))
                .expect("a request"),
        )
        .await
    }

    /// Store material, asserting it was stored, and answer with the reference it
    /// was stored under.
    async fn stage(&self, tenant: &str, material: &str) -> String {
        let (status, body) = self
            .post_material(
                "/secrets",
                &json!({ "tenant": tenant, "material": material }),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "staging refused: {body}");
        body["reference"]
            .as_str()
            .expect("a stored reference")
            .to_owned()
    }

    /// A deployment that derives availability: the index a snapshot would carry,
    /// and this replica's own circuits.
    fn deriving(
        authorizer: FakeAdminAuthorizer,
        index: AvailabilityIndex,
        runtime: RuntimeObservations,
    ) -> Self {
        let store = Arc::new(InMemoryControlPlane::new());
        let api = Arc::new(
            AdminApi::new(
                Arc::new(AdminService::stateful(store.clone())),
                Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
                Arc::new(authorizer),
            )
            .with_availability(Arc::new(StaticAvailability {
                index: Some(Arc::new(index)),
                runtime,
            })),
        );
        Self {
            api,
            store,
            secrets: Arc::new(InMemorySecrets::new()),
        }
    }

    /// A deployment whose availability reader is attached and derives nothing:
    /// the shape every shipped binary currently has, since no compiler is wired
    /// to project a view.
    fn attached_but_underiving() -> Self {
        let store = Arc::new(InMemoryControlPlane::new());
        let api = Arc::new(
            AdminApi::new(
                Arc::new(AdminService::stateful(store.clone())),
                Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
                Arc::new(FakeAdminAuthorizer::permissive()),
            )
            .with_availability(Arc::new(StaticAvailability {
                index: None,
                runtime: RuntimeObservations::none(),
            })),
        );
        Self {
            api,
            store,
            secrets: Arc::new(InMemorySecrets::new()),
        }
    }

    /// The same control plane, read through a narrower grant: what a tenant
    /// administrator sees of a deployment somebody with deployment authority
    /// built.
    fn narrowed(&self, scopes: &[ResourceScope]) -> Self {
        self.reauthorize(Arc::new(FakeAdminAuthorizer::permissive().within(scopes)))
    }

    fn reauthorize(&self, authorizer: Arc<dyn AdminAuthorizer>) -> Self {
        let mut api = AdminApi::new(
            Arc::new(AdminService::stateful(self.store.clone()).with_secrets(self.secrets.clone())),
            Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
            authorizer,
        );
        if let Some(catalogue) = &self.api.catalogue {
            api = api.with_catalogue(catalogue.clone());
        }
        Self {
            api: Arc::new(api),
            store: self.store.clone(),
            secrets: self.secrets.clone(),
        }
    }

    async fn send(&self, request: Request<Body>) -> (StatusCode, Value) {
        let response = router(self.api.clone())
            .oneshot(request)
            .await
            .expect("a response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.send(
            Request::get(format!("{ADMIN_PREFIX}{path}"))
                .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .expect("a request"),
        )
        .await
    }

    /// A read, with its validator and the raw body: a `304` has no body to
    /// parse, so a conditional read cannot be characterised through
    /// [`Deployment::get`].
    async fn get_conditional(
        &self,
        path: &str,
        if_none_match: Option<&str>,
    ) -> (StatusCode, Option<String>, Vec<u8>) {
        let mut request = Request::get(format!("{ADMIN_PREFIX}{path}"))
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"));
        if let Some(validator) = if_none_match {
            request = request.header(axum::http::header::IF_NONE_MATCH, validator);
        }
        let response = router(self.api.clone())
            .oneshot(request.body(Body::empty()).expect("a request"))
            .await
            .expect("a response");
        let status = response.status();
        let etag = response
            .headers()
            .get(axum::http::header::ETAG)
            .map(|value| value.to_str().expect("a readable validator").to_owned());
        let body = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        (status, etag, body.to_vec())
    }

    /// A read, with the headers the conditional contract puts on it: the
    /// validator, and the directives that keep a per-caller projection out of a
    /// shared cache.
    async fn get_with_headers(
        &self,
        path: &str,
        if_none_match: Option<&str>,
    ) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        let mut request = Request::get(format!("{ADMIN_PREFIX}{path}"))
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"));
        if let Some(validator) = if_none_match {
            request = request.header(axum::http::header::IF_NONE_MATCH, validator);
        }
        let response = router(self.api.clone())
            .oneshot(request.body(Body::empty()).expect("a request"))
            .await
            .expect("a response");
        let status = response.status();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        (status, headers, body.to_vec())
    }

    /// Publish a document, with the preconditions a mutation must carry.
    async fn post(
        &self,
        path: &str,
        key: &str,
        expected: &str,
        body: &Value,
    ) -> (StatusCode, Value) {
        self.post_with(path, key, expected, body, false).await
    }

    async fn dry_run(
        &self,
        path: &str,
        key: &str,
        expected: &str,
        body: &Value,
    ) -> (StatusCode, Value) {
        self.post_with(path, key, expected, body, true).await
    }

    async fn post_with(
        &self,
        path: &str,
        key: &str,
        expected: &str,
        body: &Value,
        dry_run: bool,
    ) -> (StatusCode, Value) {
        let mut request = Request::post(format!("{ADMIN_PREFIX}{path}"))
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .header(IDEMPOTENCY_KEY_HEADER, key)
            .header(EXPECTED_REVISION_HEADER, expected);
        if dry_run {
            request = request.header(DRY_RUN_HEADER, "true");
        }
        self.send(
            request
                .body(Body::from(body.to_string()))
                .expect("a request"),
        )
        .await
    }

    /// Publish a document that is expected to succeed, returning the revision it
    /// published — which is the expected revision of whatever comes next.
    async fn publish(&self, path: &str, key: &str, expected: &str, body: &Value) -> String {
        let (status, response) = self.post(path, key, expected, body).await;
        assert_eq!(status, StatusCode::OK, "{path} refused: {response}");
        assert_eq!(response["result"], "published", "{path}: {response}");
        response["revision"]
            .as_str()
            .expect("a published revision")
            .to_owned()
    }
}

// ---------------------------------------------------------------------------
// The documents, as a caller writes them
// ---------------------------------------------------------------------------

fn tenant_document() -> Value {
    json!({
        "summary": "onboard the acme tenant",
        "mutation": "create",
        "resource": {
            "tenant": fixtures::tenant_id(1).to_string(),
            "slug": "acme",
            "display_name": "Acme",
        }
    })
}

fn project_document() -> Value {
    json!({
        "summary": "add acme's production project",
        "mutation": "create",
        "resource": {
            "project": fixtures::project_id(2).to_string(),
            "tenant": fixtures::tenant_id(1).to_string(),
            "slug": "production",
            "display_name": "Production",
        }
    })
}

fn provider_document() -> Value {
    json!({
        "summary": "connect acme to openai",
        "mutation": "create",
        "resource": {
            "provider": fixtures::resource_id(10).to_string(),
            "tenant": fixtures::tenant_id(1).to_string(),
            "slug": "openai",
            "display_name": "OpenAI",
            "wire_family": "openai-chat",
            "endpoint": "https://api.openai.com",
        }
    })
}

fn credential_document() -> Value {
    json!({
        "summary": "stage acme's openai key",
        "mutation": "create",
        "resource": {
            "credential": fixtures::resource_id(11).to_string(),
            "tenant": fixtures::tenant_id(1).to_string(),
            "provider": fixtures::resource_id(10).to_string(),
            "slug": "openai-primary",
            "display_name": "OpenAI primary",
            "secret": fixtures::secret_id(12).to_string(),
        }
    })
}

fn catalog_document() -> Value {
    let blob = *fixtures::blob_backed_catalog(13)
        .body
        .blob()
        .expect("a blob body");
    json!({
        "summary": "import the openai catalogue",
        "mutation": "create",
        "resource": {
            "catalog": fixtures::resource_id(13).to_string(),
            "slug": "openai-models",
            "digest": blob.digest.to_string(),
            "size_bytes": blob.size_bytes,
        }
    })
}

fn model_document() -> Value {
    json!({
        "summary": "enable gpt-4o for acme",
        "mutation": "create",
        "resource": {
            "enablement": fixtures::resource_id(14).to_string(),
            "tenant": fixtures::tenant_id(1).to_string(),
            "slug": "gpt-4o",
            "offering": fixtures::offering_id("gpt-4o").to_string(),
            "catalog": fixtures::resource_id(13).to_string(),
            "snapshot": fixtures::catalog_snapshot().to_string(),
            "wire_family": "openai-chat",
        }
    })
}

fn alias_document() -> Value {
    json!({
        "summary": "point acme's default alias at gpt-4o",
        "mutation": "create",
        "resource": {
            "alias": fixtures::resource_id(15).to_string(),
            "tenant": fixtures::tenant_id(1).to_string(),
            "project": fixtures::project_id(2).to_string(),
            "slug": "default",
            "wire_family": "openai-chat",
            "targets": [{ "enablement": fixtures::resource_id(14).to_string() }],
        }
    })
}

fn policy_document() -> Value {
    json!({
        "summary": "cap acme's spend and concurrency",
        "resource": {
            "tenant": fixtures::tenant_id(1).to_string(),
            "slug": "acme-limits",
            "epoch": 1,
            "subject_limit_microdollars": 50_000_000u64,
            "namespace_limit_microdollars": 500_000_000u64,
            "reservation_ttl_seconds": 300,
            "max_in_flight_per_subject": 8,
            "lease_ttl_seconds": 60,
        }
    })
}

/// The whole deployment, in the order an operator builds it: each document
/// published against the revision the previous one produced.
async fn build(deployment: &Deployment) -> String {
    let documents = [
        ("/tenants", tenant_document()),
        ("/projects", project_document()),
        ("/providers", provider_document()),
        ("/credentials", credential_document()),
        ("/catalogs", catalog_document()),
        ("/models", model_document()),
        ("/aliases", alias_document()),
        ("/policies", policy_document()),
    ];
    let mut expected = EXPECTED_REVISION_EMPTY.to_owned();
    for (index, (path, document)) in documents.iter().enumerate() {
        expected = deployment
            .publish(path, &format!("key-{index}"), &expected, document)
            .await;
    }
    expected
}

// ---------------------------------------------------------------------------
// Building a deployment
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_resource_family_publishes_and_is_readable_as_state() {
    let deployment = Deployment::new();
    let head = build(&deployment).await;
    assert_eq!(deployment.store.published_revisions(), 8);

    let (status, state) = deployment.get("/state").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(state["revision"], head);
    let kinds: Vec<&str> = state["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .map(|resource| resource["kind"].as_str().expect("a kind"))
        .collect();
    for kind in [
        "tenant",
        "project",
        "provider",
        "provider-credential",
        "catalog-model",
        "model-enablement",
        "alias",
        "policy",
    ] {
        assert!(kinds.contains(&kind), "{kind} is missing from {kinds:?}");
    }
    // A state read describes bodies, never renders them: no secret reference's
    // material, and no body payload, can appear in the projection.
    let rendered = state.to_string();
    assert!(!rendered.contains("secret_material"), "{rendered}");
}

/// Republishing a credential reauthors it: the document is the complete
/// credential, so a repointed secret takes effect — and takes the credential
/// back to `staged`, because material serves only after a candidate compiles
/// against it.
#[tokio::test]
async fn republishing_a_credential_repoints_it_at_the_material_the_document_names() {
    let deployment = Deployment::new();
    let mut head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    head = deployment
        .publish("/providers", "key-2", &head, &provider_document())
        .await;
    head = deployment
        .publish("/credentials", "key-3", &head, &credential_document())
        .await;

    let mut repointed = credential_document();
    repointed["mutation"] = json!("update");
    repointed["resource"]["display_name"] = json!("OpenAI rotated");
    repointed["resource"]["secret"] = fixtures::secret_id(13).to_string().into();
    let head = deployment
        .publish("/credentials", "key-4", &head, &repointed)
        .await;

    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(&head).expect("a revision"))
        .await
        .expect("the published revision hydrates");
    let credential = loaded
        .state()
        .version_of(
            crate::desired_state::ResourceKind::ProviderCredential,
            fixtures::resource_id(11),
        )
        .expect("the credential is desired");
    let body =
        crate::desired_state::ProviderCredentialBody::read(credential).expect("a credential body");
    assert_eq!(body.secret().secret, fixtures::secret_id(13));
    assert_eq!(body.display_name().as_str(), "OpenAI rotated");
    assert_eq!(
        body.lifecycle(),
        crate::desired_state::SecretLifecycle::Staged
    );
}

/// Rotation advances the version the credential is *in*, not the one the
/// document spells. The document names a credential, and `secret_version` is
/// optional, so rotating from a body already past its first version must not
/// fall back to the document's default and hand an operator an older secret
/// under the name of a rotation.
#[tokio::test]
async fn rotating_a_credential_advances_the_version_currently_in_force() {
    let deployment = Deployment::new();
    let mut head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    head = deployment
        .publish("/providers", "key-2", &head, &provider_document())
        .await;
    head = deployment
        .publish("/credentials", "key-3", &head, &credential_document())
        .await;

    let mut rotate = credential_document();
    rotate["mutation"] = json!("update");
    rotate["resource"]["rotate"] = json!(true);
    for (key, expected) in [2_u64, 3, 4].into_iter().enumerate() {
        head = deployment
            .publish("/credentials", &format!("key-{}", key + 4), &head, &rotate)
            .await;
        assert_eq!(
            credential_secret(&deployment, &head).await.version.get(),
            expected,
            "each rotation advances from the version in force"
        );
    }

    // The material a rotation lands on is staged: rotation stores material, and
    // putting it in service stays a separate decision.
    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(&head).expect("a revision"))
        .await
        .expect("the published revision hydrates");
    let credential = loaded
        .state()
        .version_of(
            crate::desired_state::ResourceKind::ProviderCredential,
            fixtures::resource_id(11),
        )
        .expect("the credential is desired");
    let body =
        crate::desired_state::ProviderCredentialBody::read(credential).expect("a credential body");
    assert_eq!(
        body.lifecycle(),
        crate::desired_state::SecretLifecycle::Staged
    );
    assert_eq!(body.secret().secret, fixtures::secret_id(12));
}

/// An edit that says nothing about material must not move any: `secret_version`
/// is unstated when omitted, not "version 1". A rename that republished a
/// rotated credential at v1 would re-stage it — taking the credential out of
/// service — and nothing in the document would have said so.
#[tokio::test]
async fn editing_a_credential_without_a_version_keeps_the_one_in_force() {
    let deployment = Deployment::new();
    let mut head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    head = deployment
        .publish("/providers", "key-2", &head, &provider_document())
        .await;
    head = deployment
        .publish("/credentials", "key-3", &head, &credential_document())
        .await;

    let mut rotate = credential_document();
    rotate["mutation"] = json!("update");
    rotate["resource"]["rotate"] = json!(true);
    head = deployment
        .publish("/credentials", "key-4", &head, &rotate)
        .await;

    let mut activate = credential_document();
    activate["mutation"] = json!("update");
    activate["resource"]["lifecycle"] = json!("active");
    head = deployment
        .publish("/credentials", "key-5", &head, &activate)
        .await;

    let mut renamed = credential_document();
    renamed["mutation"] = json!("update");
    renamed["resource"]["display_name"] = json!("OpenAI primary (eu)");
    head = deployment
        .publish("/credentials", "key-6", &head, &renamed)
        .await;

    let body = credential_body(&deployment, &head).await;
    assert_eq!(
        body.secret().version.get(),
        2,
        "a rename says nothing about material, so the version in force stands"
    );
    assert_eq!(
        body.lifecycle(),
        crate::desired_state::SecretLifecycle::Active,
        "and the credential stays in service"
    );
    assert_eq!(body.display_name().as_str(), "OpenAI primary (eu)");
}

/// The credential fixture's body at `head`.
async fn credential_body(
    deployment: &Deployment,
    head: &str,
) -> crate::desired_state::ProviderCredentialBody {
    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(head).expect("a revision"))
        .await
        .expect("the published revision hydrates");
    let credential = loaded
        .state()
        .version_of(
            crate::desired_state::ResourceKind::ProviderCredential,
            fixtures::resource_id(11),
        )
        .expect("the credential is desired");
    crate::desired_state::ProviderCredentialBody::read(credential).expect("a credential body")
}

/// The secret reference the credential fixture's resource holds at `head`.
async fn credential_secret(deployment: &Deployment, head: &str) -> crate::desired_state::SecretRef {
    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(head).expect("a revision"))
        .await
        .expect("the published revision hydrates");
    let credential = loaded
        .state()
        .version_of(
            crate::desired_state::ResourceKind::ProviderCredential,
            fixtures::resource_id(11),
        )
        .expect("the credential is desired");
    crate::desired_state::ProviderCredentialBody::read(credential)
        .expect("a credential body")
        .secret()
}

/// A resource other resources pin can still be advanced. Dependency edges name
/// an exact version and one request publishes one resource, so the candidate
/// carries the dependents forward itself rather than leaving an operator with a
/// deployment that can never be changed again.
#[tokio::test]
async fn advancing_a_resource_other_resources_pin_carries_those_resources_forward() {
    let deployment = Deployment::new();
    let mut head = build(&deployment).await;

    let mut disabled = model_document();
    disabled["mutation"] = json!("update");
    disabled["resource"]["state"] = json!("disabled");
    let (status, outcome) = deployment
        .post("/models", "key-model-2", &head, &disabled)
        .await;
    assert_eq!(status, StatusCode::OK, "{outcome}");
    let retired_revision = outcome["revision"].as_str().expect("a revision").to_owned();
    let alias_delta = outcome["diff"]["resources"]
        .as_array()
        .expect("resource diff")
        .iter()
        .find(|delta| delta["kind"] == "alias")
        .expect("the implicitly retired alias is in the revision diff");
    assert_eq!(alias_delta["change"], "updated");
    let (status, audit) = deployment.get(&format!("/audit/{retired_revision}")).await;
    assert_eq!(status, StatusCode::OK, "{audit}");
    assert_eq!(audit["events"].as_array().expect("audit events").len(), 1);
    head = retired_revision;

    let mut reimported = catalog_document();
    reimported["mutation"] = json!("update");
    reimported["resource"]["size_bytes"] = json!(4_096);
    head = deployment
        .publish("/catalogs", "key-catalog-2", &head, &reimported)
        .await;

    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(&head).expect("a revision"))
        .await
        .expect("the published revision hydrates");
    let state = loaded.state();
    let alias = state
        .version_of(
            crate::desired_state::ResourceKind::Alias,
            fixtures::resource_id(15),
        )
        .expect("the alias is desired");
    let alias_body = crate::desired_state::ModelAliasBody::read(alias).expect("an alias body");
    assert_eq!(
        (alias_body.is_enabled(), alias_body.targets().len()),
        (false, 0),
        "retiring the last target retires the alias in the same revision"
    );
    // A stale alias write cannot reactivate a name against the retired model.
    let (status, error) = deployment
        .post("/aliases", "key-alias-2", &head, &alias_document())
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(error["error"]["type"], "validation_failed", "{error}");
}

#[tokio::test]
async fn republishing_an_already_disabled_enablement_preserves_a_disabled_alias_target() {
    let deployment = Deployment::new();
    let mut head = build(&deployment).await;

    let mut disable_model = model_document();
    disable_model["mutation"] = json!("update");
    disable_model["resource"]["state"] = json!("disabled");
    head = deployment
        .publish("/models", "key-model-disable", &head, &disable_model)
        .await;

    // An explicitly disabled alias may retain a historical target. This is the
    // legacy shape restack must preserve when the already-disabled enablement is
    // republished for metadata or catalogue carry-forward.
    let mut disabled_alias = alias_document();
    disabled_alias["mutation"] = json!("update");
    disabled_alias["resource"]["state"] = json!("disabled");
    disabled_alias["resource"]["targets"] =
        json!([{ "enablement": fixtures::resource_id(14).to_string(), "version": 2 }]);
    head = deployment
        .publish(
            "/aliases",
            "key-alias-legacy-target",
            &head,
            &disabled_alias,
        )
        .await;

    let mut republish_disabled = model_document();
    republish_disabled["mutation"] = json!("update");
    republish_disabled["resource"]["state"] = json!("disabled");
    republish_disabled["resource"]["observed_input_micros_per_million"] = json!(3_000);
    republish_disabled["resource"]["observed_output_micros_per_million"] = json!(2_000);
    let (status, outcome) = deployment
        .post(
            "/models",
            "key-model-republish-disabled",
            &head,
            &republish_disabled,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{outcome}");
    let next = outcome["revision"].as_str().expect("a revision").to_owned();

    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(&next).expect("a revision"))
        .await
        .expect("the republished revision hydrates");
    let alias = loaded
        .state()
        .version_of(
            crate::desired_state::ResourceKind::Alias,
            fixtures::resource_id(15),
        )
        .expect("the alias is desired");
    let body = crate::desired_state::ModelAliasBody::read(alias).expect("an alias body");
    assert!(!body.is_enabled());
    assert_eq!(
        body.targets(),
        &[crate::desired_state::AliasTarget::new(
            fixtures::resource_id(14),
            crate::desired_state::ResourceVersionNumber::new(3).expect("version"),
        )],
        "republication is not an enabled -> disabled transition"
    );

    // The complete revision/diff is the resource-level audit plan: the alias
    // retirement is visible alongside the mutation-intent audit event.
    let alias_delta = outcome["diff"]["resources"]
        .as_array()
        .expect("resource diff")
        .iter()
        .find(|delta| delta["kind"] == "alias")
        .expect("the carried alias is recorded in the revision diff");
    assert_eq!(alias_delta["change"], "updated");
    let (status, audit) = deployment.get(&format!("/audit/{next}")).await;
    assert_eq!(status, StatusCode::OK, "{audit}");
    assert_eq!(
        audit["events"].as_array().expect("audit events").len(),
        1,
        "one mutation event owns the revision"
    );
}

#[tokio::test]
async fn disabling_an_enablement_preserves_targets_of_a_disabled_alias() {
    let deployment = Deployment::new();
    let mut head = build(&deployment).await;

    let mut disabled_alias = alias_document();
    disabled_alias["mutation"] = json!("update");
    disabled_alias["resource"]["state"] = json!("disabled");
    head = deployment
        .publish(
            "/aliases",
            "key-alias-disabled-history",
            &head,
            &disabled_alias,
        )
        .await;

    let mut disable_model = model_document();
    disable_model["mutation"] = json!("update");
    disable_model["resource"]["state"] = json!("disabled");
    head = deployment
        .publish(
            "/models",
            "key-model-disable-history",
            &head,
            &disable_model,
        )
        .await;

    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(&head).expect("a revision"))
        .await
        .expect("the retirement revision hydrates");
    let alias = loaded
        .state()
        .version_of(
            crate::desired_state::ResourceKind::Alias,
            fixtures::resource_id(15),
        )
        .expect("the disabled alias is retained");
    let body = crate::desired_state::ModelAliasBody::read(alias).expect("an alias body");
    assert!(!body.is_enabled());
    assert_eq!(
        body.targets().len(),
        1,
        "disabled alias history is retained"
    );
    assert_eq!(body.targets()[0].version.get(), 2);
}

#[tokio::test]
async fn disabling_an_alias_can_clear_targets_in_one_revision() {
    let deployment = Deployment::new();
    let mut head = build(&deployment).await;
    let mut disabled = alias_document();
    disabled["mutation"] = json!("update");
    disabled["resource"]["state"] = json!("disabled");
    disabled["resource"]["targets"] = json!([]);

    head = deployment
        .publish("/aliases", "key-alias-disable", &head, &disabled)
        .await;

    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(&head).expect("a revision"))
        .await
        .expect("the published revision hydrates");
    let alias = loaded
        .state()
        .version_of(
            crate::desired_state::ResourceKind::Alias,
            fixtures::resource_id(15),
        )
        .expect("the disabled alias is retained");
    let body = crate::desired_state::ModelAliasBody::read(alias).expect("an alias body");
    assert!(!body.is_enabled());
    assert!(body.targets().is_empty());
}

/// A refreshed catalogue is a new snapshot, and an enablement's snapshot is part
/// of what it is: re-importing different content under a row an enablement reads
/// from is refused by name, rather than published into a state whose pins no
/// longer resolve.
#[tokio::test]
async fn refreshing_a_catalogue_an_enablement_reads_from_is_refused_by_name() {
    let deployment = Deployment::new();
    let head = build(&deployment).await;

    let refreshed = *fixtures::second_blob_backed_catalog(23)
        .body
        .blob()
        .expect("a blob body");
    let mut reimported = catalog_document();
    reimported["mutation"] = json!("update");
    reimported["resource"]["digest"] = refreshed.digest.to_string().into();
    reimported["resource"]["size_bytes"] = json!(refreshed.size_bytes);

    let (status, error) = deployment
        .post("/catalogs", "key-catalog-refresh", &head, &reimported)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
    assert_eq!(error["error"]["type"], "validation_failed", "{error}");
    assert_eq!(
        error["error"]["rule"], "pinned_snapshot_withdrawn",
        "the refusal names why, not just that: {error}"
    );
    assert_eq!(
        deployment.store.published_revisions(),
        8,
        "a refused candidate publishes nothing"
    );
}

/// The way through the refusal above: the refreshed snapshot arrives as its own
/// catalogue resource, and offerings are enabled against it, leaving the
/// enablements that read the old snapshot to be retired on their own schedule.
#[tokio::test]
async fn a_refreshed_catalogue_is_imported_as_its_own_resource_and_enabled_against() {
    let deployment = Deployment::new();
    let mut head = build(&deployment).await;

    let refreshed = *fixtures::second_blob_backed_catalog(23)
        .body
        .blob()
        .expect("a blob body");
    let mut imported = catalog_document();
    imported["resource"]["catalog"] = fixtures::resource_id(23).to_string().into();
    imported["resource"]["slug"] = json!("openai-models-2026-08");
    imported["resource"]["digest"] = refreshed.digest.to_string().into();
    imported["resource"]["size_bytes"] = json!(refreshed.size_bytes);
    head = deployment
        .publish("/catalogs", "key-catalog-refresh", &head, &imported)
        .await;

    let mut disabled = model_document();
    disabled["mutation"] = json!("update");
    disabled["resource"]["state"] = json!("disabled");
    head = deployment
        .publish("/models", "key-model-retire", &head, &disabled)
        .await;

    let mut enabled = model_document();
    enabled["resource"]["enablement"] = fixtures::resource_id(24).to_string().into();
    enabled["resource"]["slug"] = json!("gpt-4o-2026-08");
    enabled["resource"]["catalog"] = fixtures::resource_id(23).to_string().into();
    enabled["resource"]["snapshot"] = refreshed.digest.to_string().into();
    head = deployment
        .publish("/models", "key-model-refresh", &head, &enabled)
        .await;

    let mut retargeted = alias_document();
    retargeted["mutation"] = json!("update");
    retargeted["resource"]["targets"] =
        json!([{ "enablement": fixtures::resource_id(24).to_string() }]);
    let head = deployment
        .publish("/aliases", "key-alias-refresh", &head, &retargeted)
        .await;

    let loaded = deployment
        .store
        .load_revision(crate::desired_state::RevisionId::parse(&head).expect("a revision"))
        .await
        .expect("the published revision hydrates");
    let state = loaded.state();
    let enablement = state
        .version_of(
            crate::desired_state::ResourceKind::ModelEnablement,
            fixtures::resource_id(24),
        )
        .expect("the refreshed enablement is desired");
    let body =
        crate::desired_state::ModelEnablementBody::read(enablement).expect("an enablement body");
    assert!(
        body.offering().is_pinned_to(refreshed.digest),
        "the new enablement reads the refreshed snapshot"
    );
    let alias = state
        .version_of(
            crate::desired_state::ResourceKind::Alias,
            fixtures::resource_id(15),
        )
        .expect("the alias is desired");
    let alias_body = crate::desired_state::ModelAliasBody::read(alias).expect("an alias body");
    assert_eq!(
        alias_body.primary().expect("a target").enablement,
        fixtures::resource_id(24),
        "the alias serves the refreshed enablement"
    );
}

#[tokio::test]
async fn a_second_publication_of_a_resource_supersedes_it_rather_than_duplicating_it() {
    let deployment = Deployment::new();
    let head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    let mut renamed = tenant_document();
    renamed["resource"]["display_name"] = json!("Acme Corporation");
    renamed["mutation"] = json!("update");
    deployment
        .publish("/tenants", "key-2", &head, &renamed)
        .await;

    let (_, state) = deployment.get("/state").await;
    let tenants: Vec<&Value> = state["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .filter(|resource| resource["kind"] == "tenant")
        .collect();
    assert_eq!(tenants.len(), 1, "the tenant was duplicated: {tenants:?}");
    assert_eq!(tenants[0]["version"], 2);
}

// ---------------------------------------------------------------------------
// Preconditions: conflict, replay, reuse
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_stale_expected_revision_is_a_conflict_that_names_the_head() {
    let deployment = Deployment::new();
    let head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;

    // A second writer, still holding "the control plane is empty".
    let (status, body) = deployment
        .post(
            "/projects",
            "key-2",
            EXPECTED_REVISION_EMPTY,
            &project_document(),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["type"], "revision_conflict");
    assert_eq!(body["error"]["revision"], head);
    assert_eq!(deployment.store.published_revisions(), 1);

    // Re-read, retry: the same document lands against the head it conflicted on.
    deployment
        .publish("/projects", "key-3", &head, &project_document())
        .await;
}

#[tokio::test]
async fn a_lost_response_is_replayed_rather_than_published_twice() {
    let deployment = Deployment::new();
    let first = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;

    // The client never saw the response and retries byte-for-byte, including the
    // expected revision it was written against.
    let (status, body) = deployment
        .post(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "replayed");
    assert_eq!(body["revision"], first);
    assert_eq!(
        deployment.store.published_revisions(),
        1,
        "a retry published a second revision"
    );
}

#[tokio::test]
async fn an_idempotency_key_cannot_be_spent_on_a_different_candidate() {
    let deployment = Deployment::new();
    deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;

    let mut different = tenant_document();
    different["resource"]["display_name"] = json!("Not Acme");
    let (status, body) = deployment
        .post("/tenants", "key-1", EXPECTED_REVISION_EMPTY, &different)
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"]["type"], "idempotency_key_reused");
    assert_eq!(deployment.store.published_revisions(), 1);
}

// ---------------------------------------------------------------------------
// Dry run
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_dry_run_diffs_the_candidate_and_leaves_the_store_untouched() {
    let deployment = Deployment::new();
    let (status, body) = deployment
        .dry_run(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], "dry_run");
    assert_eq!(body["mode"], "dry-run");
    assert_eq!(body["diff"]["summary"]["added"], 1);
    assert!(body.get("revision").is_none());
    assert_eq!(deployment.store.published_revisions(), 0);

    // And the key it rehearsed with is still spendable: a rehearsal consumes
    // nothing.
    deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
}

#[tokio::test]
async fn a_dry_run_of_an_invalid_candidate_refuses_without_publishing() {
    let deployment = Deployment::new();
    let (status, body) = deployment
        .dry_run(
            "/models",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &model_document(),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "validation_failed");
    assert_eq!(deployment.store.published_revisions(), 0);
}

// ---------------------------------------------------------------------------
// Invalid graphs and malformed documents
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_enablement_pinning_a_catalogue_that_is_not_published_is_refused() {
    let deployment = Deployment::new();
    let head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    // The catalogue row this enablement depends on was never imported.
    let (status, body) = deployment
        .post("/models", "key-2", &head, &model_document())
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "validation_failed");
    assert_eq!(deployment.store.published_revisions(), 1);
}

#[tokio::test]
async fn an_alias_whose_target_does_not_exist_is_refused() {
    let deployment = Deployment::new();
    let mut head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    head = deployment
        .publish("/projects", "key-2", &head, &project_document())
        .await;
    let (status, body) = deployment
        .post("/aliases", "key-3", &head, &alias_document())
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "validation_failed");
    assert_eq!(deployment.store.published_revisions(), 2);
}

#[tokio::test]
async fn a_document_that_is_not_its_schema_is_refused_before_the_control_plane() {
    let store = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(store.clone()));
    let api = Arc::new(AdminApi::new(
        Arc::new(AdminService::stateful(counting.clone())),
        Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
        Arc::new(FakeAdminAuthorizer::permissive()),
    ));
    let deployment = Deployment {
        api,
        store,
        secrets: Arc::new(InMemorySecrets::new()),
    };

    let cases = [
        // An unknown field is a typo the caller must see, not an omission to
        // publish silently.
        (
            json!({
                "summary": "onboard acme",
                "resource": {
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "slug": "acme",
                    "display_name": "Acme",
                    "lifecycle_state": "active",
                }
            }),
            "admin_request_invalid",
        ),
        // An id that is not one.
        (
            json!({
                "summary": "onboard acme",
                "resource": {
                    "tenant": "acme",
                    "slug": "acme",
                    "display_name": "Acme",
                }
            }),
            "admin_request_invalid",
        ),
        // A value a future build might know, and this one does not.
        (
            json!({
                "summary": "onboard acme",
                "resource": {
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "slug": "acme",
                    "display_name": "Acme",
                    "lifecycle": "hibernating",
                }
            }),
            "admin_request_invalid",
        ),
        // A summary is required: an audit trail of empty strings is not one.
        (
            json!({
                "summary": "",
                "resource": {
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "slug": "acme",
                    "display_name": "Acme",
                }
            }),
            "audit_summary_invalid",
        ),
    ];
    for (document, code) in cases {
        let (status, body) = deployment
            .post("/tenants", "key-1", EXPECTED_REVISION_EMPTY, &document)
            .await;
        assert_eq!(body["error"]["type"], code, "{status}: {body}");
    }
    assert_eq!(
        counting.calls(),
        0,
        "a document this build cannot read reached the control plane"
    );
}

/// The budget settings share one lower bound, and so do the concurrency ones,
/// so a refusal that named the last one checked would send an administrator to
/// edit a field that was right: the message names the setting they actually set
/// to zero.
#[tokio::test]
async fn a_zero_budget_cap_is_refused_against_the_setting_the_caller_wrote() {
    let deployment = Deployment::new();
    let cases = [
        ("subject_limit_microdollars", json!(0)),
        ("namespace_limit_microdollars", json!(0)),
        ("reservation_ttl_seconds", json!(0)),
        ("max_in_flight_per_subject", json!(0)),
        ("lease_ttl_seconds", json!(0)),
    ];
    for (field, value) in cases {
        let mut document = policy_document();
        document["resource"][field] = value;
        let (status, body) = deployment
            .post("/policies", "key-1", EXPECTED_REVISION_EMPTY, &document)
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {body}");
        assert_eq!(body["error"]["type"], "admin_request_invalid", "{body}");
        let message = body["error"]["message"]
            .as_str()
            .unwrap_or_else(|| panic!("{field}: a message naming the field"));
        assert!(
            message.contains(&format!("`{field}`")),
            "{field} was set to zero, and the refusal says: {message}"
        );
    }
}

#[tokio::test]
async fn policy_middleware_is_typed_and_failed_publication_does_not_advance() {
    let deployment = Deployment::new();
    let expected = deployment
        .publish(
            "/tenants",
            "tenant",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    let mut registered = policy_document();
    registered["resource"]["content_middleware"] = json!([{
        "id": "axond.redact",
        "scopes": ["request", "response", "stream_event"],
        "failure_posture": "fail_closed",
        "max_duration_milliseconds": 25,
        "guardrail": {
            "key_env": "GW_GUARDRAIL_KEY",
            "rules": [
                {"id": "deny", "pattern": "forbidden", "action": "block"},
                {"id": "email", "pattern": "[a-z]+@example\\.com", "action": "redact"}
            ]
        }
    }]);
    let revision = deployment
        .publish("/policies", "registered", &expected, &registered)
        .await;

    let mut unavailable = policy_document();
    unavailable["resource"]["epoch"] = json!(2);
    unavailable["resource"]["content_middleware"] = json!([{
        "id": "future.middleware",
        "scopes": ["request"],
        "failure_posture": "fail_closed",
        "max_duration_milliseconds": 25,
    }]);
    let (status, body) = deployment
        .post("/policies", "unknown-middleware", &revision, &unavailable)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "validation_failed");
    assert_eq!(body["error"]["rule"], "content_middleware_unavailable");

    let mut fail_open_redaction = registered.clone();
    fail_open_redaction["resource"]["epoch"] = json!(2);
    fail_open_redaction["resource"]["content_middleware"][0]["failure_posture"] =
        json!("fail_open");
    let (status, body) = deployment
        .post(
            "/policies",
            "fail-open-redaction",
            &revision,
            &fail_open_redaction,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "admin_request_invalid");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires failure posture `fail_closed`")
    );

    let mut core_stage = policy_document();
    core_stage["resource"]["epoch"] = json!(2);
    core_stage["resource"]["content_middleware"] = json!([{
        "id": "authentication",
        "scopes": ["request"],
        "failure_posture": "fail_closed",
        "max_duration_milliseconds": 25,
    }]);
    let (status, body) = deployment
        .post("/policies", "core-stage", &revision, &core_stage)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "admin_request_invalid");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("compiled core stage")
    );

    let mut reorder = policy_document();
    reorder["resource"]["epoch"] = json!(2);
    reorder["resource"]["core_stages"] = json!(["authentication", "admission"]);
    let (status, body) = deployment
        .post("/policies", "core-order", &revision, &reorder)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "admin_request_invalid");

    let mut removed = policy_document();
    removed["resource"]["epoch"] = json!(2);
    let next = deployment
        .publish("/policies", "removed", &revision, &removed)
        .await;
    assert_ne!(
        next, revision,
        "the refused candidates consumed no revision"
    );
}

/// Nothing here removes a resource, so the audit trail may not say one was
/// removed: `delete` is accepted only for a document that retires the resource
/// through its own lifecycle, and refused for one that leaves it serving.
#[tokio::test]
async fn a_deletion_must_retire_the_resource_it_claims_to_delete() {
    let deployment = Deployment::new();
    let mut head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;

    let mut renamed = tenant_document();
    renamed["mutation"] = json!("delete");
    renamed["resource"]["display_name"] = json!("Acme, retired");
    let (status, body) = deployment.post("/tenants", "key-2", &head, &renamed).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "admin_request_invalid");

    let mut deleted = tenant_document();
    deleted["mutation"] = json!("delete");
    deleted["resource"]["lifecycle"] = json!("deleted");
    head = deployment
        .publish("/tenants", "key-3", &head, &deleted)
        .await;

    let (status, audit) = deployment.get(&format!("/audit/{head}")).await;
    assert_eq!(status, StatusCode::OK, "{audit}");
    assert_eq!(audit["events"][0]["kind"], "delete", "{audit}");
}

/// A handler parses a document whole, so the surface declares how much it will
/// buffer — and refuses the excess in its own envelope, before anything is
/// parsed or the control plane is touched.
#[tokio::test]
async fn an_oversized_document_is_refused_in_the_administrative_envelope() {
    let store = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(store.clone()));
    let api = Arc::new(AdminApi::new(
        Arc::new(AdminService::stateful(counting.clone())),
        Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
        Arc::new(FakeAdminAuthorizer::permissive()),
    ));
    let deployment = Deployment {
        api,
        store,
        secrets: Arc::new(InMemorySecrets::new()),
    };
    let mut document = tenant_document();
    document["summary"] = json!("x".repeat(ADMIN_MAX_REQUEST_BYTES + 1));

    let (status, body) = deployment
        .post("/tenants", "key-1", EXPECTED_REVISION_EMPTY, &document)
        .await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["type"], "admin_request_too_large");
    assert_eq!(
        counting.calls(),
        0,
        "an oversized body reached the control plane"
    );
}

#[tokio::test]
async fn an_unknown_administrative_path_answers_in_the_administrative_envelope() {
    let deployment = Deployment::new();
    let (status, body) = deployment.get("/tenants").await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(body["error"]["type"], "admin_method_not_allowed");

    let (status, body) = deployment.get("/nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "admin_route_not_found");
}

// ---------------------------------------------------------------------------
// Authentication and authorization
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_route_answers_without_an_administrative_credential() {
    let deployment = Deployment::new();
    for spec in super::admin_route_specs() {
        let path = format!("{ADMIN_PREFIX}{}", super::router::concrete_path(&spec));
        let builder = if spec.action.writes() {
            Request::post(&path)
                .header(IDEMPOTENCY_KEY_HEADER, "key-1")
                .header(EXPECTED_REVISION_HEADER, EXPECTED_REVISION_EMPTY)
        } else {
            Request::get(&path)
        };
        let (status, body) = deployment
            .send(builder.body(Body::empty()).expect("a request"))
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} answered: {body}");
        assert_eq!(body["error"]["type"], "admin_unauthenticated");
    }
    assert_eq!(deployment.store.published_revisions(), 0);
}

#[tokio::test]
async fn an_inference_credential_carries_no_administrative_authority() {
    let deployment = Deployment::new();
    for (name, value) in [
        (
            axum::http::header::AUTHORIZATION.as_str(),
            "Bearer axt1.token.signature",
        ),
        (INFERENCE_KEY_HEADER, "gateway-inference-key"),
    ] {
        let (status, body) = deployment
            .send(
                Request::post(format!("{ADMIN_PREFIX}/tenants"))
                    .header(name, value)
                    .header(IDEMPOTENCY_KEY_HEADER, "key-1")
                    .header(EXPECTED_REVISION_HEADER, EXPECTED_REVISION_EMPTY)
                    .body(Body::from(tenant_document().to_string()))
                    .expect("a request"),
            )
            .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["type"], "admin_unauthenticated");
    }
    assert_eq!(deployment.store.published_revisions(), 0);
}

#[tokio::test]
async fn a_tenant_scoped_administrator_cannot_publish_outside_its_tenant() {
    let tenant = ResourceScope::Tenant(fixtures::tenant_id(1));
    let deployment =
        Deployment::with_authorizer(FakeAdminAuthorizer::permissive().within(&[tenant]));

    // A tenant row is deployment-scoped: creating one is not a tenant
    // administrator's to authorize.
    let (status, body) = deployment
        .post(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["type"], "admin_forbidden");

    // And another tenant's project is refused on the scope in the *document*,
    // not on the caller's word.
    let mut elsewhere = project_document();
    elsewhere["resource"]["tenant"] = json!(fixtures::tenant_id(7).to_string());
    let (status, body) = deployment
        .post("/projects", "key-2", EXPECTED_REVISION_EMPTY, &elsewhere)
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["type"], "admin_forbidden");
    assert_eq!(deployment.store.published_revisions(), 0);
}

// ---------------------------------------------------------------------------
// The management catalogue
// ---------------------------------------------------------------------------

/// The read a tenant administrator makes after publishing: the enablement it
/// created, the alias that names it, and the facts this build could not consult.
#[tokio::test]
async fn the_management_catalogue_reports_what_a_tenant_published() {
    let deployment = Deployment::new();
    let head = build(&deployment).await;

    let (status, view) = deployment
        .get(&format!("/catalogue?tenant={}", fixtures::tenant_id(1)))
        .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["revision"], head);
    assert_eq!(view["scope"]["kind"], "tenant");

    let entries = view["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 1, "{view}");
    let entry = &entries[0];
    assert_eq!(entry["slug"], "gpt-4o");
    assert_eq!(
        entry["offering"],
        fixtures::offering_id("gpt-4o").to_string()
    );
    assert_eq!(
        entry["catalog_snapshot"],
        fixtures::catalog_snapshot().to_string()
    );
    assert_eq!(entry["state"], "enabled");
    assert_eq!(entry["aliases"], json!(["default"]));
    let aliases = view["aliases"].as_array().expect("aliases");
    assert_eq!(aliases.len(), 1, "{view}");
    assert_eq!(aliases[0]["slug"], "default");
    assert_eq!(aliases[0]["scope"]["kind"], "project");
    assert_eq!(aliases[0]["routable"], json!(false));
    assert_eq!(aliases[0]["unavailable"], json!([]));
    assert_eq!(aliases[0]["targets"].as_array().unwrap().len(), 1);
    // Offering metadata was not consultable, so Unpriced is pending rather than
    // a definitive verdict. billable stays false and agrees with price; routable
    // requires a covering compiled price.
    assert_eq!(entry["billable"], json!(false));
    assert!(entry.get("price").is_none(), "{entry}");
    assert_eq!(entry["routable"], json!(false));
    assert_eq!(entry["unavailable"], json!([]));
    assert_eq!(
        view["pending"],
        json!(["offering-metadata", "availability"])
    );

    let (status, filtered) = deployment
        .get(&format!(
            "/catalogue?tenant={}&state=enabled&wire_family=openai-chat&offering={}&billable=false",
            fixtures::tenant_id(1),
            fixtures::offering_id("gpt-4o")
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{filtered}");
    assert_eq!(filtered["entries"].as_array().expect("entries").len(), 1);
    assert_eq!(filtered["entries"][0]["slug"], "gpt-4o");
}

/// Compiled `PricingSnapshot::price` decides `billable`, not `approved_price`.
#[tokio::test]
async fn the_management_catalogue_treats_a_covering_book_as_billable() {
    const CATALOG: &str = include_str!("../backends/fixtures/models_dev/catalog.identity.json");
    let snapshot = ModelsDevAdapter::default()
        .parse(
            CATALOG.as_bytes(),
            SourceValidators::default(),
            std::time::UNIX_EPOCH,
        )
        .expect("the catalogue fixture parses");
    let store = Arc::new(InMemoryCatalogStore::new());
    store
        .activate(
            &RetainedCatalog {
                source: snapshot.source.clone(),
                payload: RawPayload::new(CATALOG.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("the exact payload is retained");
    let deployment = Deployment::with_catalogue(store);
    let digest = snapshot.source.raw.digest;
    let offering = OfferingId::of("openai", "openai/gpt-5.5").expect("an offering id");

    let mut expected = EXPECTED_REVISION_EMPTY.to_owned();
    expected = deployment
        .publish("/tenants", "key-0", &expected, &tenant_document())
        .await;
    expected = deployment
        .publish("/projects", "key-1", &expected, &project_document())
        .await;
    expected = deployment
        .publish(
            "/catalogs",
            "key-2",
            &expected,
            &json!({
                "summary": "import the openai catalogue",
                "mutation": "create",
                "resource": {
                    "catalog": fixtures::resource_id(13).to_string(),
                    "slug": "openai-models",
                    "digest": digest.to_string(),
                    "size_bytes": snapshot.source.raw.size_bytes,
                }
            }),
        )
        .await;
    expected = deployment
        .publish(
            "/models",
            "key-3",
            &expected,
            &json!({
                "summary": "enable gpt-4o for acme",
                "mutation": "create",
                "resource": {
                    "enablement": fixtures::resource_id(14).to_string(),
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "slug": "gpt-4o",
                    "offering": offering.to_string(),
                    "catalog": fixtures::resource_id(13).to_string(),
                    "snapshot": digest.to_string(),
                    "wire_family": "openai-chat",
                }
            }),
        )
        .await;
    expected = deployment
        .publish("/aliases", "key-4", &expected, &alias_document())
        .await;
    let _head = deployment
        .publish(
            "/prices",
            "key-5",
            &expected,
            &json!({
                "summary": "approve openai rates",
                "mutation": "create",
                "resource": {
                    "price_book": fixtures::resource_id(31).to_string(),
                    "slug": "deployment-prices",
                    "catalog": snapshot.content.content_id().checksum().to_string(),
                    "catalog_version": 1,
                    "state": "approved",
                    "approved_at_millis": 0,
                    "rules": [{
                        "provider": "openai",
                        "model": "openai/gpt-5.5",
                        "precedence": "baseline",
                        "from_millis": 0,
                        "input_nano_dollars_per_million": 2_500_000u64,
                        "output_nano_dollars_per_million": 10_000_000u64,
                        "origin": "operator"
                    }]
                }
            }),
        )
        .await;

    let (status, view) = deployment
        .get(&format!("/catalogue?tenant={}", fixtures::tenant_id(1)))
        .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    let entry = &view["entries"].as_array().expect("entries")[0];
    assert_eq!(entry["slug"], "gpt-4o");
    assert!(entry.get("price").is_some(), "{entry}");
    assert_eq!(entry["billable"], json!(true), "{entry}");
    assert_eq!(
        entry["billable"].as_bool(),
        Some(entry.get("price").is_some()),
        "{entry}"
    );
    assert_eq!(entry["unavailable"], json!([]), "{entry}");
    assert!(entry.get("notices").is_none(), "{entry}");
    assert_eq!(view["aliases"][0]["routable"], json!(true), "{view}");
    assert_eq!(view["aliases"][0]["unavailable"], json!([]), "{view}");
}

/// The scope is a request parameter, so it is checked against the grant like any
/// other: a tenant administrator cannot read another tenant's catalogue, and the
/// refusal does not tell it whether that tenant has anything enabled.
#[tokio::test]
async fn a_catalogue_read_outside_the_grant_is_forbidden() {
    let built = Deployment::new();
    build(&built).await;
    let deployment = built.narrowed(&[
        ResourceScope::Tenant(fixtures::tenant_id(1)),
        ResourceScope::Project {
            tenant: fixtures::tenant_id(1),
            project: fixtures::project_id(2),
        },
    ]);

    let (status, body) = deployment
        .get(&format!("/catalogue?tenant={}", fixtures::tenant_id(7)))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["type"], "admin_forbidden");

    // The scope a project read is checked against is the pair, so a grant that
    // covers the pair reads it and one that names another tenant does not.
    let (status, view) = deployment
        .get(&format!(
            "/catalogue?tenant={}&project={}",
            fixtures::tenant_id(1),
            fixtures::project_id(2)
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["scope"]["kind"], "project");
    assert_eq!(view["entries"].as_array().expect("entries").len(), 1);
}

/// A malformed filter is a typed refusal rather than a silently ignored
/// parameter: a caller that filtered on a spelling or enum this build does not
/// know must not be handed an unfiltered catalogue and believe it was filtered.
#[tokio::test]
async fn a_catalogue_filter_that_cannot_be_parsed_is_refused() {
    let deployment = Deployment::new();
    build(&deployment).await;

    for query in [
        "tenant=not-a-uuid".to_owned(),
        format!("tenant={}&state=retired", fixtures::tenant_id(1)),
        format!("tenant={}&wire_family=telepathy", fixtures::tenant_id(1)),
        format!("tenant={}&offering=nonsense", fixtures::tenant_id(1)),
        format!("tenant={}&capability=telepathy", fixtures::tenant_id(1)),
        format!("tenant={}&modality=telepathy", fixtures::tenant_id(1)),
        format!("tenant={}&lifecycle=telepathy", fixtures::tenant_id(1)),
        format!("tenant={}&availability=telepathy", fixtures::tenant_id(1)),
        format!("tenant={}&unknown=1", fixtures::tenant_id(1)),
        format!("tenant={}&source=invented", fixtures::tenant_id(1)),
        format!("tenant={}&source=imported", fixtures::tenant_id(1)),
        format!(
            "tenant={}&source=imported&provider=",
            fixtures::tenant_id(1)
        ),
        format!("tenant={}&source=imported&q=", fixtures::tenant_id(1)),
        format!(
            "tenant={}&source=imported&provider=%20",
            fixtures::tenant_id(1)
        ),
        format!("tenant={}&q=ab", fixtures::tenant_id(1)),
        format!("tenant={}&source=imported&q=ab", fixtures::tenant_id(1)),
        format!(
            "tenant={}&state=enabled&state=disabled",
            fixtures::tenant_id(1)
        ),
        "project=nothing".to_owned(),
    ] {
        let (status, body) = deployment.get(&format!("/catalogue?{query}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}: {body}");
        assert_eq!(body["error"]["type"], "admin_request_invalid", "{query}");
    }

    let oversized = format!(
        "tenant={}&offering={}",
        fixtures::tenant_id(1),
        "x".repeat(super::handlers::CATALOGUE_MAX_QUERY_BYTES)
    );
    let (status, body) = deployment.get(&format!("/catalogue?{oversized}")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "admin_request_invalid");
    assert!(!body.to_string().contains(&"x".repeat(64)));
}

#[tokio::test]
async fn imported_browse_lists_not_enabled_offerings() {
    const CATALOG: &str = include_str!("../backends/fixtures/models_dev/catalog.identity.json");
    let snapshot = ModelsDevAdapter::default()
        .parse(
            CATALOG.as_bytes(),
            SourceValidators::default(),
            std::time::UNIX_EPOCH,
        )
        .expect("the catalogue fixture parses");
    let store = Arc::new(InMemoryCatalogStore::new());
    store
        .activate(
            &RetainedCatalog {
                source: snapshot.source.clone(),
                payload: RawPayload::new(CATALOG.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("the exact payload is retained");
    let deployment = Deployment::with_catalogue(store);
    deployment
        .publish(
            "/tenants",
            "key-0",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;

    let (status, view) = deployment
        .get(&format!(
            "/catalogue?tenant={}&source=imported&provider=hpc-ai",
            fixtures::tenant_id(1)
        ))
        .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    let entries = view["entries"].as_array().expect("entries");
    assert_eq!(entries.len(), 1, "{view}");
    assert!(entries[0].get("enablement").is_none(), "{view}");
    assert!(entries[0].get("slug").is_none(), "{view}");
    assert!(entries[0].get("state").is_none(), "{view}");
    assert_eq!(entries[0]["unavailable"], json!(["not-enabled"]));
    assert_eq!(entries[0]["routable"], json!(false));
    assert_eq!(entries[0]["billable"], json!(false));
    assert_eq!(entries[0]["metadata"]["provider"], "hpc-ai");
}

/// Refresh is a write without a revision: no idempotency key, no expected
/// revision, and the control plane does not move.
#[tokio::test]
async fn catalogue_refresh_does_not_require_idempotency_and_does_not_publish() {
    let (status, body) = Deployment::new()
        .post_material("/catalogue/refresh", &json!({}))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "admin_request_invalid");
    assert_ne!(body["error"]["type"], "idempotency_key_required");
    assert_ne!(body["error"]["type"], "expected_revision_required");
}

#[tokio::test]
async fn catalogue_refresh_returns_impact_without_a_revision_bump() {
    let handle = crate::backends::catalog_runtime::start(
        &crate::config::CatalogConfig {
            source: crate::config::CatalogSourceBackend::Seed,
            store: crate::config::CatalogStoreBackend::InMemory,
            bootstrap: crate::config::CatalogBootstrap::Seed,
            refresh_interval_seconds: 86_400,
            ..crate::config::CatalogConfig::default()
        },
        None,
        &std::collections::HashMap::new(),
        std::future::pending(),
    )
    .await
    .expect("an offline catalogue starts")
    .expect("an enabled catalogue yields a handle");
    let deployment = Deployment::with_catalog_handle(handle);
    let head = deployment
        .publish(
            "/tenants",
            "key-0",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    let published = deployment.store.published_revisions();

    let (status, body) = deployment
        .post_material("/catalogue/refresh", &json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("revision").is_none(), "{body}");
    assert!(body["catalogue"]["content_id"].is_string(), "{body}");
    assert_eq!(body["catalogue"]["consecutive_refusals"], json!(0));
    assert_eq!(body["impact"]["pins_unmoved"], json!(0));
    assert_eq!(body["impact"]["withdrawn"], json!([]));
    assert_eq!(deployment.store.published_revisions(), published);

    let (status, view) = deployment
        .get(&format!("/catalogue?tenant={}", fixtures::tenant_id(1)))
        .await;
    assert_eq!(status, StatusCode::OK, "{view}");
    assert_eq!(view["revision"], head);
}

#[tokio::test]
async fn catalogue_refresh_keeps_last_known_good_on_refusal() {
    let handle = crate::backends::catalog_runtime::start(
        &crate::config::CatalogConfig {
            source: crate::config::CatalogSourceBackend::ModelsDev,
            store: crate::config::CatalogStoreBackend::InMemory,
            bootstrap: crate::config::CatalogBootstrap::Seed,
            source_url: Some("http://127.0.0.1:1/catalog.json".to_owned()),
            refresh_timeout_seconds: 1,
            refresh_interval_seconds: 86_400,
            retry_initial_seconds: 1,
            retry_max_seconds: 1,
            ..crate::config::CatalogConfig::default()
        },
        None,
        &std::collections::HashMap::new(),
        std::future::pending(),
    )
    .await
    .expect("a seed-bootstrapped catalogue starts")
    .expect("an enabled catalogue yields a handle");
    let before = handle.status().report().expect("boot published a report");
    let content_id = before
        .active
        .expect("seed bootstrap is active")
        .content_id
        .short();
    let deployment = Deployment::with_catalog_handle(handle);
    let published = deployment.store.published_revisions();

    let (status, body) = deployment
        .post_material("/catalogue/refresh", &json!({}))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["catalogue"]["content_id"], json!(content_id));
    assert!(
        body["catalogue"]["consecutive_refusals"]
            .as_u64()
            .expect("a refusal count")
            >= 1,
        "{body}"
    );
    assert!(body["catalogue"]["last_refusal"].is_string(), "{body}");
    assert_eq!(deployment.store.published_revisions(), published);
}

#[tokio::test]
async fn catalogue_refresh_is_unavailable_when_the_import_task_has_stopped() {
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let handle = crate::backends::catalog_runtime::start(
        &crate::config::CatalogConfig {
            source: crate::config::CatalogSourceBackend::Seed,
            store: crate::config::CatalogStoreBackend::InMemory,
            bootstrap: crate::config::CatalogBootstrap::Seed,
            refresh_interval_seconds: 86_400,
            ..crate::config::CatalogConfig::default()
        },
        None,
        &std::collections::HashMap::new(),
        async move {
            let _ = stopped.await;
        },
    )
    .await
    .expect("an offline catalogue starts")
    .expect("an enabled catalogue yields a handle");
    let deployment = Deployment::with_catalog_handle(handle.clone());
    stop.send(()).expect("the task is listening");
    while handle.refresh_now().await.is_some() {
        tokio::task::yield_now().await;
    }
    let (status, body) = deployment
        .post_material("/catalogue/refresh", &json!({}))
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"]["type"], "catalog_store_unavailable");
    assert_eq!(body["error"]["retryable"], json!(true));
}

// ---------------------------------------------------------------------------
// History, audit, rollback
// ---------------------------------------------------------------------------

#[tokio::test]
async fn history_is_bounded_newest_first_and_audit_names_the_actor() {
    let deployment = Deployment::new();
    let head = build(&deployment).await;

    let (status, page) = deployment.get("/history?limit=3").await;
    assert_eq!(status, StatusCode::OK);
    let revisions = page["revisions"].as_array().expect("revisions");
    assert_eq!(revisions.len(), 3);
    assert_eq!(revisions[0]["revision"], head);

    let (status, body) = deployment.get("/history?limit=0").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "history_limit_invalid");

    // A query string the extractor cannot read is still an administrative
    // refusal: a client branching on `AdminError::CODES` never meets a body it
    // cannot parse.
    for query in ["/history?limit=abc", "/history?page=2"] {
        let (status, body) = deployment.get(query).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{query}");
        assert_eq!(body["error"]["type"], "admin_request_invalid", "{query}");
    }

    let (status, audit) = deployment.get(&format!("/audit/{head}")).await;
    assert_eq!(status, StatusCode::OK);
    let events = audit["events"].as_array().expect("events");
    assert!(!events.is_empty());
    assert_eq!(events[0]["actor"]["kind"], "human");
    assert_eq!(events[0]["actor"]["subject"], SUBJECT);
    assert_eq!(events[0]["summary"], "cap acme's spend and concurrency");
}

// ---------------------------------------------------------------------------
// Conditional reads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_read_a_caller_already_holds_answers_not_modified_without_a_body() {
    let deployment = Deployment::new();
    build(&deployment).await;

    for path in ["/state", "/history", "/convergence"] {
        let (status, etag, body) = deployment.get_conditional(path, None).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        let validator = etag.expect("every administrative read carries a validator");
        // `/convergence` answers a weak validator, because its reported lag moves
        // while nothing about the replica's convergence state does.
        let expected = if path == "/convergence" { "W/\"" } else { "\"" };
        assert!(validator.starts_with(expected), "{path}: {validator}");

        let (status, repeat, body_again) = deployment.get_conditional(path, Some(&validator)).await;
        assert_eq!(status, StatusCode::NOT_MODIFIED, "{path}");
        // The validator is echoed on the `304`, so a poller keeps conditioning on
        // the one it holds rather than falling back to full reads.
        assert_eq!(repeat.as_deref(), Some(validator.as_str()), "{path}");
        assert!(body_again.is_empty(), "{path}: a 304 carries no body");
        assert!(!body.is_empty(), "{path}");

        // `*` matches any current representation, and a read that answers has one.
        let (status, _, _) = deployment.get_conditional(path, Some("*")).await;
        assert_eq!(status, StatusCode::NOT_MODIFIED, "{path}");
    }
}

#[tokio::test]
async fn a_validator_stops_matching_once_the_state_it_described_changes() {
    let deployment = Deployment::new();
    let head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    let (_, before, _) = deployment.get_conditional("/state", None).await;
    let before = before.expect("a validator");

    deployment
        .publish("/projects", "key-2", &head, &project_document())
        .await;

    let (status, after, body) = deployment.get_conditional("/state", Some(&before)).await;
    assert_eq!(status, StatusCode::OK);
    let after = after.expect("a validator");
    assert_ne!(after, before);
    // The validator describes the bytes, not the revision: the projection a
    // caller receives is the one its new validator was taken over.
    let state: Value = serde_json::from_slice(&body).expect("a state view");
    assert!(
        state["resources"]
            .as_array()
            .expect("resources")
            .iter()
            .any(|resource| resource["kind"] == "project"),
        "{state}",
    );
    let (status, _, _) = deployment.get_conditional("/state", Some(&after)).await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn a_conditional_the_surface_cannot_use_is_answered_in_full() {
    let deployment = Deployment::new();
    build(&deployment).await;
    let (_, validator, _) = deployment.get_conditional("/state", None).await;
    let validator = validator.expect("a validator");

    // A validator for another representation, a mangled one, and one this
    // surface never issued: none may be read as a match, because a wrong `304`
    // hands an operator a stale answer during an incident.
    let (_, history, _) = deployment.get_conditional("/history", None).await;
    let history = history.expect("a validator");
    for conditional in [
        history.clone(),
        "\"not-a-checksum\"".to_owned(),
        "garbage".to_owned(),
        "W/\"not-a-checksum\"".to_owned(),
        // Not an entity-tag: a doubled prefix names no representation, however
        // closely the rest of it resembles the current one.
        format!("W/W/{validator}"),
    ] {
        let (status, echoed, body) = deployment
            .get_conditional("/state", Some(&conditional))
            .await;
        assert_eq!(status, StatusCode::OK, "{conditional}");
        assert_eq!(echoed.as_deref(), Some(validator.as_str()), "{conditional}");
        assert!(!body.is_empty(), "{conditional}");
    }

    // A weak validator over the *current* representation can only be an
    // intermediary weakening one that came from here, and still matches.
    let (status, _, _) = deployment
        .get_conditional("/state", Some(&format!("W/{validator}")))
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn a_conditional_read_is_still_authenticated_and_authorized() {
    let deployment = Deployment::with_authorizer(FakeAdminAuthorizer::permissive());
    build(&deployment).await;
    let (_, validator, _) = deployment.get_conditional("/state", None).await;
    let validator = validator.expect("a validator");

    // A validator is not a credential: presenting one without an administrative
    // credential is an unauthenticated read, not a free `304`.
    let response = router(deployment.api.clone())
        .oneshot(
            Request::get(format!("{ADMIN_PREFIX}/state"))
                .header(axum::http::header::IF_NONE_MATCH, &validator)
                .body(Body::empty())
                .expect("a request"),
        )
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response.headers().get(axum::http::header::ETAG).is_none());

    // Nor does it bypass authorization: a caller without deployment authority
    // is refused whether or not it names the representation.
    let scoped = Deployment::with_authorizer(
        FakeAdminAuthorizer::permissive().within(&[ResourceScope::Tenant(fixtures::tenant_id(1))]),
    );
    let (status, etag, _) = scoped.get_conditional("/state", Some(&validator)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(etag.is_none());
}

#[tokio::test]
async fn a_rollback_republishes_an_earlier_state_as_a_new_revision() {
    let deployment = Deployment::new();
    let first = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    let second = deployment
        .publish("/projects", "key-2", &first, &project_document())
        .await;

    let rollback = json!({
        "summary": "the project was created against the wrong tenant",
        "revision": first,
    });
    let (status, body) = deployment
        .post("/rollback", "key-3", &second, &rollback)
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], "published");
    let restored = body["revision"].as_str().expect("a revision");
    assert_ne!(restored, first, "a rollback moves forward, not backwards");
    assert_eq!(body["diff"]["summary"]["removed"], 1);

    let (_, state) = deployment.get("/state").await;
    assert_eq!(state["revision"], restored);
    let kinds: Vec<&str> = state["resources"]
        .as_array()
        .expect("resources")
        .iter()
        .map(|resource| resource["kind"].as_str().expect("a kind"))
        .collect();
    assert_eq!(kinds, vec!["tenant"]);
    // The rolled-back-from revision is still readable: history is append-only.
    assert_eq!(
        deployment.get(&format!("/audit/{second}")).await.0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn a_rollback_to_a_revision_that_does_not_exist_is_refused() {
    let deployment = Deployment::new();
    let head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    let rollback = json!({
        "summary": "restore a revision nobody published",
        "revision": fixtures::revision_id(99).to_string(),
    });
    let (status, body) = deployment
        .post("/rollback", "key-2", &head, &rollback)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["type"], "revision_not_found");
    assert_eq!(deployment.store.published_revisions(), 1);
}

// ---------------------------------------------------------------------------
// Stateless mode
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_stateless_deployment_refuses_every_administrative_route_by_mode() {
    for spec in super::admin_route_specs() {
        let path = format!("{ADMIN_PREFIX}{}", super::router::concrete_path(&spec));
        let builder = if spec.action.writes() {
            Request::post(&path)
        } else {
            Request::get(&path)
        };
        let response = refusing_router()
            .oneshot(builder.body(Body::empty()).expect("a request"))
            .await
            .expect("a response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("a body")
            .to_bytes();
        let body: Value = serde_json::from_slice(&body).expect("an administrative envelope");
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{path}: {body}");
        assert_eq!(body["error"]["type"], "stateful_mode_required");
        // Unauthenticated, and still the *mode* answer: the refusal cannot depend
        // on a credential a stateless deployment has no way to issue.
        assert_eq!(body["error"]["retryable"], false);
    }
}

/// A replica's derived availability, as a snapshot would carry it.
struct StaticAvailability {
    index: Option<Arc<AvailabilityIndex>>,
    runtime: RuntimeObservations,
}

impl AvailabilityReader for StaticAvailability {
    fn read(&self) -> Option<(Arc<AvailabilityIndex>, RuntimeObservations)> {
        Some((self.index.clone()?, self.runtime.clone()))
    }
}

/// A record every authority permits, resting on the evidence it is handed.
fn entitled(evidence: DiscoveryObservation) -> AvailabilityRecord {
    AvailabilityRecord {
        presence: CataloguePresence::Present,
        enablement: Enablement::Enabled,
        entitlement: Entitlement::Granted,
        policy: PolicyDecision::Permitted,
        discovery: Some(evidence),
        ..AvailabilityRecord::default()
    }
}

/// Two tenants' derived availability in one index, which is what a replica
/// actually holds: the read must never widen past the scope it was asked about.
fn two_tenant_index() -> (AvailabilityIndex, ScopeRef, ScopeRef) {
    let mine = ScopeRef::tenant(fixtures::tenant_id(1));
    let theirs = ScopeRef::tenant(fixtures::tenant_id(11));
    let target = TargetRef::parse("openai", "gpt-4o").expect("a well-formed target");
    let observation = |scope: ScopeRef| {
        DiscoveryObservation::new(
            scope,
            target.clone(),
            DiscoveryResult::Present,
            DiscoveryCompleteness::Complete,
            DiscoverySource::ProviderListing,
            SystemTime::now(),
        )
        .detailed("listed by https://api.example.test/v1/models?key=sk-live-never-served")
    };
    let index = AvailabilityIndex::builder()
        .record(
            AvailabilityKey::new(mine, target.clone()),
            entitled(observation(mine)),
        )
        .record(
            AvailabilityKey::new(theirs, target.clone()),
            entitled(observation(theirs)),
        )
        .build();
    (index, mine, theirs)
}

/// A replica that derives no view says so, rather than answering "no models" —
/// which reads identically to an entitlement a caller has just lost.
#[tokio::test]
async fn an_availability_read_distinguishes_deriving_nothing_from_finding_nothing() {
    let deployment = Deployment::new();

    let (status, body) = deployment
        .get(&format!("/availability?tenant={}", fixtures::tenant_id(1)))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deriving"], json!(false));
    assert_eq!(body["targets"], json!([]));

    // Attached and deriving nothing is the same answer: what the flag reports is
    // whether a view exists, not whether a reader was wired up.
    let attached = Deployment::attached_but_underiving();
    let (status, body) = attached
        .get(&format!("/availability?tenant={}", fixtures::tenant_id(1)))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deriving"], json!(false));
    assert_eq!(body["targets"], json!([]));

    let (index, mine, _) = two_tenant_index();
    let deriving = Deployment::deriving(
        FakeAdminAuthorizer::permissive(),
        index,
        RuntimeObservations::none(),
    );
    let (status, body) = deriving
        .get(&format!("/availability?tenant={}", mine.tenant))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deriving"], json!(true));
    assert_eq!(body["targets"].as_array().expect("targets").len(), 1);
    assert_eq!(body["targets"][0]["state"], json!("available"));
    assert_eq!(body["targets"][0]["provider"], json!("openai"));
}

/// The read is answered from the replica's own memory: an availability question
/// asked *because* the control plane is unreachable must not need it.
#[tokio::test]
async fn an_availability_read_reaches_no_control_plane() {
    let (index, mine, _) = two_tenant_index();
    let store = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(store.clone()));
    let api = Arc::new(
        AdminApi::new(
            Arc::new(AdminService::stateful(counting.clone())),
            Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
            Arc::new(FakeAdminAuthorizer::permissive()),
        )
        .with_availability(Arc::new(StaticAvailability {
            index: Some(Arc::new(index)),
            runtime: RuntimeObservations::none(),
        })),
    );
    let deployment = Deployment {
        api,
        store,
        secrets: Arc::new(InMemorySecrets::new()),
    };

    let (status, body) = deployment
        .get(&format!("/availability?tenant={}", mine.tenant))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deriving"], json!(true));
    assert_eq!(
        counting.calls(),
        0,
        "an availability read consulted the control plane"
    );
}

/// One tenant's derived entitlements are not another's, and a grant that does
/// not enclose the scope is refused rather than narrowed.
#[tokio::test]
async fn an_availability_read_is_confined_to_the_scope_the_grant_encloses() {
    let (index, mine, theirs) = two_tenant_index();
    let deployment = Deployment::deriving(
        FakeAdminAuthorizer::permissive().within(&[ResourceScope::Tenant(mine.tenant)]),
        index,
        RuntimeObservations::none(),
    );

    let (status, body) = deployment
        .get(&format!("/availability?tenant={}", mine.tenant))
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["targets"].as_array().expect("targets").len(), 1);
    // A tenant's own answer carries no discovery machinery: which listing the
    // deployment took, and what a probe's error body said, are the operator's.
    assert_eq!(body["targets"][0].get("source"), None);
    let serialized = body.to_string();
    assert!(!serialized.contains("sk-live"), "{serialized}");
    assert!(!serialized.contains("api.example.test"), "{serialized}");

    let (status, _) = deployment
        .get(&format!("/availability?tenant={}", theirs.tenant))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

/// Availability is a question about a tenant's models. A deployment-wide answer
/// would be every tenant's entitlements in one document, so it is refused rather
/// than served to the one caller who could read it.
#[tokio::test]
async fn an_availability_read_must_name_the_tenant_it_asks_about() {
    let (index, _, _) = two_tenant_index();
    let deployment = Deployment::deriving(
        FakeAdminAuthorizer::permissive(),
        index,
        RuntimeObservations::none(),
    );

    let (status, body) = deployment.get("/availability").await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "admin_request_invalid");

    // And the same answer for a caller who could never have asked deployment-wide:
    // the request shape is refused before any authority is consulted, so a tenant
    // administrator's typo is not answered with a forbidden — nor recorded as one
    // in the denial trail.
    let (index, mine, _) = two_tenant_index();
    let scoped = Deployment::deriving(
        FakeAdminAuthorizer::permissive().within(&[ResourceScope::Tenant(mine.tenant)]),
        index,
        RuntimeObservations::none(),
    );

    let (status, body) = scoped.get("/availability").await;

    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "admin_request_invalid");
    let denials = ControlPlaneStore::denials(
        scoped.store.as_ref(),
        &DenialPage::for_scope(Some(mine.tenant)),
        10,
    )
    .await
    .expect("the denial trail");
    assert!(
        denials.is_empty(),
        "a malformed query is not an access denial: {denials:?}"
    );
}

/// A project's enablements are *overrides* of its tenant's, so a project that
/// has overridden nothing may still call everything its tenant enabled — and the
/// read says so rather than reporting a project with no models.
#[tokio::test]
async fn an_availability_read_of_a_project_carries_what_the_project_inherits() {
    let tenant = fixtures::tenant_id(1);
    let project = fixtures::project_id(2);
    let scope = ScopeRef::tenant(tenant);
    let inherited = TargetRef::parse("openai", "gpt-4o").expect("a well-formed target");
    let overridden = TargetRef::parse("openai", "o3").expect("a well-formed target");
    let index = AvailabilityIndex::builder()
        .record(
            AvailabilityKey::new(scope, inherited.clone()),
            entitled(DiscoveryObservation::new(
                scope,
                inherited,
                DiscoveryResult::Present,
                DiscoveryCompleteness::Complete,
                DiscoverySource::ProviderListing,
                SystemTime::now(),
            )),
        )
        .record(
            AvailabilityKey::new(
                ScopeRef {
                    tenant,
                    project: Some(project),
                },
                overridden,
            ),
            AvailabilityRecord {
                enablement: Enablement::NotEnabled,
                ..AvailabilityRecord::enabled()
            },
        )
        .build();
    let deployment = Deployment::deriving(
        FakeAdminAuthorizer::permissive(),
        index,
        RuntimeObservations::none(),
    );

    let (status, body) = deployment
        .get(&format!("/availability?tenant={tenant}&project={project}"))
        .await;

    assert_eq!(status, StatusCode::OK);
    let targets = body["targets"].as_array().expect("targets");
    assert_eq!(targets.len(), 2, "{body}");
    assert_eq!(targets[0]["model"], json!("gpt-4o"));
    assert_eq!(targets[0]["state"], json!("available"));
    // The project's own record still replaces what it overrides, including when
    // the override is a refusal.
    assert_eq!(targets[1]["model"], json!("o3"));
    assert_eq!(targets[1]["state"], json!("denied"));
}

/// This replica's own circuits are overlaid at the instant of the question, so
/// two replicas answer honestly rather than one answering for the fleet.
#[tokio::test]
async fn an_availability_read_overlays_this_replicas_own_health() {
    let (index, mine, _) = two_tenant_index();
    let deployment = Deployment::deriving(
        FakeAdminAuthorizer::permissive(),
        index,
        RuntimeObservations::of_circuits([("openai/gpt-4o".to_owned(), CircuitState::Open)]),
    );

    let (status, body) = deployment
        .get(&format!("/availability?tenant={}", mine.tenant))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["targets"][0]["state"], json!("unavailable"));
    // An operator trusted with the whole deployment is told which authority
    // refused, because "why can this tenant not reach this model" is the
    // question the read exists to answer.
    assert_eq!(body["targets"][0]["decided_by"], json!("runtime"));
}

/// The same answer to a tenant's own administrator says only what it is, not
/// which of the deployment's authorities decided it.
///
/// Disclosure follows the caller's authority rather than the scope the query
/// names — an availability read always names a tenant, so the scope cannot tell
/// a root operator apart from a tenant administrator asking about themselves.
#[tokio::test]
async fn an_availability_read_by_a_tenants_own_administrator_names_no_authority() {
    let (index, mine, _) = two_tenant_index();
    let scoped = Deployment::deriving(
        FakeAdminAuthorizer::permissive().within(&[ResourceScope::Tenant(mine.tenant)]),
        index,
        RuntimeObservations::of_circuits([("openai/gpt-4o".to_owned(), CircuitState::Open)]),
    );

    let (status, body) = scoped
        .get(&format!("/availability?tenant={}", mine.tenant))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["targets"][0]["state"], json!("unavailable"));
    // A tenant learns that the target is not being attempted, not that this
    // replica's breaker is open.
    assert_eq!(body["targets"][0]["decided_by"], json!("undisclosed"));
}

/// An availability read is a read like the others: an operator watching a target
/// through an incident conditions on the answer it already holds, and pays for a
/// body only when the answer moved.
#[tokio::test]
async fn an_availability_read_a_caller_already_holds_answers_not_modified() {
    let (index, mine, _) = two_tenant_index();
    let deployment = Deployment::deriving(
        FakeAdminAuthorizer::permissive(),
        index,
        RuntimeObservations::none(),
    );
    let path = format!("/availability?tenant={}", mine.tenant);

    let (status, etag, body) = deployment.get_conditional(&path, None).await;
    assert_eq!(status, StatusCode::OK);
    let validator = etag.expect("every administrative read carries a validator");
    assert!(validator.starts_with('"'), "{validator}");
    assert!(!body.is_empty());

    let (status, repeat, body_again) = deployment.get_conditional(&path, Some(&validator)).await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert_eq!(repeat.as_deref(), Some(validator.as_str()));
    assert!(body_again.is_empty(), "a 304 carries no body");
}

// ---------------------------------------------------------------------------
// The credential lifecycle: `/admin/v1/secrets`
// ---------------------------------------------------------------------------

const MATERIAL: &str = "sk-live-do-not-log-this";
const ROTATED: &str = "sk-live-the-replacement";

/// The tenant every material case below owns its secrets as.
fn owning_tenant() -> String {
    fixtures::tenant_id(1).to_string()
}

/// Nothing a material call answers with may carry what was presented — not the
/// stored value, not a prefix of it, not a fingerprint derived from it.
fn carries_no_material(body: &Value) {
    let rendered = body.to_string();
    for material in [MATERIAL, ROTATED] {
        assert!(
            !rendered.contains(material),
            "material reached a caller: {rendered}"
        );
        // A prefix long enough to identify a key is a leak too.
        assert!(
            !rendered.contains(&material[..12]),
            "a prefix of material reached a caller: {rendered}"
        );
    }
}

#[tokio::test]
async fn material_is_stored_under_a_reference_and_never_answered_back() {
    let deployment = Deployment::new();
    let tenant = owning_tenant();
    let (status, staged) = deployment
        .post_material(
            "/secrets",
            &json!({ "tenant": tenant, "material": MATERIAL }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{staged}");
    carries_no_material(&staged);
    // Staged, not active: storing material makes nothing servable.
    assert_eq!(staged["lifecycle"], "staged");
    assert_eq!(staged["version"], 1);
    assert_eq!(staged["owner"], tenant);
    let reference = staged["reference"]
        .as_str()
        .expect("a reference")
        .to_owned();
    assert!(reference.ends_with("@v1"), "{reference}");

    // And there is no route that gives it back: the versions read is the widest
    // thing a caller may ask for.
    let secret = staged["secret"].as_str().expect("a secret id");
    let (status, versions) = deployment
        .get(&format!("/secrets/{secret}?tenant={tenant}"))
        .await;
    assert_eq!(status, StatusCode::OK, "{versions}");
    carries_no_material(&versions);
    assert_eq!(versions["versions"].as_array().expect("versions").len(), 1);
    assert_eq!(versions["versions"][0]["reference"], reference);
    assert_eq!(deployment.store.published_revisions(), 0);
}

#[tokio::test]
async fn a_rotation_stages_the_next_version_beside_the_one_in_service() {
    let deployment = Deployment::new();
    let tenant = owning_tenant();
    let first = deployment.stage(&tenant, MATERIAL).await;
    let (status, activated) = deployment
        .post_material(
            "/secrets/lifecycle",
            &json!({ "tenant": tenant, "reference": first, "lifecycle": "active" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{activated}");
    assert_eq!(activated["changed"], true);

    let (status, rotated) = deployment
        .post_material(
            "/secrets/rotate",
            &json!({ "tenant": tenant, "reference": first, "material": ROTATED }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{rotated}");
    carries_no_material(&rotated);
    assert_eq!(rotated["version"], 2);
    assert_eq!(rotated["lifecycle"], "staged");

    // Both versions exist at once, which is what makes a cutover reversible: the
    // old one is still resolvable while the new one is provable.
    let secret = rotated["secret"].as_str().expect("a secret id");
    let (_, versions) = deployment
        .get(&format!("/secrets/{secret}?tenant={tenant}"))
        .await;
    let versions = versions["versions"].as_array().expect("versions").clone();
    assert_eq!(versions.len(), 2, "{versions:?}");
    assert_eq!(versions[0]["lifecycle"], "active");
    assert_eq!(versions[0]["resolvable"], true);
    assert_eq!(versions[1]["lifecycle"], "staged");
    assert_eq!(versions[1]["resolvable"], true);

    // Withdrawing the superseded version is the end of the rotation, and it is
    // visible as rotation status rather than inferred.
    let (status, disabled) = deployment
        .post_material(
            "/secrets/lifecycle",
            &json!({ "tenant": tenant, "reference": first, "lifecycle": "disabled" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{disabled}");
    let (_, versions) = deployment
        .get(&format!("/secrets/{secret}?tenant={tenant}"))
        .await;
    assert_eq!(versions["versions"][0]["lifecycle"], "disabled");
    assert_eq!(versions["versions"][0]["resolvable"], false);
    assert_eq!(
        deployment.store.published_revisions(),
        0,
        "secret material lifecycle calls do not publish a revision or AuditEvent"
    );
}

/// The rotation an operator repeats — a retried request, or a second
/// administrator doing what the first already did — must not be reported as a
/// bad key: the material was never examined, and an operator told their key was
/// refused re-issues a credential that was never at fault.
#[tokio::test]
async fn a_repeated_rotation_is_a_conflict_rather_than_a_refusal_of_the_material() {
    let deployment = Deployment::new();
    let tenant = owning_tenant();
    let first = deployment.stage(&tenant, MATERIAL).await;
    let body = json!({ "tenant": tenant, "reference": first, "material": ROTATED });
    let (status, rotated) = deployment.post_material("/secrets/rotate", &body).await;
    assert_eq!(status, StatusCode::OK, "{rotated}");
    let next = rotated["reference"]
        .as_str()
        .expect("a reference")
        .to_owned();

    let (status, again) = deployment.post_material("/secrets/rotate", &body).await;
    assert_eq!(status, StatusCode::CONFLICT, "{again}");
    assert_eq!(again["error"]["type"], "secret_version_exists");
    assert_ne!(
        again["error"]["type"], "secret_material_refused",
        "the presented material was never examined, so it cannot be what was refused"
    );
    // Not retryable: the version this would mint exists, and replaying cannot
    // change that — the caller re-reads the versions and rotates from the current
    // one.
    assert_eq!(again["error"]["retryable"], false);
    // The refusal names the version that already exists, which is what tells the
    // caller the rotation it wanted has happened.
    assert_eq!(again["error"]["resource"], next);
    carries_no_material(&again);

    // And the stored version is the first rotation's, untouched: a version is
    // immutable, so the second call could not have overwritten what a credential
    // pinning it already resolves to.
    let secret = rotated["secret"].as_str().expect("a secret id");
    let (_, versions) = deployment
        .get(&format!("/secrets/{secret}?tenant={tenant}"))
        .await;
    let listed = versions["versions"].as_array().expect("versions").clone();
    assert_eq!(listed.len(), 2, "{listed:?}");
    assert_eq!(listed[1]["reference"], next);
    assert_eq!(listed[1]["lifecycle"], "staged");
}

/// The versions read is polled — by an operator watching a staged version reach
/// `active` — so it answers the same conditional contract as every other
/// administrative projection, rather than a bare body.
#[tokio::test]
async fn the_versions_read_answers_the_conditional_contract() {
    let deployment = Deployment::new();
    let tenant = owning_tenant();
    let reference = deployment.stage(&tenant, MATERIAL).await;
    let secret = reference.split('@').next().expect("a secret id").to_owned();
    let path = format!("/secrets/{secret}?tenant={tenant}");

    let (status, headers, body) = deployment.get_with_headers(&path, None).await;
    assert_eq!(status, StatusCode::OK);
    let validator = headers
        .get(axum::http::header::ETAG)
        .expect("a validator")
        .to_str()
        .expect("a readable validator")
        .to_owned();
    // Strong: this projection is validated by the bytes it answers with.
    assert!(validator.starts_with('"'), "{validator}");
    // Per-caller — a project-scoped grant reads a narrower projection of the same
    // secret — so no shared cache may reuse it for another administrator.
    assert_eq!(
        headers
            .get(axum::http::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok()),
        Some("private, no-cache"),
    );
    assert_eq!(
        headers
            .get(axum::http::header::VARY)
            .and_then(|value| value.to_str().ok()),
        Some("authorization"),
    );
    assert!(!body.is_empty());

    let (status, headers, body) = deployment.get_with_headers(&path, Some(&validator)).await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert_eq!(
        headers
            .get(axum::http::header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(validator.as_str()),
    );
    assert!(body.is_empty(), "a 304 carries no body");

    // A lifecycle move is a new representation, so the validator the caller holds
    // stops matching and the next poll is answered in full.
    let (status, moved) = deployment
        .post_material(
            "/secrets/lifecycle",
            &json!({ "tenant": tenant, "reference": reference, "lifecycle": "active" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{moved}");
    let (status, headers, body) = deployment.get_with_headers(&path, Some(&validator)).await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(
        headers
            .get(axum::http::header::ETAG)
            .and_then(|value| value.to_str().ok()),
        Some(validator.as_str()),
    );
    let versions: Value = serde_json::from_slice(&body).expect("a projection");
    assert_eq!(versions["versions"][0]["lifecycle"], "active");
    carries_no_material(&versions);
}

/// Version listing is an operational rotation projection, not a control-plane
/// or provider calibration endpoint: it returns only store metadata for the
/// tenant the caller named and does not load desired state or unwrap material.
#[tokio::test]
async fn the_versions_read_is_tenant_scoped_metadata_only() {
    let control_plane = Arc::new(InMemoryControlPlane::new());
    let counting = Arc::new(CountingStore::new(control_plane));
    let secrets = Arc::new(InMemorySecrets::new());
    let tenant = fixtures::tenant_id(1);
    let reference = fixtures::secret_ref(1);
    secrets.seed(
        crate::desired_state::secrets::SecretOwner::tenant(tenant),
        reference,
        MATERIAL,
        crate::desired_state::SecretLifecycle::Active,
    );
    let api = Arc::new(AdminApi::new(
        Arc::new(AdminService::stateful(counting.clone()).with_secrets(secrets)),
        Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
        Arc::new(FakeAdminAuthorizer::permissive()),
    ));

    let response = router(api)
        .oneshot(
            Request::get(format!(
                "{ADMIN_PREFIX}/secrets/{}?tenant={tenant}",
                reference.secret
            ))
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .body(Body::empty())
            .expect("a request"),
        )
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();
    let body: Value = serde_json::from_slice(&body).expect("a projection");

    assert_eq!(body["secret"], reference.secret.to_string());
    assert_eq!(body["owner"], tenant.to_string());
    assert_eq!(body["versions"][0]["reference"], reference.to_string());
    assert_eq!(body["versions"][0]["lifecycle"], "active");
    assert_eq!(body["versions"][0]["resolvable"], true);
    carries_no_material(&body);
    assert_eq!(
        counting.calls(),
        0,
        "version listing consulted the control plane"
    );
}

#[tokio::test]
async fn moving_a_version_to_the_state_it_already_holds_is_not_a_second_change() {
    let deployment = Deployment::new();
    let tenant = owning_tenant();
    let reference = deployment.stage(&tenant, MATERIAL).await;
    let body = json!({ "tenant": tenant, "reference": reference, "lifecycle": "revoked" });
    let (status, first) = deployment.post_material("/secrets/lifecycle", &body).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["changed"], true);
    // The retry a client makes after a lost response: same state, no second
    // change, which is what stands in for an idempotency key here.
    let (status, again) = deployment.post_material("/secrets/lifecycle", &body).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(again["changed"], false);
    assert_eq!(again["lifecycle"], "revoked");
}

#[tokio::test]
async fn destroying_material_the_desired_state_still_pins_is_refused() {
    let deployment = Deployment::new();
    let tenant = owning_tenant();
    let reference = deployment.stage(&tenant, MATERIAL).await;
    let secret = reference.split('@').next().expect("a secret id").to_owned();

    // A credential that pins exactly this version, published the ordinary way.
    let head = deployment
        .publish(
            "/tenants",
            "key-1",
            EXPECTED_REVISION_EMPTY,
            &tenant_document(),
        )
        .await;
    let head = deployment
        .publish("/providers", "key-2", &head, &provider_document())
        .await;
    let mut credential = credential_document();
    credential["resource"]["secret"] = json!(secret);
    credential["resource"]["secret_version"] = json!(1);
    let head = deployment
        .publish("/credentials", "key-3", &head, &credential)
        .await;

    let tombstone = json!({ "tenant": tenant, "reference": reference, "lifecycle": "tombstoned" });
    let (status, refusal) = deployment
        .post_material("/secrets/lifecycle", &tombstone)
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{refusal}");
    assert_eq!(refusal["error"]["type"], "secret_in_use");

    // Revocation is not gated the same way: a leaked key is withdrawable at
    // once, and withdrawing it is what fails the *next* candidate rather than
    // the snapshot serving now.
    let (status, revoked) = deployment
        .post_material(
            "/secrets/lifecycle",
            &json!({ "tenant": tenant, "reference": reference, "lifecycle": "revoked" }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{revoked}");

    // Unpinned, and then destroyable: the credential is retired first, which is
    // what stops a candidate revision from resolving the version at all.
    let mut retire = credential_document();
    retire["mutation"] = json!("update");
    retire["resource"]["secret"] = json!(secret);
    retire["resource"]["secret_version"] = json!(1);
    retire["resource"]["lifecycle"] = json!("revoked");
    deployment
        .publish("/credentials", "key-4", &head, &retire)
        .await;
    let (status, destroyed) = deployment
        .post_material("/secrets/lifecycle", &tombstone)
        .await;
    assert_eq!(status, StatusCode::OK, "{destroyed}");
    assert_eq!(destroyed["lifecycle"], "tombstoned");
}

#[tokio::test]
async fn one_tenants_administrator_cannot_reach_another_tenants_material() {
    let deployment = Deployment::with_authorizer(
        FakeAdminAuthorizer::permissive().within(&[ResourceScope::Tenant(fixtures::tenant_id(1))]),
    );
    let ours = owning_tenant();
    let theirs = fixtures::tenant_id(2).to_string();
    let reference = deployment.stage(&ours, MATERIAL).await;
    let secret = reference.split('@').next().expect("a secret id").to_owned();

    let (status, refusal) = deployment
        .post_material(
            "/secrets",
            &json!({ "tenant": theirs, "material": MATERIAL }),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{refusal}");
    carries_no_material(&refusal);

    // A grant that *is* held, aimed at material another tenant owns: the store
    // answers as it does for a reference that was never stored, so this route is
    // not a way to learn that one exists.
    let deployment = Deployment::new();
    deployment.secrets.seed(
        crate::desired_state::secrets::SecretOwner::tenant(fixtures::tenant_id(2)),
        crate::desired_state::secrets::SecretRef::parse(&reference).expect("a reference"),
        MATERIAL,
        crate::desired_state::secrets::SecretLifecycle::Active,
    );
    let (status, versions) = deployment
        .get(&format!("/secrets/{secret}?tenant={ours}"))
        .await;
    assert_eq!(status, StatusCode::OK, "{versions}");
    assert_eq!(
        versions,
        json!({
            "secret": secret,
            "owner": ours,
            "versions": [],
        })
    );
    let (status, refusal) = deployment
        .post_material(
            "/secrets/lifecycle",
            &json!({ "tenant": ours, "reference": reference, "lifecycle": "revoked" }),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{refusal}");
    assert_eq!(refusal["error"]["type"], "secret_not_found");
}

#[tokio::test]
async fn tombstoning_a_foreign_pinned_version_is_not_an_existence_probe() {
    let deployment = Deployment::new();
    let ours = fixtures::tenant_id(1).to_string();
    let theirs = fixtures::tenant_id(2).to_string();
    let foreign_owner = crate::desired_state::secrets::SecretOwner::tenant(fixtures::tenant_id(2));
    let foreign_reference = fixtures::secret_ref(12);

    deployment.secrets.seed(
        foreign_owner,
        foreign_reference,
        MATERIAL,
        crate::desired_state::SecretLifecycle::Active,
    );

    // The desired state pins the foreign version as resolvable. A caller that
    // owns tenant 1 must still receive the same not-found answer as for an
    // unreferenced foreign version: ownership is established before this
    // deployment-wide reference-use check.
    let mut tenant = tenant_document();
    tenant["resource"]["tenant"] = json!(theirs);
    tenant["resource"]["slug"] = json!("globex");
    let mut provider = provider_document();
    provider["resource"]["tenant"] = json!(theirs);
    let mut credential = credential_document();
    credential["resource"]["tenant"] = json!(theirs);
    credential["resource"]["secret"] = json!(foreign_reference.secret.to_string());

    let mut head = deployment
        .publish(
            "/tenants",
            "key-foreign-1",
            EXPECTED_REVISION_EMPTY,
            &tenant,
        )
        .await;
    head = deployment
        .publish("/providers", "key-foreign-2", &head, &provider)
        .await;
    deployment
        .publish("/credentials", "key-foreign-3", &head, &credential)
        .await;

    let (status, pinned_refusal) = deployment
        .post_material(
            "/secrets/lifecycle",
            &json!({
                "tenant": ours,
                "reference": foreign_reference.to_string(),
                "lifecycle": "tombstoned",
            }),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{pinned_refusal}");
    assert_eq!(pinned_refusal["error"]["type"], "secret_not_found");

    let (status, absent_refusal) = deployment
        .post_material(
            "/secrets/lifecycle",
            &json!({
                "tenant": ours,
                "reference": fixtures::secret_ref(99).to_string(),
                "lifecycle": "tombstoned",
            }),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{absent_refusal}");
    assert_eq!(absent_refusal["error"]["type"], "secret_not_found");
}

#[tokio::test]
async fn a_body_that_carries_material_is_refused_without_echoing_it() {
    let deployment = Deployment::new();
    let tenant = owning_tenant();
    // Valid JSON, wrong shape: serde renders the offending input into some of
    // its messages, and the offending input here is a provider key.
    let (status, refusal) = deployment
        .post_material(
            "/secrets",
            &json!({ "tenant": tenant, "material": { "value": MATERIAL } }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refusal}");
    assert_eq!(refusal["error"]["type"], "admin_request_invalid");
    carries_no_material(&refusal);

    // Empty material is refused before it is stored, rather than becoming a
    // version that can never authenticate anything.
    let (status, refusal) = deployment
        .post_material("/secrets", &json!({ "tenant": tenant, "material": "" }))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refusal}");
    assert_eq!(refusal["error"]["type"], "secret_material_refused");

    // A lifecycle value is not material by type, but it is still caller input
    // and must not be copied into either the response or operator detail.
    let lifecycle_value = "sk-lifecycle-value-must-not-echo";
    let (status, refusal) = deployment
        .post_material(
            "/secrets/lifecycle",
            &json!({
                "tenant": tenant,
                "reference": fixtures::secret_ref(12).to_string(),
                "lifecycle": lifecycle_value,
            }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refusal}");
    assert_eq!(refusal["error"]["type"], "admin_request_invalid");
    assert!(
        !refusal.to_string().contains(lifecycle_value),
        "caller input reached the response: {refusal}"
    );

    // A reference that names no version is refused: every operation here is
    // aimed at an exact version.
    let (status, refusal) = deployment
        .post_material(
            "/secrets/rotate",
            &json!({
                "tenant": tenant,
                "reference": fixtures::secret_id(7).to_string(),
                "material": ROTATED,
            }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refusal}");
    carries_no_material(&refusal);
}

#[tokio::test]
async fn a_secret_store_outage_refuses_the_call_and_names_no_backend_detail() {
    let deployment = Deployment::new();
    let tenant = owning_tenant();
    deployment.secrets.set_unavailable(true);
    let (status, refusal) = deployment
        .post_material(
            "/secrets",
            &json!({ "tenant": tenant, "material": MATERIAL }),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{refusal}");
    assert_eq!(refusal["error"]["type"], "secret_store_unavailable");
    assert_eq!(refusal["error"]["retryable"], true);
    carries_no_material(&refusal);
}

#[tokio::test]
async fn material_calls_need_the_material_authority_and_not_the_publishing_one() {
    let deployment =
        Deployment::with_authorizer(FakeAdminAuthorizer::permitting(&[AdminAction::Publish]));
    let tenant = owning_tenant();
    let (status, refusal) = deployment
        .post_material(
            "/secrets",
            &json!({ "tenant": tenant, "material": MATERIAL }),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{refusal}");
    assert_eq!(refusal["error"]["type"], "admin_forbidden");
    carries_no_material(&refusal);
}

// ---------------------------------------------------------------------------
// POST /admin/v1/bindings
// ---------------------------------------------------------------------------

async fn imported_deployment() -> (Deployment, crate::backends::catalog::CatalogSnapshot) {
    let snapshot = seed_snapshot();
    let store = Arc::new(InMemoryCatalogStore::new());
    store
        .activate(
            &RetainedCatalog {
                source: snapshot.source.clone(),
                payload: RawPayload::new(SEED_PAYLOAD.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("the seed is retained");
    (Deployment::with_catalogue(store), snapshot)
}

async fn foundation(deployment: &Deployment) -> String {
    let mut expected = EXPECTED_REVISION_EMPTY.to_owned();
    expected = deployment
        .publish("/tenants", "found-0", &expected, &tenant_document())
        .await;
    expected = deployment
        .publish("/projects", "found-1", &expected, &project_document())
        .await;
    expected = deployment
        .publish("/providers", "found-2", &expected, &provider_document())
        .await;
    deployment
        .publish("/credentials", "found-3", &expected, &credential_document())
        .await
}

fn binding_document() -> Value {
    json!({
        "summary": "enable gpt-4o",
        "mutation": "create",
        "resource": {
            "tenant": fixtures::tenant_id(1).to_string(),
            "project": fixtures::project_id(2).to_string(),
            "targets": [{
                "provider": "openai",
                "model": "gpt-4o",
                "price": {
                    "input_microdollars_per_million": 2_500_000u64,
                    "output_microdollars_per_million": 10_000_000u64
                }
            }]
        }
    })
}

fn binding_update() -> Value {
    let mut document = binding_document();
    document["mutation"] = json!("update");
    document
}

#[tokio::test]
async fn four_step_then_binding_adopts_the_book_and_alias_id() {
    let (deployment, snapshot) = imported_deployment().await;
    let mut expected = foundation(&deployment).await;
    let digest = snapshot.source.raw.digest;
    expected = deployment
        .publish(
            "/catalogs",
            "four-0",
            &expected,
            &json!({
                "summary": "pin the imported catalogue",
                "mutation": "create",
                "resource": {
                    "catalog": fixtures::resource_id(13).to_string(),
                    "slug": "openai-models",
                    "digest": digest.to_string(),
                    "size_bytes": snapshot.source.raw.size_bytes,
                }
            }),
        )
        .await;
    expected = deployment
        .publish(
            "/models",
            "four-1",
            &expected,
            &json!({
                "summary": "enable gpt-4o",
                "mutation": "create",
                "resource": {
                    "enablement": fixtures::resource_id(14).to_string(),
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "project": fixtures::project_id(2).to_string(),
                    "slug": "gpt-4o",
                    "offering": fixtures::offering_id("gpt-4o").to_string(),
                    "catalog": fixtures::resource_id(13).to_string(),
                    "snapshot": digest.to_string(),
                    "wire_family": "openai-chat",
                }
            }),
        )
        .await;
    expected = deployment
        .publish(
            "/prices",
            "four-2",
            &expected,
            &json!({
                "summary": "approve openai gpt-4o",
                "mutation": "create",
                "resource": {
                    "price_book": fixtures::resource_id(31).to_string(),
                    "slug": "deployment-prices",
                    "catalog": snapshot.content.content_id().checksum().to_string(),
                    "catalog_version": 1,
                    "state": "approved",
                    "approved_at_millis": 0,
                    "rules": [{
                        "provider": "openai",
                        "model": "gpt-4o",
                        "precedence": "baseline",
                        "from_millis": 0,
                        "input_nano_dollars_per_million": 2_500_000_000u64,
                        "output_nano_dollars_per_million": 10_000_000_000u64,
                        "origin": "operator"
                    }]
                }
            }),
        )
        .await;
    expected = deployment
        .publish(
            "/aliases",
            "four-3",
            &expected,
            &json!({
                "summary": "alias gpt-4o",
                "mutation": "create",
                "resource": {
                    "alias": fixtures::resource_id(15).to_string(),
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "project": fixtures::project_id(2).to_string(),
                    "slug": "gpt-4o",
                    "wire_family": "openai-chat",
                    "targets": [{ "enablement": fixtures::resource_id(14).to_string() }],
                }
            }),
        )
        .await;

    let (status, body) = deployment
        .post("/bindings", "bind-adopt", &expected, &binding_update())
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], "unchanged", "{body}");

    let loaded = deployment
        .store
        .load_desired_revision()
        .await
        .expect("head")
        .expect("published");
    let books: Vec<_> = loaded
        .state()
        .resources()
        .filter(|resource| resource.reference.kind == ResourceKind::Price)
        .collect();
    assert_eq!(books.len(), 1, "one deployment book");
    assert_eq!(books[0].reference.id, fixtures::resource_id(31));
    let aliases: Vec<_> = loaded
        .state()
        .resources()
        .filter(|resource| resource.reference.kind == ResourceKind::Alias)
        .collect();
    assert_eq!(aliases.len(), 1, "one alias");
    assert_eq!(aliases[0].reference.id, fixtures::resource_id(15));
    let enablements: Vec<_> = loaded
        .state()
        .resources()
        .filter(|resource| resource.reference.kind == ResourceKind::ModelEnablement)
        .collect();
    assert_eq!(enablements.len(), 1, "one enablement");
    assert_eq!(enablements[0].reference.id, fixtures::resource_id(14));
}

#[tokio::test]
async fn identical_reapply_is_unchanged_only_when_expected_matches_head() {
    let (deployment, _) = imported_deployment().await;
    let expected = foundation(&deployment).await;
    let head = deployment
        .publish("/bindings", "bind-1", &expected, &binding_document())
        .await;
    let (status, body) = deployment
        .post("/bindings", "bind-2", &head, &binding_update())
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], "unchanged", "{body}");
    assert_eq!(body["revision"], head);
    assert_eq!(body["diff"]["resources"].as_array().unwrap().len(), 0);
    assert_eq!(deployment.store.published_revisions(), 5);
}

#[tokio::test]
async fn lost_response_retry_of_first_pin_classifies_deployment_and_replays() {
    let snapshot = seed_snapshot();
    let catalogue = Arc::new(InMemoryCatalogStore::new());
    catalogue
        .activate(
            &RetainedCatalog {
                source: snapshot.source.clone(),
                payload: RawPayload::new(SEED_PAYLOAD.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("the seed is retained");
    let recorder = RecordingAuthorizer::permissive();
    let deployment = Deployment::with_catalogue_authorizer(catalogue, recorder.clone());
    let expected = foundation(&deployment).await;
    let first = deployment
        .publish("/bindings", "bind-retry", &expected, &binding_document())
        .await;
    recorder.calls.lock().expect("not poisoned").clear();
    let (status, body) = deployment
        .post("/bindings", "bind-retry", &expected, &binding_document())
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], "replayed", "{body}");
    assert_eq!(body["revision"], first);
    assert_eq!(deployment.store.published_revisions(), 5);
    let calls = recorder.calls.lock().expect("not poisoned").clone();
    assert!(
        calls.iter().any(|(action, surface, scope)| {
            *action == AdminAction::Publish
                && *surface == Surface::Model
                && *scope == ResourceScope::Deployment
        }),
        "retry must probe Model at Deployment, got {calls:?}"
    );
    assert!(
        calls.iter().any(|(action, surface, scope)| {
            *action == AdminAction::Publish
                && *surface == Surface::Price
                && *scope == ResourceScope::Deployment
        }),
        "retry must probe Price at Deployment, got {calls:?}"
    );
}

#[tokio::test]
async fn dry_run_with_stale_expected_is_409_from_apply() {
    let (deployment, _) = imported_deployment().await;
    let expected = foundation(&deployment).await;
    let _head = deployment
        .publish("/bindings", "bind-dry", &expected, &binding_document())
        .await;
    let (status, body) = deployment
        .dry_run("/bindings", "bind-dry-stale", &expected, &binding_update())
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["type"], "revision_conflict");
}

#[tokio::test]
async fn pin_follow_disables_the_old_enablement_and_retargets_the_alias() {
    let snapshot = seed_snapshot();
    let mutated = SEED_PAYLOAD.replace("\"input\": 2.5", "\"input\": 2.51");
    let next = ModelsDevAdapter::default()
        .parse(
            mutated.as_bytes(),
            SourceValidators::default(),
            SystemTime::now(),
        )
        .expect("a one-field edit still parses");
    assert_ne!(next.source.raw.digest, snapshot.source.raw.digest);

    let store = Arc::new(InMemoryCatalogStore::new());
    store
        .activate(
            &RetainedCatalog {
                source: snapshot.source.clone(),
                payload: RawPayload::new(SEED_PAYLOAD.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("seed");
    let deployment = Deployment::with_catalogue(store.clone());
    let expected = foundation(&deployment).await;
    let head = deployment
        .publish("/bindings", "bind-pin-1", &expected, &binding_document())
        .await;

    store
        .activate(
            &RetainedCatalog {
                source: next.source.clone(),
                payload: RawPayload::new(mutated.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("refresh");

    let (status, body) = deployment
        .post("/bindings", "bind-pin-2", &head, &binding_update())
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], "published", "{body}");

    let loaded = deployment
        .store
        .load_desired_revision()
        .await
        .expect("head")
        .expect("published");
    let enablements: Vec<_> = loaded
        .state()
        .resources()
        .filter(|resource| resource.reference.kind == ResourceKind::ModelEnablement)
        .filter_map(|resource| {
            ModelEnablementBody::read(resource)
                .ok()
                .map(|body| (resource, body))
        })
        .collect();
    let enabled: Vec<_> = enablements
        .iter()
        .filter(|(_, body)| body.is_enabled())
        .collect();
    assert_eq!(enabled.len(), 1, "one enabled pin");
    assert_eq!(enabled[0].1.offering().snapshot, next.source.raw.digest);
    assert!(
        enablements.iter().any(|(_, body)| !body.is_enabled()
            && body.offering().snapshot == snapshot.source.raw.digest),
        "old pin is disabled"
    );

    let followed = body["revision"].as_str().expect("revision").to_owned();
    let locked_payload = mutated.replace("\"input\": 2.51", "\"input\": 2.52");
    let locked_snap = ModelsDevAdapter::default()
        .parse(
            locked_payload.as_bytes(),
            SourceValidators::default(),
            SystemTime::now(),
        )
        .expect("another edit still parses");
    store
        .activate(
            &RetainedCatalog {
                source: locked_snap.source.clone(),
                payload: RawPayload::new(locked_payload.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("second refresh");
    let (status, locked) = deployment
        .post(
            "/bindings",
            "bind-pin-lock",
            &followed,
            &json!({
                "summary": "lock gpt-4o",
                "mutation": "update",
                "resource": {
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "project": fixtures::project_id(2).to_string(),
                    "pin": "lock",
                    "targets": [{
                        "provider": "openai",
                        "model": "gpt-4o",
                        "price": {
                            "input_microdollars_per_million": 2_500_000u64,
                            "output_microdollars_per_million": 10_000_000u64
                        }
                    }]
                }
            }),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{locked}");
    assert_eq!(locked["error"]["type"], "binding_refused");
    assert_eq!(locked["error"]["rule"], "pin_locked");
}

fn seed_with_second_dotted_openai_model() -> String {
    let mut catalog: Value = serde_json::from_str(SEED_PAYLOAD).expect("seed json");
    let mut dotted = catalog["providers"]["openai"]["models"]["gpt-4o"].clone();
    dotted["id"] = json!("gpt-4.1");
    dotted["name"] = json!("GPT-4.1");
    catalog["providers"]["openai"]["models"]["gpt-4.1"] = dotted;
    catalog.to_string()
}

fn dotted_binding(model: &str, input: u64, output: u64, mutation: &str) -> Value {
    json!({
        "summary": format!("{mutation} {model}"),
        "mutation": mutation,
        "resource": {
            "tenant": fixtures::tenant_id(1).to_string(),
            "project": fixtures::project_id(2).to_string(),
            "targets": [{
                "provider": "openai",
                "model": model,
                "catalog": { "provider": "openai", "model": model },
                "price": {
                    "input_microdollars_per_million": input,
                    "output_microdollars_per_million": output
                }
            }]
        }
    })
}

#[tokio::test]
async fn two_dotted_aliases_pin_follow_in_one_project() {
    let v1 = seed_with_second_dotted_openai_model();
    let snapshot = ModelsDevAdapter::default()
        .parse(
            v1.as_bytes(),
            SourceValidators::default(),
            SystemTime::now(),
        )
        .expect("seed with gpt-4.1 parses");
    let v2 = v1.replace("2024-08-06", "2024-08-07");
    let next = ModelsDevAdapter::default()
        .parse(
            v2.as_bytes(),
            SourceValidators::default(),
            SystemTime::now(),
        )
        .expect("refreshed seed parses");
    assert_ne!(next.source.raw.digest, snapshot.source.raw.digest);

    let store = Arc::new(InMemoryCatalogStore::new());
    store
        .activate(
            &RetainedCatalog {
                source: snapshot.source.clone(),
                payload: RawPayload::new(v1.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("seed");
    let deployment = Deployment::with_catalogue(store.clone());
    let mut expected = foundation(&deployment).await;
    expected = deployment
        .publish(
            "/bindings",
            "bind-dot-1",
            &expected,
            &dotted_binding("gpt-5.5", 5_000_000, 30_000_000, "create"),
        )
        .await;
    expected = deployment
        .publish(
            "/bindings",
            "bind-dot-2",
            &expected,
            &dotted_binding("gpt-4.1", 2_500_000, 10_000_000, "create"),
        )
        .await;

    store
        .activate(
            &RetainedCatalog {
                source: next.source.clone(),
                payload: RawPayload::new(v2.as_bytes()),
            },
            SystemTime::now(),
        )
        .await
        .expect("refresh");

    expected = deployment
        .publish(
            "/bindings",
            "bind-dot-3",
            &expected,
            &dotted_binding("gpt-5.5", 5_000_000, 30_000_000, "update"),
        )
        .await;
    let (status, body) = deployment
        .post(
            "/bindings",
            "bind-dot-4",
            &expected,
            &dotted_binding("gpt-4.1", 2_500_000, 10_000_000, "update"),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"], "published", "{body}");

    let loaded = deployment
        .store
        .load_desired_revision()
        .await
        .expect("head")
        .expect("published");
    let mut aliases: Vec<_> = loaded
        .state()
        .resources()
        .filter(|resource| resource.reference.kind == ResourceKind::Alias)
        .map(|resource| resource.slug.as_str())
        .collect();
    aliases.sort_unstable();
    assert_eq!(aliases, ["gpt-4.1", "gpt-5.5"]);

    let enablements: Vec<_> = loaded
        .state()
        .resources()
        .filter(|resource| resource.reference.kind == ResourceKind::ModelEnablement)
        .filter_map(|resource| {
            ModelEnablementBody::read(resource)
                .ok()
                .map(|body| (resource.slug.as_str(), body))
        })
        .collect();
    assert_eq!(enablements.len(), 4, "two pins each, old disabled");
    let mut enabled: Vec<_> = enablements
        .iter()
        .filter(|(_, body)| body.is_enabled())
        .map(|(slug, _)| *slug)
        .collect();
    enabled.sort_unstable();
    enabled.dedup();
    assert_eq!(enabled.len(), 2, "two distinct enabled slugs: {enabled:?}");
    assert!(
        !enabled.contains(&"gpt-5.5") && !enabled.contains(&"gpt-4.1"),
        "disabled rows keep the published ids, got {enabled:?}"
    );
    assert!(
        enablements
            .iter()
            .any(|(slug, body)| *slug == "gpt-5.5" && !body.is_enabled())
            && enablements
                .iter()
                .any(|(slug, body)| *slug == "gpt-4.1" && !body.is_enabled()),
        "old pins keep the preferred slugs"
    );
}

#[tokio::test]
async fn published_not_neutral_uses_catalog_model_when_provider_equals_slug() {
    let (deployment, _) = imported_deployment().await;
    let expected = foundation(&deployment).await;
    let document = json!({
        "summary": "enable gpt-5.5",
        "mutation": "create",
        "resource": {
            "tenant": fixtures::tenant_id(1).to_string(),
            "project": fixtures::project_id(2).to_string(),
            "targets": [{
                "provider": "openai",
                "model": "gpt-5.5",
                "catalog": { "provider": "openai", "model": "gpt-5.5" },
                "price": {
                    "input_microdollars_per_million": 5_000_000u64,
                    "output_microdollars_per_million": 30_000_000u64
                }
            }]
        }
    });
    let _head = deployment
        .publish("/bindings", "bind-neutral", &expected, &document)
        .await;
    let loaded = deployment
        .store
        .load_desired_revision()
        .await
        .expect("head")
        .expect("published");
    let offering = OfferingId::of("openai", "openai/gpt-5.5").expect("id");
    let wrong = OfferingId::of("openai", "gpt-5.5").expect("id");
    let enablement = loaded
        .state()
        .resources()
        .find(|resource| resource.reference.kind == ResourceKind::ModelEnablement)
        .expect("an enablement");
    let body = ModelEnablementBody::read(enablement).expect("readable");
    assert_eq!(body.offering().offering, offering);
    assert_ne!(body.offering().offering, wrong);
    let alias = loaded
        .state()
        .resources()
        .find(|resource| resource.reference.kind == ResourceKind::Alias)
        .expect("an alias");
    assert_eq!(alias.slug.as_str(), "gpt-5.5");
}

#[tokio::test]
async fn catalogue_identity_is_required_when_provider_is_not_the_callable() {
    let (deployment, _) = imported_deployment().await;
    let mut expected = foundation(&deployment).await;
    expected = deployment
        .publish(
            "/providers",
            "found-prod",
            &expected,
            &json!({
                "summary": "openai-prod",
                "mutation": "create",
                "resource": {
                    "provider": fixtures::resource_id(40).to_string(),
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "slug": "openai-prod",
                    "display_name": "OpenAI prod",
                    "wire_family": "openai-chat",
                    "endpoint": "https://api.openai.com",
                }
            }),
        )
        .await;

    let mismatched = json!({
        "summary": "openai-prod is not openai",
        "mutation": "create",
        "resource": {
            "tenant": fixtures::tenant_id(1).to_string(),
            "project": fixtures::project_id(2).to_string(),
            "targets": [{
                "provider": "openai-prod",
                "model": "gpt-4o",
                "catalog": { "provider": "openai", "model": "gpt-4o" },
                "price": {
                    "input_microdollars_per_million": 2_500_000u64,
                    "output_microdollars_per_million": 10_000_000u64
                }
            }]
        }
    });
    let (status, body) = deployment
        .post("/bindings", "bind-id-1", &expected, &mismatched)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "binding_refused");
    assert_eq!(body["error"]["rule"], "catalogue_identity_required");

    let azure = json!({
        "summary": "azure-openai is not a callable",
        "mutation": "create",
        "resource": {
            "tenant": fixtures::tenant_id(1).to_string(),
            "project": fixtures::project_id(2).to_string(),
            "targets": [{
                "provider": "azure-openai",
                "model": "gpt-4o",
                "price": {
                    "input_microdollars_per_million": 2_500_000u64,
                    "output_microdollars_per_million": 10_000_000u64
                }
            }]
        }
    });
    expected = deployment
        .publish(
            "/providers",
            "found-azure",
            &expected,
            &json!({
                "summary": "azure-openai",
                "mutation": "create",
                "resource": {
                    "provider": fixtures::resource_id(41).to_string(),
                    "tenant": fixtures::tenant_id(1).to_string(),
                    "slug": "azure-openai",
                    "display_name": "Azure OpenAI",
                    "wire_family": "openai-chat",
                    "endpoint": "https://example.openai.azure.com",
                }
            }),
        )
        .await;
    let (status, body) = deployment
        .post("/bindings", "bind-id-2", &expected, &azure)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "binding_refused");
    assert_eq!(body["error"]["rule"], "catalogue_identity_required");
}

#[tokio::test]
async fn binding_book_is_actor_aware_and_enablements_have_no_approved_price() {
    let (deployment, _) = imported_deployment().await;
    let expected = foundation(&deployment).await;
    let _head = deployment
        .publish("/bindings", "bind-actor", &expected, &binding_document())
        .await;
    let loaded = deployment
        .store
        .load_desired_revision()
        .await
        .expect("head")
        .expect("published");
    let book = loaded
        .state()
        .resources()
        .find(|resource| resource.reference.kind == ResourceKind::Price)
        .expect("a book");
    let body = PriceBookBody::read(book).expect("readable");
    assert_eq!(
        body.approval().approver(),
        Some(&Actor::Human {
            issuer: ISSUER.to_owned(),
            subject: SUBJECT.to_owned(),
        })
    );
    for resource in loaded
        .state()
        .resources()
        .filter(|resource| resource.reference.kind == ResourceKind::ModelEnablement)
    {
        let body = ModelEnablementBody::read(resource).expect("readable");
        assert!(
            body.billable_price().is_none(),
            "expander leaves approved_price unset"
        );
    }
}

#[tokio::test]
async fn a_rate_change_on_existing_coverage_is_refused() {
    let (deployment, _) = imported_deployment().await;
    let expected = foundation(&deployment).await;
    let head = deployment
        .publish("/bindings", "bind-rate-1", &expected, &binding_document())
        .await;
    let mut changed = binding_update();
    changed["resource"]["targets"][0]["price"]["input_microdollars_per_million"] =
        json!(3_000_000u64);
    let (status, body) = deployment
        .post("/bindings", "bind-rate-2", &head, &changed)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["type"], "binding_refused");
    assert_eq!(body["error"]["rule"], "price_change_requires_interval");
}

#[tokio::test]
async fn pin_lock_on_first_apply_pins_the_active_digest() {
    let (deployment, snapshot) = imported_deployment().await;
    let expected = foundation(&deployment).await;
    let mut document = binding_document();
    document["resource"]["pin"] = json!("lock");
    let _head = deployment
        .publish("/bindings", "bind-lock-first", &expected, &document)
        .await;
    let loaded = deployment
        .store
        .load_desired_revision()
        .await
        .expect("head")
        .expect("published");
    let enablement = loaded
        .state()
        .resources()
        .find(|resource| resource.reference.kind == ResourceKind::ModelEnablement)
        .expect("an enablement");
    let body = ModelEnablementBody::read(enablement).expect("readable");
    assert_eq!(body.offering().snapshot, snapshot.source.raw.digest);
    assert!(body.is_enabled());
}

#[tokio::test]
async fn a_missing_stale_expected_is_a_conflict_from_apply() {
    let (deployment, _) = imported_deployment().await;
    let expected = foundation(&deployment).await;
    let _head = deployment
        .publish("/bindings", "bind-missing", &expected, &binding_document())
        .await;
    let (status, body) = deployment
        .post(
            "/bindings",
            "bind-missing-stale",
            &fixtures::revision_id(99).to_string(),
            &binding_update(),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["type"], "revision_conflict");
}

#[tokio::test]
async fn unchanged_still_requires_a_publish_grant() {
    let (deployment, _) = imported_deployment().await;
    let expected = foundation(&deployment).await;
    let head = deployment
        .publish("/bindings", "bind-auth-1", &expected, &binding_document())
        .await;
    let reader = deployment.reauthorize(Arc::new(FakeAdminAuthorizer::permitting(&[
        AdminAction::ReadState,
    ])));
    let (status, body) = reader
        .post("/bindings", "bind-auth-2", &head, &binding_update())
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["type"], "admin_forbidden");
}
