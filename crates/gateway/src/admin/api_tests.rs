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

use super::auth::AdminAction;
use super::auth::INFERENCE_KEY_HEADER;
use super::fakes::{CountingStore, FakeAdminAuthenticator, FakeAdminAuthorizer};
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
use crate::backends::control_plane::ControlPlaneStore;
use crate::backends::fakes::InMemorySecrets;
use crate::desired_state::oracle::InMemoryControlPlane;
use crate::desired_state::{DenialPage, ResourceScope, fixtures};

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
        Self { api, store }
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
        Self { api, store }
    }

    /// The same control plane, read through a narrower grant: what a tenant
    /// administrator sees of a deployment somebody with deployment authority
    /// built.
    fn narrowed(&self, scopes: &[ResourceScope]) -> Self {
        Self {
            api: Arc::new(AdminApi::new(
                Arc::new(AdminService::stateful(self.store.clone())),
                Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
                Arc::new(FakeAdminAuthorizer::permissive().within(scopes)),
            )),
            store: self.store.clone(),
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
    head = deployment
        .publish("/models", "key-model-2", &head, &disabled)
        .await;

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
    let enablement = state
        .version_of(
            crate::desired_state::ResourceKind::ModelEnablement,
            fixtures::resource_id(14),
        )
        .expect("the enablement is desired");
    let alias = state
        .version_of(
            crate::desired_state::ResourceKind::Alias,
            fixtures::resource_id(15),
        )
        .expect("the alias is desired");
    let alias_body = crate::desired_state::ModelAliasBody::read(alias).expect("an alias body");
    assert_eq!(
        alias_body.primary().expect("a target").version,
        enablement.reference.version,
        "the alias follows the enablement it names"
    );
    // Re-posting an alias document that omits its target version resolves
    // against the enablement the state holds rather than against version 1.
    deployment
        .publish("/aliases", "key-alias-2", &head, &alias_document())
        .await;
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
        let path = format!("{ADMIN_PREFIX}{}", spec.path.replace("{revision}", "rev"));
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
/// created, the alias that names it, and the one thing still standing between
/// them and a routable model.
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
    assert_eq!(aliases[0]["targets"].as_array().unwrap().len(), 1);
    // Enabled and named, and still not routable: nobody approved a price. The
    // read says which of the two acts is missing rather than reporting a bare
    // "unavailable".
    assert_eq!(entry["billable"], json!(false));
    assert_eq!(entry["routable"], json!(false));
    assert_eq!(entry["unavailable"], json!(["unpriced"]));
    // And it says what this build could not consult, so an operator does not read
    // silence as an all-clear.
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
/// parameter: a caller that filtered on a spelling this build does not know must
/// not be handed an unfiltered catalogue and believe it was filtered.
#[tokio::test]
async fn a_catalogue_filter_this_build_cannot_read_is_refused() {
    let deployment = Deployment::new();
    build(&deployment).await;

    for query in [
        "tenant=not-a-uuid".to_owned(),
        format!("tenant={}&state=retired", fixtures::tenant_id(1)),
        format!("tenant={}&wire_family=telepathy", fixtures::tenant_id(1)),
        format!("tenant={}&offering=nonsense", fixtures::tenant_id(1)),
        format!("tenant={}&unknown=1", fixtures::tenant_id(1)),
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
        let path = format!("{ADMIN_PREFIX}{}", spec.path.replace("{revision}", "rev"));
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
    let deployment = Deployment { api, store };

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
    assert_eq!(versions["versions"].as_array().expect("versions").len(), 0);
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
