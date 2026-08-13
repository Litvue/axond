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

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::auth::INFERENCE_KEY_HEADER;
use super::fakes::{CountingStore, FakeAdminAuthenticator, FakeAdminAuthorizer};
use super::protocol::{
    ADMIN_PREFIX, DRY_RUN_HEADER, EXPECTED_REVISION_EMPTY, EXPECTED_REVISION_HEADER,
    IDEMPOTENCY_KEY_HEADER,
};
use super::router::{AdminApi, refusing_router, router};
use super::service::AdminService;
use crate::backends::control_plane::ControlPlaneStore;
use crate::desired_state::oracle::InMemoryControlPlane;
use crate::desired_state::{ResourceScope, fixtures};

const TOKEN: &str = "human-admin-token";
const ISSUER: &str = "https://idp.example";
const SUBJECT: &str = "operator@example";

/// One administrative deployment under test: the surface, and the store behind
/// it.
struct Deployment {
    api: Arc<AdminApi>,
    store: Arc<InMemoryControlPlane>,
}

impl Deployment {
    fn new() -> Self {
        Self::with_authorizer(FakeAdminAuthorizer::permissive())
    }

    fn with_authorizer(authorizer: FakeAdminAuthorizer) -> Self {
        let store = Arc::new(InMemoryControlPlane::new());
        let api = Arc::new(AdminApi::new(
            Arc::new(AdminService::stateful(store.clone())),
            Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
            Arc::new(authorizer),
        ));
        Self { api, store }
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
    let deployment = Deployment { api, store };

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
        let builder = if spec.action.mutates() {
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
        let builder = if spec.action.mutates() {
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
