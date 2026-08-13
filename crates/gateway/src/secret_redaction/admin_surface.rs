//! The administrative surface, swept response by response.
//!
//! [`super::journal`] proves the *store* never persists material and
//! [`super::request_path`] proves a *served request* never emits it. Neither
//! covers the surface an operator actually holds a session against: `/admin/v1`
//! renders documents back as diffs, state, history, audit trails, convergence,
//! idempotent replays, and refusals, and every one of those is a rendering of a
//! credential that names live material.
//!
//! So this module drives the real route table over the real PostgreSQL journal,
//! publishes a credential pointing at a secret that has genuinely been staged
//! and activated in the production store, and sweeps every response body an
//! administrator can obtain — including the error envelopes, which are the
//! surface most likely to render a document verbatim while explaining what was
//! wrong with it.
//!
//! The material is resolved out of the store and held live across every
//! assertion, and the sweep is checked against it first: a redaction test whose
//! sentinel never entered the system passes for the wrong reason.
//!
//! Requires PostgreSQL in CI (`AXOND_TEST_REQUIRE_SERVICES=1` turns a missing
//! DSN into a panic rather than a skip), so a green stateful lane means these
//! ran.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt as _;
use serde_json::{Value, json};
use tower::util::ServiceExt as _;

use super::harness::{PROVIDER_MATERIAL, material, owner, sweep};
use crate::admin::fakes::{FakeAdminAuthenticator, FakeAdminAuthorizer};
use crate::admin::protocol::{
    ADMIN_PREFIX, DRY_RUN_HEADER, EXPECTED_REVISION_EMPTY, EXPECTED_REVISION_HEADER,
    IDEMPOTENCY_KEY_HEADER,
};
use crate::admin::router::{AdminApi, router};
use crate::admin::service::AdminService;
use crate::backends::secrets::{SecretResolver as _, SecretStore as _};
use crate::desired_state::{SecretLifecycle, SecretRef, fixtures};

const TOKEN: &str = "human-admin-token";
const ISSUER: &str = "https://idp.example";
const SUBJECT: &str = "operator@example";

/// The administrative surface under test, mounted over the durable journal.
struct Console {
    api: Arc<AdminApi>,
}

impl Console {
    async fn send(&self, request: Request<Body>) -> (StatusCode, String) {
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
        (status, String::from_utf8_lossy(&body).into_owned())
    }

    async fn get(&self, path: &str) -> (StatusCode, String) {
        self.send(
            Request::get(format!("{ADMIN_PREFIX}{path}"))
                .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .expect("a request"),
        )
        .await
    }

    async fn post(
        &self,
        path: &str,
        key: &str,
        expected: &str,
        document: &Value,
        dry_run: bool,
    ) -> (StatusCode, String) {
        let mut request = Request::post(format!("{ADMIN_PREFIX}{path}"))
            .header(axum::http::header::AUTHORIZATION, format!("Bearer {TOKEN}"))
            .header(IDEMPOTENCY_KEY_HEADER, key)
            .header(EXPECTED_REVISION_HEADER, expected);
        if dry_run {
            request = request.header(DRY_RUN_HEADER, "true");
        }
        self.send(
            request
                .body(Body::from(document.to_string()))
                .expect("a request"),
        )
        .await
    }

    /// Publish a document that must succeed, returning the revision it produced.
    async fn publish(&self, path: &str, key: &str, expected: &str, document: &Value) -> String {
        let (status, body) = self.post(path, key, expected, document, false).await;
        assert_eq!(status, StatusCode::OK, "{path} refused: {body}");
        let parsed: Value = serde_json::from_str(&body).expect("a JSON response");
        parsed["revision"]
            .as_str()
            .expect("a published revision")
            .to_owned()
    }
}

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

fn provider_document() -> Value {
    json!({
        "summary": "connect acme to the fake provider",
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

/// The credential an administrator publishes: a *reference* to the version that
/// was staged in the store, and never the material behind it.
fn credential_document(secret: &SecretRef) -> Value {
    json!({
        "summary": "stage acme's provider key",
        "mutation": "create",
        "resource": {
            "credential": fixtures::resource_id(11).to_string(),
            "tenant": fixtures::tenant_id(1).to_string(),
            "provider": fixtures::resource_id(10).to_string(),
            "slug": "openai-primary",
            "display_name": "OpenAI primary",
            "secret": secret.secret.to_string(),
            "secret_version": secret.version.get(),
            "lifecycle": "active",
        }
    })
}

/// Every administrative response about a credential renders the reference and
/// never the material, including the refusals.
#[tokio::test]
async fn no_administrative_response_discloses_the_material_a_credential_names() {
    let Some((journal, journal_schema)) = super::journal::journal().await else {
        return;
    };
    let Some((secrets, secret_schema)) = super::stateful::store().await else {
        return;
    };
    let sweep = sweep();

    // Material that genuinely exists: staged and activated through the shipped
    // store, then resolved and held live so the sweep has something to find.
    let staged = secrets
        .stage(owner(), material(PROVIDER_MATERIAL))
        .await
        .expect("the store accepts material");
    secrets
        .transition(owner(), &staged.reference, SecretLifecycle::Active)
        .await
        .expect("staged material may be activated");
    let live = secrets
        .resolve(owner(), &staged.reference)
        .await
        .expect("the runtime resolves it");
    sweep.assert_present(
        "the material the credential names",
        "provider",
        live.expose(),
    );

    let console = Console {
        api: Arc::new(AdminApi::new(
            Arc::new(AdminService::stateful(Arc::new(journal))),
            Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
            Arc::new(FakeAdminAuthorizer::permissive()),
        )),
    };
    let credential = credential_document(&staged.reference);

    let mut head = EXPECTED_REVISION_EMPTY.to_owned();
    for (index, (path, document)) in [
        ("/tenants", tenant_document()),
        ("/providers", provider_document()),
    ]
    .iter()
    .enumerate()
    {
        head = console
            .publish(path, &format!("key-{index}"), &head, document)
            .await;
    }

    // A dry run renders the diff of a candidate that names the secret, without
    // publishing it.
    let (status, diff) = console
        .post("/credentials", "dry-run", &head, &credential, true)
        .await;
    assert_eq!(status, StatusCode::OK, "{diff}");
    sweep.assert_absent("a dry-run diff", &diff);

    let (status, published) = console
        .post("/credentials", "publish", &head, &credential, false)
        .await;
    assert_eq!(status, StatusCode::OK, "{published}");
    sweep.assert_absent("a publish response", &published);
    let parsed: Value = serde_json::from_str(&published).expect("a JSON response");
    let revision = parsed["revision"]
        .as_str()
        .expect("a published revision")
        .to_owned();

    // A replay of the same key returns the original outcome, from stored
    // idempotency state rather than from a fresh evaluation.
    let (status, replayed) = console
        .post("/credentials", "publish", &head, &credential, false)
        .await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    let parsed: Value = serde_json::from_str(&replayed).expect("a JSON response");
    assert_eq!(parsed["result"], "replayed", "{replayed}");
    sweep.assert_absent("an idempotent replay", &replayed);

    // A key spent on a different candidate is refused, and the refusal explains
    // the conflict without echoing what was in either document.
    let mut repointed = credential.clone();
    repointed["resource"]["display_name"] = json!("OpenAI renamed");
    let (status, conflict) = console
        .post("/credentials", "publish", &head, &repointed, false)
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");
    sweep.assert_absent("a reused-key refusal", &conflict);

    // A document is the surface most likely to be echoed back, and `secret` is
    // the field an operator can paste material into instead of a reference to
    // it. The refusal has to explain the mistake without repeating it.
    let mut mispasted = credential.clone();
    mispasted["resource"]["secret"] = json!(PROVIDER_MATERIAL);
    let (status, refusal) = console
        .post("/credentials", "mispasted", &revision, &mispasted, false)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refusal}");
    sweep.assert_absent("a mispasted-material refusal", &refusal);

    for path in [
        "/state".to_owned(),
        "/history".to_owned(),
        format!("/audit/{revision}"),
        "/convergence".to_owned(),
    ] {
        let (status, body) = console.get(&path).await;
        assert_eq!(status, StatusCode::OK, "{path}: {body}");
        assert!(!body.is_empty(), "{path} answered with nothing to sweep");
        sweep.assert_absent(&format!("the {path} response"), &body);
    }

    // The state read has to be about the credential for its sweep to mean
    // anything: an empty projection would pass trivially.
    let (_, state) = console.get("/state").await;
    assert!(
        state.contains("provider-credential"),
        "the state read does not describe the credential: {state}"
    );

    sweep.assert_absent(
        "the journal's durable rows",
        &super::journal::dump(&journal_schema).await,
    );
    sweep.assert_absent(
        "the secret store's rows",
        &super::stateful::dump(&secret_schema).await,
    );
    super::stateful::drop_schema(&secret_schema).await;
}
