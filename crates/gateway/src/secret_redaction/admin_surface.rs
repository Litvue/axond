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
use crate::convergence::{RevisionStatus, SnapshotSource, SystemClock};
use crate::desired_state::{RevisionId, SecretLifecycle, SecretRef, fixtures};

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

/// A catalogue snapshot document: `digest` is a long opaque content address, so
/// it is a field a mispasted key fits without looking out of place.
fn catalog_document() -> Value {
    json!({
        "summary": "pin the catalogue snapshot",
        "mutation": "create",
        "resource": {
            "catalog": fixtures::resource_id(12).to_string(),
            "slug": "models-dev",
            "digest": format!("sha256:{}", "a".repeat(64)),
            "size_bytes": 4_096,
        }
    })
}

/// A model enablement document, whose `offering` and `snapshot` are both long
/// opaque digests.
fn model_document() -> Value {
    json!({
        "summary": "enable one offering for acme",
        "mutation": "create",
        "resource": {
            "enablement": fixtures::resource_id(13).to_string(),
            "tenant": fixtures::tenant_id(1).to_string(),
            "slug": "gpt-4o",
            "offering": format!("off_{}", "b".repeat(64)),
            "catalog": fixtures::resource_id(12).to_string(),
            "snapshot": format!("sha256:{}", "a".repeat(64)),
            "wire_family": "openai-chat",
            "state": "enabled",
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

    // A convergence status is attached, and driven below with the revision this
    // test actually publishes: an unreconciled replica answers `/convergence`
    // with a projection of nothing, which would sweep clean for the wrong
    // reason.
    let convergence = Arc::new(RevisionStatus::new(Box::new(SystemClock)));
    let console = Console {
        api: Arc::new(
            AdminApi::new(
                Arc::new(AdminService::stateful(Arc::new(journal))),
                Arc::new(FakeAdminAuthenticator::new().with_human(TOKEN, ISSUER, SUBJECT)),
                Arc::new(FakeAdminAuthorizer::permissive()),
            )
            .with_convergence(convergence.clone()),
        ),
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
    assert!(
        refusal.contains("not a `sct_`-prefixed secret id"),
        "a wrong-prefix refusal should identify the expected form: {refusal}"
    );
    sweep.assert_absent("a mispasted-material refusal", &refusal);

    // A reference with the right prefix but a malformed UUID needs a different
    // explanation, and still must not repeat the pasted identifier.
    const MALFORMED_SECRET_REFERENCE: &str = "sct_not-a-hyphenated-uuid";
    let mut malformed_reference = credential.clone();
    malformed_reference["resource"]["secret"] = json!(MALFORMED_SECRET_REFERENCE);
    let (status, refusal) = console
        .post(
            "/credentials",
            "malformed-reference",
            &revision,
            &malformed_reference,
            false,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refusal}");
    assert!(
        refusal.contains("hyphenated 8-4-4-4-12 uuid"),
        "a right-prefix refusal should identify the malformed identifier: {refusal}"
    );
    assert!(
        !refusal.contains(MALFORMED_SECRET_REFERENCE),
        "a malformed secret reference must not be echoed: {refusal}"
    );

    // Provider endpoints are another document field whose value can reach
    // publication-time validation. The refusal names the required form, but
    // never copies the endpoint that was pasted into the admin request.
    let mut mispasted_endpoint = provider_document();
    mispasted_endpoint["resource"]["provider"] = json!(fixtures::resource_id(99).to_string());
    // Keep this fixture distinct from the provider published above so the
    // endpoint validation is the refusal that the assertion exercises.
    mispasted_endpoint["resource"]["slug"] = json!("mispasted-endpoint");
    mispasted_endpoint["resource"]["endpoint"] = json!(PROVIDER_MATERIAL);
    let (status, refusal) = console
        .post(
            "/providers",
            "mispasted-provider-endpoint",
            &revision,
            &mispasted_endpoint,
            false,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{refusal}");
    assert!(
        refusal.contains("not an absolute http(s) origin"),
        "an endpoint refusal should identify the expected form: {refusal}"
    );
    sweep.assert_absent("a mispasted provider endpoint refusal", &refusal);

    // The same disclosure boundary applies when material is pasted into a
    // different document field. Each value is deliberately invalid for its
    // field so the response exercises validation rather than publishing a
    // caller-chosen name that merely happens to resemble a key.
    for (field, value) in [
        ("credential", json!(format!("{PROVIDER_MATERIAL}!"))),
        ("tenant", json!(format!("{PROVIDER_MATERIAL}!"))),
        ("project", json!(format!("{PROVIDER_MATERIAL}!"))),
        ("provider", json!(format!("{PROVIDER_MATERIAL}!"))),
        ("slug", json!(format!("{PROVIDER_MATERIAL}!"))),
        ("display_name", json!(format!("{PROVIDER_MATERIAL}\n"))),
    ] {
        let mut invalid = credential.clone();
        invalid["resource"][field] = value;
        let key = format!("mispasted-{field}");
        let (status, refusal) = console
            .post("/credentials", &key, &revision, &invalid, false)
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {refusal}");
        sweep.assert_absent(&format!("a mispasted {field} refusal"), &refusal);
    }

    // The credential is not the only document with fields shaped like material.
    // A digest, a catalogue content address and an offering identity are all long
    // opaque strings, and a closed-set field is refused with the set this build
    // accepts rather than with whatever arrived in it.
    for (path, document, field, value, expected) in [
        (
            "/catalogs",
            catalog_document(),
            "digest",
            json!(PROVIDER_MATERIAL),
            "is not prefixed `sha256:`",
        ),
        (
            "/models",
            model_document(),
            "offering",
            json!(PROVIDER_MATERIAL),
            "is not prefixed `off_`",
        ),
        (
            "/models",
            model_document(),
            "snapshot",
            json!(format!("sha256:{PROVIDER_MATERIAL}")),
            "does not carry 64 lowercase hex digits",
        ),
        (
            "/models",
            model_document(),
            "wire_family",
            json!(PROVIDER_MATERIAL),
            "it accepts `openai-chat`, `anthropic-messages`",
        ),
        (
            "/models",
            model_document(),
            "state",
            json!(PROVIDER_MATERIAL),
            "it accepts `enabled`, `disabled`",
        ),
        (
            "/tenants",
            tenant_document(),
            "lifecycle",
            json!(PROVIDER_MATERIAL),
            "it accepts `active`, `disabled`, `deleted`",
        ),
    ] {
        let mut invalid = document;
        invalid["resource"][field] = value;
        let key = format!("mispasted-{path}-{field}");
        let (status, refusal) = console.post(path, &key, &revision, &invalid, false).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{field}: {refusal}");
        assert!(
            refusal.contains(expected),
            "the {field} refusal should say {expected}: {refusal}"
        );
        sweep.assert_absent(&format!("a mispasted {path} {field} refusal"), &refusal);
    }

    // A reconciler's view of the revision this test published, recorded the way
    // the convergence loop records it, so `/convergence` projects real revision
    // identity rather than the empty report of a replica with nothing attached.
    let converged = RevisionId::parse(&revision).expect("a published revision id");
    convergence.observe_desired(Some(converged));
    convergence.record_published(
        converged,
        1,
        SnapshotSource::ControlPlane,
        std::time::Duration::from_millis(120),
    );

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

    // And the convergence read has to be about the revision that carries the
    // credential, for the same reason.
    let (_, body) = console.get("/convergence").await;
    let projection: Value = serde_json::from_str(&body).expect("a JSON response");
    assert_eq!(projection["reconciling"], json!(true), "{body}");
    assert_eq!(projection["converged"], json!(true), "{body}");
    assert_eq!(projection["desired"], json!(revision), "{body}");
    assert_eq!(projection["active"], json!(revision), "{body}");
    assert_eq!(projection["source"], json!("control-plane"), "{body}");
    assert_eq!(projection["generation"], json!(1), "{body}");

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
