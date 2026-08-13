//! The credential lifecycle end to end, against the production secret store.
//!
//! [`super::lifecycle`] asserts the same properties against an in-memory store,
//! which is where the *timing* cases belong: gating a provider call, dropping a
//! superseded snapshot, counting resolutions. What that cannot show is that the
//! shipped store is the one the runtime can be driven through — an envelope,
//! per-version rows, ownership checked on every read — so this module runs the
//! zero-redeploy sequence an operator actually performs against PostgreSQL:
//!
//! 1. stage material, activate it, publish the credential, serve a request with
//!    it;
//! 2. rotate: stage the next version, activate it, withdraw the old credential,
//!    serve a request with the new key — no restart anywhere;
//! 3. roll back to the previous revision and serve with the previous key, which
//!    only works because the old version's row still exists and still resolves;
//! 4. tombstone the old version and watch a candidate that pins it be refused
//!    whole, with the last known good snapshot still serving the current key.
//!
//! Nothing here reaches the store from the request path: every resolution happens
//! while a candidate is compiled, which the resolution count asserts.
//!
//! Skipped when no PostgreSQL DSN is configured, and a panic instead of a skip
//! under `AXOND_TEST_REQUIRE_SERVICES=1`, so the stateful lane cannot pass by not
//! running it.

use std::sync::Arc;
use std::time::Duration;

use axum::http::StatusCode;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use tower::util::ServiceExt as _;

use super::harness::{
    FakeProvider, PROVIDER_MATERIAL, ROTATED_MATERIAL, Replica, chat_request, material, owner,
    state_pinning, sweep,
};
use crate::backends::secrets::envelope::DeploymentKek;
use crate::backends::secrets::postgres::{PostgresSecrets, SecretStoreSettings};
use crate::backends::secrets::{KekRef, SecretStore as _};
use crate::convergence::Outcome;
use crate::desired_state::{
    ResourceVersionNumber, SecretLifecycle, SecretOwner, SecretRef, Uuid7Generator, fixtures,
};
use crate::routes::router;

/// A store on a schema of its own, so a run leaves nothing for the next one.
pub(super) async fn store() -> Option<(Arc<PostgresSecrets>, String)> {
    let dsn = crate::test_services::postgres_dsn()?;
    let schema = format!(
        "axond_secret_runtime_{}",
        Uuid7Generator::new().next().to_string().replace('-', "")
    );
    let (client, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
        .await
        .expect("connect to the test database");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("create the test schema");
    let kek = DeploymentKek::parse(
        KekRef("AXOND_TEST_KEK".to_owned()),
        &STANDARD.encode([7u8; 32]),
    )
    .expect("a 32-byte key");
    let store = PostgresSecrets::connect(
        &dsn,
        SecretStoreSettings {
            schema: Some(schema.clone()),
            connect_timeout: Duration::from_secs(5),
            ..SecretStoreSettings::default()
        },
        kek,
    )
    .await
    .expect("the store applies its own schema");
    Some((Arc::new(store), schema))
}

pub(super) async fn drop_schema(schema: &str) {
    let Some(dsn) = crate::test_services::postgres_dsn() else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
        .await
        .expect("connect to the test database");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .await
        .expect("drop the test schema");
}

/// Every row of the store's table, rendered as text: what a backup, a dump, or a
/// stolen replica would hold.
pub(super) async fn dump(schema: &str) -> String {
    let dsn = crate::test_services::postgres_dsn().expect("a configured DSN");
    let (client, connection) = tokio_postgres::connect(&dsn, crate::usage::tls_connector())
        .await
        .expect("connect to the test database");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let rows = client
        .query(
            &format!(r#"SELECT t::text FROM "{schema}"."axond_secret" t"#),
            &[],
        )
        .await
        .expect("the store's rows as text");
    assert!(
        !rows.is_empty(),
        "the store holds no rows, so the sweep would be vacuous"
    );
    rows.into_iter()
        .map(|row| row.get::<_, Option<String>>(0).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Stage `plaintext` as a new secret and put it in service, answering with the
/// version it landed at.
async fn in_service(secrets: &PostgresSecrets, plaintext: &str) -> SecretRef {
    let staged = secrets
        .stage(owner(), material(plaintext))
        .await
        .expect("the store accepts material");
    activate(secrets, staged.reference).await
}

/// Rotate `reference` onto `plaintext` and put the successor in service.
///
/// Two moves rather than one, because that is the contract: rotation stores
/// material, and putting it in service is a separate decision.
async fn rotated_into_service(
    secrets: &PostgresSecrets,
    reference: SecretRef,
    plaintext: &str,
) -> SecretRef {
    let staged = secrets
        .rotate(owner(), &reference, material(plaintext))
        .await
        .expect("the store accepts the next version");
    assert_eq!(
        staged.reference,
        reference.rotated(),
        "a rotation mints the next version of the same secret"
    );
    activate(secrets, staged.reference).await
}

async fn activate(secrets: &PostgresSecrets, reference: SecretRef) -> SecretRef {
    secrets
        .transition(owner(), &reference, SecretLifecycle::Active)
        .await
        .expect("staged material may be activated");
    reference
}

#[tokio::test]
async fn the_credential_lifecycle_rotates_and_rolls_back_without_a_restart() {
    let Some((secrets, schema)) = store().await else {
        return;
    };
    let provider = FakeProvider::serving().await;
    let replica = Replica::backed_by(&provider, Arc::clone(&secrets), Vec::new());
    let sweep = sweep();

    // 1. The first key in service.
    let first = in_service(&secrets, PROVIDER_MATERIAL).await;
    replica
        .publish("first", state_pinning(first, ResourceVersionNumber::FIRST))
        .await;
    let outcome = replica.converge().await;
    assert!(matches!(outcome, Outcome::Published { .. }), "{outcome:?}");
    let response = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        provider.presented().last().expect("a served request"),
        &format!("Bearer {PROVIDER_MATERIAL}"),
        "the shipped projection authenticated the call with the staged material"
    );

    // 2. A rotation: new version, new revision, new pool. No restart, and the
    //    replica that serves it is the one that was already serving.
    let rotated = rotated_into_service(&secrets, first, ROTATED_MATERIAL).await;
    replica
        .publish(
            "rotation",
            state_pinning(rotated, ResourceVersionNumber::FIRST.next()),
        )
        .await;
    let outcome = replica.converge().await;
    assert!(matches!(outcome, Outcome::Published { .. }), "{outcome:?}");
    let response = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        provider.presented().last().expect("a served request"),
        &format!("Bearer {ROTATED_MATERIAL}"),
    );

    // 3. A rollback is a republication of the earlier state, and it restores the
    //    material as well as the manifest: the old version's row is still there,
    //    because a rotation writes a row rather than updating one.
    replica
        .publish(
            "rollback",
            state_pinning(first, ResourceVersionNumber::FIRST.next().next()),
        )
        .await;
    let outcome = replica.converge().await;
    assert!(matches!(outcome, Outcome::Published { .. }), "{outcome:?}");
    let response = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        provider.presented().last().expect("a served request"),
        &format!("Bearer {PROVIDER_MATERIAL}"),
    );

    // Every resolution so far happened while a candidate was compiled — three
    // publications, three resolutions — and none of the four requests resolved
    // anything.
    assert_eq!(replica.compiler.resolutions(), 3);

    // 4. The database holds ciphertext and references, never material.
    sweep.assert_absent("the secret store's rows", &dump(&schema).await);

    drop_schema(&schema).await;
}

/// Withdrawing material takes it out of the *next* snapshot and leaves the
/// serving one alone: a candidate pinning a revoked version is refused whole, and
/// the replica keeps serving the key it already resolved.
///
/// The failure this rules out is the one that would make revocation unusable in
/// production: a revocation that emptied a live pool would turn an
/// administrative hygiene step into an outage.
#[tokio::test]
async fn a_revoked_version_refuses_the_candidate_and_leaves_the_snapshot_serving() {
    let Some((secrets, schema)) = store().await else {
        return;
    };
    let provider = FakeProvider::serving().await;
    let replica = Replica::backed_by(&provider, Arc::clone(&secrets), Vec::new());

    let first = in_service(&secrets, PROVIDER_MATERIAL).await;
    replica
        .publish("first", state_pinning(first, ResourceVersionNumber::FIRST))
        .await;
    replica.converge().await;
    assert_eq!(replica.generation(), 1);

    // A second version, revoked before it ever served: material an administrator
    // decided against, or a key that leaked between staging and activation.
    let rotated = rotated_into_service(&secrets, first, ROTATED_MATERIAL).await;
    secrets
        .transition(owner(), &rotated, SecretLifecycle::Revoked)
        .await
        .expect("active material may be revoked");
    replica
        .publish(
            "rotation",
            state_pinning(rotated, ResourceVersionNumber::FIRST.next()),
        )
        .await;
    let outcome = replica.converge().await;
    assert!(
        matches!(outcome, Outcome::Rejected { reason, .. } if reason == "secret"),
        "{outcome:?}"
    );

    let report = replica.reconciler.report();
    let rejection = report.last_rejection.as_ref().expect("a recorded refusal");
    let sweep = sweep();
    sweep.assert_absent("a refusal's detail", &rejection.detail);
    assert!(
        rejection.detail.contains(&rotated.to_string()),
        "the refusal names the version an operator has to act on: {}",
        rejection.detail
    );

    // Still generation 1, still serving, still with the key generation 1 resolved.
    assert_eq!(replica.generation(), 1);
    let response = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        provider.presented().last().expect("a served request"),
        &format!("Bearer {PROVIDER_MATERIAL}"),
    );

    drop_schema(&schema).await;
}

/// A version that is not in the production store yet refuses the candidate as
/// a whole: the already published snapshot remains the last known good one,
/// and once the missing row is staged the same desired revision recovers
/// without a restart.
#[tokio::test]
async fn a_missing_version_keeps_the_last_known_good_snapshot_serving() {
    let Some((secrets, schema)) = store().await else {
        return;
    };
    let provider = FakeProvider::serving().await;
    let replica = Replica::backed_by(&provider, Arc::clone(&secrets), Vec::new());
    let sweep = sweep();

    let first = in_service(&secrets, PROVIDER_MATERIAL).await;
    replica
        .publish("first", state_pinning(first, ResourceVersionNumber::FIRST))
        .await;
    assert!(matches!(
        replica.converge().await,
        Outcome::Published { .. }
    ));

    let first_response = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(first_response.status(), StatusCode::OK);
    sweep.assert_present(
        "the initial fake provider",
        "provider",
        provider.presented().first().expect("a served request"),
    );

    // Publish a credential body before its rotated row exists. This is a
    // resolution failure, rather than a lifecycle refusal for a row that was
    // already present and then withdrawn.
    let missing = first.rotated();
    replica
        .publish(
            "missing-version",
            state_pinning(missing, ResourceVersionNumber::FIRST.next()),
        )
        .await;
    let outcome = replica.converge().await;
    assert!(
        matches!(outcome, Outcome::Rejected { reason, .. } if reason == "secret"),
        "{outcome:?}"
    );

    let report = replica.reconciler.report();
    let rejection = report.last_rejection.as_ref().expect("a recorded refusal");
    sweep.assert_absent("a missing-version refusal", &format!("{rejection:?}"));
    assert!(
        rejection.detail.contains(&missing.to_string()),
        "the refusal names the missing version: {}",
        rejection.detail
    );
    assert_eq!(replica.generation(), 1);

    let last_known_good = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(last_known_good.status(), StatusCode::OK);
    assert_eq!(
        provider.presented().last().expect("a served request"),
        &format!("Bearer {PROVIDER_MATERIAL}"),
    );

    // Repair the store, then retry convergence against the same desired
    // revision. The replica publishes it in place and begins serving the new
    // material without a process restart.
    let rotated = rotated_into_service(&secrets, first, ROTATED_MATERIAL).await;
    assert_eq!(rotated, missing);
    assert!(matches!(
        replica.converge().await,
        Outcome::Published { .. }
    ));
    assert_eq!(replica.generation(), 2);

    let recovered = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(recovered.status(), StatusCode::OK);
    assert_eq!(
        provider.presented().last().expect("a served request"),
        &format!("Bearer {ROTATED_MATERIAL}"),
    );
    sweep.assert_present(
        "the recovered fake provider",
        "rotated",
        provider.presented().last().expect("a served request"),
    );

    drop_schema(&schema).await;
}

/// One secret has one owner, and the store checks it on every read: a credential
/// published under a different owner cannot resolve another's material, so the
/// candidate is refused rather than served with the wrong tenant's key.
#[tokio::test]
async fn another_owners_material_never_reaches_a_pool() {
    let Some((secrets, schema)) = store().await else {
        return;
    };
    let provider = FakeProvider::serving().await;
    let replica = Replica::backed_by(&provider, Arc::clone(&secrets), Vec::new());

    // Material staged by a *different* tenant, named by this revision's
    // credential — which is what a cross-tenant read would look like from the
    // store's side.
    let foreign = SecretOwner::tenant(fixtures::tenant_id(9));
    let staged = secrets
        .stage(foreign, material(PROVIDER_MATERIAL))
        .await
        .expect("the store accepts material");
    secrets
        .transition(foreign, &staged.reference, SecretLifecycle::Active)
        .await
        .expect("staged material may be activated");

    replica
        .publish(
            "cross-owner",
            state_pinning(staged.reference, ResourceVersionNumber::FIRST),
        )
        .await;
    let outcome = replica.converge().await;
    assert!(
        matches!(outcome, Outcome::Rejected { reason, .. } if reason == "secret"),
        "{outcome:?}"
    );
    // Nothing was published, so nothing can be served: no snapshot ever held
    // another owner's key.
    assert_eq!(replica.generation(), 0);
    sweep().assert_absent(
        "a cross-owner refusal",
        &format!("{:?}", replica.reconciler.report()),
    );

    drop_schema(&schema).await;
}
