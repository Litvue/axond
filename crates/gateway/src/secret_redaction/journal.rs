//! Durable state, swept row by row.
//!
//! The other modules assert about values in memory; this one asserts about what
//! survives the process. A credential's *reference* is meant to be durable and
//! its *material* is not, and the only convincing way to state that is to
//! publish real revisions through the real journal and then read every row of
//! every table back as text.
//!
//! Reading the whole schema rather than the columns a test knows about is the
//! point: a leak that mattered would arrive in a column no test thought to
//! check — an audit event's payload, a hydration cache, a debugging column added
//! later. `SELECT t::text FROM <table> t` renders whatever is there.
//!
//! The material has to be *in the process* for that to mean anything. A test
//! that published bodies without ever holding a key would sweep a journal that
//! never had one to store, and would stay green if a body started carrying
//! plaintext tomorrow. So the sentinels are staged into a store and resolved out
//! of it here, held live across every assertion, and the sweep is checked
//! against the resolved values before it is trusted against the schema.
//!
//! These tests require Postgres in CI (`AXOND_TEST_REQUIRE_SERVICES=1` makes a
//! missing DSN a panic rather than a skip), so a green run means they ran.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio_postgres::Config;

use super::harness::{
    PROVIDER_MATERIAL, ROTATED_MATERIAL, first, material, owner, state_pinning, sweep,
};
use crate::backends::control_plane::postgres::{ControlPlaneSettings, PostgresControlPlane};
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::backends::fakes::InMemorySecrets;
use crate::backends::secrets::SecretResolver as _;
use crate::desired_state::{
    DesiredState, ExpectedRevision, ResourceVersionNumber, RevisionId, SecretLifecycle, SecretRef,
    fixtures,
};

/// A journal on a schema of its own, or `None` when no Postgres is configured
/// and the suite is not running in required mode.
async fn journal() -> Option<(PostgresControlPlane, String)> {
    let dsn = crate::test_services::postgres_dsn()?;
    let schema = format!(
        "secret_redaction_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("a monotonic wall clock")
            .as_nanos()
    );
    let mut config: Config = dsn.parse().expect("a parseable test DSN");
    config.connect_timeout(Duration::from_secs(5));
    let (client, connection) = config
        .connect(crate::usage::tls_connector())
        .await
        .expect("a connection to create the test schema");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("CREATE SCHEMA {schema}"))
        .await
        .expect("a fresh test schema");
    let settings = ControlPlaneSettings {
        schema: Some(schema.clone()),
        operation_timeout: Duration::from_secs(10),
        connect_timeout: Duration::from_secs(5),
        ..ControlPlaneSettings::default()
    };
    let store = PostgresControlPlane::connect(&dsn, settings)
        .await
        .expect("a migrated journal");
    Some((store, schema))
}

/// Every row of every table in the journal's schema, rendered as text.
async fn dump(schema: &str) -> String {
    let dsn = crate::test_services::postgres_dsn().expect("a configured DSN");
    let mut config: Config = dsn.parse().expect("a parseable test DSN");
    config.connect_timeout(Duration::from_secs(5));
    let (client, connection) = config
        .connect(crate::usage::tls_connector())
        .await
        .expect("a connection to read the schema back");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let tables = client
        .query(
            "SELECT tablename FROM pg_tables WHERE schemaname = $1 ORDER BY tablename",
            &[&schema.to_owned()],
        )
        .await
        .expect("the schema's tables");
    assert!(
        !tables.is_empty(),
        "the journal's schema has no tables, so the sweep would be vacuous"
    );

    let mut dumped = String::new();
    for table in tables {
        let name: String = table.get(0);
        dumped.push_str(&format!("-- {schema}.{name}\n"));
        let rows = client
            .query(
                &format!(r#"SELECT t::text FROM "{schema}"."{name}" t"#),
                &[],
            )
            .await
            .expect("a table's rows as text");
        for row in rows {
            let rendered: Option<String> = row.get(0);
            dumped.push_str(rendered.as_deref().unwrap_or("(null)"));
            dumped.push('\n');
        }
    }
    dumped
}

/// Stage each `(reference, plaintext)` pair into a store and resolve it back,
/// returning the plaintext the store handed over.
///
/// The store is dropped; the material is not. What the caller holds is the same
/// thing the runtime holds between a compilation and a publication — a resolved
/// key, alive in the process that is talking to the journal — which is the only
/// state in which "the journal never saw it" is a claim with content.
async fn live_material(pairs: &[(SecretRef, &'static str)]) -> Vec<String> {
    let secrets = InMemorySecrets::new();
    let mut resolved = Vec::with_capacity(pairs.len());
    for (reference, plaintext) in pairs {
        secrets.seed(
            owner(),
            *reference,
            SecretLifecycle::Active,
            material(plaintext),
        );
        resolved.push(
            secrets
                .resolve(owner(), reference)
                .await
                .expect("active material resolves")
                .expose()
                .to_owned(),
        );
    }
    resolved
}

async fn publish(
    store: &PostgresControlPlane,
    key: &str,
    expected: ExpectedRevision,
    state: DesiredState,
) -> Result<RevisionId, ControlPlaneError> {
    store
        .publish_revision(fixtures::candidate(expected, key, state))
        .await
        .map(|manifest| manifest.id)
}

/// Publish, rotate, replay, and then read the entire schema back: no sentinel is
/// anywhere in it, and every surface the journal answers with — manifests,
/// hydrated revisions, audit events, their `Debug` — is clean too.
///
/// One test rather than five because the expensive part is the schema, and
/// because the assertion is about the *whole* of durable state: splitting it
/// would let each half sweep only what it wrote.
#[tokio::test]
async fn no_durable_row_or_read_carries_secret_material() {
    let Some((store, schema)) = journal().await else {
        return;
    };
    let sweep = sweep();
    let rotated = first().rotated();

    // The material exists, in this process, for as long as the journal is being
    // swept: `resolved` is held to the end of the test so nothing here can pass
    // because the key was never around to leak.
    let resolved =
        live_material(&[(first(), PROVIDER_MATERIAL), (rotated, ROTATED_MATERIAL)]).await;
    for (label, plaintext) in [("provider", &resolved[0]), ("rotated", &resolved[1])] {
        sweep.assert_present("the material resolved out of the store", label, plaintext);
    }

    let first_revision = publish(
        &store,
        "publish-first",
        ExpectedRevision::Empty,
        state_pinning(first(), ResourceVersionNumber::FIRST),
    )
    .await
    .expect("the first revision publishes");
    let rotation = publish(
        &store,
        "publish-rotation",
        ExpectedRevision::Exactly(first_revision),
        state_pinning(rotated, ResourceVersionNumber::FIRST.next()),
    )
    .await
    .expect("the rotation publishes");

    // An idempotent replay: the same key with the same state returns the same
    // revision, and the replay path is its own serialization surface.
    let replayed = publish(
        &store,
        "publish-rotation",
        ExpectedRevision::Exactly(first_revision),
        state_pinning(rotated, ResourceVersionNumber::FIRST.next()),
    )
    .await
    .expect("a replay returns the original outcome");
    assert_eq!(replayed, rotation);

    // A conflicting replay: the refusal is an error value built from stored
    // state, which is exactly the kind of value that quotes too much.
    let conflict = publish(
        &store,
        "publish-rotation",
        ExpectedRevision::Exactly(first_revision),
        state_pinning(first(), ResourceVersionNumber::FIRST.next()),
    )
    .await
    .expect_err("a reused key carrying different state is refused");
    assert!(
        matches!(conflict, ControlPlaneError::IdempotencyKeyReused { .. }),
        "{conflict:?}"
    );
    sweep.assert_absent("a refused replay", &conflict.to_string());
    sweep.assert_absent("a refused replay's Debug", &format!("{conflict:?}"));

    // Every read the store offers.
    for id in [first_revision, rotation] {
        let manifest = store.load_manifest(id).await.expect("a retained manifest");
        sweep.assert_absent("a revision manifest", &format!("{manifest:?}"));
        let loaded = store.load_revision(id).await.expect("a hydrated revision");
        sweep.assert_absent("a hydrated revision", &format!("{loaded:?}"));
        sweep.assert_absent(
            "a hydrated revision's state",
            &format!("{:?}", loaded.state()),
        );
        for event in store.audit_trail(id).await.expect("an audit trail") {
            sweep.assert_absent("an audit event", &format!("{event:?}"));
        }
    }
    let desired = store
        .load_desired_revision()
        .await
        .expect("the head hydrates")
        .expect("a published head");
    sweep.assert_absent("the desired revision", &format!("{desired:?}"));

    // And the storage itself, column by column.
    sweep.assert_absent("the journal's durable rows", &dump(&schema).await);

    // The tripwire: the material the sweep looked for is material that would
    // have had to be *somewhere* had the credential carried it, and the
    // reference it carried instead is durable and readable.
    // Resource bodies are stored as canonical bytes, which `::text` renders as
    // hex — which is exactly why the sweep searches encodings rather than
    // plaintext, and why the tripwire has to look for the same encoding.
    let rows = dump(&schema).await;
    let identifier = rotated.secret.to_string();
    let hexed: String = identifier
        .bytes()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    assert!(
        rows.contains(&identifier) || rows.contains(&hexed),
        "the credential's secret reference must be durable, or the sweep proves nothing"
    );
    drop(resolved);
}
