//! The lifecycle around live material: disclosure, rotation, failure, retirement.
//!
//! These are the properties that cannot be stated about a single value, only
//! about a sequence of them over time, which is why they are asserted against
//! the real [`Reconciler`], the real `ArcSwap` publication seam, and real
//! in-flight requests rather than against a mock:
//!
//! * material is disclosed exactly once, to the runtime that must present it,
//!   and never back to an administrator afterwards;
//! * a rotation cannot cut a request that is already talking to a provider;
//! * a resolution that fails leaves the last known good serving;
//! * a retired version's material is destroyed once nothing references it, and
//!   every surface that used to answer with it answers with a refusal instead.

use std::sync::Arc;

use axum::http::StatusCode;
use http_body_util::BodyExt as _;
use tower::util::ServiceExt as _;

use super::harness::{
    FakeProvider, PROVIDER_MATERIAL, ROTATED_MATERIAL, Replica, bootstrap, bootstrap_env,
    chat_request, first, material, owner, state_pinning, state_sharing, sweep,
};
use crate::backends::fakes::InMemorySecrets;
use crate::backends::secrets::{SecretResolver as _, SecretStore as _};
use crate::budget::NoBudget;
use crate::convergence::Outcome;
use crate::desired_state::{ResourceVersionNumber, SecretLifecycle};
use crate::routes::router;
use crate::state::AppState;
use crate::usage::{UsageFanout, UsageSink};

/// Staging is the one moment an administrator holds plaintext. Afterwards the
/// store answers questions *about* the version — who owns it, what state it is
/// in, whether it exists — and the material itself is reachable only by the
/// resolution the runtime performs to build a snapshot.
///
/// The distinction is the whole point of an opaque reference: were `describe`
/// to answer with material, every surface that renders a descriptor would
/// inherit the leak, and no amount of redaction downstream would help.
#[tokio::test]
async fn material_is_disclosed_to_the_runtime_and_never_read_back_by_an_administrator() {
    let secrets = InMemorySecrets::new();
    let staged = secrets
        .stage(owner(), material(PROVIDER_MATERIAL))
        .await
        .expect("the store accepts material");
    let sweep = sweep();

    // Everything an administrator can ask afterwards.
    let descriptor = secrets
        .describe(owner(), &staged.reference)
        .await
        .expect("the version is described");
    sweep.assert_absent("a secret descriptor", &format!("{descriptor:?}"));
    sweep.assert_absent(
        "a staged descriptor's reference",
        &staged.reference.to_string(),
    );
    assert!(
        secrets
            .exists(owner(), &staged.reference)
            .await
            .expect("existence is answerable")
    );
    let transition = secrets
        .transition(owner(), &staged.reference, SecretLifecycle::Active)
        .await
        .expect("staged material may be activated");
    sweep.assert_absent("a lifecycle transition", &format!("{transition:?}"));

    // And the one caller that is entitled to it: the runtime resolving a
    // snapshot. The tripwire proves the sweep above was searching for material
    // that really is in the store.
    let resolved = secrets
        .resolve(owner(), &staged.reference)
        .await
        .expect("the runtime resolves it");
    sweep.assert_present("the store's resolution", "provider", resolved.expose());
    sweep.assert_absent("resolved material's Debug", &format!("{resolved:?}"));
}

/// A rotation publishes a new snapshot; a request that is already talking to a
/// provider keeps the snapshot it started with. Both are consequences of the
/// same design — a snapshot is whole and immutable, and publication is a
/// pointer swap — and this is the test that they hold together with real
/// material in flight.
///
/// The failure this rules out is not hypothetical: a store that swapped
/// credential material *inside* a live snapshot would break every request whose
/// provider call was already authenticated with the previous key, and would do
/// so only under load, only during a rotation.
#[tokio::test]
async fn a_rotation_publishes_new_material_without_cutting_an_in_flight_request() {
    let provider = FakeProvider::gated().await;
    let replica = Replica::new(&provider);
    replica
        .secrets
        .seed(owner(), first(), PROVIDER_MATERIAL, SecretLifecycle::Active);
    replica
        .publish(
            "first",
            state_pinning(first(), ResourceVersionNumber::FIRST),
        )
        .await;
    let published = replica.converge().await;
    assert!(
        matches!(published, Outcome::Published { .. }),
        "{published:?}"
    );
    assert_eq!(replica.generation(), 1);

    // A request that is inside the provider call, holding generation 1.
    let in_flight = tokio::spawn(router(replica.state.clone()).oneshot(chat_request()));
    provider.await_arrival().await;

    // The administrator rotates: new material, new secret version, a republished
    // credential body pinning it.
    let rotated = first().rotated();
    replica
        .secrets
        .seed(owner(), rotated, ROTATED_MATERIAL, SecretLifecycle::Active);
    let desired = replica
        .publish(
            "rotation",
            state_pinning(rotated, ResourceVersionNumber::FIRST.next()),
        )
        .await;
    let outcome = replica.converge().await;
    assert!(
        matches!(outcome, Outcome::Published { revision, generation, .. }
            if revision == desired && generation == 2),
        "{outcome:?}"
    );

    // The in-flight request finishes — with the key it started with.
    provider.release(2);
    let response = in_flight
        .await
        .expect("the task joins")
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);

    // And the next request uses the new key.
    let next = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(next.status(), StatusCode::OK);

    let presented = provider.presented();
    assert_eq!(presented.len(), 2, "{presented:?}");
    assert_eq!(presented[0], format!("Bearer {PROVIDER_MATERIAL}"));
    assert_eq!(presented[1], format!("Bearer {ROTATED_MATERIAL}"));
    // Resolution happened twice — once per compilation — and not once per
    // request: material is taken out of the store off the request path.
    assert_eq!(replica.compiler.resolutions(), 2);
}

/// Exposure is counted in versions unwrapped, not in bodies pointing at them.
///
/// Two credentials sharing one secret version are one read of the store, and
/// the materialization is what makes that true. A count that grew per
/// credential instead would overstate exposure the moment a revision had more
/// than one credential, and the assertions built on it would stop meaning what
/// they say.
#[tokio::test]
async fn two_credentials_sharing_a_version_take_material_out_of_the_store_once() {
    let provider = FakeProvider::serving().await;
    let replica = Replica::new(&provider);
    replica
        .secrets
        .seed(owner(), first(), PROVIDER_MATERIAL, SecretLifecycle::Active);
    replica
        .publish(
            "shared",
            state_sharing(first(), ResourceVersionNumber::FIRST),
        )
        .await;
    let published = replica.converge().await;
    assert!(
        matches!(published, Outcome::Published { .. }),
        "{published:?}"
    );

    assert_eq!(replica.compiler.resolutions(), 1);
    assert_eq!(replica.compiler.ledger().retained(), vec![first()]);
}

/// A candidate whose material cannot be resolved is refused *whole*: the
/// replica keeps serving the revision it had, with the key it had, and the
/// refusal names the reference and the reason without naming the material.
///
/// This is the property that makes a secret-store outage survivable. A build
/// that resolved credentials into a partially-mutated snapshot would take the
/// deployment down with the store.
#[tokio::test]
async fn a_failed_resolution_keeps_the_last_known_good_snapshot_serving() {
    let provider = FakeProvider::serving().await;
    let replica = Replica::new(&provider);
    replica
        .secrets
        .seed(owner(), first(), PROVIDER_MATERIAL, SecretLifecycle::Active);
    replica
        .publish(
            "first",
            state_pinning(first(), ResourceVersionNumber::FIRST),
        )
        .await;
    replica.converge().await;
    assert_eq!(replica.generation(), 1);

    // A rotation whose new version the store cannot produce — the administrator
    // published the body before the material landed, or the store is down.
    let rotated = first().rotated();
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
    assert_eq!(rejection.reason, "secret");
    let sweep = sweep();
    sweep.assert_absent("a refusal's detail", &rejection.detail);
    sweep.assert_absent("a refusal's Debug", &format!("{rejection:?}"));
    sweep.assert_absent("the convergence report", &format!("{report:?}"));
    // The reference is named, which is what an operator acts on.
    assert!(
        rejection.detail.contains(&rotated.secret.to_string()),
        "{}",
        rejection.detail
    );

    // Still serving generation 1, still with the material generation 1 resolved.
    assert_eq!(replica.generation(), 1);
    let response = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        provider.presented().last().expect("a served request"),
        &format!("Bearer {PROVIDER_MATERIAL}")
    );

    // And the store outage recovers into a publication rather than needing a
    // restart: the same candidate compiles once the material is there.
    replica
        .secrets
        .seed(owner(), rotated, ROTATED_MATERIAL, SecretLifecycle::Active);
    let outcome = replica.converge().await;
    assert!(matches!(outcome, Outcome::Published { .. }), "{outcome:?}");
    assert_eq!(replica.generation(), 2);
}

/// Retiring a version destroys its material once nothing references it, and
/// every surface that could have answered with it answers with a refusal.
///
/// "Once nothing references it" is asserted structurally rather than by
/// sleeping: the superseded snapshot is dropped, and the assertion that no
/// strong reference to it survives is what makes the subsequent tombstone the
/// *retirement* of unreferenced material rather than the destruction of
/// material a live request still needs.
#[tokio::test]
async fn retired_material_is_destroyed_once_no_snapshot_references_it() {
    let provider = FakeProvider::serving().await;
    let replica = Replica::new(&provider);
    let rotated = first().rotated();
    for (reference, plaintext) in [(first(), PROVIDER_MATERIAL), (rotated, ROTATED_MATERIAL)] {
        replica
            .secrets
            .seed(owner(), reference, plaintext, SecretLifecycle::Active);
    }
    replica
        .publish(
            "first",
            state_pinning(first(), ResourceVersionNumber::FIRST),
        )
        .await;
    replica.converge().await;

    // Hold the generation-1 snapshot the way an in-flight request holds one.
    let superseded = replica.state.config();
    replica
        .publish(
            "rotation",
            state_pinning(rotated, ResourceVersionNumber::FIRST.next()),
        )
        .await;
    replica.converge().await;
    assert_eq!(replica.generation(), 2);

    // While a reference survives, the old material is still resolvable: retiring
    // it here is what would cut the request holding it. Both versions' unwrapped
    // material is live at once, which is what a rotation *is* from the ledger's
    // side.
    assert!(replica.secrets.holds_material(&first()));
    let ledger = replica.compiler.ledger();
    assert!(ledger.holds(first()), "{:?}", ledger.retained());
    assert!(ledger.holds(rotated), "{:?}", ledger.retained());
    let weak = Arc::downgrade(&superseded);
    drop(superseded);
    assert!(
        weak.upgrade().is_none(),
        "the superseded snapshot is still referenced, so retirement would be premature"
    );
    // Dropping the last snapshot holding the superseded version zeroizes its
    // material without anybody scheduling the release.
    assert!(
        !ledger.holds(first()),
        "unwrapped material outlived the last snapshot referencing it: {:?}",
        ledger.retained()
    );
    assert_eq!(ledger.retained(), vec![rotated]);

    // Nothing references it: the operator retires the version.
    replica
        .secrets
        .transition(owner(), &first(), SecretLifecycle::Revoked)
        .await
        .expect("an active version may be revoked");
    let transition = replica
        .secrets
        .transition(owner(), &first(), SecretLifecycle::Tombstoned)
        .await
        .expect("a revoked version may be tombstoned");
    assert_eq!(transition.state(), SecretLifecycle::Tombstoned);

    assert!(
        !replica.secrets.holds_material(&first()),
        "tombstoning must destroy the material, not relabel it"
    );
    let sweep = sweep();
    let error = replica
        .secrets
        .resolve(owner(), &first())
        .await
        .expect_err("retired material cannot be resolved");
    sweep.assert_absent("a retired version's resolution error", &error.to_string());
    sweep.assert_absent("a retired version's error Debug", &format!("{error:?}"));
    assert!(
        !replica
            .secrets
            .exists(owner(), &first())
            .await
            .expect("existence is answerable")
    );

    // The replica is unaffected: it is serving the version that was not retired.
    let response = router(replica.state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        provider.presented().last().expect("a served request"),
        &format!("Bearer {ROTATED_MATERIAL}")
    );
}

/// The stateless credential path is unchanged by any of this: a config that
/// resolves its credentials from the environment still boots, still serves, and
/// still keeps its material out of everything it renders.
///
/// Asserted here rather than left implicit because the whole point of the
/// stateful slice is that it is *additive*: the default deployment reads keys
/// from env vars and must keep doing so.
#[tokio::test]
async fn the_stateless_credential_path_still_serves_and_still_redacts() {
    let provider = FakeProvider::serving().await;
    let mut config = bootstrap(&provider.base_url);
    config.credential.push(crate::config::Credential {
        namespace: "platform".to_owned(),
        provider: "openai".to_owned(),
        env: Some("AXOND_STATELESS_OPENAI".to_owned()),
        secret: None,
        id: Some("stateless".to_owned()),
        weight: 1,
    });
    config.model.push(crate::config::Model {
        name: "fast".to_owned(),
        targets: vec![crate::config::Target {
            provider: "openai".to_owned(),
            model: "gpt-4o".to_owned(),
            price: gateway_core::catalog::ModelPrice {
                input_microdollars_per_million: 1_000_000,
                output_microdollars_per_million: 2_000_000,
                reasoning_microdollars_per_million: None,
                cache_read_microdollars_per_million: None,
                cache_write_microdollars_per_million: None,
            },
        }],
    });
    let mut env = bootstrap_env();
    env.insert(
        "AXOND_STATELESS_OPENAI".to_owned(),
        PROVIDER_MATERIAL.to_owned(),
    );
    let sinks: Vec<Box<dyn UsageSink>> = Vec::new();
    let state = AppState::new(config, &env, UsageFanout::new(sinks), Box::new(NoBudget))
        .expect("the config is servable");

    let response = router(state.clone())
        .oneshot(chat_request())
        .await
        .expect("a response");
    assert_eq!(response.status(), StatusCode::OK);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();

    let sweep = sweep();
    sweep.assert_present(
        "the fake provider",
        "provider",
        provider.presented().last().expect("a served request"),
    );
    sweep.assert_absent_bytes("a stateless response body", &body);
    let snapshot = state.config();
    sweep.assert_absent(
        "a stateless snapshot's config",
        &format!("{:?}", snapshot.config),
    );
    sweep.assert_absent(
        "a stateless snapshot's credential fingerprints",
        &format!("{:?}", snapshot.gateway_key_fingerprints),
    );
}

/// A compiler is not a request-time resolver: the same revision compiled twice
/// takes material out of the store twice, and a served request takes none.
///
/// Stated separately because it is the property that bounds *exposure*: how
/// often material crosses the store boundary is how often it can be observed
/// crossing it, and "once per publication" is a small, auditable number.
#[tokio::test]
async fn material_crosses_the_store_boundary_once_per_compilation() {
    let provider = FakeProvider::serving().await;
    let replica = Replica::new(&provider);
    replica
        .secrets
        .seed(owner(), first(), PROVIDER_MATERIAL, SecretLifecycle::Active);
    replica
        .publish(
            "first",
            state_pinning(first(), ResourceVersionNumber::FIRST),
        )
        .await;
    replica.converge().await;
    assert_eq!(replica.compiler.resolutions(), 1);

    for _ in 0..3 {
        let response = router(replica.state.clone())
            .oneshot(chat_request())
            .await
            .expect("a response");
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(
        replica.compiler.resolutions(),
        1,
        "requests must serve from the snapshot, not from the store"
    );

    // A converged replica does not recompile, so it does not re-resolve either.
    replica.converge().await;
    assert_eq!(replica.compiler.resolutions(), 1);
}
