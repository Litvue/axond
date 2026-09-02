//! Everything one served request emits, swept for the material that served it.
//!
//! A request authenticated with the inbound sentinel, dispatched to a provider
//! with the provider sentinel, is the moment both keys are simultaneously in
//! memory, in a header, in a span, and in a billing record. Whatever escapes,
//! escapes here — so this module runs exactly that request with a capturing log
//! subscriber, an in-memory span exporter, and a recording usage sink attached
//! at once, and sweeps all four outputs plus the response.
//!
//! The tripwire matters more here than anywhere else: the assertion that the
//! fake provider was presented with the sentinel is what distinguishes "nothing
//! leaked" from "nothing happened".

use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use http_body_util::BodyExt as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use tower::util::ServiceExt as _;
use tracing_subscriber::layer::SubscriberExt as _;

use super::harness::{
    CapturingSink, FakeProvider, PROVIDER_MATERIAL, Replica, chat_request, first, live_material,
    owner, state_pinning, sweep,
};
use crate::desired_state::{ResourceVersionNumber, SecretLifecycle};
use crate::routes::router;
use crate::shutdown::Phase;
use crate::status::registry::CachedStatusRegistry;
use crate::status::{Component, ComponentObservation, StatusReason, StatusScope};

/// Everything written to the log layer, as bytes.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    fn rendered(&self) -> Vec<u8> {
        self.0.lock().expect("not poisoned").clone()
    }
}

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("not poisoned").extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

/// One request, served end to end, with every operational surface recording.
///
/// The subscriber is unfiltered on purpose: a redaction test that only swept
/// `INFO` and above would miss the `DEBUG` line that renders a whole request
/// context, which is exactly the line a leak arrives on.
#[tokio::test]
#[ignore = "ADR 0063: projected credentials / alias snapshots withdrawn"]
async fn a_served_request_leaks_its_credentials_into_nothing_it_emits() {
    let provider = FakeProvider::serving().await;
    let usage = CapturingSink::default();
    let replica = Replica::with_sinks(&provider, vec![Box::new(usage.clone())]);
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

    let logs = CapturedLogs::default();
    let exporter = InMemorySpanExporter::default();
    let traces = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let subscriber = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(logs.clone()),
        )
        .with(tracing_opentelemetry::layer().with_tracer(traces.tracer("axond-secret-redaction")));
    crate::telemetry::testing::keep_callsites_answerable();
    let dispatch = tracing::Dispatch::new(subscriber);

    let state = replica.state.clone();
    let response = tokio::spawn(async move {
        let _default = tracing::dispatcher::set_default(&dispatch);
        router(state)
            .oneshot(chat_request())
            .await
            .expect("a response")
    })
    .await
    .expect("the task joins");

    assert_eq!(response.status(), StatusCode::OK);
    let headers = format!("{:?}", response.headers());
    let body = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();

    let sweep = sweep();
    // The tripwire: the provider really was authenticated with the sentinel, so
    // every assertion below is about material that was genuinely in play.
    sweep.assert_present(
        "the fake provider",
        "provider",
        provider.presented().last().expect("a served request"),
    );

    sweep.assert_absent("a response's headers", &headers);
    sweep.assert_absent_bytes("a response's body", &body);
    sweep.assert_absent_bytes("the request's log output", &logs.rendered());

    traces.force_flush().expect("spans flush");
    let spans = exporter.get_finished_spans().expect("exported spans");
    assert!(!spans.is_empty(), "the request produced no spans to sweep");
    sweep.assert_absent("the request's spans", &format!("{spans:?}"));

    let records = usage.records();
    assert_eq!(records.len(), 1, "{records:?}");
    let record = &records[0];
    sweep.assert_absent("a usage record's Debug", &format!("{record:?}"));
    sweep.assert_absent(
        "a serialized usage record",
        &serde_json::to_string(record).expect("a serializable record"),
    );
    // Attribution is by label, which is what makes per-key spend reportable
    // without the key: the credential's slug, never its material.
    assert_eq!(record.credential_id, "primary");
}

/// A request the provider refuses produces an error response, an error log, and
/// an error record. Failure is where redaction usually breaks: the handler that
/// carefully logs a reference on the happy path is the one that formats the
/// whole request context into the error.
#[tokio::test]
async fn a_failed_request_leaks_its_credentials_into_no_error_surface() {
    let provider = FakeProvider::unreachable();
    let usage = CapturingSink::default();
    let replica = Replica::with_sinks(&provider, vec![Box::new(usage.clone())]);
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
    // The tripwire an unreachable upstream cannot provide by being presented
    // with the key: the material was resolved into the snapshot, so the request
    // below really does carry it as far as the transport gets.
    assert_eq!(
        replica.compiler.resolutions(),
        1,
        "the credential never left the store, so nothing below could leak it"
    );

    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(logs.clone()),
    );
    crate::telemetry::testing::keep_callsites_answerable();
    let dispatch = tracing::Dispatch::new(subscriber);
    let state = replica.state.clone();
    let response = tokio::spawn(async move {
        let _default = tracing::dispatcher::set_default(&dispatch);
        router(state)
            .oneshot(chat_request())
            .await
            .expect("a response")
    })
    .await
    .expect("the task joins");

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();
    let sweep = sweep();
    sweep.assert_absent_bytes("an error response's body", &body);
    sweep.assert_absent_bytes("an error's log output", &logs.rendered());
    for record in usage.records() {
        sweep.assert_absent("a failed request's usage record", &format!("{record:?}"));
    }
}

/// An unauthenticated caller learns nothing about the key it failed to present,
/// and the replica's own inbound key appears in neither the refusal nor its log.
#[tokio::test]
async fn a_rejected_caller_learns_nothing_about_the_key_it_failed_to_present() {
    let provider = FakeProvider::serving().await;
    let replica = Replica::new(&provider);
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_writer(logs.clone()),
    );
    crate::telemetry::testing::keep_callsites_answerable();
    let dispatch = tracing::Dispatch::new(subscriber);
    let state = replica.state.clone();
    // A near-miss: the same key with one character changed, which a naive
    // "expected X, got Y" diagnostic would render in full.
    let mut wrong = super::harness::INBOUND_MATERIAL.to_owned();
    wrong.pop();
    wrong.push('9');
    let request = axum::http::Request::post(format!(
        "/ns/{}/v1/chat/completions",
        super::harness::SERVING_PATH_NS
    ))
    .header("content-type", "application/json")
    .header(axum::http::header::AUTHORIZATION, format!("Bearer {wrong}"))
    .body(axum::body::Body::from(r#"{"model":"fast","messages":[]}"#))
    .expect("a valid request");
    let response = tokio::spawn(async move {
        let _default = tracing::dispatcher::set_default(&dispatch);
        router(state).oneshot(request).await.expect("a response")
    })
    .await
    .expect("the task joins");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = response
        .into_body()
        .collect()
        .await
        .expect("a body")
        .to_bytes();
    let sweep = sweep();
    sweep.assert_absent_bytes("a rejection's body", &body);
    sweep.assert_absent_bytes("a rejection's log output", &logs.rendered());
}

/// The status surface reports that a secret could not be resolved, in the
/// bounded vocabulary and with no material and no reference in it.
///
/// The refusal's *detail* legitimately names the secret reference — an operator
/// has to know which one — and the projection is what keeps that detail out of
/// the response: what a caller receives is a reason code.
#[tokio::test]
async fn the_status_response_reports_a_secret_failure_without_disclosing_anything() {
    let provider = FakeProvider::serving().await;
    let replica = Replica::new(&provider);
    replica
        .publish(
            "first",
            state_pinning(first(), ResourceVersionNumber::FIRST),
        )
        .await;
    // The candidate cannot resolve: nothing was staged in the replica's store.
    // The material is live in the process regardless, so the sweep below has
    // something to find if the projection ever starts rendering it.
    let live = live_material(&[(first(), PROVIDER_MATERIAL)]).await;
    sweep().assert_present("the material resolved out of a store", "provider", &live[0]);
    replica.converge().await;
    let report = replica.reconciler.report();
    assert_eq!(
        report
            .last_rejection
            .as_ref()
            .expect("a recorded refusal")
            .reason,
        "secret"
    );

    let registry = CachedStatusRegistry::stateless();
    registry.publish(ComponentObservation::unavailable(
        Component::SecretStore,
        StatusReason::AuthenticationRejected,
        // What a probe learns from a store that refused the replica's own
        // identity: never a response field, and asserted as such below.
        "the secret store rejected this replica's credentials".to_owned(),
    ));
    let view = registry.view();

    let sweep = sweep();
    for scope in [StatusScope::Deployment, StatusScope::Namespace] {
        let response = view.project(scope, Phase::Serving, Some(&report));
        let json = serde_json::to_string(&response).expect("a serializable response");
        sweep.assert_absent("a status response", &json);
        assert!(
            !json.contains(&first().secret.to_string()),
            "a status response must not carry a secret identifier: {json}"
        );
    }
    let deployment = view.project(StatusScope::Deployment, Phase::Serving, Some(&report));
    let revision = deployment.revision.expect("an operator sees convergence");
    assert!(!revision.converged);
    assert_eq!(revision.reason, Some(StatusReason::SecretUnresolved.code()));
    drop(live);
}
