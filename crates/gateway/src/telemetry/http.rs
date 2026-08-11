//! The middleware half of the instrumentation.
//!
//! A tower layer, rather than span macros in the handlers: it owns the server
//! span, joins the caller's trace, and emits the coarse HTTP metrics for every
//! route — including the ones that never reach a provider. Handlers stay free
//! of span plumbing and only record the fields they alone know.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

use axum::extract::MatchedPath;
use axum::http::{Request, Response};
use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_http::HeaderExtractor;
use tower::{Layer, Service};
use tracing::field::Empty;
use tracing::{Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::telemetry::metrics;

/// Wraps the router with the server span, inbound context extraction, and HTTP
/// metrics.
#[derive(Clone, Copy, Default)]
pub struct TelemetryLayer;

impl<S> Layer<S> for TelemetryLayer {
    type Service = Telemetry<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Telemetry { inner }
    }
}

#[derive(Clone)]
pub struct Telemetry<S> {
    inner: S,
}

type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for Telemetry<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let method = request.method().as_str().to_owned();
        let route = route_of(&request);
        let span = server_span(&request, &method, &route);

        // The clone is the readiness-preserving handoff: the ready `self.inner`
        // drives this request while the fresh clone takes its place.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(
            async move {
                let started = Instant::now();
                let response = inner.call(request).await?;
                let status = response.status().as_u16();
                let span = Span::current();
                span.record("http.response.status_code", status);
                metrics::record_http(
                    &method,
                    &route,
                    status,
                    started.elapsed().as_secs_f64() * 1_000.0,
                );
                Ok(response)
            }
            .instrument(span),
        )
    }
}

/// The matched axum route, so metric and span dimensions stay low-cardinality.
/// Unmatched requests collapse to one label rather than carrying the raw path,
/// which a caller could otherwise use to mint unbounded metric series.
fn route_of<B>(request: &Request<B>) -> String {
    request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched| matched.as_str().to_owned())
        .unwrap_or_else(|| UNMATCHED_ROUTE.to_owned())
}

const UNMATCHED_ROUTE: &str = "/{unmatched}";

/// The per-request server span. Every gateway-specific field is declared
/// `Empty` here and filled in later from the canonical usage record, so the
/// handlers never create spans of their own.
fn server_span<B>(request: &Request<B>, method: &str, route: &str) -> Span {
    let span = tracing::info_span!(
        target: "axond.http",
        "http.server.request",
        http.request.method = method,
        http.route = route,
        http.response.status_code = Empty,
        trace_id = Empty,
        span_id = Empty,
        axond.request_id = Empty,
        axond.namespace = Empty,
        axond.subject = Empty,
        gen_ai.request.model = Empty,
        axond.target.provider = Empty,
        axond.target.model = Empty,
        axond.credential_source = Empty,
        axond.status = Empty,
        axond.retry_count = Empty,
        gen_ai.usage.input_tokens = Empty,
        gen_ai.usage.output_tokens = Empty,
        axond.cost_microdollars = Empty,
        axond.latency_ms = Empty,
        axond.ttft_ms = Empty,
    );

    // With telemetry disabled there is no propagator and no tracer, so skip the
    // extraction entirely instead of running it into no-ops.
    if super::is_exporting() {
        let parent =
            global::get_text_map_propagator(|p| p.extract(&HeaderExtractor(request.headers())));
        let _ = span.set_parent(parent);
        let context = span.context();
        let span_context = context.span().span_context().clone();
        if span_context.is_valid() {
            span.record("trace_id", span_context.trace_id().to_string());
            span.record("span_id", span_context.span_id().to_string());
        }
    }

    span
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{
        ATTEMPT_OK, LEASE_PARKED, LEASE_RATE_LIMITED, LEASE_SERVED, credential_lease_span,
        finish_credential_lease, finish_upstream_attempt, upstream_attempt_span,
    };
    use axum::Router;
    use axum::body::Body;
    use axum::routing::post;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use std::sync::atomic::Ordering;
    use tower::util::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    const INBOUND_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const INBOUND_SPAN: &str = "00f067aa0ba902b7";

    /// The handler stands in for `chat_completions`: it opens one attempt span
    /// per upstream call, exactly as the route does.
    async fn dispatch() -> &'static str {
        let attempt = upstream_attempt_span(0, "openai", "gpt-4o", "platform");
        let _entered = attempt.enter();
        let parked = credential_lease_span("parked", "platform", 0);
        finish_credential_lease(&parked, LEASE_PARKED);
        let rate_limited = credential_lease_span("a", "platform", 1);
        finish_credential_lease(&rate_limited, LEASE_RATE_LIMITED);
        finish_upstream_attempt(&attempt, ATTEMPT_OK, 12, Some(12));
        drop(_entered);
        let rotated = upstream_attempt_span(1, "openai", "gpt-4o", "platform");
        let _entered = rotated.enter();
        let served = credential_lease_span("b", "platform", 2);
        finish_credential_lease(&served, LEASE_SERVED);
        finish_upstream_attempt(&rotated, ATTEMPT_OK, 12, Some(12));
        "ok"
    }

    #[tokio::test]
    async fn inbound_traceparent_parents_the_server_and_attempt_spans() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("axond-test")))
            .try_init()
            .expect("install the test subscriber");
        super::super::EXPORTING.store(true, Ordering::Relaxed);

        let app = Router::new()
            .route("/v1/chat/completions", post(dispatch))
            .layer(TelemetryLayer);
        let response = app
            .oneshot(
                axum::http::Request::post("/v1/chat/completions")
                    .header(
                        "traceparent",
                        format!("00-{INBOUND_TRACE}-{INBOUND_SPAN}-01"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);

        let spans = exporter.get_finished_spans().expect("exported spans");
        let server = spans
            .iter()
            .find(|span| span.name == "http.server.request")
            .expect("a server span");
        let attempt = spans
            .iter()
            .find(|span| span.name == "axond.upstream.attempt")
            .expect("an attempt span");
        let attempts: Vec<_> = spans
            .iter()
            .filter(|span| span.name == "axond.upstream.attempt")
            .collect();
        let leases: Vec<_> = spans
            .iter()
            .filter(|span| span.name == "axond.credential.lease")
            .collect();

        assert_eq!(server.span_context.trace_id().to_string(), INBOUND_TRACE);
        assert_eq!(server.parent_span_id.to_string(), INBOUND_SPAN);
        assert_eq!(attempt.parent_span_id, server.span_context.span_id());
        assert_eq!(attempt.span_context.trace_id().to_string(), INBOUND_TRACE);
        assert_eq!(attempts.len(), 2);
        assert_eq!(leases.len(), 3);
        for lease in &leases {
            assert!(attempts.iter().any(|parent| {
                lease.parent_span_id == parent.span_context.span_id()
                    && lease.span_context.trace_id() == parent.span_context.trace_id()
            }));
        }

        let attribute = |span: &opentelemetry_sdk::trace::SpanData, key: &str| {
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.to_string())
        };
        assert_eq!(
            attribute(server, "http.route").as_deref(),
            Some("/v1/chat/completions")
        );
        assert_eq!(
            attribute(server, "http.response.status_code").as_deref(),
            Some("200")
        );
        assert_eq!(attribute(attempt, "axond.ttft_ms").as_deref(), Some("12"));
        assert_eq!(
            attribute(attempt, "axond.target.provider").as_deref(),
            Some("openai")
        );
        for (id, status) in [("parked", "parked"), ("a", "rate_limited"), ("b", "served")] {
            let lease = leases
                .iter()
                .find(|span| attribute(span, "axond.credential.id").as_deref() == Some(id))
                .expect("credential lease");
            assert_eq!(attribute(lease, "axond.status").as_deref(), Some(status));
        }
    }
}
