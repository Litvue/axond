//! Telemetry: traces, metrics, and log correlation.
//!
//! Three properties shape this module (ADR 0007):
//!
//! * **Off by default.** With no `OTEL_EXPORTER_OTLP_ENDPOINT` the process
//!   installs no tracer or meter provider, so the OpenTelemetry globals stay
//!   no-ops and the request path pays nothing beyond JSON logging.
//! * **Instrumentation is layered, not scattered.** The server span, inbound
//!   context extraction, and HTTP-level metrics live in [`http::TelemetryLayer`];
//!   outbound `traceparent` injection lives in the transport crate. Handlers
//!   only fill in the fields they alone know (alias, target, tokens, cost).
//! * **Nothing sensitive.** Spans and metrics carry identifiers and counts —
//!   never credentials, prompts, or completions.

mod exporter;
pub mod http;
pub mod metrics;
mod spans;

pub use http::TelemetryLayer;
pub use spans::{
    ATTEMPT_ERROR, ATTEMPT_OK, LEASE_ERROR, LEASE_PARKED, LEASE_RATE_LIMITED, LEASE_SERVED,
    RELOAD_APPLIED, RELOAD_REJECTED, config_reload_span, credential_lease_span,
    finish_config_reload, finish_credential_lease, finish_upstream_attempt, record_attempt_timeout,
    record_request, record_routing, record_streamed, trace_id, upstream_attempt_span,
};

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use opentelemetry::global;
use opentelemetry::logs::LoggerProvider as _;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// `service.name` for every exported span and metric, matching the sibling
/// `actord`/`custodian` services.
pub const SERVICE_NAME: &str = "axond";

/// The bound used when a guard is dropped without an explicit
/// [`TelemetryGuard::shutdown`] — the CLI subcommands and the tests.
const FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// The OTLP/HTTP signals axond exports.
const SIGNALS: [&str; 3] = ["traces", "metrics", "logs"];

static EXPORTING: AtomicBool = AtomicBool::new(false);

/// The logger the OTLP usage sink emits through. Filled at init when export is
/// on; the sink refuses to be configured when it is empty, so a usage record
/// never disappears into a no-op provider.
static USAGE_LOGGER: OnceLock<SdkLogger> = OnceLock::new();

/// Instrumentation scope for exported usage records — distinct from the
/// gateway's own diagnostic logs, which stay on stdout.
pub const USAGE_SCOPE: &str = "axond.usage";

/// The logger for the OTLP usage sink, or `None` when OTLP export is off.
pub fn usage_logger() -> Option<SdkLogger> {
    USAGE_LOGGER.get().cloned()
}

/// Whether OTLP export was installed at boot. Instrumentation consults this to
/// skip work that would otherwise run into no-op providers.
pub fn is_exporting() -> bool {
    EXPORTING.load(Ordering::Relaxed)
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TelemetryError(String);

/// Resolved exporter configuration. `endpoint = None` is the default posture:
/// logs to stdout, no OTLP export, no exporter on the request path.
#[derive(Debug, Clone, Default)]
pub struct TelemetryConfig {
    pub endpoint: Option<String>,
}

impl TelemetryConfig {
    /// Read the standard OTLP environment. Only OTLP/HTTP is supported, so an
    /// explicit `grpc` protocol is rejected at boot rather than silently
    /// exporting nowhere — config errors fail at boot, not at request time.
    pub fn from_env() -> Result<Self, TelemetryError> {
        Self::from_values(
            std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok().as_deref(),
            std::env::var("OTEL_EXPORTER_OTLP_PROTOCOL").ok().as_deref(),
        )
    }

    fn from_values(endpoint: Option<&str>, protocol: Option<&str>) -> Result<Self, TelemetryError> {
        let Some(endpoint) = non_empty(endpoint) else {
            return Ok(Self::default());
        };
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err(TelemetryError(
                "OTEL_EXPORTER_OTLP_ENDPOINT must be an http:// or https:// URL".to_owned(),
            ));
        }
        match non_empty(protocol).as_deref() {
            None | Some("http/protobuf") => {}
            Some(other) => {
                return Err(TelemetryError(format!(
                    "OTEL_EXPORTER_OTLP_PROTOCOL=`{other}` is unsupported: axond exports OTLP/HTTP, so point the endpoint at the collector's HTTP receiver"
                )));
            }
        }
        Ok(Self {
            endpoint: Some(endpoint),
        })
    }
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

/// Owns the provider handles so the process can flush on shutdown. Dropping the
/// guard shuts the exporters down; when telemetry is disabled it holds nothing.
pub struct TelemetryGuard {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
    logger: Option<SdkLoggerProvider>,
}

impl TelemetryGuard {
    /// Flush and stop the exporters within `timeout` *per signal*, reporting the
    /// signals that did not drain.
    ///
    /// The serving path calls this explicitly rather than relying on `Drop`:
    /// exported usage records and the shutdown's own spans are the ones most
    /// likely to be lost, and a failure to export them is an operational fact
    /// worth logging rather than a silently discarded `Result`.
    pub fn shutdown(&mut self, timeout: Duration) -> Vec<(&'static str, String)> {
        let mut failures = Vec::new();
        if let Some(provider) = self.tracer.take()
            && let Err(error) = provider.shutdown_with_timeout(timeout)
        {
            failures.push(("traces", error.to_string()));
        }
        if let Some(provider) = self.meter.take()
            && let Err(error) = provider.shutdown_with_timeout(timeout)
        {
            failures.push(("metrics", error.to_string()));
        }
        if let Some(provider) = self.logger.take()
            && let Err(error) = provider.shutdown_with_timeout(timeout)
        {
            failures.push(("logs", error.to_string()));
        }
        for (signal, error) in &failures {
            tracing::error!(
                signal,
                error = %error,
                "telemetry exporter did not drain within the shutdown bound"
            );
        }
        failures
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        // A no-op after an explicit `shutdown`, which takes the providers.
        let _ = self.shutdown(FLUSH_TIMEOUT);
    }
}

fn resource() -> Resource {
    Resource::builder().with_service_name(SERVICE_NAME).build()
}

/// Install the log subscriber and, when an OTLP endpoint is configured, the
/// tracer + meter providers and the W3C propagator.
pub fn init() -> Result<TelemetryGuard, TelemetryError> {
    init_with(TelemetryConfig::from_env()?)
}

/// The subscriber the optional OTLP layer is boxed against: the filtered
/// registry, before the JSON log layer is appended.
type Filtered = tracing_subscriber::layer::Layered<EnvFilter, tracing_subscriber::Registry>;
type OtelLayer = Box<dyn Layer<Filtered> + Send + Sync>;

/// JSON logs are always the last layer, so log events carry the fields of the
/// enclosing server span (including `trace_id`) whether or not OTLP is on.
fn install(filter: EnvFilter, otel: Option<OtelLayer>) -> Result<(), TelemetryError> {
    tracing_subscriber::registry()
        .with(filter)
        .with(otel)
        .with(tracing_subscriber::fmt::layer().json())
        .try_init()
        .map_err(|e| TelemetryError(format!("subscriber initialization failed: {e}")))
}

fn init_with(config: TelemetryConfig) -> Result<TelemetryGuard, TelemetryError> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,axond=info"));

    let Some(endpoint) = config.endpoint else {
        install(filter, None)?;
        return Ok(TelemetryGuard {
            tracer: None,
            meter: None,
            logger: None,
        });
    };

    let client = exporter::ExportClient::new()?;
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_http_client(client.clone())
        .with_endpoint(signal_endpoint(&endpoint, "traces"))
        .build()
        .map_err(|e| TelemetryError(format!("OTLP span exporter configuration failed: {e}")))?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource())
        .build();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_http_client(client.clone())
        .with_endpoint(signal_endpoint(&endpoint, "metrics"))
        .build()
        .map_err(|e| TelemetryError(format!("OTLP metric exporter configuration failed: {e}")))?;
    let meter_provider = SdkMeterProvider::builder()
        .with_periodic_exporter(metric_exporter)
        .with_resource(resource())
        .build();

    // Only the usage sink emits through this provider, so it stays idle unless a
    // `kind = "otlp"` usage sink is configured — one exporter stack, three
    // signals, no second HTTP client (ADR 0009).
    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_http_client(client)
        .with_endpoint(signal_endpoint(&endpoint, "logs"))
        .build()
        .map_err(|e| TelemetryError(format!("OTLP log exporter configuration failed: {e}")))?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(resource())
        .build();
    let _ = USAGE_LOGGER.set(logger_provider.logger(USAGE_SCOPE));

    let otel: OtelLayer =
        Box::new(tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer(SERVICE_NAME)));

    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    install(filter, Some(otel))?;

    metrics::init();
    EXPORTING.store(true, Ordering::Relaxed);

    Ok(TelemetryGuard {
        tracer: Some(tracer_provider),
        meter: Some(meter_provider),
        logger: Some(logger_provider),
    })
}

/// OTLP/HTTP wants a per-signal path. Accept either a base endpoint
/// (`http://collector:4318`) or one already pointing at a signal — an endpoint
/// naming *one* signal still has to yield the right URL for the other, so any
/// signal path is stripped before the requested one is appended.
fn signal_endpoint(endpoint: &str, signal: &str) -> String {
    let base = SIGNALS
        .iter()
        .fold(endpoint.trim_end_matches('/'), |base, s| {
            base.trim_end_matches(&format!("/v1/{s}"))
        });
    format!("{}/v1/{signal}", base.trim_end_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_endpoint_means_telemetry_is_off() {
        let config = TelemetryConfig::from_values(None, None).expect("default config");
        assert!(config.endpoint.is_none());
        let config = TelemetryConfig::from_values(Some("  "), None).expect("blank is off");
        assert!(config.endpoint.is_none());
    }

    #[test]
    fn rejects_unsupported_protocol_and_scheme() {
        assert!(TelemetryConfig::from_values(Some("http://collector:4318"), Some("grpc")).is_err());
        assert!(TelemetryConfig::from_values(Some("collector:4318"), None).is_err());
    }

    #[test]
    fn signal_paths_are_appended_once() {
        assert_eq!(
            signal_endpoint("http://collector:4318", "traces"),
            "http://collector:4318/v1/traces"
        );
        assert_eq!(
            signal_endpoint("http://collector:4318/v1/metrics", "metrics"),
            "http://collector:4318/v1/metrics"
        );
        // An endpoint naming one signal must still resolve the other.
        assert_eq!(
            signal_endpoint("http://collector:4318/v1/traces", "metrics"),
            "http://collector:4318/v1/metrics"
        );
        assert_eq!(
            signal_endpoint("http://collector:4318/otlp/", "traces"),
            "http://collector:4318/otlp/v1/traces"
        );
    }

    #[test]
    fn resource_carries_the_service_name() {
        let resource = resource();
        assert_eq!(
            resource
                .get(&opentelemetry::Key::from_static_str("service.name"))
                .map(|v| v.as_str().to_string()),
            Some(SERVICE_NAME.to_string())
        );
    }
}
