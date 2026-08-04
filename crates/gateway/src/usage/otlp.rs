//! Usage as OTel log records, for operators who keep everything in one
//! observability backend.
//!
//! This sink is not a second copy of the metrics from ADR 0007. Metrics are
//! aggregates over low-cardinality dimensions; a usage record is the per-request
//! fact, with the identifiers (`request_id`, `subject`, `credential_id`,
//! `trace_id`) that make a single call explainable. Emitting it as a log record
//! keeps those identifiers where high cardinality is affordable.
//!
//! It rides the exporter stack telemetry already installed — same endpoint, same
//! `reqwest` client, same resource — and the SDK's batch log processor does the
//! buffering, so this sink is not wrapped in [`super::BatchedSink`]: `emit` is a
//! non-blocking hand-off to that processor's own bounded queue.

use async_trait::async_trait;
use opentelemetry::logs::{AnyValue, LogRecord as _, Logger as _, Severity};
use opentelemetry::trace::{SpanId, TraceId};
use opentelemetry_sdk::logs::SdkLogger;

use crate::telemetry;

use super::{UsageRecord, UsageSink, UsageSinkError};

/// Event name every exported usage record carries, so a backend can select them
/// without matching on a message body.
const EVENT_NAME: &str = "axond.usage";

pub struct OtlpUsageSink {
    logger: SdkLogger,
}

impl OtlpUsageSink {
    /// Fails when OTLP export is off, rather than emitting into a provider that
    /// goes nowhere: a usage sink that silently discards is the failure mode
    /// this whole issue exists to prevent.
    pub fn new() -> Result<Self, UsageSinkError> {
        let logger = telemetry::usage_logger().ok_or_else(|| {
            UsageSinkError::invalid(
                "otlp",
                "OTLP export is off; set OTEL_EXPORTER_OTLP_ENDPOINT or remove the sink",
            )
        })?;
        Ok(Self { logger })
    }
}

#[async_trait]
impl UsageSink for OtlpUsageSink {
    fn name(&self) -> &'static str {
        "otlp"
    }

    async fn record(&self, record: &UsageRecord) {
        let mut log = self.logger.create_log_record();
        log.set_event_name(EVENT_NAME);
        log.set_severity_number(Severity::Info);
        log.set_severity_text("INFO");
        log.set_body(AnyValue::String(EVENT_NAME.into()));
        log.add_attributes(attributes(record));
        // Joins the row to the caller's trace. The span that produced it has
        // already closed (and for a streamed request, closed well before
        // settlement), so only the trace id is known here.
        if let Some(trace_id) = record.trace_id.as_deref().and_then(parse_trace_id) {
            log.set_trace_context(trace_id, SpanId::INVALID, None);
        }
        self.logger.emit(log);
    }
}

/// The record's fields as log attributes, sharing the naming of the metric
/// dimensions in `telemetry::metrics` so a query can pivot between them.
fn attributes(record: &UsageRecord) -> Vec<(&'static str, AnyValue)> {
    let mut attributes = vec![
        (
            "axond.schema_version",
            AnyValue::Int(i64::from(record.schema_version)),
        ),
        (
            "axond.request_id",
            AnyValue::String(record.request_id.clone().into()),
        ),
        (
            "axond.namespace",
            AnyValue::String(record.namespace.clone().into()),
        ),
        (
            "axond.subject",
            AnyValue::String(record.subject.clone().into()),
        ),
        (
            "gen_ai.request.model",
            AnyValue::String(record.model.clone().into()),
        ),
        (
            "axond.target.provider",
            AnyValue::String(record.target_provider.clone().into()),
        ),
        (
            "axond.target.model",
            AnyValue::String(record.target_model.clone().into()),
        ),
        (
            "axond.credential_source",
            AnyValue::String(record.credential_source.into()),
        ),
        (
            "axond.credential_id",
            AnyValue::String(record.credential_id.clone().into()),
        ),
        (
            "axond.status",
            AnyValue::String(record.status.as_str().into()),
        ),
        (
            "gen_ai.usage.input_tokens",
            AnyValue::Int(clamped(record.input_tokens)),
        ),
        (
            "gen_ai.usage.output_tokens",
            AnyValue::Int(clamped(record.output_tokens)),
        ),
        (
            "axond.cost_microdollars",
            AnyValue::Int(clamped(record.cost_microdollars)),
        ),
        (
            "axond.catalog_version",
            AnyValue::Int(clamped(record.catalog_version)),
        ),
        (
            "axond.latency_ms",
            AnyValue::Int(clamped(record.latency_ms)),
        ),
    ];
    if let Some(trace_id) = &record.trace_id {
        attributes.push(("axond.trace_id", AnyValue::String(trace_id.clone().into())));
    }
    attributes
}

fn clamped(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn parse_trace_id(hex: &str) -> Option<TraceId> {
    TraceId::from_hex(hex)
        .ok()
        .filter(|id| *id != TraceId::INVALID)
}

#[cfg(test)]
mod tests {
    use super::super::tests::sample_record;
    use super::*;

    #[test]
    fn attributes_carry_the_identifiers_metrics_cannot() {
        let record = sample_record();
        let attributes = attributes(&record);
        let keys: Vec<&str> = attributes.iter().map(|(key, _)| *key).collect();
        for key in [
            "axond.request_id",
            "axond.subject",
            "axond.credential_id",
            "axond.trace_id",
            "axond.schema_version",
        ] {
            assert!(keys.contains(&key), "missing `{key}`");
        }
        assert_eq!(
            attributes
                .iter()
                .find(|(key, _)| *key == "axond.cost_microdollars")
                .map(|(_, value)| value.clone()),
            Some(AnyValue::Int(640))
        );
    }

    #[test]
    fn an_untraced_record_omits_the_trace_id() {
        let mut record = sample_record();
        record.trace_id = None;
        let keys: Vec<&str> = attributes(&record).iter().map(|(key, _)| *key).collect();
        assert!(!keys.contains(&"axond.trace_id"));
    }

    #[test]
    fn only_a_well_formed_trace_id_becomes_trace_context() {
        assert!(parse_trace_id("4bf92f3577b34da6a3ce929d0e0e4736").is_some());
        assert!(parse_trace_id("00000000000000000000000000000000").is_none());
        assert!(parse_trace_id("not-a-trace-id").is_none());
    }

    #[test]
    fn the_sink_refuses_to_be_built_when_export_is_off() {
        // The test binary installs no exporter, so the usage logger is empty.
        if telemetry::usage_logger().is_none() {
            assert!(OtlpUsageSink::new().is_err());
        }
    }
}
