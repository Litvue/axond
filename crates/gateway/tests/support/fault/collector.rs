//! An OTLP/HTTP receiver, so a matrix row can assert on the telemetry the
//! process actually exported rather than on the logs it happened to print.
//!
//! The payloads are protobuf and are kept as bytes. Two questions are asked of
//! them, and neither needs a decoder: whether an instrument or span *name*
//! appears — an instrument only reaches an export once it has recorded a point,
//! so a name is proof the fault was counted — and whether any secret, DSN, or
//! provider URL appears, which is the leakage check on the one surface an
//! operator forwards off the box.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use bytes::Bytes;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value;
use opentelemetry_proto::tonic::metrics::v1::metric;
use prost::Message;
use tokio::sync::oneshot;

/// One export the collector received.
#[derive(Clone, Debug)]
pub struct Export {
    pub signal: &'static str,
    pub bytes: Bytes,
}

/// One decoded OTLP explicit-histogram point.
#[derive(Clone, Debug, PartialEq)]
pub struct HistogramPoint {
    pub count: u64,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub explicit_bounds: Vec<f64>,
    pub bucket_counts: Vec<u64>,
    pub attributes: usize,
    pub time_unix_nano: u64,
}

#[derive(Default)]
struct CollectorState {
    exports: Mutex<Vec<Export>>,
    trace_identity_caches: Mutex<BTreeMap<String, TraceIdentityCache>>,
}

#[derive(Default)]
struct TraceIdentityCache {
    exports_seen: usize,
    trace_exports_decoded: usize,
    trace_id_occurrences: BTreeMap<String, u64>,
    error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TraceIdentityObservation {
    pub trace_ids: BTreeSet<String>,
    pub occurrences: BTreeMap<String, u64>,
}

pub struct Collector {
    /// The base endpoint, as `OTEL_EXPORTER_OTLP_ENDPOINT` wants it.
    pub endpoint: String,
    state: Arc<CollectorState>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Collector {
    pub async fn start() -> Self {
        let state = Arc::new(CollectorState::default());
        let app = Router::new()
            .route("/v1/traces", post(traces))
            .route("/v1/metrics", post(metrics))
            .route("/v1/logs", post(logs))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("the OTLP collector binds");
        let addr = listener.local_addr().expect("a bound address");
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });
        Self {
            endpoint: format!("http://{addr}"),
            state,
            shutdown: Some(tx),
        }
    }

    pub fn exports(&self) -> Vec<Export> {
        self.state.exports.lock().expect("collector lock").clone()
    }

    /// How many exports arrived, per signal.
    pub fn counts(&self) -> BTreeMap<String, u64> {
        let mut counts = BTreeMap::new();
        for export in self.exports() {
            *counts.entry(export.signal.to_owned()).or_default() += 1;
        }
        counts
    }

    pub fn bytes(&self) -> u64 {
        self.exports().iter().map(|e| e.bytes.len() as u64).sum()
    }

    /// Distinct trace ids exported by one expected process identity.
    ///
    /// Rollout gives every replica a dedicated receiver. Any resource in that
    /// receiver carrying a different or missing `service.instance.id` is an
    /// invalid witness rather than evidence another process may borrow.
    pub fn trace_ids_for_instance(
        &self,
        expected_instance: &str,
    ) -> Result<BTreeSet<String>, String> {
        Ok(self
            .trace_identity_observation(expected_instance)?
            .trace_ids)
    }

    pub fn trace_identity_observation(
        &self,
        expected_instance: &str,
    ) -> Result<TraceIdentityObservation, String> {
        // Settlement polls this method several times while exporters flush.
        // Decode only exports that arrived since the last observation instead
        // of repeatedly cloning and decoding the complete retained corpus.
        let mut caches = self
            .state
            .trace_identity_caches
            .lock()
            .expect("collector trace cache lock");
        let cache = caches.entry(expected_instance.to_owned()).or_default();
        if let Some(error) = &cache.error {
            return Err(error.clone());
        }
        let exports = self.state.exports.lock().expect("collector lock");
        for export in exports
            .iter()
            .skip(cache.exports_seen)
            .filter(|export| export.signal == "traces")
        {
            cache.trace_exports_decoded += 1;
            let request = match ExportTraceServiceRequest::decode(export.bytes.clone()) {
                Ok(request) => request,
                Err(error) => {
                    let error = format!("invalid OTLP trace export: {error}");
                    cache.error = Some(error.clone());
                    return Err(error);
                }
            };
            for resource_spans in request.resource_spans {
                let spans = resource_spans
                    .scope_spans
                    .into_iter()
                    .flat_map(|scope| scope.spans)
                    .collect::<Vec<_>>();
                if spans.is_empty() {
                    continue;
                }
                let Some(resource) = resource_spans.resource else {
                    let error =
                        format!("OTLP trace export for `{expected_instance}` has no resource");
                    cache.error = Some(error.clone());
                    return Err(error);
                };
                let instances = resource
                    .attributes
                    .into_iter()
                    .filter(|attribute| attribute.key == "service.instance.id")
                    .filter_map(|attribute| attribute.value)
                    .filter_map(|value| value.value)
                    .filter_map(|value| match value {
                        any_value::Value::StringValue(instance) => Some(instance),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if instances != [expected_instance] {
                    let error = format!(
                        "OTLP trace receiver for `{expected_instance}` observed resource identities {instances:?}"
                    );
                    cache.error = Some(error.clone());
                    return Err(error);
                }
                for span in spans {
                    let Some(trace_id) = canonical_trace_id(&span.trace_id) else {
                        let error = format!(
                            "OTLP trace receiver for `{expected_instance}` observed a malformed trace id"
                        );
                        cache.error = Some(error.clone());
                        return Err(error);
                    };
                    *cache.trace_id_occurrences.entry(trace_id).or_default() += 1;
                }
            }
        }
        cache.exports_seen = exports.len();
        Ok(TraceIdentityObservation {
            trace_ids: cache.trace_id_occurrences.keys().cloned().collect(),
            occurrences: cache.trace_id_occurrences.clone(),
        })
    }

    #[cfg(test)]
    pub fn trace_exports_decoded_for_instance(&self, expected_instance: &str) -> usize {
        self.state
            .trace_identity_caches
            .lock()
            .expect("collector trace cache lock")
            .get(expected_instance)
            .map_or(0, |cache| cache.trace_exports_decoded)
    }

    /// Whether `needle` appears in any exported payload of `signal`.
    pub fn signal_contains(&self, signal: &str, needle: &str) -> bool {
        self.exports()
            .iter()
            .any(|export| export.signal == signal && contains(&export.bytes, needle.as_bytes()))
    }

    /// Whether `needle` appears in any exported payload at all.
    pub fn contains(&self, needle: &str) -> bool {
        self.exports()
            .iter()
            .any(|export| contains(&export.bytes, needle.as_bytes()))
    }

    /// Decode every explicit-histogram point exported under `name`.
    ///
    /// Qualification needs the values, bounds, and absence of labels rather
    /// than only proof that an instrument name occurred in protobuf bytes.
    pub fn histogram_points(&self, name: &str) -> Result<Vec<HistogramPoint>, String> {
        let mut found = Vec::new();
        for export in self.exports().into_iter().filter(|e| e.signal == "metrics") {
            let request = ExportMetricsServiceRequest::decode(export.bytes)
                .map_err(|error| format!("invalid OTLP metrics export: {error}"))?;
            for metric in request
                .resource_metrics
                .into_iter()
                .flat_map(|resource| resource.scope_metrics)
                .flat_map(|scope| scope.metrics)
                .filter(|metric| metric.name == name)
            {
                let Some(metric::Data::Histogram(histogram)) = metric.data else {
                    return Err(format!("OTLP metric `{name}` is not an explicit histogram"));
                };
                found.extend(
                    histogram
                        .data_points
                        .into_iter()
                        .map(|point| HistogramPoint {
                            count: point.count,
                            sum: point.sum,
                            min: point.min,
                            max: point.max,
                            explicit_bounds: point.explicit_bounds,
                            bucket_counts: point.bucket_counts,
                            attributes: point.attributes.len(),
                            time_unix_nano: point.time_unix_nano,
                        }),
                );
            }
        }
        Ok(found)
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty()
        && haystack.len() >= needle.len()
        && haystack
            .windows(needle.len())
            .any(|window| window == needle)
}

fn canonical_trace_id(bytes: &[u8]) -> Option<String> {
    if bytes.len() != 16 || bytes.iter().all(|byte| *byte == 0) {
        return None;
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Some(encoded)
}

type Accepted = (StatusCode, [(&'static str, &'static str); 1], Bytes);

async fn traces(state: State<Arc<CollectorState>>, body: Bytes) -> Accepted {
    receive(state, "traces", body)
}

async fn metrics(state: State<Arc<CollectorState>>, body: Bytes) -> Accepted {
    receive(state, "metrics", body)
}

async fn logs(state: State<Arc<CollectorState>>, body: Bytes) -> Accepted {
    receive(state, "logs", body)
}

fn receive(
    State(state): State<Arc<CollectorState>>,
    signal: &'static str,
    body: Bytes,
) -> Accepted {
    state.exports.lock().expect("collector lock").push(Export {
        signal,
        bytes: body,
    });
    // An empty `Export*ServiceResponse` is the success answer, and an empty
    // protobuf message is zero bytes.
    (
        StatusCode::OK,
        [("content-type", "application/x-protobuf")],
        Bytes::new(),
    )
}
