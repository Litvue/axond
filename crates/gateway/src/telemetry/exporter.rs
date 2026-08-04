//! The OTLP exporter's HTTP client.
//!
//! The exporter drives the same pooled `reqwest` client the gateway already
//! ships rather than the one `opentelemetry-otlp` would bring along: a second
//! HTTP stack would mean a duplicate `reqwest` (and a second TLS provider) in a
//! binary whose whole point is to be a single static file. The client is
//! constructed only when OTLP export is enabled.
//!
//! The SDK drives exports from its own threads (the batch span processor and
//! the periodic metric reader), which are not Tokio worker threads and have no
//! reactor. So the request is spawned onto the runtime captured at init and the
//! caller awaits the join handle, which needs no reactor of its own.

use async_trait::async_trait;
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};
use tokio::runtime::Handle;

#[derive(Debug, Clone)]
pub(super) struct ExportClient {
    client: reqwest::Client,
    runtime: Handle,
}

impl ExportClient {
    /// Must be called from within the Tokio runtime that will serve requests.
    pub(super) fn new() -> Result<Self, super::TelemetryError> {
        let runtime = Handle::try_current().map_err(|e| {
            super::TelemetryError(format!(
                "telemetry must be initialized on a Tokio runtime: {e}"
            ))
        })?;
        Ok(Self {
            client: reqwest::Client::new(),
            runtime,
        })
    }
}

#[async_trait]
impl HttpClient for ExportClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (parts, body) = request.into_parts();
        let request = self
            .client
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers)
            .body(body)
            .build()?;

        let client = self.client.clone();
        let sent = self
            .runtime
            .spawn(async move {
                let response = client.execute(request).await?;
                let status = response.status();
                let headers = response.headers().clone();
                response.bytes().await.map(|body| (status, headers, body))
            })
            .await
            .map_err(|e| HttpError::from(format!("OTLP export task failed: {e}")))?;

        let (status, headers, body) = sent?;
        let mut builder = Response::builder().status(status);
        if let Some(target) = builder.headers_mut() {
            *target = headers;
        }
        Ok(builder.body(body)?)
    }
}
