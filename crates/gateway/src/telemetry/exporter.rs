//! The OTLP exporter's HTTP client.
//!
//! The exporter drives the same pooled `reqwest` client the gateway already
//! ships rather than the one `opentelemetry-otlp` would bring along: a second
//! HTTP stack would mean a duplicate `reqwest` (and a second TLS provider) in a
//! binary whose whole point is to be a single static file. The client is
//! constructed only when OTLP export is enabled.

use async_trait::async_trait;
use opentelemetry_http::{Bytes, HttpClient, HttpError, Request, Response};

#[derive(Debug, Clone)]
pub(super) struct ExportClient(reqwest::Client);

impl ExportClient {
    pub(super) fn new() -> Self {
        Self(reqwest::Client::new())
    }
}

#[async_trait]
impl HttpClient for ExportClient {
    async fn send_bytes(&self, request: Request<Bytes>) -> Result<Response<Bytes>, HttpError> {
        let (parts, body) = request.into_parts();
        let request = self
            .0
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers)
            .body(body)
            .build()?;

        let response = self.0.execute(request).await?;
        let mut builder = Response::builder().status(response.status());
        if let Some(headers) = builder.headers_mut() {
            headers.extend(
                response
                    .headers()
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone())),
            );
        }
        Ok(builder.body(response.bytes().await?)?)
    }
}
