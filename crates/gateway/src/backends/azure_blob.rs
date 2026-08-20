//! Azure Blob Storage implementation of the provider-neutral object-store CAS contract.
//!
//! The production constructors accept only credential-free HTTPS container URLs and an
//! injected Azure [`TokenCredential`]. This keeps workload-identity selection outside the
//! adapter and prevents SAS tokens or user information from reaching error messages. An
//! explicitly named development constructor permits loopback HTTP for Azurite and tests.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use azure_core::credentials::TokenCredential;
use azure_core::error::ErrorKind as AzureErrorKind;
use azure_core::http::{Etag, RequestContent, StatusCode, Url};
use azure_storage_blob::clients::{BlobClient, BlobClientOptions};
use azure_storage_blob::models::{
    BlobClientDeleteOptions, BlobClientDownloadOptions, BlobClientUploadOptions,
    DeleteSnapshotsOptionType, HttpRange,
};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;

use super::object_store::{
    ObjectKey, ObjectStore, ObjectStoreError, ObjectStoreLimits, ObjectStoreMaintenance,
    ObjectStoreOperation, ObjectValue, ObjectVersion,
};

/// Configuration errors detected before an Azure client is constructed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AzureBlobObjectStoreConfigError {
    #[error("Azure Blob Storage production container URL must use HTTPS")]
    ProductionRequiresHttps,
    #[error("Azure Blob Storage development HTTP container URL must use a loopback host")]
    DevelopmentHttpRequiresLoopback,
    #[error("Azure Blob Storage container URL must not contain credentials, query, or fragment")]
    SensitiveUrlComponents,
    #[error("Azure Blob Storage container URL must identify one valid container")]
    InvalidContainerUrl,
}

/// Azure block-blob storage backed by native ETag conditions.
#[derive(Clone)]
pub struct AzureBlobObjectStore {
    container_url: Url,
    credential: Option<Arc<dyn TokenCredential>>,
    client_options: BlobClientOptions,
    limits: ObjectStoreLimits,
}

impl fmt::Debug for AzureBlobObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AzureBlobObjectStore")
            .field("container_url", &self.container_url)
            .field(
                "credential",
                &self.credential.as_ref().map(|_| "<redacted>"),
            )
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl AzureBlobObjectStore {
    /// Construct a production store using the SDK's default client configuration.
    pub fn new(
        container_url: Url,
        credential: Arc<dyn TokenCredential>,
        limits: ObjectStoreLimits,
    ) -> Result<Self, AzureBlobObjectStoreConfigError> {
        Self::new_with_client_options(
            container_url,
            credential,
            limits,
            BlobClientOptions::default(),
        )
    }

    /// Construct a production store with an injected Azure client seam.
    ///
    /// `client_options` may provide a custom transport, retry policy, or test client. The
    /// container URL remains HTTPS-only and may not carry a SAS token or user information.
    pub fn new_with_client_options(
        container_url: Url,
        credential: Arc<dyn TokenCredential>,
        limits: ObjectStoreLimits,
        client_options: BlobClientOptions,
    ) -> Result<Self, AzureBlobObjectStoreConfigError> {
        Self::construct(
            container_url,
            Some(credential),
            limits,
            client_options,
            UrlPolicy::Production,
        )
    }

    /// Construct a development store that may use loopback HTTP for Azurite or a fake server.
    ///
    /// This constructor is deliberately explicit. Non-loopback HTTP is rejected, and the same
    /// credential/query/fragment restrictions as production still apply.
    pub fn for_development_http(
        container_url: Url,
        credential: Option<Arc<dyn TokenCredential>>,
        limits: ObjectStoreLimits,
        client_options: BlobClientOptions,
    ) -> Result<Self, AzureBlobObjectStoreConfigError> {
        Self::construct(
            container_url,
            credential,
            limits,
            client_options,
            UrlPolicy::Development,
        )
    }

    fn construct(
        container_url: Url,
        credential: Option<Arc<dyn TokenCredential>>,
        limits: ObjectStoreLimits,
        client_options: BlobClientOptions,
        policy: UrlPolicy,
    ) -> Result<Self, AzureBlobObjectStoreConfigError> {
        validate_container_url(&container_url, policy)?;
        Ok(Self {
            container_url,
            credential,
            client_options,
            limits,
        })
    }

    fn check_write(
        &self,
        key: &ObjectKey,
        operation: ObjectStoreOperation,
        bytes: &Bytes,
    ) -> Result<(), ObjectStoreError> {
        let limit = self.limits.max_write_bytes();
        if bytes.len() > limit {
            return Err(ObjectStoreError::PayloadTooLarge {
                key: key.clone(),
                operation,
                observed: bytes.len(),
                limit,
            });
        }
        Ok(())
    }

    fn blob_client(
        &self,
        key: &ObjectKey,
        operation: ObjectStoreOperation,
    ) -> Result<BlobClient, ObjectStoreError> {
        let mut blob_url = self.container_url.clone();
        let mut path = blob_url.path_segments_mut().map_err(|_| {
            ObjectStoreError::integrity(key.clone(), "container URL cannot contain blob paths")
        })?;
        for segment in key.as_str().split('/') {
            path.push(segment);
        }
        drop(path);

        BlobClient::new(
            blob_url,
            self.credential.clone(),
            Some(self.client_options.clone()),
        )
        .map_err(|error| map_azure_error(key, operation, error))
    }

    fn download_options(&self) -> BlobClientDownloadOptions<'static> {
        let limit = self.limits.max_read_bytes();
        let probe_length = limit.saturating_add(1);
        BlobClientDownloadOptions {
            range: u64::try_from(probe_length)
                .ok()
                .map(|length| HttpRange::new(0, length)),
            parallel: NonZeroUsize::new(1),
            partition_size: NonZeroUsize::new(probe_length),
            ..Default::default()
        }
    }

    async fn upload(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
        operation: ObjectStoreOperation,
        options: BlobClientUploadOptions<'_>,
    ) -> Result<ObjectVersion, ObjectStoreError> {
        self.check_write(key, operation, &bytes)?;
        let result = self
            .blob_client(key, operation)?
            .upload(RequestContent::from(bytes.to_vec()), Some(options))
            .await
            .map_err(|error| map_azure_error(key, operation, error))?;
        parse_etag(key, result.etag)
    }
}

#[async_trait]
impl ObjectStore for AzureBlobObjectStore {
    fn name(&self) -> &'static str {
        "azure-blob"
    }

    fn limits(&self) -> ObjectStoreLimits {
        self.limits
    }

    async fn get(&self, key: &ObjectKey) -> Result<ObjectValue, ObjectStoreError> {
        let response = self
            .blob_client(key, ObjectStoreOperation::Get)?
            .download(Some(self.download_options()))
            .await
            .map_err(|error| map_azure_error(key, ObjectStoreOperation::Get, error))?;
        let version = parse_etag(key, response.properties.etag)?;
        let limit = self.limits.max_read_bytes();

        if let Some(observed) = response
            .headers
            .get_optional_str(&"content-range".into())
            .and_then(content_range_total)
            .or_else(|| response.properties.content_length.and_then(u64_to_usize))
            .filter(|observed| *observed > limit)
        {
            return Err(ObjectStoreError::PayloadTooLarge {
                key: key.clone(),
                operation: ObjectStoreOperation::Get,
                observed,
                limit,
            });
        }

        let mut body = response.body;
        let mut bytes = BytesMut::with_capacity(
            response
                .properties
                .content_length
                .and_then(u64_to_usize)
                .unwrap_or(0)
                .min(limit),
        );
        while let Some(chunk) = body.next().await {
            let chunk =
                chunk.map_err(|error| map_azure_error(key, ObjectStoreOperation::Get, error))?;
            let observed = bytes.len().saturating_add(chunk.len());
            if observed > limit {
                return Err(ObjectStoreError::PayloadTooLarge {
                    key: key.clone(),
                    operation: ObjectStoreOperation::Get,
                    observed,
                    limit,
                });
            }
            bytes.extend_from_slice(&chunk);
        }

        Ok(ObjectValue {
            bytes: bytes.freeze(),
            version,
        })
    }

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<ObjectVersion, ObjectStoreError> {
        self.upload(
            key,
            bytes,
            ObjectStoreOperation::PutIfAbsent,
            BlobClientUploadOptions::default().if_not_exists(),
        )
        .await
    }

    async fn replace_if_version(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
        expected: &ObjectVersion,
    ) -> Result<ObjectVersion, ObjectStoreError> {
        validate_etag(expected.as_opaque()).map_err(|message| {
            ObjectStoreError::integrity(key.clone(), format!("invalid expected ETag: {message}"))
        })?;
        self.upload(
            key,
            bytes,
            ObjectStoreOperation::ReplaceIfVersion,
            BlobClientUploadOptions {
                if_match: Some(Etag::from(expected.as_opaque())),
                ..Default::default()
            },
        )
        .await
    }
}

#[async_trait]
impl ObjectStoreMaintenance for AzureBlobObjectStore {
    async fn delete_if_version(
        &self,
        key: &ObjectKey,
        expected: &ObjectVersion,
    ) -> Result<(), ObjectStoreError> {
        validate_etag(expected.as_opaque()).map_err(|message| {
            ObjectStoreError::integrity(key.clone(), format!("invalid expected ETag: {message}"))
        })?;
        self.blob_client(key, ObjectStoreOperation::DeleteIfVersion)?
            .delete(Some(BlobClientDeleteOptions {
                // Azure otherwise refuses to delete a base blob that has
                // snapshots. Including them is the strongest erasure request
                // the adapter can issue; account-level soft-delete/version
                // retention may still delay physical destruction and is an
                // operator-visible deployment property.
                delete_snapshots: Some(DeleteSnapshotsOptionType::Include),
                if_match: Some(Etag::from(expected.as_opaque())),
                ..Default::default()
            }))
            .await
            .map_err(|error| map_azure_error(key, ObjectStoreOperation::DeleteIfVersion, error))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum UrlPolicy {
    Production,
    Development,
}

fn validate_container_url(
    url: &Url,
    policy: UrlPolicy,
) -> Result<(), AzureBlobObjectStoreConfigError> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(AzureBlobObjectStoreConfigError::SensitiveUrlComponents);
    }
    let host = url
        .host_str()
        .ok_or(AzureBlobObjectStoreConfigError::InvalidContainerUrl)?;
    match (policy, url.scheme()) {
        (_, "https") => {}
        (UrlPolicy::Production, _) => {
            return Err(AzureBlobObjectStoreConfigError::ProductionRequiresHttps);
        }
        (UrlPolicy::Development, "http") if is_loopback_host(host) => {}
        (UrlPolicy::Development, "http") => {
            return Err(AzureBlobObjectStoreConfigError::DevelopmentHttpRequiresLoopback);
        }
        (UrlPolicy::Development, _) => {
            return Err(AzureBlobObjectStoreConfigError::InvalidContainerUrl);
        }
    }

    let segments = url
        .path_segments()
        .ok_or(AzureBlobObjectStoreConfigError::InvalidContainerUrl)?
        .collect::<Vec<_>>();
    let expected_segments = match policy {
        UrlPolicy::Production => 1..=1,
        UrlPolicy::Development => 1..=2,
    };
    if !expected_segments.contains(&segments.len())
        || segments.iter().any(|segment| segment.is_empty())
        || !valid_container_name(segments[segments.len() - 1])
    {
        return Err(AzureBlobObjectStoreConfigError::InvalidContainerUrl);
    }
    Ok(())
}

fn valid_container_name(name: &str) -> bool {
    (3..=63).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && name
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && name
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !name.contains("--")
}

fn is_loopback_host(host: &str) -> bool {
    let address = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    address.eq_ignore_ascii_case("localhost")
        || address
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

fn parse_etag(key: &ObjectKey, etag: Option<Etag>) -> Result<ObjectVersion, ObjectStoreError> {
    let token = etag.ok_or_else(|| {
        ObjectStoreError::integrity(key.clone(), "Azure response omitted its ETag")
    })?;
    validate_etag(token.as_ref())
        .map_err(|message| ObjectStoreError::integrity(key.clone(), message))?;
    ObjectVersion::opaque(token.to_string())
        .map_err(|_| ObjectStoreError::integrity(key.clone(), "Azure response ETag was empty"))
}

fn validate_etag(value: &str) -> Result<(), &'static str> {
    let Some(inner) = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    else {
        return Err("Azure response ETag was not a quoted strong entity tag");
    };
    if inner
        .bytes()
        .any(|byte| byte == b'"' || byte < 0x21 || byte == 0x7f || !byte.is_ascii())
    {
        return Err("Azure response ETag contained invalid entity-tag bytes");
    }
    Ok(())
}

fn content_range_total(value: &str) -> Option<usize> {
    value
        .strip_prefix("bytes ")?
        .split_once('/')?
        .1
        .parse()
        .ok()
}

fn u64_to_usize(value: u64) -> Option<usize> {
    usize::try_from(value).ok()
}

fn map_azure_error(
    key: &ObjectKey,
    operation: ObjectStoreOperation,
    error: azure_core::Error,
) -> ObjectStoreError {
    if let Some(status) = error.http_status() {
        if status == StatusCode::NotFound {
            return ObjectStoreError::NotFound { key: key.clone() };
        }
        if matches!(
            operation,
            ObjectStoreOperation::PutIfAbsent
                | ObjectStoreOperation::ReplaceIfVersion
                | ObjectStoreOperation::DeleteIfVersion
        ) && matches!(
            status,
            StatusCode::Conflict | StatusCode::PreconditionFailed
        ) {
            return ObjectStoreError::PreconditionFailed {
                key: key.clone(),
                operation,
            };
        }
        if matches!(
            status,
            StatusCode::Unauthorized
                | StatusCode::Forbidden
                | StatusCode::RequestTimeout
                | StatusCode::TooManyRequests
        ) || status.is_server_error()
        {
            return ObjectStoreError::unavailable(
                operation,
                "Azure Blob Storage request failed transiently",
            );
        }
        return ObjectStoreError::integrity(
            key.clone(),
            "Azure Blob Storage returned an unexpected HTTP response",
        );
    }

    match error.kind() {
        AzureErrorKind::Connection | AzureErrorKind::Io | AzureErrorKind::Credential => {
            ObjectStoreError::unavailable(
                operation,
                "Azure Blob Storage request could not be completed",
            )
        }
        AzureErrorKind::DataConversion | AzureErrorKind::Other => ObjectStoreError::integrity(
            key.clone(),
            "Azure Blob Storage returned a malformed response",
        ),
        AzureErrorKind::HttpResponse { .. } => ObjectStoreError::integrity(
            key.clone(),
            "Azure Blob Storage returned an unclassified HTTP response",
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::Router;
    use axum::body::{Body, to_bytes};
    use axum::extract::State;
    use axum::http::{HeaderMap, Method, Request, Response, StatusCode as HttpStatusCode, Uri};
    use axum::routing::any;
    use azure_core::http::RetryOptions;
    use futures::stream;
    use tokio::net::TcpListener;
    use tokio::sync::mpsc;

    use super::*;
    use crate::backends::object_store::ObjectStoreErrorKind;

    struct RecordedRequest {
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    }

    struct FakeState {
        responses: Mutex<VecDeque<Response<Body>>>,
        requests: mpsc::UnboundedSender<RecordedRequest>,
    }

    struct FakeServer {
        container_url: Url,
        requests: mpsc::UnboundedReceiver<RecordedRequest>,
        task: tokio::task::JoinHandle<()>,
    }

    impl FakeServer {
        async fn start(responses: Vec<Response<Body>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind fake Azure server");
            let address = listener.local_addr().expect("fake server address");
            let (requests_tx, requests) = mpsc::unbounded_channel();
            let state = Arc::new(FakeState {
                responses: Mutex::new(responses.into()),
                requests: requests_tx,
            });
            let app = Router::new().fallback(any(fake_azure)).with_state(state);
            let task = tokio::spawn(async move {
                axum::serve(listener, app)
                    .await
                    .expect("serve fake Azure requests");
            });
            let container_url =
                Url::parse(&format!("http://{address}/devstoreaccount1/axond-state"))
                    .expect("fake container URL");
            Self {
                container_url,
                requests,
                task,
            }
        }

        async fn request(&mut self) -> RecordedRequest {
            tokio::time::timeout(Duration::from_secs(2), self.requests.recv())
                .await
                .expect("request timeout")
                .expect("request channel closed")
        }
    }

    impl Drop for FakeServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn fake_azure(
        State(state): State<Arc<FakeState>>,
        request: Request<Body>,
    ) -> Response<Body> {
        let (parts, body) = request.into_parts();
        let body = to_bytes(body, 1_024 * 1_024)
            .await
            .expect("bounded fake request body");
        state
            .requests
            .send(RecordedRequest {
                method: parts.method,
                uri: parts.uri,
                headers: parts.headers,
                body,
            })
            .expect("record fake request");
        state
            .responses
            .lock()
            .expect("response queue lock")
            .pop_front()
            .expect("fake response")
    }

    fn response(
        status: HttpStatusCode,
        etag: Option<&str>,
        body: impl Into<Body>,
    ) -> Response<Body> {
        let mut builder = Response::builder().status(status);
        if let Some(etag) = etag {
            builder = builder.header("etag", etag);
        }
        builder.body(body.into()).expect("fake response")
    }

    fn ranged_response(etag: &str, bytes: &'static [u8]) -> Response<Body> {
        Response::builder()
            .status(HttpStatusCode::PARTIAL_CONTENT)
            .header("etag", etag)
            .header("content-length", bytes.len())
            .header(
                "content-range",
                format!("bytes 0-{}/{}", bytes.len() - 1, bytes.len()),
            )
            .body(Body::from(bytes))
            .expect("fake ranged response")
    }

    fn error_response(status: HttpStatusCode) -> Response<Body> {
        Response::builder()
            .status(status)
            .header("x-ms-error-code", "FakeError")
            .body(Body::empty())
            .expect("fake error response")
    }

    fn limits(read: usize, write: usize) -> ObjectStoreLimits {
        ObjectStoreLimits::new(
            NonZeroUsize::new(read).expect("read limit"),
            NonZeroUsize::new(write).expect("write limit"),
        )
    }

    fn development_store(server: &FakeServer, limits: ObjectStoreLimits) -> AzureBlobObjectStore {
        let mut options = BlobClientOptions::default();
        options.client_options.retry = RetryOptions::none();
        AzureBlobObjectStore::for_development_http(
            server.container_url.clone(),
            None,
            limits,
            options,
        )
        .expect("development Azure store")
    }

    fn key() -> ObjectKey {
        ObjectKey::parse("namespaces/ns_1/revisions/rev-1.0/head.json").expect("object key")
    }

    #[test]
    fn production_is_https_only_and_development_http_is_loopback_only() {
        let production_http =
            Url::parse("http://account.blob.core.windows.net/axond-state").expect("HTTP URL");
        assert_eq!(
            validate_container_url(&production_http, UrlPolicy::Production),
            Err(AzureBlobObjectStoreConfigError::ProductionRequiresHttps)
        );

        let remote_development = Url::parse("http://example.test/axond-state").expect("HTTP URL");
        assert_eq!(
            validate_container_url(&remote_development, UrlPolicy::Development),
            Err(AzureBlobObjectStoreConfigError::DevelopmentHttpRequiresLoopback)
        );

        let production =
            Url::parse("https://account.blob.core.windows.net/axond-state").expect("HTTPS URL");
        assert!(validate_container_url(&production, UrlPolicy::Production).is_ok());
        let azurite =
            Url::parse("http://127.0.0.1:10000/account/axond-state").expect("Azurite URL");
        assert!(validate_container_url(&azurite, UrlPolicy::Development).is_ok());
        let ipv6_azurite =
            Url::parse("http://[::1]:10000/account/axond-state").expect("IPv6 Azurite URL");
        assert!(validate_container_url(&ipv6_azurite, UrlPolicy::Development).is_ok());
    }

    #[test]
    fn container_url_refuses_sensitive_and_ambiguous_components() {
        for value in [
            "https://user:password@example.test/axond-state",
            "https://example.test/axond-state?sig=secret",
            "https://example.test/axond-state#fragment",
        ] {
            let url = Url::parse(value).expect("URL");
            assert_eq!(
                validate_container_url(&url, UrlPolicy::Production),
                Err(AzureBlobObjectStoreConfigError::SensitiveUrlComponents)
            );
        }
        for value in [
            "https://example.test/",
            "https://example.test/UPPERCASE",
            "https://example.test/a--b",
            "https://example.test/container/extra",
        ] {
            let url = Url::parse(value).expect("URL");
            assert_eq!(
                validate_container_url(&url, UrlPolicy::Production),
                Err(AzureBlobObjectStoreConfigError::InvalidContainerUrl)
            );
        }
    }

    #[tokio::test]
    async fn exact_key_get_preserves_path_and_opaque_etag() {
        let mut server = FakeServer::start(vec![ranged_response("\"etag/one\"", b"value")]).await;
        let store = development_store(&server, limits(16, 16));

        let value = store.get(&key()).await.expect("download object");
        assert_eq!(value.bytes, Bytes::from_static(b"value"));
        assert_eq!(value.version.as_opaque(), "\"etag/one\"");

        let request = server.request().await;
        assert_eq!(request.method, Method::GET);
        assert_eq!(
            request.uri.path(),
            "/devstoreaccount1/axond-state/namespaces/ns_1/revisions/rev-1.0/head.json"
        );
        assert_eq!(request.uri.query(), None);
        assert_eq!(request.headers["range"], "bytes=0-16");
    }

    #[tokio::test]
    async fn empty_blob_get_uses_the_sdk_safe_retry_and_returns_a_value() {
        let empty = Response::builder()
            .status(HttpStatusCode::OK)
            .header("etag", "\"empty\"")
            .header("content-length", 0)
            .body(Body::empty())
            .expect("empty blob response");
        let mut server = FakeServer::start(vec![
            error_response(HttpStatusCode::RANGE_NOT_SATISFIABLE),
            empty,
        ])
        .await;
        let store = development_store(&server, limits(16, 16));

        let value = store.get(&key()).await.expect("download empty object");
        assert!(value.bytes.is_empty());
        assert_eq!(value.version.as_opaque(), "\"empty\"");

        let ranged = server.request().await;
        assert_eq!(ranged.method, Method::GET);
        assert_eq!(ranged.headers["range"], "bytes=0-16");
        let retry = server.request().await;
        assert_eq!(retry.method, Method::GET);
        assert!(retry.headers.get("range").is_none());
    }

    #[tokio::test]
    async fn create_uses_native_if_none_match() {
        let mut server = FakeServer::start(vec![response(
            HttpStatusCode::CREATED,
            Some("\"created\""),
            Body::empty(),
        )])
        .await;
        let store = development_store(&server, limits(32, 32));

        let version = store
            .put_if_absent(&key(), Bytes::from_static(b"immutable"))
            .await
            .expect("create blob");
        assert_eq!(version.as_opaque(), "\"created\"");

        let request = server.request().await;
        assert_eq!(request.method, Method::PUT);
        assert_eq!(request.headers["if-none-match"], "*");
        assert_eq!(request.headers["x-ms-blob-type"], "BlockBlob");
        assert_eq!(request.body, Bytes::from_static(b"immutable"));
    }

    #[tokio::test]
    async fn replacement_uses_the_exact_opaque_etag() {
        let mut server = FakeServer::start(vec![response(
            HttpStatusCode::CREATED,
            Some("\"replacement\""),
            Body::empty(),
        )])
        .await;
        let store = development_store(&server, limits(32, 32));
        let expected = ObjectVersion::opaque("\"opaque/etag-1\"").expect("expected ETag");

        let version = store
            .replace_if_version(&key(), Bytes::from_static(b"replacement"), &expected)
            .await
            .expect("replace blob");
        assert_eq!(version.as_opaque(), "\"replacement\"");

        let request = server.request().await;
        assert_eq!(request.method, Method::PUT);
        assert_eq!(request.headers["if-match"], "\"opaque/etag-1\"");
        assert!(request.headers.get("if-none-match").is_none());
        assert_eq!(request.body, Bytes::from_static(b"replacement"));
    }

    #[tokio::test]
    async fn maintenance_delete_uses_the_exact_opaque_etag() {
        let mut server = FakeServer::start(vec![response(
            HttpStatusCode::ACCEPTED,
            None,
            Body::empty(),
        )])
        .await;
        let store = development_store(&server, limits(32, 32));
        let expected = ObjectVersion::opaque("\"opaque/etag-1\"").expect("expected ETag");

        store
            .delete_if_version(&key(), &expected)
            .await
            .expect("delete exact blob version");

        let request = server.request().await;
        assert_eq!(request.method, Method::DELETE);
        assert_eq!(request.headers["if-match"], "\"opaque/etag-1\"");
        assert_eq!(request.headers["x-ms-delete-snapshots"], "include");
        assert!(request.body.is_empty());
    }

    #[tokio::test]
    async fn maintenance_delete_maps_missing_stale_and_auth_failures() {
        let server = FakeServer::start(vec![
            error_response(HttpStatusCode::NOT_FOUND),
            error_response(HttpStatusCode::PRECONDITION_FAILED),
            error_response(HttpStatusCode::FORBIDDEN),
        ])
        .await;
        let store = development_store(&server, limits(32, 32));
        let expected = ObjectVersion::opaque("\"current\"").expect("ETag");

        assert_eq!(
            store
                .delete_if_version(&key(), &expected)
                .await
                .expect_err("missing")
                .kind(),
            ObjectStoreErrorKind::NotFound
        );
        assert_eq!(
            store
                .delete_if_version(&key(), &expected)
                .await
                .expect_err("stale")
                .kind(),
            ObjectStoreErrorKind::PreconditionFailed
        );
        assert_eq!(
            store
                .delete_if_version(&key(), &expected)
                .await
                .expect_err("auth")
                .kind(),
            ObjectStoreErrorKind::Unavailable
        );
    }

    #[tokio::test]
    async fn response_statuses_map_to_the_contract_taxonomy() {
        let responses = vec![
            error_response(HttpStatusCode::NOT_FOUND),
            error_response(HttpStatusCode::CONFLICT),
            error_response(HttpStatusCode::PRECONDITION_FAILED),
            error_response(HttpStatusCode::REQUEST_TIMEOUT),
            error_response(HttpStatusCode::TOO_MANY_REQUESTS),
            error_response(HttpStatusCode::INTERNAL_SERVER_ERROR),
            error_response(HttpStatusCode::UNAUTHORIZED),
            error_response(HttpStatusCode::FORBIDDEN),
            error_response(HttpStatusCode::BAD_REQUEST),
        ];
        let server = FakeServer::start(responses).await;
        let store = development_store(&server, limits(32, 32));
        let expected = ObjectVersion::opaque("\"current\"").expect("ETag");

        let not_found = store.get(&key()).await.expect_err("404");
        assert_eq!(not_found.kind(), ObjectStoreErrorKind::NotFound);
        let conflict = store
            .put_if_absent(&key(), Bytes::from_static(b"value"))
            .await
            .expect_err("409");
        assert_eq!(conflict.kind(), ObjectStoreErrorKind::PreconditionFailed);
        let stale = store
            .replace_if_version(&key(), Bytes::from_static(b"value"), &expected)
            .await
            .expect_err("412");
        assert_eq!(stale.kind(), ObjectStoreErrorKind::PreconditionFailed);
        for _ in 0..3 {
            let transient = store.get(&key()).await.expect_err("transient status");
            assert_eq!(transient.kind(), ObjectStoreErrorKind::Unavailable);
        }
        for _ in 0..2 {
            let auth = store.get(&key()).await.expect_err("Azure auth status");
            assert_eq!(auth.kind(), ObjectStoreErrorKind::Unavailable);
        }
        let malformed = store.get(&key()).await.expect_err("unexpected status");
        assert_eq!(malformed.kind(), ObjectStoreErrorKind::Integrity);
    }

    #[tokio::test]
    async fn missing_conditional_replace_matches_the_portable_not_found_contract() {
        let server = FakeServer::start(vec![error_response(HttpStatusCode::NOT_FOUND)]).await;
        let store = development_store(&server, limits(32, 32));
        let expected = ObjectVersion::opaque("\"never-created\"").expect("ETag");

        let error = store
            .replace_if_version(&key(), Bytes::from_static(b"value"), &expected)
            .await
            .expect_err("a missing CAS target is not found");
        assert_eq!(error.kind(), ObjectStoreErrorKind::NotFound);
    }

    #[tokio::test]
    async fn absent_or_malformed_response_etags_fail_integrity_validation() {
        let server = FakeServer::start(vec![
            ranged_response_without_etag(b"value"),
            ranged_response("not-quoted", b"value"),
            response(HttpStatusCode::CREATED, None, Body::empty()),
        ])
        .await;
        let store = development_store(&server, limits(32, 32));

        for _ in 0..2 {
            let error = store.get(&key()).await.expect_err("invalid download ETag");
            assert_eq!(error.kind(), ObjectStoreErrorKind::Integrity);
        }
        let error = store
            .put_if_absent(&key(), Bytes::from_static(b"value"))
            .await
            .expect_err("missing upload ETag");
        assert_eq!(error.kind(), ObjectStoreErrorKind::Integrity);
    }

    fn ranged_response_without_etag(bytes: &'static [u8]) -> Response<Body> {
        Response::builder()
            .status(HttpStatusCode::PARTIAL_CONTENT)
            .header("content-length", bytes.len())
            .header(
                "content-range",
                format!("bytes 0-{}/{}", bytes.len() - 1, bytes.len()),
            )
            .body(Body::from(bytes))
            .expect("fake ranged response")
    }

    #[tokio::test]
    async fn oversized_stream_is_cut_off_without_buffering_the_full_response() {
        const CHUNKS: usize = 20;
        let polled = Arc::new(AtomicUsize::new(0));
        let stream_polled = Arc::clone(&polled);
        let chunks = stream::unfold(0, move |index| {
            let stream_polled = Arc::clone(&stream_polled);
            async move {
                if index == CHUNKS {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
                stream_polled.fetch_add(1, Ordering::SeqCst);
                Some((
                    Ok::<Bytes, Infallible>(Bytes::from_static(b"four")),
                    index + 1,
                ))
            }
        });
        let streaming = Response::builder()
            .status(HttpStatusCode::OK)
            .header("etag", "\"streamed\"")
            .body(Body::from_stream(chunks))
            .expect("streaming response");
        let mut server = FakeServer::start(vec![streaming]).await;
        let store = development_store(&server, limits(5, 32));

        let error = tokio::time::timeout(Duration::from_secs(1), store.get(&key()))
            .await
            .expect("bounded read completes before full body")
            .expect_err("oversized body");
        assert!(matches!(
            error,
            ObjectStoreError::PayloadTooLarge {
                operation: ObjectStoreOperation::Get,
                observed: 8,
                limit: 5,
                ..
            }
        ));
        let request = server.request().await;
        assert_eq!(request.headers["range"], "bytes=0-5");
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert!(
            polled.load(Ordering::SeqCst) < CHUNKS,
            "the fake response must be dropped before all chunks are polled"
        );
    }

    #[tokio::test]
    async fn write_limit_is_enforced_before_a_request_is_sent() {
        let mut server = FakeServer::start(vec![]).await;
        let store = development_store(&server, limits(32, 4));
        let error = store
            .put_if_absent(&key(), Bytes::from_static(b"large"))
            .await
            .expect_err("write bound");
        assert_eq!(error.kind(), ObjectStoreErrorKind::PayloadTooLarge);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), server.requests.recv())
                .await
                .is_err(),
            "oversized writes must not reach Azure"
        );
    }
}
