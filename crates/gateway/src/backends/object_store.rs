//! Provider-neutral exact-key object storage with compare-and-swap writes.
//!
//! This is the narrow durable-storage foundation selected by ADR 0062. It is
//! intentionally not a desired-state store: publication, revision manifests,
//! signatures, and idempotency belong to layers above this contract.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;

use super::{BackendFailure, FailureCategory};

/// Maximum encoded key length accepted by every object-store implementation.
pub const MAX_OBJECT_KEY_BYTES: usize = 1_024;

/// An exact object key, validated before it reaches a provider adapter.
///
/// Keys are relative slash-separated ASCII paths. Empty and dot segments are
/// refused so adapters never have to normalize two spellings to one provider
/// key. The type does not assign meaning to segments.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectKey(String);

impl ObjectKey {
    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidObjectKey> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidObjectKey::Empty);
        }
        if value.len() > MAX_OBJECT_KEY_BYTES {
            return Err(InvalidObjectKey::TooLong {
                limit: MAX_OBJECT_KEY_BYTES,
            });
        }
        if value.starts_with('/') || value.ends_with('/') {
            return Err(InvalidObjectKey::BoundarySlash);
        }
        for segment in value.split('/') {
            if segment.is_empty() {
                return Err(InvalidObjectKey::EmptySegment);
            }
            if matches!(segment, "." | "..") {
                return Err(InvalidObjectKey::DotSegment);
            }
        }
        if let Some((index, byte)) = value.bytes().enumerate().find(|(_, byte)| {
            !byte.is_ascii_alphanumeric() && !matches!(byte, b'/' | b'-' | b'_' | b'.')
        }) {
            return Err(InvalidObjectKey::InvalidByte { index, byte });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidObjectKey {
    #[error("object key must not be empty")]
    Empty,
    #[error("object key exceeds {limit} bytes")]
    TooLong { limit: usize },
    #[error("object key must not begin or end with a slash")]
    BoundarySlash,
    #[error("object key must not contain an empty path segment")]
    EmptySegment,
    #[error("object key must not contain `.` or `..` path segments")]
    DotSegment,
    #[error("object key contains unsupported byte 0x{byte:02x} at index {index}")]
    InvalidByte { index: usize, byte: u8 },
}

/// A provider-issued concurrency token.
///
/// Consumers may compare and return the token, but cannot order it or infer a
/// content digest from it. Adapters must preserve the provider token exactly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectVersion(String);

impl ObjectVersion {
    /// Wrap a provider token without assigning it ordering or hash semantics.
    pub fn opaque(token: impl Into<String>) -> Result<Self, InvalidObjectVersion> {
        let token = token.into();
        if token.is_empty() {
            return Err(InvalidObjectVersion);
        }
        Ok(Self(token))
    }

    pub fn as_opaque(&self) -> &str {
        &self.0
    }

    fn memory(sequence: u64) -> Self {
        Self(format!("memory-{sequence:020}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("object version must not be empty")]
pub struct InvalidObjectVersion;

/// Exact bytes and the version that identified them at read time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectValue {
    pub bytes: Bytes,
    pub version: ObjectVersion,
}

/// Per-operation payload bounds enforced by an implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStoreLimits {
    max_read_bytes: NonZeroUsize,
    max_write_bytes: NonZeroUsize,
}

impl ObjectStoreLimits {
    pub const fn new(max_read_bytes: NonZeroUsize, max_write_bytes: NonZeroUsize) -> Self {
        Self {
            max_read_bytes,
            max_write_bytes,
        }
    }

    pub const fn for_max_object_bytes(max_object_bytes: NonZeroUsize) -> Self {
        Self::new(max_object_bytes, max_object_bytes)
    }

    pub const fn max_read_bytes(self) -> usize {
        self.max_read_bytes.get()
    }

    pub const fn max_write_bytes(self) -> usize {
        self.max_write_bytes.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreOperation {
    Get,
    PutIfAbsent,
    ReplaceIfVersion,
}

impl fmt::Display for ObjectStoreOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Get => "get",
            Self::PutIfAbsent => "put_if_absent",
            Self::ReplaceIfVersion => "replace_if_version",
        })
    }
}

/// Stable classifications callers use without parsing provider messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreErrorKind {
    NotFound,
    PreconditionFailed,
    Unavailable,
    Integrity,
    PayloadTooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ObjectStoreError {
    #[error("object `{key}` was not found")]
    NotFound { key: ObjectKey },
    #[error("object `{key}` failed the {operation} precondition")]
    PreconditionFailed {
        key: ObjectKey,
        operation: ObjectStoreOperation,
    },
    #[error("object store unavailable during {operation}: {message}")]
    Unavailable {
        operation: ObjectStoreOperation,
        message: String,
    },
    #[error("object `{key}` failed integrity validation: {message}")]
    Integrity { key: ObjectKey, message: String },
    #[error(
        "object `{key}` {operation} payload is {observed} bytes, exceeding the {limit}-byte limit"
    )]
    PayloadTooLarge {
        key: ObjectKey,
        operation: ObjectStoreOperation,
        observed: usize,
        limit: usize,
    },
}

impl ObjectStoreError {
    pub const fn kind(&self) -> ObjectStoreErrorKind {
        match self {
            Self::NotFound { .. } => ObjectStoreErrorKind::NotFound,
            Self::PreconditionFailed { .. } => ObjectStoreErrorKind::PreconditionFailed,
            Self::Unavailable { .. } => ObjectStoreErrorKind::Unavailable,
            Self::Integrity { .. } => ObjectStoreErrorKind::Integrity,
            Self::PayloadTooLarge { .. } => ObjectStoreErrorKind::PayloadTooLarge,
        }
    }

    pub fn unavailable(operation: ObjectStoreOperation, message: impl Into<String>) -> Self {
        Self::Unavailable {
            operation,
            message: message.into(),
        }
    }

    pub fn integrity(key: ObjectKey, message: impl Into<String>) -> Self {
        Self::Integrity {
            key,
            message: message.into(),
        }
    }
}

impl BackendFailure for ObjectStoreError {
    fn category(&self) -> FailureCategory {
        match self {
            Self::NotFound { .. } => FailureCategory::NotFound,
            Self::PreconditionFailed { .. } => FailureCategory::Conflict,
            Self::Unavailable { .. } => FailureCategory::Unavailable,
            Self::Integrity { .. } => FailureCategory::Corrupt,
            Self::PayloadTooLarge { .. } => FailureCategory::Invalid,
        }
    }
}

/// Strong exact-key reads and native conditional writes.
#[async_trait]
pub trait ObjectStore: Send + Sync {
    fn name(&self) -> &'static str;

    fn limits(&self) -> ObjectStoreLimits;

    async fn get(&self, key: &ObjectKey) -> Result<ObjectValue, ObjectStoreError>;

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<ObjectVersion, ObjectStoreError>;

    async fn replace_if_version(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
        expected: &ObjectVersion,
    ) -> Result<ObjectVersion, ObjectStoreError>;
}

#[derive(Debug, Clone)]
pub struct InMemoryObjectStore {
    limits: ObjectStoreLimits,
    state: Arc<Mutex<MemoryState>>,
}

#[derive(Debug, Default)]
struct MemoryState {
    next_sequence: u64,
    objects: BTreeMap<ObjectKey, ObjectValue>,
}

impl InMemoryObjectStore {
    pub fn new(limits: ObjectStoreLimits) -> Self {
        Self {
            limits,
            state: Arc::new(Mutex::new(MemoryState::default())),
        }
    }

    fn check_payload(
        &self,
        key: &ObjectKey,
        operation: ObjectStoreOperation,
        observed: usize,
        limit: usize,
    ) -> Result<(), ObjectStoreError> {
        if observed > limit {
            return Err(ObjectStoreError::PayloadTooLarge {
                key: key.clone(),
                operation,
                observed,
                limit,
            });
        }
        Ok(())
    }

    fn lock(
        &self,
        key: &ObjectKey,
    ) -> Result<std::sync::MutexGuard<'_, MemoryState>, ObjectStoreError> {
        self.state
            .lock()
            .map_err(|_| ObjectStoreError::integrity(key.clone(), "in-memory state lock poisoned"))
    }

    fn issue_version(
        state: &mut MemoryState,
        key: &ObjectKey,
    ) -> Result<ObjectVersion, ObjectStoreError> {
        state.next_sequence = state.next_sequence.checked_add(1).ok_or_else(|| {
            ObjectStoreError::integrity(key.clone(), "version sequence exhausted")
        })?;
        Ok(ObjectVersion::memory(state.next_sequence))
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    fn name(&self) -> &'static str {
        "in-memory"
    }

    fn limits(&self) -> ObjectStoreLimits {
        self.limits
    }

    async fn get(&self, key: &ObjectKey) -> Result<ObjectValue, ObjectStoreError> {
        let value = self
            .lock(key)?
            .objects
            .get(key)
            .cloned()
            .ok_or_else(|| ObjectStoreError::NotFound { key: key.clone() })?;
        self.check_payload(
            key,
            ObjectStoreOperation::Get,
            value.bytes.len(),
            self.limits.max_read_bytes(),
        )?;
        Ok(value)
    }

    async fn put_if_absent(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
    ) -> Result<ObjectVersion, ObjectStoreError> {
        self.check_payload(
            key,
            ObjectStoreOperation::PutIfAbsent,
            bytes.len(),
            self.limits.max_write_bytes(),
        )?;
        let mut state = self.lock(key)?;
        if state.objects.contains_key(key) {
            return Err(ObjectStoreError::PreconditionFailed {
                key: key.clone(),
                operation: ObjectStoreOperation::PutIfAbsent,
            });
        }
        let version = Self::issue_version(&mut state, key)?;
        state.objects.insert(
            key.clone(),
            ObjectValue {
                bytes,
                version: version.clone(),
            },
        );
        Ok(version)
    }

    async fn replace_if_version(
        &self,
        key: &ObjectKey,
        bytes: Bytes,
        expected: &ObjectVersion,
    ) -> Result<ObjectVersion, ObjectStoreError> {
        self.check_payload(
            key,
            ObjectStoreOperation::ReplaceIfVersion,
            bytes.len(),
            self.limits.max_write_bytes(),
        )?;
        let mut state = self.lock(key)?;
        if state
            .objects
            .get(key)
            .is_none_or(|current| current.version != *expected)
        {
            return Err(ObjectStoreError::PreconditionFailed {
                key: key.clone(),
                operation: ObjectStoreOperation::ReplaceIfVersion,
            });
        }
        let version = Self::issue_version(&mut state, key)?;
        state.objects.insert(
            key.clone(),
            ObjectValue {
                bytes,
                version: version.clone(),
            },
        );
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Barrier;

    fn key() -> ObjectKey {
        ObjectKey::parse("environments/test/head.json").expect("valid key")
    }

    fn limits(max: usize) -> ObjectStoreLimits {
        ObjectStoreLimits::for_max_object_bytes(NonZeroUsize::new(max).expect("non-zero limit"))
    }

    #[tokio::test]
    async fn first_write_is_read_with_the_returned_version() {
        let store = InMemoryObjectStore::new(limits(32));
        let version = store
            .put_if_absent(&key(), Bytes::from_static(b"first"))
            .await
            .expect("first write");

        let value = store.get(&key()).await.expect("stored value");
        assert_eq!(value.bytes, Bytes::from_static(b"first"));
        assert_eq!(value.version, version);
    }

    #[tokio::test]
    async fn duplicate_create_is_a_precondition_failure_and_preserves_bytes() {
        let store = InMemoryObjectStore::new(limits(32));
        let original = store
            .put_if_absent(&key(), Bytes::from_static(b"first"))
            .await
            .expect("first write");

        let error = store
            .put_if_absent(&key(), Bytes::from_static(b"second"))
            .await
            .expect_err("duplicate create must lose");
        assert_eq!(error.kind(), ObjectStoreErrorKind::PreconditionFailed);
        let value = store.get(&key()).await.expect("original remains");
        assert_eq!(value.bytes, Bytes::from_static(b"first"));
        assert_eq!(value.version, original);
    }

    #[tokio::test]
    async fn current_version_replaces_and_issues_a_new_version() {
        let store = InMemoryObjectStore::new(limits(32));
        let original = store
            .put_if_absent(&key(), Bytes::from_static(b"first"))
            .await
            .expect("first write");
        assert_eq!(original.as_opaque(), "memory-00000000000000000001");

        let replacement = store
            .replace_if_version(&key(), Bytes::from_static(b"second"), &original)
            .await
            .expect("CAS replacement");
        assert_ne!(
            replacement, original,
            "successful writes advance the version"
        );
        assert_eq!(replacement.as_opaque(), "memory-00000000000000000002");
        let value = store.get(&key()).await.expect("replacement");
        assert_eq!(value.bytes, Bytes::from_static(b"second"));
        assert_eq!(value.version, replacement);
    }

    #[tokio::test]
    async fn stale_version_conflicts_and_preserves_the_winner() {
        let store = InMemoryObjectStore::new(limits(32));
        let stale = store
            .put_if_absent(&key(), Bytes::from_static(b"first"))
            .await
            .expect("first write");
        let winner = store
            .replace_if_version(&key(), Bytes::from_static(b"second"), &stale)
            .await
            .expect("first replacement");

        let error = store
            .replace_if_version(&key(), Bytes::from_static(b"third"), &stale)
            .await
            .expect_err("stale replacement must lose");
        assert_eq!(error.kind(), ObjectStoreErrorKind::PreconditionFailed);
        let value = store.get(&key()).await.expect("winner remains");
        assert_eq!(value.bytes, Bytes::from_static(b"second"));
        assert_eq!(value.version, winner);
    }

    #[tokio::test]
    async fn missing_object_is_explicitly_not_found() {
        let store = InMemoryObjectStore::new(limits(32));
        let error = store.get(&key()).await.expect_err("missing object");
        assert_eq!(error.kind(), ObjectStoreErrorKind::NotFound);
        assert_eq!(error.category(), FailureCategory::NotFound);
    }

    #[tokio::test]
    async fn read_and_write_payload_bounds_are_refused() {
        let store = InMemoryObjectStore::new(ObjectStoreLimits::new(
            NonZeroUsize::new(3).expect("non-zero read limit"),
            NonZeroUsize::new(4).expect("non-zero write limit"),
        ));
        store
            .put_if_absent(&key(), Bytes::from_static(b"four"))
            .await
            .expect("within write bound");

        let read = store.get(&key()).await.expect_err("over read bound");
        assert!(matches!(
            read,
            ObjectStoreError::PayloadTooLarge {
                operation: ObjectStoreOperation::Get,
                observed: 4,
                limit: 3,
                ..
            }
        ));

        let other = ObjectKey::parse("objects/other").expect("valid key");
        let write = store
            .put_if_absent(&other, Bytes::from_static(b"large"))
            .await
            .expect_err("over write bound");
        assert!(matches!(
            write,
            ObjectStoreError::PayloadTooLarge {
                operation: ObjectStoreOperation::PutIfAbsent,
                observed: 5,
                limit: 4,
                ..
            }
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_have_exactly_one_winner() {
        const WRITERS: usize = 8;
        let store = Arc::new(InMemoryObjectStore::new(limits(32)));
        let original = store
            .put_if_absent(&key(), Bytes::from_static(b"first"))
            .await
            .expect("first write");
        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut writers = Vec::new();

        for index in 0..WRITERS {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            let expected = original.clone();
            writers.push(tokio::spawn(async move {
                barrier.wait().await;
                store
                    .replace_if_version(&key(), Bytes::from(format!("writer-{index}")), &expected)
                    .await
            }));
        }

        let mut wins = 0;
        let mut conflicts = 0;
        for writer in writers {
            match writer.await.expect("writer task") {
                Ok(_) => wins += 1,
                Err(error) if error.kind() == ObjectStoreErrorKind::PreconditionFailed => {
                    conflicts += 1;
                }
                Err(error) => panic!("unexpected writer error: {error}"),
            }
        }
        assert_eq!(wins, 1);
        assert_eq!(conflicts, WRITERS - 1);
        assert_ne!(store.get(&key()).await.expect("winner").version, original);
    }

    #[test]
    fn backend_failure_mapping_preserves_the_explicit_taxonomy() {
        let key = key();
        let unavailable = ObjectStoreError::unavailable(ObjectStoreOperation::Get, "offline");
        let integrity = ObjectStoreError::integrity(key.clone(), "digest mismatch");
        assert_eq!(unavailable.kind(), ObjectStoreErrorKind::Unavailable);
        assert_eq!(unavailable.category(), FailureCategory::Unavailable);
        assert!(unavailable.retryable());
        assert_eq!(integrity.kind(), ObjectStoreErrorKind::Integrity);
        assert_eq!(integrity.category(), FailureCategory::Corrupt);
        assert!(!integrity.retryable());
    }
}
