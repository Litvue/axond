//! Immutable object-store publication selected by ADR 0062.
//!
//! A publication uploads every content-addressed object with create-only
//! semantics and then performs exactly one conditional write to an environment
//! head. The head is the only mutable object. Store versions remain opaque CAS
//! tokens; SHA-256 digests identify content and are never used as CAS tokens.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::backends::object_store::{
    ObjectKey, ObjectStore, ObjectStoreError, ObjectStoreErrorKind, ObjectVersion,
};

use super::{Checksum, IdempotencyKey, InvalidChecksum, InvalidIdempotencyKey};

const HEAD_SCHEMA_VERSION: u64 = 1;
const MANIFEST_SCHEMA_VERSION: u64 = 1;
const HEAD_INTEGRITY_ALGORITHM: &str = "sha256";
const HEAD_INTEGRITY_DOMAIN: &[u8] = b"axond.environment-head.v1\0";
pub const MAX_HEAD_DOCUMENT_BYTES: usize = 4 * 1024;
pub const MAX_REVISION_MANIFEST_BYTES: usize = 1024 * 1024;
pub const MAX_MANIFEST_OBJECTS: usize = 4096;

/// A slash-free environment name used as one exact object-key segment.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnvironmentId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidEnvironmentId {
    #[error("environment identifier must not be empty")]
    Empty,
    #[error("environment identifier is {length} bytes, over the {max}-byte limit")]
    TooLong { length: usize, max: usize },
    #[error("environment identifier must begin and end with a lowercase ASCII letter or digit")]
    Boundary,
    #[error("environment identifier contains unsupported byte 0x{byte:02x} at index {index}")]
    InvalidByte { index: usize, byte: u8 },
}

impl EnvironmentId {
    pub const MAX_LEN: usize = 128;

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidEnvironmentId> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidEnvironmentId::Empty);
        }
        if value.len() > Self::MAX_LEN {
            return Err(InvalidEnvironmentId::TooLong {
                length: value.len(),
                max: Self::MAX_LEN,
            });
        }
        let bytes = value.as_bytes();
        let boundary = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
        if !boundary(bytes[0]) || !boundary(bytes[bytes.len() - 1]) {
            return Err(InvalidEnvironmentId::Boundary);
        }
        if let Some((index, byte)) = bytes.iter().copied().enumerate().find(|(_, byte)| {
            !byte.is_ascii_lowercase()
                && !byte.is_ascii_digit()
                && !matches!(byte, b'-' | b'_' | b'.')
        }) {
            return Err(InvalidEnvironmentId::InvalidByte { index, byte });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EnvironmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn digest_segment(digest: Checksum) -> String {
    let mut segment = String::with_capacity(64);
    for byte in digest.as_bytes() {
        use fmt::Write as _;
        write!(&mut segment, "{byte:02x}").expect("writing to a String cannot fail");
    }
    segment
}

pub fn environment_head_key(environment: &EnvironmentId) -> ObjectKey {
    ObjectKey::parse(format!("environments/{environment}/head.json"))
        .expect("a validated environment always forms a valid object key")
}

pub fn revision_manifest_key(digest: Checksum) -> ObjectKey {
    ObjectKey::parse(format!(
        "revisions/{}/manifest.cbor",
        digest_segment(digest)
    ))
    .expect("a lowercase digest always forms a valid object key")
}

pub fn namespace_resource_key(digest: Checksum) -> ObjectKey {
    ObjectKey::parse(format!(
        "resources/namespaces/{}.cbor",
        digest_segment(digest)
    ))
    .expect("a lowercase digest always forms a valid object key")
}

pub fn deployment_resource_key(digest: Checksum) -> ObjectKey {
    ObjectKey::parse(format!(
        "resources/deployment/{}.cbor",
        digest_segment(digest)
    ))
    .expect("a lowercase digest always forms a valid object key")
}

pub fn secret_key(digest: Checksum) -> ObjectKey {
    ObjectKey::parse(format!("secrets/{}.bin", digest_segment(digest)))
        .expect("a lowercase digest always forms a valid object key")
}

/// The bounded mutable document at `environments/{environment}/head.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadDocument {
    active_revision: Checksum,
    sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HeadDocumentError {
    #[error("head document is {observed} bytes, over the {limit}-byte limit")]
    Oversized { observed: usize, limit: usize },
    #[error("head document is malformed JSON")]
    Malformed,
    #[error("head schema version {found} is not supported")]
    UnknownSchema { found: u64 },
    #[error("head active revision digest {source}")]
    InvalidDigest { source: InvalidChecksum },
    #[error("head sequence must be greater than zero")]
    ZeroSequence,
    #[error("head integrity algorithm `{found}` is not supported")]
    UnknownIntegrityAlgorithm { found: String },
    #[error("head integrity digest {source}")]
    InvalidIntegrityDigest { source: InvalidChecksum },
    #[error("head integrity digest does not match its publication metadata")]
    IntegrityMismatch,
    #[error("head JSON is not in its deterministic encoding")]
    NonCanonical,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireHead {
    schema_version: u64,
    active_revision: String,
    sequence: u64,
    integrity: WireIntegrity,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireIntegrity {
    algorithm: String,
    digest: String,
}

impl HeadDocument {
    pub fn new(active_revision: Checksum, sequence: u64) -> Result<Self, HeadDocumentError> {
        if sequence == 0 {
            return Err(HeadDocumentError::ZeroSequence);
        }
        Ok(Self {
            active_revision,
            sequence,
        })
    }

    pub const fn active_revision(&self) -> Checksum {
        self.active_revision
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    fn integrity_digest(active_revision: Checksum, sequence: u64) -> Checksum {
        let mut bytes = Vec::with_capacity(HEAD_INTEGRITY_DOMAIN.len() + 32 + 16);
        bytes.extend_from_slice(HEAD_INTEGRITY_DOMAIN);
        bytes.extend_from_slice(&HEAD_SCHEMA_VERSION.to_be_bytes());
        bytes.extend_from_slice(active_revision.as_bytes());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        Checksum::of(&bytes)
    }

    pub fn encode(&self) -> Bytes {
        let wire = WireHead {
            schema_version: HEAD_SCHEMA_VERSION,
            active_revision: self.active_revision.to_string(),
            sequence: self.sequence,
            integrity: WireIntegrity {
                algorithm: HEAD_INTEGRITY_ALGORITHM.to_owned(),
                digest: Self::integrity_digest(self.active_revision, self.sequence).to_string(),
            },
        };
        Bytes::from(serde_json::to_vec(&wire).expect("head fields always serialize"))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, HeadDocumentError> {
        if bytes.len() > MAX_HEAD_DOCUMENT_BYTES {
            return Err(HeadDocumentError::Oversized {
                observed: bytes.len(),
                limit: MAX_HEAD_DOCUMENT_BYTES,
            });
        }
        let wire: WireHead =
            serde_json::from_slice(bytes).map_err(|_| HeadDocumentError::Malformed)?;
        if wire.schema_version != HEAD_SCHEMA_VERSION {
            return Err(HeadDocumentError::UnknownSchema {
                found: wire.schema_version,
            });
        }
        if wire.sequence == 0 {
            return Err(HeadDocumentError::ZeroSequence);
        }
        if wire.integrity.algorithm != HEAD_INTEGRITY_ALGORITHM {
            return Err(HeadDocumentError::UnknownIntegrityAlgorithm {
                found: wire.integrity.algorithm,
            });
        }
        let active_revision = Checksum::parse(&wire.active_revision)
            .map_err(|source| HeadDocumentError::InvalidDigest { source })?;
        let integrity = Checksum::parse(&wire.integrity.digest)
            .map_err(|source| HeadDocumentError::InvalidIntegrityDigest { source })?;
        if integrity != Self::integrity_digest(active_revision, wire.sequence) {
            return Err(HeadDocumentError::IntegrityMismatch);
        }
        let document = Self {
            active_revision,
            sequence: wire.sequence,
        };
        if document.encode().as_ref() != bytes {
            return Err(HeadDocumentError::NonCanonical);
        }
        Ok(document)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ImmutableObjectKind {
    NamespaceResource,
    DeploymentResource,
    Secret,
}

impl ImmutableObjectKind {
    const fn tag(self) -> u64 {
        match self {
            Self::NamespaceResource => 0,
            Self::DeploymentResource => 1,
            Self::Secret => 2,
        }
    }

    fn from_tag(tag: u64) -> Result<Self, RevisionManifestError> {
        match tag {
            0 => Ok(Self::NamespaceResource),
            1 => Ok(Self::DeploymentResource),
            2 => Ok(Self::Secret),
            _ => Err(RevisionManifestError::Malformed),
        }
    }

    fn key(self, digest: Checksum) -> ObjectKey {
        match self {
            Self::NamespaceResource => namespace_resource_key(digest),
            Self::DeploymentResource => deployment_resource_key(digest),
            Self::Secret => secret_key(digest),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmutableObject {
    pub kind: ImmutableObjectKind,
    pub bytes: Bytes,
}

impl ImmutableObject {
    pub fn digest(&self) -> Checksum {
        Checksum::of(&self.bytes)
    }

    pub fn key(&self) -> ObjectKey {
        self.kind.key(self.digest())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ImmutableReference {
    kind: ImmutableObjectKind,
    digest: Checksum,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BlobRevisionManifest {
    parent: Option<Checksum>,
    sequence: u64,
    idempotency_key: IdempotencyKey,
    desired_state_checksum: Checksum,
    objects: Vec<ImmutableReference>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RevisionManifestError {
    #[error("revision manifest is {observed} bytes, over the {limit}-byte limit")]
    Oversized { observed: usize, limit: usize },
    #[error("revision manifest is malformed or not deterministic CBOR")]
    Malformed,
    #[error("revision manifest schema version {found} is not supported")]
    UnknownSchema { found: u64 },
    #[error("revision manifest idempotency key {source}")]
    InvalidIdempotencyKey { source: InvalidIdempotencyKey },
    #[error("revision manifest sequence must be greater than zero")]
    ZeroSequence,
    #[error("revision manifest sequence {actual} does not match the linked sequence {expected}")]
    SequenceMismatch { expected: u64, actual: u64 },
    #[error("revision manifest sequence 1 must not name a parent")]
    ParentBeforeFirstSequence,
    #[error(
        "revision manifest contains {observed} object references, over the {limit}-object limit"
    )]
    TooManyObjects { observed: usize, limit: usize },
}

impl BlobRevisionManifest {
    fn encode(&self) -> Result<Bytes, RevisionManifestError> {
        if self.sequence == 0 {
            return Err(RevisionManifestError::ZeroSequence);
        }
        if self.objects.len() > MAX_MANIFEST_OBJECTS {
            return Err(RevisionManifestError::TooManyObjects {
                observed: self.objects.len(),
                limit: MAX_MANIFEST_OBJECTS,
            });
        }
        let mut bytes = Vec::with_capacity(192 + self.objects.len() * 40);
        cbor_array(&mut bytes, 6);
        cbor_unsigned(&mut bytes, MANIFEST_SCHEMA_VERSION);
        match self.parent {
            Some(parent) => cbor_bytes(&mut bytes, parent.as_bytes()),
            None => bytes.push(0xf6),
        }
        cbor_unsigned(&mut bytes, self.sequence);
        cbor_text(&mut bytes, self.idempotency_key.as_str());
        cbor_bytes(&mut bytes, self.desired_state_checksum.as_bytes());
        cbor_array(&mut bytes, self.objects.len() as u64);
        for object in &self.objects {
            cbor_array(&mut bytes, 2);
            cbor_unsigned(&mut bytes, object.kind.tag());
            cbor_bytes(&mut bytes, object.digest.as_bytes());
        }
        if bytes.len() > MAX_REVISION_MANIFEST_BYTES {
            return Err(RevisionManifestError::Oversized {
                observed: bytes.len(),
                limit: MAX_REVISION_MANIFEST_BYTES,
            });
        }
        Ok(Bytes::from(bytes))
    }

    fn decode(bytes: &[u8]) -> Result<Self, RevisionManifestError> {
        if bytes.len() > MAX_REVISION_MANIFEST_BYTES {
            return Err(RevisionManifestError::Oversized {
                observed: bytes.len(),
                limit: MAX_REVISION_MANIFEST_BYTES,
            });
        }
        let mut cursor = CborCursor::new(bytes);
        cursor.array_exact(6)?;
        let schema = cursor.unsigned()?;
        if schema != MANIFEST_SCHEMA_VERSION {
            return Err(RevisionManifestError::UnknownSchema { found: schema });
        }
        let parent = cursor.optional_digest()?;
        let sequence = cursor.unsigned()?;
        if sequence == 0 {
            return Err(RevisionManifestError::ZeroSequence);
        }
        let idempotency_key = IdempotencyKey::parse(cursor.text()?)
            .map_err(|source| RevisionManifestError::InvalidIdempotencyKey { source })?;
        let desired_state_checksum = cursor.digest()?;
        let object_count = cursor.array_len()?;
        if object_count > MAX_MANIFEST_OBJECTS {
            return Err(RevisionManifestError::TooManyObjects {
                observed: object_count,
                limit: MAX_MANIFEST_OBJECTS,
            });
        }
        let mut objects = Vec::with_capacity(object_count);
        for _ in 0..object_count {
            cursor.array_exact(2)?;
            objects.push(ImmutableReference {
                kind: ImmutableObjectKind::from_tag(cursor.unsigned()?)?,
                digest: cursor.digest()?,
            });
        }
        if !cursor.is_empty() {
            return Err(RevisionManifestError::Malformed);
        }
        let manifest = Self {
            parent,
            sequence,
            idempotency_key,
            desired_state_checksum,
            objects,
        };
        if manifest.encode()?.as_ref() != bytes {
            return Err(RevisionManifestError::Malformed);
        }
        Ok(manifest)
    }
}

fn cbor_header(bytes: &mut Vec<u8>, major: u8, value: u64) {
    match value {
        0..=23 => bytes.push((major << 5) | value as u8),
        24..=0xff => bytes.extend_from_slice(&[(major << 5) | 24, value as u8]),
        0x100..=0xffff => {
            bytes.push((major << 5) | 25);
            bytes.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            bytes.push((major << 5) | 26);
            bytes.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            bytes.push((major << 5) | 27);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
    }
}

fn cbor_unsigned(bytes: &mut Vec<u8>, value: u64) {
    cbor_header(bytes, 0, value);
}

fn cbor_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    cbor_header(bytes, 2, value.len() as u64);
    bytes.extend_from_slice(value);
}

fn cbor_text(bytes: &mut Vec<u8>, value: &str) {
    cbor_header(bytes, 3, value.len() as u64);
    bytes.extend_from_slice(value.as_bytes());
}

fn cbor_array(bytes: &mut Vec<u8>, length: u64) {
    cbor_header(bytes, 4, length);
}

struct CborCursor<'a> {
    rest: &'a [u8],
}

impl<'a> CborCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { rest: bytes }
    }

    fn is_empty(&self) -> bool {
        self.rest.is_empty()
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], RevisionManifestError> {
        if self.rest.len() < count {
            return Err(RevisionManifestError::Malformed);
        }
        let (taken, rest) = self.rest.split_at(count);
        self.rest = rest;
        Ok(taken)
    }

    fn byte(&mut self) -> Result<u8, RevisionManifestError> {
        Ok(self.take(1)?[0])
    }

    fn header(&mut self, expected_major: u8) -> Result<u64, RevisionManifestError> {
        let first = self.byte()?;
        if first >> 5 != expected_major {
            return Err(RevisionManifestError::Malformed);
        }
        let additional = first & 0x1f;
        let value = match additional {
            value @ 0..=23 => u64::from(value),
            24 => u64::from(self.byte()?),
            25 => u64::from(u16::from_be_bytes(
                self.take(2)?.try_into().expect("two bytes were taken"),
            )),
            26 => u64::from(u32::from_be_bytes(
                self.take(4)?.try_into().expect("four bytes were taken"),
            )),
            27 => u64::from_be_bytes(self.take(8)?.try_into().expect("eight bytes were taken")),
            _ => return Err(RevisionManifestError::Malformed),
        };
        let shortest = match additional {
            0..=23 => true,
            24 => value >= 24,
            25 => value > 0xff,
            26 => value > 0xffff,
            27 => value > 0xffff_ffff,
            _ => false,
        };
        if !shortest {
            return Err(RevisionManifestError::Malformed);
        }
        Ok(value)
    }

    fn unsigned(&mut self) -> Result<u64, RevisionManifestError> {
        self.header(0)
    }

    fn array_len(&mut self) -> Result<usize, RevisionManifestError> {
        usize::try_from(self.header(4)?).map_err(|_| RevisionManifestError::Malformed)
    }

    fn array_exact(&mut self, expected: usize) -> Result<(), RevisionManifestError> {
        if self.array_len()? != expected {
            return Err(RevisionManifestError::Malformed);
        }
        Ok(())
    }

    fn bytes(&mut self) -> Result<&'a [u8], RevisionManifestError> {
        let length =
            usize::try_from(self.header(2)?).map_err(|_| RevisionManifestError::Malformed)?;
        self.take(length)
    }

    fn text(&mut self) -> Result<&'a str, RevisionManifestError> {
        std::str::from_utf8({
            let length =
                usize::try_from(self.header(3)?).map_err(|_| RevisionManifestError::Malformed)?;
            self.take(length)?
        })
        .map_err(|_| RevisionManifestError::Malformed)
    }

    fn digest(&mut self) -> Result<Checksum, RevisionManifestError> {
        let digest: [u8; 32] = self
            .bytes()?
            .try_into()
            .map_err(|_| RevisionManifestError::Malformed)?;
        Ok(Checksum::from_bytes(digest))
    }

    fn optional_digest(&mut self) -> Result<Option<Checksum>, RevisionManifestError> {
        if self.rest.first() == Some(&0xf6) {
            self.rest = &self.rest[1..];
            Ok(None)
        } else {
            self.digest().map(Some)
        }
    }
}

/// The caller's optimistic view of the environment head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedHead {
    Empty,
    Revision(Checksum),
}

impl ExpectedHead {
    fn matches(self, actual: Option<Checksum>) -> bool {
        matches!((self, actual), (Self::Empty, None))
            || matches!((self, actual), (Self::Revision(expected), Some(actual)) if expected == actual)
    }
}

impl fmt::Display for ExpectedHead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("an empty environment"),
            Self::Revision(digest) => write!(formatter, "revision {digest}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobPublicationRequest {
    pub expected: ExpectedHead,
    pub idempotency_key: IdempotencyKey,
    pub desired_state_checksum: Checksum,
    pub objects: Vec<ImmutableObject>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationOutcome {
    pub revision: Checksum,
    pub sequence: u64,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlobPublicationError {
    #[error(transparent)]
    Store(#[from] ObjectStoreError),
    #[error("stored environment head is invalid: {0}")]
    Head(HeadDocumentError),
    #[error("stored revision manifest is invalid: {0}")]
    Manifest(RevisionManifestError),
    #[error("immutable object `{key}` already exists with different bytes")]
    ImmutableCollision { key: ObjectKey },
    #[error("expected {expected}, but the active revision is {actual:?}")]
    Conflict {
        expected: ExpectedHead,
        actual: Option<Checksum>,
    },
    #[error("the final head write had an ambiguous unavailable outcome")]
    AmbiguousUnavailable,
    #[error("idempotency key `{key}` was already bound to a different desired-state checksum")]
    IdempotencyKeyReuse { key: IdempotencyKey },
    #[error("idempotency history exceeds the configured {limit}-revision bound")]
    HistoryLimitExceeded { limit: usize },
    #[error("environment head sequence is exhausted")]
    SequenceOverflow,
}

impl From<HeadDocumentError> for BlobPublicationError {
    fn from(error: HeadDocumentError) -> Self {
        Self::Head(error)
    }
}

impl From<RevisionManifestError> for BlobPublicationError {
    fn from(error: RevisionManifestError) -> Self {
        Self::Manifest(error)
    }
}

#[derive(Debug, Clone)]
struct ReadHead {
    document: HeadDocument,
    version: ObjectVersion,
}

/// Provider-neutral immutable publisher over an [`ObjectStore`].
pub struct BlobPublication<S> {
    store: Arc<S>,
    history_limit: NonZeroUsize,
}

impl<S> Clone for BlobPublication<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            history_limit: self.history_limit,
        }
    }
}

impl<S: ObjectStore> BlobPublication<S> {
    pub fn new(store: Arc<S>, history_limit: NonZeroUsize) -> Self {
        Self {
            store,
            history_limit,
        }
    }

    pub async fn publish(
        &self,
        environment: &EnvironmentId,
        request: BlobPublicationRequest,
    ) -> Result<PublicationOutcome, BlobPublicationError> {
        let original = self.read_head(environment).await?;

        if let Some(replay) = self
            .find_idempotency(
                original
                    .as_ref()
                    .map(|head| (head.document.active_revision, head.document.sequence)),
                &request.idempotency_key,
                request.desired_state_checksum,
            )
            .await?
        {
            return Ok(replay);
        }

        let actual = original.as_ref().map(|head| head.document.active_revision);
        if !request.expected.matches(actual) {
            return Err(BlobPublicationError::Conflict {
                expected: request.expected,
                actual,
            });
        }

        let sequence = original
            .as_ref()
            .map_or(Some(1), |head| head.document.sequence.checked_add(1))
            .ok_or(BlobPublicationError::SequenceOverflow)?;

        let mut objects = request
            .objects
            .iter()
            .map(|object| ImmutableReference {
                kind: object.kind,
                digest: object.digest(),
            })
            .collect::<Vec<_>>();
        objects.sort_unstable();
        objects.dedup();
        if objects.len() > MAX_MANIFEST_OBJECTS {
            return Err(RevisionManifestError::TooManyObjects {
                observed: objects.len(),
                limit: MAX_MANIFEST_OBJECTS,
            }
            .into());
        }

        for object in &request.objects {
            self.confirm_immutable(object.key(), object.bytes.clone())
                .await?;
        }

        let manifest = BlobRevisionManifest {
            parent: actual,
            sequence,
            idempotency_key: request.idempotency_key.clone(),
            desired_state_checksum: request.desired_state_checksum,
            objects,
        };
        let manifest_bytes = manifest.encode()?;
        let revision = Checksum::of(&manifest_bytes);
        self.confirm_immutable(revision_manifest_key(revision), manifest_bytes)
            .await?;

        let head = HeadDocument::new(revision, sequence)?;
        let head_key = environment_head_key(environment);
        let final_write = match &original {
            None => self.store.put_if_absent(&head_key, head.encode()).await,
            Some(original) => {
                self.store
                    .replace_if_version(&head_key, head.encode(), &original.version)
                    .await
            }
        };

        match final_write {
            Ok(_) => Ok(PublicationOutcome {
                revision,
                sequence,
                replayed: false,
            }),
            Err(error)
                if matches!(
                    error.kind(),
                    ObjectStoreErrorKind::PreconditionFailed | ObjectStoreErrorKind::Unavailable
                ) =>
            {
                self.reconcile_final_write(
                    environment,
                    &request,
                    original.as_ref(),
                    revision,
                    sequence,
                    error.kind(),
                )
                .await
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn read_head(
        &self,
        environment: &EnvironmentId,
    ) -> Result<Option<ReadHead>, BlobPublicationError> {
        match self.store.get(&environment_head_key(environment)).await {
            Ok(value) => Ok(Some(ReadHead {
                document: HeadDocument::decode(&value.bytes)?,
                version: value.version,
            })),
            Err(error) if error.kind() == ObjectStoreErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn read_manifest(
        &self,
        digest: Checksum,
    ) -> Result<BlobRevisionManifest, BlobPublicationError> {
        let key = revision_manifest_key(digest);
        let value = self.store.get(&key).await?;
        if Checksum::of(&value.bytes) != digest {
            return Err(ObjectStoreError::integrity(
                key,
                "manifest bytes do not match their content address",
            )
            .into());
        }
        Ok(BlobRevisionManifest::decode(&value.bytes)?)
    }

    async fn find_idempotency(
        &self,
        mut revision: Option<(Checksum, u64)>,
        key: &IdempotencyKey,
        desired_state_checksum: Checksum,
    ) -> Result<Option<PublicationOutcome>, BlobPublicationError> {
        let limit = self.history_limit.get();
        for _ in 0..limit {
            let Some((digest, expected_sequence)) = revision else {
                return Ok(None);
            };
            let manifest = self.read_manifest(digest).await?;
            if manifest.sequence != expected_sequence {
                return Err(RevisionManifestError::SequenceMismatch {
                    expected: expected_sequence,
                    actual: manifest.sequence,
                }
                .into());
            }
            if manifest.idempotency_key == *key {
                if manifest.desired_state_checksum != desired_state_checksum {
                    return Err(BlobPublicationError::IdempotencyKeyReuse { key: key.clone() });
                }
                return Ok(Some(PublicationOutcome {
                    revision: digest,
                    sequence: manifest.sequence,
                    replayed: true,
                }));
            }
            revision = match manifest.parent {
                Some(_) if manifest.sequence == 1 => {
                    return Err(RevisionManifestError::ParentBeforeFirstSequence.into());
                }
                Some(parent) => Some((parent, manifest.sequence - 1)),
                None => None,
            };
        }
        if revision.is_some() {
            Err(BlobPublicationError::HistoryLimitExceeded { limit })
        } else {
            Ok(None)
        }
    }

    async fn confirm_immutable(
        &self,
        key: ObjectKey,
        bytes: Bytes,
    ) -> Result<(), BlobPublicationError> {
        match self.store.put_if_absent(&key, bytes.clone()).await {
            Ok(_) => Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    ObjectStoreErrorKind::PreconditionFailed | ObjectStoreErrorKind::Unavailable
                ) =>
            {
                match self.store.get(&key).await {
                    Ok(existing) if existing.bytes == bytes => Ok(()),
                    Ok(_) => Err(BlobPublicationError::ImmutableCollision { key }),
                    Err(read_error)
                        if error.kind() == ObjectStoreErrorKind::Unavailable
                            && read_error.kind() == ObjectStoreErrorKind::NotFound =>
                    {
                        Err(error.into())
                    }
                    Err(read_error) => Err(read_error.into()),
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn reconcile_final_write(
        &self,
        environment: &EnvironmentId,
        request: &BlobPublicationRequest,
        original: Option<&ReadHead>,
        intended_revision: Checksum,
        intended_sequence: u64,
        failure: ObjectStoreErrorKind,
    ) -> Result<PublicationOutcome, BlobPublicationError> {
        let current = match self.read_head(environment).await {
            Ok(current) => current,
            Err(BlobPublicationError::Store(error))
                if failure == ObjectStoreErrorKind::Unavailable
                    && error.kind() == ObjectStoreErrorKind::Unavailable =>
            {
                return Err(BlobPublicationError::AmbiguousUnavailable);
            }
            Err(error) => return Err(error),
        };
        if current.as_ref().is_some_and(|head| {
            head.document.active_revision == intended_revision
                && head.document.sequence == intended_sequence
        }) {
            return Ok(PublicationOutcome {
                revision: intended_revision,
                sequence: intended_sequence,
                replayed: true,
            });
        }

        if let Some(replay) = self
            .find_idempotency(
                current
                    .as_ref()
                    .map(|head| (head.document.active_revision, head.document.sequence)),
                &request.idempotency_key,
                request.desired_state_checksum,
            )
            .await?
        {
            return Ok(replay);
        }

        let actual = current.as_ref().map(|head| head.document.active_revision);
        let original_digest = original.map(|head| head.document.active_revision);
        if failure == ObjectStoreErrorKind::PreconditionFailed || actual != original_digest {
            Err(BlobPublicationError::Conflict {
                expected: request.expected,
                actual,
            })
        } else {
            Err(BlobPublicationError::AmbiguousUnavailable)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use tokio::sync::Barrier;

    use crate::backends::object_store::{
        InMemoryObjectStore, ObjectStoreLimits, ObjectStoreOperation, ObjectValue,
    };

    use super::*;

    const NO_FAULT: u8 = 0;
    const FAIL_BEFORE_HEAD: u8 = 1;
    const FAIL_AFTER_HEAD: u8 = 2;

    fn limits() -> ObjectStoreLimits {
        ObjectStoreLimits::for_max_object_bytes(
            NonZeroUsize::new(2 * 1024 * 1024).expect("non-zero limit"),
        )
    }

    fn environment() -> EnvironmentId {
        EnvironmentId::parse("production-us-east").expect("valid environment")
    }

    fn immutable(label: &'static [u8]) -> ImmutableObject {
        ImmutableObject {
            kind: ImmutableObjectKind::NamespaceResource,
            bytes: Bytes::from_static(label),
        }
    }

    fn request(
        expected: ExpectedHead,
        key: &str,
        state: &'static [u8],
        object: ImmutableObject,
    ) -> BlobPublicationRequest {
        BlobPublicationRequest {
            expected,
            idempotency_key: IdempotencyKey::parse(key).expect("valid idempotency key"),
            desired_state_checksum: Checksum::of(state),
            objects: vec![object],
        }
    }

    fn publisher<S: ObjectStore>(store: Arc<S>) -> BlobPublication<S> {
        BlobPublication::new(store, NonZeroUsize::new(32).expect("non-zero history"))
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Event {
        operation: ObjectStoreOperation,
        key: ObjectKey,
    }

    #[derive(Clone)]
    struct RecordingFaultStore {
        inner: InMemoryObjectStore,
        events: Arc<Mutex<Vec<Event>>>,
        fault: Arc<AtomicU8>,
    }

    impl RecordingFaultStore {
        fn new() -> Self {
            Self {
                inner: InMemoryObjectStore::new(limits()),
                events: Arc::new(Mutex::new(Vec::new())),
                fault: Arc::new(AtomicU8::new(NO_FAULT)),
            }
        }

        fn set_fault(&self, fault: u8) {
            self.fault.store(fault, Ordering::SeqCst);
        }

        fn events(&self) -> Vec<Event> {
            self.events.lock().expect("events lock").clone()
        }

        fn record(&self, operation: ObjectStoreOperation, key: &ObjectKey) {
            self.events.lock().expect("events lock").push(Event {
                operation,
                key: key.clone(),
            });
        }

        fn is_head(key: &ObjectKey) -> bool {
            key.as_str().starts_with("environments/") && key.as_str().ends_with("/head.json")
        }

        fn unavailable(operation: ObjectStoreOperation) -> ObjectStoreError {
            ObjectStoreError::unavailable(operation, "injected head-write response loss")
        }
    }

    #[async_trait]
    impl ObjectStore for RecordingFaultStore {
        fn name(&self) -> &'static str {
            "recording-fault"
        }

        fn limits(&self) -> ObjectStoreLimits {
            self.inner.limits()
        }

        async fn get(&self, key: &ObjectKey) -> Result<ObjectValue, ObjectStoreError> {
            self.record(ObjectStoreOperation::Get, key);
            self.inner.get(key).await
        }

        async fn put_if_absent(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
        ) -> Result<ObjectVersion, ObjectStoreError> {
            self.record(ObjectStoreOperation::PutIfAbsent, key);
            if Self::is_head(key)
                && self
                    .fault
                    .compare_exchange(
                        FAIL_BEFORE_HEAD,
                        NO_FAULT,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
            {
                return Err(Self::unavailable(ObjectStoreOperation::PutIfAbsent));
            }
            let result = self.inner.put_if_absent(key, bytes).await;
            if Self::is_head(key)
                && result.is_ok()
                && self
                    .fault
                    .compare_exchange(
                        FAIL_AFTER_HEAD,
                        NO_FAULT,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
            {
                return Err(Self::unavailable(ObjectStoreOperation::PutIfAbsent));
            }
            result
        }

        async fn replace_if_version(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
            expected: &ObjectVersion,
        ) -> Result<ObjectVersion, ObjectStoreError> {
            self.record(ObjectStoreOperation::ReplaceIfVersion, key);
            if Self::is_head(key)
                && self
                    .fault
                    .compare_exchange(
                        FAIL_BEFORE_HEAD,
                        NO_FAULT,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
            {
                return Err(Self::unavailable(ObjectStoreOperation::ReplaceIfVersion));
            }
            let result = self.inner.replace_if_version(key, bytes, expected).await;
            if Self::is_head(key)
                && result.is_ok()
                && self
                    .fault
                    .compare_exchange(
                        FAIL_AFTER_HEAD,
                        NO_FAULT,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    )
                    .is_ok()
            {
                return Err(Self::unavailable(ObjectStoreOperation::ReplaceIfVersion));
            }
            result
        }
    }

    #[derive(Clone)]
    struct RacingStore {
        inner: InMemoryObjectStore,
        replacements: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
    }

    impl RacingStore {
        fn new() -> Self {
            Self {
                inner: InMemoryObjectStore::new(limits()),
                replacements: Arc::new(AtomicUsize::new(0)),
                barrier: Arc::new(Barrier::new(2)),
            }
        }
    }

    #[async_trait]
    impl ObjectStore for RacingStore {
        fn name(&self) -> &'static str {
            "racing"
        }

        fn limits(&self) -> ObjectStoreLimits {
            self.inner.limits()
        }

        async fn get(&self, key: &ObjectKey) -> Result<ObjectValue, ObjectStoreError> {
            self.inner.get(key).await
        }

        async fn put_if_absent(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
        ) -> Result<ObjectVersion, ObjectStoreError> {
            self.inner.put_if_absent(key, bytes).await
        }

        async fn replace_if_version(
            &self,
            key: &ObjectKey,
            bytes: Bytes,
            expected: &ObjectVersion,
        ) -> Result<ObjectVersion, ObjectStoreError> {
            if RecordingFaultStore::is_head(key)
                && self.replacements.fetch_add(1, Ordering::SeqCst) < 2
            {
                self.barrier.wait().await;
            }
            self.inner.replace_if_version(key, bytes, expected).await
        }
    }

    #[test]
    fn identifiers_and_content_addresses_form_the_exact_keys() {
        let environment = environment();
        let digest = Checksum::of(b"content");
        let segment = digest_segment(digest);

        assert_eq!(
            environment_head_key(&environment).as_str(),
            "environments/production-us-east/head.json"
        );
        assert_eq!(
            revision_manifest_key(digest).as_str(),
            format!("revisions/{segment}/manifest.cbor")
        );
        assert_eq!(
            namespace_resource_key(digest).as_str(),
            format!("resources/namespaces/{segment}.cbor")
        );
        assert_eq!(
            deployment_resource_key(digest).as_str(),
            format!("resources/deployment/{segment}.cbor")
        );
        assert_eq!(
            secret_key(digest).as_str(),
            format!("secrets/{segment}.bin")
        );
        for invalid in ["", "Prod", "-prod", "prod-", "prod/us"] {
            assert!(
                EnvironmentId::parse(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn head_documents_are_deterministic_bounded_and_strict() {
        let head = HeadDocument::new(Checksum::of(b"revision"), 7).expect("valid head");
        let encoded = head.encode();
        assert_eq!(HeadDocument::decode(&encoded), Ok(head));

        let unknown = String::from_utf8(encoded.to_vec())
            .expect("head is UTF-8")
            .replacen("\"schema_version\":1", "\"schema_version\":2", 1);
        assert!(matches!(
            HeadDocument::decode(unknown.as_bytes()),
            Err(HeadDocumentError::UnknownSchema { found: 2 })
        ));
        let invalid_digest = String::from_utf8(encoded.to_vec())
            .expect("head is UTF-8")
            .replacen("sha256:", "sha257:", 1);
        assert!(matches!(
            HeadDocument::decode(invalid_digest.as_bytes()),
            Err(HeadDocumentError::InvalidDigest { .. })
        ));
        let overflow = br#"{"schema_version":1,"active_revision":"sha256:0000000000000000000000000000000000000000000000000000000000000000","sequence":18446744073709551616,"integrity":{"algorithm":"sha256","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000"}}"#;
        assert_eq!(
            HeadDocument::decode(overflow),
            Err(HeadDocumentError::Malformed)
        );
        assert!(matches!(
            HeadDocument::decode(&vec![b'x'; MAX_HEAD_DOCUMENT_BYTES + 1]),
            Err(HeadDocumentError::Oversized { .. })
        ));
    }

    #[tokio::test]
    async fn immutable_uploads_and_manifest_are_confirmed_before_head() {
        let store = Arc::new(RecordingFaultStore::new());
        let publication = publisher(Arc::clone(&store));
        publication
            .publish(
                &environment(),
                request(
                    ExpectedHead::Empty,
                    "create-1",
                    b"state-1",
                    immutable(b"namespace-1"),
                ),
            )
            .await
            .expect("publication");

        let writes = store
            .events()
            .into_iter()
            .filter(|event| event.operation != ObjectStoreOperation::Get)
            .collect::<Vec<_>>();
        assert_eq!(writes.len(), 3);
        assert!(writes[0].key.as_str().starts_with("resources/namespaces/"));
        assert!(writes[1].key.as_str().starts_with("revisions/"));
        assert_eq!(
            writes[2].key,
            environment_head_key(&environment()),
            "head must be the last and only mutable write"
        );
    }

    #[tokio::test]
    async fn crash_before_head_leaves_the_old_head_active() {
        let store = Arc::new(RecordingFaultStore::new());
        let publication = publisher(Arc::clone(&store));
        let first = publication
            .publish(
                &environment(),
                request(
                    ExpectedHead::Empty,
                    "create-1",
                    b"state-1",
                    immutable(b"namespace-1"),
                ),
            )
            .await
            .expect("first publication");
        store.set_fault(FAIL_BEFORE_HEAD);
        let unreachable = immutable(b"namespace-2");
        let unreachable_key = unreachable.key();
        let error = publication
            .publish(
                &environment(),
                request(
                    ExpectedHead::Revision(first.revision),
                    "create-2",
                    b"state-2",
                    unreachable,
                ),
            )
            .await
            .expect_err("head write is unavailable");
        assert_eq!(error, BlobPublicationError::AmbiguousUnavailable);

        let head = store
            .inner
            .get(&environment_head_key(&environment()))
            .await
            .expect("old head");
        assert_eq!(
            HeadDocument::decode(&head.bytes)
                .expect("valid head")
                .active_revision(),
            first.revision
        );
        store
            .inner
            .get(&unreachable_key)
            .await
            .expect("immutable upload remains safely unreachable");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stale_cas_has_one_winner_and_one_explicit_conflict() {
        let store = Arc::new(RacingStore::new());
        let publication = publisher(Arc::clone(&store));
        let first = publication
            .publish(
                &environment(),
                request(
                    ExpectedHead::Empty,
                    "create-1",
                    b"state-1",
                    immutable(b"namespace-1"),
                ),
            )
            .await
            .expect("first publication");

        let left = tokio::spawn({
            let publication = publication.clone();
            async move {
                publication
                    .publish(
                        &environment(),
                        request(
                            ExpectedHead::Revision(first.revision),
                            "left",
                            b"state-left",
                            immutable(b"namespace-left"),
                        ),
                    )
                    .await
            }
        });
        let right = tokio::spawn({
            let publication = publication.clone();
            async move {
                publication
                    .publish(
                        &environment(),
                        request(
                            ExpectedHead::Revision(first.revision),
                            "right",
                            b"state-right",
                            immutable(b"namespace-right"),
                        ),
                    )
                    .await
            }
        });
        let results = [
            left.await.expect("left task"),
            right.await.expect("right task"),
        ];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(result, Err(BlobPublicationError::Conflict { .. })))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn existing_same_content_is_reused_but_a_digest_collision_is_refused() {
        let store = Arc::new(InMemoryObjectStore::new(limits()));
        let publication = publisher(Arc::clone(&store));
        let shared = immutable(b"shared-resource");
        let first = publication
            .publish(
                &environment(),
                request(ExpectedHead::Empty, "one", b"state-1", shared.clone()),
            )
            .await
            .expect("first publication");
        publication
            .publish(
                &environment(),
                request(
                    ExpectedHead::Revision(first.revision),
                    "two",
                    b"state-2",
                    shared,
                ),
            )
            .await
            .expect("same immutable bytes are confirmed and reused");

        let collision = immutable(b"address-owner");
        store
            .put_if_absent(&collision.key(), Bytes::from_static(b"different-bytes"))
            .await
            .expect("seed impossible provider corruption");
        let head = HeadDocument::decode(
            &store
                .get(&environment_head_key(&environment()))
                .await
                .expect("head")
                .bytes,
        )
        .expect("valid head");
        let error = publication
            .publish(
                &environment(),
                request(
                    ExpectedHead::Revision(head.active_revision()),
                    "three",
                    b"state-3",
                    collision.clone(),
                ),
            )
            .await
            .expect_err("different bytes at a content address must fail closed");
        assert_eq!(
            error,
            BlobPublicationError::ImmutableCollision {
                key: collision.key()
            }
        );
    }

    #[tokio::test]
    async fn lost_successful_head_response_is_recovered_by_reread() {
        let store = Arc::new(RecordingFaultStore::new());
        let publication = publisher(Arc::clone(&store));
        store.set_fault(FAIL_AFTER_HEAD);
        let outcome = publication
            .publish(
                &environment(),
                request(
                    ExpectedHead::Empty,
                    "lost-response",
                    b"state-1",
                    immutable(b"namespace-1"),
                ),
            )
            .await
            .expect("reread proves the intended head was committed");
        assert_eq!(outcome.sequence, 1);
        assert!(outcome.replayed);
    }

    #[tokio::test]
    async fn idempotency_replays_before_stale_conflict_and_reuse_is_rejected() {
        let store = Arc::new(InMemoryObjectStore::new(limits()));
        let publication = publisher(Arc::clone(&store));
        let original_request = request(
            ExpectedHead::Empty,
            "retry-me",
            b"state-1",
            immutable(b"namespace-1"),
        );
        let first = publication
            .publish(&environment(), original_request.clone())
            .await
            .expect("first publication");
        let replay = publication
            .publish(&environment(), original_request)
            .await
            .expect("stale Empty expectation is ignored for an exact replay");
        assert_eq!(replay.revision, first.revision);
        assert_eq!(replay.sequence, first.sequence);
        assert!(replay.replayed);

        let error = publication
            .publish(
                &environment(),
                request(
                    ExpectedHead::Empty,
                    "retry-me",
                    b"different-state",
                    immutable(b"namespace-other"),
                ),
            )
            .await
            .expect_err("key reuse must win over stale-head reporting");
        assert!(matches!(
            error,
            BlobPublicationError::IdempotencyKeyReuse { .. }
        ));
    }

    #[tokio::test]
    async fn bounded_history_exhaustion_fails_closed() {
        let store = Arc::new(InMemoryObjectStore::new(limits()));
        let full = publisher(Arc::clone(&store));
        let first = full
            .publish(
                &environment(),
                request(
                    ExpectedHead::Empty,
                    "one",
                    b"state-1",
                    immutable(b"namespace-1"),
                ),
            )
            .await
            .expect("first");
        let second = full
            .publish(
                &environment(),
                request(
                    ExpectedHead::Revision(first.revision),
                    "two",
                    b"state-2",
                    immutable(b"namespace-2"),
                ),
            )
            .await
            .expect("second");

        let bounded = BlobPublication::new(
            Arc::clone(&store),
            NonZeroUsize::new(1).expect("non-zero history"),
        );
        let error = bounded
            .publish(
                &environment(),
                request(
                    ExpectedHead::Revision(second.revision),
                    "three",
                    b"state-3",
                    immutable(b"namespace-3"),
                ),
            )
            .await
            .expect_err("unsearched retained history cannot be treated as absent");
        assert_eq!(
            error,
            BlobPublicationError::HistoryLimitExceeded { limit: 1 }
        );
    }

    #[tokio::test]
    async fn every_successful_publication_advances_the_sequence_once() {
        let store = Arc::new(InMemoryObjectStore::new(limits()));
        let publication = publisher(Arc::clone(&store));
        let mut expected = ExpectedHead::Empty;
        for (index, (key, state, body)) in [
            (
                "one",
                b"state-1" as &'static [u8],
                b"body-1" as &'static [u8],
            ),
            ("two", b"state-2", b"body-2"),
            ("three", b"state-3", b"body-3"),
        ]
        .into_iter()
        .enumerate()
        {
            let outcome = publication
                .publish(
                    &environment(),
                    request(expected, key, state, immutable(body)),
                )
                .await
                .expect("publication");
            assert_eq!(outcome.sequence, index as u64 + 1);
            expected = ExpectedHead::Revision(outcome.revision);
        }
    }
}
