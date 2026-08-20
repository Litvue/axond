//! Immutable object-store publication selected by ADR 0062.
//!
//! A publication uploads every content-addressed object with create-only
//! semantics and then performs exactly one conditional write to an environment
//! head. The head is the only mutable object. Store versions remain opaque CAS
//! tokens; SHA-256 digests identify content and are never used as CAS tokens.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::backends::object_store::{
    ObjectKey, ObjectStore, ObjectStoreError, ObjectStoreErrorKind, ObjectVersion,
};

use super::publication_auth::{
    PublicationAuthenticationError, PublicationSignature, PublicationSigner, PublicationTrustStore,
};
use super::{Checksum, IdempotencyKey, MutationId, MutationKind};

const HEAD_SCHEMA_VERSION: u64 = 2;
const MANIFEST_SCHEMA_VERSION: u64 = 2;
const HEAD_INTEGRITY_ALGORITHM: &str = "sha256";
const HEAD_INTEGRITY_DOMAIN: &[u8] = b"axond.environment-head.integrity.v2\0";
const HEAD_SIGNATURE_DOMAIN: &[u8] = b"axond.environment-head.signature.v2\0";
const MANIFEST_SIGNATURE_DOMAIN: &[u8] = b"axond.revision-manifest.signature.v2\0";
const ACTOR_BINDING_DOMAIN: &[u8] = b"axond.publication.actor.v1\0";
const GRANT_BINDING_DOMAIN: &[u8] = b"axond.publication.grant.v1\0";
const IDEMPOTENCY_BINDING_DOMAIN: &[u8] = b"axond.publication.idempotency.v2\0";
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

/// A domain-separated digest of canonical, non-secret actor attribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicationActorBinding(Checksum);

impl PublicationActorBinding {
    pub fn of(canonical_attribution: &[u8]) -> Self {
        Self(domain_checksum(ACTOR_BINDING_DOMAIN, canonical_attribution))
    }

    pub const fn checksum(self) -> Checksum {
        self.0
    }
}

/// A domain-separated digest of the exact authorization grant used to publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicationGrantBinding(Checksum);

impl PublicationGrantBinding {
    pub fn of(canonical_grant: &[u8]) -> Self {
        Self(domain_checksum(GRANT_BINDING_DOMAIN, canonical_grant))
    }

    pub const fn checksum(self) -> Checksum {
        self.0
    }
}

fn domain_checksum(domain: &[u8], value: &[u8]) -> Checksum {
    let mut bytes = Vec::with_capacity(domain.len() + 8 + value.len());
    bytes.extend_from_slice(domain);
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
    Checksum::of(&bytes)
}

/// Authenticated administrative provenance carried by every blob revision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicationAuthorization {
    actor: PublicationActorBinding,
    grant: PublicationGrantBinding,
    mutation: MutationId,
    mutation_kind: MutationKind,
}

impl PublicationAuthorization {
    pub const fn new(
        actor: PublicationActorBinding,
        grant: PublicationGrantBinding,
        mutation: MutationId,
        mutation_kind: MutationKind,
    ) -> Self {
        Self {
            actor,
            grant,
            mutation,
            mutation_kind,
        }
    }

    pub const fn actor(self) -> PublicationActorBinding {
        self.actor
    }

    pub const fn grant(self) -> PublicationGrantBinding {
        self.grant
    }

    pub const fn mutation(self) -> MutationId {
        self.mutation
    }

    pub const fn mutation_kind(self) -> MutationKind {
        self.mutation_kind
    }
}

/// The bounded, signed mutable document at
/// `environments/{environment}/head.json`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadDocument {
    environment: EnvironmentId,
    active_revision: Checksum,
    sequence: u64,
    integrity: Checksum,
    signature: PublicationSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HeadDocumentError {
    #[error("head document is {observed} bytes, over the {limit}-byte limit")]
    Oversized { observed: usize, limit: usize },
    #[error("head document is malformed JSON")]
    Malformed,
    #[error("head document is unsigned")]
    Unsigned,
    #[error("head schema version {found} is not supported")]
    UnknownSchema { found: u64 },
    #[error("head environment identifier is invalid")]
    InvalidEnvironment,
    #[error("head belongs to a different environment")]
    EnvironmentMismatch,
    #[error("head active revision digest is invalid")]
    InvalidDigest,
    #[error("head sequence must be greater than zero")]
    ZeroSequence,
    #[error("head integrity algorithm is not supported")]
    UnknownIntegrityAlgorithm,
    #[error("head integrity digest is invalid")]
    InvalidIntegrityDigest,
    #[error("head integrity digest does not match its publication metadata")]
    IntegrityMismatch,
    #[error(transparent)]
    Authentication(#[from] PublicationAuthenticationError),
    #[error("head sequence {actual} is below the accepted sequence floor {minimum}")]
    Rollback { minimum: u64, actual: u64 },
    #[error("the environment head is absent below the accepted sequence floor {minimum}")]
    MissingBelowFloor { minimum: u64 },
    #[error("head JSON is not in its deterministic encoding")]
    NonCanonical,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireHead {
    schema_version: u64,
    environment: String,
    active_revision: String,
    sequence: u64,
    integrity: WireIntegrity,
    signature: Option<WireSignature>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireIntegrity {
    algorithm: String,
    digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSignature {
    schema_version: u64,
    algorithm: String,
    key_id: String,
    value: String,
}

impl HeadDocument {
    fn sign(
        environment: EnvironmentId,
        active_revision: Checksum,
        sequence: u64,
        signer: &PublicationSigner,
    ) -> Result<Self, HeadDocumentError> {
        if sequence == 0 {
            return Err(HeadDocumentError::ZeroSequence);
        }
        let integrity = Self::integrity_digest(&environment, active_revision, sequence);
        let signature = signer.sign(&Self::signature_bytes(
            &environment,
            active_revision,
            sequence,
            integrity,
            super::publication_auth::PUBLICATION_SIGNATURE_SCHEMA,
            signer.key_id().as_str(),
            signer.algorithm().as_str(),
        ));
        Ok(Self {
            environment,
            active_revision,
            sequence,
            integrity,
            signature,
        })
    }

    pub fn environment(&self) -> &EnvironmentId {
        &self.environment
    }

    pub const fn active_revision(&self) -> Checksum {
        self.active_revision
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    fn integrity_digest(
        environment: &EnvironmentId,
        active_revision: Checksum,
        sequence: u64,
    ) -> Checksum {
        let mut bytes =
            Vec::with_capacity(HEAD_INTEGRITY_DOMAIN.len() + environment.as_str().len() + 56);
        bytes.extend_from_slice(HEAD_INTEGRITY_DOMAIN);
        bytes.extend_from_slice(&HEAD_SCHEMA_VERSION.to_be_bytes());
        bytes.extend_from_slice(&(environment.as_str().len() as u64).to_be_bytes());
        bytes.extend_from_slice(environment.as_str().as_bytes());
        bytes.extend_from_slice(active_revision.as_bytes());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        Checksum::of(&bytes)
    }

    fn signature_bytes(
        environment: &EnvironmentId,
        active_revision: Checksum,
        sequence: u64,
        integrity: Checksum,
        signature_schema: u64,
        key_id: &str,
        algorithm: &str,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            HEAD_SIGNATURE_DOMAIN.len()
                + environment.as_str().len()
                + key_id.len()
                + algorithm.len()
                + 128,
        );
        bytes.extend_from_slice(HEAD_SIGNATURE_DOMAIN);
        bytes.extend_from_slice(&HEAD_SCHEMA_VERSION.to_be_bytes());
        append_length_prefixed(&mut bytes, environment.as_str().as_bytes());
        bytes.extend_from_slice(active_revision.as_bytes());
        bytes.extend_from_slice(&sequence.to_be_bytes());
        append_length_prefixed(&mut bytes, HEAD_INTEGRITY_ALGORITHM.as_bytes());
        bytes.extend_from_slice(integrity.as_bytes());
        bytes.extend_from_slice(&signature_schema.to_be_bytes());
        append_length_prefixed(&mut bytes, algorithm.as_bytes());
        append_length_prefixed(&mut bytes, key_id.as_bytes());
        bytes
    }

    pub fn encode(&self) -> Bytes {
        let wire = WireHead {
            schema_version: HEAD_SCHEMA_VERSION,
            environment: self.environment.as_str().to_owned(),
            active_revision: self.active_revision.to_string(),
            sequence: self.sequence,
            integrity: WireIntegrity {
                algorithm: HEAD_INTEGRITY_ALGORITHM.to_owned(),
                digest: self.integrity.to_string(),
            },
            signature: Some(WireSignature {
                schema_version: self.signature.schema_version(),
                algorithm: self.signature.algorithm().as_str().to_owned(),
                key_id: self.signature.key_id().as_str().to_owned(),
                value: BASE64_STANDARD.encode(self.signature.value()),
            }),
        };
        Bytes::from(serde_json::to_vec(&wire).expect("head fields always serialize"))
    }

    /// Verify canonical head bytes against bootstrap trust and an anti-rollback
    /// floor. No public parse-only API exists.
    fn verify(
        bytes: &[u8],
        expected_environment: &EnvironmentId,
        minimum_sequence: u64,
        trust: &PublicationTrustStore,
    ) -> Result<Self, HeadDocumentError> {
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
        let environment = EnvironmentId::parse(wire.environment)
            .map_err(|_| HeadDocumentError::InvalidEnvironment)?;
        if &environment != expected_environment {
            return Err(HeadDocumentError::EnvironmentMismatch);
        }
        if wire.sequence == 0 {
            return Err(HeadDocumentError::ZeroSequence);
        }
        if wire.sequence < minimum_sequence {
            return Err(HeadDocumentError::Rollback {
                minimum: minimum_sequence,
                actual: wire.sequence,
            });
        }
        if wire.integrity.algorithm != HEAD_INTEGRITY_ALGORITHM {
            return Err(HeadDocumentError::UnknownIntegrityAlgorithm);
        }
        let active_revision =
            Checksum::parse(&wire.active_revision).map_err(|_| HeadDocumentError::InvalidDigest)?;
        let integrity = Checksum::parse(&wire.integrity.digest)
            .map_err(|_| HeadDocumentError::InvalidIntegrityDigest)?;
        if integrity != Self::integrity_digest(&environment, active_revision, wire.sequence) {
            return Err(HeadDocumentError::IntegrityMismatch);
        }
        let signature_wire = wire.signature.ok_or(HeadDocumentError::Unsigned)?;
        let signature_bytes = BASE64_STANDARD
            .decode(signature_wire.value.as_bytes())
            .map_err(|_| PublicationAuthenticationError::InvalidSignatureEncoding)?;
        let signature = PublicationSignature::decode(
            signature_wire.schema_version,
            &signature_wire.algorithm,
            &signature_wire.key_id,
            &signature_bytes,
        )?;
        let document = Self {
            environment,
            active_revision,
            sequence: wire.sequence,
            integrity,
            signature,
        };
        if document.encode().as_ref() != bytes {
            return Err(HeadDocumentError::NonCanonical);
        }
        trust.verify(
            &document.signature,
            &Self::signature_bytes(
                &document.environment,
                document.active_revision,
                document.sequence,
                document.integrity,
                document.signature.schema_version(),
                document.signature.key_id().as_str(),
                document.signature.algorithm().as_str(),
            ),
        )?;
        Ok(document)
    }
}

fn append_length_prefixed(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

/// Shared monotonic floor for one process's view of a publication environment.
///
/// A verified observation raises the floor. A later valid-but-older head, or a
/// missing head after any sequence was observed, is a typed rollback refusal.
#[derive(Debug, Clone)]
pub struct PublicationSequenceGuard {
    environment: EnvironmentId,
    minimum: Arc<AtomicU64>,
}

impl PublicationSequenceGuard {
    pub fn new(environment: EnvironmentId, minimum_sequence: u64) -> Self {
        Self {
            environment,
            minimum: Arc::new(AtomicU64::new(minimum_sequence)),
        }
    }

    pub fn environment(&self) -> &EnvironmentId {
        &self.environment
    }

    pub fn minimum_sequence(&self) -> u64 {
        self.minimum.load(Ordering::Acquire)
    }

    pub fn verify_head(
        &self,
        bytes: &[u8],
        trust: &PublicationTrustStore,
    ) -> Result<HeadDocument, HeadDocumentError> {
        let head = HeadDocument::verify(bytes, &self.environment, self.minimum_sequence(), trust)?;
        self.observe(head.sequence())?;
        Ok(head)
    }

    fn observe(&self, sequence: u64) -> Result<(), HeadDocumentError> {
        loop {
            let floor = self.minimum_sequence();
            if sequence < floor {
                return Err(HeadDocumentError::Rollback {
                    minimum: floor,
                    actual: sequence,
                });
            }
            if sequence == floor
                || self
                    .minimum
                    .compare_exchange(floor, sequence, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                return Ok(());
            }
        }
    }

    /// Verify that an absent head is compatible with the sequence floor.
    /// Absence is valid only before this process or its persisted recovery state
    /// has authenticated any publication for the environment.
    pub fn verify_absent(&self) -> Result<(), HeadDocumentError> {
        let minimum = self.minimum_sequence();
        if minimum == 0 {
            Ok(())
        } else {
            Err(HeadDocumentError::MissingBelowFloor { minimum })
        }
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
    environment: EnvironmentId,
    parent: Option<Checksum>,
    sequence: u64,
    authorization: PublicationAuthorization,
    idempotency_binding: Checksum,
    desired_state_checksum: Checksum,
    objects: Vec<ImmutableReference>,
    signature: PublicationSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RevisionManifestError {
    #[error("revision manifest is {observed} bytes, over the {limit}-byte limit")]
    Oversized { observed: usize, limit: usize },
    #[error("revision manifest is malformed or not deterministic CBOR")]
    Malformed,
    #[error("revision manifest is unsigned")]
    Unsigned,
    #[error("revision manifest schema version {found} is not supported")]
    UnknownSchema { found: u64 },
    #[error("revision manifest environment identifier is invalid")]
    InvalidEnvironment,
    #[error("revision manifest belongs to a different environment")]
    EnvironmentMismatch,
    #[error("revision manifest mutation identifier is invalid")]
    InvalidMutation,
    #[error("revision manifest mutation kind {found} is not supported")]
    UnknownMutationKind { found: u64 },
    #[error("revision manifest bytes do not match their linked content address")]
    DigestMismatch,
    #[error(transparent)]
    Authentication(#[from] PublicationAuthenticationError),
    #[error("revision manifest sequence must be greater than zero")]
    ZeroSequence,
    #[error("revision manifest sequence {actual} does not match the linked sequence {expected}")]
    SequenceMismatch { expected: u64, actual: u64 },
    #[error("revision manifest sequence 1 must not name a parent")]
    ParentBeforeFirstSequence,
    #[error("revision manifest sequence {sequence} must name its preceding revision")]
    MissingParentAfterFirstSequence { sequence: u64 },
    #[error(
        "revision manifest contains {observed} object references, over the {limit}-object limit"
    )]
    TooManyObjects { observed: usize, limit: usize },
    #[error("revision manifest object references are not strictly ordered and unique")]
    NonCanonicalObjects,
}

impl BlobRevisionManifest {
    #[allow(clippy::too_many_arguments)]
    fn sign(
        environment: EnvironmentId,
        parent: Option<Checksum>,
        sequence: u64,
        authorization: PublicationAuthorization,
        idempotency_binding: Checksum,
        desired_state_checksum: Checksum,
        objects: Vec<ImmutableReference>,
        signer: &PublicationSigner,
    ) -> Result<Self, RevisionManifestError> {
        let signature = signer.sign(&Self::signature_bytes(
            &environment,
            parent,
            sequence,
            authorization,
            idempotency_binding,
            desired_state_checksum,
            &objects,
            super::publication_auth::PUBLICATION_SIGNATURE_SCHEMA,
            signer.algorithm().as_str(),
            signer.key_id().as_str(),
        )?);
        Ok(Self {
            environment,
            parent,
            sequence,
            authorization,
            idempotency_binding,
            desired_state_checksum,
            objects,
            signature,
        })
    }

    fn encode(&self) -> Result<Bytes, RevisionManifestError> {
        self.encode_with_signature(true)
    }

    fn encode_with_signature(&self, include_value: bool) -> Result<Bytes, RevisionManifestError> {
        if self.sequence == 0 {
            return Err(RevisionManifestError::ZeroSequence);
        }
        if self.objects.len() > MAX_MANIFEST_OBJECTS {
            return Err(RevisionManifestError::TooManyObjects {
                observed: self.objects.len(),
                limit: MAX_MANIFEST_OBJECTS,
            });
        }
        if self.objects.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(RevisionManifestError::NonCanonicalObjects);
        }
        let mut bytes = Vec::with_capacity(384 + self.objects.len() * 40);
        cbor_array(&mut bytes, 9);
        cbor_unsigned(&mut bytes, MANIFEST_SCHEMA_VERSION);
        cbor_text(&mut bytes, self.environment.as_str());
        match self.parent {
            Some(parent) => cbor_bytes(&mut bytes, parent.as_bytes()),
            None => bytes.push(0xf6),
        }
        cbor_unsigned(&mut bytes, self.sequence);
        encode_authorization(&mut bytes, self.authorization);
        cbor_bytes(&mut bytes, self.idempotency_binding.as_bytes());
        cbor_bytes(&mut bytes, self.desired_state_checksum.as_bytes());
        cbor_array(&mut bytes, self.objects.len() as u64);
        for object in &self.objects {
            cbor_array(&mut bytes, 2);
            cbor_unsigned(&mut bytes, object.kind.tag());
            cbor_bytes(&mut bytes, object.digest.as_bytes());
        }
        cbor_array(&mut bytes, if include_value { 4 } else { 3 });
        cbor_unsigned(&mut bytes, self.signature.schema_version());
        cbor_text(&mut bytes, self.signature.algorithm().as_str());
        cbor_text(&mut bytes, self.signature.key_id().as_str());
        if include_value {
            cbor_bytes(&mut bytes, self.signature.value());
        }
        if bytes.len() > MAX_REVISION_MANIFEST_BYTES {
            return Err(RevisionManifestError::Oversized {
                observed: bytes.len(),
                limit: MAX_REVISION_MANIFEST_BYTES,
            });
        }
        Ok(Bytes::from(bytes))
    }

    #[allow(clippy::too_many_arguments)]
    fn signature_bytes(
        environment: &EnvironmentId,
        parent: Option<Checksum>,
        sequence: u64,
        authorization: PublicationAuthorization,
        idempotency_binding: Checksum,
        desired_state_checksum: Checksum,
        objects: &[ImmutableReference],
        signature_schema: u64,
        signature_algorithm: &str,
        key_id: &str,
    ) -> Result<Vec<u8>, RevisionManifestError> {
        let placeholder =
            PublicationSignature::decode(signature_schema, signature_algorithm, key_id, &[0; 64])?;
        let manifest = Self {
            environment: environment.clone(),
            parent,
            sequence,
            authorization,
            idempotency_binding,
            desired_state_checksum,
            objects: objects.to_vec(),
            signature: placeholder,
        };
        let encoded = manifest.encode_with_signature(false)?;
        let mut bytes = Vec::with_capacity(MANIFEST_SIGNATURE_DOMAIN.len() + encoded.len());
        bytes.extend_from_slice(MANIFEST_SIGNATURE_DOMAIN);
        bytes.extend_from_slice(&encoded);
        Ok(bytes)
    }

    fn decode_and_verify(
        bytes: &[u8],
        expected_environment: &EnvironmentId,
        expected_digest: Checksum,
        trust: &PublicationTrustStore,
    ) -> Result<Self, RevisionManifestError> {
        if bytes.len() > MAX_REVISION_MANIFEST_BYTES {
            return Err(RevisionManifestError::Oversized {
                observed: bytes.len(),
                limit: MAX_REVISION_MANIFEST_BYTES,
            });
        }
        if Checksum::of(bytes) != expected_digest {
            return Err(RevisionManifestError::DigestMismatch);
        }
        let mut cursor = CborCursor::new(bytes);
        match cursor.array_len()? {
            8 => return Err(RevisionManifestError::Unsigned),
            9 => {}
            _ => return Err(RevisionManifestError::Malformed),
        }
        let schema = cursor.unsigned()?;
        if schema != MANIFEST_SCHEMA_VERSION {
            return Err(RevisionManifestError::UnknownSchema { found: schema });
        }
        let environment = EnvironmentId::parse(cursor.text()?.to_owned())
            .map_err(|_| RevisionManifestError::InvalidEnvironment)?;
        if &environment != expected_environment {
            return Err(RevisionManifestError::EnvironmentMismatch);
        }
        let parent = cursor.optional_digest()?;
        let sequence = cursor.unsigned()?;
        if sequence == 0 {
            return Err(RevisionManifestError::ZeroSequence);
        }
        let authorization = decode_authorization(&mut cursor)?;
        let idempotency_binding = cursor.digest()?;
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
        if objects.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(RevisionManifestError::NonCanonicalObjects);
        }
        cursor.array_exact(4)?;
        let signature_schema = cursor.unsigned()?;
        let signature_algorithm = cursor.text()?;
        let key_id = cursor.text()?;
        let signature_value = cursor.bytes()?;
        let signature = PublicationSignature::decode(
            signature_schema,
            signature_algorithm,
            key_id,
            signature_value,
        )?;
        if !cursor.is_empty() {
            return Err(RevisionManifestError::Malformed);
        }
        let manifest = Self {
            environment,
            parent,
            sequence,
            authorization,
            idempotency_binding,
            desired_state_checksum,
            objects,
            signature,
        };
        if manifest.encode()?.as_ref() != bytes {
            return Err(RevisionManifestError::Malformed);
        }
        trust.verify(
            &manifest.signature,
            &Self::signature_bytes(
                &manifest.environment,
                manifest.parent,
                manifest.sequence,
                manifest.authorization,
                manifest.idempotency_binding,
                manifest.desired_state_checksum,
                &manifest.objects,
                manifest.signature.schema_version(),
                manifest.signature.algorithm().as_str(),
                manifest.signature.key_id().as_str(),
            )?,
        )?;
        Ok(manifest)
    }
}

/// A revision manifest whose content address, environment, sequence, canonical
/// form, signature, and parent shape have all been checked.
///
/// Hydration APIs should accept this type rather than raw bytes. Its only
/// constructor performs verification, which makes signature verification a
/// type-level prerequisite rather than a caller convention.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedRevisionManifest(BlobRevisionManifest);

impl VerifiedRevisionManifest {
    pub fn verify(
        bytes: &[u8],
        environment: &EnvironmentId,
        digest: Checksum,
        expected_sequence: u64,
        trust: &PublicationTrustStore,
    ) -> Result<Self, RevisionManifestError> {
        let manifest = BlobRevisionManifest::decode_and_verify(bytes, environment, digest, trust)?;
        if manifest.sequence != expected_sequence {
            return Err(RevisionManifestError::SequenceMismatch {
                expected: expected_sequence,
                actual: manifest.sequence,
            });
        }
        match (manifest.parent, manifest.sequence) {
            (Some(_), 1) => return Err(RevisionManifestError::ParentBeforeFirstSequence),
            (None, sequence) if sequence > 1 => {
                return Err(RevisionManifestError::MissingParentAfterFirstSequence { sequence });
            }
            _ => {}
        }
        Ok(Self(manifest))
    }

    pub fn environment(&self) -> &EnvironmentId {
        &self.0.environment
    }

    pub const fn parent(&self) -> Option<Checksum> {
        self.0.parent
    }

    pub const fn sequence(&self) -> u64 {
        self.0.sequence
    }

    pub const fn authorization(&self) -> PublicationAuthorization {
        self.0.authorization
    }

    pub const fn idempotency_binding(&self) -> Checksum {
        self.0.idempotency_binding
    }

    pub const fn desired_state_checksum(&self) -> Checksum {
        self.0.desired_state_checksum
    }

    pub fn objects(&self) -> impl ExactSizeIterator<Item = (ImmutableObjectKind, Checksum)> + '_ {
        self.0
            .objects
            .iter()
            .map(|reference| (reference.kind, reference.digest))
    }
}

fn mutation_kind_tag(kind: MutationKind) -> u64 {
    match kind {
        MutationKind::Create => 0,
        MutationKind::Update => 1,
        MutationKind::Delete => 2,
        MutationKind::Rotate => 3,
        MutationKind::Rollback => 4,
    }
}

fn mutation_kind_from_tag(tag: u64) -> Result<MutationKind, RevisionManifestError> {
    match tag {
        0 => Ok(MutationKind::Create),
        1 => Ok(MutationKind::Update),
        2 => Ok(MutationKind::Delete),
        3 => Ok(MutationKind::Rotate),
        4 => Ok(MutationKind::Rollback),
        found => Err(RevisionManifestError::UnknownMutationKind { found }),
    }
}

fn encode_authorization(bytes: &mut Vec<u8>, authorization: PublicationAuthorization) {
    cbor_array(bytes, 4);
    cbor_bytes(bytes, authorization.actor().checksum().as_bytes());
    cbor_bytes(bytes, authorization.grant().checksum().as_bytes());
    cbor_text(bytes, &authorization.mutation().to_string());
    cbor_unsigned(bytes, mutation_kind_tag(authorization.mutation_kind()));
}

fn decode_authorization(
    cursor: &mut CborCursor<'_>,
) -> Result<PublicationAuthorization, RevisionManifestError> {
    cursor.array_exact(4)?;
    let actor = PublicationActorBinding(cursor.digest()?);
    let grant = PublicationGrantBinding(cursor.digest()?);
    let mutation =
        MutationId::parse(cursor.text()?).map_err(|_| RevisionManifestError::InvalidMutation)?;
    let mutation_kind = mutation_kind_from_tag(cursor.unsigned()?)?;
    Ok(PublicationAuthorization::new(
        actor,
        grant,
        mutation,
        mutation_kind,
    ))
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
    pub authorization: PublicationAuthorization,
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

/// The maximum number of retained manifests one publication may inspect for an
/// idempotency key.
///
/// This is an availability bound as well as a resource bound. Until immutable
/// idempotency checkpoints exist, a novel key is publishable only while the
/// complete reachable history fits inside this limit. If a parent remains
/// after the limit is consumed, publication fails closed with
/// [`BlobPublicationError::HistoryLimitExceeded`]. Exact replays found inside
/// the bounded window still succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdempotencyHistoryLimit(NonZeroUsize);

impl IdempotencyHistoryLimit {
    pub const fn new(revisions: NonZeroUsize) -> Self {
        Self(revisions)
    }

    pub const fn get(self) -> usize {
        self.0.get()
    }
}

/// Whether the retained chain can be searched completely under the configured
/// idempotency bound.
///
/// Operators can inspect this before admitting administrative mutations. An
/// exhausted status means every genuinely novel mutation will be refused; it
/// does not authorize pruning or an unbounded fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdempotencyHistoryStatus {
    Searchable {
        retained_revisions: usize,
        limit: IdempotencyHistoryLimit,
    },
    Exhausted {
        inspected_revisions: usize,
        limit: IdempotencyHistoryLimit,
    },
}

impl IdempotencyHistoryStatus {
    pub const fn permits_novel_publication(self) -> bool {
        matches!(self, Self::Searchable { .. })
    }
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
    #[error("the authenticated idempotency binding was already used for different desired state")]
    IdempotencyKeyReuse,
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
    environment: EnvironmentId,
    history_limit: IdempotencyHistoryLimit,
    signer: Arc<PublicationSigner>,
    trust: PublicationTrustStore,
    sequence_guard: PublicationSequenceGuard,
}

impl<S> Clone for BlobPublication<S> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            environment: self.environment.clone(),
            history_limit: self.history_limit,
            signer: Arc::clone(&self.signer),
            trust: self.trust.clone(),
            sequence_guard: self.sequence_guard.clone(),
        }
    }
}

impl<S: ObjectStore> BlobPublication<S> {
    pub fn new(
        store: Arc<S>,
        environment: EnvironmentId,
        history_limit: IdempotencyHistoryLimit,
        signer: Arc<PublicationSigner>,
        trust: PublicationTrustStore,
        minimum_sequence: u64,
    ) -> Result<Self, PublicationAuthenticationError> {
        // Refuse a publisher whose active signer is absent from bootstrap
        // trust. This prevents successful writes that no replica can verify.
        let challenge = b"axond.publication.bootstrap-trust.v1\0";
        let signature = signer.sign(challenge);
        trust.verify(&signature, challenge)?;
        Ok(Self {
            store,
            sequence_guard: PublicationSequenceGuard::new(environment.clone(), minimum_sequence),
            environment,
            history_limit,
            signer,
            trust,
        })
    }

    pub fn environment(&self) -> &EnvironmentId {
        &self.environment
    }

    pub const fn history_limit(&self) -> IdempotencyHistoryLimit {
        self.history_limit
    }

    /// Report whether the complete retained history fits inside the configured
    /// idempotency-search bound.
    ///
    /// This reads at most [`Self::history_limit`] manifests. Corrupt, missing,
    /// or unavailable history remains an error; exhaustion is a normal public
    /// status so an administrative surface can expose it before accepting a
    /// novel mutation.
    pub async fn idempotency_history_status(
        &self,
    ) -> Result<IdempotencyHistoryStatus, BlobPublicationError> {
        let head = self.read_head().await?;
        let mut revision = head
            .as_ref()
            .map(|head| (head.document.active_revision, head.document.sequence));
        let limit = self.history_limit.get();
        let mut inspected = 0;
        while inspected < limit {
            let Some((digest, expected_sequence)) = revision else {
                return Ok(IdempotencyHistoryStatus::Searchable {
                    retained_revisions: inspected,
                    limit: self.history_limit,
                });
            };
            let manifest = self.read_manifest(digest, expected_sequence).await?;
            inspected += 1;
            revision = Self::verified_parent(&manifest, expected_sequence)?;
        }
        Ok(if revision.is_some() {
            IdempotencyHistoryStatus::Exhausted {
                inspected_revisions: inspected,
                limit: self.history_limit,
            }
        } else {
            IdempotencyHistoryStatus::Searchable {
                retained_revisions: inspected,
                limit: self.history_limit,
            }
        })
    }

    pub async fn publish(
        &self,
        request: BlobPublicationRequest,
    ) -> Result<PublicationOutcome, BlobPublicationError> {
        let original = self.read_head().await?;

        if let Some(replay) = self
            .find_idempotency(
                original
                    .as_ref()
                    .map(|head| (head.document.active_revision, head.document.sequence)),
                request.authorization,
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

        let manifest = BlobRevisionManifest::sign(
            self.environment.clone(),
            actual,
            sequence,
            request.authorization,
            self.idempotency_binding(request.authorization, &request.idempotency_key),
            request.desired_state_checksum,
            objects,
            &self.signer,
        )?;
        let manifest_bytes = manifest.encode()?;
        let revision = Checksum::of(&manifest_bytes);
        self.confirm_immutable(revision_manifest_key(revision), manifest_bytes)
            .await?;

        let head = HeadDocument::sign(self.environment.clone(), revision, sequence, &self.signer)?;
        let head_key = environment_head_key(&self.environment);
        let final_write = match &original {
            None => self.store.put_if_absent(&head_key, head.encode()).await,
            Some(original) => {
                self.store
                    .replace_if_version(&head_key, head.encode(), &original.version)
                    .await
            }
        };

        match final_write {
            Ok(_) => {
                self.sequence_guard.observe(sequence)?;
                Ok(PublicationOutcome {
                    revision,
                    sequence,
                    replayed: false,
                })
            }
            Err(error)
                if matches!(
                    error.kind(),
                    ObjectStoreErrorKind::PreconditionFailed | ObjectStoreErrorKind::Unavailable
                ) =>
            {
                self.reconcile_final_write(
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

    async fn read_head(&self) -> Result<Option<ReadHead>, BlobPublicationError> {
        match self
            .store
            .get(&environment_head_key(&self.environment))
            .await
        {
            Ok(value) => Ok(Some(ReadHead {
                document: self.sequence_guard.verify_head(&value.bytes, &self.trust)?,
                version: value.version,
            })),
            Err(error) if error.kind() == ObjectStoreErrorKind::NotFound => {
                self.sequence_guard.verify_absent()?;
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn read_manifest(
        &self,
        digest: Checksum,
        expected_sequence: u64,
    ) -> Result<VerifiedRevisionManifest, BlobPublicationError> {
        let key = revision_manifest_key(digest);
        let value = self.store.get(&key).await?;
        Ok(VerifiedRevisionManifest::verify(
            &value.bytes,
            &self.environment,
            digest,
            expected_sequence,
            &self.trust,
        )?)
    }

    fn idempotency_binding(
        &self,
        authorization: PublicationAuthorization,
        key: &IdempotencyKey,
    ) -> Checksum {
        let mut bytes = Vec::with_capacity(
            IDEMPOTENCY_BINDING_DOMAIN.len()
                + self.environment.as_str().len()
                + key.as_str().len()
                + 96,
        );
        bytes.extend_from_slice(IDEMPOTENCY_BINDING_DOMAIN);
        append_length_prefixed(&mut bytes, self.environment.as_str().as_bytes());
        bytes.extend_from_slice(authorization.actor().checksum().as_bytes());
        bytes.extend_from_slice(authorization.grant().checksum().as_bytes());
        append_length_prefixed(&mut bytes, key.as_str().as_bytes());
        Checksum::of(&bytes)
    }

    async fn find_idempotency(
        &self,
        mut revision: Option<(Checksum, u64)>,
        authorization: PublicationAuthorization,
        key: &IdempotencyKey,
        desired_state_checksum: Checksum,
    ) -> Result<Option<PublicationOutcome>, BlobPublicationError> {
        let limit = self.history_limit.get();
        for _ in 0..limit {
            let Some((digest, expected_sequence)) = revision else {
                return Ok(None);
            };
            let manifest = self.read_manifest(digest, expected_sequence).await?;
            let parent = Self::verified_parent(&manifest, expected_sequence)?;
            if manifest.idempotency_binding() == self.idempotency_binding(authorization, key) {
                if manifest.desired_state_checksum() != desired_state_checksum {
                    return Err(BlobPublicationError::IdempotencyKeyReuse);
                }
                return Ok(Some(PublicationOutcome {
                    revision: digest,
                    sequence: manifest.sequence(),
                    replayed: true,
                }));
            }
            revision = parent;
        }
        if revision.is_some() {
            Err(BlobPublicationError::HistoryLimitExceeded { limit })
        } else {
            Ok(None)
        }
    }

    fn verified_parent(
        manifest: &VerifiedRevisionManifest,
        expected_sequence: u64,
    ) -> Result<Option<(Checksum, u64)>, BlobPublicationError> {
        if manifest.sequence() != expected_sequence {
            return Err(RevisionManifestError::SequenceMismatch {
                expected: expected_sequence,
                actual: manifest.sequence(),
            }
            .into());
        }
        match manifest.parent() {
            Some(_) if manifest.sequence() == 1 => {
                Err(RevisionManifestError::ParentBeforeFirstSequence.into())
            }
            Some(parent) => Ok(Some((parent, manifest.sequence() - 1))),
            None if manifest.sequence() == 1 => Ok(None),
            None => Err(RevisionManifestError::MissingParentAfterFirstSequence {
                sequence: manifest.sequence(),
            }
            .into()),
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
        request: &BlobPublicationRequest,
        original: Option<&ReadHead>,
        intended_revision: Checksum,
        intended_sequence: u64,
        failure: ObjectStoreErrorKind,
    ) -> Result<PublicationOutcome, BlobPublicationError> {
        let current = match self.read_head().await {
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
                request.authorization,
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

#[cfg(fuzzing)]
fn fuzz_trust() -> PublicationTrustStore {
    const PUBLIC_KEY: [u8; 32] = [
        0xd5, 0x89, 0x3f, 0x17, 0xe4, 0xba, 0x43, 0xd4, 0x0e, 0xce, 0xe2, 0xc7, 0x65, 0x45, 0xa7,
        0x5a, 0xe4, 0x6c, 0x23, 0xca, 0xf1, 0xf6, 0x78, 0x02, 0x7e, 0x66, 0x2c, 0x9f, 0x6e, 0x82,
        0xd2, 0x05,
    ];
    PublicationTrustStore::new([super::publication_auth::TrustedPublicationKey::ed25519_v1(
        super::publication_auth::PublicationKeyId::parse("publication-test-key")
            .expect("fixed fuzz key id"),
        &PUBLIC_KEY,
    )
    .expect("fixed fuzz public key")])
    .expect("fixed fuzz trust")
}

#[cfg(fuzzing)]
pub(crate) fn fuzz_decode_head(bytes: &[u8]) -> Result<(), (&'static str, String)> {
    let environment = EnvironmentId::parse("production-us-east").expect("fixed environment");
    HeadDocument::verify(bytes, &environment, 0, &fuzz_trust())
        .map(|_| ())
        .map_err(|error| {
            let code = match &error {
                HeadDocumentError::Oversized { .. } => "head_oversized",
                HeadDocumentError::Malformed => "head_malformed",
                HeadDocumentError::Unsigned => "head_unsigned",
                HeadDocumentError::UnknownSchema { .. } => "head_unknown_schema",
                HeadDocumentError::InvalidEnvironment => "head_invalid_environment",
                HeadDocumentError::EnvironmentMismatch => "head_environment_mismatch",
                HeadDocumentError::InvalidDigest => "head_invalid_digest",
                HeadDocumentError::ZeroSequence => "head_zero_sequence",
                HeadDocumentError::UnknownIntegrityAlgorithm => "head_unknown_integrity_algorithm",
                HeadDocumentError::InvalidIntegrityDigest => "head_invalid_integrity_digest",
                HeadDocumentError::IntegrityMismatch => "head_integrity_mismatch",
                HeadDocumentError::Authentication(error) => match error {
                    PublicationAuthenticationError::UnknownSignatureSchema { .. } => {
                        "head_unknown_signature_schema"
                    }
                    PublicationAuthenticationError::UnknownAlgorithm => "head_unknown_algorithm",
                    PublicationAuthenticationError::UnknownKey => "head_unknown_key",
                    PublicationAuthenticationError::InvalidSignatureEncoding => {
                        "head_invalid_signature_encoding"
                    }
                    PublicationAuthenticationError::InvalidSignature => "head_invalid_signature",
                    _ => "head_invalid_authentication",
                },
                HeadDocumentError::Rollback { .. } => "head_rollback",
                HeadDocumentError::MissingBelowFloor { .. } => "head_missing_below_floor",
                HeadDocumentError::NonCanonical => "head_non_canonical",
            };
            (code, error.to_string())
        })
}

#[cfg(fuzzing)]
pub(crate) fn fuzz_decode_revision_manifest(bytes: &[u8]) -> Result<(), (&'static str, String)> {
    let environment = EnvironmentId::parse("production-us-east").expect("fixed environment");
    BlobRevisionManifest::decode_and_verify(bytes, &environment, Checksum::of(bytes), &fuzz_trust())
        .map(|_| ())
        .map_err(|error| {
            let code = match &error {
                RevisionManifestError::Oversized { .. } => "manifest_oversized",
                RevisionManifestError::Malformed => "manifest_malformed",
                RevisionManifestError::Unsigned => "manifest_unsigned",
                RevisionManifestError::UnknownSchema { .. } => "manifest_unknown_schema",
                RevisionManifestError::InvalidEnvironment => "manifest_invalid_environment",
                RevisionManifestError::EnvironmentMismatch => "manifest_environment_mismatch",
                RevisionManifestError::InvalidMutation => "manifest_invalid_mutation",
                RevisionManifestError::UnknownMutationKind { .. } => {
                    "manifest_unknown_mutation_kind"
                }
                RevisionManifestError::DigestMismatch => "manifest_digest_mismatch",
                RevisionManifestError::Authentication(error) => match error {
                    PublicationAuthenticationError::UnknownSignatureSchema { .. } => {
                        "manifest_unknown_signature_schema"
                    }
                    PublicationAuthenticationError::UnknownAlgorithm => {
                        "manifest_unknown_algorithm"
                    }
                    PublicationAuthenticationError::UnknownKey => "manifest_unknown_key",
                    PublicationAuthenticationError::InvalidSignatureEncoding => {
                        "manifest_invalid_signature_encoding"
                    }
                    PublicationAuthenticationError::InvalidSignature => {
                        "manifest_invalid_signature"
                    }
                    _ => "manifest_invalid_authentication",
                },
                RevisionManifestError::ZeroSequence => "manifest_zero_sequence",
                RevisionManifestError::SequenceMismatch { .. } => "manifest_sequence_mismatch",
                RevisionManifestError::ParentBeforeFirstSequence => {
                    "manifest_parent_before_first_sequence"
                }
                RevisionManifestError::MissingParentAfterFirstSequence { .. } => {
                    "manifest_missing_parent_after_first_sequence"
                }
                RevisionManifestError::TooManyObjects { .. } => "manifest_too_many_objects",
                RevisionManifestError::NonCanonicalObjects => "manifest_non_canonical_objects",
            };
            (code, error.to_string())
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

    use async_trait::async_trait;
    use ring::rand::SystemRandom;
    use ring::signature::Ed25519KeyPair;
    use tokio::sync::Barrier;

    use crate::backends::object_store::{
        InMemoryObjectStore, ObjectStoreLimits, ObjectStoreOperation, ObjectValue,
    };

    use super::*;

    const NO_FAULT: u8 = 0;
    const FAIL_BEFORE_HEAD: u8 = 1;
    const FAIL_AFTER_HEAD: u8 = 2;
    const TEST_SIGNING_KEY_PKCS8_BASE64: &str = "MFMCAQEwBQYDK2VwBCIEIOn86WlkmKxquZ/ElW4lZfyxCVYnoaMnF56WoS4ICpKVoSMDIQDViT8X5LpD1A7O4sdlRada5GwjyvH2eAJ+ZiyfboLSBQ==";

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

    fn authorization() -> PublicationAuthorization {
        PublicationAuthorization::new(
            PublicationActorBinding::of(b"test-operator"),
            PublicationGrantBinding::of(b"test-admin-grant"),
            MutationId::new(
                crate::desired_state::Uuid7::from_parts(7, 0, 7).expect("valid mutation id"),
            ),
            MutationKind::Update,
        )
    }

    fn signer() -> Arc<PublicationSigner> {
        let pkcs8 = BASE64_STANDARD
            .decode(TEST_SIGNING_KEY_PKCS8_BASE64)
            .expect("fixed test signing key");
        Arc::new(
            PublicationSigner::from_ed25519_pkcs8(
                super::super::PublicationKeyId::parse("publication-test-key")
                    .expect("valid key id"),
                &pkcs8,
            )
            .expect("valid test signer"),
        )
    }

    fn fresh_signer(key_id: &str) -> Arc<PublicationSigner> {
        let pkcs8 =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("fresh test signing key");
        Arc::new(
            PublicationSigner::from_ed25519_pkcs8(
                super::super::PublicationKeyId::parse(key_id).expect("valid key id"),
                pkcs8.as_ref(),
            )
            .expect("valid fresh signer"),
        )
    }

    fn trust() -> PublicationTrustStore {
        PublicationTrustStore::new([signer().trusted_key()]).expect("test trust")
    }

    fn verify_head(bytes: &[u8]) -> Result<HeadDocument, HeadDocumentError> {
        HeadDocument::verify(bytes, &environment(), 0, &trust())
    }

    fn request(
        expected: ExpectedHead,
        key: &str,
        state: &'static [u8],
        object: ImmutableObject,
    ) -> BlobPublicationRequest {
        BlobPublicationRequest {
            expected,
            authorization: authorization(),
            idempotency_key: IdempotencyKey::parse(key).expect("valid idempotency key"),
            desired_state_checksum: Checksum::of(state),
            objects: vec![object],
        }
    }

    fn publisher<S: ObjectStore>(store: Arc<S>) -> BlobPublication<S> {
        BlobPublication::new(
            store,
            environment(),
            IdempotencyHistoryLimit::new(NonZeroUsize::new(32).expect("non-zero history")),
            signer(),
            trust(),
            0,
        )
        .expect("trusted test publisher")
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
        let signer = signer();
        let head = HeadDocument::sign(environment(), Checksum::of(b"revision"), 7, &signer)
            .expect("valid head");
        let encoded = head.encode();
        assert_eq!(verify_head(&encoded), Ok(head));

        let unknown = String::from_utf8(encoded.to_vec())
            .expect("head is UTF-8")
            .replacen("\"schema_version\":2", "\"schema_version\":3", 1);
        assert!(matches!(
            verify_head(unknown.as_bytes()),
            Err(HeadDocumentError::UnknownSchema { found: 3 })
        ));
        let invalid_digest = String::from_utf8(encoded.to_vec())
            .expect("head is UTF-8")
            .replacen("sha256:", "sha257:", 1);
        assert!(matches!(
            verify_head(invalid_digest.as_bytes()),
            Err(HeadDocumentError::InvalidDigest)
        ));
        let overflow = br#"{"schema_version":2,"environment":"production-us-east","active_revision":"sha256:0000000000000000000000000000000000000000000000000000000000000000","sequence":18446744073709551616,"integrity":{"algorithm":"sha256","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000"},"signature":null}"#;
        assert_eq!(verify_head(overflow), Err(HeadDocumentError::Malformed));
        assert!(matches!(
            verify_head(&vec![b'x'; MAX_HEAD_DOCUMENT_BYTES + 1]),
            Err(HeadDocumentError::Oversized { .. })
        ));

        let mut unsigned: serde_json::Value = serde_json::from_slice(&encoded).expect("head JSON");
        unsigned
            .as_object_mut()
            .expect("head object")
            .remove("signature");
        assert_eq!(
            verify_head(&serde_json::to_vec(&unsigned).expect("unsigned JSON")),
            Err(HeadDocumentError::Unsigned)
        );

        let diagnostic_canary = "Bearer-head-diagnostic-canary";
        let mut invalid: serde_json::Value = serde_json::from_slice(&encoded).expect("head JSON");
        invalid["active_revision"] = serde_json::Value::String(diagnostic_canary.to_owned());
        let error = verify_head(&serde_json::to_vec(&invalid).expect("invalid head"))
            .expect_err("invalid digest");
        assert!(!error.to_string().contains(diagnostic_canary));
        assert!(!format!("{error:?}").contains(diagnostic_canary));

        let mut invalid: serde_json::Value = serde_json::from_slice(&encoded).expect("head JSON");
        invalid["integrity"]["algorithm"] = serde_json::Value::String(diagnostic_canary.to_owned());
        let error = verify_head(&serde_json::to_vec(&invalid).expect("invalid head"))
            .expect_err("unknown integrity algorithm");
        assert!(!error.to_string().contains(diagnostic_canary));
        assert!(!format!("{error:?}").contains(diagnostic_canary));
    }

    #[test]
    fn authenticated_heads_refuse_tamper_replay_wrong_keys_and_rollback() {
        let signer = signer();
        let head = HeadDocument::sign(environment(), Checksum::of(b"revision-seven"), 7, &signer)
            .expect("signed head");
        let encoded = head.encode();

        let other_environment = EnvironmentId::parse("staging-us-east").expect("environment");
        assert_eq!(
            HeadDocument::verify(&encoded, &other_environment, 0, &trust()),
            Err(HeadDocumentError::EnvironmentMismatch)
        );

        let tampered = String::from_utf8(encoded.to_vec())
            .expect("head JSON")
            .replace(
                &Checksum::of(b"revision-seven").to_string(),
                &Checksum::of(b"tampered-revision").to_string(),
            );
        assert_eq!(
            verify_head(tampered.as_bytes()),
            Err(HeadDocumentError::IntegrityMismatch)
        );

        let wrong_signer = fresh_signer(signer.key_id().as_str());
        let wrong_trust =
            PublicationTrustStore::new([wrong_signer.trusted_key()]).expect("wrong-key trust");
        assert_eq!(
            HeadDocument::verify(&encoded, &environment(), 0, &wrong_trust),
            Err(HeadDocumentError::Authentication(
                PublicationAuthenticationError::InvalidSignature
            ))
        );

        let unknown_key_id = "x".repeat(signer.key_id().as_str().len());
        let unknown_key = String::from_utf8(encoded.to_vec())
            .expect("head JSON")
            .replace(signer.key_id().as_str(), &unknown_key_id);
        assert_eq!(
            verify_head(unknown_key.as_bytes()),
            Err(HeadDocumentError::Authentication(
                PublicationAuthenticationError::UnknownKey
            ))
        );

        let unknown_algorithm = String::from_utf8(encoded.to_vec())
            .expect("head JSON")
            .replace("ed25519.v1", "ed25519.v9");
        assert_eq!(
            verify_head(unknown_algorithm.as_bytes()),
            Err(HeadDocumentError::Authentication(
                PublicationAuthenticationError::UnknownAlgorithm
            ))
        );

        let unknown_signature_schema = String::from_utf8(encoded.to_vec())
            .expect("head JSON")
            .replace("\"schema_version\":1", "\"schema_version\":9");
        assert_eq!(
            verify_head(unknown_signature_schema.as_bytes()),
            Err(HeadDocumentError::Authentication(
                PublicationAuthenticationError::UnknownSignatureSchema { found: 9 }
            ))
        );

        let guard = PublicationSequenceGuard::new(environment(), 0);
        guard
            .verify_head(&encoded, &trust())
            .expect("first verified observation");
        let older = HeadDocument::sign(environment(), Checksum::of(b"revision-six"), 6, &signer)
            .expect("signed older head")
            .encode();
        assert_eq!(
            guard.verify_head(&older, &trust()),
            Err(HeadDocumentError::Rollback {
                minimum: 7,
                actual: 6,
            })
        );
        assert_eq!(
            guard.verify_absent(),
            Err(HeadDocumentError::MissingBelowFloor { minimum: 7 })
        );
    }

    #[test]
    fn manifest_signature_binds_every_publication_decision() {
        let signer = signer();
        let manifest = BlobRevisionManifest::sign(
            environment(),
            Some(Checksum::of(b"parent-six")),
            7,
            authorization(),
            Checksum::of(b"idempotency-binding"),
            Checksum::of(b"desired-state"),
            vec![ImmutableReference {
                kind: ImmutableObjectKind::NamespaceResource,
                digest: Checksum::of(b"namespace-body"),
            }],
            &signer,
        )
        .expect("signed manifest");
        let encoded = manifest.encode().expect("manifest bytes");
        VerifiedRevisionManifest::verify(
            &encoded,
            &environment(),
            Checksum::of(&encoded),
            7,
            &trust(),
        )
        .expect("valid manifest");
        let duplicate_reference = manifest.objects[0];
        assert_eq!(
            BlobRevisionManifest::sign(
                environment(),
                Some(Checksum::of(b"parent-six")),
                7,
                authorization(),
                Checksum::of(b"idempotency-binding"),
                Checksum::of(b"desired-state"),
                vec![duplicate_reference, duplicate_reference],
                &signer,
            ),
            Err(RevisionManifestError::NonCanonicalObjects)
        );
        assert_eq!(
            VerifiedRevisionManifest::verify(
                &encoded,
                &EnvironmentId::parse("staging-us-east").expect("environment"),
                Checksum::of(&encoded),
                7,
                &trust(),
            ),
            Err(RevisionManifestError::EnvironmentMismatch)
        );

        let assert_signature_refusal = |mutated: BlobRevisionManifest, expected_sequence| {
            let bytes = mutated.encode().expect("mutated manifest bytes");
            assert_eq!(
                VerifiedRevisionManifest::verify(
                    &bytes,
                    &environment(),
                    Checksum::of(&bytes),
                    expected_sequence,
                    &trust(),
                ),
                Err(RevisionManifestError::Authentication(
                    PublicationAuthenticationError::InvalidSignature
                ))
            );
        };

        let mut parent = manifest.clone();
        parent.parent = Some(Checksum::of(b"different-parent"));
        assert_signature_refusal(parent, 7);

        let mut sequence = manifest.clone();
        sequence.sequence = 8;
        assert_signature_refusal(sequence, 8);

        let mut actor = manifest.clone();
        actor.authorization.actor = PublicationActorBinding::of(b"other-operator");
        assert_signature_refusal(actor, 7);

        let mut grant = manifest.clone();
        grant.authorization.grant = PublicationGrantBinding::of(b"other-grant");
        assert_signature_refusal(grant, 7);

        let mut mutation = manifest.clone();
        mutation.authorization.mutation =
            MutationId::new(crate::desired_state::Uuid7::from_parts(8, 0, 8).expect("mutation id"));
        assert_signature_refusal(mutation, 7);

        let mut mutation_kind = manifest.clone();
        mutation_kind.authorization.mutation_kind = MutationKind::Rollback;
        assert_signature_refusal(mutation_kind, 7);

        let mut idempotency = manifest.clone();
        idempotency.idempotency_binding = Checksum::of(b"different-idempotency");
        assert_signature_refusal(idempotency, 7);

        let mut desired_state = manifest.clone();
        desired_state.desired_state_checksum = Checksum::of(b"different-state");
        assert_signature_refusal(desired_state, 7);

        let mut objects = manifest.clone();
        objects.objects[0].digest = Checksum::of(b"different-object");
        assert_signature_refusal(objects, 7);

        let mut signature = manifest.clone();
        let mut value = *signature.signature.value();
        value[0] ^= 0x80;
        signature.signature = PublicationSignature::decode(
            signature.signature.schema_version(),
            signature.signature.algorithm().as_str(),
            signature.signature.key_id().as_str(),
            &value,
        )
        .expect("mutated signature shape");
        assert_signature_refusal(signature, 7);

        let wrong_signer = fresh_signer(signer.key_id().as_str());
        let wrong_trust =
            PublicationTrustStore::new([wrong_signer.trusted_key()]).expect("wrong-key trust");
        assert_eq!(
            VerifiedRevisionManifest::verify(
                &encoded,
                &environment(),
                Checksum::of(&encoded),
                7,
                &wrong_trust,
            ),
            Err(RevisionManifestError::Authentication(
                PublicationAuthenticationError::InvalidSignature
            ))
        );

        let mut unknown_key = manifest.clone();
        let unknown_key_id = "x".repeat(unknown_key.signature.key_id().as_str().len());
        unknown_key.signature = PublicationSignature::decode(
            unknown_key.signature.schema_version(),
            unknown_key.signature.algorithm().as_str(),
            &unknown_key_id,
            unknown_key.signature.value(),
        )
        .expect("unknown key signature shape");
        let unknown_key = unknown_key.encode().expect("unknown-key manifest");
        assert_eq!(
            VerifiedRevisionManifest::verify(
                &unknown_key,
                &environment(),
                Checksum::of(&unknown_key),
                7,
                &trust(),
            ),
            Err(RevisionManifestError::Authentication(
                PublicationAuthenticationError::UnknownKey
            ))
        );

        let unknown_algorithm = encoded
            .windows("ed25519.v1".len())
            .position(|window| window == b"ed25519.v1")
            .expect("algorithm in manifest");
        let mut unknown_algorithm_bytes = encoded.to_vec();
        unknown_algorithm_bytes[unknown_algorithm..unknown_algorithm + "ed25519.v1".len()]
            .copy_from_slice(b"ed25519.v9");
        assert_eq!(
            VerifiedRevisionManifest::verify(
                &unknown_algorithm_bytes,
                &environment(),
                Checksum::of(&unknown_algorithm_bytes),
                7,
                &trust(),
            ),
            Err(RevisionManifestError::Authentication(
                PublicationAuthenticationError::UnknownAlgorithm
            ))
        );

        let mut unknown_signature_schema = encoded.to_vec();
        let signature_metadata = unknown_signature_schema
            .windows(3)
            .rposition(|window| window == [0x84, 0x01, 0x6a])
            .expect("signature metadata tuple");
        unknown_signature_schema[signature_metadata + 1] = 9;
        assert_eq!(
            VerifiedRevisionManifest::verify(
                &unknown_signature_schema,
                &environment(),
                Checksum::of(&unknown_signature_schema),
                7,
                &trust(),
            ),
            Err(RevisionManifestError::Authentication(
                PublicationAuthenticationError::UnknownSignatureSchema { found: 9 }
            ))
        );

        let mut unknown_schema = encoded.to_vec();
        assert_eq!(unknown_schema[0], 0x89, "manifest is a nine-item array");
        assert_eq!(unknown_schema[1], MANIFEST_SCHEMA_VERSION as u8);
        unknown_schema[1] = 3;
        assert_eq!(
            VerifiedRevisionManifest::verify(
                &unknown_schema,
                &environment(),
                Checksum::of(&unknown_schema),
                7,
                &trust(),
            ),
            Err(RevisionManifestError::UnknownSchema { found: 3 })
        );

        let unsigned_v2 = [0x88, MANIFEST_SCHEMA_VERSION as u8];
        assert_eq!(
            VerifiedRevisionManifest::verify(
                &unsigned_v2,
                &environment(),
                Checksum::of(&unsigned_v2),
                7,
                &trust(),
            ),
            Err(RevisionManifestError::Unsigned)
        );
    }

    #[test]
    fn bootstrap_trust_supports_key_rotation_overlap_and_refuses_unknown_signers() {
        let first = signer();
        let next = fresh_signer("publication-next-key");
        let overlap = PublicationTrustStore::new([first.trusted_key(), next.trusted_key()])
            .expect("overlapping trust");
        for (sequence, signer) in [(1, &first), (2, &next)] {
            let head = HeadDocument::sign(
                environment(),
                Checksum::of(format!("revision-{sequence}").as_bytes()),
                sequence,
                signer,
            )
            .expect("signed rotation head")
            .encode();
            HeadDocument::verify(&head, &environment(), sequence, &overlap)
                .expect("both rotation keys verify during overlap");
        }

        let store = Arc::new(InMemoryObjectStore::new(limits()));
        let only_next = PublicationTrustStore::new([next.trusted_key()]).expect("next trust");
        assert!(matches!(
            BlobPublication::new(
                store,
                environment(),
                IdempotencyHistoryLimit::new(NonZeroUsize::new(32).expect("non-zero history")),
                first,
                only_next,
                0,
            ),
            Err(PublicationAuthenticationError::UnknownKey)
        ));
    }

    #[test]
    fn head_and_manifest_signatures_cannot_cross_protocol_domains() {
        let signer = signer();
        let trust = trust();
        let head = HeadDocument::sign(environment(), Checksum::of(b"revision"), 1, &signer)
            .expect("signed head");
        let manifest = BlobRevisionManifest::sign(
            environment(),
            None,
            1,
            authorization(),
            Checksum::of(b"idempotency"),
            Checksum::of(b"state"),
            Vec::new(),
            &signer,
        )
        .expect("signed manifest");
        let head_bytes = HeadDocument::signature_bytes(
            &head.environment,
            head.active_revision,
            head.sequence,
            head.integrity,
            head.signature.schema_version(),
            head.signature.key_id().as_str(),
            head.signature.algorithm().as_str(),
        );
        let manifest_bytes = BlobRevisionManifest::signature_bytes(
            &manifest.environment,
            manifest.parent,
            manifest.sequence,
            manifest.authorization,
            manifest.idempotency_binding,
            manifest.desired_state_checksum,
            &manifest.objects,
            manifest.signature.schema_version(),
            manifest.signature.algorithm().as_str(),
            manifest.signature.key_id().as_str(),
        )
        .expect("manifest signature bytes");
        assert_eq!(
            trust.verify(&manifest.signature, &head_bytes),
            Err(PublicationAuthenticationError::InvalidSignature)
        );
        assert_eq!(
            trust.verify(&head.signature, &manifest_bytes),
            Err(PublicationAuthenticationError::InvalidSignature)
        );
    }

    #[test]
    fn signed_publication_fixture_is_stable() {
        let manifest = BlobRevisionManifest::sign(
            environment(),
            None,
            1,
            authorization(),
            Checksum::of(b"fixture-idempotency"),
            Checksum::of(b"fixture-state"),
            vec![ImmutableReference {
                kind: ImmutableObjectKind::NamespaceResource,
                digest: Checksum::of(b"fixture-namespace"),
            }],
            &signer(),
        )
        .expect("fixture manifest");
        let manifest = manifest.encode().expect("fixture manifest bytes");
        let head = HeadDocument::sign(environment(), Checksum::of(&manifest), 1, &signer())
            .expect("fixture head")
            .encode();
        assert!(verify_head(&head).is_ok());
        assert!(
            VerifiedRevisionManifest::verify(
                &manifest,
                &environment(),
                Checksum::of(&manifest),
                1,
                &trust(),
            )
            .is_ok()
        );
        assert_eq!(
            HeadDocument::verify(&head, &environment(), 0, &trust())
                .expect("verified fixture head")
                .encode(),
            head
        );
    }

    #[test]
    fn revision_parent_shape_fails_closed_at_both_sequence_boundaries() {
        let manifest = |parent, sequence| {
            BlobRevisionManifest::sign(
                environment(),
                parent,
                sequence,
                authorization(),
                Checksum::of(b"idempotency"),
                Checksum::of(b"state"),
                Vec::new(),
                &signer(),
            )
            .expect("signed manifest")
            .encode()
            .expect("encoded manifest")
        };
        let before_first_bytes = manifest(Some(Checksum::of(b"impossible-parent")), 1);
        let before_first = VerifiedRevisionManifest::verify(
            &before_first_bytes,
            &environment(),
            Checksum::of(&before_first_bytes),
            1,
            &trust(),
        )
        .expect_err("sequence one cannot have a parent");
        assert!(matches!(
            before_first,
            RevisionManifestError::ParentBeforeFirstSequence
        ));

        let missing_bytes = manifest(None, 2);
        let missing = VerifiedRevisionManifest::verify(
            &missing_bytes,
            &environment(),
            Checksum::of(&missing_bytes),
            2,
            &trust(),
        )
        .expect_err("later sequences must link to their predecessor");
        assert!(matches!(
            missing,
            RevisionManifestError::MissingParentAfterFirstSequence { sequence: 2 }
        ));
    }

    #[tokio::test]
    async fn immutable_uploads_and_manifest_are_confirmed_before_head() {
        let store = Arc::new(RecordingFaultStore::new());
        let publication = publisher(Arc::clone(&store));
        publication
            .publish(request(
                ExpectedHead::Empty,
                "create-1",
                b"state-1",
                immutable(b"namespace-1"),
            ))
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
            .publish(request(
                ExpectedHead::Empty,
                "create-1",
                b"state-1",
                immutable(b"namespace-1"),
            ))
            .await
            .expect("first publication");
        store.set_fault(FAIL_BEFORE_HEAD);
        let unreachable = immutable(b"namespace-2");
        let unreachable_key = unreachable.key();
        let error = publication
            .publish(request(
                ExpectedHead::Revision(first.revision),
                "create-2",
                b"state-2",
                unreachable,
            ))
            .await
            .expect_err("head write is unavailable");
        assert_eq!(error, BlobPublicationError::AmbiguousUnavailable);

        let head = store
            .inner
            .get(&environment_head_key(&environment()))
            .await
            .expect("old head");
        assert_eq!(
            verify_head(&head.bytes)
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
            .publish(request(
                ExpectedHead::Empty,
                "create-1",
                b"state-1",
                immutable(b"namespace-1"),
            ))
            .await
            .expect("first publication");

        let left = tokio::spawn({
            let publication = publication.clone();
            async move {
                publication
                    .publish(request(
                        ExpectedHead::Revision(first.revision),
                        "left",
                        b"state-left",
                        immutable(b"namespace-left"),
                    ))
                    .await
            }
        });
        let right = tokio::spawn({
            let publication = publication.clone();
            async move {
                publication
                    .publish(request(
                        ExpectedHead::Revision(first.revision),
                        "right",
                        b"state-right",
                        immutable(b"namespace-right"),
                    ))
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
            .publish(request(
                ExpectedHead::Empty,
                "one",
                b"state-1",
                shared.clone(),
            ))
            .await
            .expect("first publication");
        publication
            .publish(request(
                ExpectedHead::Revision(first.revision),
                "two",
                b"state-2",
                shared,
            ))
            .await
            .expect("same immutable bytes are confirmed and reused");

        let collision = immutable(b"address-owner");
        store
            .put_if_absent(&collision.key(), Bytes::from_static(b"different-bytes"))
            .await
            .expect("seed impossible provider corruption");
        let head = verify_head(
            &store
                .get(&environment_head_key(&environment()))
                .await
                .expect("head")
                .bytes,
        )
        .expect("valid head");
        let error = publication
            .publish(request(
                ExpectedHead::Revision(head.active_revision()),
                "three",
                b"state-3",
                collision.clone(),
            ))
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
            .publish(request(
                ExpectedHead::Empty,
                "lost-response",
                b"state-1",
                immutable(b"namespace-1"),
            ))
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
            .publish(original_request.clone())
            .await
            .expect("first publication");
        let replay = publication
            .publish(original_request)
            .await
            .expect("stale Empty expectation is ignored for an exact replay");
        assert_eq!(replay.revision, first.revision);
        assert_eq!(replay.sequence, first.sequence);
        assert!(replay.replayed);

        let error = publication
            .publish(request(
                ExpectedHead::Empty,
                "retry-me",
                b"different-state",
                immutable(b"namespace-other"),
            ))
            .await
            .expect_err("key reuse must win over stale-head reporting");
        assert!(matches!(error, BlobPublicationError::IdempotencyKeyReuse));
    }

    #[tokio::test]
    async fn idempotency_is_authorization_scoped_and_durable_metadata_is_redacted() {
        let store = Arc::new(InMemoryObjectStore::new(limits()));
        let publication = publisher(Arc::clone(&store));
        let secret_shaped_key = "Bearer-do-not-persist-or-render";
        let mut first_request = request(
            ExpectedHead::Empty,
            secret_shaped_key,
            b"state-one",
            immutable(b"namespace-one"),
        );
        first_request.authorization = PublicationAuthorization::new(
            PublicationActorBinding::of(b"operator-private-subject"),
            PublicationGrantBinding::of(b"grant-private-description"),
            first_request.authorization.mutation(),
            MutationKind::Create,
        );
        let first = publication
            .publish(first_request.clone())
            .await
            .expect("first publication");

        let manifest_bytes = store
            .get(&revision_manifest_key(first.revision))
            .await
            .expect("stored manifest")
            .bytes;
        for forbidden in [
            secret_shaped_key.as_bytes(),
            b"operator-private-subject".as_slice(),
            b"grant-private-description".as_slice(),
        ] {
            assert!(
                !manifest_bytes
                    .windows(forbidden.len())
                    .any(|window| window == forbidden),
                "raw attribution or idempotency input entered durable metadata"
            );
        }

        let mut other_authority = first_request.clone();
        other_authority.expected = ExpectedHead::Revision(first.revision);
        other_authority.authorization = PublicationAuthorization::new(
            PublicationActorBinding::of(b"different-operator"),
            PublicationGrantBinding::of(b"different-grant"),
            MutationId::new(crate::desired_state::Uuid7::from_parts(9, 0, 9).expect("mutation id")),
            MutationKind::Update,
        );
        other_authority.desired_state_checksum = Checksum::of(b"state-two");
        other_authority.objects = vec![immutable(b"namespace-two")];
        let second = publication
            .publish(other_authority)
            .await
            .expect("another authority owns an independent idempotency scope");
        assert_eq!(second.sequence, 2);

        let mut reuse = first_request;
        reuse.desired_state_checksum = Checksum::of(b"different-state");
        reuse.objects = vec![immutable(b"different-namespace")];
        let error = publication
            .publish(reuse)
            .await
            .expect_err("same authority cannot reuse a binding for different state");
        assert_eq!(error, BlobPublicationError::IdempotencyKeyReuse);
        let rendered = error.to_string();
        assert!(!rendered.contains(secret_shaped_key));
        assert!(!rendered.contains("operator-private-subject"));
        assert!(!rendered.contains("grant-private-description"));
    }

    #[tokio::test]
    async fn bounded_history_exhaustion_is_visible_and_fails_novel_writes_closed() {
        let store = Arc::new(InMemoryObjectStore::new(limits()));
        let full = publisher(Arc::clone(&store));
        let first = full
            .publish(request(
                ExpectedHead::Empty,
                "one",
                b"state-1",
                immutable(b"namespace-1"),
            ))
            .await
            .expect("first");
        let bounded = BlobPublication::new(
            Arc::clone(&store),
            environment(),
            IdempotencyHistoryLimit::new(NonZeroUsize::new(1).expect("non-zero history")),
            signer(),
            trust(),
            0,
        )
        .expect("trusted bounded publisher");
        let searchable = bounded
            .idempotency_history_status()
            .await
            .expect("history at its exact bound remains searchable");
        assert_eq!(
            searchable,
            IdempotencyHistoryStatus::Searchable {
                retained_revisions: 1,
                limit: bounded.history_limit(),
            }
        );
        assert!(searchable.permits_novel_publication());

        let second = full
            .publish(request(
                ExpectedHead::Revision(first.revision),
                "two",
                b"state-2",
                immutable(b"namespace-2"),
            ))
            .await
            .expect("second");

        assert_eq!(bounded.history_limit().get(), 1);
        let status = bounded
            .idempotency_history_status()
            .await
            .expect("history status");
        assert_eq!(
            status,
            IdempotencyHistoryStatus::Exhausted {
                inspected_revisions: 1,
                limit: bounded.history_limit(),
            }
        );
        assert!(!status.permits_novel_publication());

        let replay = bounded
            .publish(request(
                ExpectedHead::Empty,
                "two",
                b"state-2",
                immutable(b"namespace-2"),
            ))
            .await
            .expect("an exact replay inside the window remains available");
        assert_eq!(replay.revision, second.revision);
        assert!(replay.replayed);

        let error = bounded
            .publish(request(
                ExpectedHead::Revision(second.revision),
                "three",
                b"state-3",
                immutable(b"namespace-3"),
            ))
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
                .publish(request(expected, key, state, immutable(body)))
                .await
                .expect("publication");
            assert_eq!(outcome.sequence, index as u64 + 1);
            expected = ExpectedHead::Revision(outcome.revision);
        }
    }
}
