//! Namespace-native envelope encryption for immutable blob secret objects.
//!
//! This is the v2 object format. It is deliberately separate from
//! [`super::envelope`]: changing the legacy row envelope would make existing
//! Postgres material unreadable, while accepting either format through one
//! decoder would weaken both formats' compatibility refusal.
//!
//! A stored object is one deterministic canonical-CBOR fixed array:
//!
//! ```text
//! [2, "aes256-kw.aes256-gcm.envelope.v2", kek_id,
//!  wrapped_dek, material_nonce, ciphertext]
//! ```
//!
//! The deployment environment, namespace owner, and exact [`SecretRef`] are
//! intentionally absent. They come from an authenticated desired-state
//! manifest and are authenticated as binary, length-prefixed additional data
//! for the material AEAD. The fixed 32-byte DEK is wrapped with nonce-free RFC
//! 3394 AES-256-KW. Copying an object to another environment, namespace, secret
//! id, version, or KEK id therefore cannot make the complete object open.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use aes_kw::{KeyInit as _, KwAes256};
use async_trait::async_trait;
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::digest::{SHA256, digest};
use ring::rand::{SecureRandom, SystemRandom};
use secrecy::zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

use super::{
    Capabilities, ENVELOPE_CAPABILITIES, SecretError, SecretMaterial, SecretOwner, SecretResolver,
};
use crate::backends::object_store::{ObjectStore, ObjectStoreError};
use crate::desired_state::namespaces::NamespaceSecretRequest;
use crate::desired_state::secrets::SecretRef;
use crate::desired_state::{
    ActivationReadyRevision, AuthenticatedSecretBinding, BlobPublicationError, BlobReader,
    BlobSecretAuthority, BlobSecretBindingError, BlobSecretPublicationBinding, Checksum,
    EnvironmentId, ImmutableObjectKind,
};
use crate::namespace::NamespaceId;

/// The scheme identifier stored in every v2 blob envelope.
pub const SCHEME: &str = "aes256-kw.aes256-gcm.envelope.v2";

/// AES-256 key size, for key-encryption keys and per-secret data keys.
pub const KEY_BYTES: usize = 32;

/// The maximum plaintext carried by one secret object.
pub const MAX_PLAINTEXT_BYTES: usize = 64 * 1024;

const TAG_BYTES: usize = 16;
const AES_KW_INTEGRITY_BYTES: usize = 8;
const WRAPPED_DEK_BYTES: usize = KEY_BYTES + AES_KW_INTEGRITY_BYTES;
const MAX_CIPHERTEXT_BYTES: usize = MAX_PLAINTEXT_BYTES + TAG_BYTES;
const MAX_KEK_ID_BYTES: usize = 64;
const FIELD_COUNT: u8 = 6;
const SCHEMA_VERSION: u8 = 2;
const AAD_DOMAIN: &[u8] = b"axond.secret.envelope.v2\0";

/// Maximum number of KEKs admitted at one time, including the active key.
pub const MAX_KEK_RING_KEYS: usize = 8;

/// Maximum canonical encoded object size, including the largest legal KEK id.
///
/// Kept as a literal expression over format fields so a format edit cannot
/// silently invalidate the pre-parse allocation bound.
pub const MAX_SEALED_BYTES: usize = 1 // six-element array
    + 1 // schema version
    + 2 + SCHEME.len() // scheme text (u8 length argument)
    + 2 + MAX_KEK_ID_BYTES // KEK id text (one-byte length argument)
    + 2 + WRAPPED_DEK_BYTES // wrapped DEK
    + 1 + NONCE_LEN // material nonce
    + 5 + MAX_CIPHERTEXT_BYTES; // ciphertext (u32 length argument at the ceiling)

/// A stable, non-secret identifier for one KEK in a deployment's key ring.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KekId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidKekId {
    #[error("a KEK identifier must not be empty")]
    Empty,
    #[error("a KEK identifier is over the 64-byte limit")]
    TooLong,
    #[error("a KEK identifier must use only ASCII letters, digits, `.`, `-`, and `_`")]
    Character,
}

impl KekId {
    pub fn parse(input: &str) -> Result<Self, InvalidKekId> {
        if input.is_empty() {
            return Err(InvalidKekId::Empty);
        }
        if input.len() > MAX_KEK_ID_BYTES {
            return Err(InvalidKekId::TooLong);
        }
        if !input
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
        {
            return Err(InvalidKekId::Character);
        }
        Ok(Self(input.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for KekId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for KekId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A consumed, zeroizing bootstrap KEK.
///
/// Construction accepts exactly 32 bytes in an already-zeroizing owned buffer.
/// There is no public byte accessor, formatter, serializer, or clone. Ring
/// construction consumes and zeroizes this staging value after expanding it.
pub(crate) struct KekMaterial(Zeroizing<[u8; KEY_BYTES]>);

impl KekMaterial {
    #[cfg(test)]
    fn from_array(bytes: [u8; KEY_BYTES]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Consume a dynamically sized bootstrap value, refusing any non-AES-256
    /// length. The caller cannot accidentally pass a non-zeroizing owner.
    pub(crate) fn from_owned(bytes: Zeroizing<Vec<u8>>) -> Result<Self, KekRingError> {
        if bytes.len() != KEY_BYTES {
            return Err(KekRingError::KeyLength { found: bytes.len() });
        }
        let mut exact = [0_u8; KEY_BYTES];
        exact.copy_from_slice(&bytes);
        Ok(Self(Zeroizing::new(exact)))
    }

    fn fingerprint(&self) -> KekFingerprint {
        let value = digest(&SHA256, self.0.as_ref());
        let mut fingerprint = [0_u8; 32];
        fingerprint.copy_from_slice(value.as_ref());
        KekFingerprint(fingerprint)
    }

    fn into_key(self) -> Result<KwAes256, KekRingError> {
        KwAes256::new_from_slice(self.0.as_ref()).map_err(|_| KekRingError::KeyRejected)
    }
}

impl fmt::Debug for KekMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KekMaterial(<redacted>)")
    }
}

/// SHA-256 equality token used only while building a ring.
///
/// It is deliberately private, has no formatter or accessor, and is zeroized
/// even though a KEK fingerprint is not itself key material.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct KekFingerprint([u8; 32]);

impl Drop for KekFingerprint {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KekRingError {
    #[error("a key-encryption key must contain exactly 32 bytes, not {found}")]
    KeyLength { found: usize },
    #[error("the AES-256 key-encryption key was rejected")]
    KeyRejected,
    #[error("the key ring already contains that KEK identifier")]
    DuplicateId,
    #[error("different KEK identifiers must not alias the same key material")]
    DuplicateMaterial,
    #[error("a key ring contains more than the {maximum}-key limit")]
    TooMany { maximum: usize },
    #[error("a decrypt key ring must contain at least one KEK")]
    Empty,
}

/// Borrowed internal AAD view. It has no constructor visible outside this
/// module; production callers must supply one of the authority wrappers above.
#[derive(Clone, Copy)]
struct BlobSecretContext<'a> {
    environment: &'a EnvironmentId,
    namespace: &'a NamespaceId,
    reference: &'a SecretRef,
}

impl<'a> From<&'a BlobSecretPublicationBinding> for BlobSecretContext<'a> {
    fn from(binding: &'a BlobSecretPublicationBinding) -> Self {
        Self {
            environment: binding.environment(),
            namespace: binding.owner(),
            reference: binding.reference(),
        }
    }
}

impl<'a> From<&'a AuthenticatedSecretBinding> for BlobSecretContext<'a> {
    fn from(binding: &'a AuthenticatedSecretBinding) -> Self {
        Self {
            environment: binding.environment(),
            namespace: binding.owner(),
            reference: binding.reference(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum AadPurpose {
    Material = 1,
    #[cfg(any(test, fuzzing))]
    InvalidTestDomain = 255,
}

/// A strict parser refusal. No variant carries rejected bytes or text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CodecError {
    #[error("the sealed object exceeds its encoded-size bound")]
    Oversized,
    #[error("the sealed object is truncated")]
    Truncated,
    #[error("the sealed object is not the v2 fixed-array shape")]
    Shape,
    #[error("the sealed object uses a schema or scheme this build does not read")]
    Compatibility,
    #[error("the sealed object does not use deterministic canonical CBOR")]
    NonCanonical,
    #[error("the sealed object contains an invalid KEK identifier")]
    KekId,
    #[error("the sealed object contains an invalid fixed-size field")]
    FixedField,
    #[error("the sealed object contains an invalid ciphertext length")]
    Ciphertext,
    #[error("the sealed object has trailing bytes")]
    Trailing,
}

/// Sealing/opening failure with no plaintext, AAD, key, or ciphertext payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BlobEnvelopeError {
    #[error("secret material must not be empty")]
    EmptyMaterial,
    #[error("secret material exceeds the 64 KiB limit")]
    MaterialTooLarge,
    #[error("secure random bytes are unavailable")]
    Random,
    #[error("the sealed object names no available key-encryption key")]
    UnknownKek,
    #[error("the sealed object does not open in its authenticated context")]
    Unopenable,
    #[error(transparent)]
    Codec(#[from] CodecError),
}

/// A validated v2 sealed object.
///
/// Fields stay private and `Debug` reports only non-sensitive size metadata.
/// Environment, namespace, and reference are not fields because they must come
/// from the authenticated manifest that names this content-addressed object.
#[derive(Clone, PartialEq, Eq)]
pub struct SealedBlobSecret {
    kek_id: KekId,
    wrapped_dek: [u8; WRAPPED_DEK_BYTES],
    material_nonce: [u8; NONCE_LEN],
    ciphertext: Vec<u8>,
}

impl fmt::Debug for SealedBlobSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SealedBlobSecret")
            .field("kek_id", &self.kek_id)
            .field("ciphertext_len", &self.ciphertext.len())
            .finish_non_exhaustive()
    }
}

impl SealedBlobSecret {
    pub fn kek_id(&self) -> &KekId {
        &self.kek_id
    }

    /// Encode the one accepted canonical representation.
    pub fn to_canonical_cbor(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(encoded_size(
            self.kek_id.as_str().len(),
            self.ciphertext.len(),
        ));
        encoded.push(0x80 | FIELD_COUNT);
        encoded.push(SCHEMA_VERSION);
        encode_text(&mut encoded, SCHEME);
        encode_text(&mut encoded, self.kek_id.as_str());
        encode_bytes(&mut encoded, &self.wrapped_dek);
        encode_bytes(&mut encoded, &self.material_nonce);
        encode_bytes(&mut encoded, &self.ciphertext);
        debug_assert!(encoded.len() <= MAX_SEALED_BYTES);
        encoded
    }

    /// Parse only the exact bounded canonical representation.
    pub fn from_canonical_cbor(encoded: &[u8]) -> Result<Self, CodecError> {
        if encoded.len() > MAX_SEALED_BYTES {
            return Err(CodecError::Oversized);
        }
        let mut reader = CborReader::new(encoded);
        reader.expect(0x80 | FIELD_COUNT, CodecError::Shape)?;
        reader.expect(SCHEMA_VERSION, CodecError::Compatibility)?;
        reader.expect_text(SCHEME.as_bytes(), CodecError::Compatibility)?;
        let kek_text = reader.text(MAX_KEK_ID_BYTES)?;
        let kek_text = std::str::from_utf8(kek_text).map_err(|_| CodecError::KekId)?;
        let kek_id = KekId::parse(kek_text).map_err(|_| CodecError::KekId)?;
        let wrapped_dek = reader.bytes_exact::<WRAPPED_DEK_BYTES>()?;
        let material_nonce = reader.bytes_exact::<NONCE_LEN>()?;
        let ciphertext = reader.bytes_bounded(TAG_BYTES + 1, MAX_CIPHERTEXT_BYTES)?;
        reader.finish()?;
        let sealed = Self {
            kek_id,
            wrapped_dek,
            material_nonce,
            ciphertext: ciphertext.to_vec(),
        };
        // The reader enforces every minimal length form. Equality closes the
        // proof over the complete object and guards future field additions.
        if sealed.to_canonical_cbor() != encoded {
            return Err(CodecError::NonCanonical);
        }
        Ok(sealed)
    }
}

/// Bounded decrypt-only KEKs supplied to serving replicas.
///
/// There is no active key and no sealing method. Population validates every id
/// and non-exported raw-material fingerprint before expanding any key, then
/// returns one complete immutable ring or an error.
pub(crate) struct KekDecryptRing {
    keys: BTreeMap<KekId, KwAes256>,
}

impl fmt::Debug for KekDecryptRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KekDecryptRing")
            .field("key_count", &self.keys.len())
            .finish()
    }
}

impl KekDecryptRing {
    /// Atomically populate one bounded decrypt-only set.
    pub(crate) fn from_entries(entries: Vec<(KekId, KekMaterial)>) -> Result<Self, KekRingError> {
        if entries.is_empty() {
            return Err(KekRingError::Empty);
        }
        if entries.len() > MAX_KEK_RING_KEYS {
            return Err(KekRingError::TooMany {
                maximum: MAX_KEK_RING_KEYS,
            });
        }

        assert_zeroize_on_drop::<KwAes256>();
        let mut ids = BTreeSet::new();
        let mut fingerprints = BTreeSet::new();
        for (id, material) in &entries {
            if !ids.insert(id.clone()) {
                return Err(KekRingError::DuplicateId);
            }
            if !fingerprints.insert(material.fingerprint()) {
                return Err(KekRingError::DuplicateMaterial);
            }
        }

        let mut keys = BTreeMap::new();
        for (id, material) in entries {
            keys.insert(id, material.into_key()?);
        }
        Ok(Self { keys })
    }
}

/// The only secret crypto capability a serving resolver receives.
///
/// This type owns decrypt-only keys and has no path to a publication binding,
/// random DEK generation, or a seal API.
pub(super) struct BlobSecretOpener {
    ring: KekDecryptRing,
}

impl fmt::Debug for BlobSecretOpener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlobSecretOpener")
            .field("ring", &self.ring)
            .finish()
    }
}

impl BlobSecretOpener {
    pub(super) const fn new(ring: KekDecryptRing) -> Self {
        Self { ring }
    }

    pub(super) fn open(
        &self,
        binding: AuthenticatedSecretBinding,
        sealed: &SealedBlobSecret,
    ) -> Result<SecretMaterial, BlobEnvelopeError> {
        self.open_with_purpose(binding, sealed, AadPurpose::Material)
    }

    fn open_with_purpose(
        &self,
        binding: AuthenticatedSecretBinding,
        sealed: &SealedBlobSecret,
        material_purpose: AadPurpose,
    ) -> Result<SecretMaterial, BlobEnvelopeError> {
        if Checksum::of(&sealed.to_canonical_cbor()) != binding.ciphertext_digest() {
            return Err(BlobEnvelopeError::Unopenable);
        }
        let context = BlobSecretContext::from(&binding);
        let kek = self
            .ring
            .keys
            .get(&sealed.kek_id)
            .ok_or(BlobEnvelopeError::UnknownKek)?;
        let mut dek = Zeroizing::new([0_u8; KEY_BYTES]);
        kek.unwrap_key(&sealed.wrapped_dek, dek.as_mut())
            .map_err(|_| BlobEnvelopeError::Unopenable)?;
        let data_key = UnboundKey::new(&AES_256_GCM, dek.as_ref())
            .map(LessSafeKey::new)
            .map_err(|_| BlobEnvelopeError::Unopenable)?;

        let mut ciphertext = sealed.ciphertext.clone();
        let opened = data_key
            .open_in_place(
                Nonce::assume_unique_for_key(sealed.material_nonce),
                Aad::from(aad(material_purpose, context, &sealed.kek_id)),
                &mut ciphertext,
            )
            .map_err(|_| BlobEnvelopeError::Unopenable)
            .and_then(|plaintext| {
                std::str::from_utf8(plaintext)
                    .map(|text| SecretMaterial::new(text.to_owned()))
                    .map_err(|_| BlobEnvelopeError::Unopenable)
            });
        ciphertext.zeroize();
        opened
    }
}

/// Candidate-scoped, read-only resolver for immutable blob secret objects.
///
/// The authority is consumed at construction and retains the authenticated
/// candidate's activation witness. The reader is trust-only and the opener
/// owns decrypt-only keys. No generic owner/reference lookup is implemented:
/// every successful resolution must pass through the candidate's exact,
/// active deployment secret index entry.
pub(crate) struct BlobSecretResolver<S> {
    authority: BlobSecretAuthority,
    reader: BlobReader<S>,
    opener: BlobSecretOpener,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum BlobSecretResolverConstructionError {
    #[error("blob reader environment does not match the authenticated candidate environment")]
    EnvironmentMismatch,
}

impl<S: ObjectStore> BlobSecretResolver<S> {
    /// Build a resolver only for the candidate and environment selected by the
    /// authenticated reader. The decrypt ring is consumed and cannot seal.
    pub(crate) fn new(
        authority: BlobSecretAuthority,
        reader: BlobReader<S>,
        ring: KekDecryptRing,
    ) -> Result<Self, BlobSecretResolverConstructionError> {
        if authority.environment().as_str() != reader.environment().as_str() {
            return Err(BlobSecretResolverConstructionError::EnvironmentMismatch);
        }
        Ok(Self {
            authority,
            reader,
            opener: BlobSecretOpener::new(ring),
        })
    }

    /// The activation witness remains owned by the resolver for the lifetime
    /// of the candidate-scoped resolver.
    pub(super) const fn activation(&self) -> &ActivationReadyRevision {
        self.authority.activation()
    }

    async fn resolve_indexed(
        &self,
        request: &NamespaceSecretRequest,
    ) -> Result<SecretMaterial, SecretError> {
        let binding = self
            .authority
            .bind(request)
            .map_err(|error| binding_error(request.reference(), error))?;
        let digest = binding.ciphertext_digest();
        let object = self
            .reader
            .read_immutable_object(ImmutableObjectKind::Secret, digest)
            .await
            .map_err(|error| object_error(request.reference(), error))?;
        let sealed = SealedBlobSecret::from_canonical_cbor(&object.bytes).map_err(|_| {
            SecretError::Corrupt {
                detail: "authenticated blob secret object is malformed".to_owned(),
            }
        })?;
        self.opener
            .open(binding, &sealed)
            .map_err(|error| open_error(request.reference(), sealed.kek_id(), error))
    }
}

fn binding_error(reference: SecretRef, error: BlobSecretBindingError) -> SecretError {
    match error {
        BlobSecretBindingError::Inactive { lifecycle } => SecretError::Lifecycle {
            reference,
            state: lifecycle,
        },
        BlobSecretBindingError::Undeclared | BlobSecretBindingError::Mismatch => {
            SecretError::Denied {
                backend: "blob-secrets",
                message: "namespace secret request is not authorized by the candidate".to_owned(),
            }
        }
    }
}

fn object_error(reference: SecretRef, error: BlobPublicationError) -> SecretError {
    match error {
        BlobPublicationError::Store(ObjectStoreError::NotFound { .. }) => SecretError::Corrupt {
            detail: "authenticated blob secret object is missing".to_owned(),
        },
        BlobPublicationError::Store(ObjectStoreError::Unavailable { .. }) => {
            SecretError::Unavailable {
                backend: "blob-object-store",
                message: "blob object store unavailable while reading candidate secret".to_owned(),
            }
        }
        BlobPublicationError::Store(ObjectStoreError::Integrity { .. })
        | BlobPublicationError::Store(ObjectStoreError::PayloadTooLarge { .. })
        | BlobPublicationError::Store(ObjectStoreError::PreconditionFailed { .. })
        | BlobPublicationError::Authentication(_)
        | BlobPublicationError::Head(_)
        | BlobPublicationError::Manifest(_)
        | BlobPublicationError::ImmutableCollision { .. }
        | BlobPublicationError::ImmutableDigestMismatch { .. }
        | BlobPublicationError::Conflict { .. }
        | BlobPublicationError::AmbiguousUnavailable
        | BlobPublicationError::IdempotencyKeyReuse
        | BlobPublicationError::HistoryLimitExceeded { .. }
        | BlobPublicationError::SequenceOverflow
        | BlobPublicationError::ActiveManifestMismatch
        | BlobPublicationError::ActiveHeadChanged => SecretError::Corrupt {
            detail: format!("authenticated blob secret object could not be read for {reference}"),
        },
    }
}

fn open_error(reference: SecretRef, kek: &KekId, error: BlobEnvelopeError) -> SecretError {
    match error {
        BlobEnvelopeError::UnknownKek | BlobEnvelopeError::Unopenable => SecretError::Unwrap {
            reference,
            kek: super::KekRef(kek.as_str().to_owned()),
        },
        BlobEnvelopeError::EmptyMaterial
        | BlobEnvelopeError::MaterialTooLarge
        | BlobEnvelopeError::Random
        | BlobEnvelopeError::Codec(_) => SecretError::Corrupt {
            detail: "authenticated blob secret object could not be opened".to_owned(),
        },
    }
}

#[async_trait]
impl<S: ObjectStore> SecretResolver for BlobSecretResolver<S> {
    fn name(&self) -> &'static str {
        "blob-secrets"
    }

    fn capabilities(&self) -> Capabilities {
        ENVELOPE_CAPABILITIES
    }

    async fn resolve(
        &self,
        _owner: SecretOwner,
        _reference: &SecretRef,
    ) -> Result<SecretMaterial, SecretError> {
        Err(SecretError::Denied {
            backend: self.name(),
            message: "blob secret resolution requires an authenticated namespace candidate"
                .to_owned(),
        })
    }

    async fn exists(
        &self,
        _owner: SecretOwner,
        _reference: &SecretRef,
    ) -> Result<bool, SecretError> {
        Ok(false)
    }

    async fn resolve_namespace(
        &self,
        request: &NamespaceSecretRequest,
    ) -> Result<SecretMaterial, SecretError> {
        self.resolve_indexed(request).await
    }
}

/// Publisher-only capability for one create-only secret reservation.
///
/// It owns exactly one active encryption KEK and the opaque reservation binding
/// it seals for. It has no opening API and cannot be constructed without the
/// publication authority type that this crypto slice cannot mint in production.
pub(super) struct BlobSecretSealer {
    binding: BlobSecretPublicationBinding,
    active: KekId,
    key: KwAes256,
    rng: SystemRandom,
}

impl fmt::Debug for BlobSecretSealer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BlobSecretSealer")
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl BlobSecretSealer {
    pub(super) fn new(
        binding: BlobSecretPublicationBinding,
        active: (KekId, KekMaterial),
    ) -> Result<Self, KekRingError> {
        assert_zeroize_on_drop::<KwAes256>();
        Ok(Self {
            binding,
            active: active.0,
            key: active.1.into_key()?,
            rng: SystemRandom::new(),
        })
    }

    pub(super) fn active_id(&self) -> &KekId {
        &self.active
    }

    pub(super) fn seal(
        &self,
        material: &SecretMaterial,
    ) -> Result<SealedBlobSecret, BlobEnvelopeError> {
        let plaintext = material.expose().as_bytes();
        validate_plaintext(plaintext)?;
        let mut dek = [0_u8; KEY_BYTES];
        let mut material_nonce = [0_u8; NONCE_LEN];
        if self.rng.fill(&mut dek).is_err() || self.rng.fill(&mut material_nonce).is_err() {
            dek.zeroize();
            return Err(BlobEnvelopeError::Random);
        }
        let result = self.seal_with_parts(plaintext, dek, material_nonce);
        dek.zeroize();
        result
    }

    fn seal_with_parts(
        &self,
        plaintext: &[u8],
        mut dek: [u8; KEY_BYTES],
        material_nonce: [u8; NONCE_LEN],
    ) -> Result<SealedBlobSecret, BlobEnvelopeError> {
        let result = (|| {
            validate_plaintext(plaintext)?;
            let mut wrapped_dek = [0_u8; WRAPPED_DEK_BYTES];
            self.key
                .wrap_key(&dek, &mut wrapped_dek)
                .map_err(|_| BlobEnvelopeError::Unopenable)?;

            let data_key = UnboundKey::new(&AES_256_GCM, &dek)
                .map(LessSafeKey::new)
                .map_err(|_| BlobEnvelopeError::Unopenable)?;
            let mut ciphertext = plaintext.to_vec();
            if data_key
                .seal_in_place_append_tag(
                    Nonce::assume_unique_for_key(material_nonce),
                    Aad::from(aad(
                        AadPurpose::Material,
                        BlobSecretContext::from(&self.binding),
                        &self.active,
                    )),
                    &mut ciphertext,
                )
                .is_err()
            {
                ciphertext.zeroize();
                return Err(BlobEnvelopeError::Unopenable);
            }
            Ok(SealedBlobSecret {
                kek_id: self.active.clone(),
                wrapped_dek,
                material_nonce,
                ciphertext,
            })
        })();
        dek.zeroize();
        result
    }
}

/// Exercise bounded seal/open invariants with committed synthetic material.
///
/// This seam is absent from normal and published builds. Its numeric scenarios
/// are interpreted here, beside the private fields they mutate, so fuzzing does
/// not widen the publisher-facing API.
#[cfg(fuzzing)]
pub(crate) fn fuzz_seal_open(
    material: &[u8],
    scenario: u8,
    primary_seed: u8,
    secondary_seed: u8,
    identity_seed: u64,
    version_seed: u16,
) -> &'static str {
    use crate::desired_state::ids::{SecretId, Uuid7};
    use crate::desired_state::secrets::SecretVersion;

    fn fuzz_material(seed: u8) -> KekMaterial {
        KekMaterial::from_owned(Zeroizing::new(vec![seed; KEY_BYTES]))
            .expect("the fuzz seam always supplies exactly 32 bytes")
    }

    fn fuzz_binding(
        environment: &EnvironmentId,
        namespace: &NamespaceId,
        reference: SecretRef,
        sealed: &SealedBlobSecret,
    ) -> AuthenticatedSecretBinding {
        AuthenticatedSecretBinding::synthetic(
            environment,
            namespace,
            reference,
            Checksum::of(&sealed.to_canonical_cbor()),
        )
    }

    fn fuzz_publication(
        environment: &EnvironmentId,
        namespace: &NamespaceId,
        reference: SecretRef,
    ) -> BlobSecretPublicationBinding {
        BlobSecretPublicationBinding::synthetic(environment, namespace, reference)
    }

    fn fuzz_opener(entries: Vec<(&str, u8)>) -> BlobSecretOpener {
        BlobSecretOpener::new(
            KekDecryptRing::from_entries(
                entries
                    .into_iter()
                    .map(|(id, seed)| (KekId::parse(id).unwrap(), fuzz_material(seed)))
                    .collect(),
            )
            .expect("bounded synthetic decrypt ring"),
        )
    }

    let environment = EnvironmentId::parse("fuzz-environment").expect("static environment");
    let namespace = NamespaceId::parse("fuzz-namespace").expect("static namespace");
    let secret = SecretId::new(
        Uuid7::from_parts(
            identity_seed & ((1_u64 << 48) - 1),
            version_seed & 0x0fff,
            identity_seed.rotate_left(17),
        )
        .expect("bounded UUIDv7 parts"),
    );
    let version = SecretVersion::new(u64::from(version_seed) + 1).expect("one-based version");
    let reference = SecretRef::new(secret, version);

    if scenario % 14 == 11 {
        let result = KekDecryptRing::from_entries(vec![
            (KekId::parse("active").unwrap(), fuzz_material(primary_seed)),
            (KekId::parse("alias").unwrap(), fuzz_material(primary_seed)),
        ]);
        assert!(matches!(result, Err(KekRingError::DuplicateMaterial)));
        return "alias_rejected";
    }

    let opener = fuzz_opener(vec![("active", primary_seed)]);
    let sealer = BlobSecretSealer::new(
        fuzz_publication(&environment, &namespace, reference),
        (KekId::parse("active").unwrap(), fuzz_material(primary_seed)),
    )
    .expect("one synthetic active KEK");

    if scenario % 14 == 12 {
        let raw = if std::str::from_utf8(material).is_err() {
            material
        } else {
            &[0xff, 0xfe][..]
        };
        let raw = if raw.is_empty() { &[0xff][..] } else { raw };
        let raw = &raw[..raw.len().min(MAX_PLAINTEXT_BYTES)];
        let sealed = sealer
            .seal_with_parts(raw, [0x5a; KEY_BYTES], [0xa5; NONCE_LEN])
            .expect("bounded non-UTF-8 fixture seals");
        assert_eq!(
            opener
                .open(
                    fuzz_binding(&environment, &namespace, reference, &sealed),
                    &sealed,
                )
                .err(),
            Some(BlobEnvelopeError::Unopenable)
        );
        return "invalid_utf8_refused";
    }

    let Ok(text) = std::str::from_utf8(material) else {
        return "input_not_utf8";
    };
    let secret_material = SecretMaterial::new(text.to_owned());
    if material.is_empty() {
        assert_eq!(
            sealer.seal(&secret_material),
            Err(BlobEnvelopeError::EmptyMaterial)
        );
        return "empty_refused";
    }
    if material.len() > MAX_PLAINTEXT_BYTES {
        assert_eq!(
            sealer.seal(&secret_material),
            Err(BlobEnvelopeError::MaterialTooLarge)
        );
        return "oversized_refused";
    }

    let mut sealed = sealer
        .seal(&secret_material)
        .expect("bounded UTF-8 material seals");
    match scenario % 14 {
        0 => {
            assert_eq!(
                opener
                    .open(
                        fuzz_binding(&environment, &namespace, reference, &sealed),
                        &sealed,
                    )
                    .unwrap()
                    .expose(),
                text
            );
            "roundtrip"
        }
        1 => {
            let wrong = EnvironmentId::parse("other-environment").unwrap();
            assert_eq!(
                opener
                    .open(
                        fuzz_binding(&wrong, &namespace, reference, &sealed),
                        &sealed
                    )
                    .err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "wrong_environment"
        }
        2 => {
            let wrong = NamespaceId::parse("other-namespace").unwrap();
            assert_eq!(
                opener
                    .open(
                        fuzz_binding(&environment, &wrong, reference, &sealed),
                        &sealed
                    )
                    .err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "wrong_namespace"
        }
        3 => {
            let other = SecretRef::new(
                SecretId::new(Uuid7::from_parts(1, 1, identity_seed ^ 1).unwrap()),
                version,
            );
            assert_eq!(
                opener
                    .open(
                        fuzz_binding(&environment, &namespace, other, &sealed),
                        &sealed,
                    )
                    .err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "wrong_reference"
        }
        4 => {
            let other = SecretRef::new(secret, version.next());
            assert_eq!(
                opener
                    .open(
                        fuzz_binding(&environment, &namespace, other, &sealed),
                        &sealed,
                    )
                    .err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "wrong_version"
        }
        5 => {
            assert_eq!(
                opener
                    .open_with_purpose(
                        fuzz_binding(&environment, &namespace, reference, &sealed),
                        &sealed,
                        AadPurpose::InvalidTestDomain,
                    )
                    .err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "wrong_purpose"
        }
        6 => {
            let binding = fuzz_binding(&environment, &namespace, reference, &sealed);
            sealed.wrapped_dek[usize::from(version_seed) % WRAPPED_DEK_BYTES] ^= 1;
            assert_eq!(
                opener.open(binding, &sealed).err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "wrapped_mutation"
        }
        7 => {
            let binding = fuzz_binding(&environment, &namespace, reference, &sealed);
            sealed.material_nonce[usize::from(version_seed) % NONCE_LEN] ^= 1;
            assert_eq!(
                opener.open(binding, &sealed).err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "nonce_mutation"
        }
        8 => {
            let binding = fuzz_binding(&environment, &namespace, reference, &sealed);
            let offset = usize::from(version_seed) % sealed.ciphertext.len();
            sealed.ciphertext[offset] ^= 1;
            assert_eq!(
                opener.open(binding, &sealed).err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "ciphertext_mutation"
        }
        9 => {
            let unknown_seed = primary_seed.wrapping_add(1);
            let unknown = fuzz_opener(vec![("unknown", unknown_seed)]);
            assert_eq!(
                unknown
                    .open(
                        fuzz_binding(&environment, &namespace, reference, &sealed),
                        &sealed,
                    )
                    .err(),
                Some(BlobEnvelopeError::UnknownKek)
            );
            "unknown_key"
        }
        10 => {
            let mut new_seed = secondary_seed;
            if new_seed == primary_seed {
                new_seed = new_seed.wrapping_add(1);
            }
            let rotated = fuzz_opener(vec![("new", new_seed), ("active", primary_seed)]);
            assert_eq!(
                rotated
                    .open(
                        fuzz_binding(&environment, &namespace, reference, &sealed),
                        &sealed,
                    )
                    .unwrap()
                    .expose(),
                text
            );
            let rotated_sealer = BlobSecretSealer::new(
                fuzz_publication(&environment, &namespace, reference),
                (KekId::parse("new").unwrap(), fuzz_material(new_seed)),
            )
            .unwrap();
            let new_sealed = rotated_sealer.seal(&secret_material).unwrap();
            assert_eq!(new_sealed.kek_id().as_str(), "new");
            assert_eq!(
                opener
                    .open(
                        fuzz_binding(&environment, &namespace, reference, &new_sealed),
                        &new_sealed,
                    )
                    .err(),
                Some(BlobEnvelopeError::UnknownKek)
            );
            "rotation"
        }
        13 => {
            let binding = fuzz_binding(&environment, &namespace, reference, &sealed);
            sealed.kek_id = KekId::parse("other-id").unwrap();
            assert_eq!(
                opener.open(binding, &sealed).err(),
                Some(BlobEnvelopeError::Unopenable)
            );
            "stored_id_mutation"
        }
        11 | 12 | 14..=u8::MAX => unreachable!("scenario reduced modulo 14"),
    }
}

fn validate_plaintext(plaintext: &[u8]) -> Result<(), BlobEnvelopeError> {
    if plaintext.is_empty() {
        return Err(BlobEnvelopeError::EmptyMaterial);
    }
    if plaintext.len() > MAX_PLAINTEXT_BYTES {
        return Err(BlobEnvelopeError::MaterialTooLarge);
    }
    Ok(())
}

fn aad(purpose: AadPurpose, context: BlobSecretContext<'_>, kek_id: &KekId) -> Vec<u8> {
    let environment = context.environment.as_str().as_bytes();
    let namespace = context.namespace.as_str().as_bytes();
    let kek = kek_id.as_str().as_bytes();
    let mut aad = Vec::with_capacity(
        AAD_DOMAIN.len() + 1 + 2 + environment.len() + 2 + namespace.len() + 16 + 8 + 2 + kek.len(),
    );
    aad.extend_from_slice(AAD_DOMAIN);
    aad.push(purpose as u8);
    push_length_prefixed(&mut aad, environment);
    push_length_prefixed(&mut aad, namespace);
    aad.extend_from_slice(context.reference.secret.uuid().as_bytes());
    aad.extend_from_slice(&context.reference.version.get().to_be_bytes());
    push_length_prefixed(&mut aad, kek);
    aad
}

fn push_length_prefixed(target: &mut Vec<u8>, value: &[u8]) {
    let length = u16::try_from(value.len()).expect("all AAD text types are bounded below u16");
    target.extend_from_slice(&length.to_be_bytes());
    target.extend_from_slice(value);
}

fn encoded_size(kek_id_len: usize, ciphertext_len: usize) -> usize {
    1 + 1
        + cbor_item_size(SCHEME.len())
        + cbor_item_size(kek_id_len)
        + cbor_item_size(WRAPPED_DEK_BYTES)
        + cbor_item_size(NONCE_LEN)
        + cbor_item_size(ciphertext_len)
}

#[inline(always)]
fn assert_zeroize_on_drop<T: ZeroizeOnDrop>() {}

fn cbor_item_size(payload: usize) -> usize {
    cbor_length_size(payload) + payload
}

fn cbor_length_size(length: usize) -> usize {
    match length {
        0..=23 => 1,
        24..=0xff => 2,
        0x100..=0xffff => 3,
        _ => 5,
    }
}

fn encode_text(target: &mut Vec<u8>, text: &str) {
    encode_length(target, 3, text.len());
    target.extend_from_slice(text.as_bytes());
}

fn encode_bytes(target: &mut Vec<u8>, bytes: &[u8]) {
    encode_length(target, 2, bytes.len());
    target.extend_from_slice(bytes);
}

fn encode_length(target: &mut Vec<u8>, major: u8, length: usize) {
    let prefix = major << 5;
    match length {
        0..=23 => target.push(prefix | u8::try_from(length).expect("small length")),
        24..=0xff => {
            target.push(prefix | 24);
            target.push(u8::try_from(length).expect("u8 length"));
        }
        0x100..=0xffff => {
            target.push(prefix | 25);
            target.extend_from_slice(&u16::try_from(length).expect("u16 length").to_be_bytes());
        }
        _ => {
            target.push(prefix | 26);
            target.extend_from_slice(
                &u32::try_from(length)
                    .expect("bounded u32 length")
                    .to_be_bytes(),
            );
        }
    }
}

struct CborReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> CborReader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn expect(&mut self, expected: u8, error: CodecError) -> Result<(), CodecError> {
        let actual = self
            .take(1)?
            .first()
            .copied()
            .ok_or(CodecError::Truncated)?;
        if actual == expected {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn expect_text(&mut self, expected: &[u8], error: CodecError) -> Result<(), CodecError> {
        let bytes = self.item(3, expected.len(), expected.len())?;
        if bytes == expected {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn text(&mut self, maximum: usize) -> Result<&'a [u8], CodecError> {
        self.item(3, 1, maximum)
    }

    fn bytes_exact<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        self.item(2, N, N)?
            .try_into()
            .map_err(|_| CodecError::FixedField)
    }

    fn bytes_bounded(&mut self, minimum: usize, maximum: usize) -> Result<&'a [u8], CodecError> {
        self.item(2, minimum, maximum).map_err(|error| match error {
            CodecError::FixedField => CodecError::Ciphertext,
            other => other,
        })
    }

    fn item(
        &mut self,
        expected_major: u8,
        minimum: usize,
        maximum: usize,
    ) -> Result<&'a [u8], CodecError> {
        let initial = self.take(1)?[0];
        if initial >> 5 != expected_major {
            return Err(CodecError::Shape);
        }
        let additional = initial & 0x1f;
        let length = match additional {
            value @ 0..=23 => usize::from(value),
            24 => {
                let value = usize::from(self.take(1)?[0]);
                if value < 24 {
                    return Err(CodecError::NonCanonical);
                }
                value
            }
            25 => {
                let bytes: [u8; 2] = self.take(2)?.try_into().expect("two bytes");
                let value = usize::from(u16::from_be_bytes(bytes));
                if value <= 0xff {
                    return Err(CodecError::NonCanonical);
                }
                value
            }
            26 => {
                let bytes: [u8; 4] = self.take(4)?.try_into().expect("four bytes");
                let value = usize::try_from(u32::from_be_bytes(bytes))
                    .map_err(|_| CodecError::Oversized)?;
                if value <= 0xffff {
                    return Err(CodecError::NonCanonical);
                }
                value
            }
            _ => return Err(CodecError::NonCanonical),
        };
        if length < minimum || length > maximum {
            return Err(CodecError::FixedField);
        }
        self.take(length)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CodecError::Oversized)?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or(CodecError::Truncated)?;
        self.offset = end;
        Ok(bytes)
    }

    fn finish(self) -> Result<(), CodecError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(CodecError::Trailing)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use super::*;
    use base64::Engine as _;
    use bytes::Bytes;

    use crate::backends::object_store::{InMemoryObjectStore, ObjectStore, ObjectStoreLimits};
    use crate::desired_state::fixtures::{
        flat_namespace_state_with_active_credential_digest, secret_id,
    };
    use crate::desired_state::publication::{
        BlobPublication, BlobPublicationRequest, EnvironmentId as PublicationEnvironmentId,
        ExpectedHead, IdempotencyHistoryLimit, ImmutableObject, PublicationActorBinding,
        PublicationAuthorization, PublicationGrantBinding,
    };
    use crate::desired_state::{
        Canonical, DesiredState, IdempotencyKey, MutationId, MutationKind, PublicationKeyId,
        PublicationSigner, PublicationTrustStore, ResourceScope, Uuid7,
    };

    const PLAINTEXT: &str = "synthetic-provider-key-never-log";

    fn environment_id(text: &str) -> EnvironmentId {
        EnvironmentId::parse(text).expect("environment fixture")
    }

    fn namespace_id(text: &str) -> NamespaceId {
        NamespaceId::parse(text).expect("namespace fixture")
    }

    fn secret_reference(number: u64, version: u64) -> SecretRef {
        let first = SecretRef::first(secret_id(number));
        (1..version).fold(first, |reference, _| reference.rotated())
    }

    fn id(text: &str) -> KekId {
        KekId::parse(text).expect("KEK id fixture")
    }

    fn decrypt_ring(entries: &[(&str, u8)]) -> KekDecryptRing {
        KekDecryptRing::from_entries(
            entries
                .iter()
                .map(|(id_text, byte)| (id(id_text), KekMaterial::from_array([*byte; KEY_BYTES])))
                .collect(),
        )
        .expect("decrypt KEK fixture")
    }

    fn opener(entries: &[(&str, u8)]) -> BlobSecretOpener {
        BlobSecretOpener::new(decrypt_ring(entries))
    }

    fn fixture() -> (EnvironmentId, NamespaceId, SecretRef) {
        (
            environment_id("prod-east"),
            namespace_id("acme-prod"),
            secret_reference(1, 3),
        )
    }

    fn publication(
        environment: &EnvironmentId,
        namespace: &NamespaceId,
        reference: SecretRef,
    ) -> BlobSecretPublicationBinding {
        BlobSecretPublicationBinding::synthetic(environment, namespace, reference)
    }

    fn test_sealer(
        id_text: &str,
        byte: u8,
        environment: &EnvironmentId,
        namespace: &NamespaceId,
        reference: SecretRef,
    ) -> BlobSecretSealer {
        BlobSecretSealer::new(
            publication(environment, namespace, reference),
            (id(id_text), KekMaterial::from_array([byte; KEY_BYTES])),
        )
        .expect("active KEK fixture")
    }

    fn binding(
        environment: &EnvironmentId,
        namespace: &NamespaceId,
        reference: SecretRef,
        sealed: &SealedBlobSecret,
    ) -> AuthenticatedSecretBinding {
        AuthenticatedSecretBinding::synthetic(
            environment,
            namespace,
            reference,
            Checksum::of(&sealed.to_canonical_cbor()),
        )
    }

    const TEST_SIGNING_KEY_PKCS8_BASE64: &str = "MFMCAQEwBQYDK2VwBCIEIOn86WlkmKxquZ/ElW4lZfyxCVYnoaMnF56WoS4ICpKVoSMDIQDViT8X5LpD1A7O4sdlRada5GwjyvH2eAJ+ZiyfboLSBQ==";

    fn resolver_object_store() -> Arc<InMemoryObjectStore> {
        Arc::new(InMemoryObjectStore::new(
            ObjectStoreLimits::for_max_object_bytes(
                NonZeroUsize::new(2 * 1024 * 1024).expect("non-zero object limit"),
            ),
        ))
    }

    fn publication_signer() -> Arc<PublicationSigner> {
        let pkcs8 = base64::engine::general_purpose::STANDARD
            .decode(TEST_SIGNING_KEY_PKCS8_BASE64)
            .expect("fixed test signing key");
        Arc::new(
            PublicationSigner::from_ed25519_pkcs8(
                PublicationKeyId::parse("blob-secret-test-key").expect("valid key id"),
                &pkcs8,
            )
            .expect("valid test signer"),
        )
    }

    fn resolver_resource_objects(state: &DesiredState) -> Vec<ImmutableObject> {
        state
            .resources()
            .map(|resource| ImmutableObject {
                kind: if resource.scope == ResourceScope::Deployment {
                    ImmutableObjectKind::DeploymentResource
                } else {
                    ImmutableObjectKind::NamespaceResource
                },
                bytes: Bytes::from(
                    resource
                        .canonical()
                        .to_canonical_bytes()
                        .expect("canonical resource"),
                ),
            })
            .collect()
    }

    async fn publish_resolver_state(
        store: Arc<InMemoryObjectStore>,
        state: &DesiredState,
    ) -> PublicationTrustStore {
        let signer = publication_signer();
        let trust = PublicationTrustStore::new([signer.trusted_key()]).expect("test trust");
        BlobPublication::new(
            store,
            PublicationEnvironmentId::parse("blob-secret-test").expect("valid environment"),
            IdempotencyHistoryLimit::new(NonZeroUsize::new(8).expect("non-zero history")),
            signer,
            trust.clone(),
            None,
        )
        .expect("trusted publisher")
        .publish(BlobPublicationRequest {
            expected: ExpectedHead::Empty,
            authorization: PublicationAuthorization::new(
                PublicationActorBinding::of(b"blob-secret-test-actor"),
                PublicationGrantBinding::of(b"blob-secret-test-grant"),
                MutationId::new(Uuid7::from_parts(44, 0, 44).expect("valid mutation id")),
                MutationKind::Create,
            ),
            idempotency_key: IdempotencyKey::parse("blob-secret-test")
                .expect("valid idempotency key"),
            desired_state_checksum: state.checksum().expect("state checksum"),
            objects: resolver_resource_objects(state),
        })
        .await
        .expect("state publication");
        trust
    }

    #[tokio::test]
    async fn resolver_reads_only_the_candidate_indexed_secret_object() {
        let store = resolver_object_store();
        let environment = environment_id("blob-secret-test");
        let namespace = namespace_id("acme");
        let reference = secret_reference(953, 1);
        let sealer = test_sealer("primary", 0x11, &environment, &namespace, reference);
        let sealed = sealer
            .seal(&SecretMaterial::new(PLAINTEXT.to_owned()))
            .expect("sealed fixture material");
        let encoded = sealed.to_canonical_cbor();
        let digest = Checksum::of(&encoded);
        let state = flat_namespace_state_with_active_credential_digest(digest);
        let trust = publish_resolver_state(Arc::clone(&store), &state).await;
        store
            .put_if_absent(
                &crate::desired_state::publication::secret_key(digest),
                Bytes::from(encoded),
            )
            .await
            .expect("secret object publication");

        let source = crate::desired_state::BlobRevisionSource::new(
            BlobReader::new(
                Arc::clone(&store),
                PublicationEnvironmentId::parse("blob-secret-test").expect("valid environment"),
                trust,
            ),
            crate::desired_state::BlobHydrationLimits::default(),
        );
        let authority = source
            .candidate()
            .await
            .expect("candidate hydration")
            .expect("active candidate")
            .into_secret_authority()
            .expect("flat namespace authority");
        let request = authority
            .namespaces()
            .secret_request(&namespace, reference)
            .expect("indexed secret");
        let resolver = BlobSecretResolver::new(
            authority,
            BlobReader::new(
                Arc::clone(&store),
                PublicationEnvironmentId::parse("blob-secret-test").expect("valid environment"),
                PublicationTrustStore::new([publication_signer().trusted_key()])
                    .expect("test trust"),
            ),
            decrypt_ring(&[("primary", 0x11)]),
        );

        // The reader trust store is only used for immutable object digest
        // validation here; the resolver still requires an exact environment
        // match at construction.
        assert!(resolver.is_ok());
        let resolver = resolver.expect("matching candidate reader");
        assert_eq!(
            resolver.resolve_namespace(&request).await.unwrap().expose(),
            PLAINTEXT
        );
        assert!(matches!(
            resolver
                .resolve_namespace(
                    &request.with_lifecycle(crate::desired_state::SecretLifecycle::Staged)
                )
                .await,
            Err(SecretError::Lifecycle {
                state: crate::desired_state::SecretLifecycle::Staged,
                ..
            })
        ));
        assert!(matches!(
            resolver
                .resolve_namespace(&request.with_reference(secret_reference(954, 1)))
                .await,
            Err(SecretError::Denied { .. })
        ));
        assert!(matches!(
            resolver
                .resolve_namespace(
                    &request.with_ciphertext_digest(Checksum::of(b"different-ciphertext"))
                )
                .await,
            Err(SecretError::Denied { .. })
        ));
    }

    #[tokio::test]
    async fn candidate_scoped_resolution_uses_the_real_snapshot_compiler() {
        let store = resolver_object_store();
        let environment = environment_id("blob-secret-test");
        let namespace = namespace_id("acme");
        let reference = secret_reference(953, 1);
        let sealer = test_sealer("primary", 0x11, &environment, &namespace, reference);
        let sealed = sealer
            .seal(&SecretMaterial::new(PLAINTEXT.to_owned()))
            .expect("sealed fixture material");
        let encoded = sealed.to_canonical_cbor();
        let state = flat_namespace_state_with_active_credential_digest(Checksum::of(&encoded));
        let trust = publish_resolver_state(Arc::clone(&store), &state).await;
        store
            .put_if_absent(
                &crate::desired_state::publication::secret_key(Checksum::of(&encoded)),
                Bytes::from(encoded),
            )
            .await
            .expect("secret object publication");

        let source = crate::desired_state::BlobRevisionSource::new(
            BlobReader::new(
                Arc::clone(&store),
                PublicationEnvironmentId::parse("blob-secret-test").expect("valid environment"),
                trust.clone(),
            ),
            crate::desired_state::BlobHydrationLimits::default(),
        );
        let candidate = source
            .candidate()
            .await
            .expect("candidate hydration")
            .expect("active candidate");
        let revision = crate::convergence::compile::testing::revision_with(state);
        let compiler = crate::convergence::RevisionCompiler::new(
            crate::convergence::compile::testing::stateful_bootstrap(),
            HashMap::new(),
            crate::convergence::StateModelProjection,
        );

        let snapshot = compiler
            .compile_with_blob_candidate(
                &revision,
                candidate,
                BlobReader::new(
                    store,
                    PublicationEnvironmentId::parse("blob-secret-test").expect("valid environment"),
                    trust,
                ),
                decrypt_ring(&[("primary", 0x11)]),
                7,
            )
            .await
            .expect("candidate-scoped material must compile");
        assert_eq!(snapshot.secrets().len(), 1);
        assert_eq!(
            snapshot
                .secrets()
                .get(reference)
                .expect("compiled secret")
                .expose(),
            PLAINTEXT
        );
    }

    #[tokio::test]
    async fn a_blob_candidate_for_a_different_state_refuses_compilation() {
        let store = resolver_object_store();
        let environment = environment_id("blob-secret-test");
        let namespace = namespace_id("acme");
        let reference = secret_reference(953, 1);
        let sealer = test_sealer("primary", 0x11, &environment, &namespace, reference);
        let sealed = sealer
            .seal(&SecretMaterial::new(PLAINTEXT.to_owned()))
            .expect("sealed fixture material");
        let encoded = sealed.to_canonical_cbor();
        let candidate_state =
            flat_namespace_state_with_active_credential_digest(Checksum::of(&encoded));
        let trust = publish_resolver_state(Arc::clone(&store), &candidate_state).await;
        store
            .put_if_absent(
                &crate::desired_state::publication::secret_key(Checksum::of(&encoded)),
                Bytes::from(encoded),
            )
            .await
            .expect("secret object publication");
        let candidate = crate::desired_state::BlobRevisionSource::new(
            BlobReader::new(
                Arc::clone(&store),
                PublicationEnvironmentId::parse("blob-secret-test").expect("valid environment"),
                trust.clone(),
            ),
            crate::desired_state::BlobHydrationLimits::default(),
        )
        .candidate()
        .await
        .expect("candidate hydration")
        .expect("active candidate");

        let other_state = flat_namespace_state_with_active_credential_digest(Checksum::of(
            b"different-ciphertext",
        ));
        let revision = crate::convergence::compile::testing::revision_with(other_state);
        let compiler = crate::convergence::RevisionCompiler::new(
            crate::convergence::compile::testing::stateful_bootstrap(),
            HashMap::new(),
            crate::convergence::StateModelProjection,
        );
        let error = match compiler
            .compile_with_blob_candidate(
                &revision,
                candidate,
                BlobReader::new(
                    store,
                    PublicationEnvironmentId::parse("blob-secret-test").expect("valid environment"),
                    trust,
                ),
                decrypt_ring(&[("primary", 0x11)]),
                7,
            )
            .await
        {
            Ok(_) => panic!("a mismatched candidate cannot be published"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("does not match the loaded revision"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn resolver_rejects_a_reader_for_another_environment() {
        let store = resolver_object_store();
        let environment = environment_id("blob-secret-test");
        let namespace = namespace_id("acme");
        let reference = secret_reference(953, 1);
        let sealer = test_sealer("primary", 0x11, &environment, &namespace, reference);
        let sealed = sealer
            .seal(&SecretMaterial::new(PLAINTEXT.to_owned()))
            .expect("sealed fixture material");
        let state = flat_namespace_state_with_active_credential_digest(Checksum::of(
            &sealed.to_canonical_cbor(),
        ));
        let trust = publish_resolver_state(Arc::clone(&store), &state).await;
        let authority = crate::desired_state::BlobRevisionSource::new(
            BlobReader::new(
                Arc::clone(&store),
                PublicationEnvironmentId::parse("blob-secret-test").expect("valid environment"),
                trust.clone(),
            ),
            crate::desired_state::BlobHydrationLimits::default(),
        )
        .candidate()
        .await
        .expect("candidate hydration")
        .expect("active candidate")
        .into_secret_authority()
        .expect("flat namespace authority");

        assert!(matches!(
            BlobSecretResolver::new(
                authority,
                BlobReader::new(
                    store,
                    PublicationEnvironmentId::parse("other-environment")
                        .expect("valid environment"),
                    trust,
                ),
                decrypt_ring(&[("primary", 0x11)]),
            ),
            Err(BlobSecretResolverConstructionError::EnvironmentMismatch)
        ));
    }

    fn deterministic_sealed() -> (
        BlobSecretOpener,
        EnvironmentId,
        NamespaceId,
        SecretRef,
        SealedBlobSecret,
    ) {
        let (environment, namespace, reference) = fixture();
        let sealer = test_sealer("primary-2026-08", 0x11, &environment, &namespace, reference);
        let sealed = sealer
            .seal_with_parts(PLAINTEXT.as_bytes(), [0x22; KEY_BYTES], [0x44; NONCE_LEN])
            .expect("deterministic seal");
        (
            opener(&[("primary-2026-08", 0x11)]),
            environment,
            namespace,
            reference,
            sealed,
        )
    }

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        bytes.iter().fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").expect("write to string");
            output
        })
    }

    fn decode_hex<const N: usize>(text: &str) -> [u8; N] {
        assert_eq!(text.len(), N * 2, "hex fixture has the wrong length");
        let mut decoded = [0_u8; N];
        for (index, byte) in decoded.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
                .expect("RFC vector is hexadecimal");
        }
        decoded
    }

    #[test]
    fn axond_owns_rfc3394_aes256_key_wrap_vectors() {
        // RFC 3394 section 4.6: wrap 256 bits under a 256-bit KEK.
        let kek = decode_hex::<KEY_BYTES>(
            "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
        );
        let key_data = decode_hex::<KEY_BYTES>(
            "00112233445566778899aabbccddeeff000102030405060708090a0b0c0d0e0f",
        );
        let expected = decode_hex::<WRAPPED_DEK_BYTES>(
            "28c9f404c4b810f4cbccb35cfb87f8263f5786e2d80ed326cbc7f0e71a99f43bfb988b9b7a02dd21",
        );
        let wrapper = KwAes256::new_from_slice(&kek).expect("AES-256 KEK");
        let mut wrapped = [0_u8; WRAPPED_DEK_BYTES];
        wrapper.wrap_key(&key_data, &mut wrapped).expect("RFC wrap");
        assert_eq!(wrapped, expected);

        let mut opened = [0xa5_u8; KEY_BYTES];
        assert_eq!(
            wrapper
                .unwrap_key(&expected, &mut opened)
                .expect("RFC unwrap"),
            key_data
        );
        assert_eq!(opened, key_data);

        let mut corrupted = expected;
        corrupted[WRAPPED_DEK_BYTES / 2] ^= 1;
        opened.fill(0xa5);
        assert_eq!(
            wrapper.unwrap_key(&corrupted, &mut opened),
            Err(aes_kw::Error::IntegrityCheckFailed)
        );
        assert_eq!(opened, [0_u8; KEY_BYTES]);
    }

    #[test]
    fn deterministic_crypto_and_codec_golden_vector() {
        let (opener, environment, namespace, reference, sealed) = deterministic_sealed();
        let encoded = sealed.to_canonical_cbor();
        assert_eq!(
            hex(&encoded),
            "860278206165733235362d6b772e6165733235362d67636d2e656e76656c6f70652e76326f7072696d6172792d323032362d3038582887ca1088b05590d44c4f867da9ccf5f78ee09ffa6b0b193be942fcdeead5ec3558b9a2f43e1a683e4c4444444444444444444444445830e176cb178a4697ebd6882a524dabbea574fbde7cafdf901e5e2847781fae1410e06da89c43a4e140a0cb961dbce26adb"
        );
        let decoded = SealedBlobSecret::from_canonical_cbor(&encoded).unwrap();
        assert_eq!(decoded, sealed);
        assert_eq!(
            opener
                .open(
                    binding(&environment, &namespace, reference, &decoded),
                    &decoded,
                )
                .unwrap()
                .expose(),
            PLAINTEXT
        );
    }

    #[test]
    fn context_and_purpose_are_cryptographic_boundaries() {
        let (opener, environment, namespace, reference, sealed) = deterministic_sealed();
        let other_environment = environment_id("prod-west");
        let other_namespace = namespace_id("globex-prod");
        let other_secret = secret_reference(2, reference.version.get());
        let other_version = secret_reference(1, reference.version.get() + 1);
        for wrong in [
            binding(&other_environment, &namespace, reference, &sealed),
            binding(&environment, &other_namespace, reference, &sealed),
            binding(&environment, &namespace, other_secret, &sealed),
            binding(&environment, &namespace, other_version, &sealed),
        ] {
            assert!(matches!(
                opener.open(wrong, &sealed),
                Err(BlobEnvelopeError::Unopenable)
            ));
        }
        assert!(matches!(
            opener.open_with_purpose(
                binding(&environment, &namespace, reference, &sealed),
                &sealed,
                AadPurpose::InvalidTestDomain,
            ),
            Err(BlobEnvelopeError::Unopenable)
        ));
        assert!(matches!(
            opener.open(
                AuthenticatedSecretBinding::synthetic(
                    &environment,
                    &namespace,
                    reference,
                    Checksum::of(b"not-the-indexed-ciphertext"),
                ),
                &sealed,
            ),
            Err(BlobEnvelopeError::Unopenable)
        ));
    }

    #[test]
    fn key_id_and_key_rotation_fail_closed_and_decrypt_only_keys_work() {
        let (environment, namespace, reference) = fixture();
        let old_sealer = test_sealer("old", 0x11, &environment, &namespace, reference);
        let old_opener = opener(&[("old", 0x11)]);
        let sealed = old_sealer
            .seal(&SecretMaterial::new(PLAINTEXT.to_owned()))
            .unwrap();
        let rotated_without_old = opener(&[("new", 0x22)]);
        assert!(matches!(
            rotated_without_old.open(
                binding(&environment, &namespace, reference, &sealed),
                &sealed
            ),
            Err(BlobEnvelopeError::UnknownKek)
        ));
        let rotated = opener(&[("new", 0x22), ("old", 0x11)]);
        assert_eq!(
            rotated
                .open(
                    binding(&environment, &namespace, reference, &sealed),
                    &sealed,
                )
                .unwrap()
                .expose(),
            PLAINTEXT
        );
        let new_sealer = test_sealer("new", 0x22, &environment, &namespace, reference);
        let newly_sealed = new_sealer
            .seal(&SecretMaterial::new(PLAINTEXT.to_owned()))
            .unwrap();
        assert_eq!(newly_sealed.kek_id(), &id("new"));
        assert!(matches!(
            old_opener.open(
                binding(&environment, &namespace, reference, &newly_sealed),
                &newly_sealed
            ),
            Err(BlobEnvelopeError::UnknownKek)
        ));
        assert_eq!(rotated.ring.keys.len(), 2);
    }

    #[test]
    fn wrong_key_material_under_the_right_id_fails_closed() {
        let (valid_opener, environment, namespace, reference, sealed) = deterministic_sealed();
        assert_eq!(sealed.kek_id(), &id("primary-2026-08"));
        let wrong_material = opener(&[("primary-2026-08", 0x99)]);
        assert_eq!(valid_opener.ring.keys.len(), 1);
        assert!(matches!(
            wrong_material.open(
                binding(&environment, &namespace, reference, &sealed),
                &sealed
            ),
            Err(BlobEnvelopeError::Unopenable)
        ));
    }

    #[test]
    fn a_stored_kek_id_is_itself_authenticated() {
        let (_, environment, namespace, reference, sealed) = deterministic_sealed();
        let opener = opener(&[("primary-2026-08", 0x11), ("secondary-2026-08", 0x12)]);
        let mut changed = sealed;
        changed.kek_id = id("secondary-2026-08");
        assert!(matches!(
            opener.open(
                binding(&environment, &namespace, reference, &changed),
                &changed
            ),
            Err(BlobEnvelopeError::Unopenable)
        ));
    }

    #[test]
    fn every_stored_byte_is_authenticated_or_a_strict_codec_field() {
        let (opener, environment, namespace, reference, sealed) = deterministic_sealed();
        let encoded = sealed.to_canonical_cbor();
        for offset in 0..encoded.len() {
            let mut changed = encoded.clone();
            changed[offset] ^= 1;
            if let Ok(changed) = SealedBlobSecret::from_canonical_cbor(&changed) {
                assert!(
                    opener
                        .open(
                            binding(&environment, &namespace, reference, &changed),
                            &changed,
                        )
                        .is_err(),
                    "mutation at byte {offset} opened"
                );
            }
        }
    }

    #[test]
    fn canonical_cbor_is_the_only_accepted_spelling() {
        let (_, _, _, _, sealed) = deterministic_sealed();
        let encoded = sealed.to_canonical_cbor();

        let mut indefinite = encoded.clone();
        indefinite[0] = 0x9f;
        assert_eq!(
            SealedBlobSecret::from_canonical_cbor(&indefinite),
            Err(CodecError::Shape)
        );

        let mut nonminimal_scheme = encoded.clone();
        nonminimal_scheme.splice(2..4, [0x79, 0, u8::try_from(SCHEME.len()).unwrap()]);
        assert_eq!(
            SealedBlobSecret::from_canonical_cbor(&nonminimal_scheme),
            Err(CodecError::NonCanonical)
        );

        let kek_header = 2 + cbor_length_size(SCHEME.len()) + SCHEME.len();
        let kek_len = encoded[kek_header] & 0x1f;
        let mut nonminimal_kek = encoded.clone();
        nonminimal_kek.splice(kek_header..=kek_header, [0x78, kek_len]);
        assert_eq!(
            SealedBlobSecret::from_canonical_cbor(&nonminimal_kek),
            Err(CodecError::NonCanonical)
        );

        let mut trailing = encoded;
        trailing.push(0);
        assert_eq!(
            SealedBlobSecret::from_canonical_cbor(&trailing),
            Err(CodecError::Trailing)
        );
    }

    #[test]
    fn every_truncation_and_oversized_object_is_refused() {
        let (_, _, _, _, sealed) = deterministic_sealed();
        let encoded = sealed.to_canonical_cbor();
        for length in 0..encoded.len() {
            assert!(
                SealedBlobSecret::from_canonical_cbor(&encoded[..length]).is_err(),
                "accepted prefix of {length} bytes"
            );
        }
        assert_eq!(
            SealedBlobSecret::from_canonical_cbor(&vec![0; MAX_SEALED_BYTES + 1]),
            Err(CodecError::Oversized)
        );
    }

    #[test]
    fn plaintext_and_sealed_bounds_are_exact() {
        let (environment, namespace, reference) = fixture();
        let sealer = test_sealer("primary", 0x11, &environment, &namespace, reference);
        let opener = opener(&[("primary", 0x11)]);
        assert_eq!(
            sealer.seal(&SecretMaterial::new(String::new())),
            Err(BlobEnvelopeError::EmptyMaterial)
        );
        assert_eq!(
            sealer.seal(&SecretMaterial::new("x".repeat(MAX_PLAINTEXT_BYTES + 1))),
            Err(BlobEnvelopeError::MaterialTooLarge)
        );
        let maximum = SecretMaterial::new("x".repeat(MAX_PLAINTEXT_BYTES));
        let sealed = sealer.seal(&maximum).unwrap();
        let encoded = sealed.to_canonical_cbor();
        assert_eq!(
            encoded.len(),
            encoded_size("primary".len(), MAX_CIPHERTEXT_BYTES)
        );
        assert!(encoded.len() <= MAX_SEALED_BYTES);
        assert_eq!(
            opener
                .open(
                    binding(&environment, &namespace, reference, &sealed),
                    &SealedBlobSecret::from_canonical_cbor(&encoded).unwrap()
                )
                .unwrap()
                .expose()
                .len(),
            MAX_PLAINTEXT_BYTES
        );

        let widest_sealer = test_sealer(
            &"k".repeat(MAX_KEK_ID_BYTES),
            0x11,
            &environment,
            &namespace,
            reference,
        );
        let widest = widest_sealer.seal(&maximum).unwrap();
        assert_eq!(widest.to_canonical_cbor().len(), MAX_SEALED_BYTES);
        assert_eq!(
            MAX_SEALED_BYTES,
            encoded_size(MAX_KEK_ID_BYTES, MAX_CIPHERTEXT_BYTES)
        );
    }

    #[test]
    fn plaintext_limit_is_a_utf8_byte_limit() {
        let (environment, namespace, reference) = fixture();
        let sealer = test_sealer("primary", 0x11, &environment, &namespace, reference);
        let opener = opener(&[("primary", 0x11)]);
        let exact = "é".repeat(MAX_PLAINTEXT_BYTES / 2);
        assert_eq!(exact.len(), MAX_PLAINTEXT_BYTES);
        let sealed = sealer
            .seal(&SecretMaterial::new(exact.clone()))
            .expect("exact multibyte byte limit");
        assert_eq!(
            opener
                .open(
                    binding(&environment, &namespace, reference, &sealed),
                    &sealed,
                )
                .unwrap()
                .expose(),
            exact
        );

        let over = format!("{exact}x");
        assert_eq!(over.len(), MAX_PLAINTEXT_BYTES + 1);
        assert_eq!(
            sealer.seal(&SecretMaterial::new(over)),
            Err(BlobEnvelopeError::MaterialTooLarge)
        );
    }

    #[test]
    fn authenticated_non_utf8_plaintext_is_still_refused() {
        let (environment, namespace, reference) = fixture();
        let sealer = test_sealer("primary", 0x11, &environment, &namespace, reference);
        let opener = opener(&[("primary", 0x11)]);
        let sealed = sealer
            .seal_with_parts(&[0xff, 0xfe], [0x22; KEY_BYTES], [0x44; NONCE_LEN])
            .expect("test-only raw bytes can be sealed");
        assert_eq!(
            opener
                .open(
                    binding(&environment, &namespace, reference, &sealed),
                    &sealed,
                )
                .err(),
            Some(BlobEnvelopeError::Unopenable)
        );
    }

    #[test]
    fn exact_key_and_identifier_bounds_are_enforced() {
        assert_eq!(
            KekMaterial::from_owned(Zeroizing::new(vec![0; KEY_BYTES - 1])).unwrap_err(),
            KekRingError::KeyLength {
                found: KEY_BYTES - 1
            }
        );
        assert_eq!(
            KekMaterial::from_owned(Zeroizing::new(vec![0; KEY_BYTES + 1])).unwrap_err(),
            KekRingError::KeyLength {
                found: KEY_BYTES + 1
            }
        );
        KekMaterial::from_owned(Zeroizing::new(vec![0; KEY_BYTES])).expect("exact AES-256 key");
        assert!(KekId::parse(&"k".repeat(MAX_KEK_ID_BYTES)).is_ok());
        assert!(matches!(
            KekId::parse(&"k".repeat(MAX_KEK_ID_BYTES + 1)),
            Err(InvalidKekId::TooLong)
        ));
        for invalid in ["", "has space", "line\nbreak", "slash/value"] {
            assert!(KekId::parse(invalid).is_err());
        }
    }

    #[test]
    fn ring_population_is_bounded_atomic_and_rejects_key_aliases() {
        let exact = (0..MAX_KEK_RING_KEYS)
            .map(|index| {
                (
                    id(&format!("decrypt-{index}")),
                    KekMaterial::from_array([u8::try_from(index).unwrap(); KEY_BYTES]),
                )
            })
            .collect();
        let widest = KekDecryptRing::from_entries(exact).expect("the exact ring bound");
        assert_eq!(widest.keys.len(), MAX_KEK_RING_KEYS);

        let too_many = (0..=MAX_KEK_RING_KEYS)
            .map(|index| {
                (
                    id(&format!("decrypt-{index}")),
                    KekMaterial::from_array([u8::try_from(index).unwrap(); KEY_BYTES]),
                )
            })
            .collect();
        assert!(matches!(
            KekDecryptRing::from_entries(too_many),
            Err(KekRingError::TooMany {
                maximum: MAX_KEK_RING_KEYS
            })
        ));
        assert!(matches!(
            KekDecryptRing::from_entries(Vec::new()),
            Err(KekRingError::Empty)
        ));

        assert!(matches!(
            KekDecryptRing::from_entries(vec![
                (id("same"), KekMaterial::from_array([1; KEY_BYTES])),
                (id("same"), KekMaterial::from_array([2; KEY_BYTES])),
            ]),
            Err(KekRingError::DuplicateId)
        ));
        assert!(matches!(
            KekDecryptRing::from_entries(vec![
                (id("first"), KekMaterial::from_array([3; KEY_BYTES])),
                (id("alias"), KekMaterial::from_array([3; KEY_BYTES])),
            ]),
            Err(KekRingError::DuplicateMaterial)
        ));
    }

    #[test]
    fn context_is_not_stored_and_nothing_sensitive_is_rendered() {
        let (opener, environment, namespace, reference, sealed) = deterministic_sealed();
        let sealer = test_sealer("primary-2026-08", 0x11, &environment, &namespace, reference);
        let encoded = sealed.to_canonical_cbor();
        for absent in [
            environment.as_str().as_bytes(),
            namespace.as_str().as_bytes(),
            reference.to_string().as_bytes(),
            reference.secret.uuid().as_bytes(),
            PLAINTEXT.as_bytes(),
            &[0x11; KEY_BYTES],
        ] {
            assert!(
                !encoded.windows(absent.len()).any(|window| window == absent),
                "authenticated context or material entered the object"
            );
        }

        let rendered = [
            format!("{opener:?}"),
            format!("{sealer:?}"),
            format!("{sealed:?}"),
            format!("{:?}", KekMaterial::from_array([0x11; KEY_BYTES])),
            BlobEnvelopeError::Unopenable.to_string(),
            BlobEnvelopeError::UnknownKek.to_string(),
            CodecError::Compatibility.to_string(),
        ]
        .join(" ");
        for absent in [PLAINTEXT, "11111111", "33333333", "44444444"] {
            assert!(!rendered.contains(absent), "rendered secret-derived bytes");
        }
    }

    #[test]
    fn aad_is_binary_length_prefixed_and_purpose_separated() {
        let (environment, namespace, reference) = fixture();
        let publication = publication(&environment, &namespace, reference);
        let kek = id("primary-2026-08");
        let material = aad(
            AadPurpose::Material,
            BlobSecretContext::from(&publication),
            &kek,
        );
        assert_eq!(&material[..AAD_DOMAIN.len()], AAD_DOMAIN);
        assert_eq!(material[AAD_DOMAIN.len()], AadPurpose::Material as u8);
        assert_eq!(
            hex(&material),
            "61786f6e642e7365637265742e656e76656c6f70652e76320001000970726f642d65617374000961636d652d70726f64000000000001700080000000000000010000000000000003000f7072696d6172792d323032362d3038"
        );
    }
}
