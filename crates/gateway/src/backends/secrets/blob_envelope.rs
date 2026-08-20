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
//! [1, "aes256-gcm.envelope.v2", kek_id,
//!  dek_nonce, wrapped_dek, material_nonce, ciphertext]
//! ```
//!
//! The deployment environment, namespace owner, and exact [`SecretRef`] are
//! intentionally absent. They come from an authenticated desired-state
//! manifest and are authenticated as binary, length-prefixed additional data
//! for both AEAD operations. Copying an object to another environment,
//! namespace, secret id, or version therefore cannot make it open.

use std::collections::BTreeMap;
use std::fmt;

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use secrecy::zeroize::Zeroize;

use super::SecretMaterial;
use crate::desired_state::secrets::SecretRef;
use crate::namespace::NamespaceId;

/// The scheme identifier stored in every v2 blob envelope.
pub const SCHEME: &str = "aes256-gcm.envelope.v2";

/// AES-256 key size, for key-encryption keys and per-secret data keys.
pub const KEY_BYTES: usize = 32;

/// The maximum plaintext carried by one secret object.
pub const MAX_PLAINTEXT_BYTES: usize = 64 * 1024;

const TAG_BYTES: usize = 16;
const WRAPPED_DEK_BYTES: usize = KEY_BYTES + TAG_BYTES;
const MAX_CIPHERTEXT_BYTES: usize = MAX_PLAINTEXT_BYTES + TAG_BYTES;
const MAX_KEK_ID_BYTES: usize = 64;
const MAX_ENVIRONMENT_ID_BYTES: usize = 63;
const FIELD_COUNT: u8 = 7;
const SCHEMA_VERSION: u8 = 1;
const AAD_DOMAIN: &[u8] = b"axond.secret.envelope.v2\0";

/// Maximum canonical encoded object size, including the largest legal KEK id.
///
/// Kept as a literal expression over format fields so a format edit cannot
/// silently invalidate the pre-parse allocation bound.
pub const MAX_SEALED_BYTES: usize = 1 // seven-element array
    + 1 // schema version
    + 1 + SCHEME.len() // scheme text
    + 2 + MAX_KEK_ID_BYTES // KEK id text (one-byte length argument)
    + 1 + NONCE_LEN // DEK nonce
    + 2 + WRAPPED_DEK_BYTES // wrapped DEK
    + 1 + NONCE_LEN // material nonce
    + 5 + MAX_CIPHERTEXT_BYTES; // ciphertext (u32 length argument at the ceiling)

/// A deployment/environment identity supplied by authenticated desired state.
///
/// The same canonical URL-segment alphabet as [`NamespaceId`] avoids alternate
/// object-prefix spellings. Refused input is retained only for diagnostics and
/// is never rendered by its error.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnvironmentId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidEnvironmentId {
    #[error("an environment identifier must not be empty")]
    Empty,
    #[error("an environment identifier is over the 63-byte limit")]
    TooLong,
    #[error(
        "an environment identifier must be one canonical ASCII segment using letters, digits, `-`, or `_`"
    )]
    Character,
    #[error("an environment identifier must start and end with an ASCII letter or digit")]
    Boundary,
}

impl EnvironmentId {
    pub fn parse(input: &str) -> Result<Self, InvalidEnvironmentId> {
        if input.is_empty() {
            return Err(InvalidEnvironmentId::Empty);
        }
        if input.len() > MAX_ENVIRONMENT_ID_BYTES {
            return Err(InvalidEnvironmentId::TooLong);
        }
        if !input
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(InvalidEnvironmentId::Character);
        }
        if !input
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
            || !input
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
        {
            return Err(InvalidEnvironmentId::Boundary);
        }
        Ok(Self(input.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EnvironmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for EnvironmentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

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
/// Construction accepts exactly 32 bytes. There is no byte accessor, formatter,
/// serializer, or clone; conversion into a ring copies the bytes into `ring`'s
/// opaque key and zeroizes this staging buffer immediately.
pub struct KekMaterial([u8; KEY_BYTES]);

impl KekMaterial {
    pub fn from_array(bytes: [u8; KEY_BYTES]) -> Self {
        Self(bytes)
    }

    /// Consume a dynamically sized bootstrap value, refusing any non-AES-256
    /// length and zeroizing the consumed buffer on both success and failure.
    pub fn from_bytes(mut bytes: Vec<u8>) -> Result<Self, KekRingError> {
        if bytes.len() != KEY_BYTES {
            let found = bytes.len();
            bytes.zeroize();
            return Err(KekRingError::KeyLength { found });
        }
        let mut exact = [0_u8; KEY_BYTES];
        exact.copy_from_slice(&bytes);
        bytes.zeroize();
        Ok(Self(exact))
    }

    fn into_key(mut self) -> Result<LessSafeKey, KekRingError> {
        let result = UnboundKey::new(&AES_256_GCM, &self.0)
            .map(LessSafeKey::new)
            .map_err(|_| KekRingError::KeyRejected);
        self.0.zeroize();
        result
    }
}

impl Drop for KekMaterial {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for KekMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("KekMaterial(<redacted>)")
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
}

/// Authenticated identity supplied alongside an immutable ciphertext object.
///
/// This type has no formatter so a caller cannot accidentally render the AAD.
#[derive(Clone, Copy)]
pub struct BlobSecretContext<'a> {
    pub environment: &'a EnvironmentId,
    pub namespace: &'a NamespaceId,
    pub reference: &'a SecretRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum AadPurpose {
    WrapDek = 1,
    Material = 2,
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
    dek_nonce: [u8; NONCE_LEN],
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
        encode_bytes(&mut encoded, &self.dek_nonce);
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
        let dek_nonce = reader.bytes_exact::<NONCE_LEN>()?;
        let wrapped_dek = reader.bytes_exact::<WRAPPED_DEK_BYTES>()?;
        let material_nonce = reader.bytes_exact::<NONCE_LEN>()?;
        let ciphertext = reader.bytes_bounded(TAG_BYTES + 1, MAX_CIPHERTEXT_BYTES)?;
        reader.finish()?;
        let sealed = Self {
            kek_id,
            dek_nonce,
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

/// One active encryption key plus any number of decrypt-only keys.
///
/// New objects always name `active`. Opening selects the exact stored id, so a
/// rolling KEK rotation can retain old keys without ever encrypting new material
/// under them.
pub struct KekRing {
    active: KekId,
    keys: BTreeMap<KekId, LessSafeKey>,
    rng: SystemRandom,
}

impl fmt::Debug for KekRing {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KekRing")
            .field("active", &self.active)
            .field("key_count", &self.keys.len())
            .finish()
    }
}

impl KekRing {
    pub fn new(active: KekId, material: KekMaterial) -> Result<Self, KekRingError> {
        let key = material.into_key()?;
        let mut keys = BTreeMap::new();
        keys.insert(active.clone(), key);
        Ok(Self {
            active,
            keys,
            rng: SystemRandom::new(),
        })
    }

    pub fn add_decrypt_only(
        &mut self,
        id: KekId,
        material: KekMaterial,
    ) -> Result<(), KekRingError> {
        if self.keys.contains_key(&id) {
            return Err(KekRingError::DuplicateId);
        }
        self.keys.insert(id, material.into_key()?);
        Ok(())
    }

    pub fn active_id(&self) -> &KekId {
        &self.active
    }

    pub fn seal(
        &self,
        context: BlobSecretContext<'_>,
        material: &SecretMaterial,
    ) -> Result<SealedBlobSecret, BlobEnvelopeError> {
        let plaintext = material.expose().as_bytes();
        validate_plaintext(plaintext)?;
        let mut dek = [0_u8; KEY_BYTES];
        let mut dek_nonce = [0_u8; NONCE_LEN];
        let mut material_nonce = [0_u8; NONCE_LEN];
        if self.rng.fill(&mut dek).is_err()
            || self.rng.fill(&mut dek_nonce).is_err()
            || self.rng.fill(&mut material_nonce).is_err()
        {
            dek.zeroize();
            return Err(BlobEnvelopeError::Random);
        }
        let result = self.seal_with_parts(context, plaintext, dek, dek_nonce, material_nonce);
        dek.zeroize();
        result
    }

    pub fn open(
        &self,
        context: BlobSecretContext<'_>,
        sealed: &SealedBlobSecret,
    ) -> Result<SecretMaterial, BlobEnvelopeError> {
        self.open_with_purposes(context, sealed, AadPurpose::WrapDek, AadPurpose::Material)
    }

    fn seal_with_parts(
        &self,
        context: BlobSecretContext<'_>,
        plaintext: &[u8],
        mut dek: [u8; KEY_BYTES],
        dek_nonce: [u8; NONCE_LEN],
        material_nonce: [u8; NONCE_LEN],
    ) -> Result<SealedBlobSecret, BlobEnvelopeError> {
        let result = (|| {
            validate_plaintext(plaintext)?;
            let active_key = self
                .keys
                .get(&self.active)
                .ok_or(BlobEnvelopeError::UnknownKek)?;

            let mut wrapped_dek = dek.to_vec();
            if active_key
                .seal_in_place_append_tag(
                    Nonce::assume_unique_for_key(dek_nonce),
                    Aad::from(aad(AadPurpose::WrapDek, context, &self.active)),
                    &mut wrapped_dek,
                )
                .is_err()
            {
                wrapped_dek.zeroize();
                return Err(BlobEnvelopeError::Unopenable);
            }
            let wrapped_dek: [u8; WRAPPED_DEK_BYTES] = wrapped_dek
                .try_into()
                .map_err(|_| BlobEnvelopeError::Unopenable)?;

            let data_key = UnboundKey::new(&AES_256_GCM, &dek)
                .map(LessSafeKey::new)
                .map_err(|_| BlobEnvelopeError::Unopenable)?;
            let mut ciphertext = plaintext.to_vec();
            if data_key
                .seal_in_place_append_tag(
                    Nonce::assume_unique_for_key(material_nonce),
                    Aad::from(aad(AadPurpose::Material, context, &self.active)),
                    &mut ciphertext,
                )
                .is_err()
            {
                ciphertext.zeroize();
                return Err(BlobEnvelopeError::Unopenable);
            }
            Ok(SealedBlobSecret {
                kek_id: self.active.clone(),
                dek_nonce,
                wrapped_dek,
                material_nonce,
                ciphertext,
            })
        })();
        dek.zeroize();
        result
    }

    fn open_with_purposes(
        &self,
        context: BlobSecretContext<'_>,
        sealed: &SealedBlobSecret,
        dek_purpose: AadPurpose,
        material_purpose: AadPurpose,
    ) -> Result<SecretMaterial, BlobEnvelopeError> {
        let kek = self
            .keys
            .get(&sealed.kek_id)
            .ok_or(BlobEnvelopeError::UnknownKek)?;
        let mut wrapped = sealed.wrapped_dek.to_vec();
        let opened_dek = kek
            .open_in_place(
                Nonce::assume_unique_for_key(sealed.dek_nonce),
                Aad::from(aad(dek_purpose, context, &sealed.kek_id)),
                &mut wrapped,
            )
            .map_err(|_| BlobEnvelopeError::Unopenable);
        let data_key = opened_dek.and_then(|dek| {
            UnboundKey::new(&AES_256_GCM, dek)
                .map(LessSafeKey::new)
                .map_err(|_| BlobEnvelopeError::Unopenable)
        });
        wrapped.zeroize();
        let data_key = data_key?;

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
        + cbor_item_size(NONCE_LEN)
        + cbor_item_size(WRAPPED_DEK_BYTES)
        + cbor_item_size(NONCE_LEN)
        + cbor_item_size(ciphertext_len)
}

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
    use super::*;
    use crate::desired_state::fixtures::secret_id;

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

    fn kek_ring(id_text: &str, byte: u8) -> KekRing {
        KekRing::new(id(id_text), KekMaterial::from_array([byte; KEY_BYTES])).expect("KEK fixture")
    }

    fn fixture() -> (EnvironmentId, NamespaceId, SecretRef) {
        (
            environment_id("prod-east"),
            namespace_id("acme-prod"),
            secret_reference(1, 3),
        )
    }

    fn context<'a>(
        environment: &'a EnvironmentId,
        namespace: &'a NamespaceId,
        reference: &'a SecretRef,
    ) -> BlobSecretContext<'a> {
        BlobSecretContext {
            environment,
            namespace,
            reference,
        }
    }

    fn deterministic_sealed() -> (
        KekRing,
        EnvironmentId,
        NamespaceId,
        SecretRef,
        SealedBlobSecret,
    ) {
        let ring = kek_ring("primary-2026-08", 0x11);
        let (environment, namespace, reference) = fixture();
        let sealed = ring
            .seal_with_parts(
                context(&environment, &namespace, &reference),
                PLAINTEXT.as_bytes(),
                [0x22; KEY_BYTES],
                [0x33; NONCE_LEN],
                [0x44; NONCE_LEN],
            )
            .expect("deterministic seal");
        (ring, environment, namespace, reference, sealed)
    }

    fn hex(bytes: &[u8]) -> String {
        use std::fmt::Write as _;
        bytes.iter().fold(String::new(), |mut output, byte| {
            write!(output, "{byte:02x}").expect("write to string");
            output
        })
    }

    #[test]
    fn deterministic_crypto_and_codec_golden_vector() {
        let (ring, environment, namespace, reference, sealed) = deterministic_sealed();
        let encoded = sealed.to_canonical_cbor();
        assert_eq!(
            hex(&encoded),
            "8701766165733235362d67636d2e656e76656c6f70652e76326f7072696d6172792d323032362d30384c3333333333333333333333335830a6a17064e8b0570e909ac94f8509c10a24ea2d16a8fba78eda682ba781aca80828a4c8f7a69a4efe1c4218d72ed1dfc14c4444444444444444444444445830e176cb178a4697ebd6882a524dabbea574fbde7cafdf901e5e2847781fae1410eddb1ba39355ad66efc151350fdfe8ae"
        );
        let decoded = SealedBlobSecret::from_canonical_cbor(&encoded).unwrap();
        assert_eq!(decoded, sealed);
        assert_eq!(
            ring.open(context(&environment, &namespace, &reference), &decoded)
                .unwrap()
                .expose(),
            PLAINTEXT
        );
    }

    #[test]
    fn context_and_purpose_are_cryptographic_boundaries() {
        let (ring, environment, namespace, reference, sealed) = deterministic_sealed();
        let other_environment = environment_id("prod-west");
        let other_namespace = namespace_id("globex-prod");
        let other_secret = secret_reference(2, reference.version.get());
        let other_version = secret_reference(1, reference.version.get() + 1);
        for wrong in [
            context(&other_environment, &namespace, &reference),
            context(&environment, &other_namespace, &reference),
            context(&environment, &namespace, &other_secret),
            context(&environment, &namespace, &other_version),
        ] {
            assert!(matches!(
                ring.open(wrong, &sealed),
                Err(BlobEnvelopeError::Unopenable)
            ));
        }
        let right = context(&environment, &namespace, &reference);
        assert!(matches!(
            ring.open_with_purposes(right, &sealed, AadPurpose::Material, AadPurpose::Material),
            Err(BlobEnvelopeError::Unopenable)
        ));
        assert!(matches!(
            ring.open_with_purposes(right, &sealed, AadPurpose::WrapDek, AadPurpose::WrapDek),
            Err(BlobEnvelopeError::Unopenable)
        ));
    }

    #[test]
    fn key_id_and_key_rotation_fail_closed_and_decrypt_only_keys_work() {
        let old = kek_ring("old", 0x11);
        let (environment, namespace, reference) = fixture();
        let sealed = old
            .seal(
                context(&environment, &namespace, &reference),
                &SecretMaterial::new(PLAINTEXT.to_owned()),
            )
            .unwrap();
        let mut rotated = kek_ring("new", 0x22);
        assert!(matches!(
            rotated.open(context(&environment, &namespace, &reference), &sealed),
            Err(BlobEnvelopeError::UnknownKek)
        ));
        rotated
            .add_decrypt_only(id("old"), KekMaterial::from_array([0x11; KEY_BYTES]))
            .unwrap();
        assert_eq!(
            rotated
                .open(context(&environment, &namespace, &reference), &sealed)
                .unwrap()
                .expose(),
            PLAINTEXT
        );
        let newly_sealed = rotated
            .seal(
                context(&environment, &namespace, &reference),
                &SecretMaterial::new(PLAINTEXT.to_owned()),
            )
            .unwrap();
        assert_eq!(newly_sealed.kek_id(), &id("new"));
        assert!(matches!(
            old.open(context(&environment, &namespace, &reference), &newly_sealed),
            Err(BlobEnvelopeError::UnknownKek)
        ));
        assert_eq!(
            rotated.add_decrypt_only(id("old"), KekMaterial::from_array([0x33; KEY_BYTES])),
            Err(KekRingError::DuplicateId)
        );
    }

    #[test]
    fn wrong_key_material_under_the_right_id_fails_closed() {
        let (ring, environment, namespace, reference, sealed) = deterministic_sealed();
        assert_eq!(ring.active_id(), sealed.kek_id());
        let wrong_material = kek_ring("primary-2026-08", 0x99);
        assert!(matches!(
            wrong_material.open(context(&environment, &namespace, &reference), &sealed),
            Err(BlobEnvelopeError::Unopenable)
        ));
    }

    #[test]
    fn a_stored_kek_id_is_itself_authenticated() {
        let (mut ring, environment, namespace, reference, sealed) = deterministic_sealed();
        ring.add_decrypt_only(
            id("secondary-2026-08"),
            KekMaterial::from_array([0x11; KEY_BYTES]),
        )
        .unwrap();
        let mut changed = sealed;
        changed.kek_id = id("secondary-2026-08");
        assert!(matches!(
            ring.open(context(&environment, &namespace, &reference), &changed),
            Err(BlobEnvelopeError::Unopenable)
        ));
    }

    #[test]
    fn every_stored_byte_is_authenticated_or_a_strict_codec_field() {
        let (ring, environment, namespace, reference, sealed) = deterministic_sealed();
        let encoded = sealed.to_canonical_cbor();
        for offset in 0..encoded.len() {
            let mut changed = encoded.clone();
            changed[offset] ^= 1;
            if let Ok(changed) = SealedBlobSecret::from_canonical_cbor(&changed) {
                assert!(
                    ring.open(context(&environment, &namespace, &reference), &changed)
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
        nonminimal_scheme.splice(2..3, [0x78, u8::try_from(SCHEME.len()).unwrap()]);
        assert_eq!(
            SealedBlobSecret::from_canonical_cbor(&nonminimal_scheme),
            Err(CodecError::NonCanonical)
        );

        let kek_header = 2 + 1 + SCHEME.len();
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
        let ring = kek_ring("primary", 0x11);
        let (environment, namespace, reference) = fixture();
        let context = context(&environment, &namespace, &reference);
        assert_eq!(
            ring.seal(context, &SecretMaterial::new(String::new())),
            Err(BlobEnvelopeError::EmptyMaterial)
        );
        assert_eq!(
            ring.seal(
                context,
                &SecretMaterial::new("x".repeat(MAX_PLAINTEXT_BYTES + 1))
            ),
            Err(BlobEnvelopeError::MaterialTooLarge)
        );
        let maximum = SecretMaterial::new("x".repeat(MAX_PLAINTEXT_BYTES));
        let sealed = ring.seal(context, &maximum).unwrap();
        let encoded = sealed.to_canonical_cbor();
        assert_eq!(
            encoded.len(),
            encoded_size("primary".len(), MAX_CIPHERTEXT_BYTES)
        );
        assert!(encoded.len() <= MAX_SEALED_BYTES);
        assert_eq!(
            ring.open(
                context,
                &SealedBlobSecret::from_canonical_cbor(&encoded).unwrap()
            )
            .unwrap()
            .expose()
            .len(),
            MAX_PLAINTEXT_BYTES
        );

        let widest_ring = kek_ring(&"k".repeat(MAX_KEK_ID_BYTES), 0x11);
        let widest = widest_ring.seal(context, &maximum).unwrap();
        assert_eq!(widest.to_canonical_cbor().len(), MAX_SEALED_BYTES);
        assert_eq!(
            MAX_SEALED_BYTES,
            encoded_size(MAX_KEK_ID_BYTES, MAX_CIPHERTEXT_BYTES)
        );
    }

    #[test]
    fn exact_key_and_identifier_bounds_are_enforced() {
        assert_eq!(
            KekMaterial::from_bytes(vec![0; KEY_BYTES - 1]).unwrap_err(),
            KekRingError::KeyLength {
                found: KEY_BYTES - 1
            }
        );
        assert_eq!(
            KekMaterial::from_bytes(vec![0; KEY_BYTES + 1]).unwrap_err(),
            KekRingError::KeyLength {
                found: KEY_BYTES + 1
            }
        );
        KekMaterial::from_bytes(vec![0; KEY_BYTES]).expect("exact AES-256 key");
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
    fn context_is_not_stored_and_nothing_sensitive_is_rendered() {
        let (ring, environment, namespace, reference, sealed) = deterministic_sealed();
        let encoded = sealed.to_canonical_cbor();
        for absent in [
            environment.as_str().as_bytes(),
            namespace.as_str().as_bytes(),
            reference.to_string().as_bytes(),
            PLAINTEXT.as_bytes(),
        ] {
            assert!(
                !encoded.windows(absent.len()).any(|window| window == absent),
                "authenticated context or material entered the object"
            );
        }

        let rendered = [
            format!("{ring:?}"),
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
        let context = context(&environment, &namespace, &reference);
        let kek = id("primary-2026-08");
        let wrapping = aad(AadPurpose::WrapDek, context, &kek);
        let material = aad(AadPurpose::Material, context, &kek);
        assert_eq!(&wrapping[..AAD_DOMAIN.len()], AAD_DOMAIN);
        assert_eq!(wrapping[AAD_DOMAIN.len()], AadPurpose::WrapDek as u8);
        assert_eq!(material[AAD_DOMAIN.len()], AadPurpose::Material as u8);
        assert_eq!(wrapping.len(), material.len());
        assert_ne!(wrapping, material);
        assert_eq!(
            hex(&wrapping),
            "61786f6e642e7365637265742e656e76656c6f70652e76320001000970726f642d65617374000961636d652d70726f64000000000001700080000000000000010000000000000003000f7072696d6172792d323032362d3038"
        );
    }
}
