//! Bootstrap trust and signing primitives for blob-backed desired-state publication.
//!
//! The private signing key belongs only on administrative publishers. Serving
//! replicas receive a [`PublicationTrustStore`] containing public keys. The
//! wire formats live in `publication`; this module deliberately exposes no
//! unsigned publication constructor.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use ring::signature::{self, Ed25519KeyPair, KeyPair};

pub const PUBLICATION_SIGNATURE_SCHEMA: u64 = 1;
pub const ED25519_V1_ALGORITHM: &str = "ed25519.v1";
const ED25519_PUBLIC_KEY_BYTES: usize = 32;
const ED25519_SIGNATURE_BYTES: usize = 64;

/// A bounded, non-secret identifier for one publication verification key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PublicationKeyId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidPublicationKeyId {
    #[error("publication key identifier must not be empty")]
    Empty,
    #[error("publication key identifier exceeds the 128-byte limit")]
    TooLong,
    #[error("publication key identifier contains unsupported characters")]
    InvalidCharacter,
}

impl PublicationKeyId {
    pub const MAX_LEN: usize = 128;

    pub fn parse(value: impl Into<String>) -> Result<Self, InvalidPublicationKeyId> {
        let value = value.into();
        if value.is_empty() {
            return Err(InvalidPublicationKeyId::Empty);
        }
        if value.len() > Self::MAX_LEN {
            return Err(InvalidPublicationKeyId::TooLong);
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
        {
            return Err(InvalidPublicationKeyId::InvalidCharacter);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PublicationKeyId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Signature algorithm carried by authenticated publication metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicationSignatureAlgorithm {
    Ed25519V1,
}

impl PublicationSignatureAlgorithm {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ed25519V1 => ED25519_V1_ALGORITHM,
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, PublicationAuthenticationError> {
        match value {
            ED25519_V1_ALGORITHM => Ok(Self::Ed25519V1),
            _ => Err(PublicationAuthenticationError::UnknownAlgorithm),
        }
    }
}

/// Public bootstrap trust material for one publication signer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedPublicationKey {
    key_id: PublicationKeyId,
    algorithm: PublicationSignatureAlgorithm,
    public_key: [u8; ED25519_PUBLIC_KEY_BYTES],
}

impl TrustedPublicationKey {
    pub fn ed25519_v1(
        key_id: PublicationKeyId,
        public_key: &[u8],
    ) -> Result<Self, PublicationAuthenticationError> {
        let public_key = public_key.try_into().map_err(|_| {
            PublicationAuthenticationError::InvalidPublicKey {
                key_id: key_id.clone(),
            }
        })?;
        Ok(Self {
            key_id,
            algorithm: PublicationSignatureAlgorithm::Ed25519V1,
            public_key,
        })
    }

    pub fn key_id(&self) -> &PublicationKeyId {
        &self.key_id
    }
}

/// Verification-only bootstrap trust for blob publication documents.
#[derive(Debug, Clone)]
pub struct PublicationTrustStore {
    keys: Arc<BTreeMap<PublicationKeyId, TrustedPublicationKey>>,
}

impl PublicationTrustStore {
    pub fn new(
        keys: impl IntoIterator<Item = TrustedPublicationKey>,
    ) -> Result<Self, PublicationAuthenticationError> {
        let mut indexed = BTreeMap::new();
        for key in keys {
            let key_id = key.key_id.clone();
            if indexed.insert(key_id.clone(), key).is_some() {
                return Err(PublicationAuthenticationError::DuplicateKey { key_id });
            }
        }
        if indexed.is_empty() {
            return Err(PublicationAuthenticationError::EmptyTrustStore);
        }
        Ok(Self {
            keys: Arc::new(indexed),
        })
    }

    pub(crate) fn verify(
        &self,
        signature: &PublicationSignature,
        message: &[u8],
    ) -> Result<(), PublicationAuthenticationError> {
        if signature.schema_version != PUBLICATION_SIGNATURE_SCHEMA {
            return Err(PublicationAuthenticationError::UnknownSignatureSchema {
                found: signature.schema_version,
            });
        }
        let key = self
            .keys
            .get(&signature.key_id)
            .ok_or(PublicationAuthenticationError::UnknownKey)?;
        if key.algorithm != signature.algorithm {
            return Err(PublicationAuthenticationError::UnknownAlgorithm);
        }
        match signature.algorithm {
            PublicationSignatureAlgorithm::Ed25519V1 => {
                signature::UnparsedPublicKey::new(&signature::ED25519, key.public_key)
                    .verify(message, &signature.value)
                    .map_err(|_| PublicationAuthenticationError::InvalidSignature)
            }
        }
    }
}

/// Administrative Ed25519 publication signer.
///
/// `Debug` intentionally omits key material. The PKCS#8 input is never retained
/// separately or exposed through an accessor.
pub struct PublicationSigner {
    key_id: PublicationKeyId,
    key_pair: Ed25519KeyPair,
}

impl fmt::Debug for PublicationSigner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublicationSigner")
            .field("key_id", &self.key_id)
            .field("key_material", &"[REDACTED]")
            .finish()
    }
}

impl PublicationSigner {
    pub fn from_ed25519_pkcs8(
        key_id: PublicationKeyId,
        pkcs8: &[u8],
    ) -> Result<Self, PublicationAuthenticationError> {
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8).map_err(|_| {
            PublicationAuthenticationError::InvalidSigningKey {
                key_id: key_id.clone(),
            }
        })?;
        Ok(Self { key_id, key_pair })
    }

    pub fn key_id(&self) -> &PublicationKeyId {
        &self.key_id
    }

    pub const fn algorithm(&self) -> PublicationSignatureAlgorithm {
        PublicationSignatureAlgorithm::Ed25519V1
    }

    pub fn trusted_key(&self) -> TrustedPublicationKey {
        TrustedPublicationKey::ed25519_v1(self.key_id.clone(), self.key_pair.public_key().as_ref())
            .expect("an Ed25519 key pair always exposes a 32-byte public key")
    }

    pub(crate) fn sign(&self, message: &[u8]) -> PublicationSignature {
        PublicationSignature {
            schema_version: PUBLICATION_SIGNATURE_SCHEMA,
            algorithm: self.algorithm(),
            key_id: self.key_id.clone(),
            value: self
                .key_pair
                .sign(message)
                .as_ref()
                .try_into()
                .expect("an Ed25519 signature is always 64 bytes"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PublicationSignature {
    schema_version: u64,
    algorithm: PublicationSignatureAlgorithm,
    key_id: PublicationKeyId,
    value: [u8; ED25519_SIGNATURE_BYTES],
}

impl PublicationSignature {
    pub(crate) fn decode(
        schema_version: u64,
        algorithm: &str,
        key_id: &str,
        value: &[u8],
    ) -> Result<Self, PublicationAuthenticationError> {
        if schema_version != PUBLICATION_SIGNATURE_SCHEMA {
            return Err(PublicationAuthenticationError::UnknownSignatureSchema {
                found: schema_version,
            });
        }
        let algorithm = PublicationSignatureAlgorithm::parse(algorithm)?;
        let key_id = PublicationKeyId::parse(key_id.to_owned())
            .map_err(|_| PublicationAuthenticationError::InvalidKeyId)?;
        let value = value
            .try_into()
            .map_err(|_| PublicationAuthenticationError::InvalidSignatureEncoding)?;
        Ok(Self {
            schema_version,
            algorithm,
            key_id,
            value,
        })
    }

    pub(crate) const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    pub(crate) const fn algorithm(&self) -> PublicationSignatureAlgorithm {
        self.algorithm
    }

    pub(crate) fn key_id(&self) -> &PublicationKeyId {
        &self.key_id
    }

    pub(crate) fn value(&self) -> &[u8; ED25519_SIGNATURE_BYTES] {
        &self.value
    }
}

/// Fail-closed publication authentication taxonomy.
///
/// Variants never retain rejected bytes, key material, signatures, or unknown
/// algorithm text, so rendering an error cannot disclose durable input.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PublicationAuthenticationError {
    #[error("publication trust must contain at least one verification key")]
    EmptyTrustStore,
    #[error("publication trust contains duplicate key identifier `{key_id}`")]
    DuplicateKey { key_id: PublicationKeyId },
    #[error("publication key identifier is invalid")]
    InvalidKeyId,
    #[error("publication public key `{key_id}` is not a 32-byte Ed25519 key")]
    InvalidPublicKey { key_id: PublicationKeyId },
    #[error("publication signing key `{key_id}` is not valid Ed25519 PKCS#8")]
    InvalidSigningKey { key_id: PublicationKeyId },
    #[error("publication signature schema version {found} is not supported")]
    UnknownSignatureSchema { found: u64 },
    #[error("publication signature algorithm is not supported")]
    UnknownAlgorithm,
    #[error("publication signature key is not trusted")]
    UnknownKey,
    #[error("publication signature encoding is invalid")]
    InvalidSignatureEncoding,
    #[error("publication signature is invalid")]
    InvalidSignature,
}
