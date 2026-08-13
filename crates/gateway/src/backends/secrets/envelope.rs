//! Envelope encryption: the deployment KEK, and what a sealed record is.
//!
//! Material is never stored under the deployment key itself. Every version gets
//! a fresh 256-bit data-encryption key, the material is sealed under that, and
//! the DEK is sealed under the KEK the deployment references — so a KEK rotation
//! is a re-wrap of small fixed-size blobs rather than a re-encryption of every
//! secret, and one recovered DEK exposes exactly one version.
//!
//! Two properties are worth stating because the types enforce them rather than
//! the storage layer remembering to:
//!
//! - **A sealed record is bound to its reference and its owner.** Both AADs
//!   carry the scheme, the [`SecretOwner`], and the exact [`SecretRef`], so a row
//!   copied to another tenant, renumbered to another version, or moved to another
//!   secret id stops opening. Storage-level tampering is therefore an unwrap
//!   failure — [`SecretError::Unwrap`](super::SecretError::Unwrap), which is
//!   `Corrupt` and pages an operator — rather than a silent authorization with
//!   somebody else's key.
//! - **Plaintext and key bytes leave by one door each.** The KEK is an opaque
//!   [`LessSafeKey`] this module never exposes, DEK bytes are zeroized before the
//!   frame they were generated in returns, and material comes back as
//!   [`SecretMaterial`], which has no `Display`, no `Serialize`, and a `Debug`
//!   that prints a marker.

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use ring::aead::{AES_256_GCM, Aad, LessSafeKey, NONCE_LEN, Nonce, UnboundKey};
use ring::rand::{SecureRandom, SystemRandom};
use secrecy::zeroize::Zeroize;

use super::{KekRef, SecretMaterial};
use crate::desired_state::secrets::{SecretOwner, SecretRef};

/// The sealing scheme recorded on every row.
///
/// Stored rather than assumed, so a future scheme is a value a reader refuses
/// explicitly instead of a decryption failure it cannot explain.
pub const SCHEME: &str = "aes256-gcm.envelope.v1";

/// AES-256 keys, both the KEK and every DEK.
pub const KEY_LEN: usize = 32;

/// Why the deployment's key-encryption key is not usable.
///
/// Every arm names the *reference* — the env var or file the operator pointed at
/// — and never any part of the material, including its bytes, its prefix, or a
/// hash of it. A length is reported because a 16-byte key is an operator error
/// worth naming precisely, and knowing a rejected key was the wrong length
/// discloses nothing about the right one.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KekError {
    #[error("key-encryption key `{reference}` is empty")]
    Empty { reference: KekRef },
    #[error(
        "key-encryption key `{reference}` is not base64: a KEK is {KEY_LEN} random bytes, \
         base64-encoded (`openssl rand -base64 {KEY_LEN}`)"
    )]
    Encoding { reference: KekRef },
    #[error(
        "key-encryption key `{reference}` decodes to {found} bytes; AES-256 needs exactly \
         {KEY_LEN}"
    )]
    Length { reference: KekRef, found: usize },
}

/// Why sealing or opening a record failed.
///
/// Deliberately coarse on the opening side: which of nonce, tag, AAD, or wrapped
/// DEK failed is not information a caller may act on differently, and reporting
/// it would describe stored ciphertext to whoever reads the log.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// The system CSPRNG refused. Not a storage failure and not retryable here.
    #[error("secure random bytes are unavailable")]
    Random,
    /// The record does not open under this KEK: a rotated or wrong key, a
    /// tampered row, or a record bound to a different reference or owner.
    #[error("the sealed record does not open under this key-encryption key")]
    Unopenable,
    /// A record written by a scheme this build does not implement.
    #[error("sealed record uses scheme `{found}`, which this build does not read")]
    UnknownScheme { found: String },
    /// A record whose fixed-size fields are not the sizes the scheme defines.
    #[error("sealed record is malformed: {detail}")]
    Malformed { detail: String },
}

/// Material sealed for storage: the ciphertext, the wrapped DEK, both nonces,
/// and the non-secret labels a reader needs before it can try to open it.
///
/// The byte fields are ciphertext, so they are safe at rest — but there is no
/// derived `Debug` printing them anyway, because a ciphertext in a log line is
/// still an artefact nobody asked for.
#[derive(Clone, PartialEq, Eq)]
pub struct SealedSecret {
    /// The scheme these bytes were produced by. See [`SCHEME`].
    pub scheme: String,
    /// The KEK reference the DEK was wrapped under, so a rotation can tell a
    /// re-wrapped row from one still sealed under the previous key.
    pub kek: KekRef,
    /// The DEK, sealed under the KEK, with its tag appended.
    pub wrapped_dek: Vec<u8>,
    pub dek_nonce: Vec<u8>,
    /// The material, sealed under the DEK, with its tag appended.
    pub ciphertext: Vec<u8>,
    pub nonce: Vec<u8>,
}

impl std::fmt::Debug for SealedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedSecret")
            .field("scheme", &self.scheme)
            .field("kek", &self.kek)
            .field("ciphertext_len", &self.ciphertext.len())
            .finish_non_exhaustive()
    }
}

impl SealedSecret {
    /// Check the fixed-size fields before any key is touched, so a malformed row
    /// is a described refusal rather than an opaque unwrap failure.
    fn validate(&self) -> Result<(), EnvelopeError> {
        if self.scheme != SCHEME {
            return Err(EnvelopeError::UnknownScheme {
                found: self.scheme.clone(),
            });
        }
        for (field, nonce) in [("dek_nonce", &self.dek_nonce), ("nonce", &self.nonce)] {
            if nonce.len() != NONCE_LEN {
                return Err(EnvelopeError::Malformed {
                    detail: format!(
                        "{field} is {} bytes, and the scheme uses {NONCE_LEN}",
                        nonce.len()
                    ),
                });
            }
        }
        if self.wrapped_dek.len() != KEY_LEN + AES_256_GCM.tag_len() {
            return Err(EnvelopeError::Malformed {
                detail: format!(
                    "wrapped_dek is {} bytes, and the scheme wraps {KEY_LEN} plus a {}-byte tag",
                    self.wrapped_dek.len(),
                    AES_256_GCM.tag_len()
                ),
            });
        }
        if self.ciphertext.len() < AES_256_GCM.tag_len() {
            return Err(EnvelopeError::Malformed {
                detail: "ciphertext is shorter than its own authentication tag".to_owned(),
            });
        }
        Ok(())
    }
}

/// The deployment's key-encryption key, resolved from its bootstrap reference.
///
/// Holds the key as a `ring` [`LessSafeKey`] and nothing else: there is no
/// accessor for the bytes, so a future caller cannot log, serialize, or re-derive
/// them. The nonce discipline `LessSafeKey` leaves to the caller is satisfied by
/// generating a fresh random 96-bit nonce per seal and never sealing twice under
/// one key without one — every DEK seals exactly one message, and the KEK's
/// nonces are random per wrap.
pub struct DeploymentKek {
    reference: KekRef,
    key: LessSafeKey,
    rng: SystemRandom,
}

impl std::fmt::Debug for DeploymentKek {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeploymentKek")
            .field("reference", &self.reference)
            .finish_non_exhaustive()
    }
}

impl DeploymentKek {
    /// Resolve the key from the material an operator put behind `reference`.
    ///
    /// Base64, padded or not, surrounding whitespace tolerated: the value comes
    /// from an env var or a file an operator wrote, and a trailing newline is not
    /// a configuration error worth an outage. The decoded buffer is zeroized on
    /// every path out of here, including the failing ones.
    pub fn parse(reference: KekRef, encoded: &str) -> Result<Self, KekError> {
        let trimmed = encoded.trim();
        if trimmed.is_empty() {
            return Err(KekError::Empty { reference });
        }
        let mut bytes = STANDARD
            .decode(trimmed)
            .or_else(|_| STANDARD_NO_PAD.decode(trimmed))
            .map_err(|_| KekError::Encoding {
                reference: reference.clone(),
            })?;
        let result = if bytes.len() == KEY_LEN {
            UnboundKey::new(&AES_256_GCM, &bytes)
                .map(|key| Self {
                    reference: reference.clone(),
                    key: LessSafeKey::new(key),
                    rng: SystemRandom::new(),
                })
                .map_err(|_| KekError::Length {
                    reference: reference.clone(),
                    found: bytes.len(),
                })
        } else {
            Err(KekError::Length {
                reference,
                found: bytes.len(),
            })
        };
        bytes.zeroize();
        result
    }

    /// The reference the key was resolved from: a name, for logs and for the
    /// `kek` column beside every sealed row.
    pub const fn reference(&self) -> &KekRef {
        &self.reference
    }

    /// Seal material for one exact version of one owner's secret.
    pub fn seal(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        material: &SecretMaterial,
    ) -> Result<SealedSecret, EnvelopeError> {
        let mut dek = [0u8; KEY_LEN];
        self.rng.fill(&mut dek).map_err(|_| EnvelopeError::Random)?;
        let result = self.seal_with(owner, reference, material, &dek);
        dek.zeroize();
        result
    }

    fn seal_with(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        material: &SecretMaterial,
        dek: &[u8; KEY_LEN],
    ) -> Result<SealedSecret, EnvelopeError> {
        let dek_nonce = self.nonce()?;
        let nonce = self.nonce()?;

        let mut wrapped_dek = dek.to_vec();
        self.key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(dek_nonce),
                Aad::from(dek_aad(owner, reference)),
                &mut wrapped_dek,
            )
            .map_err(|_| EnvelopeError::Unopenable)?;

        let data_key = LessSafeKey::new(
            UnboundKey::new(&AES_256_GCM, dek).map_err(|_| EnvelopeError::Random)?,
        );
        let mut ciphertext = material.expose().as_bytes().to_vec();
        let sealed = data_key
            .seal_in_place_append_tag(
                Nonce::assume_unique_for_key(nonce),
                Aad::from(material_aad(owner, reference)),
                &mut ciphertext,
            )
            .map_err(|_| EnvelopeError::Unopenable);
        if sealed.is_err() {
            ciphertext.zeroize();
            return Err(EnvelopeError::Unopenable);
        }

        Ok(SealedSecret {
            scheme: SCHEME.to_owned(),
            kek: self.reference.clone(),
            wrapped_dek,
            dek_nonce: dek_nonce.to_vec(),
            ciphertext,
            nonce: nonce.to_vec(),
        })
    }

    /// Open a sealed record for the owner and version it was sealed for.
    ///
    /// The owner and reference are inputs rather than fields of the record: they
    /// are the AAD, so opening *is* the check that the row is the one the caller
    /// asked for. A row from another tenant does not open, whatever its columns
    /// say.
    pub fn open(
        &self,
        owner: SecretOwner,
        reference: &SecretRef,
        sealed: &SealedSecret,
    ) -> Result<SecretMaterial, EnvelopeError> {
        sealed.validate()?;
        let mut wrapped = sealed.wrapped_dek.clone();
        let dek = self
            .key
            .open_in_place(
                nonce_of(&sealed.dek_nonce)?,
                Aad::from(dek_aad(owner, reference)),
                &mut wrapped,
            )
            .map_err(|_| EnvelopeError::Unopenable)?;
        let data_key = UnboundKey::new(&AES_256_GCM, dek)
            .map(LessSafeKey::new)
            .map_err(|_| EnvelopeError::Unopenable);
        wrapped.zeroize();
        let data_key = data_key?;

        let mut ciphertext = sealed.ciphertext.clone();
        let opened = data_key
            .open_in_place(
                nonce_of(&sealed.nonce)?,
                Aad::from(material_aad(owner, reference)),
                &mut ciphertext,
            )
            .map_err(|_| EnvelopeError::Unopenable)
            .and_then(|plaintext| {
                std::str::from_utf8(plaintext)
                    .map(|text| SecretMaterial::new(text.to_owned()))
                    .map_err(|_| EnvelopeError::Malformed {
                        detail: "opened material is not valid UTF-8".to_owned(),
                    })
            });
        ciphertext.zeroize();
        opened
    }

    fn nonce(&self) -> Result<[u8; NONCE_LEN], EnvelopeError> {
        let mut nonce = [0u8; NONCE_LEN];
        self.rng
            .fill(&mut nonce)
            .map_err(|_| EnvelopeError::Random)?;
        Ok(nonce)
    }
}

fn nonce_of(bytes: &[u8]) -> Result<Nonce, EnvelopeError> {
    let bytes: [u8; NONCE_LEN] = bytes.try_into().map_err(|_| EnvelopeError::Malformed {
        detail: format!("a nonce is {NONCE_LEN} bytes"),
    })?;
    Ok(Nonce::assume_unique_for_key(bytes))
}

/// What the wrapped DEK is authenticated against.
fn dek_aad(owner: SecretOwner, reference: &SecretRef) -> Vec<u8> {
    format!("{SCHEME}|dek|{owner}|{reference}").into_bytes()
}

/// What the sealed material is authenticated against. Distinct from the DEK's,
/// so a wrapped DEK and a ciphertext can never be interchanged.
fn material_aad(owner: SecretOwner, reference: &SecretRef) -> Vec<u8> {
    format!("{SCHEME}|material|{owner}|{reference}").into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::fixtures::{project_id, secret_id, tenant_id};

    const PLAINTEXT: &str = "sk-live-do-not-log";

    fn kek(label: &str, seed: u8) -> DeploymentKek {
        DeploymentKek::parse(KekRef(label.to_owned()), &STANDARD.encode([seed; KEY_LEN]))
            .expect("32 base64 bytes are a key")
    }

    fn owner() -> SecretOwner {
        SecretOwner::tenant(tenant_id(1))
    }

    fn reference() -> SecretRef {
        SecretRef::first(secret_id(1))
    }

    #[test]
    fn material_round_trips_under_the_reference_it_was_sealed_for() {
        let kek = kek("AXOND_KEK", 7);
        let sealed = kek
            .seal(
                owner(),
                &reference(),
                &SecretMaterial::new(PLAINTEXT.into()),
            )
            .expect("sealing");
        assert_eq!(sealed.scheme, SCHEME);
        assert_eq!(sealed.kek, KekRef("AXOND_KEK".to_owned()));
        assert_eq!(
            kek.open(owner(), &reference(), &sealed).unwrap().expose(),
            PLAINTEXT
        );

        // The record is ciphertext at rest, and neither it nor its Debug carries
        // the material.
        assert!(!sealed.ciphertext.windows(6).any(|w| w == b"sk-liv"));
        assert!(!format!("{sealed:?}").contains("sk-live"));
    }

    /// Two seals of one plaintext differ, so equal material is not detectable
    /// from stored rows — including across owners.
    #[test]
    fn sealing_is_randomized() {
        let kek = kek("AXOND_KEK", 7);
        let material = SecretMaterial::new(PLAINTEXT.into());
        let first = kek.seal(owner(), &reference(), &material).unwrap();
        let second = kek.seal(owner(), &reference(), &material).unwrap();
        assert_ne!(first.ciphertext, second.ciphertext);
        assert_ne!(first.wrapped_dek, second.wrapped_dek);
        assert_ne!(first.nonce, second.nonce);
    }

    /// The AAD is the isolation: a row read as another owner's, another version,
    /// or another secret does not open, whatever the storage layer believed.
    #[test]
    fn a_record_only_opens_for_its_own_owner_and_version() {
        let kek = kek("AXOND_KEK", 7);
        let sealed = kek
            .seal(
                owner(),
                &reference(),
                &SecretMaterial::new(PLAINTEXT.into()),
            )
            .unwrap();

        for (owner, reference) in [
            (SecretOwner::tenant(tenant_id(9)), reference()),
            (
                SecretOwner::project(tenant_id(1), project_id(2)),
                reference(),
            ),
            (owner(), reference().rotated()),
            (owner(), SecretRef::first(secret_id(2))),
        ] {
            assert_eq!(
                kek.open(owner, &reference, &sealed).err(),
                Some(EnvelopeError::Unopenable)
            );
        }
    }

    #[test]
    fn a_different_kek_cannot_open_a_record() {
        let sealed = kek("AXOND_KEK", 7)
            .seal(
                owner(),
                &reference(),
                &SecretMaterial::new(PLAINTEXT.into()),
            )
            .unwrap();
        assert_eq!(
            kek("AXOND_KEK_ROTATED", 8)
                .open(owner(), &reference(), &sealed)
                .err(),
            Some(EnvelopeError::Unopenable)
        );
    }

    #[test]
    fn tampering_with_stored_bytes_is_refused() {
        let kek = kek("AXOND_KEK", 7);
        let sealed = kek
            .seal(
                owner(),
                &reference(),
                &SecretMaterial::new(PLAINTEXT.into()),
            )
            .unwrap();

        let mut flipped = sealed.clone();
        flipped.ciphertext[0] ^= 0x01;
        assert_eq!(
            kek.open(owner(), &reference(), &flipped).err(),
            Some(EnvelopeError::Unopenable)
        );

        let mut rewrapped = sealed.clone();
        rewrapped.wrapped_dek[0] ^= 0x01;
        assert_eq!(
            kek.open(owner(), &reference(), &rewrapped).err(),
            Some(EnvelopeError::Unopenable)
        );

        // Malformed fixed-size fields are described rather than reported as a
        // failed decryption an operator would go looking for a key for.
        let mut truncated = sealed.clone();
        truncated.nonce.pop();
        assert!(matches!(
            kek.open(owner(), &reference(), &truncated),
            Err(EnvelopeError::Malformed { .. })
        ));

        let mut short_dek = sealed.clone();
        short_dek.wrapped_dek.pop();
        assert!(matches!(
            kek.open(owner(), &reference(), &short_dek),
            Err(EnvelopeError::Malformed { .. })
        ));

        let mut newer = sealed;
        newer.scheme = "aes256-gcm.envelope.v2".to_owned();
        assert_eq!(
            kek.open(owner(), &reference(), &newer).err(),
            Some(EnvelopeError::UnknownScheme {
                found: "aes256-gcm.envelope.v2".to_owned()
            })
        );
    }

    #[test]
    fn a_kek_reference_is_named_and_its_material_never_is() {
        let reference = KekRef("AXOND_KEK".to_owned());
        let errors = [
            DeploymentKek::parse(reference.clone(), "   ").unwrap_err(),
            DeploymentKek::parse(reference.clone(), "not base64!!").unwrap_err(),
            DeploymentKek::parse(reference.clone(), &STANDARD.encode([1u8; 16])).unwrap_err(),
        ];
        assert!(matches!(errors[0], KekError::Empty { .. }));
        assert!(matches!(errors[1], KekError::Encoding { .. }));
        assert!(matches!(errors[2], KekError::Length { found: 16, .. }));
        for error in &errors {
            let rendered = error.to_string();
            assert!(rendered.contains("AXOND_KEK"), "{rendered}");
            assert!(!rendered.contains("AAAA"), "{rendered}");
        }

        // Whitespace an operator's file or unit ends with is not a boot failure,
        // and unpadded base64 is accepted too.
        let padded = STANDARD.encode([3u8; KEY_LEN]);
        DeploymentKek::parse(reference.clone(), &format!("{padded}\n"))
            .expect("a trailing newline");
        let unpadded = STANDARD_NO_PAD.encode([3u8; KEY_LEN]);
        DeploymentKek::parse(reference, &unpadded).expect("unpadded base64");
    }

    #[test]
    fn a_key_is_not_debuggable() {
        let rendered = format!("{:?}", kek("AXOND_KEK", 7));
        assert!(rendered.contains("AXOND_KEK"));
        assert!(!rendered.contains("BwcH"), "{rendered}");
    }
}
