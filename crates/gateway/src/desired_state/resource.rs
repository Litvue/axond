//! Content-addressed blob references for retained catalogue snapshots.
//!
//! A [`BlobRef`] records a payload's kind, checksum, and byte length without
//! embedding the payload. Verification checks both length and content, so a
//! stored catalogue cannot hydrate from truncated or substituted bytes.
//!
//! ADR 0063 withdrew resource identities, versions, and dependency envelopes.
//! This module retains the content-addressed blob primitives used by catalogue
//! snapshots.

use super::canonical::{Canonical, CanonicalValue, Checksum};

/// What a content-addressed blob holds.
///
/// Naming the classes keeps a blob self-describing: a manifest entry says "this
/// digest is a catalogue snapshot", so a mismatched or misfiled payload is
/// detectable without parsing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BlobKind {
    /// A full upstream model-metadata snapshot (models.dev), which is large,
    /// immutable, and shared by every revision that did not change it.
    CatalogSnapshot,
}

impl BlobKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CatalogSnapshot => "catalog-snapshot",
        }
    }
}

/// A reference to an immutable payload stored once and addressed by its digest.
///
/// This is how a manifest can pin a multi-megabyte catalogue snapshot without
/// containing it. Content addressing does the deduplication for free: a revision
/// that changes only an alias re-references the same digest, so N revisions of a
/// deployment hold one copy of the snapshot, not N. It also makes the reference
/// self-verifying — [`BlobRef::verify`] is the only way to accept a payload, so a
/// truncated or substituted blob is a typed error rather than state that hydrates
/// into a running snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobRef {
    pub kind: BlobKind,
    pub digest: Checksum,
    pub size_bytes: u64,
}

/// Why a blob payload was not accepted for its reference.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlobError {
    #[error("blob {expected} is {actual_bytes} bytes, not the referenced {expected_bytes}")]
    Size {
        expected: Checksum,
        expected_bytes: u64,
        actual_bytes: u64,
    },
    #[error("blob payload hashes to {actual}, not the referenced {expected}")]
    Digest {
        expected: Checksum,
        actual: Checksum,
    },
}

impl BlobRef {
    /// The reference for a payload that is in hand.
    pub fn of(kind: BlobKind, payload: &[u8]) -> Self {
        Self {
            kind,
            digest: Checksum::of(payload),
            size_bytes: payload.len() as u64,
        }
    }

    /// Check a payload against this reference.
    ///
    /// Size is checked first only so the error names the cheaper discrepancy
    /// when both are wrong; either failure is a refusal, never a warning.
    pub fn verify(&self, payload: &[u8]) -> Result<(), BlobError> {
        if payload.len() as u64 != self.size_bytes {
            return Err(BlobError::Size {
                expected: self.digest,
                expected_bytes: self.size_bytes,
                actual_bytes: payload.len() as u64,
            });
        }
        let actual = Checksum::of(payload);
        if actual != self.digest {
            return Err(BlobError::Digest {
                expected: self.digest,
                actual,
            });
        }
        Ok(())
    }
}

impl std::fmt::Display for BlobRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{} ({} bytes)",
            self.kind.as_str(),
            self.digest,
            self.size_bytes
        )
    }
}

impl Canonical for BlobRef {
    fn canonical(&self) -> CanonicalValue {
        CanonicalValue::map([
            ("kind", CanonicalValue::string(self.kind.as_str())),
            (
                "digest",
                CanonicalValue::Bytes(self.digest.as_bytes().to_vec()),
            ),
            ("size_bytes", CanonicalValue::integer(self.size_bytes)),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_blob_reference_verifies_its_payload() {
        let payload = b"{\"models\":[]}".repeat(64);
        let reference = BlobRef::of(BlobKind::CatalogSnapshot, &payload);
        assert_eq!(reference.size_bytes, payload.len() as u64);
        reference
            .verify(&payload)
            .expect("the payload it addresses");

        // Content addressing deduplicates: the same snapshot in another revision
        // is the same reference, so it is stored once.
        assert_eq!(reference, BlobRef::of(BlobKind::CatalogSnapshot, &payload));
    }

    #[test]
    fn a_substituted_or_truncated_blob_is_refused() {
        let payload = b"catalogue".to_vec();
        let reference = BlobRef::of(BlobKind::CatalogSnapshot, &payload);

        let truncated = &payload[..payload.len() - 1];
        assert!(matches!(
            reference.verify(truncated),
            Err(BlobError::Size {
                expected_bytes: 9,
                actual_bytes: 8,
                ..
            })
        ));

        let substituted = b"catalogxes".to_vec();
        let error = reference
            .verify(&substituted[..9])
            .expect_err("same length, different bytes");
        assert!(matches!(error, BlobError::Digest { .. }));
        assert!(error.to_string().contains("hashes to"));
    }
}
