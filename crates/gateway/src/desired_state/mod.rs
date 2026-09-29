//! Value types the live gateway still borrows from the withdrawn desired-state
//! domain (ADR 0063): canonical encoding and checksums, identifiers, catalogue
//! offerings, and the typed policy and pricing documents.

pub mod canonical;
pub mod ids;
pub mod resource;

pub use canonical::{Canonical, CanonicalError, CanonicalValue, Checksum};
pub use resource::{BlobError, BlobKind, BlobRef};
