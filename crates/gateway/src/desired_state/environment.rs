//! Publication-scoped deployment environment identity.
//!
//! This is shared by immutable publication and every consumer of authenticated
//! publication metadata. Keeping one type prevents a secret backend from
//! accepting an environment spelling that the signed publication protocol
//! would refuse (or vice versa).

use std::fmt;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_environment_alphabet_and_bound_are_exact() {
        for valid in ["a", "prod-us.east_1", &"a".repeat(EnvironmentId::MAX_LEN)] {
            assert!(EnvironmentId::parse(valid).is_ok(), "refused {valid:?}");
        }
        for invalid in ["", "Prod", "-prod", "prod-", "prod/us", "prod us"] {
            assert!(
                EnvironmentId::parse(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
        assert!(matches!(
            EnvironmentId::parse("a".repeat(EnvironmentId::MAX_LEN + 1)),
            Err(InvalidEnvironmentId::TooLong { .. })
        ));
    }
}
