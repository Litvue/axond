//! Flat namespace identity and grants (ADR 0062).
//!
//! A namespace is an opaque consumer-selected isolation key. Its text form is
//! deliberately one canonical URL segment: Axond never splits it into tenant,
//! project, or any other hierarchy.

use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// An opaque, canonical namespace identifier.
///
/// The identifier is case-sensitive and is never normalized. Restricting it to
/// unreserved ASCII avoids alternate percent-encoded URL spellings; excluding
/// `.` also rules out dot-segment interpretation by intermediaries.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NamespaceId(String);

/// Why a namespace identifier was refused.
///
/// Refused text is never rendered. Namespace input reaches HTTP errors and log
/// lines, and a secret pasted into the path must not be echoed back.
#[derive(Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidNamespaceId {
    #[error("a namespace identifier must not be empty")]
    Empty,
    #[error("a namespace identifier is over the {max}-byte limit")]
    TooLong {
        value: String,
        length: usize,
        max: usize,
    },
    #[error(
        "a namespace identifier contains a character outside ASCII letters, digits, `-`, and `_`"
    )]
    Character { value: String },
    #[error("a namespace identifier must start and end with an ASCII letter or digit")]
    Boundary { value: String },
}

impl fmt::Debug for InvalidNamespaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("InvalidNamespaceId(<redacted>)")
    }
}

impl NamespaceId {
    /// Keeps URLs, storage keys, attribution fields, and authorization indexes
    /// bounded while leaving enough room for consumer-generated opaque ids.
    pub const MAX_LEN: usize = 63;

    pub fn parse(input: &str) -> Result<Self, InvalidNamespaceId> {
        if input.is_empty() {
            return Err(InvalidNamespaceId::Empty);
        }
        if input.len() > Self::MAX_LEN {
            return Err(InvalidNamespaceId::TooLong {
                value: input.to_owned(),
                length: input.len(),
                max: Self::MAX_LEN,
            });
        }
        if !input
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(InvalidNamespaceId::Character {
                value: input.to_owned(),
            });
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
            return Err(InvalidNamespaceId::Boundary {
                value: input.to_owned(),
            });
        }
        Ok(Self(input.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NamespaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for NamespaceId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for NamespaceId {
    type Err = InvalidNamespaceId;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

impl Serialize for NamespaceId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for NamespaceId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let input = String::deserialize(deserializer)?;
        Self::parse(&input).map_err(de::Error::custom)
    }
}

/// The namespace authority produced by inbound authentication.
///
/// Current static keys and `axt1` tokens each produce a one-namespace grant.
/// Keeping the grant as a set makes the authorization boundary explicit
/// without making the URL subordinate to a token claim: the path still selects
/// exactly one namespace. Set/all token-claim projection is a later slice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceGrant(GrantKind);

#[derive(Clone, Debug, PartialEq, Eq)]
enum GrantKind {
    All,
    Set(BTreeSet<NamespaceId>),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidNamespaceGrant {
    #[error("a namespace grant must name at least one namespace")]
    Empty,
    #[error("a namespace grant names {count} namespaces, over the {max}-namespace limit")]
    TooMany { count: usize, max: usize },
}

impl NamespaceGrant {
    pub const MAX_NAMESPACES: usize = 64;

    pub fn one(namespace: NamespaceId) -> Self {
        Self(GrantKind::Set(BTreeSet::from([namespace])))
    }

    pub fn set(
        namespaces: impl IntoIterator<Item = NamespaceId>,
    ) -> Result<Self, InvalidNamespaceGrant> {
        let namespaces = namespaces.into_iter().collect::<BTreeSet<_>>();
        if namespaces.is_empty() {
            return Err(InvalidNamespaceGrant::Empty);
        }
        if namespaces.len() > Self::MAX_NAMESPACES {
            return Err(InvalidNamespaceGrant::TooMany {
                count: namespaces.len(),
                max: Self::MAX_NAMESPACES,
            });
        }
        Ok(Self(GrantKind::Set(namespaces)))
    }

    pub const fn all() -> Self {
        Self(GrantKind::All)
    }

    pub const fn is_all(&self) -> bool {
        matches!(self.0, GrantKind::All)
    }

    pub fn permits(&self, namespace: &NamespaceId) -> bool {
        match &self.0 {
            GrantKind::All => true,
            GrantKind::Set(namespaces) => namespaces.contains(namespace),
        }
    }

    pub fn namespaces(&self) -> Option<&BTreeSet<NamespaceId>> {
        match &self.0 {
            GrantKind::All => None,
            GrantKind::Set(namespaces) => Some(namespaces),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_accepts_only_one_canonical_opaque_url_segment() {
        let longest = "z".repeat(NamespaceId::MAX_LEN);
        for input in ["a", "platform", "Acme_01-prod", longest.as_str()] {
            let id = NamespaceId::parse(input).expect("canonical namespace id");
            assert_eq!(id.as_str(), input);
            assert_eq!(id.to_string(), input);
            assert_eq!(format!("{id:?}"), input);
            assert_eq!(input.parse::<NamespaceId>().unwrap(), id);
        }

        assert_eq!(NamespaceId::parse(""), Err(InvalidNamespaceId::Empty));
        assert!(matches!(
            NamespaceId::parse(&"a".repeat(NamespaceId::MAX_LEN + 1)),
            Err(InvalidNamespaceId::TooLong { .. })
        ));
        for input in [
            "acme/core",
            "acme.core",
            "acme%2fcore",
            "acme%2Fcore",
            ".",
            "..",
            "café",
            "acme core",
            "acme\\core",
        ] {
            assert!(
                matches!(
                    NamespaceId::parse(input),
                    Err(InvalidNamespaceId::Character { .. })
                ),
                "`{input}` must not have an alternate URL interpretation"
            );
        }
        for input in ["-acme", "acme-", "_acme", "acme_"] {
            assert!(matches!(
                NamespaceId::parse(input),
                Err(InvalidNamespaceId::Boundary { .. })
            ));
        }
    }

    #[test]
    fn parser_refusals_do_not_echo_the_input() {
        let material = "sk-live-0123456789abcdefghij/secret";
        let error = NamespaceId::parse(material).expect_err("slash is refused");
        assert!(!error.to_string().contains(material));
        assert!(!format!("{error:?}").contains(material));
    }

    #[test]
    fn serde_is_a_validated_string_round_trip() {
        let id = NamespaceId::parse("Acme_01-prod").unwrap();
        assert_eq!(serde_json::to_string(&id).unwrap(), r#""Acme_01-prod""#);
        assert_eq!(
            serde_json::from_str::<NamespaceId>(r#""Acme_01-prod""#).unwrap(),
            id
        );

        for invalid in [r#"""#, r#""acme/core""#, r#""acme%2Fcore""#, r#""café""#] {
            assert!(
                serde_json::from_str::<NamespaceId>(invalid).is_err(),
                "{invalid}"
            );
        }
        for wrong_type in ["null", "1", "true", "[]", "{}"] {
            assert!(
                serde_json::from_str::<NamespaceId>(wrong_type).is_err(),
                "{wrong_type}"
            );
        }
    }

    #[test]
    fn grants_authorize_the_selected_namespace_only() {
        let acme = NamespaceId::parse("acme").unwrap();
        let globex = NamespaceId::parse("globex").unwrap();
        assert!(NamespaceGrant::one(acme.clone()).permits(&acme));
        assert!(!NamespaceGrant::one(acme).permits(&globex));
    }

    #[test]
    fn set_and_all_grants_are_bounded_and_explicit() {
        let acme = NamespaceId::parse("acme").unwrap();
        let globex = NamespaceId::parse("globex").unwrap();
        let set = NamespaceGrant::set([globex.clone(), acme.clone(), acme.clone()]).unwrap();
        assert!(set.permits(&acme));
        assert!(set.permits(&globex));
        assert_eq!(set.namespaces().unwrap().len(), 2);
        assert!(NamespaceGrant::all().permits(&acme));
        assert!(NamespaceGrant::all().namespaces().is_none());
        assert_eq!(
            NamespaceGrant::set(Vec::new()),
            Err(InvalidNamespaceGrant::Empty)
        );
    }
}
