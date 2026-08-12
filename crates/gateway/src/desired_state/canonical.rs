//! The canonical serializer and the checksums taken over its output.
//!
//! Desired state has to have *one* byte representation, because a checksum over
//! it is what makes two questions answerable: "is the state a replica loaded the
//! state that was published?" and "is this candidate the same change I already
//! applied?" (#164, #165, #166). JSON cannot answer either — key order, integer
//! spelling, and float formatting are all free variables.
//!
//! So this module defines a small value model and a byte encoding for it:
//!
//! - **Deterministic ordering.** Map keys are sorted by their encoded bytes, and
//!   a [`CanonicalValue::Set`] — the shape for a collection whose order carries
//!   no meaning — is sorted by its members' encodings. A caller therefore cannot
//!   change the bytes by building the same state in a different order.
//! - **Normalized integers.** Every integer, of every width and signedness,
//!   encodes as one 16-byte two's-complement `i128`, so `1u32` and `1i64` are
//!   the same bytes.
//! - **No floating point.** [`CanonicalValue`] has no float variant, so a
//!   non-associative, platform-formattable value cannot enter a checksum at all.
//!   Prices are micro-dollar integers for exactly this reason (ADR 0010), and
//!   [`CanonicalValue::try_from_json`] rejects a JSON float with a typed error
//!   rather than rounding it.
//! - **Normalized strings.** Strings are UTF-8, length-prefixed rather than
//!   delimited or escaped (so no escaping choice can vary), and refused if they
//!   carry control characters or a byte-order mark. Unicode-equivalence
//!   normalization is *not* attempted here: everything identity-bearing is an
//!   ASCII [`Slug`](super::ids::Slug) or a UUID, and human-facing prose is
//!   normalized at the admin edge before it ever reaches a checksum.
//! - **Explicit versioning.** [`SerializerVersion`] is written into the byte
//!   stream. A future encoding change is a new variant, which means old
//!   checksums stay verifiable instead of silently becoming wrong.
//!
//! Encoding is fallible on purpose: the errors are the ones that would otherwise
//! produce two byte strings for one state (duplicate map keys, duplicate set
//! members) or an unverifiable one (a rejected string).

use std::fmt;

use ring::digest::{Context, SHA256};

/// The versioned canonical encoding.
///
/// One variant today. The version is in the bytes, so a second encoding cannot
/// be mistaken for the first, and a stored checksum records which encoding
/// produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum SerializerVersion {
    /// Tagged, length-prefixed, sorted. See the module documentation.
    #[default]
    V1,
}

impl SerializerVersion {
    /// The domain separator written before any value, so canonical bytes cannot
    /// be confused with another format's bytes that happen to collide.
    const MAGIC: &'static [u8] = b"axond.desired-state\0";

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::V1 => "axond.desired-state.v1",
        }
    }

    const fn tag(self) -> u8 {
        match self {
            Self::V1 => 1,
        }
    }

    /// Encode a value into its canonical bytes.
    pub fn encode(self, value: &CanonicalValue) -> Result<Vec<u8>, CanonicalError> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(Self::MAGIC);
        out.push(self.tag());
        value.write(&mut out)?;
        Ok(out)
    }
}

impl fmt::Display for SerializerVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The value model desired state canonicalizes through.
///
/// Note what is missing: floats, and any notion of "null". An absent field is
/// absent from the map; there is no second way to spell it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CanonicalValue {
    Bool(bool),
    /// Any integer, normalized to one width and signedness.
    Integer(i128),
    String(String),
    /// Opaque bytes — a digest, a key fingerprint. Never secret material.
    Bytes(Vec<u8>),
    /// An ordered sequence: position carries meaning (alias target priority,
    /// for instance), so it is preserved exactly.
    List(Vec<CanonicalValue>),
    /// An unordered collection: the caller's order carries no meaning, so it is
    /// sorted and duplicates are refused.
    Set(Vec<CanonicalValue>),
    /// A string-keyed record. Keys are sorted; duplicates are refused.
    Map(Vec<(String, CanonicalValue)>),
}

/// Why a value could not be canonicalized.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CanonicalError {
    #[error("canonical strings may not contain the control character {codepoint:#06x}")]
    ControlCharacter { codepoint: u32 },
    #[error("canonical strings may not contain a byte-order mark")]
    ByteOrderMark,
    #[error("duplicate map key `{key}`")]
    DuplicateKey { key: String },
    #[error("duplicate member in a set-like collection")]
    DuplicateSetMember,
    #[error(
        "floating-point values have no canonical form and cannot be checksummed; \
         use an integer (micro-dollars for money)"
    )]
    FloatingPoint,
    #[error("JSON null has no canonical form; omit the field instead")]
    Null,
}

impl CanonicalValue {
    /// A record built from unsorted pairs. Sorting happens at encode time, so
    /// callers never have to.
    pub fn map<K: Into<String>>(fields: impl IntoIterator<Item = (K, CanonicalValue)>) -> Self {
        Self::Map(
            fields
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        )
    }

    /// A set-like collection built from members in any order.
    pub fn set(members: impl IntoIterator<Item = CanonicalValue>) -> Self {
        Self::Set(members.into_iter().collect())
    }

    pub fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }

    pub fn integer(value: impl Into<i128>) -> Self {
        Self::Integer(value.into())
    }

    /// Convert parsed JSON — what an admin request arrives as — into a canonical
    /// value, refusing what has no canonical form.
    ///
    /// This is where "no floating point" is enforced against the outside world:
    /// a request body carrying `1.5` is rejected with
    /// [`CanonicalError::FloatingPoint`], not silently truncated, and `null` is
    /// rejected rather than becoming a second spelling of "absent".
    pub fn try_from_json(value: &serde_json::Value) -> Result<Self, CanonicalError> {
        match value {
            serde_json::Value::Null => Err(CanonicalError::Null),
            serde_json::Value::Bool(value) => Ok(Self::Bool(*value)),
            serde_json::Value::Number(number) => {
                if let Some(value) = number.as_i64() {
                    Ok(Self::Integer(i128::from(value)))
                } else if let Some(value) = number.as_u64() {
                    Ok(Self::Integer(i128::from(value)))
                } else {
                    Err(CanonicalError::FloatingPoint)
                }
            }
            serde_json::Value::String(value) => Ok(Self::String(value.clone())),
            serde_json::Value::Array(items) => Ok(Self::List(
                items
                    .iter()
                    .map(Self::try_from_json)
                    .collect::<Result<_, _>>()?,
            )),
            serde_json::Value::Object(fields) => Ok(Self::Map(
                fields
                    .iter()
                    .map(|(key, value)| {
                        Self::try_from_json(value).map(|value| (key.clone(), value))
                    })
                    .collect::<Result<_, _>>()?,
            )),
        }
    }

    /// The canonical bytes under the current serializer.
    pub fn to_canonical_bytes(&self) -> Result<Vec<u8>, CanonicalError> {
        SerializerVersion::default().encode(self)
    }

    /// The SHA-256 checksum of the canonical bytes.
    pub fn checksum(&self) -> Result<Checksum, CanonicalError> {
        Ok(Checksum::of(&self.to_canonical_bytes()?))
    }

    fn tag(&self) -> u8 {
        match self {
            Self::Bool(_) => 0x01,
            Self::Integer(_) => 0x02,
            Self::String(_) => 0x03,
            Self::Bytes(_) => 0x04,
            Self::List(_) => 0x05,
            Self::Set(_) => 0x06,
            Self::Map(_) => 0x07,
        }
    }

    fn write(&self, out: &mut Vec<u8>) -> Result<(), CanonicalError> {
        out.push(self.tag());
        match self {
            Self::Bool(value) => out.push(u8::from(*value)),
            Self::Integer(value) => out.extend_from_slice(&value.to_be_bytes()),
            Self::String(value) => {
                let bytes = check_string(value)?;
                write_len(out, bytes.len());
                out.extend_from_slice(bytes);
            }
            Self::Bytes(value) => {
                write_len(out, value.len());
                out.extend_from_slice(value);
            }
            Self::List(items) => {
                write_len(out, items.len());
                for item in items {
                    item.write(out)?;
                }
            }
            Self::Set(members) => {
                let mut encoded = members
                    .iter()
                    .map(|member| {
                        let mut bytes = Vec::new();
                        member.write(&mut bytes)?;
                        Ok(bytes)
                    })
                    .collect::<Result<Vec<_>, CanonicalError>>()?;
                encoded.sort_unstable();
                if encoded.windows(2).any(|pair| pair[0] == pair[1]) {
                    return Err(CanonicalError::DuplicateSetMember);
                }
                write_len(out, encoded.len());
                for member in encoded {
                    out.extend_from_slice(&member);
                }
            }
            Self::Map(fields) => {
                let mut encoded = fields
                    .iter()
                    .map(|(key, value)| {
                        let mut bytes = Vec::new();
                        Self::String(key.clone()).write(&mut bytes)?;
                        value.write(&mut bytes)?;
                        Ok((key.as_str(), bytes))
                    })
                    .collect::<Result<Vec<_>, CanonicalError>>()?;
                encoded.sort_unstable_by(|left, right| left.0.cmp(right.0));
                if let Some(pair) = encoded.windows(2).find(|pair| pair[0].0 == pair[1].0) {
                    return Err(CanonicalError::DuplicateKey {
                        key: pair[0].0.to_owned(),
                    });
                }
                write_len(out, encoded.len());
                for (_, field) in encoded {
                    out.extend_from_slice(&field);
                }
            }
        }
        Ok(())
    }
}

/// Lengths and counts are fixed-width, so no length is spellable two ways.
fn write_len(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(&(len as u64).to_be_bytes());
}

fn check_string(value: &str) -> Result<&[u8], CanonicalError> {
    for character in value.chars() {
        if character == '\u{feff}' {
            return Err(CanonicalError::ByteOrderMark);
        }
        if character.is_control() {
            return Err(CanonicalError::ControlCharacter {
                codepoint: u32::from(character),
            });
        }
    }
    Ok(value.as_bytes())
}

/// Anything with a canonical form.
///
/// Implemented by the domain types rather than derived from `Serialize`, because
/// the canonical form is a contract: which fields participate in a checksum, and
/// which collections are order-significant, are decisions to make explicitly
/// once per type rather than inherit from a serialization attribute.
pub trait Canonical {
    fn canonical(&self) -> CanonicalValue;

    /// The checksum of this value's canonical bytes.
    fn checksum(&self) -> Result<Checksum, CanonicalError> {
        self.canonical().checksum()
    }
}

/// The algorithm every checksum in the domain uses.
pub const CHECKSUM_ALGORITHM: &str = "sha256";

/// A SHA-256 checksum of canonical bytes, and the address of a content-addressed
/// blob.
///
/// Fixed-width bytes rather than a string, so an unparseable or truncated digest
/// cannot be constructed, and equality is a 32-byte comparison rather than a
/// string comparison that depends on hex case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Checksum([u8; 32]);

/// Why a checksum could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidChecksum {
    #[error("checksum `{0}` is not prefixed `{CHECKSUM_ALGORITHM}:`")]
    Algorithm(String),
    #[error("checksum `{0}` is not 64 lowercase hex digits")]
    Digits(String),
}

impl Checksum {
    /// Hash arbitrary bytes. Used for canonical bytes and for blob payloads.
    pub fn of(bytes: &[u8]) -> Self {
        let mut context = Context::new(&SHA256);
        context.update(bytes);
        let mut digest = [0u8; 32];
        digest.copy_from_slice(context.finish().as_ref());
        Self(digest)
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn parse(text: &str) -> Result<Self, InvalidChecksum> {
        let digits = text
            .strip_prefix(CHECKSUM_ALGORITHM)
            .and_then(|rest| rest.strip_prefix(':'))
            .ok_or_else(|| InvalidChecksum::Algorithm(text.to_owned()))?;
        if digits.len() != 64 {
            return Err(InvalidChecksum::Digits(text.to_owned()));
        }
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&digits[index * 2..index * 2 + 2], 16)
                .map_err(|_| InvalidChecksum::Digits(text.to_owned()))?;
        }
        if digits.chars().any(|c| c.is_ascii_uppercase()) {
            return Err(InvalidChecksum::Digits(text.to_owned()));
        }
        Ok(Self(bytes))
    }
}

impl fmt::Display for Checksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(CHECKSUM_ALGORITHM)?;
        f.write_str(":")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(fields: &[(&str, CanonicalValue)]) -> CanonicalValue {
        CanonicalValue::map(fields.iter().map(|(key, value)| (*key, value.clone())))
    }

    #[test]
    fn field_order_does_not_change_the_bytes() {
        let one = map(&[
            ("alias", CanonicalValue::string("fast")),
            ("enabled", CanonicalValue::Bool(true)),
            ("weight", CanonicalValue::integer(3u32)),
        ]);
        let other = map(&[
            ("weight", CanonicalValue::integer(3u8)),
            ("alias", CanonicalValue::string("fast")),
            ("enabled", CanonicalValue::Bool(true)),
        ]);
        assert_eq!(
            one.to_canonical_bytes().unwrap(),
            other.to_canonical_bytes().unwrap()
        );
        assert_eq!(one.checksum().unwrap(), other.checksum().unwrap());
    }

    #[test]
    fn set_member_order_does_not_change_the_bytes_but_list_order_does() {
        let ascending = CanonicalValue::set([
            CanonicalValue::string("a"),
            CanonicalValue::string("b"),
            CanonicalValue::string("c"),
        ]);
        let descending = CanonicalValue::set([
            CanonicalValue::string("c"),
            CanonicalValue::string("b"),
            CanonicalValue::string("a"),
        ]);
        assert_eq!(
            ascending.to_canonical_bytes().unwrap(),
            descending.to_canonical_bytes().unwrap()
        );

        // Priority is a list: reordering targets *is* a different desired state.
        let first = CanonicalValue::List(vec![
            CanonicalValue::string("primary"),
            CanonicalValue::string("fallback"),
        ]);
        let flipped = CanonicalValue::List(vec![
            CanonicalValue::string("fallback"),
            CanonicalValue::string("primary"),
        ]);
        assert_ne!(
            first.to_canonical_bytes().unwrap(),
            flipped.to_canonical_bytes().unwrap()
        );
    }

    #[test]
    fn integers_are_width_and_sign_normalized() {
        assert_eq!(
            CanonicalValue::integer(1u8).to_canonical_bytes().unwrap(),
            CanonicalValue::integer(1i64).to_canonical_bytes().unwrap()
        );
        assert_ne!(
            CanonicalValue::integer(1i8).to_canonical_bytes().unwrap(),
            CanonicalValue::integer(-1i8).to_canonical_bytes().unwrap()
        );
        // A distinct type, not a spelling of the same value.
        assert_ne!(
            CanonicalValue::integer(1u8).to_canonical_bytes().unwrap(),
            CanonicalValue::string("1").to_canonical_bytes().unwrap()
        );
        assert_ne!(
            CanonicalValue::integer(1u8).to_canonical_bytes().unwrap(),
            CanonicalValue::Bool(true).to_canonical_bytes().unwrap()
        );
    }

    #[test]
    fn length_prefixing_keeps_concatenations_unambiguous() {
        // Delimiter-free framing: no pair of strings can be re-cut into another
        // pair, which is what makes a checksum over a record meaningful.
        let left = CanonicalValue::List(vec![
            CanonicalValue::string("ab"),
            CanonicalValue::string("c"),
        ]);
        let right = CanonicalValue::List(vec![
            CanonicalValue::string("a"),
            CanonicalValue::string("bc"),
        ]);
        assert_ne!(
            left.to_canonical_bytes().unwrap(),
            right.to_canonical_bytes().unwrap()
        );
    }

    #[test]
    fn the_encoding_is_version_tagged() {
        let value = CanonicalValue::Bool(true);
        let bytes = value.to_canonical_bytes().unwrap();
        assert!(bytes.starts_with(SerializerVersion::MAGIC));
        assert_eq!(bytes[SerializerVersion::MAGIC.len()], 1);
        assert_eq!(
            SerializerVersion::default().as_str(),
            "axond.desired-state.v1"
        );
        assert_eq!(
            SerializerVersion::V1.to_string(),
            "axond.desired-state.v1",
            "the version is displayable for diagnostics"
        );
    }

    #[test]
    fn ambiguous_collections_are_refused() {
        let duplicate_key = CanonicalValue::Map(vec![
            ("a".to_owned(), CanonicalValue::Bool(true)),
            ("a".to_owned(), CanonicalValue::Bool(false)),
        ]);
        assert_eq!(
            duplicate_key.to_canonical_bytes(),
            Err(CanonicalError::DuplicateKey {
                key: "a".to_owned()
            })
        );
        let duplicate_member =
            CanonicalValue::set([CanonicalValue::string("a"), CanonicalValue::string("a")]);
        assert_eq!(
            duplicate_member.to_canonical_bytes(),
            Err(CanonicalError::DuplicateSetMember)
        );
        // The same value twice in a *list* is meaningful, not a mistake.
        assert!(
            CanonicalValue::List(vec![
                CanonicalValue::string("a"),
                CanonicalValue::string("a")
            ])
            .to_canonical_bytes()
            .is_ok()
        );
    }

    #[test]
    fn unrepresentable_strings_are_refused() {
        assert_eq!(
            CanonicalValue::string("line\nbreak").to_canonical_bytes(),
            Err(CanonicalError::ControlCharacter { codepoint: 0x0a })
        );
        assert_eq!(
            CanonicalValue::string("\u{feff}prod").to_canonical_bytes(),
            Err(CanonicalError::ByteOrderMark)
        );
        // Non-ASCII prose is fine; it is only identity that is ASCII-only.
        assert!(CanonicalValue::string("Éire").to_canonical_bytes().is_ok());
    }

    #[test]
    fn json_floats_and_nulls_cannot_enter_a_checksum() {
        let body: serde_json::Value = serde_json::json!({
            "input_microdollars_per_million": 2_500_000,
            "enabled": true,
            "targets": ["primary", "fallback"],
        });
        let canonical = CanonicalValue::try_from_json(&body).expect("integers canonicalize");
        assert!(canonical.to_canonical_bytes().is_ok());

        assert_eq!(
            CanonicalValue::try_from_json(&serde_json::json!({ "price": 1.5 })),
            Err(CanonicalError::FloatingPoint)
        );
        assert_eq!(
            CanonicalValue::try_from_json(&serde_json::json!({ "price": null })),
            Err(CanonicalError::Null)
        );
        // Even an integral float is refused: `2.0` reaching a checksum means a
        // float reached the domain.
        assert_eq!(
            CanonicalValue::try_from_json(&serde_json::json!(2.0f64)),
            Err(CanonicalError::FloatingPoint)
        );
    }

    #[test]
    fn json_object_key_order_does_not_change_the_checksum() {
        let one: serde_json::Value =
            serde_json::from_str(r#"{"a":1,"b":{"c":2,"d":[1,2]},"e":"x"}"#).unwrap();
        let other: serde_json::Value =
            serde_json::from_str(r#"{"e":"x","b":{"d":[1,2],"c":2},"a":1}"#).unwrap();
        assert_eq!(
            CanonicalValue::try_from_json(&one).unwrap().checksum(),
            CanonicalValue::try_from_json(&other).unwrap().checksum()
        );
    }

    #[test]
    fn checksums_are_sha256_over_the_canonical_bytes() {
        let value = CanonicalValue::string("prod");
        let bytes = value.to_canonical_bytes().unwrap();
        assert_eq!(value.checksum().unwrap(), Checksum::of(&bytes));
        assert_ne!(value.checksum().unwrap(), Checksum::of(b"prod"));
    }

    #[test]
    fn the_checksum_text_form_round_trips() {
        let checksum = Checksum::of(b"payload");
        let text = checksum.to_string();
        assert!(text.starts_with("sha256:"));
        assert_eq!(text.len(), 7 + 64);
        assert_eq!(Checksum::parse(&text).unwrap(), checksum);

        assert!(matches!(
            Checksum::parse(&text.replace("sha256:", "sha512:")),
            Err(InvalidChecksum::Algorithm(_))
        ));
        assert!(matches!(
            Checksum::parse(&text[..text.len() - 1]),
            Err(InvalidChecksum::Digits(_))
        ));
        assert!(
            matches!(
                Checksum::parse(&format!("sha256:{}", "A".repeat(64))),
                Err(InvalidChecksum::Digits(_))
            ),
            "one text form only, so equality never depends on hex case"
        );
        assert_eq!(
            Checksum::from_bytes(*checksum.as_bytes()),
            checksum,
            "raw digest bytes round-trip for #165's fixed-width column"
        );
    }

    #[test]
    fn the_canonical_trait_hashes_through_the_versioned_serializer() {
        struct Price(u64);
        impl Canonical for Price {
            fn canonical(&self) -> CanonicalValue {
                CanonicalValue::map([("microdollars", CanonicalValue::integer(self.0))])
            }
        }
        let price = Price(2_500_000);
        assert_eq!(
            price.checksum().unwrap(),
            price.canonical().checksum().unwrap()
        );
    }
}
