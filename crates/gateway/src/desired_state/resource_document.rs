//! Canonical resource documents stored as authenticated immutable objects.
//!
//! The publication layer authenticates an immutable object's content address;
//! this module authenticates its *meaning*. It decodes the existing canonical
//! serializer, accepts only the exact [`ResourceVersion`] envelope, and does
//! not interpret a resource body schema. Secret objects are deliberately not
//! accepted here: their bytes are ciphertext for the later secret path.

use std::collections::BTreeSet;

use super::canonical::{Canonical, CanonicalDecodeError, CanonicalValue, SerializerVersion};
use super::ids::{ProjectId, ResourceId, Slug, TenantId};
use super::publication::{ImmutableObject, ImmutableObjectKind};
use super::resource::{
    BlobKind, BlobRef, ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion,
    ResourceVersionNumber,
};

/// Why an authenticated immutable object is not a canonical resource document.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BlobResourceDocumentError {
    #[error("secret immutable objects contain ciphertext, not resource documents")]
    SecretObject,
    #[error("immutable object kind is not a resource document")]
    UnsupportedObjectKind,
    #[error(transparent)]
    Canonical(#[from] CanonicalDecodeError),
    #[error("resource document has the wrong shape for its {field} field")]
    InvalidField { field: &'static str },
    #[error("resource document has an unknown field")]
    UnknownField,
    #[error("resource document is missing a required field")]
    MissingField,
    #[error("resource document places a resource kind at an invalid scope")]
    ScopeMismatch,
    #[error("resource document kind does not match its immutable object scope")]
    ObjectScopeMismatch,
    #[error("resource document does not re-encode to the authenticated canonical bytes")]
    NonCanonicalEnvelope,
}

/// Decoder for the canonical resource documents in namespace and deployment
/// immutable objects.
pub struct BlobResourceDocument;

impl BlobResourceDocument {
    /// Decode an authenticated immutable resource object into its validated
    /// domain envelope.
    ///
    /// `ImmutableObject` values returned by
    /// [`super::publication::BlobReader::read_immutable_object`] and
    /// [`super::publication::BlobPublication::read_immutable_object`] have
    /// already passed the content-address check. This method intentionally
    /// performs no I/O, projection, serving, or secret decryption.
    pub fn decode(object: &ImmutableObject) -> Result<ResourceVersion, BlobResourceDocumentError> {
        let object_scope = match object.kind {
            ImmutableObjectKind::NamespaceResource => false,
            ImmutableObjectKind::DeploymentResource => true,
            ImmutableObjectKind::Secret => return Err(BlobResourceDocumentError::SecretObject),
        };

        let value = SerializerVersion::default().decode(&object.bytes)?;
        let resource = parse_resource_version(&value)?;
        if !resource.reference.kind.permits(&resource.scope) {
            return Err(BlobResourceDocumentError::ScopeMismatch);
        }
        if (resource.scope == ResourceScope::Deployment) != object_scope {
            return Err(BlobResourceDocumentError::ObjectScopeMismatch);
        }

        let round_trip = SerializerVersion::default().encode(&resource.canonical());
        if round_trip.as_deref() != Ok(object.bytes.as_ref()) {
            return Err(BlobResourceDocumentError::NonCanonicalEnvelope);
        }
        Ok(resource)
    }
}

fn parse_resource_version(
    value: &CanonicalValue,
) -> Result<ResourceVersion, BlobResourceDocumentError> {
    let fields = exact_map(value, RESOURCE_FIELDS)?;
    let reference = parse_resource_ref(required(fields, "reference")?)?;
    let scope = parse_scope(required(fields, "scope")?)?;
    let slug = parse_slug(required(fields, "slug")?)?;
    let body = parse_body(required(fields, "body")?)?;
    let depends_on = parse_dependencies(required(fields, "depends_on")?)?;

    Ok(ResourceVersion {
        reference,
        scope,
        slug,
        body,
        depends_on,
    })
}

const RESOURCE_FIELDS: &[&str] = &["reference", "scope", "slug", "body", "depends_on"];
const REFERENCE_FIELDS: &[&str] = &["kind", "id", "version"];
const BLOB_FIELDS: &[&str] = &["kind", "digest", "size_bytes"];

fn exact_map<'a>(
    value: &'a CanonicalValue,
    expected: &[&str],
) -> Result<&'a [(String, CanonicalValue)], BlobResourceDocumentError> {
    let fields = map_fields(value)?;
    if fields
        .iter()
        .any(|(key, _)| !expected.contains(&key.as_str()))
        || expected
            .iter()
            .any(|key| fields.iter().filter(|(found, _)| found == key).count() != 1)
    {
        return Err(
            if fields
                .iter()
                .any(|(key, _)| !expected.contains(&key.as_str()))
            {
                BlobResourceDocumentError::UnknownField
            } else {
                BlobResourceDocumentError::MissingField
            },
        );
    }
    Ok(fields)
}

fn map_fields(
    value: &CanonicalValue,
) -> Result<&[(String, CanonicalValue)], BlobResourceDocumentError> {
    let CanonicalValue::Map(fields) = value else {
        return Err(BlobResourceDocumentError::InvalidField { field: "map" });
    };
    Ok(fields)
}

fn required<'a>(
    fields: &'a [(String, CanonicalValue)],
    key: &str,
) -> Result<&'a CanonicalValue, BlobResourceDocumentError> {
    fields
        .iter()
        .find_map(|(field, value)| (field == key).then_some(value))
        .ok_or(BlobResourceDocumentError::MissingField)
}

fn text<'a>(
    value: &'a CanonicalValue,
    field: &'static str,
) -> Result<&'a str, BlobResourceDocumentError> {
    match value {
        CanonicalValue::String(value) => Ok(value),
        _ => Err(BlobResourceDocumentError::InvalidField { field }),
    }
}

fn integer(value: &CanonicalValue, field: &'static str) -> Result<u64, BlobResourceDocumentError> {
    match value {
        CanonicalValue::Integer(value) => {
            u64::try_from(*value).map_err(|_| BlobResourceDocumentError::InvalidField { field })
        }
        _ => Err(BlobResourceDocumentError::InvalidField { field }),
    }
}

fn parse_resource_kind(value: &CanonicalValue) -> Result<ResourceKind, BlobResourceDocumentError> {
    let value = text(value, "kind")?;
    ResourceKind::ALL
        .iter()
        .copied()
        .find(|kind| kind.as_str() == value)
        .ok_or(BlobResourceDocumentError::InvalidField { field: "kind" })
}

fn parse_resource_ref(value: &CanonicalValue) -> Result<ResourceRef, BlobResourceDocumentError> {
    let fields = exact_map(value, REFERENCE_FIELDS)?;
    let kind = parse_resource_kind(required(fields, "kind")?)?;
    let id = ResourceId::parse(text(required(fields, "id")?, "id")?)
        .map_err(|_| BlobResourceDocumentError::InvalidField { field: "id" })?;
    let version = ResourceVersionNumber::new(integer(required(fields, "version")?, "version")?)
        .ok_or(BlobResourceDocumentError::InvalidField { field: "version" })?;
    Ok(ResourceRef::new(kind, id, version))
}

fn parse_scope(value: &CanonicalValue) -> Result<ResourceScope, BlobResourceDocumentError> {
    let fields = map_fields(value)?;
    let kind = text(required(fields, "kind")?, "kind")?;
    match kind {
        "deployment" => {
            exact_map(value, &["kind"])?;
            Ok(ResourceScope::Deployment)
        }
        "tenant" => {
            exact_map(value, &["kind", "tenant"])?;
            let tenant = TenantId::parse(text(required(fields, "tenant")?, "tenant")?)
                .map_err(|_| BlobResourceDocumentError::InvalidField { field: "tenant" })?;
            Ok(ResourceScope::Tenant(tenant))
        }
        "project" => {
            exact_map(value, &["kind", "tenant", "project"])?;
            let tenant = TenantId::parse(text(required(fields, "tenant")?, "tenant")?)
                .map_err(|_| BlobResourceDocumentError::InvalidField { field: "tenant" })?;
            let project = ProjectId::parse(text(required(fields, "project")?, "project")?)
                .map_err(|_| BlobResourceDocumentError::InvalidField { field: "project" })?;
            Ok(ResourceScope::Project { tenant, project })
        }
        _ => Err(BlobResourceDocumentError::InvalidField { field: "kind" }),
    }
}

fn parse_slug(value: &CanonicalValue) -> Result<Slug, BlobResourceDocumentError> {
    Slug::parse(text(value, "slug")?)
        .map_err(|_| BlobResourceDocumentError::InvalidField { field: "slug" })
}

fn parse_body(value: &CanonicalValue) -> Result<ResourceBody, BlobResourceDocumentError> {
    let fields = map_fields(value)?;
    match text(required(fields, "form")?, "form")? {
        "inline" => {
            exact_map(value, &["form", "value"])?;
            Ok(ResourceBody::Inline(required(fields, "value")?.clone()))
        }
        "blob" => {
            exact_map(value, &["form", "blob"])?;
            Ok(ResourceBody::Blob(parse_blob_ref(required(
                fields, "blob",
            )?)?))
        }
        _ => Err(BlobResourceDocumentError::InvalidField { field: "form" }),
    }
}

fn parse_blob_ref(value: &CanonicalValue) -> Result<BlobRef, BlobResourceDocumentError> {
    let fields = exact_map(value, BLOB_FIELDS)?;
    let kind = match text(required(fields, "kind")?, "kind")? {
        "catalog-snapshot" => BlobKind::CatalogSnapshot,
        "price-book" => BlobKind::PriceBook,
        "policy-bundle" => BlobKind::PolicyBundle,
        _ => return Err(BlobResourceDocumentError::InvalidField { field: "kind" }),
    };
    let digest = match required(fields, "digest")? {
        CanonicalValue::Bytes(bytes) if bytes.len() == 32 => {
            let mut digest = [0; 32];
            digest.copy_from_slice(bytes);
            super::canonical::Checksum::from_bytes(digest)
        }
        _ => return Err(BlobResourceDocumentError::InvalidField { field: "digest" }),
    };
    let size_bytes = integer(required(fields, "size_bytes")?, "size_bytes")?;
    Ok(BlobRef {
        kind,
        digest,
        size_bytes,
    })
}

fn parse_dependencies(
    value: &CanonicalValue,
) -> Result<BTreeSet<ResourceRef>, BlobResourceDocumentError> {
    let CanonicalValue::Set(values) = value else {
        return Err(BlobResourceDocumentError::InvalidField {
            field: "depends_on",
        });
    };
    values.iter().map(parse_resource_ref).collect()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::super::canonical::{Canonical, Checksum};
    use super::super::ids::{ResourceId, Uuid7};
    use super::super::publication::{ImmutableObject, ImmutableObjectKind};
    use super::super::resource::{
        BlobKind, BlobRef, ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion,
        ResourceVersionNumber,
    };
    use super::{BlobResourceDocument, BlobResourceDocumentError};

    fn resource_id(seed: u64) -> ResourceId {
        ResourceId::new(Uuid7::from_parts(seed, 0, seed).expect("fixture UUID"))
    }

    fn resource(body: ResourceBody) -> ResourceVersion {
        let reference = ResourceRef::new(
            ResourceKind::Alias,
            resource_id(7),
            ResourceVersionNumber::FIRST,
        );
        ResourceVersion::new(
            reference,
            ResourceScope::Tenant(super::super::ids::TenantId::new(
                Uuid7::from_parts(8, 0, 8).expect("fixture tenant UUID"),
            )),
            super::super::ids::Slug::parse("gateway").expect("fixture slug"),
            body,
        )
        .depending_on([ResourceRef::new(
            ResourceKind::Provider,
            resource_id(9),
            ResourceVersionNumber::FIRST,
        )])
    }

    fn object(kind: ImmutableObjectKind, resource: &ResourceVersion) -> ImmutableObject {
        let bytes = resource
            .canonical()
            .to_canonical_bytes()
            .expect("canonical resource bytes");
        ImmutableObject {
            kind,
            bytes: Bytes::from(bytes),
        }
    }

    #[test]
    fn inline_and_blob_documents_round_trip_exactly() {
        let blob = BlobRef::of(BlobKind::CatalogSnapshot, b"catalog");
        for resource in [
            resource(ResourceBody::Inline(
                super::super::canonical::CanonicalValue::map([(
                    "model",
                    super::super::canonical::CanonicalValue::string("gpt"),
                )]),
            )),
            resource(ResourceBody::Blob(blob)),
        ] {
            let object = object(ImmutableObjectKind::NamespaceResource, &resource);
            let expected_checksum = Checksum::of(&object.bytes);
            let decoded = BlobResourceDocument::decode(&object).expect("resource document");
            let encoded = decoded
                .canonical()
                .to_canonical_bytes()
                .expect("re-encode resource document");
            assert_eq!(encoded.as_slice(), object.bytes.as_ref());
            assert_eq!(Checksum::of(&encoded), expected_checksum);
            assert_eq!(decoded, resource);
        }
    }

    #[test]
    fn malformed_trailing_and_unknown_fields_are_refused() {
        let resource = resource(ResourceBody::Inline(
            super::super::canonical::CanonicalValue::Bool(true),
        ));
        let object = object(ImmutableObjectKind::NamespaceResource, &resource);

        let mut trailing = object.bytes.to_vec();
        trailing.push(0);
        assert!(matches!(
            BlobResourceDocument::decode(&ImmutableObject {
                kind: object.kind,
                bytes: Bytes::from(trailing),
            }),
            Err(BlobResourceDocumentError::Canonical(_))
        ));

        let unknown = super::super::canonical::CanonicalValue::map([
            ("reference", resource.reference.canonical()),
            ("scope", resource.scope.canonical()),
            (
                "slug",
                super::super::canonical::CanonicalValue::string("gateway"),
            ),
            ("body", resource.body.canonical()),
            (
                "depends_on",
                super::super::canonical::CanonicalValue::set(
                    resource
                        .depends_on
                        .iter()
                        .map(super::super::canonical::Canonical::canonical),
                ),
            ),
            (
                "future",
                super::super::canonical::CanonicalValue::Bool(true),
            ),
        ])
        .to_canonical_bytes()
        .expect("unknown-field bytes");
        assert!(matches!(
            BlobResourceDocument::decode(&ImmutableObject {
                kind: object.kind,
                bytes: Bytes::from(unknown),
            }),
            Err(BlobResourceDocumentError::UnknownField)
        ));

        assert!(matches!(
            BlobResourceDocument::decode(&ImmutableObject {
                kind: object.kind,
                bytes: Bytes::from_static(b"not canonical"),
            }),
            Err(BlobResourceDocumentError::Canonical(_))
        ));
    }

    #[test]
    fn secret_immutable_objects_are_not_resource_documents() {
        let resource = resource(ResourceBody::Inline(
            super::super::canonical::CanonicalValue::Bool(true),
        ));
        let object = object(ImmutableObjectKind::Secret, &resource);
        assert_eq!(
            BlobResourceDocument::decode(&object),
            Err(BlobResourceDocumentError::SecretObject)
        );
    }
}
