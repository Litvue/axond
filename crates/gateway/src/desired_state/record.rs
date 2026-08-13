//! The strict reader every typed body schema is read through.
//!
//! A body is a [`CanonicalValue::Map`] inside a [`ResourceVersion`], and reading
//! one is the same six checks whatever schema it declares: the right resource
//! kind, an inline form, a record shape, the schema identifier this build reads,
//! no field this build does not know, and each field parsed into a domain type
//! rather than carried as text. [`tenancy`](super::tenancy) established those
//! rules for the first two schemas; this module holds them once so a second
//! schema inherits them instead of restating them.
//!
//! The reader is generic over its error type rather than owning one, because the
//! *classification* of a refusal is a per-schema question — each schema's error
//! enum answers it with its own `is_incompatible`. So [`Record`] decides
//! what is wrong and each schema's error enum decides what that means, and no
//! schema can drift into reading a body more loosely than another.

use std::fmt;

use super::canonical::{CanonicalValue, Checksum};
use super::ids::{InvalidId, ProjectId, ResourceId, TenantId};
use super::resource::{ResourceBody, ResourceKind, ResourceRef, ResourceVersion};
use super::tenancy::{DisplayName, InvalidDisplayName};

/// The field every typed body declares its schema in.
pub(super) const SCHEMA_FIELD: &str = "schema";
pub(super) const TENANT_ID_FIELD: &str = "tenant_id";
pub(super) const PROJECT_ID_FIELD: &str = "project_id";
pub(super) const DISPLAY_NAME_FIELD: &str = "display_name";

/// What a schema's error enum must be able to say, so [`Record`] can say it.
///
/// One constructor per refusal the reader can reach. Implementing this is what
/// makes an error enum a *body* error: the arms are not optional, so a schema
/// cannot quietly accept a body shape another schema refuses.
pub(super) trait BodyError: Sized {
    fn kind(reference: ResourceRef, expected: ResourceKind, found: ResourceKind) -> Self;
    fn not_inline(reference: ResourceRef) -> Self;
    fn not_a_record(reference: ResourceRef) -> Self;
    fn schema(reference: ResourceRef, expected: &'static str, found: String) -> Self;
    fn missing_field(reference: ResourceRef, field: &'static str) -> Self;
    fn unknown_field(reference: ResourceRef, schema: &'static str, field: String) -> Self;
    /// A field a schema *reserves*: one this build knows and refuses to read from
    /// a body, rather than one it does not know.
    ///
    /// Defaults to [`unknown_field`](Self::unknown_field), because a schema that
    /// reserves nothing cannot reach it.
    fn reserved_field(reference: ResourceRef, schema: &'static str, field: String) -> Self {
        Self::unknown_field(reference, schema, field)
    }
    fn field_type(reference: ResourceRef, field: &'static str) -> Self;
    fn malformed_id(reference: ResourceRef, field: &'static str, source: InvalidId) -> Self;
    fn malformed_display_name(
        reference: ResourceRef,
        field: &'static str,
        source: InvalidDisplayName,
    ) -> Self;
    fn identity_mismatch(reference: ResourceRef, declared: String, identity: ResourceId) -> Self;

    /// A field that must be a set of strings and is not.
    ///
    /// Defaulted to the wrong-type refusal, because "a set was expected here" is
    /// a wrong type; a schema that has a better word for it says so by overriding
    /// this.
    fn field_set(reference: ResourceRef, field: &'static str) -> Self {
        Self::field_type(reference, field)
    }

    /// A field that must be a digest and is not one.
    fn malformed_checksum(reference: ResourceRef, field: &'static str) -> Self {
        Self::field_type(reference, field)
    }
}

/// One inline record, read strictly.
pub(super) struct Record<'a, E> {
    reference: ResourceRef,
    fields: &'a [(String, CanonicalValue)],
    error: std::marker::PhantomData<E>,
}

impl<'a, E: BodyError> Record<'a, E> {
    /// Open a resource's body as a record of `schema`, refusing a body of the
    /// wrong kind, form, schema, or field set.
    pub(super) fn open(
        resource: &'a ResourceVersion,
        kind: ResourceKind,
        schema: &'static str,
        known: &[&str],
    ) -> Result<Self, E> {
        Self::open_reserving(resource, kind, schema, known, &[])
    }

    /// Open a record whose schema also *reserves* field names: names this build
    /// knows and refuses, which are refused as themselves rather than as fields
    /// this build does not know.
    pub(super) fn open_reserving(
        resource: &'a ResourceVersion,
        kind: ResourceKind,
        schema: &'static str,
        known: &[&str],
        reserved: &[&str],
    ) -> Result<Self, E> {
        Self::open_any_reserving(resource, kind, &[schema], known, reserved)
            .map(|(record, _)| record)
    }

    /// Open a record whose schema may be any of `schemas`, returning the one it
    /// declared.
    ///
    /// More than one identifier is accepted for exactly one reason: a field set
    /// only *some* states of a resource need — a tenant's lifecycle — is a second
    /// schema, and a build that reads both keeps reading the revisions written
    /// before that field existed. `expected` in a refusal is the last of them,
    /// which callers list as the kind's base schema: it is what nearly every row
    /// carries, so it is the useful half of "expected X, found Y".
    pub(super) fn open_any(
        resource: &'a ResourceVersion,
        kind: ResourceKind,
        schemas: &[&'static str],
        known: &[&str],
    ) -> Result<(Self, &'static str), E> {
        Self::open_any_reserving(resource, kind, schemas, known, &[])
    }

    fn open_any_reserving(
        resource: &'a ResourceVersion,
        kind: ResourceKind,
        schemas: &[&'static str],
        known: &[&str],
        reserved: &[&str],
    ) -> Result<(Self, &'static str), E> {
        let reference = resource.reference;
        if reference.kind != kind {
            return Err(E::kind(reference, kind, reference.kind));
        }
        let ResourceBody::Inline(value) = &resource.body else {
            return Err(E::not_inline(reference));
        };
        let CanonicalValue::Map(fields) = value else {
            return Err(E::not_a_record(reference));
        };
        let record = Self {
            reference,
            fields,
            error: std::marker::PhantomData,
        };
        let declared = record.string(SCHEMA_FIELD)?;
        let schema = *schemas
            .iter()
            .find(|candidate| **candidate == declared)
            .ok_or_else(|| {
                E::schema(
                    reference,
                    schemas.last().copied().unwrap_or(""),
                    declared.to_owned(),
                )
            })?;
        // A reserved name is refused before the unknown-field rule, so the
        // boundary it marks is reported as itself rather than as a version skew.
        if let Some((field, _)) = fields
            .iter()
            .find(|(field, _)| reserved.contains(&field.as_str()))
        {
            return Err(E::reserved_field(reference, schema, field.clone()));
        }
        if let Some((field, _)) = fields
            .iter()
            .find(|(field, _)| field != SCHEMA_FIELD && !known.contains(&field.as_str()))
        {
            return Err(E::unknown_field(reference, schema, field.clone()));
        }
        Ok((record, schema))
    }

    pub(super) const fn reference(&self) -> ResourceRef {
        self.reference
    }

    fn value(&self, field: &'static str) -> Result<&'a CanonicalValue, E> {
        self.fields
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, value)| value)
            .ok_or_else(|| E::missing_field(self.reference, field))
    }

    pub(super) fn string(&self, field: &'static str) -> Result<&'a str, E> {
        match self.value(field)? {
            CanonicalValue::String(text) => Ok(text),
            _ => Err(E::field_type(self.reference, field)),
        }
    }

    /// A field a schema defines but a body need not carry.
    ///
    /// Absence and a wrong type stay distinct: an optional field is optional,
    /// never a place a non-string may live.
    pub(super) fn optional_string(&self, field: &'static str) -> Result<Option<&'a str>, E> {
        match self.fields.iter().find(|(name, _)| name == field) {
            None => Ok(None),
            Some((_, CanonicalValue::String(text))) => Ok(Some(text)),
            Some(_) => Err(E::field_type(self.reference, field)),
        }
    }

    /// A set-valued field of strings, refusing a list — order would be meaning —
    /// or a member that is not a string.
    pub(super) fn string_set(&self, field: &'static str) -> Result<Vec<&'a str>, E> {
        let CanonicalValue::Set(members) = self.value(field)? else {
            return Err(E::field_set(self.reference, field));
        };
        members
            .iter()
            .map(|member| match member {
                CanonicalValue::String(text) => Ok(text.as_str()),
                _ => Err(E::field_set(self.reference, field)),
            })
            .collect()
    }

    /// An optional digest field, parsed through [`Checksum`] so a stored value
    /// that is not a digest is a refusal rather than one nothing will ever match.
    pub(super) fn optional_checksum(&self, field: &'static str) -> Result<Option<Checksum>, E> {
        self.optional_string(field)?
            .map(|text| {
                Checksum::parse(text).map_err(|_| E::malformed_checksum(self.reference, field))
            })
            .transpose()
    }

    /// A non-negative integer field, read as `u64`.
    ///
    /// The canonical model normalizes every integer to `i128`, so the range
    /// check belongs here rather than at each call site.
    pub(super) fn integer(&self, field: &'static str) -> Result<u64, E> {
        match self.value(field)? {
            CanonicalValue::Integer(value) => {
                u64::try_from(*value).map_err(|_| E::field_type(self.reference, field))
            }
            _ => Err(E::field_type(self.reference, field)),
        }
    }

    /// An integer field a schema defines but a body need not carry.
    ///
    /// Absence and a wrong type stay distinct, for the reason
    /// [`optional_string`](Self::optional_string) keeps them so.
    pub(super) fn optional_integer(&self, field: &'static str) -> Result<Option<u64>, E> {
        match self.fields.iter().find(|(name, _)| name == field) {
            None => Ok(None),
            Some((_, CanonicalValue::Integer(value))) => u64::try_from(*value)
                .map(Some)
                .map_err(|_| E::field_type(self.reference, field)),
            Some(_) => Err(E::field_type(self.reference, field)),
        }
    }

    pub(super) fn tenant(&self) -> Result<TenantId, E> {
        self.id(TENANT_ID_FIELD, TenantId::parse)
    }

    /// The owning project, for a body that may or may not name one.
    pub(super) fn optional_project(&self) -> Result<Option<ProjectId>, E> {
        match self.optional_string(PROJECT_ID_FIELD)? {
            None => Ok(None),
            Some(text) => ProjectId::parse(text)
                .map(Some)
                .map_err(|source| E::malformed_id(self.reference, PROJECT_ID_FIELD, source)),
        }
    }

    pub(super) fn project(&self) -> Result<ProjectId, E> {
        self.id(PROJECT_ID_FIELD, ProjectId::parse)
    }

    fn id<T>(
        &self,
        field: &'static str,
        parse: impl FnOnce(&str) -> Result<T, InvalidId>,
    ) -> Result<T, E> {
        parse(self.string(field)?).map_err(|source| E::malformed_id(self.reference, field, source))
    }

    /// A typed id in a field of this schema's choosing, for the ids that are not
    /// the tenancy ones.
    pub(super) fn typed_id<T>(
        &self,
        field: &'static str,
        parse: impl FnOnce(&str) -> Result<T, InvalidId>,
    ) -> Result<T, E> {
        self.id(field, parse)
    }

    pub(super) fn display_name(&self) -> Result<DisplayName, E> {
        DisplayName::parse(self.string(DISPLAY_NAME_FIELD)?)
            .map_err(|source| E::malformed_display_name(self.reference, DISPLAY_NAME_FIELD, source))
    }

    /// Bind a body's declared identity to the envelope that carries it.
    pub(super) fn identity(
        &self,
        declared: impl fmt::Display,
        identity: ResourceId,
    ) -> Result<(), E> {
        if self.reference.id == identity {
            Ok(())
        } else {
            Err(E::identity_mismatch(
                self.reference,
                declared.to_string(),
                self.reference.id,
            ))
        }
    }
}
