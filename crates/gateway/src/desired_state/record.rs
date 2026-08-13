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

use super::canonical::CanonicalValue;
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
    fn identity_mismatch(reference: ResourceRef, declared: String, identity: ResourceId) -> Self;
}

/// What a schema whose bodies carry a [`DisplayName`] must additionally say.
///
/// Separate from [`BodyError`] so a schema with no prose field — a model
/// enablement, say — does not carry an error arm nothing can construct.
pub(super) trait DisplayNameError: BodyError {
    fn malformed_display_name(
        reference: ResourceRef,
        field: &'static str,
        source: InvalidDisplayName,
    ) -> Self;
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
        if declared != schema {
            return Err(E::schema(reference, schema, declared.to_owned()));
        }
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
        Ok(record)
    }

    /// The resource this record is the body of, for a refusal a schema builds
    /// itself.
    pub(super) const fn reference(&self) -> ResourceRef {
        self.reference
    }

    /// A field's value as it was encoded, for a schema whose field is a nested
    /// record or a list rather than a scalar.
    pub(super) fn optional_value(&self, field: &'static str) -> Option<&'a CanonicalValue> {
        self.fields
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, value)| value)
    }

    pub(super) fn value(&self, field: &'static str) -> Result<&'a CanonicalValue, E> {
        self.fields
            .iter()
            .find(|(name, _)| name == field)
            .map(|(_, value)| value)
            .ok_or_else(|| E::missing_field(self.reference, field))
    }

    /// A nested record inside `field`, read as strictly as the body around it.
    ///
    /// A schema's sub-records are part of the schema, so a key they do not define
    /// is an unknown field rather than a value to drop: a body a newer release
    /// extended inside `approved_price` or inside a target must be a typed
    /// compatibility refusal, exactly as one extended at the top level is.
    /// The refusal names the path (`approved_price.effective_from`) so an
    /// operator can tell which sub-record the field appeared in.
    pub(super) fn nested(
        &self,
        value: &'a CanonicalValue,
        field: &'static str,
        schema: &'static str,
        known: &[&str],
    ) -> Result<Self, E> {
        let CanonicalValue::Map(fields) = value else {
            return Err(E::field_type(self.reference, field));
        };
        if let Some((key, _)) = fields
            .iter()
            .find(|(key, _)| !known.contains(&key.as_str()))
        {
            return Err(E::unknown_field(
                self.reference,
                schema,
                format!("{field}.{key}"),
            ));
        }
        Ok(Self {
            reference: self.reference,
            fields,
            error: std::marker::PhantomData,
        })
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

impl<'a, E: DisplayNameError> Record<'a, E> {
    pub(super) fn display_name(&self) -> Result<DisplayName, E> {
        DisplayName::parse(self.string(DISPLAY_NAME_FIELD)?)
            .map_err(|source| E::malformed_display_name(self.reference, DISPLAY_NAME_FIELD, source))
    }
}
