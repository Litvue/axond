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

    /// A body whose `schema` is present and is not text, so the identifier that
    /// decides how to read everything else is itself unreadable.
    ///
    /// Defaulted to the wrong-type refusal, because that is what it is; a schema
    /// whose classification of it differs from an ordinary wrong type — a model
    /// body, where a marker no release ever wrote is damage rather than skew —
    /// says so by overriding this.
    fn damaged_schema(reference: ResourceRef) -> Self {
        Self::field_type(reference, SCHEMA_FIELD)
    }

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

/// What a schema whose body *names* things must additionally be able to say.
///
/// Split from [`BodyError`] rather than folded into it, because the id-bearing
/// refusals are only reachable for a schema that reads a typed id or binds its
/// body's declared identity to the envelope. A schema that reads neither — a
/// deployment-wide body naming no tenant — would have to carry two arms it can
/// never reach to get the reader's common checks, and an unreachable arm is a
/// worse kind of drift than a second trait.
pub(super) trait IdentifiedBody: BodyError {
    fn malformed_id(reference: ResourceRef, field: &'static str, source: InvalidId) -> Self;
    fn identity_mismatch(reference: ResourceRef, declared: String, identity: ResourceId) -> Self;
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
        // Read directly rather than through `string`, so a marker that is
        // present and not text is its own refusal: absence is a body written
        // before this schema existed, while a non-text marker is a body no
        // release wrote.
        let CanonicalValue::String(declared) = record.value(SCHEMA_FIELD)? else {
            return Err(E::damaged_schema(reference));
        };
        let declared = declared.as_str();
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

    /// A record nested inside this body: a map that carries no schema of its own,
    /// because the body it is part of declared one.
    ///
    /// A `schema` key inside a nested record is therefore a field this build does
    /// not know, and is refused as one rather than skipped — dropping it would
    /// publish a fingerprint over bytes the reader re-wrote.
    pub(super) fn nested(
        reference: ResourceRef,
        schema: &'static str,
        field: &'static str,
        value: &'a CanonicalValue,
        known: &[&str],
    ) -> Result<Self, E> {
        let CanonicalValue::Map(fields) = value else {
            return Err(E::field_type(reference, field));
        };
        if let Some((field, _)) = fields
            .iter()
            .find(|(field, _)| !known.contains(&field.as_str()))
        {
            return Err(E::unknown_field(reference, schema, field.clone()));
        }
        Ok(Self {
            reference,
            fields,
            error: std::marker::PhantomData,
        })
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
        self.optional_value(field)
            .ok_or_else(|| E::missing_field(self.reference, field))
    }

    /// A nested record field, read as strictly as the body containing it.
    pub(super) fn record(
        &self,
        schema: &'static str,
        field: &'static str,
        known: &[&str],
    ) -> Result<Self, E> {
        Self::nested(self.reference, schema, field, self.value(field)?, known)
    }

    /// A set-like field's members.
    ///
    /// A set and not a list, because a fingerprint taken over the *stored* body
    /// only sorts and deduplicates members in the set encoding: a list-encoded
    /// field would give one body as many checksums as its members have orderings,
    /// and let one member be stated twice.
    pub(super) fn set(&self, field: &'static str) -> Result<&'a [CanonicalValue], E> {
        match self.value(field)? {
            CanonicalValue::Set(members) => Ok(members),
            _ => Err(E::field_type(self.reference, field)),
        }
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

    /// An integer field read as the canonical model stores it, for a schema whose
    /// own refusals distinguish *why* a value is out of range — a negative rate
    /// and an unrepresentable one are different things to tell an operator.
    pub(super) fn signed_integer(&self, field: &'static str) -> Result<i128, E> {
        match self.value(field)? {
            CanonicalValue::Integer(value) => Ok(*value),
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
}

impl<'a, E: IdentifiedBody> Record<'a, E> {
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

    /// An operator-facing name a body need not carry, in a field of the schema's
    /// choosing.
    pub(super) fn optional_display_name(
        &self,
        field: &'static str,
    ) -> Result<Option<DisplayName>, E> {
        self.optional_string(field)?
            .map(|text| {
                DisplayName::parse(text)
                    .map_err(|source| E::malformed_display_name(self.reference, field, source))
            })
            .transpose()
    }
}
