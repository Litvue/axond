//! Column shapes: the domain's types as text, bytes, and timestamps, and back.
//!
//! This is the only place that knows how a `ResourceScope` becomes three columns
//! or how an `Actor` becomes four. Two rules it follows throughout:
//!
//! - **Reading is parsing, not casting.** Every value comes back through the
//!   domain's own constructor — `ResourceId::parse`, `Checksum::parse`,
//!   `Slug::parse`, `SerializerVersion::decode` — so a row that holds something
//!   the domain would never have written is an [`IntegrityError`] at the
//!   boundary rather than a value that hydrates into a snapshot. The DDL's
//!   `CHECK` constraints make those failures unreachable through this code; they
//!   remain reachable through a restored backup, a manual `UPDATE`, or a dropped
//!   constraint, which is exactly when an operator needs to be told.
//! - **Identity is stored in the domain's text form.** `rev_…`, `mut_…`, `res_…`
//!   are what an operator reads in a support query and what the DDL constrains by
//!   pattern, and they parse back to the *typed* id, so a tenant id in a project
//!   column is a typed error rather than sixteen bytes that fit.

use crate::desired_state::{
    Actor, BlobKind, BlobRef, Canonical, CanonicalError, Checksum, IntegrityError, MutationKind,
    ProjectId, ResourceBody, ResourceKind, ResourceScope, ResourceVersionNumber, SerializerVersion,
    Slug, TenantId,
};

/// The serializer versions a stored row may name.
///
/// A new [`SerializerVersion`] variant is added here so an old row keeps naming
/// the encoding it was written under instead of being read as the current one.
const SERIALIZERS: &[SerializerVersion] = &[SerializerVersion::V1];

pub(super) fn unreadable(detail: impl Into<String>) -> IntegrityError {
    IntegrityError::Unreadable {
        detail: detail.into(),
    }
}

/// Wrap a canonical-encoding failure, which at write time means the caller
/// handed us state that cannot be canonicalized at all.
pub(super) fn encoding(error: CanonicalError) -> IntegrityError {
    unreadable(format!("canonical encoding failed: {error}"))
}

pub(super) fn checksum(text: &str) -> Result<Checksum, IntegrityError> {
    Checksum::parse(text).map_err(|error| unreadable(error.to_string()))
}

pub(super) fn slug(text: &str) -> Result<Slug, IntegrityError> {
    Slug::parse(text).map_err(|error| unreadable(error.to_string()))
}

pub(super) fn version_number(value: i64) -> Result<ResourceVersionNumber, IntegrityError> {
    u64::try_from(value)
        .ok()
        .and_then(ResourceVersionNumber::new)
        .ok_or_else(|| unreadable(format!("resource version {value} is not a version number")))
}

pub(super) fn resource_kind(text: &str) -> Result<ResourceKind, IntegrityError> {
    ResourceKind::ALL
        .iter()
        .copied()
        .find(|kind| kind.as_str() == text)
        .ok_or_else(|| unreadable(format!("`{text}` is not a resource kind")))
}

pub(super) fn blob_kind(text: &str) -> Result<BlobKind, IntegrityError> {
    BlobKind::ALL
        .iter()
        .copied()
        .find(|kind| kind.as_str() == text)
        .ok_or_else(|| unreadable(format!("`{text}` is not a blob kind")))
}

pub(super) fn mutation_kind(text: &str) -> Result<MutationKind, IntegrityError> {
    MutationKind::ALL
        .iter()
        .copied()
        .find(|kind| kind.as_str() == text)
        .ok_or_else(|| unreadable(format!("`{text}` is not a mutation kind")))
}

pub(super) fn serializer(text: &str) -> Result<SerializerVersion, IntegrityError> {
    SERIALIZERS
        .iter()
        .copied()
        .find(|version| version.as_str() == text)
        .ok_or_else(|| {
            // A name from this encoding's family that this build does not know is a
            // version it has not learned — the same verdict a known-but-older
            // version gets — while any other text is a value no release wrote.
            if text.starts_with(SerializerVersion::FAMILY) {
                IntegrityError::UnknownSerializer {
                    stored: text.to_owned(),
                    current: SerializerVersion::default(),
                }
            } else {
                unreadable(format!("`{text}` is not a canonical serializer"))
            }
        })
}

/// Parse a typed id from its prefixed text form.
macro_rules! id_column {
    ($name:ident, $type:ty) => {
        pub(super) fn $name(text: &str) -> Result<$type, IntegrityError> {
            <$type>::parse(text).map_err(|error| unreadable(error.to_string()))
        }
    };
}

id_column!(resource_id, crate::desired_state::ResourceId);
id_column!(revision_id, crate::desired_state::RevisionId);
id_column!(mutation_id, crate::desired_state::MutationId);
id_column!(audit_event_id, crate::desired_state::AuditEventId);

/// A scope as its three columns: the discriminant, and the ownership it implies.
pub(super) struct ScopeColumns {
    pub(super) kind: &'static str,
    pub(super) tenant: Option<String>,
    pub(super) project: Option<String>,
}

pub(super) fn scope_columns(scope: &ResourceScope) -> ScopeColumns {
    match scope {
        ResourceScope::Deployment => ScopeColumns {
            kind: "deployment",
            tenant: None,
            project: None,
        },
        ResourceScope::Tenant(tenant) => ScopeColumns {
            kind: "tenant",
            tenant: Some(tenant.to_string()),
            project: None,
        },
        ResourceScope::Project { tenant, project } => ScopeColumns {
            kind: "project",
            tenant: Some(tenant.to_string()),
            project: Some(project.to_string()),
        },
    }
}

pub(super) fn scope(
    kind: &str,
    tenant: Option<&str>,
    project: Option<&str>,
) -> Result<ResourceScope, IntegrityError> {
    let tenant = tenant
        .map(|text| TenantId::parse(text).map_err(|error| unreadable(error.to_string())))
        .transpose()?;
    let project = project
        .map(|text| ProjectId::parse(text).map_err(|error| unreadable(error.to_string())))
        .transpose()?;
    match (kind, tenant, project) {
        ("deployment", None, None) => Ok(ResourceScope::Deployment),
        ("tenant", Some(tenant), None) => Ok(ResourceScope::Tenant(tenant)),
        ("project", Some(tenant), Some(project)) => Ok(ResourceScope::Project { tenant, project }),
        (kind, tenant, project) => Err(unreadable(format!(
            "`{kind}` scope with tenant {tenant:?} and project {project:?} is not a scope"
        ))),
    }
}

/// An actor as its four columns. Which are populated is decided by the variant,
/// and the DDL refuses any other combination.
pub(super) struct ActorColumns {
    pub(super) kind: &'static str,
    pub(super) issuer: Option<String>,
    pub(super) subject: Option<String>,
    pub(super) component: Option<String>,
}

pub(super) fn actor_columns(actor: &Actor) -> ActorColumns {
    match actor {
        Actor::Human { issuer, subject } => ActorColumns {
            kind: "human",
            issuer: Some(issuer.clone()),
            subject: Some(subject.clone()),
            component: None,
        },
        Actor::Breakglass => ActorColumns {
            kind: "breakglass",
            issuer: None,
            subject: None,
            component: None,
        },
        Actor::System { component } => ActorColumns {
            kind: "system",
            issuer: None,
            subject: None,
            component: Some(component.clone()),
        },
    }
}

pub(super) fn actor(
    kind: &str,
    issuer: Option<&str>,
    subject: Option<&str>,
    component: Option<&str>,
) -> Result<Actor, IntegrityError> {
    match (kind, issuer, subject, component) {
        ("human", Some(issuer), Some(subject), None) => Ok(Actor::Human {
            issuer: issuer.to_owned(),
            subject: subject.to_owned(),
        }),
        ("breakglass", None, None, None) => Ok(Actor::Breakglass),
        ("system", None, None, Some(component)) => Ok(Actor::System {
            component: component.to_owned(),
        }),
        (kind, ..) => Err(unreadable(format!(
            "`{kind}` is not an actor, or is attributed by the wrong columns"
        ))),
    }
}

/// The scope an idempotency key is deduplicated within: a digest of the
/// authenticated caller's identity.
///
/// A digest rather than the attribution columns themselves, for two reasons. It
/// is a *key*, so it must be one fixed-width value rather than a nullable tuple
/// whose uniqueness depends on which columns happen to be populated; and it means
/// the deduplication index carries no issuer, subject, or component text, so the
/// retry window can be read, indexed, and pruned without touching attribution
/// data. Attribution lives on the mutation and the audit event, which is the
/// boundary the domain draws: this column decides *whose retry window*, never who
/// is recorded as having changed anything.
pub(super) fn caller_scope(actor: &Actor) -> Result<String, CanonicalError> {
    Ok(actor.checksum()?.to_string())
}

/// A resource body as its four columns.
pub(super) struct BodyColumns {
    pub(super) form: &'static str,
    pub(super) inline: Option<Vec<u8>>,
    pub(super) blob_kind: Option<String>,
    pub(super) blob_digest: Option<String>,
}

pub(super) fn body_columns(body: &ResourceBody) -> Result<BodyColumns, IntegrityError> {
    match body {
        ResourceBody::Inline(value) => Ok(BodyColumns {
            form: "inline",
            inline: Some(value.to_canonical_bytes().map_err(encoding)?),
            blob_kind: None,
            blob_digest: None,
        }),
        ResourceBody::Blob(reference) => Ok(BodyColumns {
            form: "blob",
            inline: None,
            blob_kind: Some(reference.kind.as_str().to_owned()),
            blob_digest: Some(reference.digest.to_string()),
        }),
    }
}

/// Rebuild a body. A blob body is reassembled from the blob row's own columns, so
/// a size that no longer matches the digest is a mismatch the manifest's checksum
/// catches rather than a value invented here.
pub(super) fn body(
    form: &str,
    inline: Option<&[u8]>,
    kind_column: Option<&str>,
    digest_column: Option<&str>,
    blob_size: Option<i64>,
) -> Result<ResourceBody, IntegrityError> {
    match (form, inline, kind_column, digest_column) {
        ("inline", Some(bytes), None, None) => SerializerVersion::default()
            .decode(bytes)
            .map(ResourceBody::Inline)
            .map_err(|error| unreadable(format!("inline body: {error}"))),
        ("blob", None, Some(kind), Some(digest)) => {
            let size = blob_size.ok_or_else(|| {
                unreadable(format!("blob body {digest} has no blob row to size it"))
            })?;
            Ok(ResourceBody::Blob(BlobRef {
                kind: blob_kind(kind)?,
                digest: checksum(digest)?,
                size_bytes: u64::try_from(size)
                    .map_err(|_| unreadable(format!("blob {digest} has a negative size")))?,
            }))
        }
        (form, ..) => Err(unreadable(format!(
            "`{form}` is not a body form, or is stored in the wrong columns"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::{CanonicalValue, ResourceId, RevisionId, Uuid7};

    fn uuid(seed: u64) -> Uuid7 {
        Uuid7::from_parts(seed, 0, seed).expect("seed in range")
    }

    /// The row says which encoding wrote it, and a name from this family that
    /// this build does not know is a build behind the writer — the verdict a
    /// *known* older version already gets — not a journal to repair. Text from no
    /// family at all is the other thing, and stays so.
    #[test]
    fn an_encoding_this_build_has_not_learned_is_a_skew_and_not_a_repair() {
        assert_eq!(
            serializer(SerializerVersion::default().as_str()).expect("the shipped encoding"),
            SerializerVersion::default()
        );

        let newer = serializer("axond.desired-state.v2").expect_err("a version this build lacks");
        assert!(
            matches!(&newer, IntegrityError::UnknownSerializer { stored, .. } if stored == "axond.desired-state.v2"),
            "{newer:?}"
        );
        assert!(newer.is_incompatible());

        let nonsense = serializer("json").expect_err("not this encoding at all");
        assert!(
            matches!(nonsense, IntegrityError::Unreadable { .. }),
            "{nonsense:?}"
        );
        assert!(!nonsense.is_incompatible());
    }

    /// Every discriminant this module writes has to be a value the shipped DDL
    /// accepts. The `CHECK` lists and these `match` arms are two spellings of one
    /// closed vocabulary, and this is the only thing that keeps them equal.
    #[test]
    fn every_written_discriminant_is_accepted_by_the_shipped_ddl() {
        let ddl = crate::backends::control_plane::schema::MIGRATIONS[0].sql;
        let tenant = TenantId::new(uuid(1));
        let project = ProjectId::new(uuid(2));
        let scopes = [
            ResourceScope::Deployment,
            ResourceScope::Tenant(tenant),
            ResourceScope::Project { tenant, project },
        ];
        for scope in &scopes {
            let columns = scope_columns(scope);
            assert!(
                ddl.contains(&format!("scope_kind = '{}'", columns.kind)),
                "the DDL does not accept scope kind `{}`",
                columns.kind
            );
        }
        for actor in [
            Actor::Human {
                issuer: "https://idp.example".to_owned(),
                subject: "u-1".to_owned(),
            },
            Actor::Breakglass,
            Actor::System {
                component: "catalog-refresh".to_owned(),
            },
        ] {
            let columns = actor_columns(&actor);
            assert!(
                ddl.contains(&format!("actor_kind = '{}'", columns.kind)),
                "the DDL does not accept actor kind `{}`",
                columns.kind
            );
        }
        for body in [
            ResourceBody::Inline(CanonicalValue::Bool(true)),
            ResourceBody::Blob(BlobRef::of(BlobKind::CatalogSnapshot, b"payload")),
        ] {
            let columns = body_columns(&body).expect("encodable body");
            assert!(
                ddl.contains(&format!("body_form = '{}'", columns.form)),
                "the DDL does not accept body form `{}`",
                columns.form
            );
        }
    }

    #[test]
    fn scopes_and_actors_round_trip_through_their_columns() {
        let tenant = TenantId::new(uuid(3));
        let project = ProjectId::new(uuid(4));
        for original in [
            ResourceScope::Deployment,
            ResourceScope::Tenant(tenant),
            ResourceScope::Project { tenant, project },
        ] {
            let columns = scope_columns(&original);
            let restored = scope(
                columns.kind,
                columns.tenant.as_deref(),
                columns.project.as_deref(),
            )
            .expect("a written scope reads back");
            assert_eq!(restored, original);
        }
        for original in [
            Actor::Human {
                issuer: "https://idp.example".to_owned(),
                subject: "u-1".to_owned(),
            },
            Actor::Breakglass,
            Actor::System {
                component: "catalog-refresh".to_owned(),
            },
        ] {
            let columns = actor_columns(&original);
            let restored = actor(
                columns.kind,
                columns.issuer.as_deref(),
                columns.subject.as_deref(),
                columns.component.as_deref(),
            )
            .expect("a written actor reads back");
            assert_eq!(restored, original);
        }
    }

    #[test]
    fn bodies_round_trip_and_a_partial_body_is_unreadable() {
        let inline = ResourceBody::Inline(CanonicalValue::map([
            ("wire_family", CanonicalValue::string("openai-chat")),
            ("weight", CanonicalValue::integer(3u8)),
        ]));
        let columns = body_columns(&inline).expect("encodable");
        let restored =
            body(columns.form, columns.inline.as_deref(), None, None, None).expect("reads back");
        // A stored body reads back in canonical order rather than the order the
        // caller happened to write, which is the same value by the only measure
        // the journal and the manifest use: the checksum.
        assert_eq!(
            restored.checksum().expect("encodable"),
            inline.checksum().expect("encodable")
        );
        assert_eq!(
            restored,
            ResourceBody::Inline(CanonicalValue::map([
                ("weight", CanonicalValue::integer(3u8)),
                ("wire_family", CanonicalValue::string("openai-chat")),
            ]))
        );

        let blob = ResourceBody::Blob(BlobRef::of(BlobKind::PriceBook, b"price book"));
        let columns = body_columns(&blob).expect("encodable");
        assert_eq!(
            body(
                columns.form,
                None,
                columns.blob_kind.as_deref(),
                columns.blob_digest.as_deref(),
                Some(10),
            )
            .expect("reads back"),
            blob
        );

        // A blob body whose blob row is gone: unreadable, not a guessed size.
        assert!(matches!(
            body(
                "blob",
                None,
                Some("price-book"),
                Some(&Checksum::of(b"price book").to_string()),
                None
            ),
            Err(IntegrityError::Unreadable { .. })
        ));
        // An inline body holding something that is not canonical bytes.
        assert!(matches!(
            body("inline", Some(b"not canonical"), None, None, None),
            Err(IntegrityError::Unreadable { .. })
        ));
    }

    #[test]
    fn a_typed_id_column_will_not_read_as_another_type() {
        let resource = ResourceId::new(uuid(5)).to_string();
        assert!(resource_id(&resource).is_ok());
        assert!(
            revision_id(&resource).is_err(),
            "a res_ id must not read as a revision id"
        );
        assert!(revision_id(&RevisionId::new(uuid(5)).to_string()).is_ok());
        assert!(resource_id("res_not-a-uuid").is_err());
    }

    #[test]
    fn a_callers_retry_window_is_scoped_to_the_caller_and_carries_no_attribution() {
        let one = Actor::Human {
            issuer: "https://idp.example".to_owned(),
            subject: "u-1".to_owned(),
        };
        let other = Actor::Human {
            issuer: "https://idp.example".to_owned(),
            subject: "u-2".to_owned(),
        };
        let scope = caller_scope(&one).expect("digest");
        assert_ne!(scope, caller_scope(&other).expect("digest"));
        assert_eq!(scope, caller_scope(&one).expect("digest"));
        assert!(!scope.contains("u-1"));
        assert!(!scope.contains("idp.example"));
        // Same subject at a different issuer is a different caller: a subject is
        // only unique within its issuer.
        assert_ne!(
            scope,
            caller_scope(&Actor::Human {
                issuer: "https://other.example".to_owned(),
                subject: "u-1".to_owned(),
            })
            .expect("digest")
        );
    }

    #[test]
    fn stored_vocabularies_are_parsed_rather_than_cast() {
        for kind in ResourceKind::ALL {
            assert_eq!(&resource_kind(kind.as_str()).expect("known kind"), kind);
        }
        for kind in BlobKind::ALL {
            assert_eq!(&blob_kind(kind.as_str()).expect("known kind"), kind);
        }
        for kind in MutationKind::ALL {
            assert_eq!(&mutation_kind(kind.as_str()).expect("known kind"), kind);
        }
        assert_eq!(
            serializer(SerializerVersion::default().as_str()).expect("known serializer"),
            SerializerVersion::default()
        );
        for unknown in ["", "tenant ", "TENANT", "future-kind"] {
            assert!(resource_kind(unknown).is_err());
        }
        assert!(serializer("axond.desired-state.v2").is_err());
    }
}
