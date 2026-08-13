//! Provider connections: where a tenant's traffic is sent, and in whose dialect.
//!
//! A provider connection is the durable half of what `[[provider]]` states in a
//! stateless config file — an endpoint and the wire family spoken to it — with
//! the half that is material removed. A credential authenticates *to* a provider
//! ([`super::credentials`]) and names it by id; nothing here holds a key, a
//! header, or a token, so a provider body is safe to project, diff, and log in
//! full.
//!
//! Two rules the schema exists to make checkable:
//!
//! - **A connection belongs to a tenant, optionally to one of its projects.** The
//!   body states the owner, and [`ProviderBody::read`] binds that statement to the
//!   scope the envelope filed it under, so a provider cannot claim one owner and
//!   live at another's scope.
//! - **An endpoint is an absolute `https` (or, for a local gateway, `http`) origin.**
//!   A relative or opaque endpoint is refused at publication, because the
//!   alternative is a snapshot that compiles and then cannot reach anything.
//!
//! The wire family is [`WireFamily`], the same vocabulary a model enablement and
//! an alias speak: a connection that speaks a dialect no enablement can name would
//! be unusable, and the shared enum is what keeps the two from drifting.

use std::collections::BTreeMap;

use super::canonical::{Canonical, CanonicalValue};
use super::ids::{InvalidId, ProjectId, ResourceId, Slug, TenantId};
use super::models::WireFamily;
use super::record::{
    BodyError, DISPLAY_NAME_FIELD, DisplayNameError, PROJECT_ID_FIELD, Record, SCHEMA_FIELD,
    TENANT_ID_FIELD,
};
use super::resource::{
    ResourceBody, ResourceKind, ResourceRef, ResourceScope, ResourceVersion, ResourceVersionNumber,
};
use super::revision::DesiredState;
use super::tenancy::{DisplayName, InvalidDisplayName};

/// The provider-connection body schema this build reads and writes.
pub const PROVIDER_SCHEMA: &str = "axond.provider.v1";

const PROVIDER_ID_FIELD: &str = "provider_id";
const WIRE_FAMILY_FIELD: &str = "wire_family";
const ENDPOINT_FIELD: &str = "endpoint";

/// Why a provider-connection body, or the set of them in a revision, was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProviderError {
    #[error("{reference} is a {} resource, not a {}", found.as_str(), expected.as_str())]
    Kind {
        reference: ResourceRef,
        expected: ResourceKind,
        found: ResourceKind,
    },
    #[error("{reference} is a blob body; a provider record is inline")]
    NotInline { reference: ResourceRef },
    #[error("{reference} is not a record")]
    NotARecord { reference: ResourceRef },
    #[error(
        "{reference} declares schema `{found}`, which this build does not read (expected `{expected}`)"
    )]
    Schema {
        reference: ResourceRef,
        expected: &'static str,
        found: String,
    },
    #[error("{reference} has no `{field}`")]
    MissingField {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} carries `{field}`, which `{schema}` does not define")]
    UnknownField {
        reference: ResourceRef,
        schema: &'static str,
        field: String,
    },
    #[error(
        "{reference} field `{field}` is not the type `{}` defines",
        PROVIDER_SCHEMA
    )]
    FieldType {
        reference: ResourceRef,
        field: &'static str,
    },
    #[error("{reference} field `{field}` is not an id: {source}")]
    MalformedId {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidId,
    },
    #[error("{reference} field `{field}` is not a display name: {source}")]
    MalformedDisplayName {
        reference: ResourceRef,
        field: &'static str,
        #[source]
        source: InvalidDisplayName,
    },
    #[error("{reference} carries {declared}, but its resource identity is {identity}")]
    IdentityMismatch {
        reference: ResourceRef,
        declared: String,
        identity: ResourceId,
    },
    #[error("{reference} declares an owner that is not the scope it is filed under")]
    OwnerMismatch { reference: ResourceRef },
    /// A wire family a newer release defined: intact storage this build declines
    /// to interpret, rather than damage.
    #[error("{reference} speaks wire family `{found}`, which this build does not know")]
    UnknownWireFamily {
        reference: ResourceRef,
        found: String,
    },
    /// An endpoint that is not an absolute `http`/`https` origin. Refused at
    /// publication rather than at the first request that cannot be sent.
    #[error("{reference} declares endpoint `{found}`, which is not an absolute http(s) origin")]
    MalformedEndpoint {
        reference: ResourceRef,
        found: String,
    },
}

impl ProviderError {
    /// Whether this refusal means *this build cannot read the body*, rather than
    /// *this body is wrong*. The division [`super::tenancy::TenancyError`] draws.
    pub fn is_incompatible(&self) -> bool {
        match self {
            Self::Schema { .. }
            | Self::UnknownField { .. }
            | Self::UnknownWireFamily { .. }
            | Self::MalformedDisplayName { .. } => true,
            Self::MissingField { field, .. } | Self::FieldType { field, .. } => {
                *field == SCHEMA_FIELD
            }
            Self::Kind { .. }
            | Self::NotInline { .. }
            | Self::NotARecord { .. }
            | Self::MalformedId { .. }
            | Self::IdentityMismatch { .. }
            | Self::OwnerMismatch { .. }
            | Self::MalformedEndpoint { .. } => false,
        }
    }

    /// The resource this refusal is about.
    pub const fn reference(&self) -> ResourceRef {
        match self {
            Self::Kind { reference, .. }
            | Self::NotInline { reference }
            | Self::NotARecord { reference }
            | Self::Schema { reference, .. }
            | Self::MissingField { reference, .. }
            | Self::UnknownField { reference, .. }
            | Self::FieldType { reference, .. }
            | Self::MalformedId { reference, .. }
            | Self::MalformedDisplayName { reference, .. }
            | Self::IdentityMismatch { reference, .. }
            | Self::OwnerMismatch { reference }
            | Self::UnknownWireFamily { reference, .. }
            | Self::MalformedEndpoint { reference, .. } => *reference,
        }
    }
}

impl BodyError for ProviderError {
    fn kind(reference: ResourceRef, expected: ResourceKind, found: ResourceKind) -> Self {
        Self::Kind {
            reference,
            expected,
            found,
        }
    }

    fn not_inline(reference: ResourceRef) -> Self {
        Self::NotInline { reference }
    }

    fn not_a_record(reference: ResourceRef) -> Self {
        Self::NotARecord { reference }
    }

    fn schema(reference: ResourceRef, expected: &'static str, found: String) -> Self {
        Self::Schema {
            reference,
            expected,
            found,
        }
    }

    fn missing_field(reference: ResourceRef, field: &'static str) -> Self {
        Self::MissingField { reference, field }
    }

    fn unknown_field(reference: ResourceRef, schema: &'static str, field: String) -> Self {
        Self::UnknownField {
            reference,
            schema,
            field,
        }
    }

    fn field_type(reference: ResourceRef, field: &'static str) -> Self {
        Self::FieldType { reference, field }
    }

    fn malformed_id(reference: ResourceRef, field: &'static str, source: InvalidId) -> Self {
        Self::MalformedId {
            reference,
            field,
            source,
        }
    }

    fn identity_mismatch(reference: ResourceRef, declared: String, identity: ResourceId) -> Self {
        Self::IdentityMismatch {
            reference,
            declared,
            identity,
        }
    }
}

impl DisplayNameError for ProviderError {
    fn malformed_display_name(
        reference: ResourceRef,
        field: &'static str,
        source: InvalidDisplayName,
    ) -> Self {
        Self::MalformedDisplayName {
            reference,
            field,
            source,
        }
    }
}

/// A tenant's or project's connection to one upstream provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderBody {
    provider: ResourceId,
    tenant: TenantId,
    project: Option<ProjectId>,
    display_name: DisplayName,
    wire_family: WireFamily,
    endpoint: String,
}

impl ProviderBody {
    pub const SCHEMA: &'static str = PROVIDER_SCHEMA;

    const KNOWN_FIELDS: &'static [&'static str] = &[
        PROVIDER_ID_FIELD,
        TENANT_ID_FIELD,
        PROJECT_ID_FIELD,
        DISPLAY_NAME_FIELD,
        WIRE_FAMILY_FIELD,
        ENDPOINT_FIELD,
    ];

    /// A connection owned by a tenant, reachable by every project of it.
    pub fn for_tenant(
        provider: ResourceId,
        tenant: TenantId,
        display_name: DisplayName,
        wire_family: WireFamily,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            provider,
            tenant,
            project: None,
            display_name,
            wire_family,
            endpoint: endpoint.into(),
        }
    }

    /// The same connection, owned by one project of the tenant instead.
    pub fn owned_by_project(self, project: ProjectId) -> Self {
        Self {
            project: Some(project),
            ..self
        }
    }

    pub const fn provider(&self) -> ResourceId {
        self.provider
    }

    pub const fn tenant(&self) -> TenantId {
        self.tenant
    }

    pub const fn project(&self) -> Option<ProjectId> {
        self.project
    }

    pub const fn display_name(&self) -> &DisplayName {
        &self.display_name
    }

    pub const fn wire_family(&self) -> WireFamily {
        self.wire_family
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The same connection pointed at another endpoint.
    pub fn at_endpoint(self, endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            ..self
        }
    }

    pub const fn resource_id(&self) -> ResourceId {
        self.provider
    }

    /// Where this connection's versions live: its owner's scope.
    pub const fn scope(&self) -> ResourceScope {
        match self.project {
            Some(project) => ResourceScope::Project {
                tenant: self.tenant,
                project,
            },
            None => ResourceScope::Tenant(self.tenant),
        }
    }

    pub fn body(&self) -> ResourceBody {
        ResourceBody::Inline(self.canonical())
    }

    pub fn version(&self, slug: Slug) -> ResourceVersion {
        self.version_at(slug, ResourceVersionNumber::FIRST)
    }

    pub fn version_at(&self, slug: Slug, version: ResourceVersionNumber) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Provider, self.resource_id(), version),
            self.scope(),
            slug,
            self.body(),
        )
    }

    /// Read a provider resource's body, binding identity and ownership to the
    /// envelope that carries them.
    pub fn read(resource: &ResourceVersion) -> Result<Self, ProviderError> {
        let record = Record::<ProviderError>::open(
            resource,
            ResourceKind::Provider,
            Self::SCHEMA,
            Self::KNOWN_FIELDS,
        )?;
        let provider = record.typed_id(PROVIDER_ID_FIELD, ResourceId::parse)?;
        record.identity(provider, provider)?;
        let body = Self {
            provider,
            tenant: record.tenant()?,
            project: record.optional_project()?,
            display_name: record.display_name()?,
            wire_family: {
                let declared = record.string(WIRE_FAMILY_FIELD)?;
                WireFamily::parse(declared).ok_or_else(|| ProviderError::UnknownWireFamily {
                    reference: resource.reference,
                    found: declared.to_owned(),
                })?
            },
            endpoint: record.string(ENDPOINT_FIELD)?.to_owned(),
        };
        if resource.scope != body.scope() {
            return Err(ProviderError::OwnerMismatch {
                reference: resource.reference,
            });
        }
        if !is_absolute_origin(&body.endpoint) {
            return Err(ProviderError::MalformedEndpoint {
                reference: resource.reference,
                found: body.endpoint,
            });
        }
        Ok(body)
    }
}

impl Canonical for ProviderBody {
    fn canonical(&self) -> CanonicalValue {
        let mut fields = vec![
            (SCHEMA_FIELD, CanonicalValue::string(Self::SCHEMA)),
            (
                PROVIDER_ID_FIELD,
                CanonicalValue::string(self.provider.to_string()),
            ),
            (
                TENANT_ID_FIELD,
                CanonicalValue::string(self.tenant.to_string()),
            ),
            (
                DISPLAY_NAME_FIELD,
                CanonicalValue::string(self.display_name.as_str()),
            ),
            (
                WIRE_FAMILY_FIELD,
                CanonicalValue::string(self.wire_family.as_str()),
            ),
            (ENDPOINT_FIELD, CanonicalValue::string(&self.endpoint)),
        ];
        if let Some(project) = self.project {
            fields.push((
                PROJECT_ID_FIELD,
                CanonicalValue::string(project.to_string()),
            ));
        }
        CanonicalValue::map(fields)
    }
}

/// Whether an endpoint is an absolute `http`/`https` URL with a host.
///
/// Deliberately not a URL parser: the domain has no dependency on one, and the
/// property that matters — a scheme this build can dial and a non-empty authority
/// — is decidable without one. Anything subtler is the transport's refusal to
/// make, at a point where the operator is watching a request rather than a
/// publication.
fn is_absolute_origin(endpoint: &str) -> bool {
    let Some(authority) = endpoint
        .strip_prefix("https://")
        .or_else(|| endpoint.strip_prefix("http://"))
    else {
        return false;
    };
    let host = authority.split(['/', '?', '#']).next().unwrap_or_default();
    !host.is_empty() && !host.contains(char::is_whitespace)
}

/// A provider connection as a revision holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provider {
    pub reference: ResourceRef,
    pub slug: Slug,
    pub body: ProviderBody,
}

/// The provider connections of one revision, read once.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Providers {
    providers: BTreeMap<ResourceId, Provider>,
}

impl Providers {
    /// Read and check every provider connection a revision declares.
    ///
    /// The single place provider bodies are interpreted, so publication and
    /// hydration cannot reach different conclusions about the same revision.
    pub fn of(state: &DesiredState) -> Result<Self, ProviderError> {
        let mut providers = Self::default();
        for resource in state.resources() {
            if resource.reference.kind != ResourceKind::Provider {
                continue;
            }
            let body = ProviderBody::read(resource)?;
            providers.providers.insert(
                body.provider(),
                Provider {
                    reference: resource.reference,
                    slug: resource.slug.clone(),
                    body,
                },
            );
        }
        Ok(providers)
    }

    /// Every connection, ordered by id.
    pub fn all(&self) -> impl ExactSizeIterator<Item = &Provider> {
        self.providers.values()
    }

    pub fn get(&self, provider: ResourceId) -> Option<&Provider> {
        self.providers.get(&provider)
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{project_id, resource_id, tenant, tenant_id};
    use super::*;

    fn body() -> ProviderBody {
        ProviderBody::for_tenant(
            resource_id(7),
            tenant_id(1),
            DisplayName::parse("OpenAI").expect("a display name"),
            WireFamily::OpenaiChat,
            "https://api.openai.com/v1",
        )
    }

    fn slug() -> Slug {
        Slug::parse("openai").expect("a slug")
    }

    fn state_with(resource: ResourceVersion) -> DesiredState {
        let mut state = DesiredState::new();
        state
            .insert(tenant(1, "acme"))
            .and_then(|state| state.insert(resource))
            .expect("a distinct reference");
        state
    }

    #[test]
    fn a_body_round_trips_through_its_canonical_form() {
        let body = body();
        let read = ProviderBody::read(&body.version(slug())).expect("a readable body");
        assert_eq!(read, body);
        assert_eq!(read.scope(), ResourceScope::Tenant(tenant_id(1)));
    }

    #[test]
    fn a_project_owned_connection_lives_at_its_project() {
        let body = body().owned_by_project(project_id(2));
        let version = body.version(slug());
        assert_eq!(
            version.scope,
            ResourceScope::Project {
                tenant: tenant_id(1),
                project: project_id(2),
            }
        );
        assert_eq!(ProviderBody::read(&version).expect("readable"), body);
    }

    #[test]
    fn an_owner_the_envelope_does_not_agree_with_is_refused() {
        let body = body();
        let mut version = body.version(slug());
        version.scope = ResourceScope::Tenant(tenant_id(9));
        assert!(matches!(
            ProviderBody::read(&version),
            Err(ProviderError::OwnerMismatch { .. })
        ));
    }

    #[test]
    fn a_relative_or_schemeless_endpoint_is_refused_at_publication() {
        for endpoint in ["api.openai.com", "/v1", "ftp://api.openai.com", "https://"] {
            let body = body().at_endpoint(endpoint);
            let error = ProviderBody::read(&body.version(slug()))
                .expect_err("an endpoint that cannot be dialled");
            assert!(
                matches!(error, ProviderError::MalformedEndpoint { .. }),
                "{endpoint}: {error}"
            );
            assert!(!error.is_incompatible(), "{endpoint}");
        }
    }

    #[test]
    fn a_wire_family_this_build_does_not_know_is_a_compatibility_refusal() {
        let body = body();
        let mut version = body.version(slug());
        let ResourceBody::Inline(CanonicalValue::Map(fields)) = &mut version.body else {
            unreachable!("a provider body is an inline record")
        };
        for (name, value) in fields.iter_mut() {
            if name == WIRE_FAMILY_FIELD {
                *value = CanonicalValue::string("gemini.v1");
            }
        }
        let error = ProviderBody::read(&version).expect_err("an unknown dialect");
        assert!(matches!(error, ProviderError::UnknownWireFamily { .. }));
        assert!(error.is_incompatible());
    }

    #[test]
    fn a_revision_reads_its_connections_once() {
        let state = state_with(body().version(slug()));
        let providers = Providers::of(&state).expect("readable connections");
        assert_eq!(providers.len(), 1);
        assert_eq!(
            providers
                .get(resource_id(7))
                .expect("the connection")
                .body
                .endpoint(),
            "https://api.openai.com/v1"
        );
    }

    #[test]
    fn an_unreadable_connection_refuses_the_whole_revision() {
        let state = state_with(body().at_endpoint("api.openai.com").version(slug()));
        assert!(matches!(
            Providers::of(&state),
            Err(ProviderError::MalformedEndpoint { .. })
        ));
    }
}
