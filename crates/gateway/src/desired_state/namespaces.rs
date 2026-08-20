//! Flat namespace desired state and deployment-scoped inbound grants (ADR 0062).
//!
//! These two resource bodies are deliberately self-contained. A namespace does
//! not inherit from a tenant, project, or principal, and a grant authenticates
//! a caller without becoming a business resource.

use std::collections::{BTreeMap, BTreeSet};

use gateway_core::{GuardrailRule, MiddlewareFailurePosture, MiddlewareScope, ModelPrice};
use serde::{Deserialize, Serialize};

use crate::config::ProviderKind;
use crate::namespace::{NamespaceGrant, NamespaceId};

use super::policy::{
    BufferedResponseRoute, ContentGuardrailRegistration, ContentMiddlewareRegistration,
};
use super::{
    Canonical, CanonicalValue, Checksum, ResourceBody, ResourceId, ResourceKind, ResourceRef,
    ResourceScope, ResourceVersion, ResourceVersionNumber, SecretRef, Slug,
};

const NAMESPACE_SCHEMA: &str = "axond.namespace.v2";
const GRANT_SCHEMA: &str = "axond.inbound-grant.v2";
const MAX_PROVIDERS: usize = 64;
const MAX_CREDENTIALS: usize = 128;
const MAX_ALIASES: usize = 256;
const MAX_TARGETS: usize = 16;
const MAX_MIDDLEWARE: usize = 32;
const MAX_SUBJECT_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceProvider {
    pub id: Slug,
    pub kind: FlatProviderKind,
    pub base_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FlatProviderKind {
    Openai,
    Anthropic,
    OpenaiCompatible,
}

impl From<FlatProviderKind> for ProviderKind {
    fn from(value: FlatProviderKind) -> Self {
        match value {
            FlatProviderKind::Openai => Self::Openai,
            FlatProviderKind::Anthropic => Self::Anthropic,
            FlatProviderKind::OpenaiCompatible => Self::OpenaiCompatible,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceCredential {
    pub id: Slug,
    pub provider: Slug,
    pub secret: SecretRef,
    pub weight: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceTarget {
    pub provider: Slug,
    pub model: String,
    pub price: ModelPrice,
    pub catalog: Option<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceAlias {
    pub name: Slug,
    pub targets: Vec<NamespaceTarget>,
}

/// Namespace-owned limits and selections that are carried as one document.
///
/// The first v2 compiler projects `token_epoch` immediately. The remaining
/// values are retained in the complete resource so the policy/middleware
/// adapter can consume them without adding another ownership resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespacePolicySpec {
    pub subject_limit_microdollars: u64,
    pub namespace_limit_microdollars: Option<u64>,
    pub reservation_ttl_seconds: u64,
    pub max_in_flight_per_subject: u64,
    pub lease_ttl_seconds: u64,
    pub middleware: Vec<ContentMiddlewareRegistration>,
    pub buffered_response_routes: Vec<BufferedResponseRoute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceBody {
    namespace: NamespaceId,
    default: bool,
    allow_platform_fallback: bool,
    providers: Vec<NamespaceProvider>,
    credentials: Vec<NamespaceCredential>,
    aliases: Vec<NamespaceAlias>,
    policy: NamespacePolicySpec,
    token_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundGrantBody {
    digest: Checksum,
    grant: NamespaceGrant,
    subject: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NamespaceStateError {
    #[error("{reference} is not a flat namespace or inbound-grant resource")]
    Kind { reference: ResourceRef },
    #[error("{reference} must be deployment-scoped")]
    Scope { reference: ResourceRef },
    #[error("{reference} does not contain a supported inline v2 body: {detail}")]
    Body {
        reference: ResourceRef,
        detail: String,
    },
    #[error("namespace `{namespace}` is declared more than once")]
    DuplicateNamespace { namespace: NamespaceId },
    #[error("exactly one flat namespace must be the default (found {count})")]
    DefaultCount { count: usize },
    #[error("grant {grant} names unknown namespace `{namespace}`")]
    UnknownGrantNamespace {
        grant: ResourceRef,
        namespace: NamespaceId,
    },
    #[error("flat namespace state has no inbound grants")]
    NoInboundGrants,
}

#[derive(Debug, Clone, Default)]
pub struct FlatNamespaces {
    namespaces: BTreeMap<NamespaceId, (ResourceRef, NamespaceBody)>,
    grants: Vec<(ResourceRef, InboundGrantBody)>,
}

impl NamespaceBody {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        namespace: NamespaceId,
        default: bool,
        allow_platform_fallback: bool,
        providers: Vec<NamespaceProvider>,
        credentials: Vec<NamespaceCredential>,
        aliases: Vec<NamespaceAlias>,
        policy: NamespacePolicySpec,
        token_epoch: u64,
    ) -> Result<Self, String> {
        let mut body = Self {
            namespace,
            default,
            allow_platform_fallback,
            providers,
            credentials,
            aliases,
            policy,
            token_epoch,
        };
        body.providers.sort_by(|left, right| left.id.cmp(&right.id));
        body.credentials
            .sort_by(|left, right| left.id.cmp(&right.id));
        body.aliases
            .sort_by(|left, right| left.name.cmp(&right.name));
        body.policy.buffered_response_routes.sort_unstable();
        if body
            .policy
            .buffered_response_routes
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err("duplicate buffered response route selection".into());
        }
        body.validate()?;
        Ok(body)
    }

    pub fn namespace(&self) -> &NamespaceId {
        &self.namespace
    }
    pub const fn is_default(&self) -> bool {
        self.default
    }
    pub const fn allow_platform_fallback(&self) -> bool {
        self.allow_platform_fallback
    }
    pub fn providers(&self) -> &[NamespaceProvider] {
        &self.providers
    }
    pub fn credentials(&self) -> &[NamespaceCredential] {
        &self.credentials
    }
    pub fn aliases(&self) -> &[NamespaceAlias] {
        &self.aliases
    }
    pub const fn policy(&self) -> &NamespacePolicySpec {
        &self.policy
    }
    pub const fn token_epoch(&self) -> u64 {
        self.token_epoch
    }

    pub fn version(&self, id: ResourceId, slug: Slug) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Namespace, id, ResourceVersionNumber::FIRST),
            ResourceScope::Deployment,
            slug,
            ResourceBody::Inline(self.canonical()),
        )
    }

    pub fn read(resource: &ResourceVersion) -> Result<Self, NamespaceStateError> {
        read_inline(resource, ResourceKind::Namespace, NAMESPACE_SCHEMA)
            .and_then(|value| StoredNamespace::from_value(value, resource.reference))
            .and_then(|stored| stored.into_body(resource.reference))
    }

    fn validate(&self) -> Result<(), String> {
        bound("providers", self.providers.len(), MAX_PROVIDERS)?;
        bound("credentials", self.credentials.len(), MAX_CREDENTIALS)?;
        bound("aliases", self.aliases.len(), MAX_ALIASES)?;
        bound(
            "middleware selections",
            self.policy.middleware.len(),
            MAX_MIDDLEWARE,
        )?;
        if self.policy.reservation_ttl_seconds == 0
            || self.policy.max_in_flight_per_subject == 0
            || self.policy.lease_ttl_seconds == 0
        {
            return Err("policy duration and concurrency bounds must be non-zero".into());
        }
        let providers = unique_slugs("provider", self.providers.iter().map(|p| &p.id))?;
        for provider in &self.providers {
            if !absolute_origin(&provider.base_url) {
                return Err(format!(
                    "provider `{}` does not declare an absolute HTTP(S) origin",
                    provider.id
                ));
            }
        }
        unique_slugs("credential", self.credentials.iter().map(|c| &c.id))?;
        for credential in &self.credentials {
            if credential.weight == 0 {
                return Err(format!("credential `{}` has zero weight", credential.id));
            }
            if !providers.contains(credential.provider.as_str()) {
                return Err(format!(
                    "credential `{}` references unknown provider `{}`",
                    credential.id, credential.provider
                ));
            }
        }
        unique_slugs("alias", self.aliases.iter().map(|a| &a.name))?;
        for alias in &self.aliases {
            if alias.targets.is_empty() || alias.targets.len() > MAX_TARGETS {
                return Err(format!(
                    "alias `{}` must have between 1 and {MAX_TARGETS} targets",
                    alias.name
                ));
            }
            for target in &alias.targets {
                if !providers.contains(target.provider.as_str()) {
                    return Err(format!(
                        "alias `{}` references unknown provider `{}`",
                        alias.name, target.provider
                    ));
                }
                if target.model.is_empty() || target.model.len() > 256 {
                    return Err(format!("alias `{}` has an invalid model id", alias.name));
                }
            }
        }
        let mut middleware = BTreeSet::new();
        for registration in &self.policy.middleware {
            if !middleware.insert(registration.id()) {
                return Err(format!(
                    "duplicate middleware selection `{}`",
                    registration.id()
                ));
            }
        }
        Ok(())
    }
}

impl InboundGrantBody {
    pub fn new(
        digest: Checksum,
        grant: NamespaceGrant,
        subject: Option<String>,
    ) -> Result<Self, String> {
        if subject
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > MAX_SUBJECT_BYTES)
        {
            return Err(format!(
                "grant subject must contain between 1 and {MAX_SUBJECT_BYTES} bytes"
            ));
        }
        Ok(Self {
            digest,
            grant,
            subject,
        })
    }

    pub const fn digest(&self) -> Checksum {
        self.digest
    }
    pub const fn grant(&self) -> &NamespaceGrant {
        &self.grant
    }
    pub fn subject(&self) -> Option<&str> {
        self.subject.as_deref()
    }

    pub fn version(&self, id: ResourceId, slug: Slug) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::InboundGrant, id, ResourceVersionNumber::FIRST),
            ResourceScope::Deployment,
            slug,
            ResourceBody::Inline(self.canonical()),
        )
    }

    pub fn read(resource: &ResourceVersion) -> Result<Self, NamespaceStateError> {
        read_inline(resource, ResourceKind::InboundGrant, GRANT_SCHEMA)
            .and_then(|value| StoredGrant::from_value(value, resource.reference))
            .and_then(|stored| stored.into_body(resource.reference))
    }
}

impl FlatNamespaces {
    pub fn of(state: &super::DesiredState) -> Result<Self, NamespaceStateError> {
        let mut result = Self::default();
        for resource in state.resources() {
            match resource.reference.kind {
                ResourceKind::Namespace => {
                    let body = NamespaceBody::read(resource)?;
                    let id = body.namespace.clone();
                    if result
                        .namespaces
                        .insert(id.clone(), (resource.reference, body))
                        .is_some()
                    {
                        return Err(NamespaceStateError::DuplicateNamespace { namespace: id });
                    }
                }
                ResourceKind::InboundGrant => result
                    .grants
                    .push((resource.reference, InboundGrantBody::read(resource)?)),
                _ => {}
            }
        }
        let defaults = result
            .namespaces
            .values()
            .filter(|(_, body)| body.is_default())
            .count();
        if defaults != 1 {
            return Err(NamespaceStateError::DefaultCount { count: defaults });
        }
        if result.grants.is_empty() {
            return Err(NamespaceStateError::NoInboundGrants);
        }
        for (reference, body) in &result.grants {
            if let Some(namespaces) = body.grant().namespaces() {
                for namespace in namespaces {
                    if !result.namespaces.contains_key(namespace) {
                        return Err(NamespaceStateError::UnknownGrantNamespace {
                            grant: *reference,
                            namespace: namespace.clone(),
                        });
                    }
                }
            }
        }
        Ok(result)
    }

    pub fn namespaces(&self) -> impl ExactSizeIterator<Item = &(ResourceRef, NamespaceBody)> {
        self.namespaces.values()
    }

    pub fn grants(&self) -> impl ExactSizeIterator<Item = &(ResourceRef, InboundGrantBody)> {
        self.grants.iter()
    }
}

impl Canonical for NamespaceBody {
    fn canonical(&self) -> CanonicalValue {
        self.stored().canonical(NAMESPACE_SCHEMA)
    }
}

impl Canonical for InboundGrantBody {
    fn canonical(&self) -> CanonicalValue {
        let namespaces = self.grant.namespaces().map(|set| {
            set.iter()
                .map(|id| id.as_str().to_owned())
                .collect::<Vec<_>>()
        });
        StoredGrant {
            digest: self.digest.to_string(),
            all_namespaces: matches!(self.grant, NamespaceGrant::All),
            namespaces,
            subject: self.subject.clone(),
        }
        .canonical(GRANT_SCHEMA)
    }
}

#[derive(Serialize, Deserialize)]
struct StoredNamespace {
    namespace: String,
    default: bool,
    allow_platform_fallback: bool,
    providers: Vec<StoredProvider>,
    credentials: Vec<StoredCredential>,
    aliases: Vec<StoredAlias>,
    policy: StoredPolicy,
    token_epoch: u64,
}

#[derive(Serialize, Deserialize)]
struct StoredProvider {
    id: String,
    kind: FlatProviderKind,
    base_url: String,
}

#[derive(Serialize, Deserialize)]
struct StoredCredential {
    id: String,
    provider: String,
    secret: String,
    weight: u32,
}

#[derive(Serialize, Deserialize)]
struct StoredAlias {
    name: String,
    targets: Vec<StoredTarget>,
}

#[derive(Serialize, Deserialize)]
struct StoredTarget {
    provider: String,
    model: String,
    input: u64,
    output: u64,
    reasoning: Option<u64>,
    cache_read: Option<u64>,
    cache_write: Option<u64>,
    catalog_provider: Option<String>,
    catalog_model: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct StoredPolicy {
    subject_limit_microdollars: u64,
    namespace_limit_microdollars: Option<u64>,
    reservation_ttl_seconds: u64,
    max_in_flight_per_subject: u64,
    lease_ttl_seconds: u64,
    middleware: Vec<StoredMiddleware>,
    buffered_response_routes: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct StoredMiddleware {
    id: String,
    scopes: Vec<MiddlewareScope>,
    failure_posture: MiddlewareFailurePosture,
    max_duration_milliseconds: u64,
    guardrail: Option<StoredGuardrail>,
}

#[derive(Serialize, Deserialize)]
struct StoredGuardrail {
    key_env: String,
    rules: Vec<GuardrailRule>,
}

#[derive(Serialize, Deserialize)]
struct StoredGrant {
    digest: String,
    all_namespaces: bool,
    namespaces: Option<Vec<String>>,
    subject: Option<String>,
}

impl NamespaceBody {
    fn stored(&self) -> StoredNamespace {
        StoredNamespace {
            namespace: self.namespace.to_string(),
            default: self.default,
            allow_platform_fallback: self.allow_platform_fallback,
            providers: self
                .providers
                .iter()
                .map(|p| StoredProvider {
                    id: p.id.to_string(),
                    kind: p.kind,
                    base_url: p.base_url.clone(),
                })
                .collect(),
            credentials: self
                .credentials
                .iter()
                .map(|c| StoredCredential {
                    id: c.id.to_string(),
                    provider: c.provider.to_string(),
                    secret: c.secret.to_string(),
                    weight: c.weight,
                })
                .collect(),
            aliases: self
                .aliases
                .iter()
                .map(|a| StoredAlias {
                    name: a.name.to_string(),
                    targets: a
                        .targets
                        .iter()
                        .map(|t| StoredTarget {
                            provider: t.provider.to_string(),
                            model: t.model.clone(),
                            input: t.price.input_microdollars_per_million,
                            output: t.price.output_microdollars_per_million,
                            reasoning: t.price.reasoning_microdollars_per_million,
                            cache_read: t.price.cache_read_microdollars_per_million,
                            cache_write: t.price.cache_write_microdollars_per_million,
                            catalog_provider: t.catalog.as_ref().map(|c| c.0.clone()),
                            catalog_model: t.catalog.as_ref().map(|c| c.1.clone()),
                        })
                        .collect(),
                })
                .collect(),
            policy: StoredPolicy {
                subject_limit_microdollars: self.policy.subject_limit_microdollars,
                namespace_limit_microdollars: self.policy.namespace_limit_microdollars,
                reservation_ttl_seconds: self.policy.reservation_ttl_seconds,
                max_in_flight_per_subject: self.policy.max_in_flight_per_subject,
                lease_ttl_seconds: self.policy.lease_ttl_seconds,
                middleware: self
                    .policy
                    .middleware
                    .iter()
                    .map(|registration| StoredMiddleware {
                        id: registration.id().to_owned(),
                        scopes: registration.scopes().to_vec(),
                        failure_posture: registration.failure_posture(),
                        max_duration_milliseconds: registration.max_duration_milliseconds(),
                        guardrail: registration.guardrail().map(|guardrail| StoredGuardrail {
                            key_env: guardrail.key_env().to_owned(),
                            rules: guardrail.rules().to_vec(),
                        }),
                    })
                    .collect(),
                buffered_response_routes: self
                    .policy
                    .buffered_response_routes
                    .iter()
                    .map(|route| route.as_str().to_owned())
                    .collect(),
            },
            token_epoch: self.token_epoch,
        }
    }
}

impl StoredNamespace {
    fn from_value(
        value: CanonicalValue,
        reference: ResourceRef,
    ) -> Result<Self, NamespaceStateError> {
        serde_json::from_value(json_from_canonical(value, reference)?)
            .map_err(|error| body_error(reference, error))
    }

    fn into_body(self, reference: ResourceRef) -> Result<NamespaceBody, NamespaceStateError> {
        let parse_slug =
            |value: String| Slug::parse(&value).map_err(|error| body_error(reference, error));
        let providers = self
            .providers
            .into_iter()
            .map(|p| {
                Ok(NamespaceProvider {
                    id: parse_slug(p.id)?,
                    kind: p.kind,
                    base_url: p.base_url,
                })
            })
            .collect::<Result<Vec<_>, NamespaceStateError>>()?;
        let credentials = self
            .credentials
            .into_iter()
            .map(|c| {
                Ok(NamespaceCredential {
                    id: parse_slug(c.id)?,
                    provider: parse_slug(c.provider)?,
                    secret: SecretRef::parse(&c.secret)
                        .map_err(|error| body_error(reference, error))?,
                    weight: c.weight,
                })
            })
            .collect::<Result<Vec<_>, NamespaceStateError>>()?;
        let aliases = self
            .aliases
            .into_iter()
            .map(|a| {
                Ok(NamespaceAlias {
                    name: parse_slug(a.name)?,
                    targets: a
                        .targets
                        .into_iter()
                        .map(|t| {
                            let catalog = match (t.catalog_provider, t.catalog_model) {
                                (None, None) => None,
                                (Some(provider), Some(model)) => Some((provider, model)),
                                _ => {
                                    return Err(body_error(
                                        reference,
                                        "catalog provider and model must appear together",
                                    ));
                                }
                            };
                            Ok(NamespaceTarget {
                                provider: parse_slug(t.provider)?,
                                model: t.model,
                                price: ModelPrice {
                                    input_microdollars_per_million: t.input,
                                    output_microdollars_per_million: t.output,
                                    reasoning_microdollars_per_million: t.reasoning,
                                    cache_read_microdollars_per_million: t.cache_read,
                                    cache_write_microdollars_per_million: t.cache_write,
                                },
                                catalog,
                            })
                        })
                        .collect::<Result<Vec<_>, NamespaceStateError>>()?,
                })
            })
            .collect::<Result<Vec<_>, NamespaceStateError>>()?;
        NamespaceBody::new(
            NamespaceId::parse(&self.namespace).map_err(|error| body_error(reference, error))?,
            self.default,
            self.allow_platform_fallback,
            providers,
            credentials,
            aliases,
            NamespacePolicySpec {
                subject_limit_microdollars: self.policy.subject_limit_microdollars,
                namespace_limit_microdollars: self.policy.namespace_limit_microdollars,
                reservation_ttl_seconds: self.policy.reservation_ttl_seconds,
                max_in_flight_per_subject: self.policy.max_in_flight_per_subject,
                lease_ttl_seconds: self.policy.lease_ttl_seconds,
                middleware: self
                    .policy
                    .middleware
                    .into_iter()
                    .map(|registration| {
                        let middleware = ContentMiddlewareRegistration::new(
                            registration.id,
                            registration.scopes,
                            registration.failure_posture,
                            registration.max_duration_milliseconds,
                        )
                        .map_err(|error| body_error(reference, error))?;
                        let Some(guardrail) = registration.guardrail else {
                            return Ok(middleware);
                        };
                        middleware
                            .with_guardrail(
                                ContentGuardrailRegistration::new(
                                    guardrail.key_env,
                                    guardrail.rules,
                                )
                                .map_err(|error| body_error(reference, error))?,
                            )
                            .map_err(|error| body_error(reference, error))
                    })
                    .collect::<Result<_, _>>()?,
                buffered_response_routes: self
                    .policy
                    .buffered_response_routes
                    .into_iter()
                    .map(|route| {
                        BufferedResponseRoute::parse(&route)
                            .map_err(|error| body_error(reference, error))
                    })
                    .collect::<Result<_, _>>()?,
            },
            self.token_epoch,
        )
        .map_err(|error| body_error(reference, error))
    }
}

impl StoredGrant {
    fn from_value(
        value: CanonicalValue,
        reference: ResourceRef,
    ) -> Result<Self, NamespaceStateError> {
        serde_json::from_value(json_from_canonical(value, reference)?)
            .map_err(|error| body_error(reference, error))
    }

    fn into_body(self, reference: ResourceRef) -> Result<InboundGrantBody, NamespaceStateError> {
        let grant = match (self.all_namespaces, self.namespaces) {
            (true, None) => NamespaceGrant::all(),
            (false, Some(values)) => NamespaceGrant::set(
                values
                    .into_iter()
                    .map(|value| {
                        NamespaceId::parse(&value).map_err(|error| body_error(reference, error))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .map_err(|error| body_error(reference, error))?,
            _ => {
                return Err(body_error(
                    reference,
                    "grant must select either all namespaces or one non-empty set",
                ));
            }
        };
        InboundGrantBody::new(
            Checksum::parse(&self.digest).map_err(|error| body_error(reference, error))?,
            grant,
            self.subject,
        )
        .map_err(|error| body_error(reference, error))
    }
}

trait StoredCanonical: Serialize {
    fn canonical(&self, schema: &str) -> CanonicalValue {
        let mut value = serde_json::to_value(self).expect("stored v2 bodies are JSON-safe");
        remove_nulls(&mut value);
        value
            .as_object_mut()
            .expect("stored body is an object")
            .insert(
                "schema".to_owned(),
                serde_json::Value::String(schema.to_owned()),
            );
        CanonicalValue::try_from_json(&value).expect("stored v2 bodies have a canonical form")
    }
}
impl StoredCanonical for StoredNamespace {}
impl StoredCanonical for StoredGrant {}

fn remove_nulls(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            fields.retain(|_, value| !value.is_null());
            fields.values_mut().for_each(remove_nulls);
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(remove_nulls),
        _ => {}
    }
}

fn read_inline(
    resource: &ResourceVersion,
    expected: ResourceKind,
    schema: &str,
) -> Result<CanonicalValue, NamespaceStateError> {
    if resource.reference.kind != expected {
        return Err(NamespaceStateError::Kind {
            reference: resource.reference,
        });
    }
    if resource.scope != ResourceScope::Deployment {
        return Err(NamespaceStateError::Scope {
            reference: resource.reference,
        });
    }
    let ResourceBody::Inline(CanonicalValue::Map(mut fields)) = resource.body.clone() else {
        return Err(body_error(resource.reference, "body must be an inline map"));
    };
    let Some(index) = fields.iter().position(|(key, _)| key == "schema") else {
        return Err(body_error(resource.reference, "body has no schema"));
    };
    let (_, CanonicalValue::String(found)) = fields.remove(index) else {
        return Err(body_error(resource.reference, "schema must be text"));
    };
    if found != schema {
        return Err(body_error(
            resource.reference,
            format!("unknown schema `{found}`"),
        ));
    }
    Ok(CanonicalValue::Map(fields))
}

fn json_from_canonical(
    value: CanonicalValue,
    reference: ResourceRef,
) -> Result<serde_json::Value, NamespaceStateError> {
    match value {
        CanonicalValue::Bool(value) => Ok(value.into()),
        CanonicalValue::Integer(value) => u64::try_from(value)
            .map(serde_json::Value::from)
            .map_err(|_| body_error(reference, "integer is outside the supported u64 range")),
        CanonicalValue::String(value) => Ok(value.into()),
        CanonicalValue::List(values) | CanonicalValue::Set(values) => values
            .into_iter()
            .map(|value| json_from_canonical(value, reference))
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        CanonicalValue::Map(fields) => fields
            .into_iter()
            .map(|(key, value)| Ok((key, json_from_canonical(value, reference)?)))
            .collect::<Result<serde_json::Map<_, _>, _>>()
            .map(serde_json::Value::Object),
        CanonicalValue::Bytes(_) => Err(body_error(
            reference,
            "byte strings are not permitted in v2 inline bodies",
        )),
    }
}

fn body_error(reference: ResourceRef, error: impl std::fmt::Display) -> NamespaceStateError {
    NamespaceStateError::Body {
        reference,
        detail: error.to_string(),
    }
}

fn bound(label: &str, count: usize, max: usize) -> Result<(), String> {
    if count > max {
        Err(format!(
            "{label} has {count} entries, over the {max}-entry limit"
        ))
    } else {
        Ok(())
    }
}

fn unique_slugs<'a>(
    label: &str,
    values: impl IntoIterator<Item = &'a Slug>,
) -> Result<BTreeSet<&'a str>, String> {
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value.as_str()) {
            return Err(format!("duplicate {label} `{value}`"));
        }
    }
    Ok(seen)
}

fn absolute_origin(value: &str) -> bool {
    value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .and_then(|rest| rest.split('/').next())
        .is_some_and(|host| !host.is_empty() && !host.contains(char::is_whitespace))
}
