//! Deployment resources, flat namespaces, and inbound grants (ADR 0062).
//!
//! Deployment resources own shared authority. A namespace pins one immutable
//! deployment resource and owns only its complete serving selections; it does
//! not inherit from a tenant, project, or principal.

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

const DEPLOYMENT_SCHEMA: &str = "axond.deployment.v2";
const NAMESPACE_SCHEMA: &str = "axond.namespace.v2";
const GRANT_SCHEMA: &str = "axond.inbound-grant.v2";
const MAX_NAMESPACES: usize = 4_096;
const MAX_GRANTS: usize = 16_384;
const MAX_TOTAL_GRANTED_NAMESPACES: usize = 262_144;
const MAX_PROVIDERS: usize = 64;
const MAX_CATALOGUE_ENTRIES: usize = 4_096;
const MAX_TRUST_ENTRIES: usize = 64;
const MAX_CREDENTIALS: usize = 128;
const MAX_ALIASES: usize = 256;
const MAX_TARGETS: usize = 16;
const MAX_MIDDLEWARE: usize = 32;
const MAX_SUBJECT_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentProvider {
    pub id: Slug,
    pub kind: FlatProviderKind,
    pub base_url: String,
}

/// Transitional source compatibility for callers of the unmerged prototype.
/// Provider authority lives only in [`DeploymentBody`].
pub type NamespaceProvider = DeploymentProvider;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentCatalogueEntry {
    pub provider: Slug,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentTrust {
    pub id: Slug,
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentBody {
    providers: Vec<DeploymentProvider>,
    catalogue: Vec<DeploymentCatalogueEntry>,
    middleware: Vec<ContentMiddlewareRegistration>,
    trust: Vec<DeploymentTrust>,
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
    pub epoch: u64,
    pub subject_limit_microdollars: u64,
    pub namespace_limit_microdollars: Option<u64>,
    pub reservation_ttl_seconds: u64,
    pub max_in_flight_per_subject: u64,
    pub lease_ttl_seconds: u64,
    pub middleware: Vec<String>,
    pub buffered_response_routes: Vec<BufferedResponseRoute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceBody {
    namespace: NamespaceId,
    default: bool,
    allow_platform_fallback: bool,
    deployment: ResourceRef,
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
    #[error("{reference} is not a flat-v2 deployment, namespace, or inbound-grant resource")]
    Kind { reference: ResourceRef },
    #[error("{reference} must be deployment-scoped")]
    Scope { reference: ResourceRef },
    #[error("{reference} does not contain a supported inline v2 body: {detail}")]
    Body {
        reference: ResourceRef,
        detail: String,
    },
    #[error("{reference} carries an incompatible v2 schema: {detail}")]
    Incompatible {
        reference: ResourceRef,
        detail: String,
    },
    #[error("flat namespace state must contain exactly one deployment resource (found {count})")]
    DeploymentCount { count: usize },
    #[error("namespace `{namespace}` pins missing deployment resource {deployment}")]
    MissingDeployment {
        namespace: NamespaceId,
        deployment: ResourceRef,
    },
    #[error("namespace `{namespace}` is declared more than once")]
    DuplicateNamespace { namespace: NamespaceId },
    #[error("grant digest {digest} is declared by both {first} and {second}")]
    DuplicateGrantDigest {
        digest: Checksum,
        first: ResourceRef,
        second: ResourceRef,
    },
    #[error("flat namespace state has {count} {label}, over the {max}-entry limit")]
    AggregateBound {
        label: &'static str,
        count: usize,
        max: usize,
    },
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

impl NamespaceStateError {
    /// Schema skew is actionable separately from malformed/corrupt content.
    pub const fn is_incompatible(&self) -> bool {
        matches!(self, Self::Incompatible { .. })
    }
}

#[derive(Debug, Clone, Default)]
pub struct FlatNamespaces {
    deployments: BTreeMap<ResourceRef, DeploymentBody>,
    namespaces: BTreeMap<NamespaceId, (ResourceRef, NamespaceBody)>,
    grants: Vec<(ResourceRef, InboundGrantBody)>,
}

impl DeploymentBody {
    pub fn new(
        mut providers: Vec<DeploymentProvider>,
        mut catalogue: Vec<DeploymentCatalogueEntry>,
        mut middleware: Vec<ContentMiddlewareRegistration>,
        mut trust: Vec<DeploymentTrust>,
    ) -> Result<Self, String> {
        providers.sort_by(|left, right| left.id.cmp(&right.id));
        catalogue.sort_by(|left, right| {
            (&left.provider, &left.model).cmp(&(&right.provider, &right.model))
        });
        middleware.sort_by(|left, right| left.id().cmp(right.id()));
        trust.sort_by(|left, right| left.id.cmp(&right.id));
        let body = Self {
            providers,
            catalogue,
            middleware,
            trust,
        };
        body.validate()?;
        Ok(body)
    }

    pub fn providers(&self) -> &[DeploymentProvider] {
        &self.providers
    }

    pub fn catalogue(&self) -> &[DeploymentCatalogueEntry] {
        &self.catalogue
    }

    pub fn middleware(&self) -> &[ContentMiddlewareRegistration] {
        &self.middleware
    }

    pub fn trust(&self) -> &[DeploymentTrust] {
        &self.trust
    }

    pub fn version(&self, id: ResourceId, slug: Slug) -> ResourceVersion {
        ResourceVersion::new(
            ResourceRef::new(ResourceKind::Deployment, id, ResourceVersionNumber::FIRST),
            ResourceScope::Deployment,
            slug,
            ResourceBody::Inline(self.canonical()),
        )
    }

    pub fn read(resource: &ResourceVersion) -> Result<Self, NamespaceStateError> {
        if !resource.depends_on.is_empty() {
            return Err(body_error(
                resource.reference,
                "deployment resources cannot depend on other resources",
            ));
        }
        let body = read_inline(resource, ResourceKind::Deployment, DEPLOYMENT_SCHEMA)
            .and_then(|value| StoredDeployment::from_value(value, resource.reference))
            .and_then(|stored| stored.into_body(resource.reference))?;
        ensure_canonical(resource, &body)?;
        Ok(body)
    }

    fn validate(&self) -> Result<(), String> {
        bound("providers", self.providers.len(), MAX_PROVIDERS)?;
        bound(
            "catalogue entries",
            self.catalogue.len(),
            MAX_CATALOGUE_ENTRIES,
        )?;
        bound(
            "middleware registrations",
            self.middleware.len(),
            MAX_MIDDLEWARE,
        )?;
        bound("trust entries", self.trust.len(), MAX_TRUST_ENTRIES)?;
        let providers = unique_slugs("provider", self.providers.iter().map(|p| &p.id))?;
        for provider in &self.providers {
            if !absolute_origin(&provider.base_url) {
                return Err(format!(
                    "provider `{}` does not declare an absolute HTTP(S) origin",
                    provider.id
                ));
            }
        }
        let mut catalogue = BTreeSet::new();
        for entry in &self.catalogue {
            if !providers.contains(entry.provider.as_str()) {
                return Err(format!(
                    "catalogue model `{}` references unknown provider `{}`",
                    entry.model, entry.provider
                ));
            }
            if entry.model.is_empty() || entry.model.len() > 256 {
                return Err("catalogue model id must contain between 1 and 256 bytes".into());
            }
            if !catalogue.insert((entry.provider.as_str(), entry.model.as_str())) {
                return Err(format!(
                    "duplicate catalogue model `{}/{}`",
                    entry.provider, entry.model
                ));
            }
        }
        let mut middleware = BTreeSet::new();
        for registration in &self.middleware {
            if !middleware.insert(registration.id()) {
                return Err(format!(
                    "duplicate middleware registration `{}`",
                    registration.id()
                ));
            }
        }
        unique_slugs("trust entry", self.trust.iter().map(|entry| &entry.id))?;
        for entry in &self.trust {
            if !absolute_origin(&entry.issuer) || !absolute_origin(&entry.jwks_url) {
                return Err(format!(
                    "trust entry `{}` must declare absolute HTTP(S) issuer and JWKS URLs",
                    entry.id
                ));
            }
            if entry.audience.is_empty() || entry.audience.len() > 256 {
                return Err(format!(
                    "trust entry `{}` has an invalid audience",
                    entry.id
                ));
            }
        }
        Ok(())
    }
}

impl NamespaceBody {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        namespace: NamespaceId,
        default: bool,
        allow_platform_fallback: bool,
        deployment: ResourceRef,
        credentials: Vec<NamespaceCredential>,
        aliases: Vec<NamespaceAlias>,
        policy: NamespacePolicySpec,
        token_epoch: u64,
    ) -> Result<Self, String> {
        let mut body = Self {
            namespace,
            default,
            allow_platform_fallback,
            deployment,
            credentials,
            aliases,
            policy,
            token_epoch,
        };
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
    pub const fn deployment(&self) -> ResourceRef {
        self.deployment
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
        .depending_on([self.deployment])
    }

    pub fn read(resource: &ResourceVersion) -> Result<Self, NamespaceStateError> {
        let body = read_inline(resource, ResourceKind::Namespace, NAMESPACE_SCHEMA)
            .and_then(|value| StoredNamespace::from_value(value, resource.reference))
            .and_then(|stored| stored.into_body(resource.reference))?;
        if resource.depends_on != BTreeSet::from([body.deployment]) {
            return Err(body_error(
                resource.reference,
                "namespace dependencies must contain exactly its pinned deployment resource",
            ));
        }
        ensure_canonical(resource, &body)?;
        Ok(body)
    }

    fn validate(&self) -> Result<(), String> {
        if self.deployment.kind != ResourceKind::Deployment {
            return Err("namespace deployment reference must have kind `deployment`".into());
        }
        bound("credentials", self.credentials.len(), MAX_CREDENTIALS)?;
        bound("aliases", self.aliases.len(), MAX_ALIASES)?;
        bound(
            "middleware selections",
            self.policy.middleware.len(),
            MAX_MIDDLEWARE,
        )?;
        if self.policy.epoch == 0
            || self.policy.subject_limit_microdollars == 0
            || self.policy.namespace_limit_microdollars == Some(0)
            || self.policy.reservation_ttl_seconds == 0
            || self.policy.max_in_flight_per_subject == 0
            || self.policy.lease_ttl_seconds == 0
        {
            return Err(
                "policy epoch, caps, duration, and concurrency bounds must be non-zero".into(),
            );
        }
        unique_slugs("credential", self.credentials.iter().map(|c| &c.id))?;
        for credential in &self.credentials {
            if credential.weight == 0 {
                return Err(format!("credential `{}` has zero weight", credential.id));
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
                if target.model.is_empty() || target.model.len() > 256 {
                    return Err(format!("alias `{}` has an invalid model id", alias.name));
                }
            }
        }
        let mut middleware = BTreeSet::new();
        for selection in &self.policy.middleware {
            validate_selector(selection)?;
            if !middleware.insert(selection.as_str()) {
                return Err(format!("duplicate middleware selection `{selection}`"));
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
        if !resource.depends_on.is_empty() {
            return Err(body_error(
                resource.reference,
                "inbound grants cannot depend on other resources",
            ));
        }
        let body = read_inline(resource, ResourceKind::InboundGrant, GRANT_SCHEMA)
            .and_then(|value| StoredGrant::from_value(value, resource.reference))
            .and_then(|stored| stored.into_body(resource.reference))?;
        ensure_canonical(resource, &body)?;
        Ok(body)
    }
}

impl FlatNamespaces {
    pub fn of(state: &super::DesiredState) -> Result<Self, NamespaceStateError> {
        let mut result = Self::default();
        let mut grant_digests = BTreeMap::new();
        let mut total_granted_namespaces = 0usize;
        for resource in state.resources() {
            match resource.reference.kind {
                ResourceKind::Deployment => {
                    result
                        .deployments
                        .insert(resource.reference, DeploymentBody::read(resource)?);
                }
                ResourceKind::Namespace => {
                    aggregate_bound("namespaces", result.namespaces.len() + 1, MAX_NAMESPACES)?;
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
                ResourceKind::InboundGrant => {
                    aggregate_bound("inbound grants", result.grants.len() + 1, MAX_GRANTS)?;
                    let body = InboundGrantBody::read(resource)?;
                    if let Some(namespaces) = body.grant().namespaces() {
                        total_granted_namespaces = total_granted_namespaces
                            .checked_add(namespaces.len())
                            .ok_or(NamespaceStateError::AggregateBound {
                                label: "granted namespace memberships",
                                count: usize::MAX,
                                max: MAX_TOTAL_GRANTED_NAMESPACES,
                            })?;
                        aggregate_bound(
                            "granted namespace memberships",
                            total_granted_namespaces,
                            MAX_TOTAL_GRANTED_NAMESPACES,
                        )?;
                    }
                    if let Some(first) = grant_digests.insert(body.digest(), resource.reference) {
                        return Err(NamespaceStateError::DuplicateGrantDigest {
                            digest: body.digest(),
                            first,
                            second: resource.reference,
                        });
                    }
                    result.grants.push((resource.reference, body));
                }
                _ => {}
            }
        }
        if result.deployments.len() != 1 {
            return Err(NamespaceStateError::DeploymentCount {
                count: result.deployments.len(),
            });
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
        for (reference, body) in result.namespaces.values() {
            let Some(deployment) = result.deployments.get(&body.deployment()) else {
                return Err(NamespaceStateError::MissingDeployment {
                    namespace: body.namespace().clone(),
                    deployment: body.deployment(),
                });
            };
            validate_namespace_references(body, deployment)
                .map_err(|detail| body_error(*reference, detail))?;
        }
        Ok(result)
    }

    pub fn deployment(&self) -> (ResourceRef, &DeploymentBody) {
        let (reference, body) = self
            .deployments
            .first_key_value()
            .expect("validated flat state has one deployment");
        (*reference, body)
    }

    pub fn namespaces(&self) -> impl ExactSizeIterator<Item = &(ResourceRef, NamespaceBody)> {
        self.namespaces.values()
    }

    pub fn grants(&self) -> impl ExactSizeIterator<Item = &(ResourceRef, InboundGrantBody)> {
        self.grants.iter()
    }
}

impl Canonical for DeploymentBody {
    fn canonical(&self) -> CanonicalValue {
        self.stored().canonical(DEPLOYMENT_SCHEMA)
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
            all_namespaces: self.grant.is_all(),
            namespaces,
            subject: self.subject.clone(),
        }
        .canonical(GRANT_SCHEMA)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredDeployment {
    providers: Vec<StoredProvider>,
    catalogue: Vec<StoredCatalogueEntry>,
    middleware: Vec<StoredMiddleware>,
    trust: Vec<StoredTrust>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredNamespace {
    namespace: String,
    default: bool,
    allow_platform_fallback: bool,
    deployment_id: String,
    deployment_version: u64,
    credentials: Vec<StoredCredential>,
    aliases: Vec<StoredAlias>,
    policy: StoredPolicy,
    token_epoch: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredProvider {
    id: String,
    kind: FlatProviderKind,
    base_url: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCatalogueEntry {
    provider: String,
    model: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredTrust {
    id: String,
    issuer: String,
    audience: String,
    jwks_url: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredCredential {
    id: String,
    provider: String,
    secret: String,
    weight: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredAlias {
    name: String,
    targets: Vec<StoredTarget>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
struct StoredPolicy {
    epoch: u64,
    subject_limit_microdollars: u64,
    namespace_limit_microdollars: Option<u64>,
    reservation_ttl_seconds: u64,
    max_in_flight_per_subject: u64,
    lease_ttl_seconds: u64,
    middleware: Vec<String>,
    buffered_response_routes: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredMiddleware {
    id: String,
    scopes: Vec<MiddlewareScope>,
    failure_posture: MiddlewareFailurePosture,
    max_duration_milliseconds: u64,
    guardrail: Option<StoredGuardrail>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredGuardrail {
    key_env: String,
    rules: Vec<GuardrailRule>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredGrant {
    digest: String,
    all_namespaces: bool,
    namespaces: Option<Vec<String>>,
    subject: Option<String>,
}

impl DeploymentBody {
    fn stored(&self) -> StoredDeployment {
        StoredDeployment {
            providers: self
                .providers
                .iter()
                .map(|provider| StoredProvider {
                    id: provider.id.to_string(),
                    kind: provider.kind,
                    base_url: provider.base_url.clone(),
                })
                .collect(),
            catalogue: self
                .catalogue
                .iter()
                .map(|entry| StoredCatalogueEntry {
                    provider: entry.provider.to_string(),
                    model: entry.model.clone(),
                })
                .collect(),
            middleware: self.middleware.iter().map(stored_middleware).collect(),
            trust: self
                .trust
                .iter()
                .map(|entry| StoredTrust {
                    id: entry.id.to_string(),
                    issuer: entry.issuer.clone(),
                    audience: entry.audience.clone(),
                    jwks_url: entry.jwks_url.clone(),
                })
                .collect(),
        }
    }
}

impl NamespaceBody {
    fn stored(&self) -> StoredNamespace {
        StoredNamespace {
            namespace: self.namespace.to_string(),
            default: self.default,
            allow_platform_fallback: self.allow_platform_fallback,
            deployment_id: self.deployment.id.to_string(),
            deployment_version: self.deployment.version.get(),
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
                epoch: self.policy.epoch,
                subject_limit_microdollars: self.policy.subject_limit_microdollars,
                namespace_limit_microdollars: self.policy.namespace_limit_microdollars,
                reservation_ttl_seconds: self.policy.reservation_ttl_seconds,
                max_in_flight_per_subject: self.policy.max_in_flight_per_subject,
                lease_ttl_seconds: self.policy.lease_ttl_seconds,
                middleware: self.policy.middleware.clone(),
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

fn stored_middleware(registration: &ContentMiddlewareRegistration) -> StoredMiddleware {
    StoredMiddleware {
        id: registration.id().to_owned(),
        scopes: registration.scopes().to_vec(),
        failure_posture: registration.failure_posture(),
        max_duration_milliseconds: registration.max_duration_milliseconds(),
        guardrail: registration.guardrail().map(|guardrail| StoredGuardrail {
            key_env: guardrail.key_env().to_owned(),
            rules: guardrail.rules().to_vec(),
        }),
    }
}

impl StoredDeployment {
    fn from_value(
        value: CanonicalValue,
        reference: ResourceRef,
    ) -> Result<Self, NamespaceStateError> {
        deserialize_v2(value, reference)
    }

    fn into_body(self, reference: ResourceRef) -> Result<DeploymentBody, NamespaceStateError> {
        let parse_slug =
            |value: String| Slug::parse(&value).map_err(|error| body_error(reference, error));
        let providers = self
            .providers
            .into_iter()
            .map(|provider| {
                Ok(DeploymentProvider {
                    id: parse_slug(provider.id)?,
                    kind: provider.kind,
                    base_url: provider.base_url,
                })
            })
            .collect::<Result<Vec<_>, NamespaceStateError>>()?;
        let catalogue = self
            .catalogue
            .into_iter()
            .map(|entry| {
                Ok(DeploymentCatalogueEntry {
                    provider: parse_slug(entry.provider)?,
                    model: entry.model,
                })
            })
            .collect::<Result<Vec<_>, NamespaceStateError>>()?;
        let middleware = self
            .middleware
            .into_iter()
            .map(|registration| middleware_from_stored(registration, reference))
            .collect::<Result<Vec<_>, _>>()?;
        let trust = self
            .trust
            .into_iter()
            .map(|entry| {
                Ok(DeploymentTrust {
                    id: parse_slug(entry.id)?,
                    issuer: entry.issuer,
                    audience: entry.audience,
                    jwks_url: entry.jwks_url,
                })
            })
            .collect::<Result<Vec<_>, NamespaceStateError>>()?;
        DeploymentBody::new(providers, catalogue, middleware, trust)
            .map_err(|error| body_error(reference, error))
    }
}

impl StoredNamespace {
    fn from_value(
        value: CanonicalValue,
        reference: ResourceRef,
    ) -> Result<Self, NamespaceStateError> {
        deserialize_v2(value, reference)
    }

    fn into_body(self, reference: ResourceRef) -> Result<NamespaceBody, NamespaceStateError> {
        let parse_slug =
            |value: String| Slug::parse(&value).map_err(|error| body_error(reference, error));
        let deployment = ResourceRef::new(
            ResourceKind::Deployment,
            ResourceId::parse(&self.deployment_id).map_err(|error| body_error(reference, error))?,
            ResourceVersionNumber::new(self.deployment_version)
                .ok_or_else(|| body_error(reference, "deployment version must be non-zero"))?,
        );
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
            deployment,
            credentials,
            aliases,
            NamespacePolicySpec {
                epoch: self.policy.epoch,
                subject_limit_microdollars: self.policy.subject_limit_microdollars,
                namespace_limit_microdollars: self.policy.namespace_limit_microdollars,
                reservation_ttl_seconds: self.policy.reservation_ttl_seconds,
                max_in_flight_per_subject: self.policy.max_in_flight_per_subject,
                lease_ttl_seconds: self.policy.lease_ttl_seconds,
                middleware: self.policy.middleware,
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
        deserialize_v2(value, reference)
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
impl StoredCanonical for StoredDeployment {}

fn middleware_from_stored(
    registration: StoredMiddleware,
    reference: ResourceRef,
) -> Result<ContentMiddlewareRegistration, NamespaceStateError> {
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
            ContentGuardrailRegistration::new(guardrail.key_env, guardrail.rules)
                .map_err(|error| body_error(reference, error))?,
        )
        .map_err(|error| body_error(reference, error))
}

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
        return Err(NamespaceStateError::Incompatible {
            reference: resource.reference,
            detail: format!("expected `{schema}`, found `{found}`"),
        });
    }
    Ok(CanonicalValue::Map(fields))
}

fn deserialize_v2<T: for<'de> Deserialize<'de>>(
    value: CanonicalValue,
    reference: ResourceRef,
) -> Result<T, NamespaceStateError> {
    serde_json::from_value(json_from_canonical(value, reference)?).map_err(|error| {
        let detail = error.to_string();
        if detail.contains("unknown field") {
            NamespaceStateError::Incompatible { reference, detail }
        } else {
            body_error(reference, detail)
        }
    })
}

fn ensure_canonical<T: Canonical>(
    resource: &ResourceVersion,
    body: &T,
) -> Result<(), NamespaceStateError> {
    if resource.body != ResourceBody::Inline(body.canonical()) {
        return Err(body_error(
            resource.reference,
            "body is valid but not in its canonical stored form",
        ));
    }
    Ok(())
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

fn aggregate_bound(
    label: &'static str,
    count: usize,
    max: usize,
) -> Result<(), NamespaceStateError> {
    if count > max {
        Err(NamespaceStateError::AggregateBound { label, count, max })
    } else {
        Ok(())
    }
}

fn validate_namespace_references(
    namespace: &NamespaceBody,
    deployment: &DeploymentBody,
) -> Result<(), String> {
    let providers = deployment
        .providers()
        .iter()
        .map(|provider| provider.id.as_str())
        .collect::<BTreeSet<_>>();
    for credential in namespace.credentials() {
        if !providers.contains(credential.provider.as_str()) {
            return Err(format!(
                "credential `{}` references unknown deployment provider `{}`",
                credential.id, credential.provider
            ));
        }
    }
    let catalogue = deployment
        .catalogue()
        .iter()
        .map(|entry| (entry.provider.as_str(), entry.model.as_str()))
        .collect::<BTreeSet<_>>();
    for alias in namespace.aliases() {
        for target in &alias.targets {
            if !providers.contains(target.provider.as_str()) {
                return Err(format!(
                    "alias `{}` references unknown deployment provider `{}`",
                    alias.name, target.provider
                ));
            }
            if let Some((provider, model)) = &target.catalog
                && !catalogue.contains(&(provider.as_str(), model.as_str()))
            {
                return Err(format!(
                    "alias `{}` references unknown deployment catalogue entry `{provider}/{model}`",
                    alias.name
                ));
            }
        }
    }
    let middleware = deployment
        .middleware()
        .iter()
        .map(ContentMiddlewareRegistration::id)
        .collect::<BTreeSet<_>>();
    for selection in &namespace.policy().middleware {
        if !middleware.contains(selection.as_str()) {
            return Err(format!(
                "namespace selects unknown deployment middleware `{selection}`"
            ));
        }
    }
    Ok(())
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

fn validate_selector(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 64
        || !value.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || (index > 0 && matches!(byte, b'.' | b'_' | b'-'))
        })
    {
        return Err(format!("middleware selection `{value}` is not canonical"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::fixtures;

    fn slug(value: &str) -> Slug {
        Slug::parse(value).unwrap()
    }

    fn deployment() -> DeploymentBody {
        DeploymentBody::new(
            vec![DeploymentProvider {
                id: slug("shared"),
                kind: FlatProviderKind::OpenaiCompatible,
                base_url: "https://provider.example/v1".to_owned(),
            }],
            vec![DeploymentCatalogueEntry {
                provider: slug("shared"),
                model: "gpt-test".to_owned(),
            }],
            vec![
                ContentMiddlewareRegistration::new(
                    "test.marker",
                    [MiddlewareScope::Request],
                    MiddlewareFailurePosture::FailClosed,
                    10,
                )
                .unwrap(),
            ],
            vec![DeploymentTrust {
                id: slug("admin"),
                issuer: "https://issuer.example".to_owned(),
                audience: "axond-admin".to_owned(),
                jwks_url: "https://issuer.example/jwks".to_owned(),
            }],
        )
        .unwrap()
    }

    fn deployment_ref() -> ResourceRef {
        ResourceRef::new(
            ResourceKind::Deployment,
            fixtures::resource_id(90),
            ResourceVersionNumber::FIRST,
        )
    }

    fn namespace() -> NamespaceBody {
        NamespaceBody::new(
            NamespaceId::parse("acme").unwrap(),
            true,
            true,
            deployment_ref(),
            Vec::new(),
            vec![NamespaceAlias {
                name: slug("fast"),
                targets: vec![NamespaceTarget {
                    provider: slug("shared"),
                    model: "gpt-test".to_owned(),
                    price: ModelPrice {
                        input_microdollars_per_million: 1,
                        output_microdollars_per_million: 2,
                        reasoning_microdollars_per_million: None,
                        cache_read_microdollars_per_million: None,
                        cache_write_microdollars_per_million: None,
                    },
                    catalog: Some(("shared".to_owned(), "gpt-test".to_owned())),
                }],
            }],
            NamespacePolicySpec {
                epoch: 1,
                subject_limit_microdollars: 1,
                namespace_limit_microdollars: Some(2),
                reservation_ttl_seconds: 3,
                max_in_flight_per_subject: 4,
                lease_ttl_seconds: 5,
                middleware: vec!["test.marker".to_owned()],
                buffered_response_routes: Vec::new(),
            },
            6,
        )
        .unwrap()
    }

    fn map_count(value: &CanonicalValue) -> usize {
        match value {
            CanonicalValue::Map(fields) => {
                1 + fields
                    .iter()
                    .map(|(_, value)| map_count(value))
                    .sum::<usize>()
            }
            CanonicalValue::List(values) | CanonicalValue::Set(values) => {
                values.iter().map(map_count).sum()
            }
            _ => 0,
        }
    }

    fn add_unknown_to_nth_map(value: &mut CanonicalValue, target: usize, seen: &mut usize) -> bool {
        match value {
            CanonicalValue::Map(fields) => {
                if *seen == target {
                    fields.push(("future_field".to_owned(), CanonicalValue::Bool(true)));
                    return true;
                }
                *seen += 1;
                fields
                    .iter_mut()
                    .any(|(_, value)| add_unknown_to_nth_map(value, target, seen))
            }
            CanonicalValue::List(values) | CanonicalValue::Set(values) => values
                .iter_mut()
                .any(|value| add_unknown_to_nth_map(value, target, seen)),
            _ => false,
        }
    }

    fn every_map_rejects_unknown_fields(
        resource: ResourceVersion,
        read: fn(&ResourceVersion) -> Result<(), NamespaceStateError>,
    ) {
        let ResourceBody::Inline(body) = &resource.body else {
            panic!("inline fixture");
        };
        for index in 0..map_count(body) {
            let mut changed = resource.clone();
            let ResourceBody::Inline(value) = &mut changed.body else {
                unreachable!();
            };
            assert!(add_unknown_to_nth_map(value, index, &mut 0));
            assert!(
                matches!(
                    read(&changed),
                    Err(NamespaceStateError::Incompatible { .. })
                ),
                "map {index} accepted an unknown field"
            );
        }
    }

    #[test]
    fn unknown_fields_are_typed_incompatible_at_every_v2_map_level() {
        every_map_rejects_unknown_fields(
            deployment().version(fixtures::resource_id(90), slug("deployment")),
            |resource| DeploymentBody::read(resource).map(|_| ()),
        );
        every_map_rejects_unknown_fields(
            namespace().version(fixtures::resource_id(91), slug("acme")),
            |resource| NamespaceBody::read(resource).map(|_| ()),
        );
        every_map_rejects_unknown_fields(
            InboundGrantBody::new(
                Checksum::of(b"grant"),
                NamespaceGrant::all(),
                Some("subject".to_owned()),
            )
            .unwrap()
            .version(fixtures::resource_id(92), slug("grant")),
            |resource| InboundGrantBody::read(resource).map(|_| ()),
        );
    }

    #[test]
    fn stored_grant_sets_must_already_be_canonical() {
        let grant = InboundGrantBody::new(
            Checksum::of(b"grant"),
            NamespaceGrant::one(NamespaceId::parse("acme").unwrap()),
            None,
        )
        .unwrap();
        let mut resource = grant.version(fixtures::resource_id(92), slug("grant"));
        let ResourceBody::Inline(CanonicalValue::Map(fields)) = &mut resource.body else {
            unreachable!();
        };
        let (_, CanonicalValue::List(namespaces)) = fields
            .iter_mut()
            .find(|(field, _)| field == "namespaces")
            .unwrap()
        else {
            unreachable!();
        };
        namespaces.push(CanonicalValue::string("acme"));
        assert!(matches!(
            InboundGrantBody::read(&resource),
            Err(NamespaceStateError::Body { .. })
        ));
    }

    #[test]
    fn duplicate_grant_digests_are_refused_before_projection() {
        let mut state = super::super::DesiredState::new();
        state
            .insert(deployment().version(fixtures::resource_id(90), slug("deployment")))
            .unwrap();
        state
            .insert(namespace().version(fixtures::resource_id(91), slug("acme")))
            .unwrap();
        for seed in [92, 93] {
            state
                .insert(
                    InboundGrantBody::new(Checksum::of(b"same"), NamespaceGrant::all(), None)
                        .unwrap()
                        .version(fixtures::resource_id(seed), slug(&format!("grant-{seed}"))),
                )
                .unwrap();
        }
        assert!(matches!(
            FlatNamespaces::of(&state),
            Err(NamespaceStateError::DuplicateGrantDigest { .. })
        ));
    }

    #[test]
    fn aggregate_bounds_and_nonzero_caps_are_enforced() {
        assert!(matches!(
            aggregate_bound("namespaces", MAX_NAMESPACES + 1, MAX_NAMESPACES),
            Err(NamespaceStateError::AggregateBound { .. })
        ));
        let mut invalid = namespace();
        invalid.policy.subject_limit_microdollars = 0;
        assert!(invalid.validate().unwrap_err().contains("non-zero"));
        invalid = namespace();
        invalid.policy.namespace_limit_microdollars = Some(0);
        assert!(invalid.validate().unwrap_err().contains("non-zero"));
    }
}
