//! What an availability verdict is *about*: a scope and a target, both named
//! through references this slice deliberately keeps thin (#206).
//!
//! Availability sits downstream of three foundations that are not landed yet —
//! tenancy scope (#144/#191), catalogue identities (#192/#146), and credential
//! resolution (#198/#145) — and it has to be evaluated per tenant from the first
//! line of code, because a verdict that is not scoped is a cross-tenant leak
//! waiting for a caller. So references here carry the *shape* of the eventual
//! identities and none of their internals:
//!
//! - a [`ScopeRef`] is the tenancy pair the durable model already defines
//!   ([`TenantId`] and an optional [`ProjectId`]), so nothing has to be invented
//!   and nothing has to be rewritten when project enablement lands;
//! - a [`TargetRef`] is a *pair of opaque tokens*, one for the provider and one
//!   for the upstream model, rather than the catalogue's own typed id. The
//!   catalogue slice owns that id, and an availability index that guessed at it
//!   would have to be unshipped; a token is what both the file-declared
//!   `[[model]]` targets and a durable catalogue entry can be reduced to.
//! - a [`CredentialRef`] is the same kind of token, and it is a *reference*: it
//!   names which credential an entitlement was decided against so an operator can
//!   correlate a verdict, and it never holds material. Nothing in [`Availability`]
//!   has a field it would fit in.
//!
//! [`Availability`]: super::Availability
//!
//! # Tokens are bounded, and that is a redaction property
//!
//! A token is non-empty, at most [`Token::MAX_LEN`] bytes, and printable ASCII
//! with no whitespace. That refuses the things that make a name unsafe to put in
//! a log line, a metric label, or an operator's terminal — control characters,
//! newlines that could forge a second log record, byte-order marks — at the
//! boundary, once, rather than at every place a verdict is rendered.

use std::fmt;

use crate::desired_state::{ProjectId, ResourceScope, TenantId};

/// Why a token was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidToken {
    #[error("a {kind} reference must not be empty")]
    Empty { kind: &'static str },
    #[error("{kind} reference is {length} bytes, over the {max}-byte limit")]
    TooLong {
        kind: &'static str,
        length: usize,
        max: usize,
    },
    #[error("{kind} reference contains {codepoint:#06x}, which is not printable ASCII")]
    Unprintable { kind: &'static str, codepoint: u32 },
}

/// A bounded, printable, whitespace-free name.
///
/// The shared shape behind [`ProviderRef`], [`ModelRef`], and [`CredentialRef`].
/// They are distinct types over it so a provider cannot be passed where a model
/// belongs, the same reason the durable ids are typed per entity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(String);

impl Token {
    pub const MAX_LEN: usize = 128;

    fn parse(kind: &'static str, input: &str) -> Result<Self, InvalidToken> {
        if input.is_empty() {
            return Err(InvalidToken::Empty { kind });
        }
        if input.len() > Self::MAX_LEN {
            return Err(InvalidToken::TooLong {
                kind,
                length: input.len(),
                max: Self::MAX_LEN,
            });
        }
        if let Some(character) = input
            .chars()
            .find(|c| !c.is_ascii_graphic() || *c == '\u{7f}')
        {
            return Err(InvalidToken::Unprintable {
                kind,
                codepoint: u32::from(character),
            });
        }
        Ok(Self(input.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

macro_rules! token_ref {
    ($name:ident, $kind:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(Token);

        impl $name {
            /// The kind name a refusal reads as.
            pub const KIND: &'static str = $kind;

            pub fn parse(input: &str) -> Result<Self, InvalidToken> {
                Token::parse($kind, input).map($name)
            }

            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

token_ref!(
    ProviderRef,
    "provider",
    "Which provider a target belongs to: the `[[provider]]` id today, a catalogue \
     provider identity after #192."
);
token_ref!(
    ModelRef,
    "model",
    "The upstream model or deployment a target names. Not a caller-facing alias: \
     availability is evaluated over what the gateway would actually call."
);
token_ref!(
    CredentialRef,
    "credential",
    "Which credential an entitlement was decided against. A reference, never \
     material, and never carried into a verdict."
);

/// Which tenancy scope a verdict is about.
///
/// Availability is per scope, always. Two tenants entitled differently to one
/// upstream model have two verdicts, and an index holds them under two keys, so
/// there is no shared entry a projection could accidentally widen. `project` is
/// optional because tenant-wide facts (an entitlement, a policy) exist before
/// project enablement narrows them (#205/#149).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopeRef {
    pub tenant: TenantId,
    pub project: Option<ProjectId>,
}

impl ScopeRef {
    /// A tenant-wide scope.
    pub const fn tenant(tenant: TenantId) -> Self {
        Self {
            tenant,
            project: None,
        }
    }

    /// A single project's scope inside a tenant.
    pub const fn project(tenant: TenantId, project: ProjectId) -> Self {
        Self {
            tenant,
            project: Some(project),
        }
    }

    /// Whether this scope is the tenant-wide one.
    pub const fn is_tenant_wide(&self) -> bool {
        self.project.is_none()
    }

    /// The availability scope a resource scope names, if it names one.
    ///
    /// [`ResourceScope::Deployment`] names none, and that is not an oversight:
    /// availability is entitlement, entitlement belongs to a tenant, and a
    /// deployment-wide availability answer would be every tenant's in one
    /// document.
    pub const fn of(scope: &ResourceScope) -> Option<Self> {
        match scope {
            ResourceScope::Deployment => None,
            ResourceScope::Tenant(tenant) => Some(Self::tenant(*tenant)),
            ResourceScope::Project { tenant, project } => Some(Self::project(*tenant, *project)),
        }
    }
}

impl fmt::Display for ScopeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.project {
            Some(project) => write!(f, "{}/{project}", self.tenant),
            None => write!(f, "{}", self.tenant),
        }
    }
}

/// Which upstream target a verdict is about.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TargetRef {
    pub provider: ProviderRef,
    pub model: ModelRef,
}

impl TargetRef {
    pub const fn new(provider: ProviderRef, model: ModelRef) -> Self {
        Self { provider, model }
    }

    /// Parse the `provider/model` pair a config target is spelled as.
    pub fn parse(provider: &str, model: &str) -> Result<Self, InvalidToken> {
        Ok(Self::new(
            ProviderRef::parse(provider)?,
            ModelRef::parse(model)?,
        ))
    }
}

impl fmt::Display for TargetRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.provider, self.model)
    }
}

/// The key an [`AvailabilityIndex`](super::AvailabilityIndex) holds one record
/// under: a scope and a target, in that order.
///
/// Ordered scope-first so iterating an index walks one tenant's targets together
/// and a scoped read is a range over a contiguous run.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AvailabilityKey {
    pub scope: ScopeRef,
    pub target: TargetRef,
}

impl AvailabilityKey {
    pub const fn new(scope: ScopeRef, target: TargetRef) -> Self {
        Self { scope, target }
    }
}

impl fmt::Display for AvailabilityKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.scope, self.target)
    }
}
