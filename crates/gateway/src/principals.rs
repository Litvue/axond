//! Inbound identity: the one deployment-wide static gateway key (ADR 0063).

use std::collections::HashSet;
use std::sync::Arc;

use secrecy::{ExposeSecret, SecretString};

use crate::namespace::{InvalidNamespaceId, NamespaceGrant, NamespaceId};

macro_rules! define_capabilities {
    ($($capability:ident),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
        pub enum Capability {
            $($capability),+
        }

        impl Capability {
        }
    };
}

define_capabilities!(
    Chat,
    Messages,
    Embeddings,
    Responses,
    Models,
    Credentials,
    CredentialsAll,
);

impl Capability {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Messages => "messages",
            Self::Embeddings => "embeddings",
            Self::Responses => "responses",
            Self::Models => "models",
            Self::Credentials => "credentials",
            Self::CredentialsAll => "credentials:all",
        }
    }
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// How a principal's authority was established. A configured
/// `[[gateway_key]]` is the only inbound identity (ADR 0063).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrincipalAuthority {
    StaticKey,
}

#[derive(Clone)]
pub struct InboundKey {
    pub namespace: String,
    pub subject: String,
    pub authority: PrincipalAuthority,
    pub signer_kid: Option<String>,
    pub scope: Option<HashSet<Capability>>,
    pub max_request_microdollars: Option<u64>,
    pub namespace_grant: Option<NamespaceGrant>,
    /// Opaque namespace attrs copied at namespaced admission (ADR 0063).
    pub attrs: Option<serde_json::Value>,
}

impl InboundKey {
    /// The flat namespace grant authentication produced for inference.
    ///
    /// Existing credentials name one namespace. Parsing at this boundary keeps
    /// legacy config storage independent from ADR 0062's public URL contract;
    /// malformed legacy names fail closed when used on a canonical route.
    pub fn namespace_grant(&self) -> Result<NamespaceGrant, InvalidNamespaceId> {
        self.namespace_grant.clone().map_or_else(
            || NamespaceId::parse(&self.namespace).map(NamespaceGrant::one),
            Ok,
        )
    }

    /// Whether this principal holds the operator's own authority over the whole
    /// deployment, rather than authority over one namespace.
    ///
    /// Only a configured static gateway key in the default namespace does: an
    /// operator placed that secret there itself. A scope narrows a static key
    /// rather than widening it, so a scoped one is not treated as unrestricted,
    /// and every minted token carries delegated authority bounded by minting and
    /// its verifier — including one that presents an operator-only capability
    /// from a signer outside `POST /v1/tokens`.
    ///
    /// This is the predicate behind the all-namespaces credential view and
    /// it lives here because
    /// authentication is the only place that knows how a principal was
    /// established.
    pub fn holds_direct_operator_authority(&self, default_namespace: &str) -> bool {
        self.authority == PrincipalAuthority::StaticKey
            && self.scope.is_none()
            && self.namespace == default_namespace
    }
}

pub(crate) struct GatewayKeyEntry {
    pub(crate) secret: SecretString,
    pub(crate) caller: InboundKey,
}

pub struct ConfigPrincipals {
    inbound_keys: Arc<[GatewayKeyEntry]>,
}

impl ConfigPrincipals {
    pub(crate) fn new(inbound_keys: Arc<[GatewayKeyEntry]>) -> Self {
        Self { inbound_keys }
    }

    pub(crate) fn count(&self) -> usize {
        self.inbound_keys.len()
    }

    #[cfg(test)]
    pub(crate) fn first_secret_debug(&self) -> String {
        format!("{:?}", self.inbound_keys[0].secret)
    }

    pub(crate) fn resolve_static(&self, credential: &str) -> Option<InboundKey> {
        resolve_static_key(&self.inbound_keys, credential).cloned()
    }
}

fn resolve_static_key<'a>(
    entries: &'a [GatewayKeyEntry],
    credential: &str,
) -> Option<&'a InboundKey> {
    entries
        .iter()
        .find(|entry| {
            constant_time_eq(
                entry.secret.expose_secret().as_bytes(),
                credential.as_bytes(),
            )
        })
        .map(|entry| &entry.caller)
}

pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

pub struct Presented<'a> {
    pub credential: &'a str,
}
