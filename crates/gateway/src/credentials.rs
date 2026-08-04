//! Namespaced provider-credential resolution.
//!
//! Credentials are read from the process environment once at startup (they are
//! fixed for the process lifetime) into a `(namespace, provider) → secret` map.
//! A future watched-file layer can swap the `Arc` for hot reload; the lookup
//! stays a pure function of the snapshot so it is testable without mutating the
//! global environment (assessment §5.1).
//!
//! Invariant: credentials are **write-only**. Nothing here ever returns a key
//! to a caller — only presence is observable.

use std::collections::HashMap;

use secrecy::SecretString;

use crate::config::Config;

/// Which key served a request, for usage attribution (delta A3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialSource {
    Platform,
    Byok,
}

pub struct Resolved {
    pub secret: SecretString,
    pub source: CredentialSource,
}

pub struct Credentials {
    /// (namespace, provider) → secret
    map: HashMap<(String, String), SecretString>,
    platform_ns: String,
}

impl Credentials {
    /// Build the snapshot from config + a captured environment map.
    pub fn from_env(config: &Config, env: &HashMap<String, String>) -> Self {
        let mut map = HashMap::new();
        for c in &config.credential {
            if let Some(v) = env.get(&c.env).filter(|v| !v.is_empty()) {
                map.insert(
                    (c.namespace.clone(), c.provider.clone()),
                    SecretString::from(v.clone()),
                );
            }
        }
        Self {
            map,
            platform_ns: config.default_namespace().to_string(),
        }
    }

    /// Resolve the key for `(namespace, provider)`, applying platform fallback
    /// only when the namespace explicitly allows it.
    pub fn resolve(&self, config: &Config, namespace: &str, provider: &str) -> Option<Resolved> {
        if let Some(secret) = self.map.get(&(namespace.to_string(), provider.to_string())) {
            let source = if namespace == self.platform_ns {
                CredentialSource::Platform
            } else {
                CredentialSource::Byok
            };
            return Some(Resolved {
                secret: secret.clone(),
                source,
            });
        }
        let allow_fallback = config
            .namespace(namespace)
            .map(|n| n.allow_platform_fallback)
            .unwrap_or(false);
        if allow_fallback
            && namespace != self.platform_ns
            && let Some(secret) = self
                .map
                .get(&(self.platform_ns.clone(), provider.to_string()))
        {
            return Some(Resolved {
                secret: secret.clone(),
                source: CredentialSource::Platform,
            });
        }
        None
    }

    /// Presence only — never the value (write-only invariant). Backs the
    /// per-namespace "which providers are live here" read surface.
    #[allow(dead_code)] // backs the readiness / provider-status endpoint (follow-up)
    pub fn is_present(&self, config: &Config, namespace: &str, provider: &str) -> bool {
        self.resolve(config, namespace, provider).is_some()
    }
}
