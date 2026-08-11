use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header, errors::ErrorKind as JwtErrorKind,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::aliases::AliasScope;
use crate::config::{Config, GatewayVerifierAlgorithm};
use crate::key_material::{self, KeyMaterialError};

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Capability {
    Chat,
    Messages,
    Embeddings,
    Models,
}

impl Capability {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "chat" => Some(Self::Chat),
            "messages" => Some(Self::Messages),
            "embeddings" => Some(Self::Embeddings),
            "models" => Some(Self::Models),
            _ => None,
        }
    }

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Messages => "messages",
            Self::Embeddings => "embeddings",
            Self::Models => "models",
        }
    }
}

impl std::fmt::Display for Capability {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Clone)]
pub struct InboundKey {
    pub namespace: String,
    pub subject: String,
    pub signer_kid: Option<String>,
    pub scope: Option<HashSet<Capability>>,
    pub alias_scope: Option<AliasScope>,
    pub max_request_microdollars: Option<u64>,
    pub jti: Option<String>,
}

pub(crate) struct GatewayKeyEntry {
    pub(crate) secret: SecretString,
    pub(crate) caller: InboundKey,
}

pub struct Presented<'a> {
    pub credential: &'a str,
}

#[derive(Debug, thiserror::Error)]
pub enum PrincipalShapeError {
    #[error(
        "principal shape `{shape}` is owned by both `{first}` and `{second}`, so authority cannot be determined"
    )]
    Duplicate {
        shape: &'static str,
        first: &'static str,
        second: &'static str,
    },
    #[error(
        "principal shapes `{first_shape}` and `{second_shape}` overlap between `{first}` and `{second}`, so authority cannot be determined"
    )]
    Overlap {
        first_shape: &'static str,
        second_shape: &'static str,
        first: &'static str,
        second: &'static str,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum PrincipalStoreError {
    #[allow(dead_code)]
    #[error("principal store unavailable")]
    Unavailable,
    #[error("token authentication failed: {0}")]
    Unauthorized(TokenVerificationError),
    #[error("token authorization failed: {0}")]
    Forbidden(TokenVerificationError),
}

#[derive(Debug, thiserror::Error)]
pub enum TokenVerificationError {
    #[error("malformed token")]
    Malformed,
    #[error("unknown verification key `{kid}`")]
    UnknownKey { kid: String },
    #[error("token algorithm does not match verifier `{kid}`")]
    AlgorithmMismatch { kid: String },
    #[error("invalid token signature")]
    InvalidSignature,
    #[error("token has expired")]
    Expired,
    #[error("token is not yet valid")]
    NotYetValid,
    #[error("token audience is invalid")]
    WrongAudience,
    #[error("token is missing required claim `{claim}`")]
    MissingClaim { claim: String },
    #[error("token lifetime is invalid for verifier `{kid}`")]
    InvalidLifetime { kid: String },
    #[error("token names unknown namespace `{namespace}`")]
    UnknownNamespace { namespace: String },
    #[error("verifier `{kid}` is not permitted for namespace `{namespace}`")]
    SignerNotPermitted { kid: String, namespace: String },
    #[error("token is not permitted to use alias `{alias}`")]
    AliasNotPermitted { alias: String },
    #[error("token aliases claim is invalid")]
    InvalidAliasClaim,
    #[error(
        "token for namespace `{namespace}` and subject `{subject}` was issued before its revocation epoch"
    )]
    IssuedBeforeEpoch { namespace: String, subject: String },
    #[error("token has been revoked")]
    Revoked,
}

impl TokenVerificationError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Malformed => "token_malformed",
            Self::UnknownKey { .. } => "token_unknown_key",
            Self::AlgorithmMismatch { .. } => "token_algorithm_mismatch",
            Self::InvalidSignature => "token_invalid_signature",
            Self::Expired => "token_expired",
            Self::NotYetValid => "token_not_yet_valid",
            Self::WrongAudience => "token_wrong_audience",
            Self::MissingClaim { .. } => "token_missing_claim",
            Self::InvalidLifetime { .. } => "token_invalid_lifetime",
            Self::UnknownNamespace { .. } => "token_unknown_namespace",
            Self::SignerNotPermitted { .. } => "token_signer_not_permitted",
            Self::AliasNotPermitted { .. } => "token_alias_not_permitted",
            Self::InvalidAliasClaim => "token_alias_claim_invalid",
            Self::IssuedBeforeEpoch { .. } => "token_issued_before_epoch",
            Self::Revoked => "token_revoked",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TokenVerifierBuildError {
    #[error("gateway verifiers require `[gateway_token] audience`")]
    MissingAudience,
    #[error("gateway_verifier `{kid}` references env var `{env}`, which is unset or empty")]
    MissingKey { kid: String, env: String },
    #[error("gateway_verifier `{kid}` key material file `{path}` failed ({kind}): {error}")]
    FileKey {
        kid: String,
        path: String,
        kind: std::io::ErrorKind,
        error: String,
    },
    #[error("gateway_verifier `{kid}` key material file `{path}` is empty")]
    EmptyFile { kid: String, path: String },
    #[error("gateway_verifier `{kid}` key material file `{path}` is not valid UTF-8")]
    InvalidFileUtf8 { kid: String, path: String },
    #[error("gateway_verifier `{kid}` must declare exactly one non-empty source")]
    InvalidSource { kid: String },
    #[error("gateway_verifier `{kid}` has an HS256 secret that is too short")]
    WeakHs256Secret { kid: String },
    #[error(
        "gateway_verifier `{kid}` key material file `{path}` has an HS256 secret that is too short"
    )]
    FileWeakHs256Secret { kid: String, path: String },
    #[error("gateway_verifier `{kid}` has invalid base64 key material")]
    InvalidBase64 { kid: String },
    #[error("gateway_verifier `{kid}` key material file `{path}` has invalid base64 key material")]
    FileInvalidBase64 { kid: String, path: String },
    #[error("gateway_verifier `{kid}` must contain a 32-byte Ed25519 public key")]
    InvalidEd25519Key { kid: String },
    #[error(
        "gateway_verifier `{kid}` key material file `{path}` must contain a 32-byte Ed25519 public key"
    )]
    FileInvalidEd25519Key { kid: String, path: String },
}

const TOKEN_CLOCK_SKEW_SECONDS: u64 = 5;

struct ResolvedVerifier {
    kid: String,
    algorithm: Algorithm,
    namespaces: HashSet<String>,
    max_ttl: Duration,
    key: DecodingKey,
    fingerprint: String,
}

#[derive(Default)]
struct NamespaceEpoch {
    namespace_min_iat: Option<u64>,
    subjects: HashMap<String, u64>,
}

pub struct TokenVerifier {
    audience: String,
    namespaces: HashSet<String>,
    verifiers: Vec<ResolvedVerifier>,
    epochs: HashMap<String, NamespaceEpoch>,
}

#[derive(Debug, Deserialize)]
struct TokenClaims {
    exp: Option<u64>,
    iat: Option<u64>,
    jti: Option<String>,
    ns: Option<String>,
    sub: Option<String>,
    scope: Option<RawScope>,
    // Keep this loose so a wrong JSON type becomes a typed 403, not a 401 decode failure.
    #[serde(default, deserialize_with = "deserialize_optional_value")]
    aliases: Option<Option<Value>>,
    max_request_microdollars: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawScope {
    String(String),
    Array(Vec<String>),
}

impl RawScope {
    fn capabilities(self) -> HashSet<Capability> {
        let values = match self {
            Self::String(value) => value.split_whitespace().map(str::to_owned).collect(),
            Self::Array(values) => values,
        };
        values
            .iter()
            .filter_map(|value| Capability::parse(value))
            .collect()
    }
}

fn deserialize_optional_value<'de, D>(deserializer: D) -> Result<Option<Option<Value>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(Option::<Value>::deserialize(deserializer)?))
}

impl TokenVerifier {
    pub(crate) fn build(
        config: &Config,
        env: &std::collections::HashMap<String, String>,
    ) -> Result<Option<Self>, TokenVerifierBuildError> {
        if config.gateway_verifier.is_empty() {
            return Ok(None);
        }
        let audience = config
            .gateway_token
            .as_ref()
            .ok_or(TokenVerifierBuildError::MissingAudience)?
            .audience
            .clone();
        let namespaces = config
            .namespace
            .iter()
            .map(|namespace| namespace.id.clone())
            .collect();
        let mut epochs: HashMap<String, NamespaceEpoch> = HashMap::new();
        for epoch in &config.gateway_token_epoch {
            let namespace = epochs.entry(epoch.namespace.clone()).or_default();
            if let Some(subject) = &epoch.subject {
                namespace.subjects.insert(subject.clone(), epoch.min_iat);
            } else {
                namespace.namespace_min_iat = Some(epoch.min_iat);
            }
        }
        let mut verifiers = Vec::with_capacity(config.gateway_verifier.len());
        for verifier in &config.gateway_verifier {
            let source =
                verifier
                    .source()
                    .ok_or_else(|| TokenVerifierBuildError::InvalidSource {
                        kid: verifier.kid.clone(),
                    })?;
            let value = key_material::resolve(source, env).map_err(|error| match error {
                KeyMaterialError::MissingEnv { name } => TokenVerifierBuildError::MissingKey {
                    kid: verifier.kid.clone(),
                    env: name,
                },
                KeyMaterialError::FileRead { path, kind, error } => {
                    TokenVerifierBuildError::FileKey {
                        kid: verifier.kid.clone(),
                        path,
                        kind,
                        error,
                    }
                }
                KeyMaterialError::EmptyFile { path } => TokenVerifierBuildError::EmptyFile {
                    kid: verifier.kid.clone(),
                    path,
                },
                KeyMaterialError::InvalidUtf8 { path } => {
                    TokenVerifierBuildError::InvalidFileUtf8 {
                        kid: verifier.kid.clone(),
                        path,
                    }
                }
            })?;
            let key = match verifier.alg {
                // HS256 material lives inside DecodingKey beyond our
                // zeroization control; Ed25519 is the preferred default.
                GatewayVerifierAlgorithm::Hs256 => {
                    if value.len() < 32 {
                        return Err(match source {
                            crate::config::KeyMaterialSource::File(path) => {
                                TokenVerifierBuildError::FileWeakHs256Secret {
                                    kid: verifier.kid.clone(),
                                    path: path.to_owned(),
                                }
                            }
                            crate::config::KeyMaterialSource::Env(_) => {
                                TokenVerifierBuildError::WeakHs256Secret {
                                    kid: verifier.kid.clone(),
                                }
                            }
                        });
                    }
                    DecodingKey::from_secret(value.as_bytes())
                }
                GatewayVerifierAlgorithm::EdDsa => {
                    let decoded = BASE64.decode(value.trim()).map_err(|_| match source {
                        crate::config::KeyMaterialSource::File(path) => {
                            TokenVerifierBuildError::FileInvalidBase64 {
                                kid: verifier.kid.clone(),
                                path: path.to_owned(),
                            }
                        }
                        crate::config::KeyMaterialSource::Env(_) => {
                            TokenVerifierBuildError::InvalidBase64 {
                                kid: verifier.kid.clone(),
                            }
                        }
                    })?;
                    if decoded.len() != 32 {
                        return Err(match source {
                            crate::config::KeyMaterialSource::File(path) => {
                                TokenVerifierBuildError::FileInvalidEd25519Key {
                                    kid: verifier.kid.clone(),
                                    path: path.to_owned(),
                                }
                            }
                            crate::config::KeyMaterialSource::Env(_) => {
                                TokenVerifierBuildError::InvalidEd25519Key {
                                    kid: verifier.kid.clone(),
                                }
                            }
                        });
                    }
                    // jsonwebtoken's EdDSA verifier takes the raw 32-byte
                    // Ed25519 public key through `from_ed_der`; this is
                    // validated by the round-trip test below.
                    DecodingKey::from_ed_der(&decoded)
                }
            };
            verifiers.push(ResolvedVerifier {
                kid: verifier.kid.clone(),
                algorithm: match verifier.alg {
                    GatewayVerifierAlgorithm::EdDsa => Algorithm::EdDSA,
                    GatewayVerifierAlgorithm::Hs256 => Algorithm::HS256,
                },
                namespaces: verifier.namespaces.iter().cloned().collect(),
                max_ttl: verifier.max_ttl,
                key,
                fingerprint: key_material::fingerprint(&verifier.kid, &value),
            });
        }
        Ok(Some(Self {
            audience,
            namespaces,
            verifiers,
            epochs,
        }))
    }

    pub(crate) fn fingerprints(&self) -> std::collections::HashMap<String, String> {
        self.verifiers
            .iter()
            .map(|verifier| (verifier.kid.clone(), verifier.fingerprint.clone()))
            .collect()
    }
}

#[async_trait]
impl PrincipalStore for TokenVerifier {
    fn name(&self) -> &'static str {
        "token-verifier"
    }

    fn shapes(&self) -> &'static [&'static str] {
        &["axt1."]
    }

    async fn resolve(
        &self,
        presented: &Presented<'_>,
    ) -> Result<Option<InboundKey>, PrincipalStoreError> {
        let token =
            presented
                .credential
                .strip_prefix("axt1.")
                .ok_or(PrincipalStoreError::Unauthorized(
                    TokenVerificationError::Malformed,
                ))?;
        let header = decode_header(token)
            .map_err(|_| PrincipalStoreError::Unauthorized(TokenVerificationError::Malformed))?;
        let kid = header.kid.ok_or(PrincipalStoreError::Unauthorized(
            TokenVerificationError::MissingClaim {
                claim: "kid".to_owned(),
            },
        ))?;
        let verifier = self
            .verifiers
            .iter()
            .find(|verifier| verifier.kid == kid)
            .ok_or_else(|| {
                PrincipalStoreError::Unauthorized(TokenVerificationError::UnknownKey {
                    kid: kid.clone(),
                })
            })?;
        if header.alg != verifier.algorithm {
            return Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::AlgorithmMismatch { kid },
            ));
        }

        let mut validation = Validation::new(verifier.algorithm);
        // Five seconds is the fixed clock-skew allowance promised by ADR 0016.
        validation.leeway = TOKEN_CLOCK_SKEW_SECONDS;
        validation.validate_nbf = true;
        validation.set_audience(std::slice::from_ref(&self.audience));
        validation.set_required_spec_claims(&["exp", "aud"]);
        let data = decode::<TokenClaims>(token, &verifier.key, &validation).map_err(|error| {
            PrincipalStoreError::Unauthorized(match error.kind() {
                JwtErrorKind::ExpiredSignature => TokenVerificationError::Expired,
                JwtErrorKind::ImmatureSignature => TokenVerificationError::NotYetValid,
                JwtErrorKind::InvalidAudience => TokenVerificationError::WrongAudience,
                JwtErrorKind::MissingRequiredClaim(claim) => TokenVerificationError::MissingClaim {
                    claim: claim.to_owned(),
                },
                JwtErrorKind::InvalidSignature => TokenVerificationError::InvalidSignature,
                _ => TokenVerificationError::Malformed,
            })
        })?;
        let claims = data.claims;
        if claims.jti.as_deref().is_none_or(str::is_empty) {
            return Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::MissingClaim {
                    claim: "jti".to_owned(),
                },
            ));
        }
        let iat = claims.iat.ok_or(PrincipalStoreError::Unauthorized(
            TokenVerificationError::MissingClaim {
                claim: "iat".to_owned(),
            },
        ))?;
        let exp = claims.exp.ok_or(PrincipalStoreError::Unauthorized(
            TokenVerificationError::MissingClaim {
                claim: "exp".to_owned(),
            },
        ))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs();
        let max_ttl = verifier.max_ttl.as_secs();
        if exp < iat
            || exp - iat > max_ttl
            || iat > now.saturating_add(TOKEN_CLOCK_SKEW_SECONDS)
            || exp
                > now
                    .saturating_add(max_ttl)
                    .saturating_add(TOKEN_CLOCK_SKEW_SECONDS)
        {
            return Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::InvalidLifetime {
                    kid: verifier.kid.clone(),
                },
            ));
        }
        let namespace = claims.ns.filter(|namespace| !namespace.is_empty()).ok_or(
            PrincipalStoreError::Unauthorized(TokenVerificationError::MissingClaim {
                claim: "ns".to_owned(),
            }),
        )?;
        if !self.namespaces.contains(&namespace) {
            return Err(PrincipalStoreError::Forbidden(
                TokenVerificationError::UnknownNamespace { namespace },
            ));
        }
        if !verifier.namespaces.contains(&namespace) {
            return Err(PrincipalStoreError::Forbidden(
                TokenVerificationError::SignerNotPermitted {
                    kid: verifier.kid.clone(),
                    namespace,
                },
            ));
        }
        let alias_scope = match claims.aliases {
            None => None,
            Some(Some(value)) => Some(
                AliasScope::parse(serde_json::from_value::<Vec<String>>(value).map_err(|_| {
                    PrincipalStoreError::Forbidden(TokenVerificationError::InvalidAliasClaim)
                })?)
                .map_err(|_| {
                    PrincipalStoreError::Forbidden(TokenVerificationError::InvalidAliasClaim)
                })?,
            ),
            Some(None) => {
                return Err(PrincipalStoreError::Forbidden(
                    TokenVerificationError::InvalidAliasClaim,
                ));
            }
        };
        let subject = claims.sub.filter(|subject| !subject.is_empty()).ok_or(
            PrincipalStoreError::Unauthorized(TokenVerificationError::MissingClaim {
                claim: "sub".to_owned(),
            }),
        )?;
        let epoch = self.epochs.get(namespace.as_str()).and_then(|namespace| {
            namespace
                .subjects
                .get(subject.as_str())
                .copied()
                .or(namespace.namespace_min_iat)
        });
        if epoch.is_some_and(|min_iat| iat < min_iat) {
            return Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::IssuedBeforeEpoch { namespace, subject },
            ));
        }
        Ok(Some(InboundKey {
            namespace,
            subject,
            signer_kid: Some(verifier.kid.clone()),
            scope: claims.scope.map(RawScope::capabilities),
            alias_scope,
            max_request_microdollars: claims.max_request_microdollars,
            jti: claims.jti,
        }))
    }
}

#[async_trait]
pub trait PrincipalStore: Send + Sync {
    fn name(&self) -> &'static str;
    fn shapes(&self) -> &'static [&'static str];
    async fn resolve(
        &self,
        presented: &Presented<'_>,
    ) -> Result<Option<InboundKey>, PrincipalStoreError>;
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

#[async_trait]
impl PrincipalStore for ConfigPrincipals {
    fn name(&self) -> &'static str {
        "config"
    }

    fn shapes(&self) -> &'static [&'static str] {
        &[]
    }

    async fn resolve(
        &self,
        presented: &Presented<'_>,
    ) -> Result<Option<InboundKey>, PrincipalStoreError> {
        Ok(self.resolve_static(presented.credential))
    }
}

pub struct PrincipalStoreChain {
    stores: Vec<Box<dyn PrincipalStore>>,
    config: ConfigPrincipals,
}

impl PrincipalStoreChain {
    pub(crate) fn new(
        stores: Vec<Box<dyn PrincipalStore>>,
        config: ConfigPrincipals,
    ) -> Result<Self, PrincipalShapeError> {
        let mut declared: Vec<(&'static str, &'static str)> = Vec::new();
        for store in &stores {
            for &shape in store.shapes() {
                for &(first_shape, first) in &declared {
                    if first_shape == shape {
                        return Err(PrincipalShapeError::Duplicate {
                            shape,
                            first,
                            second: store.name(),
                        });
                    }
                    // A longer prefix also matches credentials owned by the
                    // shorter prefix, so equality alone cannot establish
                    // unambiguous authority.
                    if first_shape.starts_with(shape) || shape.starts_with(first_shape) {
                        return Err(PrincipalShapeError::Overlap {
                            first_shape,
                            second_shape: shape,
                            first,
                            second: store.name(),
                        });
                    }
                }
                declared.push((shape, store.name()));
            }
        }
        Ok(Self { stores, config })
    }

    pub(crate) async fn resolve(
        &self,
        presented: &Presented<'_>,
    ) -> Result<Option<InboundKey>, PrincipalStoreError> {
        for store in &self.stores {
            if store
                .shapes()
                .iter()
                .any(|shape| presented.credential.starts_with(shape))
            {
                return store.resolve(presented).await;
            }
        }
        self.config.resolve(presented).await
    }

    pub(crate) fn config_count(&self) -> usize {
        self.config.count()
    }

    pub(crate) fn owner_name(&self, presented: &Presented<'_>) -> &'static str {
        self.stores
            .iter()
            .find(|store| {
                store
                    .shapes()
                    .iter()
                    .any(|shape| presented.credential.starts_with(shape))
            })
            .map_or(self.config.name(), |store| store.name())
    }

    #[cfg(test)]
    pub(crate) fn config_first_secret_debug(&self) -> String {
        self.config.first_secret_debug()
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

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde::Serialize;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    struct ShapedStore {
        name: &'static str,
        shapes: &'static [&'static str],
    }

    #[async_trait]
    impl PrincipalStore for ShapedStore {
        fn name(&self) -> &'static str {
            self.name
        }

        fn shapes(&self) -> &'static [&'static str] {
            self.shapes
        }

        async fn resolve(
            &self,
            _presented: &Presented<'_>,
        ) -> Result<Option<InboundKey>, PrincipalStoreError> {
            Ok(None)
        }
    }

    struct FailingStore;

    #[async_trait]
    impl PrincipalStore for FailingStore {
        fn name(&self) -> &'static str {
            "failing"
        }

        fn shapes(&self) -> &'static [&'static str] {
            &["axk_"]
        }

        async fn resolve(
            &self,
            _presented: &Presented<'_>,
        ) -> Result<Option<InboundKey>, PrincipalStoreError> {
            Err(PrincipalStoreError::Unavailable)
        }
    }

    fn config_principals() -> ConfigPrincipals {
        ConfigPrincipals::new(Arc::from(vec![GatewayKeyEntry {
            secret: SecretString::from("static-secret"),
            caller: InboundKey {
                namespace: "platform".to_owned(),
                subject: "AXOND_KEY".to_owned(),
                signer_kid: None,
                scope: None,
                alias_scope: None,
                max_request_microdollars: None,
                jti: None,
            },
        }]))
    }

    #[test]
    fn duplicate_shapes_are_rejected() {
        let Err(err) = PrincipalStoreChain::new(
            vec![
                Box::new(ShapedStore {
                    name: "first",
                    shapes: &["axk_"],
                }),
                Box::new(ShapedStore {
                    name: "second",
                    shapes: &["axk_"],
                }),
            ],
            config_principals(),
        ) else {
            panic!("duplicate shape ownership must be rejected");
        };

        assert!(matches!(
            err,
            PrincipalShapeError::Duplicate {
                shape: "axk_",
                first: "first",
                second: "second",
            }
        ));
    }

    #[test]
    fn overlapping_shapes_are_rejected_when_shorter_shape_comes_first() {
        let Err(err) = PrincipalStoreChain::new(
            vec![
                Box::new(ShapedStore {
                    name: "short",
                    shapes: &["axk_"],
                }),
                Box::new(ShapedStore {
                    name: "long",
                    shapes: &["axk_v2_"],
                }),
            ],
            config_principals(),
        ) else {
            panic!("overlapping shape ownership must be rejected");
        };

        assert!(matches!(
            err,
            PrincipalShapeError::Overlap {
                first_shape: "axk_",
                second_shape: "axk_v2_",
                first: "short",
                second: "long",
            }
        ));
    }

    #[test]
    fn overlapping_shapes_are_rejected_when_longer_shape_comes_first() {
        let Err(err) = PrincipalStoreChain::new(
            vec![
                Box::new(ShapedStore {
                    name: "long",
                    shapes: &["axk_v2_"],
                }),
                Box::new(ShapedStore {
                    name: "short",
                    shapes: &["axk_"],
                }),
            ],
            config_principals(),
        ) else {
            panic!("overlapping shape ownership must be rejected");
        };

        assert!(matches!(
            err,
            PrincipalShapeError::Overlap {
                first_shape: "axk_v2_",
                second_shape: "axk_",
                first: "long",
                second: "short",
            }
        ));
    }

    #[tokio::test]
    async fn an_owned_shape_does_not_fall_back_to_config() {
        let chain = PrincipalStoreChain::new(
            vec![Box::new(ShapedStore {
                name: "store",
                shapes: &["axk_"],
            })],
            config_principals(),
        )
        .expect("unique shape");

        let presented = Presented {
            credential: "axk_key_static-secret",
        };
        assert!(chain.resolve(&presented).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_owned_shape_error_does_not_fall_back_to_config() {
        let chain = PrincipalStoreChain::new(vec![Box::new(FailingStore)], config_principals())
            .expect("unique shape");
        let presented = Presented {
            credential: "axk_key_static-secret",
        };
        assert!(matches!(
            chain.resolve(&presented).await,
            Err(PrincipalStoreError::Unavailable)
        ));
    }

    #[tokio::test]
    async fn an_unowned_shape_uses_config_principals() {
        let chain = PrincipalStoreChain::new(Vec::new(), config_principals()).expect("valid chain");
        let presented = Presented {
            credential: "static-secret",
        };
        let principal = chain
            .resolve(&presented)
            .await
            .expect("config resolution succeeds")
            .expect("static key resolves");
        assert_eq!(principal.namespace, "platform");
        assert_eq!(principal.subject, "AXOND_KEY");
    }

    #[derive(Clone, Serialize)]
    struct TestClaims {
        #[serde(skip_serializing_if = "Option::is_none")]
        exp: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        iat: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        nbf: Option<u64>,
        aud: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        jti: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ns: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        sub: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        scope: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        aliases: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_request_microdollars: Option<u64>,
    }

    const ED_PRIVATE_PK8: &[u8] = &[
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20, 0x6a, 0xc3, 0xfd, 0xee, 0xee, 0x29, 0x8a, 0x92, 0x63, 0x8b, 0x70, 0x0c, 0x4b, 0x11,
        0x7c, 0xc3, 0x2e, 0x2d, 0x2a, 0xce, 0x0d, 0xfd, 0x78, 0x76, 0x94, 0xe2, 0x4c, 0xae, 0x8a,
        0xd5, 0x82, 0x34,
    ];
    const ED_PUBLIC_RAW: &[u8] = &[
        0xdb, 0xe2, 0x63, 0xd9, 0x4b, 0xcd, 0x0a, 0xf4, 0x22, 0x50, 0xf3, 0x58, 0x46, 0x04, 0xa2,
        0xd1, 0xc2, 0x52, 0x3e, 0x22, 0x48, 0xe9, 0x1b, 0x3a, 0x0f, 0x45, 0x13, 0x78, 0x4a, 0x50,
        0x56, 0x3f,
    ];

    fn temp_material(contents: &[u8]) -> String {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "axond-verifier-material-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, contents).expect("write verifier material");
        path.to_str().expect("utf-8 temp path").to_owned()
    }

    fn file_verifier_config(path: &str, algorithm: &str) -> Config {
        Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "STATIC_KEY"
namespace = "platform"

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "file-test"
alg = "{algorithm}"
file = "{path}"
namespaces = ["platform"]
max_ttl = "15m"
"#
        ))
        .expect("file verifier config")
    }

    fn token_verifier() -> TokenVerifier {
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "acme"

[[gateway_key]]
env = "STATIC_KEY"
namespace = "platform"

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "ed-test"
alg = "EdDSA"
env = "ED_PUBLIC"
namespaces = ["acme"]
max_ttl = "15m"
"#,
        )
        .expect("test verifier config");
        let env = HashMap::from([
            ("STATIC_KEY".to_owned(), "static-secret".to_owned()),
            ("ED_PUBLIC".to_owned(), BASE64.encode(ED_PUBLIC_RAW)),
        ]);
        TokenVerifier::build(&config, &env)
            .expect("test verifier builds")
            .expect("verifier is configured")
    }

    fn signed_token_with(
        kid: &str,
        algorithm: Algorithm,
        key: &EncodingKey,
        claims: TestClaims,
    ) -> String {
        let mut header = Header::new(algorithm);
        header.kid = Some(kid.to_owned());
        format!(
            "axt1.{}",
            encode(&header, &claims, key).expect("test token signs")
        )
    }

    fn signed_token(claims: TestClaims) -> String {
        signed_token_with(
            "ed-test",
            Algorithm::EdDSA,
            &EncodingKey::from_ed_der(ED_PRIVATE_PK8),
            claims,
        )
    }

    fn valid_claims() -> TestClaims {
        valid_claims_for("acme")
    }

    fn valid_claims_for(namespace: &str) -> TestClaims {
        let now = unix_now();
        TestClaims {
            exp: Some(now + 900),
            iat: Some(now),
            nbf: None,
            aud: "test-audience".to_owned(),
            jti: Some("jti-1".to_owned()),
            ns: Some(namespace.to_owned()),
            sub: Some("caller-1".to_owned()),
            scope: None,
            aliases: None,
            max_request_microdollars: None,
        }
    }

    fn unix_now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs()
    }

    #[tokio::test]
    async fn token_verifier_accepts_a_raw_base64_ed25519_public_key() {
        let verifier = token_verifier();
        let principal = verifier
            .resolve(&Presented {
                credential: &signed_token(valid_claims()),
            })
            .await
            .expect("valid token resolves")
            .expect("valid token returns a principal");
        assert_eq!(principal.namespace, "acme");
        assert_eq!(principal.subject, "caller-1");
        assert_eq!(principal.signer_kid.as_deref(), Some("ed-test"));
        assert_eq!(principal.max_request_microdollars, None);
    }

    #[tokio::test]
    async fn token_verifier_resolves_an_optional_request_cost_ceiling() {
        let verifier = token_verifier();
        let mut claims = valid_claims();
        claims.max_request_microdollars = Some(42);
        let principal = verifier
            .resolve(&Presented {
                credential: &signed_token(claims),
            })
            .await
            .expect("valid token resolves")
            .expect("valid token returns a principal");
        assert_eq!(principal.max_request_microdollars, Some(42));
    }

    #[tokio::test]
    async fn malformed_request_cost_ceiling_is_a_malformed_token() {
        let verifier = token_verifier();
        let claims = serde_json::json!({
            "exp": unix_now() + 900,
            "iat": unix_now(),
            "jti": "jti-1",
            "aud": "test-audience",
            "ns": "acme",
            "sub": "caller-1",
            "max_request_microdollars": "42"
        });
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some("ed-test".to_owned());
        let token = format!(
            "axt1.{}",
            encode(&header, &claims, &EncodingKey::from_ed_der(ED_PRIVATE_PK8))
                .expect("test token signs")
        );
        assert!(matches!(
            verifier.resolve(&Presented { credential: &token }).await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::Malformed
            ))
        ));
    }

    #[tokio::test]
    async fn token_scope_accepts_oauth_string_and_discards_unknown_capabilities() {
        let verifier = token_verifier();
        let mut claims = valid_claims();
        claims.scope = Some(serde_json::json!("chat unknown models"));
        let principal = verifier
            .resolve(&Presented {
                credential: &signed_token(claims),
            })
            .await
            .expect("valid token resolves")
            .expect("valid token returns a principal");
        let scope = principal.scope.expect("scope is present");
        assert!(scope.contains(&Capability::Chat));
        assert!(scope.contains(&Capability::Models));
        assert_eq!(scope.len(), 2);
    }

    #[tokio::test]
    async fn token_scope_accepts_an_empty_array_as_an_empty_scope() {
        let verifier = token_verifier();
        let mut claims = valid_claims();
        claims.scope = Some(serde_json::json!([]));
        let principal = verifier
            .resolve(&Presented {
                credential: &signed_token(claims),
            })
            .await
            .expect("valid token resolves")
            .expect("valid token returns a principal");
        assert!(principal.scope.expect("scope is present").is_empty());
    }

    #[tokio::test]
    async fn token_verifier_resolves_alias_scope_and_rejects_invalid_claims() {
        let verifier = token_verifier();
        let mut restricted = valid_claims();
        restricted.aliases = Some(serde_json::json!(["gpt-*", "claude-3"]));
        let principal = verifier
            .resolve(&Presented {
                credential: &signed_token(restricted),
            })
            .await
            .expect("restricted token resolves")
            .expect("restricted token returns a principal");
        let scope = principal.alias_scope.as_ref().expect("scope is present");
        assert!(scope.permits("gpt-4o"));
        assert!(scope.permits("claude-3"));
        assert!(!scope.permits("other"));

        let mut invalid = valid_claims();
        invalid.aliases = Some(serde_json::json!(["foo*bar"]));
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(invalid),
                })
                .await,
            Err(PrincipalStoreError::Forbidden(
                TokenVerificationError::InvalidAliasClaim
            ))
        ));

        let mut null = valid_claims();
        null.aliases = Some(Value::Null);
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(null),
                })
                .await,
            Err(PrincipalStoreError::Forbidden(
                TokenVerificationError::InvalidAliasClaim
            ))
        ));
    }

    #[tokio::test]
    async fn token_verifier_reads_ed25519_file_material_with_a_trailing_newline() {
        let path = temp_material(format!("{}\n", BASE64.encode(ED_PUBLIC_RAW)).as_bytes());
        let config = file_verifier_config(&path, "EdDSA");
        let env = HashMap::from([("STATIC_KEY".to_owned(), "static-secret".to_owned())]);
        let verifier = TokenVerifier::build(&config, &env)
            .expect("file verifier builds")
            .expect("verifier is configured");
        let token = signed_token_with(
            "file-test",
            Algorithm::EdDSA,
            &EncodingKey::from_ed_der(ED_PRIVATE_PK8),
            valid_claims_for("platform"),
        );
        assert!(
            verifier
                .resolve(&Presented { credential: &token })
                .await
                .expect("token resolves")
                .is_some()
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn token_verifier_reads_hs256_file_material_as_exact_bytes() {
        let exact = "01234567890123456789012345678901\n";
        let path = temp_material(exact.as_bytes());
        let config = file_verifier_config(&path, "HS256");
        let env = HashMap::from([("STATIC_KEY".to_owned(), "static-secret".to_owned())]);
        let verifier = TokenVerifier::build(&config, &env)
            .expect("file verifier builds")
            .expect("verifier is configured");
        let claims = valid_claims_for("platform");
        let exact_token = signed_token_with(
            "file-test",
            Algorithm::HS256,
            &EncodingKey::from_secret(exact.as_bytes()),
            claims.clone(),
        );
        assert!(
            verifier
                .resolve(&Presented {
                    credential: &exact_token,
                })
                .await
                .expect("exact token resolves")
                .is_some()
        );
        let trimmed_token = signed_token_with(
            "file-test",
            Algorithm::HS256,
            &EncodingKey::from_secret(exact.trim_end().as_bytes()),
            claims,
        );
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &trimmed_token,
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::InvalidSignature
            ))
        ));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn token_verifier_file_rejections_name_kid_and_path_without_material() {
        let cases = [
            ("absent", None, "failed"),
            ("empty", Some(Vec::new()), "is empty"),
            ("utf8", Some(vec![0xff, 0xfe]), "not valid UTF-8"),
            ("base64", Some(b"not base64!".to_vec()), "invalid base64"),
            (
                "length",
                Some(BASE64.encode([0u8; 31]).into_bytes()),
                "32-byte",
            ),
        ];
        for (name, contents, expected) in cases {
            let path = std::env::temp_dir().join(format!(
                "axond-verifier-rejection-{}-{}",
                std::process::id(),
                name
            ));
            let path = path.to_str().unwrap().to_owned();
            if let Some(contents) = contents {
                std::fs::write(&path, contents).unwrap();
            }
            let config = file_verifier_config(&path, "EdDSA");
            let result = TokenVerifier::build(
                &config,
                &HashMap::from([("STATIC_KEY".to_owned(), "static-secret".to_owned())]),
            );
            let Err(error) = result else {
                panic!("file material must be rejected");
            };
            let error = error.to_string();
            assert!(error.contains("file-test"), "{error}");
            assert!(error.contains(&path), "{error}");
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains("not base64!"), "{error}");
            let _ = std::fs::remove_file(path);
        }

        let secret = b"short-secret";
        let path = temp_material(secret);
        let config = file_verifier_config(&path, "HS256");
        let result = TokenVerifier::build(
            &config,
            &HashMap::from([("STATIC_KEY".to_owned(), "static-secret".to_owned())]),
        );
        let Err(error) = result else {
            panic!("short HS256 material must be rejected");
        };
        let error = error.to_string();
        assert!(
            error.contains("file-test") && error.contains(&path),
            "{error}"
        );
        assert!(error.contains("too short"), "{error}");
        assert!(!error.contains("short-secret"), "{error}");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn token_verifier_distinguishes_authentication_and_authorization_failures() {
        let verifier = token_verifier();

        let mut wrong_audience = valid_claims();
        wrong_audience.aud = "other".to_owned();
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(wrong_audience),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::WrongAudience
            ))
        ));

        let mut unknown_namespace = valid_claims();
        unknown_namespace.ns = Some("ghost".to_owned());
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(unknown_namespace),
                })
                .await,
            Err(PrincipalStoreError::Forbidden(
                TokenVerificationError::UnknownNamespace { .. }
            ))
        ));

        let mut missing_namespace = valid_claims();
        missing_namespace.ns = None;
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(missing_namespace),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::MissingClaim { ref claim }
            )) if claim == "ns"
        ));

        let mut empty_namespace = valid_claims();
        empty_namespace.ns = Some(String::new());
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(empty_namespace),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::MissingClaim { ref claim }
            )) if claim == "ns"
        ));

        let mut unpermitted_namespace = valid_claims();
        unpermitted_namespace.ns = Some("platform".to_owned());
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(unpermitted_namespace),
                })
                .await,
            Err(PrincipalStoreError::Forbidden(
                TokenVerificationError::SignerNotPermitted { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn token_verifier_rejects_an_empty_subject() {
        let verifier = token_verifier();
        let mut empty_subject = valid_claims();
        empty_subject.sub = Some(String::new());
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(empty_subject),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::MissingClaim { ref claim }
            )) if claim == "sub"
        ));
    }

    #[tokio::test]
    async fn token_verifier_requires_jti_and_enforces_max_ttl() {
        let verifier = token_verifier();
        let mut missing_jti = valid_claims();
        missing_jti.jti = None;
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(missing_jti),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::MissingClaim { ref claim }
            )) if claim == "jti"
        ));

        let mut too_long = valid_claims();
        too_long.exp = too_long.iat.map(|iat| iat + 901);
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(too_long),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::InvalidLifetime { .. }
            ))
        ));

        let future_iat = unix_now() + 10;
        let mut future_issued = valid_claims();
        future_issued.iat = Some(future_iat);
        future_issued.exp = Some(future_iat + 900);
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(future_issued),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::InvalidLifetime { .. }
            ))
        ));

        let skewed_iat = unix_now() + 2;
        let mut skewed_window = valid_claims();
        skewed_window.iat = Some(skewed_iat);
        skewed_window.exp = Some(skewed_iat + 900);
        assert!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(skewed_window),
                })
                .await
                .expect("clock-skewed token resolves")
                .is_some()
        );

        let far_future_iat = unix_now() + 10 * 365 * 24 * 60 * 60;
        let mut far_future_exp = valid_claims();
        far_future_exp.iat = Some(far_future_iat);
        far_future_exp.exp = Some(far_future_iat + 900);
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(far_future_exp),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::InvalidLifetime { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn token_verifier_rejects_a_future_not_before_claim() {
        let verifier = token_verifier();
        let mut future_nbf = valid_claims();
        future_nbf.nbf = Some(unix_now() + 60);
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(future_nbf),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::NotYetValid
            ))
        ));
    }

    /// Namespace epochs reject older issuance times, preserve the exact
    /// boundary, and let a subject-specific entry override the namespace-wide
    /// policy.
    #[tokio::test]
    async fn token_verifier_enforces_most_specific_issuance_epochs() {
        let now = unix_now();
        let namespace_epoch = now - 100;
        let subject_epoch = now - 200;
        let config = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "STATIC_KEY"
namespace = "platform"

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "ed-test"
alg = "EdDSA"
env = "ED_PUBLIC"
namespaces = ["platform"]
max_ttl = "15m"

[[gateway_token_epoch]]
namespace = "platform"
min_iat = {namespace_epoch}

[[gateway_token_epoch]]
namespace = "platform"
subject = "spared"
min_iat = {subject_epoch}
"#,
        ))
        .expect("epoch verifier config");
        let env = HashMap::from([
            ("STATIC_KEY".to_owned(), "static-secret".to_owned()),
            ("ED_PUBLIC".to_owned(), BASE64.encode(ED_PUBLIC_RAW)),
        ]);
        let verifier = TokenVerifier::build(&config, &env)
            .expect("test verifier builds")
            .expect("verifier is configured");

        let mut rejected = valid_claims_for("platform");
        rejected.iat = Some(namespace_epoch - 1);
        rejected.exp = Some(namespace_epoch - 1 + 899);
        assert!(matches!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(rejected),
                })
                .await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::IssuedBeforeEpoch { .. }
            ))
        ));

        let mut boundary = valid_claims_for("platform");
        boundary.iat = Some(namespace_epoch);
        boundary.exp = Some(namespace_epoch + 900);
        assert!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(boundary),
                })
                .await
                .expect("boundary token resolves")
                .is_some()
        );

        let mut spared = valid_claims_for("platform");
        spared.sub = Some("spared".to_owned());
        spared.iat = Some(subject_epoch + 1);
        spared.exp = Some(subject_epoch + 1 + 900);
        assert!(
            verifier
                .resolve(&Presented {
                    credential: &signed_token(spared),
                })
                .await
                .expect("subject override resolves")
                .is_some()
        );
    }

    #[test]
    fn token_verifier_rejects_a_short_hs256_secret_at_build() {
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "STATIC_KEY"
namespace = "platform"

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "hs-test"
alg = "HS256"
env = "HS_SECRET"
namespaces = ["platform"]
max_ttl = "15m"
"#,
        )
        .expect("test verifier config");
        let env = HashMap::from([
            ("STATIC_KEY".to_owned(), "static-secret".to_owned()),
            ("HS_SECRET".to_owned(), "short".to_owned()),
        ]);
        assert!(matches!(
            TokenVerifier::build(&config, &env),
            Err(TokenVerifierBuildError::WeakHs256Secret { ref kid }) if kid == "hs-test"
        ));
    }
}
