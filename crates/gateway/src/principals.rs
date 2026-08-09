use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header, errors::ErrorKind as JwtErrorKind,
};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

use crate::config::{Config, GatewayVerifierAlgorithm};

#[derive(Clone)]
pub struct InboundKey {
    pub namespace: String,
    pub subject: String,
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
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TokenVerifierBuildError {
    #[error("gateway verifiers require `[gateway_token] audience`")]
    MissingAudience,
    #[error("gateway_verifier `{kid}` references env var `{env}`, which is unset or empty")]
    MissingKey { kid: String, env: String },
    #[error("gateway_verifier `{kid}` has invalid base64 key material")]
    InvalidBase64 { kid: String },
    #[error("gateway_verifier `{kid}` must contain a 32-byte Ed25519 public key")]
    InvalidEd25519Key { kid: String },
}

struct ResolvedVerifier {
    kid: String,
    algorithm: Algorithm,
    namespaces: HashSet<String>,
    max_ttl: Duration,
    key: DecodingKey,
}

pub struct TokenVerifier {
    audience: String,
    namespaces: HashSet<String>,
    verifiers: Vec<ResolvedVerifier>,
}

#[derive(Debug, Deserialize)]
struct TokenClaims {
    exp: Option<u64>,
    iat: Option<u64>,
    jti: Option<String>,
    ns: Option<String>,
    sub: Option<String>,
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
        let mut verifiers = Vec::with_capacity(config.gateway_verifier.len());
        for verifier in &config.gateway_verifier {
            let value = env
                .get(&verifier.env)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| TokenVerifierBuildError::MissingKey {
                    kid: verifier.kid.clone(),
                    env: verifier.env.clone(),
                })?;
            let key = match verifier.alg {
                // HS256 material lives inside DecodingKey beyond our
                // zeroization control; Ed25519 is the preferred default.
                GatewayVerifierAlgorithm::Hs256 => DecodingKey::from_secret(value.as_bytes()),
                GatewayVerifierAlgorithm::EdDsa => {
                    let decoded = BASE64.decode(value).map_err(|_| {
                        TokenVerifierBuildError::InvalidBase64 {
                            kid: verifier.kid.clone(),
                        }
                    })?;
                    if decoded.len() != 32 {
                        return Err(TokenVerifierBuildError::InvalidEd25519Key {
                            kid: verifier.kid.clone(),
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
            });
        }
        Ok(Some(Self {
            audience,
            namespaces,
            verifiers,
        }))
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
        validation.leeway = 5;
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
        if exp < iat || exp - iat > verifier.max_ttl.as_secs() {
            return Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::InvalidLifetime {
                    kid: verifier.kid.clone(),
                },
            ));
        }
        let namespace = claims.ns.ok_or(PrincipalStoreError::Unauthorized(
            TokenVerificationError::MissingClaim {
                claim: "ns".to_owned(),
            },
        ))?;
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
        let subject = claims.sub.ok_or(PrincipalStoreError::Unauthorized(
            TokenVerificationError::MissingClaim {
                claim: "sub".to_owned(),
            },
        ))?;
        Ok(Some(InboundKey { namespace, subject }))
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

    #[derive(Serialize)]
    struct TestClaims {
        exp: Option<u64>,
        iat: Option<u64>,
        nbf: Option<u64>,
        aud: String,
        jti: Option<String>,
        ns: Option<String>,
        sub: Option<String>,
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

    fn signed_token(claims: TestClaims) -> String {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some("ed-test".to_owned());
        format!(
            "axt1.{}",
            encode(&header, &claims, &EncodingKey::from_ed_der(ED_PRIVATE_PK8))
                .expect("test token signs")
        )
    }

    fn valid_claims() -> TestClaims {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_secs();
        TestClaims {
            exp: Some(now + 600),
            iat: Some(now),
            nbf: None,
            aud: "test-audience".to_owned(),
            jti: Some("jti-1".to_owned()),
            ns: Some("acme".to_owned()),
            sub: Some("caller-1".to_owned()),
        }
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
    }
}
