//! OIDC/JWKS verification for human administration.
//!
//! This verifier is deliberately narrower than a general-purpose OIDC client:
//! the operator configures the exact issuer, audience, and JWKS endpoint, and
//! the gateway accepts only signed bearer tokens with an issuer-scoped subject.
//! Discovery is not performed from token data, symmetric JWKS keys are refused,
//! and a provider response is cached for a short bounded interval so an IdP is
//! not placed on every administrative request's critical path.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use jsonwebtoken::jwk::{JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use reqwest::header::ACCEPT;
use serde::Deserialize;
use tokio::sync::Mutex;

use super::auth::{AdminAuthError, AdminIdentity};
use crate::config::AdminOidc;

const KEY_CACHE_TTL: Duration = Duration::from_secs(300);
const TOKEN_CLOCK_SKEW_SECONDS: u64 = 5;
const MAX_TOKEN_BYTES: usize = 64 * 1024;
const MAX_JWKS_BYTES: usize = 1024 * 1024;

/// Why the OIDC verifier could not be constructed from validated bootstrap
/// configuration.
#[derive(Debug, thiserror::Error)]
pub enum OidcBootError {
    #[error("OIDC issuer or JWKS endpoint is not an absolute URL: {0}")]
    Url(String),
    #[error("OIDC HTTP client could not be built: {0}")]
    Client(#[source] reqwest::Error),
}

#[derive(Clone)]
struct OidcKey {
    algorithm: Algorithm,
    key: DecodingKey,
}

#[derive(Default)]
struct KeyCache {
    fetched_at: Option<Instant>,
    forced_at: Option<Instant>,
    keys: HashMap<String, OidcKey>,
}

/// A verifier for one configured OIDC issuer.
pub struct OidcVerifier {
    issuer: String,
    audience: String,
    jwks_url: String,
    client: reqwest::Client,
    cache: Mutex<KeyCache>,
}

impl OidcVerifier {
    pub fn new(config: &AdminOidc) -> Result<Self, OidcBootError> {
        let issuer = reqwest::Url::parse(config.issuer.trim())
            .map_err(|error| OidcBootError::Url(format!("issuer: {error}")))?;
        let jwks_url = reqwest::Url::parse(config.jwks_url.trim())
            .map_err(|error| OidcBootError::Url(format!("jwks_url: {error}")))?;
        if issuer.scheme() != "https" && issuer.scheme() != "http" {
            return Err(OidcBootError::Url(
                "issuer must use https or http for a local qualification endpoint".to_owned(),
            ));
        }
        if jwks_url.scheme() != "https" && jwks_url.scheme() != "http" {
            return Err(OidcBootError::Url(
                "jwks_url must use https or http for a local qualification endpoint".to_owned(),
            ));
        }
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(5))
            // A configured endpoint is an explicit trust boundary. Following
            // a redirect would let the provider move that boundary after boot.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(OidcBootError::Client)?;
        Ok(Self {
            issuer: config.issuer.trim().to_owned(),
            audience: config.audience.trim().to_owned(),
            jwks_url: jwks_url.to_string(),
            client,
            cache: Mutex::new(KeyCache::default()),
        })
    }

    /// Verify a bearer token and return only the identity the admin domain
    /// understands. Claims other than the issuer-scoped subject never leave the
    /// verifier, so handlers cannot accidentally authorize on an unreviewed
    /// provider-specific claim.
    pub async fn authenticate(&self, token: &str) -> Result<AdminIdentity, AdminAuthError> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(AdminAuthError::TokenRejected);
        }
        let header = decode_header(token).map_err(|_| AdminAuthError::TokenRejected)?;
        let kid = header
            .kid
            .filter(|kid| !kid.trim().is_empty())
            .ok_or(AdminAuthError::TokenRejected)?;
        if !supported_algorithm(header.alg) {
            return Err(AdminAuthError::TokenRejected);
        }

        let mut key = self.key(&kid, false).await?;
        if key.is_none() {
            return Err(AdminAuthError::TokenRejected);
        }
        let first = key.take().expect("checked above");
        match self.verify(token, &kid, &first) {
            Ok(identity) => Ok(identity),
            Err(VerifyFailure::Signature) => {
                // Permit a bounded same-kid rotation refresh. A bad token cannot
                // turn every request into an unbounded JWKS fetch.
                let Some(rotated) = self.key(&kid, true).await? else {
                    return Err(AdminAuthError::TokenRejected);
                };
                self.verify(token, &kid, &rotated)
                    .map_err(|_| AdminAuthError::TokenRejected)
            }
            Err(VerifyFailure::Rejected) => Err(AdminAuthError::TokenRejected),
        }
    }

    async fn key(&self, kid: &str, force: bool) -> Result<Option<OidcKey>, AdminAuthError> {
        let mut cache = self.cache.lock().await;
        let fresh = cache
            .fetched_at
            .is_some_and(|fetched| fetched.elapsed() < KEY_CACHE_TTL);
        let forced_recent = cache
            .forced_at
            .is_some_and(|forced| forced.elapsed() < KEY_CACHE_TTL);
        if fresh && (!force || forced_recent) {
            return Ok(cache.keys.get(kid).cloned());
        }

        let keys = self.fetch_keys().await?;
        cache.keys = keys;
        cache.fetched_at = Some(Instant::now());
        if force {
            cache.forced_at = Some(Instant::now());
        }
        Ok(cache.keys.get(kid).cloned())
    }

    async fn fetch_keys(&self) -> Result<HashMap<String, OidcKey>, AdminAuthError> {
        let response = self
            .client
            .get(&self.jwks_url)
            .header(ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| AdminAuthError::IdentityProviderUnavailable)?;
        if !response.status().is_success() {
            return Err(AdminAuthError::IdentityProviderUnavailable);
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_JWKS_BYTES as u64)
        {
            return Err(AdminAuthError::IdentityProviderUnavailable);
        }
        let body = response
            .bytes()
            .await
            .map_err(|_| AdminAuthError::IdentityProviderUnavailable)?;
        if body.len() > MAX_JWKS_BYTES {
            return Err(AdminAuthError::IdentityProviderUnavailable);
        }
        let set: JwkSet = serde_json::from_slice(&body)
            .map_err(|_| AdminAuthError::IdentityProviderUnavailable)?;
        parse_keys(set).map_err(|_| AdminAuthError::IdentityProviderUnavailable)
    }

    fn verify(
        &self,
        token: &str,
        kid: &str,
        key: &OidcKey,
    ) -> Result<AdminIdentity, VerifyFailure> {
        let header = decode_header(token).map_err(|_| VerifyFailure::Rejected)?;
        if header.kid.as_deref() != Some(kid) || header.alg != key.algorithm {
            return Err(VerifyFailure::Rejected);
        }
        let mut validation = Validation::new(key.algorithm);
        validation.leeway = TOKEN_CLOCK_SKEW_SECONDS;
        validation.validate_nbf = true;
        validation.set_issuer(std::slice::from_ref(&self.issuer));
        validation.set_audience(std::slice::from_ref(&self.audience));
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let data = decode::<OidcClaims>(token, &key.key, &validation).map_err(|error| {
            if matches!(
                error.kind(),
                jsonwebtoken::errors::ErrorKind::InvalidSignature
            ) {
                VerifyFailure::Signature
            } else {
                VerifyFailure::Rejected
            }
        })?;
        if data.claims.sub.trim().is_empty() || data.claims.sub.len() > 512 {
            return Err(VerifyFailure::Rejected);
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if data.claims.iat > now.saturating_add(TOKEN_CLOCK_SKEW_SECONDS) {
            return Err(VerifyFailure::Rejected);
        }
        Ok(AdminIdentity::Human {
            issuer: self.issuer.clone(),
            subject: data.claims.sub,
        })
    }
}

#[derive(Debug, Deserialize)]
struct OidcClaims {
    sub: String,
    iat: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VerifyFailure {
    Signature,
    Rejected,
}

fn supported_algorithm(algorithm: Algorithm) -> bool {
    matches!(
        algorithm,
        Algorithm::RS256
            | Algorithm::RS384
            | Algorithm::RS512
            | Algorithm::ES256
            | Algorithm::ES384
            | Algorithm::EdDSA
    )
}

fn parse_keys(set: JwkSet) -> Result<HashMap<String, OidcKey>, ()> {
    let mut keys = HashMap::new();
    for jwk in set.keys {
        if jwk.common.public_key_use == Some(PublicKeyUse::Encryption) {
            continue;
        }
        if jwk
            .common
            .key_operations
            .as_ref()
            .is_some_and(|operations| !operations.contains(&KeyOperations::Verify))
        {
            continue;
        }
        let Some(kid) = jwk
            .common
            .key_id
            .as_deref()
            .map(str::trim)
            .filter(|kid| !kid.is_empty())
        else {
            continue;
        };
        let Some(algorithm) = jwk.common.key_algorithm.and_then(jwk_algorithm) else {
            continue;
        };
        if !supported_algorithm(algorithm) {
            continue;
        }
        // The JWK algorithm is checked against the JWT header again at verify
        // time; storing it here prevents an algorithm switch from reusing a key.
        let key = DecodingKey::from_jwk(&jwk).map_err(|_| ())?;
        if keys
            .insert(kid.to_owned(), OidcKey { algorithm, key })
            .is_some()
        {
            return Err(());
        }
    }
    if keys.is_empty() {
        return Err(());
    }
    Ok(keys)
}

fn jwk_algorithm(algorithm: KeyAlgorithm) -> Option<Algorithm> {
    Some(match algorithm {
        KeyAlgorithm::RS256 => Algorithm::RS256,
        KeyAlgorithm::RS384 => Algorithm::RS384,
        KeyAlgorithm::RS512 => Algorithm::RS512,
        KeyAlgorithm::ES256 => Algorithm::ES256,
        KeyAlgorithm::ES384 => Algorithm::ES384,
        KeyAlgorithm::EdDSA => Algorithm::EdDSA,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use jsonwebtoken::{EncodingKey, Header, encode};
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use serde_json::json;

    fn key_pair() -> (Vec<u8>, Ed25519KeyPair, String) {
        let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("key pair");
        let pkcs8 = document.as_ref().to_vec();
        let pair = Ed25519KeyPair::from_pkcs8(&pkcs8).expect("pkcs8");
        let public = URL_SAFE_NO_PAD.encode(pair.public_key().as_ref());
        (pkcs8, pair, public)
    }

    fn token(pkcs8: &[u8], issuer: &str, audience: &str, subject: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        let claims = json!({
            "iss": issuer,
            "aud": audience,
            "sub": subject,
            "iat": now,
            "exp": now + 300,
        });
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some("admin-1".to_owned());
        encode(&header, &claims, &EncodingKey::from_ed_der(pkcs8)).expect("token")
    }

    #[test]
    fn jwks_parser_refuses_symmetric_and_encryption_keys() {
        let set: JwkSet = serde_json::from_value(json!({
            "keys": [
                {"kty":"oct","kid":"symmetric","alg":"HS256","k":"c2VjcmV0"},
                {"kty":"OKP","crv":"Ed25519","kid":"enc","alg":"EdDSA","use":"enc","x":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}
            ]
        }))
        .expect("jwks");
        assert!(parse_keys(set).is_err());
    }

    #[test]
    fn supported_algorithms_exclude_hmac_and_legacy_rsa() {
        assert!(supported_algorithm(Algorithm::EdDSA));
        assert!(supported_algorithm(Algorithm::RS256));
        assert!(!supported_algorithm(Algorithm::HS256));
        assert!(!supported_algorithm(Algorithm::PS256));
    }

    #[test]
    fn generated_ed25519_token_has_the_expected_shape() {
        let (pkcs8, _pair, public) = key_pair();
        let issuer = "https://idp.example";
        let token = token(&pkcs8, issuer, "axond", "alice");
        assert!(token.split('.').count() == 3);
        assert_eq!(public.len(), 43);
    }
}
