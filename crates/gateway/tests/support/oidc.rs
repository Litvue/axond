//! A local OIDC signing endpoint for black-box admin authentication tests.
//!
//! The gateway is given an explicit issuer and JWKS URL, so this fixture does
//! not need discovery or a TLS certificate. It serves only the public Ed25519
//! JWK; the private PKCS#8 bytes stay in the test process and are used to mint
//! short-lived tokens for the named subject.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::State;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;
use tokio::task::JoinHandle;

const KEY_ID: &str = "integration-admin-1";
const AUDIENCE: &str = "axond-admin";

#[derive(Clone)]
struct JwksState {
    body: Arc<String>,
}

async fn jwks(State(state): State<JwksState>) -> impl IntoResponse {
    ([(CONTENT_TYPE, "application/json")], (*state.body).clone())
}

/// A deterministic-in-shape, process-local OIDC issuer.
pub struct OidcProvider {
    address: std::net::SocketAddr,
    pkcs8: Vec<u8>,
    task: JoinHandle<()>,
}

impl OidcProvider {
    pub async fn start() -> Self {
        let document =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).expect("OIDC signing key");
        let pkcs8 = document.as_ref().to_vec();
        let pair = Ed25519KeyPair::from_pkcs8(&pkcs8).expect("OIDC PKCS#8 document");
        let x = URL_SAFE_NO_PAD.encode(pair.public_key().as_ref());
        let body = Arc::new(
            json!({
                "keys": [{
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "kid": KEY_ID,
                    "alg": "EdDSA",
                    "use": "sig",
                    "x": x,
                }]
            })
            .to_string(),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("OIDC fixture listener");
        let address = listener.local_addr().expect("OIDC fixture address");
        let app = Router::new()
            .route("/jwks", get(jwks))
            .with_state(JwksState { body });
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self {
            address,
            pkcs8,
            task,
        }
    }

    pub fn issuer(&self) -> String {
        format!("http://{}/issuer", self.address)
    }

    pub fn jwks_url(&self) -> String {
        format!("http://{}/jwks", self.address)
    }

    pub fn audience(&self) -> &'static str {
        AUDIENCE
    }

    pub fn token(&self, subject: &str) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after the epoch")
            .as_secs();
        let claims = json!({
            "iss": self.issuer(),
            "aud": AUDIENCE,
            "sub": subject,
            "iat": now,
            "exp": now + 300,
        });
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(KEY_ID.to_owned());
        encode(&header, &claims, &EncodingKey::from_ed_der(&self.pkcs8))
            .expect("OIDC integration token")
    }
}

impl Drop for OidcProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}
