//! A private seam that lets the out-of-tree fuzz project drive the untrusted
//! parsers this binary runs on its request and boot paths.
//!
//! Axond ships as a binary, so its modules have no library target a fuzz
//! harness could link against. This file is a second target over the very same
//! module sources (`[lib]` in `Cargo.toml`), compiled only when the
//! `fuzzing` feature is on: a default build, the published `.crate`, and every
//! consumer see an empty library, so nothing here widens the published API. The
//! feature is not part of the compatibility contract and must not be enabled by
//! anything but [`fuzz/`](https://github.com/Litvue/axond/tree/main/fuzz).
//!
//! Every entry point below returns a typed, owned outcome rather than an
//! internal type, for two reasons: the fuzz project asserts on the *shape* of a
//! rejection (a recoverable error, never a panic or an abort), and the internal
//! types stay free to change without a fuzz-side edit.
//!
//! No entry point performs I/O, reads the environment, or touches a real
//! secret: the verifier material below is committed synthetic test material.
#![cfg(fuzzing)]
// Only the handful of items the seams below reach are live in this target; the
// rest of the crate is compiled for its `crate::` paths.
#![allow(dead_code)]

// Keep this list identical to `main.rs`. `tests/fuzz_seam.rs` fails if it drifts.
mod admission;
mod aliases;
mod backends;
mod budget;
mod config;
mod convergence;
mod credentials;
mod desired_state;
mod error;
mod key_material;
mod mint;
mod principals;
mod rate_limit;
mod redis_support;
mod reload;
mod revocation;
mod routes;
mod shutdown;
mod state;
mod status;
mod streaming;
// The layer re-export this module makes for `main.rs` has no consumer here.
#[allow(unused_imports)]
mod telemetry;
mod usage;

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use crate::config::Config;
use crate::mint::{MintAlgorithm, MintRequest};
use crate::principals::{
    Presented, PrincipalStore, PrincipalStoreError, TokenVerificationError, TokenVerifier,
};

/// How a parser refused an input: the variant carries the class of failure, the
/// string carries the operator-facing message the process would have logged.
///
/// Presence of a variant is the fuzz assertion — a refusal is a value, so it
/// cannot have unwound, aborted, or exited.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    /// The input was not loadable at all (TOML syntax, wrong types, unknown keys).
    Load(String),
    /// The input parsed but failed a whole-graph invariant.
    Invalid(String),
    /// The input was refused as a bad request, the way a caller would see it.
    BadRequest(String),
    /// Authentication failed. The payload is the stable error code.
    Unauthenticated(&'static str),
    /// Authentication succeeded and authorization failed. Stable error code.
    Unauthorized(&'static str),
    /// A store the check needs was unavailable. Unreachable through this seam,
    /// which is stateless, and mapped rather than asserted away.
    Unavailable,
}

/// What a config the fuzzer produced turned into, without exposing [`Config`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigShape {
    pub namespaces: usize,
    pub providers: usize,
    pub models: usize,
    pub credentials: usize,
    pub gateway_keys: usize,
    pub verifiers: usize,
}

/// Parse and validate untrusted configuration text, the way `axond` does at
/// boot and on reload.
///
/// # Errors
///
/// [`Rejection::Load`] for text the loader refuses, [`Rejection::Invalid`] for
/// a config that loads but fails validation.
pub fn config_from_toml_str(input: &str) -> Result<ConfigShape, Rejection> {
    match Config::from_toml_str(input) {
        Ok(config) => Ok(ConfigShape {
            namespaces: config.namespace.len(),
            providers: config.provider.len(),
            models: config.model.len(),
            credentials: config.credential.len(),
            gateway_keys: config.gateway_key.len(),
            verifiers: config.gateway_verifier.len(),
        }),
        Err(config::ConfigError::Load(message)) => Err(Rejection::Load(message)),
        Err(config::ConfigError::Invalid(message)) => Err(Rejection::Invalid(message)),
    }
}

/// Parse the `namespaces` filter out of an untrusted `GET
/// /v1/credentials/status` query string, percent-decoding included.
///
/// `None` means the caller sent no `namespaces` parameter; `Some` carries the
/// decoded value, which may be empty.
///
/// # Errors
///
/// [`Rejection::BadRequest`] for a duplicate parameter or an undecodable value.
pub fn credentials_query_namespaces(raw_query: Option<&str>) -> Result<Option<String>, Rejection> {
    routes::fuzz_parse_credential_query(raw_query)
        .map_err(|error| Rejection::BadRequest(error.to_string()))
}

/// What a token that verified carried, without exposing `InboundKey`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedToken {
    pub namespace: String,
    pub subject: String,
    pub capabilities: usize,
    pub scoped_aliases: bool,
    pub max_request_microdollars: Option<u64>,
}

/// Verify an untrusted `axt1.` credential against the seam's synthetic
/// verifiers: one HS256 signer, one EdDSA signer, two namespaces, and an
/// issuance epoch, so signature, algorithm, audience, lifetime, namespace,
/// scope, and epoch checks are all reachable.
///
/// # Errors
///
/// [`Rejection::Unauthenticated`] or [`Rejection::Unauthorized`], carrying the
/// same stable code the gateway would answer with.
pub fn verify_token(credential: &str) -> Result<Option<VerifiedToken>, Rejection> {
    let presented = Presented { credential };
    let resolved = futures::executor::block_on(verifier().resolve(&presented));
    match resolved {
        Ok(None) => Ok(None),
        Ok(Some(key)) => Ok(Some(VerifiedToken {
            namespace: key.namespace,
            subject: key.subject,
            capabilities: key.scope.map_or(0, |scope| scope.len()),
            scoped_aliases: key.alias_scope.is_some(),
            max_request_microdollars: key.max_request_microdollars,
        })),
        Err(PrincipalStoreError::Unauthorized(error)) => {
            Err(Rejection::Unauthenticated(code(&error)))
        }
        Err(PrincipalStoreError::Forbidden(error)) => Err(Rejection::Unauthorized(code(&error))),
        Err(PrincipalStoreError::Unavailable) => Err(Rejection::Unavailable),
    }
}

/// Mint an `axt1.` credential with the seam's synthetic HS256 signer so a
/// fuzzer can reach the claim checks that sit past signature verification.
///
/// Returns `None` when the requested claims cannot be encoded at all, which is
/// a rejection of the fuzzer's request rather than a finding.
pub fn mint_hs256_token(
    namespace: &str,
    subject: &str,
    audience: &str,
    ttl_seconds: u64,
    issued_at: Option<u64>,
    scope: Option<Vec<String>>,
    aliases: Option<Vec<String>>,
) -> Option<String> {
    let scope = scope.map(|values| {
        values
            .iter()
            .filter_map(|value| principals::Capability::parse(value))
            .collect()
    });
    mint::mint_token_at(
        MintRequest {
            kid: HS256_KID,
            algorithm: MintAlgorithm::Hs256,
            key_material: HS256_MATERIAL,
            namespace,
            subject,
            audience,
            ttl: Duration::from_secs(ttl_seconds),
            aliases,
            max_request_microdollars: None,
            scope,
        },
        issued_at,
    )
    .ok()
    .map(|minted| minted.token)
}

/// The audience the seam's verifiers accept, so a fuzzer can aim at the
/// audience check from either side.
pub const AUDIENCE: &str = "fuzz.axond.invalid";

/// The namespaces the seam's config defines. `denied` is deliberately outside
/// the HS256 signer's permitted set.
pub const NAMESPACES: [&str; 2] = ["fuzz", "denied"];

/// How many capabilities the scope vocabulary defines, which bounds what any
/// token can present however its `scope` claim is shaped.
pub const CAPABILITY_COUNT: usize = principals::Capability::ALL.len();

/// The `kid` of the seam's HS256 signer.
pub const HS256_KID: &str = "fuzz-hs256";

/// The `kid` of the seam's EdDSA verifier, whose private half does not exist.
pub const EDDSA_KID: &str = "fuzz-eddsa";

/// Synthetic HS256 material. Not a secret: it is committed, published in the
/// fuzz corpus, and accepted by nothing but this seam.
const HS256_MATERIAL: &str = "axond-fuzz-hs256-material-not-a-secret";

/// A synthetic 32-byte Ed25519 public key. There is no matching private key in
/// this repository, so every EdDSA signature the fuzzer produces is invalid by
/// construction — which is the point: it keeps the signature-failure path hot.
const EDDSA_PUBLIC_BASE64: &str = "ZnV6ei1heG9uZC1lZDI1NTE5LXB1YmxpYy1rZXktMzI=";

const CONFIG: &str = r#"
[[namespace]]
id = "fuzz"
default = true

[[namespace]]
id = "denied"

[[gateway_key]]
env = "AXOND_FUZZ_STATIC_KEY"
namespace = "fuzz"

[gateway_token]
audience = "fuzz.axond.invalid"

[[gateway_verifier]]
kid = "fuzz-hs256"
alg = "HS256"
env = "AXOND_FUZZ_HS256"
namespaces = ["fuzz"]
max_ttl = "15m"

[[gateway_verifier]]
kid = "fuzz-eddsa"
alg = "EdDSA"
env = "AXOND_FUZZ_EDDSA"
namespaces = ["fuzz", "denied"]
max_ttl = "15m"

[[gateway_token_epoch]]
namespace = "denied"
min_iat = "2026-01-01T00:00:00Z"
"#;

fn verifier() -> &'static TokenVerifier {
    static VERIFIER: OnceLock<TokenVerifier> = OnceLock::new();
    VERIFIER.get_or_init(|| {
        let config = Config::from_toml_str(CONFIG).expect("the seam's own config is valid");
        let env = HashMap::from([
            (
                "AXOND_FUZZ_STATIC_KEY".to_owned(),
                "axond-fuzz-static-key-not-a-secret".to_owned(),
            ),
            ("AXOND_FUZZ_HS256".to_owned(), HS256_MATERIAL.to_owned()),
            (
                "AXOND_FUZZ_EDDSA".to_owned(),
                EDDSA_PUBLIC_BASE64.to_owned(),
            ),
        ]);
        TokenVerifier::build(&config, &env)
            .expect("the seam's own verifiers build")
            .expect("the seam configures verifiers")
    })
}

fn code(error: &TokenVerificationError) -> &'static str {
    error.code()
}
