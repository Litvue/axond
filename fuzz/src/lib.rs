//! The bodies of Axond's fuzz targets, plus the invariants they assert.
//!
//! The `fuzz_targets/*.rs` files are thin `fuzz_target!` shells around the
//! functions here, and `src/bin/smoke.rs` replays the committed seed corpora
//! through the same functions on a stable toolchain. Both drivers therefore
//! check identical properties, and a scheduled finding reproduces from a seed
//! file without a nightly compiler.
//!
//! Every target consumes untrusted bytes and asserts three things:
//!
//! 1. the parser returns — a panic, an abort, or a hang is the finding;
//! 2. a refusal is a *typed* value the gateway could answer with, never a
//!    stringly-typed surprise;
//! 3. what the parser accepts stays inside the bounds the gateway relies on
//!    (decoded lengths, namespaces, subjects, signer authority).
//!
//! Each body also returns the *class* of outcome it produced. libFuzzer ignores
//! it; the smoke aggregates it, so a seam that silently began refusing every
//! input at the door — the failure mode that makes a green fuzz lane worthless —
//! fails the lane instead of passing it.
//!
//! Nothing here opens a socket, reads a file, or looks at the environment: the
//! seam in `axond` is built from a config compiled into the binary with
//! synthetic key material, so a fuzz run is hermetic and holds no real secret.

use arbitrary::Arbitrary;
use axond::{Rejection, VerifiedToken};

/// A refusal must carry an operator-facing reason. An empty one would reach a
/// log or a response body as a blank message.
///
/// Returns the outcome class: a token's own stable code, or the variant name.
fn assert_typed(rejection: &Rejection) -> &'static str {
    match rejection {
        Rejection::Load(message) => {
            assert!(!message.is_empty(), "typed rejection carries no message");
            "load"
        }
        Rejection::Invalid(message) => {
            assert!(!message.is_empty(), "typed rejection carries no message");
            "invalid"
        }
        Rejection::BadRequest(message) => {
            assert!(!message.is_empty(), "typed rejection carries no message");
            "bad_request"
        }
        Rejection::Unauthenticated(code) | Rejection::Unauthorized(code) => {
            assert!(
                code.starts_with("token_"),
                "unexpected authentication code {code:?}"
            );
            code
        }
        // The seam resolves no store, so this is unreachable by construction.
        Rejection::Unavailable => panic!("the stateless seam reported a store as unavailable"),
    }
}

/// Untrusted configuration text: what an operator's file, a mounted ConfigMap,
/// or a reload of either hands the loader.
pub fn config_toml(data: &[u8]) -> &'static str {
    let Ok(text) = str::from_utf8(data) else {
        return "not_utf8";
    };
    let first = axond::config_from_toml_str(text);
    let outcome = match &first {
        Ok(shape) => {
            // A config that validated is a config the process would serve, so
            // its own invariants have to hold on the way out — and which
            // invariants those are is the mode's decision (ADR 0027).
            if shape.stateful {
                // The control plane owns every resource in stateful mode, so a
                // file that declared one must have been refused. An accepted
                // config that still carries one would mean two authorities
                // disagree about the same resource at boot.
                assert_eq!(
                    (
                        shape.namespaces,
                        shape.providers,
                        shape.models,
                        shape.credentials,
                        shape.gateway_keys,
                        shape.verifiers
                    ),
                    (0, 0, 0, 0, 0, 0),
                    "a stateful config was accepted with control-plane-owned sections: {shape:?}"
                );
                "accepted_stateful"
            } else {
                // Stateless mode resolves everything from the file, so it must
                // name the namespace a request resolves into, and a verifier or
                // a credential is only meaningful scoped to one.
                assert!(
                    shape.namespaces >= 1,
                    "an accepted stateless config defines no namespace"
                );
                "accepted"
            }
        }
        Err(rejection) => assert_typed(rejection),
    };
    // Loading is a pure function of the text: boot and every later reload of an
    // unchanged file must agree, or a replica could serve a config its peer
    // refused.
    assert_eq!(
        first.is_ok(),
        axond::config_from_toml_str(text).is_ok(),
        "the same configuration text was accepted and refused"
    );
    outcome
}

/// An untrusted `GET /v1/credentials/status?...` query string: malformed
/// percent-encoding, duplicate keys, empty values, and oversized inputs.
pub fn credentials_query(data: &[u8]) -> &'static str {
    let Ok(text) = str::from_utf8(data) else {
        return "not_utf8";
    };
    let outcome = axond::credentials_query_namespaces(Some(text));
    let class = match &outcome {
        Ok(Some(value)) => {
            // Percent-decoding only ever shrinks, so an accepted value cannot
            // be an amplification vector: three bytes in, one byte out.
            assert!(
                value.len() <= text.len(),
                "decoding expanded a {}-byte query into {} bytes",
                text.len(),
                value.len()
            );
            // A `%` can only survive decoding as an escaped one (`%25`), which
            // shrinks; a bare one is a rejection.
            assert!(
                value.matches('%').count() <= text.matches('%').count(),
                "decoding invented percent signs: {value:?} from {text:?}"
            );
            if value.is_empty() {
                "accepted_empty"
            } else {
                "accepted"
            }
        }
        Ok(None) => "absent",
        Err(rejection) => assert_typed(rejection),
    };
    assert_eq!(
        outcome.is_ok(),
        axond::credentials_query_namespaces(Some(text)).is_ok(),
        "the same query string was accepted and refused"
    );
    // A query the router never received must parse like an absent filter, and
    // an empty one must not be confused with it.
    assert_eq!(
        axond::credentials_query_namespaces(None).expect("no query is not a rejection"),
        None
    );
    class
}

/// What the token target does with the bytes it is given.
///
/// Raw credentials find decoding and signature bugs; minted ones get past the
/// signature so the claim checks — audience, lifetime, namespace, scope, epoch —
/// are reachable at all.
#[derive(Debug, Arbitrary)]
pub enum TokenInput<'a> {
    /// An arbitrary credential presented as `Authorization: Bearer …`.
    Presented(&'a str),
    /// Claims signed with the seam's synthetic HS256 material.
    Minted {
        namespace: &'a str,
        subject: &'a str,
        audience: Option<&'a str>,
        ttl_seconds: u64,
        issued_at: Option<u64>,
        /// `None` omits the claim, which is an *unrestricted* token; `Some` of an
        /// empty vector writes `"scope": []`, which permits nothing. Both
        /// shapes have to be reachable, because confusing them is the bug worth
        /// finding.
        scope: Option<Vec<&'a str>>,
        aliases: Option<Vec<&'a str>>,
    },
}

/// Inbound token verification: JWS decoding, key selection, signature, and
/// every claim check behind them.
pub fn token_verify(input: &TokenInput<'_>) -> &'static str {
    match input {
        TokenInput::Presented(credential) => check_verification(credential, None),
        TokenInput::Minted {
            namespace,
            subject,
            audience,
            ttl_seconds,
            issued_at,
            scope,
            aliases,
        } => {
            let audience = audience.unwrap_or(axond::AUDIENCE);
            let Some(token) = axond::mint_hs256_token(
                namespace,
                subject,
                audience,
                *ttl_seconds,
                *issued_at,
                scope
                    .as_ref()
                    .map(|values| values.iter().map(|value| (*value).to_owned()).collect()),
                aliases
                    .as_ref()
                    .map(|values| values.iter().map(|value| (*value).to_owned()).collect()),
            ) else {
                return "unmintable";
            };
            check_verification(&token, Some(audience))
        }
    }
}

/// Verify a committed token seed re-signed onto the current run, so the claim
/// check the seed is named for is reached however long ago the seed was written.
///
/// `None` for a seed that is not a signable JWS — most of the corpus, which
/// exists for the decoding path and is replayed as bytes instead.
pub fn token_verify_resigned_seed(seed: &str) -> Option<&'static str> {
    let token = axond::resign_seed_onto_this_run(seed)?;
    Some(check_verification(&token, None))
}

/// The properties that hold for every credential, however it was produced.
fn check_verification(credential: &str, minted_audience: Option<&str>) -> &'static str {
    match axond::verify_token(credential) {
        Ok(None) => {
            // The verifier owns the `axt1.` shape; declining to answer would
            // hand the credential to a store that does not own it.
            panic!("the token verifier declined to rule on {credential:?}");
        }
        Ok(Some(verified)) => {
            assert_accepted(&verified);
            if let Some(audience) = minted_audience {
                assert_eq!(
                    audience,
                    axond::AUDIENCE,
                    "a token for a foreign audience verified"
                );
                // The HS256 signer is scoped to one namespace; a signature it
                // produced must never confer authority over another.
                assert_eq!(
                    verified.namespace,
                    axond::NAMESPACES[0],
                    "the HS256 signer minted authority over a namespace it does not hold"
                );
            }
            "accepted"
        }
        Err(rejection) => assert_typed(&rejection),
    }
}

/// Prove the seam verifies signatures for real before any target trusts it.
///
/// Every `token_verify` assertion is worthless against a stubbed verifier, and
/// the fuzz workspace compiles its whole dependency graph with `--cfg fuzzing`
/// (see `.cargo/config.toml`) — a flag some crates use to weaken cryptography on
/// purpose. Rather than trust an audit of the lockfile to stay true across
/// dependency bumps, the required smoke starts here: a token the seam minted
/// verifies, and the same token with one bit flipped in each of its three
/// segments does not.
///
/// # Panics
///
/// If a signature check is not actually happening.
pub fn assert_signature_verification_is_real() {
    let token = axond::mint_hs256_token(
        axond::NAMESPACES[0],
        "signature-check",
        axond::AUDIENCE,
        300,
        None,
        None,
        None,
    )
    .expect("the seam mints its own token");
    assert!(
        matches!(axond::verify_token(&token), Ok(Some(_))),
        "the seam cannot verify a token it just minted"
    );

    let body = token
        .strip_prefix("axt1.")
        .expect("a minted token carries the axt1 prefix");
    let segments: Vec<&str> = body.split('.').collect();
    assert_eq!(segments.len(), 3, "a JWS has three segments: {body:?}");
    for segment in 0..3 {
        let mut tampered: Vec<String> = segments.iter().map(|part| (*part).to_owned()).collect();
        // The *first* character, because it carries the leading six bits of the
        // segment's first byte: rewriting a trailing character can land in
        // padding bits that decode to the same bytes.
        let mut characters: Vec<char> = tampered[segment].chars().collect();
        assert!(
            !characters.is_empty(),
            "segment {segment} of a minted token is empty"
        );
        characters[0] = if characters[0] == 'A' { 'B' } else { 'A' };
        tampered[segment] = characters.into_iter().collect();
        let credential = format!("axt1.{}", tampered.join("."));
        let outcome = axond::verify_token(&credential);
        assert!(
            matches!(
                outcome,
                Err(Rejection::Unauthenticated(_) | Rejection::Unauthorized(_))
            ),
            "tampering with segment {segment} of a minted token still verified: {outcome:?}"
        );
    }
}

fn assert_accepted(verified: &VerifiedToken) {
    assert!(
        axond::NAMESPACES.contains(&verified.namespace.as_str()),
        "a token verified into undeclared namespace {:?}",
        verified.namespace
    );
    assert!(
        !verified.subject.is_empty(),
        "a token verified without a subject"
    );
    // The scope vocabulary is closed, so a token cannot present more distinct
    // capabilities than the gateway defines.
    assert!(
        verified.capabilities <= axond::CAPABILITY_COUNT,
        "a token presented {} capabilities, more than the {} defined",
        verified.capabilities,
        axond::CAPABILITY_COUNT
    );
}
