use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use base64::{
    Engine as _,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use clap::ArgMatches;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use ring::{
    pkcs8::Document,
    rand::{SecureRandom, SystemRandom},
    signature::{Ed25519KeyPair, KeyPair},
};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;

use crate::aliases::AliasScope;
use crate::config::{Config, GatewayVerifierAlgorithm, MAX_GATEWAY_VERIFIER_TTL_SECONDS};
use crate::principals::Capability;

#[derive(Debug, Serialize)]
struct MintClaims {
    exp: u64,
    iat: u64,
    aud: String,
    jti: String,
    ns: String,
    sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    aliases: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_request_microdollars: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<Vec<String>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MintAlgorithm {
    EdDsa,
    Hs256,
}

impl MintAlgorithm {
    fn jwt(self) -> Algorithm {
        match self {
            Self::EdDsa => Algorithm::EdDSA,
            Self::Hs256 => Algorithm::HS256,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::EdDsa => "EdDSA",
            Self::Hs256 => "HS256",
        }
    }
}

pub fn run(args: &ArgMatches) -> Result<()> {
    let key_env = required(args, "key-env")?;
    let value = std::env::var(key_env)
        .with_context(|| format!("signing key env var `{key_env}` is unset or empty"))?;
    if value.is_empty() {
        bail!("signing key env var `{key_env}` is unset or empty");
    }
    let secret = SecretString::from(value);
    println!(
        "{}",
        mint_from_args(args, load_optional_config(args)?, secret.expose_secret())?
    );
    Ok(())
}

fn mint_from_args(args: &ArgMatches, config: Option<Config>, key_material: &str) -> Result<String> {
    let kid = required(args, "kid")?;
    let namespace = required(args, "namespace")?;
    let subject = required(args, "subject")?;
    if namespace.is_empty() {
        bail!("--namespace must not be empty");
    }
    if subject.is_empty() {
        bail!("--subject must not be empty");
    }

    let configured = config.as_ref().and_then(|config| {
        config
            .gateway_verifier
            .iter()
            .find(|verifier| verifier.kid == kid)
    });
    let configured_audience = configured.and_then(|_| {
        config
            .as_ref()
            .and_then(|config| config.gateway_token.as_ref())
            .map(|token| token.audience.as_str())
    });

    let algorithm = match (args.get_one::<String>("alg"), configured) {
        (Some(value), _) => parse_algorithm(value)?,
        (None, Some(verifier)) => match verifier.alg {
            GatewayVerifierAlgorithm::EdDsa => MintAlgorithm::EdDsa,
            GatewayVerifierAlgorithm::Hs256 => MintAlgorithm::Hs256,
        },
        (None, None) => bail!("--alg is required when no matching verifier config is available"),
    };

    let explicit_audience = args.get_one::<String>("audience");
    let audience = explicit_audience
        .cloned()
        .or_else(|| configured_audience.map(str::to_owned))
        .ok_or_else(|| {
            anyhow::anyhow!("--audience is required without a matching verifier config")
        })?;
    if audience.is_empty() {
        bail!("audience must not be empty");
    }

    let ttl = parse_duration(required(args, "ttl")?)?;
    let scope = args
        .get_many::<String>("scope")
        .map(|values| {
            values
                .map(|value| {
                    Capability::parse(value)
                        .ok_or_else(|| anyhow::anyhow!("unknown scope capability `{value}`"))
                })
                .collect::<Result<Vec<_>>>()
        })
        .transpose()?;
    let max_request_microdollars = args.get_one::<u64>("max-request-microdollars").copied();
    let policy_ceiling = Duration::from_secs(MAX_GATEWAY_VERIFIER_TTL_SECONDS);
    if ttl.is_zero() {
        bail!("requested TTL must be at least 1 second");
    }
    if ttl > policy_ceiling {
        bail!(
            "requested TTL exceeds the {}-second policy ceiling",
            MAX_GATEWAY_VERIFIER_TTL_SECONDS
        );
    }

    if let Some(verifier) = configured {
        let expected_algorithm = match verifier.alg {
            GatewayVerifierAlgorithm::EdDsa => MintAlgorithm::EdDsa,
            GatewayVerifierAlgorithm::Hs256 => MintAlgorithm::Hs256,
        };
        if algorithm != expected_algorithm {
            bail!(
                "algorithm `{}` does not match verifier `{kid}` algorithm `{}`",
                algorithm.name(),
                expected_algorithm.name()
            );
        }
        if ttl > verifier.max_ttl {
            bail!(
                "requested TTL exceeds verifier `{kid}` max_ttl of {} seconds",
                verifier.max_ttl.as_secs()
            );
        }
        if !verifier
            .namespaces
            .iter()
            .any(|allowed| allowed == namespace)
        {
            bail!("verifier `{kid}` is not permitted for namespace `{namespace}`");
        }
        if let Some(expected_audience) = configured_audience
            && audience != expected_audience
        {
            bail!(
                "audience `{audience}` does not match verifier `{kid}` configured audience \
                 `{expected_audience}`"
            );
        }
    }

    let aliases = args
        .get_many::<String>("alias")
        .map(|values| values.cloned().collect::<Vec<_>>());
    if let Some(patterns) = &aliases {
        AliasScope::parse(patterns.iter().map(String::as_str))
            .map_err(|error| anyhow::anyhow!("{error}"))?;
    }

    Ok(mint_token(MintRequest {
        kid,
        algorithm,
        key_material,
        namespace,
        subject,
        audience: &audience,
        ttl,
        aliases,
        max_request_microdollars,
        scope,
    })?
    .token)
}

pub(crate) struct MintRequest<'a> {
    pub(crate) kid: &'a str,
    pub(crate) algorithm: MintAlgorithm,
    pub(crate) key_material: &'a str,
    pub(crate) namespace: &'a str,
    pub(crate) subject: &'a str,
    pub(crate) audience: &'a str,
    pub(crate) ttl: Duration,
    pub(crate) aliases: Option<Vec<String>>,
    pub(crate) max_request_microdollars: Option<u64>,
    pub(crate) scope: Option<Vec<Capability>>,
}

pub(crate) struct MintedToken {
    pub(crate) token: String,
    pub(crate) exp: u64,
}

pub(crate) fn mint_token(request: MintRequest<'_>) -> Result<MintedToken> {
    mint_token_at(request, None)
}

pub(crate) fn mint_issued_at(min_iat: Option<u64>) -> Result<u64> {
    const CLOCK_SKEW_SECONDS: u64 = 5;
    let now = unix_now()?;
    match min_iat {
        Some(min_iat) if min_iat > now.saturating_add(CLOCK_SKEW_SECONDS) => {
            bail!(
                "configured token epoch {min_iat} is too far in the future to issue a valid token"
            );
        }
        Some(min_iat) => Ok(now.max(min_iat)),
        None => Ok(now),
    }
}

pub(crate) fn mint_token_at(
    request: MintRequest<'_>,
    issued_at: Option<u64>,
) -> Result<MintedToken> {
    let scope = request
        .scope
        .as_ref()
        .map(|values| values.iter().map(ToString::to_string).collect());
    mint_token_with_scope_claim(request, issued_at, scope)
}

/// Mint with the `scope` claim written verbatim, so a fuzz target can present a
/// capability name the gateway does not define and let the verifier decide.
/// Minting cannot express that, because [`MintRequest`] takes parsed
/// [`Capability`] values.
#[cfg(fuzzing)]
pub(crate) fn fuzz_mint_token_with_raw_scope(
    request: MintRequest<'_>,
    issued_at: Option<u64>,
    scope: Option<Vec<String>>,
) -> Result<MintedToken> {
    mint_token_with_scope_claim(request, issued_at, scope)
}

fn mint_token_with_scope_claim(
    request: MintRequest<'_>,
    issued_at: Option<u64>,
    scope_claim: Option<Vec<String>>,
) -> Result<MintedToken> {
    let MintRequest {
        kid,
        algorithm,
        key_material,
        namespace,
        subject,
        audience,
        ttl,
        aliases,
        max_request_microdollars,
        scope: _,
    } = request;
    let encoding_key = encoding_key(algorithm, key_material, kid)?;
    let iat = issued_at.unwrap_or(unix_now()?);
    let claims = MintClaims {
        // Saturating: a clock far enough ahead, or a TTL near `u64::MAX`, must
        // produce a token the verifier refuses for its lifetime rather than an
        // arithmetic panic. The callers bound both, so this only ever bites when
        // one of them stops doing so.
        exp: iat.saturating_add(ttl.as_secs()),
        iat,
        aud: audience.to_owned(),
        jti: random_jti()?,
        ns: namespace.to_owned(),
        sub: subject.to_owned(),
        aliases,
        max_request_microdollars,
        scope: scope_claim,
    };
    let mut header = Header::new(algorithm.jwt());
    header.kid = Some(kid.to_owned());
    Ok(MintedToken {
        token: format!("axt1.{}", encode(&header, &claims, &encoding_key)?),
        exp: claims.exp,
    })
}

pub(crate) fn validate_signing_material(
    algorithm: MintAlgorithm,
    value: &str,
    kid: &str,
) -> Result<()> {
    encoding_key(algorithm, value, kid).map(|_| ())
}

pub fn keygen(args: &ArgMatches) -> Result<()> {
    let private_path = required(args, "private-key")?;
    let kid = required(args, "kid")?;
    let env = required(args, "env")?;
    // Keygen uses a narrower paste-safe vocabulary than the config loader so
    // its shell and TOML output remains valid without escaping.
    validate_keygen_identifier("--kid", kid, true)?;
    validate_keygen_identifier("--env", env, false)?;
    let namespaces = args
        .get_many::<String>("namespace")
        .ok_or_else(|| anyhow::anyhow!("--namespace is required"))?
        .map(String::as_str)
        .collect::<Vec<_>>();
    for namespace in &namespaces {
        validate_keygen_identifier("--namespace", namespace, true)?;
    }
    let max_ttl = required(args, "max-ttl")?;
    let max_ttl_duration = parse_duration(max_ttl).context("--max-ttl is invalid")?;
    if max_ttl_duration.is_zero()
        || max_ttl_duration > Duration::from_secs(MAX_GATEWAY_VERIFIER_TTL_SECONDS)
    {
        bail!(
            "--max-ttl must be between 1 second and {} hours",
            MAX_GATEWAY_VERIFIER_TTL_SECONDS / (60 * 60)
        );
    }

    let (document, keypair) = generate_ed25519_keypair()?;
    write_private_key(private_path, document.as_ref())?;

    let public_key = STANDARD.encode(keypair.public_key().as_ref());
    println!("# Public verification key");
    println!("export {env}='{public_key}'");
    println!();
    println!("[[gateway_verifier]]");
    println!("kid = \"{kid}\"");
    println!("alg = \"EdDSA\"");
    println!("env = \"{env}\"");
    let rendered_namespaces = namespaces
        .iter()
        .map(|namespace| format!("\"{namespace}\""))
        .collect::<Vec<_>>()
        .join(", ");
    println!("namespaces = [{rendered_namespaces}]");
    println!("max_ttl = \"{max_ttl}\"");
    Ok(())
}

pub fn revoke(args: &ArgMatches) -> Result<()> {
    let jti = required(args, "jti")?;
    let config_path = args
        .get_one::<String>("config")
        .cloned()
        .or_else(|| std::env::var("AXOND_CONFIG").ok())
        .unwrap_or_else(|| "axond.toml".to_owned());
    let config = load_config_path(&config_path, true)?
        .ok_or_else(|| anyhow::anyhow!("config file `{config_path}` was not loaded"))?;
    let env = std::env::vars().collect();
    let expires_at = resolve_revocation_expiry(
        jti,
        args.get_one::<String>("ttl").map(String::as_str),
        args.get_one::<String>("expires-at").map(String::as_str),
        &config,
    )?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let store = crate::revocation::build(&config.revocation, &config.budget, &env).await?;
        store.revoke(jti, expires_at).await?;
        eprintln!("revoked jti `{jti}`");
        Ok::<(), anyhow::Error>(())
    })?;
    Ok(())
}

pub(crate) fn resolve_revocation_expiry(
    jti: &str,
    ttl: Option<&str>,
    expires_at: Option<&str>,
    config: &Config,
) -> Result<SystemTime> {
    if jti.is_empty() {
        bail!("--jti must not be empty");
    }
    if config.revocation.backend == crate::config::RevocationBackend::None {
        bail!("no denylist is configured; set [revocation].backend to redis or postgres");
    }
    match (ttl, expires_at) {
        (Some(_), Some(_)) => bail!("--ttl and --expires-at are mutually exclusive"),
        (Some(ttl), None) => SystemTime::now()
            .checked_add(parse_duration(ttl)?)
            .ok_or_else(|| anyhow::anyhow!("--ttl is too large")),
        (None, Some(value)) => {
            let seconds = value
                .parse::<u64>()
                .or_else(|_| crate::config::parse_gateway_rfc3339_utc(value))
                .map_err(|_| anyhow::anyhow!("--expires-at must be unix seconds or RFC3339 UTC"))?;
            UNIX_EPOCH
                .checked_add(Duration::from_secs(seconds))
                .ok_or_else(|| anyhow::anyhow!("--expires-at is too far in the future"))
        }
        (None, None) => {
            let max_ttl = config
                .gateway_verifier
                .iter()
                .map(|verifier| verifier.max_ttl)
                .max()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "--ttl or --expires-at is required when no gateway verifier is configured"
                    )
                })?;
            SystemTime::now()
                .checked_add(max_ttl + Duration::from_secs(5))
                .ok_or_else(|| anyhow::anyhow!("configured verifier lifetime is too large"))
        }
    }
}

fn validate_keygen_identifier(flag: &str, value: &str, allow_punctuation: bool) -> Result<()> {
    let valid = !value.is_empty()
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric()
                || character == '_'
                || (allow_punctuation && matches!(character, '.' | '-'))
        });
    if !valid {
        bail!(
            "{flag} value `{value}` contains unsupported characters; use only letters, \
             digits, and the permitted separators"
        );
    }
    Ok(())
}

fn generate_ed25519_keypair() -> Result<(Document, Ed25519KeyPair)> {
    let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| anyhow::anyhow!("failed to generate Ed25519 keypair"))?;
    let keypair = Ed25519KeyPair::from_pkcs8(document.as_ref())
        .map_err(|_| anyhow::anyhow!("generated Ed25519 keypair is invalid"))?;
    Ok((document, keypair))
}

fn encoding_key(algorithm: MintAlgorithm, value: &str, kid: &str) -> Result<EncodingKey> {
    match algorithm {
        MintAlgorithm::Hs256 => {
            if value.len() < 32 {
                bail!("signing key for `{kid}` is an HS256 secret shorter than 32 bytes");
            }
            Ok(EncodingKey::from_secret(value.as_bytes()))
        }
        MintAlgorithm::EdDsa => {
            let decoded = STANDARD
                .decode(value.trim())
                .map_err(|_| anyhow::anyhow!("signing key for `{kid}` is not valid base64"))?;
            Ed25519KeyPair::from_pkcs8(&decoded)
                .map_err(|_| anyhow::anyhow!("signing key for `{kid}` is not valid Ed25519 PKCS#8"))
                .map(|_| EncodingKey::from_ed_der(&decoded))
        }
    }
}

fn load_optional_config(args: &ArgMatches) -> Result<Option<Config>> {
    if let Some(path) = args.get_one::<String>("config") {
        return load_config_path(path, true);
    }
    let Some(path) = std::env::var("AXOND_CONFIG").ok() else {
        if let Some(hint) = cwd_config_hint(
            args.get_one::<String>("config").is_some(),
            std::env::var_os("AXOND_CONFIG").is_some(),
            Path::new("axond.toml").is_file(),
        ) {
            eprintln!("{hint}");
        }
        return Ok(None);
    };
    load_config_path(&path, false)
}

fn load_config_path(path: &str, explicit: bool) -> Result<Option<Config>> {
    if explicit && !Path::new(path).is_file() {
        return Err(anyhow::anyhow!("config file `{path}` does not exist"));
    }
    match Config::load(path) {
        Ok(config) => Ok(Some(config)),
        Err(error) => {
            if explicit {
                return Err(anyhow::anyhow!(
                    "failed to load config from `{path}`: {error}"
                ));
            }
            eprintln!(
                "warning: could not load ambient AXOND_CONFIG `{path}` ({error}); \
                 minting will enforce only the 24-hour policy ceiling"
            );
            Ok(None)
        }
    }
}

fn parse_algorithm(value: &str) -> Result<MintAlgorithm> {
    match value {
        "EdDSA" => Ok(MintAlgorithm::EdDsa),
        "HS256" => Ok(MintAlgorithm::Hs256),
        _ => bail!("unsupported signing algorithm `{value}`"),
    }
}

fn parse_duration(value: &str) -> Result<Duration> {
    let value = value.trim();
    let split = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split);
    let amount: u64 = number
        .parse()
        .with_context(|| format!("invalid duration `{value}`"))?;
    let multiplier = match suffix {
        "" | "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 24 * 60 * 60,
        _ => bail!("invalid duration `{value}`; use seconds, m, h, or d"),
    };
    amount
        .checked_mul(multiplier)
        .map(Duration::from_secs)
        .ok_or_else(|| anyhow::anyhow!("duration `{value}` is too large"))
}

fn random_jti() -> Result<String> {
    let mut bytes = [0_u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow::anyhow!("failed to generate token identifier"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn unix_now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_secs())
}

fn required<'a>(args: &'a ArgMatches, name: &str) -> Result<&'a str> {
    args.get_one::<String>(name)
        .map(String::as_str)
        .ok_or_else(|| anyhow::anyhow!("--{name} is required"))
}

fn write_private_key(path: &str, bytes: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(Path::new(path))
        .with_context(|| format!("cannot create private key file `{path}`"))?;
    #[cfg(not(unix))]
    eprintln!(
        "warning: private key file `{path}` uses inherited permissions on this platform; \
         restrict access manually"
    );
    let encoded = SecretString::from(STANDARD.encode(bytes));
    if let Err(error) = file
        .write_all(encoded.expose_secret().as_bytes())
        .and_then(|_| file.write_all(b"\n"))
    {
        drop(file);
        let _ = std::fs::remove_file(path);
        return Err(error).with_context(|| {
            format!("cannot write private key file `{path}`; delete it manually if it still exists")
        });
    }
    Ok(())
}

fn cwd_config_hint(explicit: bool, ambient: bool, exists: bool) -> Option<&'static str> {
    if exists && !explicit && !ambient {
        Some(
            "hint: axond.toml exists in the current directory; pass `--config axond.toml` \
             to additionally enforce the verifier's max_ttl, namespace list, and audience",
        )
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::principals::{
        Presented, PrincipalStore, PrincipalStoreError, TokenVerificationError, TokenVerifier,
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn ed25519_mint_round_trips_through_verifier() {
        let (private, keypair) = generate_ed25519_keypair().unwrap();
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "acme"
default = true
[[gateway_key]]
env = "STATIC"
namespace = "acme"
[gateway_token]
audience = "configured-audience"
[[gateway_verifier]]
kid = "test-kid"
alg = "EdDSA"
env = "PUBLIC"
namespaces = ["acme"]
max_ttl = "15m"
"#,
        )
        .unwrap();
        let env = HashMap::from([
            ("STATIC".to_owned(), "static".to_owned()),
            (
                "PUBLIC".to_owned(),
                format!(" {}\n", STANDARD.encode(keypair.public_key().as_ref())),
            ),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let token = mint_token(MintRequest {
            kid: "test-kid",
            algorithm: MintAlgorithm::EdDsa,
            key_material: &format!(" \n{}\t ", STANDARD.encode(private.as_ref())),
            namespace: "acme",
            subject: "caller",
            audience: "configured-audience",
            ttl: Duration::from_secs(600),
            aliases: None,
            max_request_microdollars: None,
            scope: None,
        })
        .unwrap()
        .token;
        let principal = verifier
            .resolve(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.namespace, "acme");
        assert_eq!(principal.subject, "caller");
        assert_eq!(principal.signer_kid.as_deref(), Some("test-kid"));
    }

    #[tokio::test]
    async fn hs256_mint_round_trips_through_verifier() {
        let config = verifier_config("hs-kid", "HS256", "HS_SECRET", "15m");
        let secret = "01234567890123456789012345678901";
        let env = HashMap::from([
            ("STATIC".to_owned(), "static".to_owned()),
            ("HS_SECRET".to_owned(), secret.to_owned()),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let token = mint_token(MintRequest {
            kid: "hs-kid",
            algorithm: MintAlgorithm::Hs256,
            key_material: secret,
            namespace: "acme",
            subject: "caller",
            audience: "configured-audience",
            ttl: Duration::from_secs(600),
            aliases: None,
            max_request_microdollars: None,
            scope: None,
        })
        .unwrap()
        .token;
        let principal = verifier
            .resolve(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.namespace, "acme");
        assert_eq!(principal.subject, "caller");
        assert_eq!(principal.signer_kid.as_deref(), Some("hs-kid"));
    }

    #[tokio::test]
    async fn explicit_audience_and_one_second_ttl_round_trip_through_verifier() {
        let config = verifier_config("hs-kid", "HS256", "HS_SECRET", "15m");
        let secret = "01234567890123456789012345678901";
        let env = HashMap::from([
            ("STATIC".to_owned(), "static".to_owned()),
            ("HS_SECRET".to_owned(), secret.to_owned()),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "1s",
            "--audience",
            "configured-audience",
            "--scope",
            "chat",
            "--scope",
            "models",
            "--max-request-microdollars",
            "123",
        ]);
        let token = mint_from_args(&args, Some(config), secret).unwrap();
        let principal = verifier
            .resolve(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.namespace, "acme");
        assert_eq!(principal.subject, "caller");
        assert_eq!(principal.signer_kid.as_deref(), Some("hs-kid"));
        let scope = principal.scope.expect("scope claim");
        assert!(scope.contains(&Capability::Chat));
        assert!(scope.contains(&Capability::Models));
        assert_eq!(principal.max_request_microdollars, Some(123));
    }

    /// The offline command is the operator's own key, so it keeps ADR 0019's
    /// scope-less semantics: no `--scope` writes no `scope` claim, and an
    /// unscoped token is unrestricted — `status` included. Only `POST /v1/tokens`
    /// synthesises a scope, which is why the exclusion of `status` from a
    /// scope-less mint is a property of that route and not of this command.
    #[tokio::test]
    async fn an_offline_mint_without_a_scope_stays_unscoped_and_unrestricted() {
        let config = verifier_config("hs-kid", "HS256", "HS_SECRET", "15m");
        let secret = "01234567890123456789012345678901";
        let env = HashMap::from([
            ("STATIC".to_owned(), "static".to_owned()),
            ("HS_SECRET".to_owned(), secret.to_owned()),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let base = [
            "--kid",
            "hs-kid",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "1s",
            "--audience",
            "configured-audience",
        ];

        let token = mint_from_args(&mint_args(&base), Some(config.clone()), secret).unwrap();
        let principal = verifier
            .resolve(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        assert!(
            principal.scope.is_none(),
            "an omitted --scope must leave the claim off rather than synthesise a ceiling"
        );

        let mut named = base.to_vec();
        named.extend(["--scope", "status"]);
        let token = mint_from_args(&mint_args(&named), Some(config), secret).unwrap();
        let principal = verifier
            .resolve(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        let scope = principal.scope.expect("a named scope claim");
        assert!(scope.contains(&Capability::Status));
        assert_eq!(scope.len(), 1);
    }

    #[tokio::test]
    async fn hs256_preserves_secret_whitespace() {
        let config = verifier_config("hs-kid", "HS256", "HS_SECRET", "15m");
        let secret = "0123456789012345678901234567890\n";
        let env = HashMap::from([
            ("STATIC".to_owned(), "static".to_owned()),
            ("HS_SECRET".to_owned(), secret.to_owned()),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let token = mint_token(MintRequest {
            kid: "hs-kid",
            algorithm: MintAlgorithm::Hs256,
            key_material: secret,
            namespace: "acme",
            subject: "caller",
            audience: "configured-audience",
            ttl: Duration::from_secs(600),
            aliases: None,
            max_request_microdollars: None,
            scope: None,
        })
        .unwrap()
        .token;
        let principal = verifier
            .resolve(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.subject, "caller");
    }

    #[tokio::test]
    async fn keygen_material_round_trips_through_verifier() {
        let (private, keypair) = generate_ed25519_keypair().unwrap();
        let path = std::env::temp_dir().join(format!(
            "axond-keygen-{}-{}.key",
            std::process::id(),
            random_jti().unwrap()
        ));
        write_private_key(path.to_str().unwrap(), private.as_ref()).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(STANDARD.decode(written.trim()).unwrap(), private.as_ref());
        assert!(write_private_key(path.to_str().unwrap(), private.as_ref()).is_err());
        let _ = std::fs::remove_file(&path);

        let config = verifier_config("generated-kid", "EdDSA", "PUBLIC", "15m");
        let env = HashMap::from([
            ("STATIC".to_owned(), "static".to_owned()),
            (
                "PUBLIC".to_owned(),
                STANDARD.encode(keypair.public_key().as_ref()),
            ),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let token = mint_token(MintRequest {
            kid: "generated-kid",
            algorithm: MintAlgorithm::EdDsa,
            key_material: &STANDARD.encode(private.as_ref()),
            namespace: "acme",
            subject: "generated-caller",
            audience: "configured-audience",
            ttl: Duration::from_secs(600),
            aliases: None,
            max_request_microdollars: None,
            scope: None,
        })
        .unwrap()
        .token;
        let principal = verifier
            .resolve(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.namespace, "acme");
        assert_eq!(principal.subject, "generated-caller");
        assert_eq!(principal.signer_kid.as_deref(), Some("generated-kid"));
    }

    #[test]
    fn mint_rejects_ttl_above_policy_ceiling() {
        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--alg",
            "HS256",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "25h",
            "--audience",
            "configured-audience",
        ]);
        let error = mint_from_args(&args, None, "01234567890123456789012345678901")
            .unwrap_err()
            .to_string();
        assert!(error.contains("policy ceiling"));
    }

    // Found by the `token_verify` fuzz target: an `iat` near the end of the
    // epoch used to overflow while computing `exp`, which is an abort in a
    // release build with overflow checks and a wrong `exp` without them. The
    // token it produces now is one the verifier refuses on its lifetime.
    #[test]
    fn mint_saturates_rather_than_overflowing_an_extreme_issue_time() {
        let minted = mint_token_at(
            MintRequest {
                kid: "hs-kid",
                algorithm: MintAlgorithm::Hs256,
                key_material: "01234567890123456789012345678901",
                namespace: "acme",
                subject: "caller",
                audience: "configured-audience",
                ttl: Duration::from_secs(600),
                aliases: None,
                max_request_microdollars: None,
                scope: None,
            },
            Some(u64::MAX),
        )
        .unwrap();
        assert_eq!(minted.exp, u64::MAX);
    }

    #[test]
    fn mint_rejects_zero_ttl() {
        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--alg",
            "HS256",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "0",
            "--audience",
            "configured-audience",
        ]);
        let error = mint_from_args(&args, None, "01234567890123456789012345678901")
            .unwrap_err()
            .to_string();
        assert!(error.contains("at least 1 second"));
    }

    #[test]
    fn mint_respects_configured_max_ttl_and_defaults_audience() {
        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "16m",
        ]);
        let config = verifier_config("hs-kid", "HS256", "HS_SECRET", "15m");
        let error = mint_from_args(
            &args,
            Some(config.clone()),
            "01234567890123456789012345678901",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("max_ttl"));

        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "10m",
        ]);
        let token =
            mint_from_args(&args, Some(config), "01234567890123456789012345678901").unwrap();
        assert!(token.starts_with("axt1."));
    }

    #[test]
    fn mint_rejects_namespace_not_permitted_by_matching_verifier() {
        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "other",
            "--subject",
            "caller",
            "--ttl",
            "10m",
        ]);
        let config = verifier_config("hs-kid", "HS256", "HS_SECRET", "15m");
        let error = mint_from_args(&args, Some(config), "01234567890123456789012345678901")
            .unwrap_err()
            .to_string();
        assert!(error.contains("hs-kid"));
        assert!(error.contains("other"));
    }

    #[test]
    fn mint_rejects_conflicting_configured_audience() {
        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "10m",
            "--audience",
            "wrong-audience",
        ]);
        let config = verifier_config("hs-kid", "HS256", "HS_SECRET", "15m");
        let error = mint_from_args(&args, Some(config), "01234567890123456789012345678901")
            .unwrap_err()
            .to_string();
        assert!(error.contains("hs-kid"));
        assert!(error.contains("wrong-audience"));
        assert!(error.contains("configured-audience"));
    }

    #[test]
    fn keygen_rejects_zero_and_over_ceiling_max_ttl() {
        for max_ttl in ["0", "25h"] {
            let args = keygen_args(max_ttl);
            let error = keygen(&args).unwrap_err().to_string();
            assert!(error.contains("--max-ttl"));
        }
    }

    #[test]
    fn explicit_config_failure_is_fatal_but_ambient_failure_is_optional() {
        let explicit = mint_args(&[
            "--kid",
            "hs-kid",
            "--alg",
            "HS256",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "10m",
            "--audience",
            "configured-audience",
            "--config",
            "/definitely/missing/axond.toml",
        ]);
        let error = load_optional_config(&explicit).unwrap_err().to_string();
        assert!(error.contains("config file `/definitely/missing/axond.toml` does not exist"));

        assert!(
            load_config_path("/definitely/missing/axond.toml", false)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn cwd_config_hint_only_applies_without_another_config_source() {
        assert!(cwd_config_hint(false, false, true).is_some());
        assert!(cwd_config_hint(false, false, false).is_none());
        assert!(cwd_config_hint(true, false, true).is_none());
        assert!(cwd_config_hint(false, true, true).is_none());

        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--alg",
            "HS256",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "10m",
            "--audience",
            "configured-audience",
        ]);
        let token = mint_from_args(&args, None, "01234567890123456789012345678901").unwrap();
        assert!(token.starts_with("axt1."));
    }

    #[test]
    fn mint_rejects_unknown_scope_capability() {
        let args = mint_args(&[
            "--kid",
            "hs-kid",
            "--alg",
            "HS256",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "10m",
            "--audience",
            "configured-audience",
            "--scope",
            "typo",
        ]);
        let error = mint_from_args(&args, None, "01234567890123456789012345678901")
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown scope capability"));
    }

    #[test]
    fn parse_duration_supports_common_units() {
        assert_eq!(parse_duration("15m").unwrap(), Duration::from_secs(900));
        assert_eq!(parse_duration("1h").unwrap(), Duration::from_secs(3600));
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn mint_with_different_kid_is_rejected_by_verifier() {
        let config = verifier_config("configured-kid", "HS256", "HS_SECRET", "15m");
        let secret = "01234567890123456789012345678901";
        let env = HashMap::from([
            ("STATIC".to_owned(), "static".to_owned()),
            ("HS_SECRET".to_owned(), secret.to_owned()),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let token = mint_token(MintRequest {
            kid: "different-kid",
            algorithm: MintAlgorithm::Hs256,
            key_material: secret,
            namespace: "acme",
            subject: "caller",
            audience: "configured-audience",
            ttl: Duration::from_secs(600),
            aliases: None,
            max_request_microdollars: None,
            scope: None,
        })
        .unwrap()
        .token;
        assert!(matches!(
            verifier.resolve(&Presented { credential: &token }).await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::UnknownKey { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn mint_emits_alias_claim_and_validates_patterns() {
        let config = verifier_config("configured-kid", "HS256", "HS_SECRET", "15m");
        let secret = "01234567890123456789012345678901";
        let env = HashMap::from([
            ("STATIC".to_owned(), "static".to_owned()),
            ("HS_SECRET".to_owned(), secret.to_owned()),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let args = mint_args(&[
            "--kid",
            "configured-kid",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "10m",
            "--alias",
            "gpt-*",
            "--alias",
            "claude-3",
        ]);
        let token = mint_from_args(&args, Some(config.clone()), secret).unwrap();
        let principal = verifier
            .resolve(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        let scope = principal.alias_scope.unwrap();
        assert!(scope.permits("gpt-4o"));
        assert!(scope.permits("claude-3"));
        assert!(!scope.permits("other"));

        let invalid = mint_args(&[
            "--kid",
            "configured-kid",
            "--key-env",
            "HS_SECRET",
            "--namespace",
            "acme",
            "--subject",
            "caller",
            "--ttl",
            "10m",
            "--alias",
            "foo*bar",
        ]);
        assert!(mint_from_args(&invalid, Some(config), secret).is_err());
    }

    fn verifier_config(kid: &str, algorithm: &str, env: &str, max_ttl: &str) -> Config {
        Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "acme"
default = true
[[gateway_key]]
env = "STATIC"
namespace = "acme"
[gateway_token]
audience = "configured-audience"
[[gateway_verifier]]
kid = "{kid}"
alg = "{algorithm}"
env = "{env}"
namespaces = ["acme"]
max_ttl = "{max_ttl}"
"#
        ))
        .unwrap()
    }

    fn mint_args(values: &[&str]) -> clap::ArgMatches {
        let mut argv = vec!["axond", "mint"];
        argv.extend(values);
        crate::cli()
            .try_get_matches_from(argv)
            .unwrap()
            .remove_subcommand()
            .expect("mint subcommand")
            .1
    }

    fn keygen_args(max_ttl: &str) -> clap::ArgMatches {
        keygen_args_with("test-kid", "PUBLIC", "acme", max_ttl)
    }

    fn revoke_args(values: &[&str]) -> clap::ArgMatches {
        let mut argv = vec!["axond", "revoke"];
        argv.extend(values);
        crate::cli()
            .try_get_matches_from(argv)
            .unwrap()
            .remove_subcommand()
            .expect("revoke subcommand")
            .1
    }

    fn configured_revocation() -> Config {
        let mut config = verifier_config("test-kid", "HS256", "SECRET", "15m");
        config.revocation.backend = crate::config::RevocationBackend::Redis;
        config.revocation.dsn_env = Some("REDIS_URL".to_owned());
        config
    }

    #[test]
    fn revoke_resolves_ttl_unix_and_rfc3339_expiries() {
        let config = configured_revocation();
        let before = SystemTime::now();
        let ttl = revoke_args(&["--jti", "jti-1", "--ttl", "2m"]);
        let expiry = resolve_revocation_expiry(
            "jti-1",
            ttl.get_one::<String>("ttl").map(String::as_str),
            None,
            &config,
        )
        .unwrap();
        assert!(expiry >= before + Duration::from_secs(119));

        let unix = resolve_revocation_expiry("jti-1", None, Some("2000000000"), &config).unwrap();
        assert_eq!(unix, UNIX_EPOCH + Duration::from_secs(2_000_000_000));
        let rfc3339 =
            resolve_revocation_expiry("jti-1", None, Some("2033-05-18T03:33:20Z"), &config)
                .unwrap();
        assert_eq!(rfc3339, unix);

        let error = resolve_revocation_expiry("jti-1", None, Some("18446744073709551615"), &config)
            .expect_err("an overflowing expiry must be rejected");
        assert!(
            error
                .to_string()
                .contains("--expires-at is too far in the future")
        );
    }

    #[test]
    fn revoke_defaults_beyond_the_largest_verifier_and_requires_expiry_without_one() {
        let config = configured_revocation();
        let before = SystemTime::now();
        let expiry = resolve_revocation_expiry("jti-1", None, None, &config).unwrap();
        assert!(expiry >= before + Duration::from_secs(905));

        let mut no_verifier = config;
        no_verifier.gateway_verifier.clear();
        let error = resolve_revocation_expiry("jti-1", None, None, &no_verifier)
            .expect_err("explicit expiry required");
        assert!(error.to_string().contains("--ttl or --expires-at"));
    }

    #[test]
    fn revoke_rejects_an_unconfigured_denylist() {
        let config = verifier_config("test-kid", "HS256", "SECRET", "15m");
        let error = resolve_revocation_expiry("jti-1", Some("1m"), None, &config)
            .expect_err("none backend must fail");
        assert!(error.to_string().contains("no denylist is configured"));
    }

    #[test]
    fn revoke_cli_rejects_mutually_exclusive_expiry_flags() {
        let error = crate::cli().try_get_matches_from([
            "axond",
            "revoke",
            "--jti",
            "jti-1",
            "--ttl",
            "1m",
            "--expires-at",
            "2000000000",
        ]);
        assert!(error.is_err());
    }

    fn keygen_args_with(kid: &str, env: &str, namespace: &str, max_ttl: &str) -> clap::ArgMatches {
        crate::cli()
            .try_get_matches_from([
                "axond",
                "keygen",
                "--private-key",
                "/definitely/missing/axond.key",
                "--kid",
                kid,
                "--env",
                env,
                "--namespace",
                namespace,
                "--max-ttl",
                max_ttl,
            ])
            .unwrap()
            .remove_subcommand()
            .expect("keygen subcommand")
            .1
    }

    #[test]
    fn keygen_rejects_unpasteable_identifiers() {
        for (flag, kid, env, namespace) in [
            ("--kid", "bad\"kid", "PUBLIC", "acme"),
            ("--kid", r"bad\kid", "PUBLIC", "acme"),
            ("--env", "test-kid", "BAD\"ENV", "acme"),
            ("--env", "test-kid", r"BAD\ENV", "acme"),
            ("--namespace", "test-kid", "PUBLIC", "bad\"namespace"),
            ("--namespace", "test-kid", "PUBLIC", r"bad\namespace"),
        ] {
            let args = keygen_args_with(kid, env, namespace, "15m");
            let error = keygen(&args).unwrap_err().to_string();
            assert!(error.contains(flag));
        }
    }

    #[test]
    fn keygen_accepts_paste_safe_identifiers() {
        assert!(validate_keygen_identifier("--kid", "acme-2026_08.v1", true).is_ok());
        assert!(validate_keygen_identifier("--env", "GW_VERIFY_ACME_2026_08", false).is_ok());
        assert!(validate_keygen_identifier("--namespace", "acme-prod_1.v1", true).is_ok());
    }
}
