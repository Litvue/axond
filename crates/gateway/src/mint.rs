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
    rand::{SecureRandom, SystemRandom},
    signature::{Ed25519KeyPair, KeyPair},
};
use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;

use crate::config::{Config, GatewayVerifierAlgorithm, MAX_GATEWAY_VERIFIER_TTL_SECONDS};

#[derive(Debug, Serialize)]
struct MintClaims {
    exp: u64,
    iat: u64,
    aud: String,
    jti: String,
    ns: String,
    sub: String,
}

#[derive(Clone, Copy)]
enum MintAlgorithm {
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

    let algorithm = match (args.get_one::<String>("alg"), configured) {
        (Some(value), _) => parse_algorithm(value)?,
        (None, Some(verifier)) => match verifier.alg {
            GatewayVerifierAlgorithm::EdDsa => MintAlgorithm::EdDsa,
            GatewayVerifierAlgorithm::Hs256 => MintAlgorithm::Hs256,
        },
        (None, None) => bail!("--alg is required when no matching verifier config is available"),
    };
    if let Some(verifier) = configured
        && verifier.alg
            != match algorithm {
                MintAlgorithm::EdDsa => GatewayVerifierAlgorithm::EdDsa,
                MintAlgorithm::Hs256 => GatewayVerifierAlgorithm::Hs256,
            }
    {
        bail!("--alg {} does not match verifier `{kid}`", algorithm.name());
    }

    let ttl = parse_duration(required(args, "ttl")?)?;
    let policy_ceiling = Duration::from_secs(MAX_GATEWAY_VERIFIER_TTL_SECONDS);
    if ttl > policy_ceiling {
        bail!(
            "requested TTL exceeds the {}-second policy ceiling",
            MAX_GATEWAY_VERIFIER_TTL_SECONDS
        );
    }
    if let Some(verifier) = configured
        && ttl > verifier.max_ttl
    {
        bail!(
            "requested TTL exceeds verifier `{kid}` max_ttl of {} seconds",
            verifier.max_ttl.as_secs()
        );
    }

    let audience = args
        .get_one::<String>("audience")
        .cloned()
        .or_else(|| {
            configured.and_then(|_| {
                config
                    .as_ref()
                    .and_then(|config| config.gateway_token.as_ref())
                    .map(|token| token.audience.clone())
            })
        })
        .ok_or_else(|| {
            anyhow::anyhow!("--audience is required without a matching verifier config")
        })?;
    if audience.is_empty() {
        bail!("audience must not be empty");
    }

    mint_token(
        kid,
        algorithm,
        key_material,
        namespace,
        subject,
        &audience,
        ttl,
    )
}

fn mint_token(
    kid: &str,
    algorithm: MintAlgorithm,
    key_material: &str,
    namespace: &str,
    subject: &str,
    audience: &str,
    ttl: Duration,
) -> Result<String> {
    let encoding_key = encoding_key(algorithm, key_material, kid)?;
    let now = unix_now()?;
    let claims = MintClaims {
        exp: now + ttl.as_secs(),
        iat: now,
        aud: audience.to_owned(),
        jti: random_jti()?,
        ns: namespace.to_owned(),
        sub: subject.to_owned(),
    };
    let mut header = Header::new(algorithm.jwt());
    header.kid = Some(kid.to_owned());
    Ok(format!("axt1.{}", encode(&header, &claims, &encoding_key)?))
}

pub fn keygen(args: &ArgMatches) -> Result<()> {
    let private_path = required(args, "private-key")?;
    let kid = required(args, "kid")?;
    let env = required(args, "env")?;
    let namespaces = args
        .get_many::<String>("namespace")
        .ok_or_else(|| anyhow::anyhow!("--namespace is required"))?
        .map(String::as_str)
        .collect::<Vec<_>>();
    if namespaces.iter().any(|namespace| namespace.is_empty()) {
        bail!("--namespace must not be empty");
    }
    let max_ttl = required(args, "max-ttl")?;
    parse_duration(max_ttl).context("--max-ttl is invalid")?;

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

fn generate_ed25519_keypair() -> Result<(Vec<u8>, Ed25519KeyPair)> {
    let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| anyhow::anyhow!("failed to generate Ed25519 keypair"))?;
    let bytes = document.as_ref().to_vec();
    let keypair = Ed25519KeyPair::from_pkcs8(&bytes)
        .map_err(|_| anyhow::anyhow!("generated Ed25519 keypair is invalid"))?;
    Ok((bytes, keypair))
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
                .decode(value)
                .map_err(|_| anyhow::anyhow!("signing key for `{kid}` is not valid base64"))?;
            Ed25519KeyPair::from_pkcs8(&decoded)
                .map_err(|_| anyhow::anyhow!("signing key for `{kid}` is not valid Ed25519 PKCS#8"))
                .map(|_| EncodingKey::from_ed_der(&decoded))
        }
    }
}

fn load_optional_config(args: &ArgMatches) -> Result<Option<Config>> {
    let path = args
        .get_one::<String>("config")
        .cloned()
        .or_else(|| std::env::var("AXOND_CONFIG").ok());
    path.map(|path| {
        Config::load(&path)
            .map_err(|error| anyhow::anyhow!("failed to load config from `{path}`: {error}"))
    })
    .transpose()
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
    let encoded = STANDARD.encode(bytes);
    if let Err(error) = file
        .write_all(encoded.as_bytes())
        .and_then(|_| file.write_all(b"\n"))
    {
        let _ = std::fs::remove_file(path);
        return Err(error).with_context(|| format!("cannot write private key file `{path}`"));
    }
    Ok(())
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
                STANDARD.encode(keypair.public_key().as_ref()),
            ),
        ]);
        let verifier = TokenVerifier::build(&config, &env).unwrap().unwrap();
        let token = mint_token(
            "test-kid",
            MintAlgorithm::EdDsa,
            &STANDARD.encode(private),
            "acme",
            "caller",
            "configured-audience",
            Duration::from_secs(600),
        )
        .unwrap();
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
        let token = mint_token(
            "hs-kid",
            MintAlgorithm::Hs256,
            secret,
            "acme",
            "caller",
            "configured-audience",
            Duration::from_secs(600),
        )
        .unwrap();
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
    async fn keygen_material_round_trips_through_verifier() {
        let (private, keypair) = generate_ed25519_keypair().unwrap();
        let path = std::env::temp_dir().join(format!(
            "axond-keygen-{}-{}.key",
            std::process::id(),
            random_jti().unwrap()
        ));
        write_private_key(path.to_str().unwrap(), &private).unwrap();
        let written = std::fs::read_to_string(&path).unwrap();
        assert_eq!(STANDARD.decode(written.trim()).unwrap(), private);
        assert!(write_private_key(path.to_str().unwrap(), &private).is_err());
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
        let token = mint_token(
            "generated-kid",
            MintAlgorithm::EdDsa,
            &STANDARD.encode(private),
            "acme",
            "generated-caller",
            "configured-audience",
            Duration::from_secs(600),
        )
        .unwrap();
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
        let token = mint_token(
            "different-kid",
            MintAlgorithm::Hs256,
            secret,
            "acme",
            "caller",
            "configured-audience",
            Duration::from_secs(600),
        )
        .unwrap();
        assert!(matches!(
            verifier.resolve(&Presented { credential: &token }).await,
            Err(PrincipalStoreError::Unauthorized(
                TokenVerificationError::UnknownKey { .. }
            ))
        ));
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
}
