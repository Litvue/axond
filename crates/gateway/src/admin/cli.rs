//! `axond admin`: the same routes, from a terminal.
//!
//! This is an HTTP client and deliberately nothing more. It does not open the
//! control plane, construct an [`AdminService`], or hold a store, so there is no
//! command here that can publish a revision without an idempotency key, an
//! expected revision, an authenticated identity, and the complete-candidate
//! validation the API performs — the CLI cannot be a second, weaker way in
//! (ADR 0027).
//!
//! Mutating commands send the same envelope the route parses, read from a file
//! or standard input, rather than reconstructing every resource field as flags:
//! the schema then has exactly one definition, and `--dry-run` against a real
//! deployment is how an operator checks a document before applying it.
//!
//! Budgets and limits are policy fields, so they are `policies` documents;
//! `axond admin resources` prints the mapping rather than leaving it to be
//! rediscovered from a 404.
//!
//! [`AdminService`]: super::service::AdminService

use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

use clap::{Arg, ArgAction, ArgMatches, Command};
use reqwest::Method;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};

use super::auth::{BREAKGLASS_OPERATOR_HEADER, BREAKGLASS_REASON_HEADER};
use super::protocol::{
    ADMIN_PREFIX, DRY_RUN_HEADER, EXPECTED_REVISION_EMPTY, EXPECTED_REVISION_HEADER,
    IDEMPOTENCY_KEY_HEADER,
};

/// The environment variable the administrative credential is read from.
///
/// A flag would put the credential in the shell history and in every process
/// listing on the host; an environment variable is the weakest thing that is not
/// that.
const TOKEN_ENV: &str = "AXOND_ADMIN_TOKEN";
/// Where `axond admin` talks to when `--endpoint` is not given.
const ENDPOINT_ENV: &str = "AXOND_ADMIN_ENDPOINT";
const DEFAULT_ENDPOINT: &str = "http://127.0.0.1:8080";

/// The mutating routes, as the CLI names them. One list, so `apply` accepts
/// exactly the paths the router mounts.
const RESOURCES: &[(&str, &str)] = &[
    ("tenants", "a tenant and its lifecycle state"),
    ("projects", "a project (namespace) inside a tenant"),
    (
        "providers",
        "a provider connection: wire family and endpoint",
    ),
    (
        "credentials",
        "a provider credential: a secret *reference* and its lifecycle, never material",
    ),
    ("catalogs", "a provider's model catalogue snapshot"),
    ("models", "a model enablement and its price"),
    ("aliases", "a routing alias and its ordered targets"),
    (
        "policies",
        "budgets, concurrency limits, and revocation for a scope",
    ),
];

pub fn command() -> Command {
    let endpoint = Arg::new("endpoint")
        .long("endpoint")
        .global(true)
        .help("Base URL of the gateway's administrative surface")
        .long_help(format!(
            "Base URL of the gateway's administrative surface (default: ${ENDPOINT_ENV}, else \
             {DEFAULT_ENDPOINT}). The administrative credential is read from ${TOKEN_ENV}."
        ));
    let insecure = Arg::new("insecure-plaintext")
        .long("insecure-plaintext")
        .global(true)
        .action(ArgAction::SetTrue)
        .help(format!(
            "Send ${TOKEN_ENV} over plaintext http to a remote host (refused by default)"
        ));
    let operator = Arg::new("operator")
        .long("operator")
        .global(true)
        .help("Breakglass operator identity, sent as the attribution for this action");
    let reason = Arg::new("reason")
        .long("reason")
        .global(true)
        .help("Why breakglass is being used; recorded in the audit trail");
    let tenant = Arg::new("tenant")
        .long("tenant")
        .help("Tenant id to scope to");
    let project = Arg::new("project")
        .long("project")
        .requires("tenant")
        .help("Project id to scope to; requires --tenant");

    Command::new("admin")
        .about("Read and change durable desired state through /admin/v1")
        .subcommand_required(true)
        .arg(endpoint)
        .arg(insecure)
        .arg(operator)
        .arg(reason)
        .subcommand(
            Command::new("state")
                // Deployment-wide, like the route: a scoped projection would be a
                // different read with a different authorization, not a filter.
                .about("The complete desired state at the current revision, without bodies"),
        )
        .subcommand(
            Command::new("history")
                .about("Recent revisions, newest first")
                .arg(
                    Arg::new("limit")
                        .long("limit")
                        .help("How many revisions to return (1-100)"),
                ),
        )
        .subcommand(
            Command::new("audit")
                .about("The audit record of one revision")
                .arg(
                    Arg::new("revision")
                        .long("revision")
                        .required(true)
                        .help("The revision to read"),
                ),
        )
        .subcommand(
            Command::new("convergence")
                .about("What this replica has loaded and activated, from its own cached status"),
        )
        .subcommand(
            Command::new("resources").about("List the resource kinds `axond admin apply` accepts"),
        )
        .subcommand(
            Command::new("apply")
                .about("Publish a mutation document, or validate it with --dry-run")
                .arg(
                    Arg::new("resource")
                        .long("resource")
                        .required(true)
                        .value_parser(RESOURCES.iter().map(|(name, _)| *name).collect::<Vec<_>>())
                        .help("Which resource the document describes"),
                )
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .help("The mutation document; `-` (the default) reads standard input"),
                )
                .arg(
                    Arg::new("idempotency-key")
                        .long("idempotency-key")
                        .required(true)
                        .help("Retry-safe key: the same key replays, it never publishes twice"),
                )
                .arg(
                    Arg::new("expected-revision")
                        .long("expected-revision")
                        .required(true)
                        .help(format!(
                            "The revision this change was written against, or `\
                             {EXPECTED_REVISION_EMPTY}` for an unpublished control plane"
                        )),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Validate and diff the candidate without publishing anything"),
                ),
        )
        .subcommand(
            Command::new("rollback")
                .about("Republish an earlier revision's complete state as a new revision")
                .arg(
                    Arg::new("revision")
                        .long("revision")
                        .required(true)
                        .help("The revision whose state should be restored"),
                )
                .arg(
                    Arg::new("summary")
                        .long("summary")
                        .required(true)
                        .help("Why: recorded in the audit trail"),
                )
                .arg(tenant)
                .arg(project)
                .arg(
                    Arg::new("idempotency-key")
                        .long("idempotency-key")
                        .required(true)
                        .help("Retry-safe key: the same key replays, it never publishes twice"),
                )
                .arg(
                    Arg::new("expected-revision")
                        .long("expected-revision")
                        .required(true)
                        .help("The revision the rollback was decided against"),
                )
                .arg(
                    Arg::new("dry-run")
                        .long("dry-run")
                        .action(ArgAction::SetTrue)
                        .help("Diff the restored state against the current one without publishing"),
                ),
        )
}

/// Run one `axond admin` subcommand.
///
/// A refusal from the gateway is printed as the gateway sent it — the typed
/// envelope, with its stable `code` — and exits non-zero, so a script can branch
/// on `revision_conflict` rather than on prose this command invented.
pub fn run(args: &ArgMatches) -> anyhow::Result<()> {
    let (name, sub) = args
        .subcommand()
        .expect("clap requires an `axond admin` subcommand");
    if name == "resources" {
        for (resource, description) in RESOURCES {
            println!("{resource:<12} {description}");
        }
        return Ok(());
    }
    let env: HashMap<String, String> = std::env::vars().collect();
    let call = plan(name, sub, &env)?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(send(call, base(args, &env)?, headers(args, sub, &env)?))
}

/// A required flag clap has already validated as present.
fn required<'a>(args: &'a ArgMatches, id: &str) -> &'a str {
    args.get_one::<String>(id)
        .map(String::as_str)
        .expect("clap requires this argument")
}

/// One administrative call: what to send where.
#[derive(Debug)]
struct Call {
    method: Method,
    path: String,
    query: Vec<(String, String)>,
    body: Option<String>,
}

fn plan(name: &str, args: &ArgMatches, env: &HashMap<String, String>) -> anyhow::Result<Call> {
    let call = match name {
        "state" => Call {
            method: Method::GET,
            path: "state".to_owned(),
            query: Vec::new(),
            body: None,
        },
        "history" => Call {
            method: Method::GET,
            path: "history".to_owned(),
            query: args
                .get_one::<String>("limit")
                .map(|limit| vec![("limit".to_owned(), limit.clone())])
                .unwrap_or_default(),
            body: None,
        },
        "audit" => Call {
            method: Method::GET,
            path: format!("audit/{}", required(args, "revision")),
            query: Vec::new(),
            body: None,
        },
        "convergence" => Call {
            method: Method::GET,
            path: "convergence".to_owned(),
            query: Vec::new(),
            body: None,
        },
        "apply" => Call {
            method: Method::POST,
            path: required(args, "resource").to_owned(),
            query: Vec::new(),
            body: Some(document(
                args.get_one::<String>("file").map(String::as_str),
            )?),
        },
        "rollback" => {
            let mut body = serde_json::json!({
                "summary": required(args, "summary"),
                "revision": required(args, "revision"),
            });
            for key in ["tenant", "project"] {
                if let Some(value) = args.get_one::<String>(key) {
                    body[key] = serde_json::Value::String(value.clone());
                }
            }
            Call {
                method: Method::POST,
                path: "rollback".to_owned(),
                query: Vec::new(),
                body: Some(body.to_string()),
            }
        }
        other => unreachable!("clap validates subcommands: {other}"),
    };
    // Read only to fail early on a missing credential, before a connection is
    // opened against a deployment that would answer 401.
    if !env.contains_key(TOKEN_ENV) {
        anyhow::bail!("${TOKEN_ENV} is not set: `axond admin` needs an administrative credential");
    }
    Ok(call)
}

/// The mutation document, from a file or standard input.
fn document(path: Option<&str>) -> anyhow::Result<String> {
    match path {
        None | Some("-") => {
            let mut body = String::new();
            std::io::stdin().read_to_string(&mut body)?;
            Ok(body)
        }
        Some(path) => Ok(std::fs::read_to_string(path)
            .map_err(|error| anyhow::anyhow!("failed to read `{path}`: {error}"))?),
    }
}

/// The base URL every call is sent to, refusing the ones that would put an
/// administrative credential on the wire in the clear.
///
/// `AXOND_ADMIN_TOKEN` is a bearer credential for the whole control plane: over
/// plaintext to another host it is readable by every hop in between, and a
/// mistyped `http://` scheme is exactly how that happens. Loopback is exempt
/// because there is no wire — it is also the default endpoint — and
/// `--insecure-plaintext` is the deliberate opt-in for a deployment that
/// terminates TLS in a sidecar on the same trusted path.
fn base(args: &ArgMatches, env: &HashMap<String, String>) -> anyhow::Result<String> {
    let endpoint = args
        .get_one::<String>("endpoint")
        .cloned()
        .or_else(|| env.get(ENDPOINT_ENV).cloned())
        .unwrap_or_else(|| DEFAULT_ENDPOINT.to_owned());
    let url = reqwest::Url::parse(&endpoint)
        .map_err(|error| anyhow::anyhow!("`{endpoint}` is not a URL: {error}"))?;
    if url.scheme() != "https" && !args.get_flag("insecure-plaintext") && !is_loopback(&url) {
        anyhow::bail!(
            "refusing to send the administrative credential to `{endpoint}` in the clear: use \
             https, or pass --insecure-plaintext if the plaintext hop is inside a trusted path"
        );
    }
    Ok(format!("{}{ADMIN_PREFIX}", endpoint.trim_end_matches('/')))
}

/// Whether a URL names this host, and so never reaches a network.
fn is_loopback(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    if host == "localhost" {
        return true;
    }
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .is_ok_and(|address| address.is_loopback())
}

fn headers(
    global: &ArgMatches,
    args: &ArgMatches,
    env: &HashMap<String, String>,
) -> anyhow::Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    let token = env
        .get(TOKEN_ENV)
        .ok_or_else(|| anyhow::anyhow!("${TOKEN_ENV} is not set"))?;
    headers.insert(AUTHORIZATION, value(&format!("Bearer {token}"))?);
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    for (name, key) in [
        (BREAKGLASS_OPERATOR_HEADER, "operator"),
        (BREAKGLASS_REASON_HEADER, "reason"),
    ] {
        if let Some(supplied) = global.get_one::<String>(key) {
            headers.insert(header(name), value(supplied)?);
        }
    }
    if let Some(key) = args.optional("idempotency-key") {
        headers.insert(header(IDEMPOTENCY_KEY_HEADER), value(key)?);
    }
    if let Some(expected) = args.optional("expected-revision") {
        headers.insert(header(EXPECTED_REVISION_HEADER), value(expected)?);
    }
    if args.dry_run() {
        headers.insert(header(DRY_RUN_HEADER), HeaderValue::from_static("true"));
    }
    Ok(headers)
}

/// The preconditions are read from whichever subcommand is running, and the
/// read commands define none of them. `ArgMatches::get_one` *panics* for an
/// argument the subcommand did not define rather than returning `None`, so both
/// lookups go through the fallible form.
trait OptionalArg {
    fn optional(&self, id: &str) -> Option<&String>;
    fn dry_run(&self) -> bool;
}

impl OptionalArg for ArgMatches {
    fn optional(&self, id: &str) -> Option<&String> {
        self.try_get_one::<String>(id).ok().flatten()
    }

    fn dry_run(&self) -> bool {
        self.try_get_one::<bool>("dry-run")
            .ok()
            .flatten()
            .copied()
            .unwrap_or(false)
    }
}

fn header(name: &'static str) -> HeaderName {
    HeaderName::from_static(name)
}

fn value(text: &str) -> anyhow::Result<HeaderValue> {
    HeaderValue::from_str(text)
        .map_err(|_| anyhow::anyhow!("a header value contains characters HTTP cannot carry"))
}

async fn send(call: Call, base: String, headers: HeaderMap) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        // Bounded, because an administrative command is usually run by a person
        // waiting for it, and an unreachable control plane must say so.
        .timeout(Duration::from_secs(30))
        .build()?;
    let url = format!("{base}/{}", call.path);
    let mut request = client
        .request(call.method, &url)
        .headers(headers)
        .query(&call.query);
    if let Some(body) = call.body {
        request = request.body(body);
    }
    let response = request
        .send()
        .await
        .map_err(|error| anyhow::anyhow!("{url} could not be reached: {error}"))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    // Pretty-printed when it is JSON, verbatim otherwise: an operator reads this,
    // and a body that failed to parse is evidence rather than something to hide.
    let rendered = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|json| serde_json::to_string_pretty(&json).ok())
        .unwrap_or(body);
    if status.is_success() {
        println!("{rendered}");
        return Ok(());
    }
    eprintln!("{rendered}");
    anyhow::bail!("the gateway refused this administrative request: {status}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(argv: &[&str]) -> ArgMatches {
        command().try_get_matches_from(argv).expect("valid argv")
    }

    #[test]
    fn every_mounted_mutating_route_is_reachable_from_the_cli() {
        let mounted: Vec<&str> = crate::admin::admin_route_specs()
            .iter()
            .filter(|spec| spec.action == crate::admin::AdminAction::Publish)
            .map(|spec| spec.path.trim_start_matches('/'))
            .collect();
        for path in mounted {
            assert!(
                RESOURCES.iter().any(|(resource, _)| *resource == path),
                "`{path}` is mounted but `axond admin apply --resource {path}` is not accepted"
            );
        }
    }

    #[test]
    fn a_mutation_carries_its_preconditions_and_a_read_carries_none() {
        let env = HashMap::from([(TOKEN_ENV.to_owned(), "secret".to_owned())]);
        let args = matches(&[
            "admin",
            "apply",
            "--resource",
            "tenants",
            "--file",
            "/dev/null",
            "--idempotency-key",
            "key-1",
            "--expected-revision",
            EXPECTED_REVISION_EMPTY,
            "--dry-run",
        ]);
        let sub = args.subcommand().expect("a subcommand").1;
        let sent = headers(&args, sub, &env).expect("headers");
        assert_eq!(sent[IDEMPOTENCY_KEY_HEADER], "key-1");
        assert_eq!(sent[EXPECTED_REVISION_HEADER], EXPECTED_REVISION_EMPTY);
        assert_eq!(sent[DRY_RUN_HEADER], "true");
        assert_eq!(sent[AUTHORIZATION], "Bearer secret");

        let args = matches(&["admin", "history", "--limit", "5"]);
        let sub = args.subcommand().expect("a subcommand").1;
        let sent = headers(&args, sub, &env).expect("headers");
        assert!(!sent.contains_key(IDEMPOTENCY_KEY_HEADER));
        assert!(!sent.contains_key(DRY_RUN_HEADER));
    }

    #[test]
    fn breakglass_attribution_travels_with_any_subcommand() {
        let env = HashMap::from([(TOKEN_ENV.to_owned(), "secret".to_owned())]);
        let args = matches(&[
            "admin",
            "--operator",
            "ops-oncall",
            "--reason",
            "idp outage, ticket OPS-42",
            "convergence",
        ]);
        let sub = args.subcommand().expect("a subcommand").1;
        let sent = headers(&args, sub, &env).expect("headers");
        assert_eq!(sent[BREAKGLASS_OPERATOR_HEADER], "ops-oncall");
        assert_eq!(sent[BREAKGLASS_REASON_HEADER], "idp outage, ticket OPS-42");
    }

    #[test]
    fn a_missing_credential_fails_before_a_connection_is_opened() {
        let args = matches(&["admin", "convergence"]);
        let sub = args.subcommand().expect("a subcommand").1;
        let error = plan("convergence", sub, &HashMap::new()).expect_err("no credential");
        assert!(error.to_string().contains(TOKEN_ENV), "{error}");
    }

    #[test]
    fn read_paths_are_the_mounted_ones() {
        let env = HashMap::from([(TOKEN_ENV.to_owned(), "secret".to_owned())]);
        let args = matches(&["admin", "history", "--limit", "5"]);
        let sub = args.subcommand().expect("a subcommand").1;
        let call = plan("history", sub, &env).expect("a call");
        assert_eq!(call.path, "history");
        assert_eq!(call.query, vec![("limit".to_owned(), "5".to_owned())]);

        let args = matches(&["admin", "audit", "--revision", "rev-7"]);
        let sub = args.subcommand().expect("a subcommand").1;
        assert_eq!(
            plan("audit", sub, &env).expect("a call").path,
            "audit/rev-7"
        );
    }

    #[test]
    fn a_rollback_document_is_built_from_its_flags() {
        let env = HashMap::from([(TOKEN_ENV.to_owned(), "secret".to_owned())]);
        let args = matches(&[
            "admin",
            "rollback",
            "--revision",
            "rev-7",
            "--summary",
            "revert the bad alias",
            "--tenant",
            "t-1",
            "--idempotency-key",
            "key-1",
            "--expected-revision",
            "rev-9",
        ]);
        let sub = args.subcommand().expect("a subcommand").1;
        let call = plan("rollback", sub, &env).expect("a call");
        let body: serde_json::Value =
            serde_json::from_str(&call.body.expect("a body")).expect("json");
        assert_eq!(body["revision"], "rev-7");
        assert_eq!(body["summary"], "revert the bad alias");
        assert_eq!(body["tenant"], "t-1");
        assert!(body.get("project").is_none());
    }

    #[test]
    fn a_project_scope_without_a_tenant_is_refused_by_the_parser() {
        command()
            .try_get_matches_from([
                "admin",
                "rollback",
                "--revision",
                "rev-7",
                "--summary",
                "revert",
                "--project",
                "p-1",
                "--idempotency-key",
                "key-1",
                "--expected-revision",
                "rev-9",
            ])
            .expect_err("a project is meaningless without its tenant");
    }

    #[test]
    fn the_endpoint_falls_back_to_the_environment_then_the_default() {
        let args = matches(&["admin", "convergence"]);
        assert_eq!(
            base(&args, &HashMap::new()).expect("the default endpoint is loopback"),
            format!("{DEFAULT_ENDPOINT}{ADMIN_PREFIX}")
        );
        let env = HashMap::from([(ENDPOINT_ENV.to_owned(), "https://gw.example/".to_owned())]);
        assert_eq!(
            base(&args, &env).expect("https"),
            format!("https://gw.example{ADMIN_PREFIX}")
        );
        let args = matches(&["admin", "--endpoint", "https://flag.example", "convergence"]);
        assert_eq!(
            base(&args, &env).expect("https"),
            format!("https://flag.example{ADMIN_PREFIX}")
        );
    }

    /// The administrative credential is a bearer token for the whole control
    /// plane, so a plaintext hop to another host is refused before the request
    /// is built — from either source of the endpoint, and whatever the reason
    /// for the scheme. Loopback has no hop, and the opt-in is explicit.
    #[test]
    fn a_plaintext_endpoint_off_this_host_is_refused_before_the_token_is_sent() {
        let args = matches(&["admin", "--endpoint", "http://gw.example", "convergence"]);
        let error = base(&args, &HashMap::new())
            .expect_err("a bearer credential is not sent in the clear across a network");
        let message = error.to_string();
        assert!(message.contains("http://gw.example"), "{message}");
        assert!(message.contains("https"), "{message}");

        let env = HashMap::from([(ENDPOINT_ENV.to_owned(), "http://gw.example".to_owned())]);
        base(&matches(&["admin", "convergence"]), &env)
            .expect_err("the environment is no safer than the flag");

        for loopback in [
            "http://127.0.0.1:8080",
            "http://localhost:8080",
            "http://[::1]:8080",
        ] {
            base(
                &matches(&["admin", "--endpoint", loopback, "convergence"]),
                &HashMap::new(),
            )
            .expect("a request to this host reaches no wire");
        }

        let opted_in = matches(&[
            "admin",
            "--endpoint",
            "http://gw.example",
            "--insecure-plaintext",
            "convergence",
        ]);
        assert_eq!(
            base(&opted_in, &HashMap::new()).expect("the operator said so explicitly"),
            format!("http://gw.example{ADMIN_PREFIX}")
        );
    }
}
