//! `axond admin`: the same routes, from a terminal.
//!
//! This is an HTTP client and deliberately nothing more. It does not open the
//! control plane, construct an [`AdminService`], or hold a store, so there is no
//! command here that can publish a revision without an idempotency key, an
//! expected revision, an authenticated identity, and the complete-candidate
//! validation the API performs — the CLI cannot be a second, weaker way in
//! (ADR 0027).
//!
//! Happy-path model changes are `axond admin model apply`: GET
//! `/admin/v1/catalogue` for `x-axond-expected-revision` (`empty` when the
//! control plane has never published), mint `idempotency-key`, and POST
//! `/admin/v1/bindings`. `mutation` is `create` when the alias slug is absent
//! and `update` when it exists. Expert `apply --resource` still sends a
//! caller-authored envelope and still requires those headers on the command
//! line.
//!
//! Mutating expert commands send the same envelope the route parses, read from
//! a file or standard input, rather than reconstructing every resource field as
//! flags: the schema then has exactly one definition, and `--dry-run` against a
//! real deployment is how an operator checks a document before applying it.
//!
//! Budgets and limits are policy fields, so they are `policies` documents;
//! `axond admin resources` prints the mapping rather than leaving it to be
//! rediscovered from a 404.
//!
//! [`AdminService`]: super::service::AdminService

use std::collections::HashMap;
use std::io::Read;
use std::time::Duration;

use clap::{Arg, ArgAction, ArgGroup, ArgMatches, Command};
use reqwest::Method;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use ring::rand::{SecureRandom, SystemRandom};
use serde::Deserialize;

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
    (
        "bindings",
        "one imported model expanded into a catalogue pin, enablement, price, and alias",
    ),
    ("tenants", "a tenant and its lifecycle state"),
    ("projects", "a project (namespace) inside a tenant"),
    (
        "principals",
        "a durable workload or human identity and its administrative grants",
    ),
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
        "prices",
        "a deployment price book and effective approved rates",
    ),
    (
        "policies",
        "budgets, concurrency limits, and revocation for a scope",
    ),
];

/// `axond admin secret`: the credential lifecycle, without a redeploy.
///
/// Material is read from a file or standard input, never from a flag: a flag is
/// in the shell history and in every `ps` listing on the host, which is the same
/// reason the administrative credential is an environment variable. A single
/// trailing newline is stripped, because `echo` and every editor add one and a
/// provider key does not end in one.
fn secret_command() -> Command {
    let tenant = || {
        Arg::new("tenant")
            .long("tenant")
            .required(true)
            .help("Tenant that owns the material")
    };
    let project = || {
        Arg::new("project")
            .long("project")
            .help("Project that owns the material; requires --tenant")
    };
    let material = || {
        Arg::new("material-file")
            .long("material-file")
            .short('f')
            .help("File holding the material; `-` (the default) reads standard input")
    };
    let reference = |help: &'static str| {
        Arg::new("reference")
            .long("reference")
            .required(true)
            .help(help)
    };
    Command::new("secret")
        .about("Store, rotate, and withdraw credential material")
        .long_about(
            "Store, rotate, and withdraw credential material.\n\nNothing here publishes a \
             revision: a credential document pinning a new version is `axond admin apply \
             --resource credentials`, with its own idempotency key and expected revision. And \
             nothing here reads material back \u{2014} there is no route that returns it.",
        )
        .subcommand_required(true)
        .subcommand(
            Command::new("stage")
                .about("Store material as a new secret's first version, staged")
                .arg(tenant())
                .arg(project())
                .arg(material()),
        )
        .subcommand(
            Command::new("rotate")
                .about("Store material as the next version of an existing secret, staged")
                .arg(tenant())
                .arg(project())
                .arg(reference(
                    "The exact version being rotated from, as `sct_\u{2026}@v1`",
                ))
                .arg(material()),
        )
        .subcommand(
            Command::new("lifecycle")
                .about("Activate, disable, revoke, or destroy one version's material")
                .arg(tenant())
                .arg(project())
                .arg(reference("The exact version to move, as `sct_\u{2026}@v2`"))
                .arg(
                    Arg::new("state")
                        .long("state")
                        .required(true)
                        .value_parser(["staged", "active", "disabled", "revoked", "tombstoned"])
                        .help("The state to move the version to"),
                ),
        )
        .subcommand(
            Command::new("versions")
                .about("Every version of one secret and the state each is in")
                .arg(
                    Arg::new("secret")
                        .long("secret")
                        .required(true)
                        .help("The secret to list, as `sct_\u{2026}`"),
                )
                .arg(tenant())
                .arg(project()),
        )
}

/// `axond admin model`: make a published model id callable, without authoring
/// the four-resource graph.
///
/// Apply still goes through `/admin/v1/bindings`. The CLI fills the protocol
/// preconditions so an operator is not asked for a revision id or a key.
fn model_command() -> Command {
    let tenant = || {
        Arg::new("tenant")
            .long("tenant")
            .required(true)
            .help("Tenant id the binding is for")
    };
    let project = || {
        Arg::new("project")
            .long("project")
            .help("Project id the alias is for; omitted, the tenant's only project")
    };
    let dry_run = || {
        Arg::new("dry-run")
            .long("dry-run")
            .action(ArgAction::SetTrue)
            .help("Validate and diff the candidate without publishing anything")
    };
    Command::new("model")
        .about("Make a published model id available, priced, and named on /v1/models")
        .long_about(
            "Make a published model id available, priced, and named on /v1/models.\n\n\
             `apply` GETs /admin/v1/catalogue for x-axond-expected-revision (or `empty`), \
             mints the idempotency key, and POSTs /admin/v1/bindings. It does not open \
             the control plane. Omit --name and the alias is the published model id \
             (for example gpt-4o).",
        )
        .subcommand_required(true)
        .subcommand(
            Command::new("apply")
                .about("Publish one [[model]] fragment as a binding")
                .arg(tenant())
                .arg(project())
                .arg(
                    Arg::new("file")
                        .long("file")
                        .short('f')
                        .help(
                            "A [[model]] TOML fragment (not a full axond.toml), or a binding JSON \
                             object",
                        ),
                )
                .arg(
                    Arg::new("target")
                        .long("target")
                        .help("Connection and published id as provider:model (for example openai:gpt-4o)"),
                )
                .arg(
                    Arg::new("from-catalogue")
                        .long("from-catalogue")
                        .requires("provider")
                        .help(
                            "Catalogue offering as provider/model (for example openai/gpt-4o); \
                             requires --provider for the connection",
                        ),
                )
                .arg(
                    Arg::new("provider")
                        .long("provider")
                        .requires("from-catalogue")
                        .help(
                            "Connection slug for --from-catalogue; not inferred from the \
                             catalogue id",
                        ),
                )
                .arg(
                    Arg::new("name")
                        .long("name")
                        .help("Caller-facing alias; omit to use the published model id"),
                )
                .arg(
                    Arg::new("price")
                        .long("price")
                        .value_parser(["observed"])
                        .conflicts_with_all(["price-input", "price-output"])
                        .help("Adopt catalogue rates; not a default"),
                )
                .arg(
                    Arg::new("price-input")
                        .long("price-input")
                        .requires("price-output")
                        .help("Input rate in micro-dollars per million tokens"),
                )
                .arg(
                    Arg::new("price-output")
                        .long("price-output")
                        .requires("price-input")
                        .help("Output rate in micro-dollars per million tokens"),
                )
                .arg(
                    Arg::new("pin")
                        .long("pin")
                        .value_parser(["follow", "lock"])
                        .help("Catalogue pin; default follow"),
                )
                .arg(
                    Arg::new("summary")
                        .long("summary")
                        .help("Why: recorded in the audit trail"),
                )
                .arg(dry_run())
                .group(
                    ArgGroup::new("document")
                        .args(["file", "target", "from-catalogue"])
                        .required(true),
                ),
        )
        .subcommand(
            Command::new("disable")
                .about("Withdraw an alias without deleting it")
                .arg(tenant())
                .arg(project())
                .arg(
                    Arg::new("name")
                        .long("name")
                        .required(true)
                        .help("Caller-facing alias to disable"),
                )
                .arg(
                    Arg::new("target")
                        .long("target")
                        .help("Connection and published id as provider:model, if catalogue metadata is pending"),
                )
                .arg(
                    Arg::new("summary")
                        .long("summary")
                        .help("Why: recorded in the audit trail"),
                )
                .arg(dry_run()),
        )
        .subcommand(
            Command::new("price")
                .about("Change stated rates on an existing binding")
                .arg(tenant())
                .arg(project())
                .arg(
                    Arg::new("name")
                        .long("name")
                        .required(true)
                        .help("Caller-facing alias to reprice"),
                )
                .arg(
                    Arg::new("input")
                        .long("input")
                        .required(true)
                        .help("Input rate in micro-dollars per million tokens"),
                )
                .arg(
                    Arg::new("output")
                        .long("output")
                        .required(true)
                        .help("Output rate in micro-dollars per million tokens"),
                )
                .arg(
                    Arg::new("target")
                        .long("target")
                        .help("Connection and published id as provider:model, if catalogue metadata is pending"),
                )
                .arg(
                    Arg::new("summary")
                        .long("summary")
                        .help("Why: recorded in the audit trail"),
                )
                .arg(dry_run()),
        )
        .subcommand(
            Command::new("show")
                .about("One alias and its catalogue rows")
                .arg(tenant())
                .arg(project())
                .arg(
                    Arg::new("name")
                        .long("name")
                        .required(true)
                        .help("Caller-facing alias to show"),
                ),
        )
}

/// `axond admin catalog`: imported browse and a manual refresh.
///
/// Browse is GET /admin/v1/catalogue with `source=imported`. Refresh is POST
/// /admin/v1/catalogue/refresh: it writes, it does not mutate desired state, so
/// it carries no idempotency key and no expected revision.
fn catalog_command() -> Command {
    Command::new("catalog")
        .about("Browse imported offerings and refresh the active catalogue")
        .subcommand_required(true)
        .subcommand(
            Command::new("browse")
                .about("Search the imported catalogue")
                .arg(
                    Arg::new("tenant")
                        .long("tenant")
                        .required(true)
                        .help("Tenant id to read as"),
                )
                .arg(
                    Arg::new("project")
                        .long("project")
                        .help("Project id to scope to; requires --tenant"),
                )
                .arg(
                    Arg::new("provider")
                        .long("provider")
                        .help("Catalogue provider id to narrow to"),
                )
                .arg(
                    Arg::new("q")
                        .long("q")
                        .help("Substring match over imported provider, model, and display name"),
                )
                .group(
                    ArgGroup::new("imported")
                        .args(["provider", "q"])
                        .required(true)
                        .multiple(true),
                ),
        )
        .subcommand(Command::new("refresh").about("Import now; last-known-good stays on refusal"))
}

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
        .subcommand(model_command())
        .subcommand(catalog_command())
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
        .subcommand(secret_command())
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
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        match name {
            "model" => run_model(args, sub, &env).await,
            "catalog" => run_catalog(args, sub, &env).await,
            name => {
                let call = plan(name, sub, &env)?;
                send(call, base(args, &env)?, headers(args, sub, &env)?).await
            }
        }
    })
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
        "secret" => secret_call(args)?,
        other => unreachable!("clap validates subcommands: {other}"),
    };
    require_token(env)?;
    Ok(call)
}

fn require_token(env: &HashMap<String, String>) -> anyhow::Result<()> {
    // Fail before a connection is opened against a deployment that would answer 401.
    if !env.contains_key(TOKEN_ENV) {
        anyhow::bail!("${TOKEN_ENV} is not set: `axond admin` needs an administrative credential");
    }
    Ok(())
}

/// One binding the CLI is about to POST. `name` omitted is the happy path: the
/// server defaults the alias to the published model id.
#[derive(Debug, Clone, PartialEq)]
struct BindingSpec {
    tenant: String,
    project: Option<String>,
    name: Option<String>,
    alias: String,
    pin: Option<String>,
    state: Option<String>,
    summary: String,
    /// Stated rates kept here so `price` without `--target` still lands on the
    /// catalogue-filled target.
    price: Option<serde_json::Value>,
    targets: Vec<serde_json::Value>,
}

impl BindingSpec {
    fn envelope(&self, mutation: &str) -> serde_json::Value {
        let mut resource = serde_json::Map::new();
        resource.insert("tenant".to_owned(), serde_json::json!(self.tenant));
        if let Some(project) = &self.project {
            resource.insert("project".to_owned(), serde_json::json!(project));
        }
        if let Some(name) = &self.name {
            resource.insert("name".to_owned(), serde_json::json!(name));
        }
        if let Some(state) = &self.state {
            resource.insert("state".to_owned(), serde_json::json!(state));
        }
        if let Some(pin) = &self.pin {
            resource.insert("pin".to_owned(), serde_json::json!(pin));
        }
        resource.insert("targets".to_owned(), serde_json::json!(self.targets));
        serde_json::json!({
            "summary": self.summary,
            "mutation": mutation,
            "resource": resource,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ModelFragmentFile {
    #[serde(default, rename = "model")]
    model: Vec<TomlModel>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlModel {
    #[serde(default)]
    name: Option<String>,
    /// Accepted so a `[[model]]` copied from file config does not explode;
    /// scope is `--tenant` / `--project`, not this string.
    #[allow(dead_code)]
    #[serde(default)]
    namespace: Option<String>,
    #[serde(default)]
    pin: Option<String>,
    #[serde(default)]
    state: Option<String>,
    targets: Vec<TomlTarget>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlTarget {
    provider: String,
    model: String,
    #[serde(default)]
    catalog: Option<TomlCatalog>,
    #[serde(default)]
    price: Option<TomlPrice>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TomlCatalog {
    #[serde(default)]
    provider: Option<String>,
    #[serde(default)]
    model: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TomlPrice {
    Stated {
        input_microdollars_per_million: u64,
        output_microdollars_per_million: u64,
    },
    Observed(String),
}

async fn run_model(
    global: &ArgMatches,
    args: &ArgMatches,
    env: &HashMap<String, String>,
) -> anyhow::Result<()> {
    require_token(env)?;
    let (verb, verb_args) = args
        .subcommand()
        .expect("clap requires an `axond admin model` subcommand");
    let endpoint = base(global, env)?;
    match verb {
        "show" => {
            let tenant = required(verb_args, "tenant");
            let project = verb_args.get_one::<String>("project").map(String::as_str);
            let name = required(verb_args, "name");
            let (catalogue, _) = catalogue_for_alias(
                tenant,
                project,
                name,
                endpoint,
                headers(global, verb_args, env)?,
            )
            .await?;
            let body = show_document(&catalogue, name)?;
            println!("{}", serde_json::to_string_pretty(&body)?);
            Ok(())
        }
        "apply" => {
            let spec = apply_spec(verb_args)?;
            let catalogue = fetch_json(
                catalogue_call(&spec.tenant, spec.project.as_deref(), Vec::new()),
                endpoint.clone(),
                headers(global, verb_args, env)?,
            )
            .await?;
            let mutation = if alias_published(&catalogue, &spec.alias) {
                "update"
            } else {
                "create"
            };
            post_binding(
                spec,
                mutation,
                expected_revision_of(&catalogue),
                global,
                verb_args,
                env,
                endpoint,
            )
            .await
        }
        "disable" | "price" => {
            let mut spec = match verb {
                "disable" => disable_spec(verb_args)?,
                "price" => price_spec(verb_args)?,
                _ => unreachable!("clap validates subcommands: {verb}"),
            };
            let (catalogue, project) = catalogue_for_alias(
                &spec.tenant,
                spec.project.as_deref(),
                &spec.alias,
                endpoint.clone(),
                headers(global, verb_args, env)?,
            )
            .await?;
            if spec.project.is_none() {
                spec.project = project;
            }
            hydrate_from_catalogue(&mut spec, &catalogue)?;
            post_binding(
                spec,
                "update",
                expected_revision_of(&catalogue),
                global,
                verb_args,
                env,
                endpoint,
            )
            .await
        }
        other => unreachable!("clap validates subcommands: {other}"),
    }
}

async fn post_binding(
    spec: BindingSpec,
    mutation: &str,
    expected: String,
    global: &ArgMatches,
    args: &ArgMatches,
    env: &HashMap<String, String>,
    endpoint: String,
) -> anyhow::Result<()> {
    let call = Call {
        method: Method::POST,
        path: "bindings".to_owned(),
        query: Vec::new(),
        body: Some(spec.envelope(mutation).to_string()),
    };
    send(
        call,
        endpoint,
        mutation_headers(global, args, env, &mint_idempotency_key()?, &expected)?,
    )
    .await
}

async fn run_catalog(
    global: &ArgMatches,
    args: &ArgMatches,
    env: &HashMap<String, String>,
) -> anyhow::Result<()> {
    require_token(env)?;
    let (verb, verb_args) = args
        .subcommand()
        .expect("clap requires an `axond admin catalog` subcommand");
    let call = match verb {
        "browse" => browse_call(verb_args)?,
        "refresh" => Call {
            method: Method::POST,
            path: "catalogue/refresh".to_owned(),
            query: Vec::new(),
            body: None,
        },
        other => unreachable!("clap validates subcommands: {other}"),
    };
    send(call, base(global, env)?, headers(global, verb_args, env)?).await
}

fn apply_spec(args: &ArgMatches) -> anyhow::Result<BindingSpec> {
    let mut spec = if let Some(path) = args.get_one::<String>("file") {
        spec_from_file(path, args)?
    } else if let Some(from) = args.get_one::<String>("from-catalogue") {
        spec_from_catalogue(from, args)?
    } else {
        spec_from_target(args)?
    };
    if let Some(name) = args.get_one::<String>("name") {
        spec.name = Some(name.clone());
        spec.alias = name.clone();
    }
    if let Some(pin) = args.get_one::<String>("pin") {
        spec.pin = Some(pin.clone());
    }
    if let Some(summary) = args.get_one::<String>("summary") {
        spec.summary = summary.clone();
    }
    if spec.summary.is_empty() {
        spec.summary = format!("enable {}", spec.alias);
    }
    Ok(spec)
}

fn disable_spec(args: &ArgMatches) -> anyhow::Result<BindingSpec> {
    let name = required(args, "name").to_owned();
    let targets = match args.get_one::<String>("target") {
        Some(target) => {
            let (provider, model) = split_once(target, ':', "--target")?;
            vec![target_json(provider, model, None, None)]
        }
        None => Vec::new(),
    };
    Ok(BindingSpec {
        tenant: required(args, "tenant").to_owned(),
        project: args.get_one::<String>("project").cloned(),
        name: Some(name.clone()),
        alias: name.clone(),
        pin: None,
        state: Some("disabled".to_owned()),
        summary: args
            .get_one::<String>("summary")
            .cloned()
            .unwrap_or_else(|| format!("disable {name}")),
        price: None,
        targets,
    })
}

fn price_spec(args: &ArgMatches) -> anyhow::Result<BindingSpec> {
    let name = required(args, "name").to_owned();
    let price = stated_price(
        parse_u64("--input", required(args, "input"))?,
        parse_u64("--output", required(args, "output"))?,
    );
    let targets = match args.get_one::<String>("target") {
        Some(target) => {
            let (provider, model) = split_once(target, ':', "--target")?;
            vec![target_json(provider, model, None, Some(price.clone()))]
        }
        None => Vec::new(),
    };
    Ok(BindingSpec {
        tenant: required(args, "tenant").to_owned(),
        project: args.get_one::<String>("project").cloned(),
        name: Some(name.clone()),
        alias: name.clone(),
        pin: None,
        state: None,
        summary: args
            .get_one::<String>("summary")
            .cloned()
            .unwrap_or_else(|| format!("price {name}")),
        price: Some(price),
        targets,
    })
}

fn spec_from_file(path: &str, args: &ArgMatches) -> anyhow::Result<BindingSpec> {
    let text = document(Some(path))?;
    let trimmed = text.trim_start();
    if trimmed.starts_with('{') {
        spec_from_json(&text, args)
    } else {
        spec_from_toml(&text, args)
    }
}

fn spec_from_toml(text: &str, args: &ArgMatches) -> anyhow::Result<BindingSpec> {
    let parsed: ModelFragmentFile = toml::from_str(text).map_err(|error| {
        anyhow::anyhow!("TOML is a [[model]] fragment, not a full axond.toml: {error}")
    })?;
    let [model] = parsed.model.as_slice() else {
        anyhow::bail!("TOML must contain exactly one [[model]] table");
    };
    if model.targets.is_empty() {
        anyhow::bail!("[[model]] must list at least one target");
    }
    let published = published_id(&model.targets[0].model, model.targets[0].catalog.as_ref());
    let name = model.name.clone().filter(|name| !name.is_empty());
    let alias = name.clone().unwrap_or_else(|| published.to_owned());
    if alias.is_empty() {
        anyhow::bail!("a published model id is required");
    }
    let mut targets = Vec::with_capacity(model.targets.len());
    for target in &model.targets {
        let price = match &target.price {
            None => None,
            Some(TomlPrice::Observed(token)) => {
                if token != "observed" {
                    anyhow::bail!("price must be a rate object or the token `observed`");
                }
                Some(serde_json::json!("observed"))
            }
            Some(TomlPrice::Stated {
                input_microdollars_per_million,
                output_microdollars_per_million,
            }) => Some(stated_price(
                *input_microdollars_per_million,
                *output_microdollars_per_million,
            )),
        };
        let catalog = target.catalog.as_ref().map(|catalog| {
            let mut object = serde_json::Map::new();
            if let Some(provider) = &catalog.provider {
                object.insert("provider".to_owned(), serde_json::json!(provider));
            }
            if let Some(model) = &catalog.model {
                object.insert("model".to_owned(), serde_json::json!(model));
            }
            serde_json::Value::Object(object)
        });
        let mut json = serde_json::json!({
            "provider": target.provider,
            "model": target.model,
        });
        if let Some(catalog) = catalog {
            json["catalog"] = catalog;
        }
        if let Some(price) = price {
            json["price"] = price;
        }
        targets.push(json);
    }
    Ok(BindingSpec {
        tenant: required(args, "tenant").to_owned(),
        project: args.get_one::<String>("project").cloned(),
        name,
        alias,
        pin: model.pin.clone(),
        state: model.state.clone(),
        summary: String::new(),
        price: None,
        targets,
    })
}

fn spec_from_json(text: &str, args: &ArgMatches) -> anyhow::Result<BindingSpec> {
    let value: serde_json::Value = serde_json::from_str(text)
        .map_err(|error| anyhow::anyhow!("binding JSON is invalid: {error}"))?;
    let resource = value.get("resource").unwrap_or(&value);
    let tenant = args
        .get_one::<String>("tenant")
        .cloned()
        .or_else(|| {
            resource
                .get("tenant")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| anyhow::anyhow!("--tenant is required"))?;
    let project = args.get_one::<String>("project").cloned().or_else(|| {
        resource
            .get("project")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    });
    let model_one = json_one_model(resource)?;
    let targets = json_targets(resource, model_one)?;
    let first = &targets[0];
    let model = first
        .get("model")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let catalog_model = first
        .get("catalog")
        .and_then(|catalog| catalog.get("model"))
        .and_then(serde_json::Value::as_str);
    let published = catalog_model.unwrap_or(model);
    let pick = |key: &str| {
        resource
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .or_else(|| {
                model_one
                    .and_then(|model| model.get(key))
                    .and_then(serde_json::Value::as_str)
                    .filter(|value| !value.is_empty())
            })
            .map(str::to_owned)
    };
    let name = pick("name");
    let alias = name.clone().unwrap_or_else(|| published.to_owned());
    if alias.is_empty() {
        anyhow::bail!("a published model id is required");
    }
    Ok(BindingSpec {
        tenant,
        project,
        name,
        alias,
        pin: pick("pin"),
        state: pick("state"),
        summary: value
            .get("summary")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        price: None,
        targets,
    })
}

fn json_one_model(resource: &serde_json::Value) -> anyhow::Result<Option<&serde_json::Value>> {
    let Some(models) = resource.get("models") else {
        return Ok(None);
    };
    let Some(models) = models.as_array() else {
        anyhow::bail!("binding JSON `models` must be an array");
    };
    match models.as_slice() {
        [one] => Ok(Some(one)),
        _ => anyhow::bail!("binding JSON models must contain exactly one model"),
    }
}

fn json_targets(
    resource: &serde_json::Value,
    model: Option<&serde_json::Value>,
) -> anyhow::Result<Vec<serde_json::Value>> {
    if let Some(targets) = resource.get("targets") {
        let Some(targets) = targets.as_array() else {
            anyhow::bail!("binding JSON `targets` must be an array");
        };
        if targets.is_empty() {
            anyhow::bail!("binding JSON must contain at least one target");
        }
        return Ok(targets.clone());
    }
    let Some(model) = model else {
        anyhow::bail!("binding JSON must contain targets or models");
    };
    let targets = model
        .get("targets")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("binding JSON must contain at least one target"))?;
    if targets.is_empty() {
        anyhow::bail!("binding JSON must contain at least one target");
    }
    Ok(targets)
}

fn spec_from_catalogue(from: &str, args: &ArgMatches) -> anyhow::Result<BindingSpec> {
    let (catalog_provider, published) = split_once(from, '/', "--from-catalogue")?;
    let provider = required(args, "provider");
    let price = price_from_apply_flags(args)?;
    let catalog = Some(serde_json::json!({
        "provider": catalog_provider,
        "model": published,
    }));
    Ok(BindingSpec {
        tenant: required(args, "tenant").to_owned(),
        project: args.get_one::<String>("project").cloned(),
        name: None,
        alias: published.to_owned(),
        pin: None,
        state: None,
        summary: String::new(),
        price: None,
        targets: vec![target_json(provider, published, catalog, price)],
    })
}

fn spec_from_target(args: &ArgMatches) -> anyhow::Result<BindingSpec> {
    let target = required(args, "target");
    let (provider, model) = split_once(target, ':', "--target")?;
    let price = price_from_apply_flags(args)?;
    Ok(BindingSpec {
        tenant: required(args, "tenant").to_owned(),
        project: args.get_one::<String>("project").cloned(),
        name: None,
        alias: model.to_owned(),
        pin: None,
        state: None,
        summary: String::new(),
        price: None,
        targets: vec![target_json(provider, model, None, price)],
    })
}

fn price_from_apply_flags(args: &ArgMatches) -> anyhow::Result<Option<serde_json::Value>> {
    if args.get_one::<String>("price").is_some() {
        return Ok(Some(serde_json::json!("observed")));
    }
    match (
        args.get_one::<String>("price-input"),
        args.get_one::<String>("price-output"),
    ) {
        (Some(input), Some(output)) => Ok(Some(stated_price(
            parse_u64("--price-input", input)?,
            parse_u64("--price-output", output)?,
        ))),
        (None, None) => Ok(None),
        _ => anyhow::bail!("--price-input and --price-output must be passed together"),
    }
}

fn stated_price(input: u64, output: u64) -> serde_json::Value {
    serde_json::json!({
        "input_microdollars_per_million": input,
        "output_microdollars_per_million": output,
    })
}

fn target_json(
    provider: &str,
    model: &str,
    catalog: Option<serde_json::Value>,
    price: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut target = serde_json::json!({
        "provider": provider,
        "model": model,
    });
    if let Some(catalog) = catalog {
        target["catalog"] = catalog;
    }
    if let Some(price) = price {
        target["price"] = price;
    }
    target
}

fn published_id<'a>(model: &'a str, catalog: Option<&'a TomlCatalog>) -> &'a str {
    catalog
        .and_then(|catalog| catalog.model.as_deref())
        .unwrap_or(model)
}

fn split_once<'a>(value: &'a str, sep: char, flag: &str) -> anyhow::Result<(&'a str, &'a str)> {
    let (left, right) = value.split_once(sep).ok_or_else(|| {
        anyhow::anyhow!("{flag} expects a value separated by `{sep}`, not `{value}`")
    })?;
    if left.is_empty() || right.is_empty() {
        anyhow::bail!("{flag} expects a value separated by `{sep}`, not `{value}`");
    }
    Ok((left, right))
}

fn parse_u64(flag: &str, value: &str) -> anyhow::Result<u64> {
    value
        .parse()
        .map_err(|_| anyhow::anyhow!("{flag} is not a micro-dollar amount: `{value}`"))
}

fn catalogue_call(tenant: &str, project: Option<&str>, extra: Vec<(String, String)>) -> Call {
    let mut query = vec![("tenant".to_owned(), tenant.to_owned())];
    if let Some(project) = project {
        query.push(("project".to_owned(), project.to_owned()));
    }
    query.extend(extra);
    Call {
        method: Method::GET,
        path: "catalogue".to_owned(),
        query,
        body: None,
    }
}

fn browse_call(args: &ArgMatches) -> anyhow::Result<Call> {
    if let Some(q) = args.get_one::<String>("q") {
        let chars = q.chars().count();
        if chars < 3 {
            anyhow::bail!("--q must be at least 3 characters");
        }
    }
    let mut extra = vec![("source".to_owned(), "imported".to_owned())];
    if let Some(provider) = args.get_one::<String>("provider") {
        extra.push(("provider".to_owned(), provider.clone()));
    }
    if let Some(q) = args.get_one::<String>("q") {
        extra.push(("q".to_owned(), q.clone()));
    }
    Ok(catalogue_call(
        required(args, "tenant"),
        args.get_one::<String>("project").map(String::as_str),
        extra,
    ))
}

/// Catalogue `revision`, or `empty` when the control plane has never published.
fn expected_revision_of(catalogue: &serde_json::Value) -> String {
    catalogue
        .get("revision")
        .and_then(serde_json::Value::as_str)
        .filter(|revision| !revision.is_empty())
        .unwrap_or(EXPECTED_REVISION_EMPTY)
        .to_owned()
}

fn alias_published(catalogue: &serde_json::Value, slug: &str) -> bool {
    !matching_aliases(catalogue, slug).is_empty()
}

fn matching_aliases<'a>(
    catalogue: &'a serde_json::Value,
    slug: &str,
) -> Vec<&'a serde_json::Value> {
    catalogue
        .get("aliases")
        .and_then(serde_json::Value::as_array)
        .map(|aliases| {
            aliases
                .iter()
                .filter(|alias| alias.get("slug").and_then(serde_json::Value::as_str) == Some(slug))
                .collect()
        })
        .unwrap_or_default()
}

/// Project on the unique alias with this slug. `None` is a tenant-default alias.
///
/// Tenant-wide catalogue lists every project's aliases but only tenant-default
/// enablement entries. Bindings publish project-owned enablements, so disable
/// and price re-GET with this project before reading `entries`.
fn unique_alias_project(
    catalogue: &serde_json::Value,
    slug: &str,
) -> anyhow::Result<Option<String>> {
    match matching_aliases(catalogue, slug).as_slice() {
        [] => anyhow::bail!("alias `{slug}` is not published in this catalogue"),
        [alias] => Ok(alias
            .get("scope")
            .and_then(|scope| scope.get("project"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)),
        _ => anyhow::bail!("alias `{slug}` is published in more than one project; pass --project"),
    }
}

async fn catalogue_for_alias(
    tenant: &str,
    project: Option<&str>,
    slug: &str,
    endpoint: String,
    headers: HeaderMap,
) -> anyhow::Result<(serde_json::Value, Option<String>)> {
    let catalogue = fetch_json(
        catalogue_call(tenant, project, Vec::new()),
        endpoint.clone(),
        headers.clone(),
    )
    .await?;
    if let Some(project) = project {
        if !alias_published(&catalogue, slug) {
            anyhow::bail!("alias `{slug}` is not published in this catalogue");
        }
        return Ok((catalogue, Some(project.to_owned())));
    }
    let Some(resolved) = unique_alias_project(&catalogue, slug)? else {
        return Ok((catalogue, None));
    };
    let scoped = fetch_json(
        catalogue_call(tenant, Some(&resolved), Vec::new()),
        endpoint,
        headers,
    )
    .await?;
    Ok((scoped, Some(resolved)))
}

fn hydrate_from_catalogue(
    spec: &mut BindingSpec,
    catalogue: &serde_json::Value,
) -> anyhow::Result<()> {
    // Price must not re-enable: omitted state is enabled on the expander.
    if spec.state.is_none() {
        spec.state = matching_aliases(catalogue, &spec.alias)
            .first()
            .and_then(|alias| alias.get("state"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
    }
    if spec.targets.is_empty() {
        spec.targets = vec![target_from_catalogue(catalogue, &spec.alias)?];
    }
    if let Some(price) = spec.price.clone() {
        spec.targets
            .first_mut()
            .ok_or_else(|| anyhow::anyhow!("binding has no target to price"))?["price"] = price;
    }
    Ok(())
}

fn target_from_catalogue(
    catalogue: &serde_json::Value,
    slug: &str,
) -> anyhow::Result<serde_json::Value> {
    let entries = catalogue
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let enablement = matching_aliases(catalogue, slug)
        .first()
        .and_then(|alias| alias.get("targets"))
        .and_then(serde_json::Value::as_array)
        .and_then(|targets| targets.first())
        .and_then(|target| target.get("enablement"))
        .and_then(serde_json::Value::as_str);
    let entry = enablement
        .and_then(|id| {
            entries.iter().find(|entry| {
                entry.get("enablement").and_then(serde_json::Value::as_str) == Some(id)
            })
        })
        .or_else(|| {
            entries.iter().find(|entry| {
                entry
                    .get("aliases")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|aliases| aliases.iter().any(|name| name.as_str() == Some(slug)))
            })
        });
    let Some(metadata) = entry.and_then(|entry| entry.get("metadata")) else {
        anyhow::bail!(
            "catalogue metadata for `{slug}` is not available; pass --target provider:model"
        );
    };
    let provider = metadata
        .get("provider")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("catalogue metadata for `{slug}` has no provider"))?;
    let model = metadata
        .get("published_model")
        .and_then(serde_json::Value::as_str)
        .or_else(|| metadata.get("model").and_then(serde_json::Value::as_str))
        .ok_or_else(|| anyhow::anyhow!("catalogue metadata for `{slug}` has no model"))?;
    Ok(target_json(provider, model, None, None))
}

fn show_document(catalogue: &serde_json::Value, name: &str) -> anyhow::Result<serde_json::Value> {
    let alias = catalogue
        .get("aliases")
        .and_then(serde_json::Value::as_array)
        .and_then(|aliases| {
            aliases
                .iter()
                .find(|alias| alias.get("slug").and_then(serde_json::Value::as_str) == Some(name))
                .cloned()
        });
    let entries = catalogue
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| {
            entry.get("slug").and_then(serde_json::Value::as_str) == Some(name)
                || entry
                    .get("aliases")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|aliases| aliases.iter().any(|alias| alias.as_str() == Some(name)))
        })
        .collect::<Vec<_>>();
    if alias.is_none() && entries.is_empty() {
        anyhow::bail!("alias `{name}` is not in the catalogue");
    }
    Ok(serde_json::json!({
        "revision": catalogue.get("revision"),
        "alias": alias,
        "entries": entries,
    }))
}

fn mint_idempotency_key() -> anyhow::Result<String> {
    let mut bytes = [0u8; 16];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| anyhow::anyhow!("could not mint an idempotency key"))?;
    Ok(hex_lower(&bytes))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn mutation_headers(
    global: &ArgMatches,
    args: &ArgMatches,
    env: &HashMap<String, String>,
    idempotency_key: &str,
    expected_revision: &str,
) -> anyhow::Result<HeaderMap> {
    let mut headers = headers(global, args, env)?;
    headers.insert(header(IDEMPOTENCY_KEY_HEADER), value(idempotency_key)?);
    headers.insert(header(EXPECTED_REVISION_HEADER), value(expected_revision)?);
    Ok(headers)
}

/// One `axond admin secret` call.
fn secret_call(args: &ArgMatches) -> anyhow::Result<Call> {
    let (name, args) = args
        .subcommand()
        .expect("clap requires an `axond admin secret` subcommand");
    let mut body = serde_json::Map::new();
    if name != "versions" {
        body.insert(
            "tenant".to_owned(),
            serde_json::Value::String(required(args, "tenant").to_owned()),
        );
        if let Some(project) = args.get_one::<String>("project") {
            body.insert(
                "project".to_owned(),
                serde_json::Value::String(project.clone()),
            );
        }
    }
    let call = match name {
        "stage" => {
            body.insert("material".to_owned(), material(args)?);
            Call {
                method: Method::POST,
                path: "secrets".to_owned(),
                query: Vec::new(),
                body: Some(serde_json::Value::Object(body).to_string()),
            }
        }
        "rotate" => {
            body.insert(
                "reference".to_owned(),
                serde_json::Value::String(required(args, "reference").to_owned()),
            );
            body.insert("material".to_owned(), material(args)?);
            Call {
                method: Method::POST,
                path: "secrets/rotate".to_owned(),
                query: Vec::new(),
                body: Some(serde_json::Value::Object(body).to_string()),
            }
        }
        "lifecycle" => {
            body.insert(
                "reference".to_owned(),
                serde_json::Value::String(required(args, "reference").to_owned()),
            );
            body.insert(
                "lifecycle".to_owned(),
                serde_json::Value::String(required(args, "state").to_owned()),
            );
            Call {
                method: Method::POST,
                path: "secrets/lifecycle".to_owned(),
                query: Vec::new(),
                body: Some(serde_json::Value::Object(body).to_string()),
            }
        }
        "versions" => {
            let mut query = vec![("tenant".to_owned(), required(args, "tenant").to_owned())];
            if let Some(project) = args.get_one::<String>("project") {
                query.push(("project".to_owned(), project.clone()));
            }
            Call {
                method: Method::GET,
                path: format!("secrets/{}", required(args, "secret")),
                query,
                body: None,
            }
        }
        other => unreachable!("clap validates subcommands: {other}"),
    };
    Ok(call)
}

/// The material a secret command sends, from a file or standard input.
///
/// One trailing newline is stripped and nothing else is: material is bytes an
/// operator pasted, and trimming it further would silently store something other
/// than the key they hold.
fn material(args: &ArgMatches) -> anyhow::Result<serde_json::Value> {
    let mut material = document(args.get_one::<String>("material-file").map(String::as_str))?;
    if material.ends_with('\n') {
        material.pop();
        if material.ends_with('\r') {
            material.pop();
        }
    }
    if material.is_empty() {
        anyhow::bail!("no material was read: pass --material-file, or pipe it on standard input");
    }
    Ok(serde_json::Value::String(material))
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

struct Exchange {
    ok: bool,
    status: reqwest::StatusCode,
    raw: String,
}

const ADMIN_CALL_TIMEOUT: Duration = Duration::from_secs(30);
/// Import bound is 60s (`default_catalog_refresh_timeout_seconds`); hydrate
/// and the response must still fit after that.
const CATALOG_REFRESH_TIMEOUT: Duration = Duration::from_secs(90);

fn call_timeout(path: &str) -> Duration {
    if path == "catalogue/refresh" {
        CATALOG_REFRESH_TIMEOUT
    } else {
        ADMIN_CALL_TIMEOUT
    }
}

async fn exchange(call: Call, base: String, headers: HeaderMap) -> anyhow::Result<Exchange> {
    let client = reqwest::Client::builder()
        // Bounded, because an administrative command is usually run by a person
        // waiting for it, and an unreachable control plane must say so.
        .timeout(call_timeout(&call.path))
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
    let raw = response.text().await.unwrap_or_default();
    Ok(Exchange {
        ok: status.is_success(),
        status,
        raw,
    })
}

fn render_body(raw: &str) -> String {
    // Pretty-printed when it is JSON, verbatim otherwise: an operator reads this,
    // and a body that failed to parse is evidence rather than something to hide.
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|json| serde_json::to_string_pretty(&json).ok())
        .unwrap_or_else(|| raw.to_owned())
}

async fn send(call: Call, base: String, headers: HeaderMap) -> anyhow::Result<()> {
    let response = exchange(call, base, headers).await?;
    let rendered = render_body(&response.raw);
    if response.ok {
        println!("{rendered}");
        return Ok(());
    }
    eprintln!("{rendered}");
    anyhow::bail!(
        "the gateway refused this administrative request: {}",
        response.status
    )
}

async fn fetch_json(
    call: Call,
    base: String,
    headers: HeaderMap,
) -> anyhow::Result<serde_json::Value> {
    let response = exchange(call, base, headers).await?;
    if !response.ok {
        eprintln!("{}", render_body(&response.raw));
        anyhow::bail!(
            "the gateway refused this administrative request: {}",
            response.status
        );
    }
    serde_json::from_str(&response.raw)
        .map_err(|error| anyhow::anyhow!("catalogue response was not JSON: {error}"))
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

    fn nested<'a>(args: &'a ArgMatches) -> (&'a str, &'a ArgMatches) {
        args.subcommand().expect("a subcommand")
    }

    fn apply_argv(extra: &[&str]) -> ArgMatches {
        let mut argv = vec![
            "admin",
            "model",
            "apply",
            "--tenant",
            "ten_1",
            "--project",
            "prj_1",
        ];
        argv.extend_from_slice(extra);
        matches(&argv)
    }

    fn apply_flags(extra: &[&str]) -> BindingSpec {
        let args = apply_argv(extra);
        let (_, model) = nested(&args);
        let (_, apply) = nested(model);
        apply_spec(apply).expect("a binding")
    }

    fn help_text(path: &[&str]) -> String {
        let mut cmd = command();
        let mut current = &mut cmd;
        for name in path {
            current = current
                .find_subcommand_mut(*name)
                .unwrap_or_else(|| panic!("subcommand {name}"));
        }
        let mut buf = Vec::new();
        current.write_long_help(&mut buf).expect("help");
        String::from_utf8(buf).expect("utf-8")
    }

    #[test]
    fn resources_lists_bindings_first() {
        assert_eq!(RESOURCES[0].0, "bindings");
    }

    #[test]
    fn from_catalogue_requires_a_connection_provider() {
        command()
            .try_get_matches_from([
                "admin",
                "model",
                "apply",
                "--tenant",
                "ten_1",
                "--from-catalogue",
                "openai/gpt-4o",
            ])
            .expect_err("--from-catalogue does not guess the connection");
    }

    #[test]
    fn happy_path_apply_omits_name_and_defaults_the_alias_to_the_published_id() {
        let spec = apply_flags(&[
            "--target",
            "openai:gpt-4o",
            "--price-input",
            "2500000",
            "--price-output",
            "10000000",
        ]);
        assert!(
            spec.name.is_none(),
            "happy path omits name: {:?}",
            spec.name
        );
        assert_eq!(spec.alias, "gpt-4o");
        let body = spec.envelope("create");
        assert!(body["resource"].get("name").is_none());
        assert_eq!(body["resource"]["targets"][0]["provider"], "openai");
        assert_eq!(body["resource"]["targets"][0]["model"], "gpt-4o");
        assert_eq!(
            body["resource"]["targets"][0]["price"]["input_microdollars_per_million"],
            2_500_000
        );
        assert_eq!(body["mutation"], "create");
        assert_eq!(body["resource"]["tenant"], "ten_1");
        assert_eq!(body["resource"]["project"], "prj_1");
    }

    #[test]
    fn from_catalogue_builds_catalog_identity_and_requires_the_connection_flag() {
        let spec = apply_flags(&[
            "--from-catalogue",
            "openai/gpt-4o",
            "--provider",
            "openai",
            "--price",
            "observed",
        ]);
        assert!(spec.name.is_none());
        assert_eq!(spec.alias, "gpt-4o");
        let target = &spec.targets[0];
        assert_eq!(target["provider"], "openai");
        assert_eq!(target["model"], "gpt-4o");
        assert_eq!(target["catalog"]["provider"], "openai");
        assert_eq!(target["catalog"]["model"], "gpt-4o");
        assert_eq!(target["price"], "observed");
    }

    #[test]
    fn toml_fragment_is_a_model_table_not_a_full_config() {
        let toml = r#"
[[model]]
targets = [
  { provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 } },
]
"#;
        let args = apply_argv(&["--target", "openai:unused"]);
        let (_, model) = nested(&args);
        let (_, apply) = nested(model);
        let spec = spec_from_toml(toml, apply).expect("fragment");
        assert!(spec.name.is_none());
        assert_eq!(spec.alias, "gpt-4o");
        assert_eq!(spec.targets[0]["model"], "gpt-4o");
    }

    #[test]
    fn extra_toml_tables_are_refused_before_any_post() {
        let toml = r#"
[[provider]]
id = "openai"

[[model]]
targets = [{ provider = "openai", model = "gpt-4o" }]
"#;
        let args = apply_argv(&["--target", "openai:unused"]);
        let (_, model) = nested(&args);
        let (_, apply) = nested(model);
        let error = spec_from_toml(toml, apply).expect_err("full config");
        let message = error.to_string();
        assert!(
            message.contains("[[model]] fragment") || message.contains("unknown field"),
            "{message}"
        );
    }

    #[test]
    fn apply_creates_when_the_alias_slug_is_absent_and_updates_when_it_exists() {
        let empty = serde_json::json!({ "aliases": [] });
        assert!(!alias_published(&empty, "gpt-4o"));
        let present = serde_json::json!({
            "revision": "rev_1",
            "aliases": [{ "slug": "gpt-4o" }]
        });
        assert!(alias_published(&present, "gpt-4o"));
        assert!(!alias_published(&present, "gpt-5"));
    }

    #[test]
    fn apply_uses_empty_expected_revision_when_the_catalogue_has_none() {
        assert_eq!(
            expected_revision_of(&serde_json::json!({ "aliases": [] })),
            EXPECTED_REVISION_EMPTY
        );
    }

    #[test]
    fn apply_uses_the_catalogue_revision_as_expected_revision() {
        assert_eq!(
            expected_revision_of(&serde_json::json!({ "revision": "rev_9" })),
            "rev_9"
        );
    }

    #[test]
    fn apply_mints_a_printable_idempotency_key() {
        let first = mint_idempotency_key().expect("key");
        let second = mint_idempotency_key().expect("key");
        assert_ne!(first, second);
        crate::desired_state::IdempotencyKey::parse(&first).expect("protocol key");
        crate::desired_state::IdempotencyKey::parse(&second).expect("protocol key");
    }

    #[test]
    fn apply_headers_carry_minted_preconditions_and_breakglass() {
        let env = HashMap::from([(TOKEN_ENV.to_owned(), "secret".to_owned())]);
        let args = apply_argv(&["--target", "openai:gpt-4o"]);
        let (_, model) = nested(&args);
        let (_, apply) = nested(model);
        let sent = mutation_headers(&args, apply, &env, "minted-key", EXPECTED_REVISION_EMPTY)
            .expect("headers");
        assert_eq!(sent[IDEMPOTENCY_KEY_HEADER], "minted-key");
        assert_eq!(sent[EXPECTED_REVISION_HEADER], EXPECTED_REVISION_EMPTY);
        assert_eq!(sent[AUTHORIZATION], "Bearer secret");
    }

    #[test]
    fn disable_is_an_update_with_state_disabled() {
        let args = matches(&[
            "admin",
            "model",
            "disable",
            "--tenant",
            "ten_1",
            "--project",
            "prj_1",
            "--name",
            "gpt-4o",
            "--target",
            "openai:gpt-4o",
        ]);
        let (_, model) = nested(&args);
        let (_, disable) = nested(model);
        let spec = disable_spec(disable).expect("disable");
        assert_eq!(spec.alias, "gpt-4o");
        assert_eq!(spec.state.as_deref(), Some("disabled"));
        let body = spec.envelope("update");
        assert_eq!(body["mutation"], "update");
        assert_eq!(body["resource"]["state"], "disabled");
        assert_eq!(body["resource"]["name"], "gpt-4o");
    }

    #[test]
    fn price_is_an_update_of_the_same_binding() {
        let args = matches(&[
            "admin",
            "model",
            "price",
            "--tenant",
            "ten_1",
            "--name",
            "gpt-4o",
            "--input",
            "3000000",
            "--output",
            "12000000",
            "--target",
            "openai:gpt-4o",
        ]);
        let (_, model) = nested(&args);
        let (_, price) = nested(model);
        let spec = price_spec(price).expect("price");
        let body = spec.envelope("update");
        assert_eq!(body["mutation"], "update");
        assert_eq!(
            body["resource"]["targets"][0]["price"]["input_microdollars_per_million"],
            3_000_000
        );
        assert_eq!(
            body["resource"]["targets"][0]["price"]["output_microdollars_per_million"],
            12_000_000
        );
    }

    fn price_flags(extra: &[&str]) -> BindingSpec {
        let mut argv = vec![
            "admin", "model", "price", "--tenant", "ten_1", "--name", "gpt-4o", "--input",
            "3000000", "--output", "12000000",
        ];
        argv.extend_from_slice(extra);
        let args = matches(&argv);
        let (_, model) = nested(&args);
        let (_, price) = nested(model);
        price_spec(price).expect("price")
    }

    fn scoped_catalogue(state: &str) -> serde_json::Value {
        let named = if state == "enabled" {
            serde_json::json!(["gpt-4o"])
        } else {
            serde_json::json!([])
        };
        serde_json::json!({
            "revision": "rev_3",
            "aliases": [{
                "slug": "gpt-4o",
                "state": state,
                "scope": { "kind": "project", "project": "prj_1" },
                "targets": [{ "enablement": "enb_1" }]
            }],
            "entries": [{
                "enablement": "enb_1",
                "aliases": named,
                "metadata": {
                    "provider": "openai",
                    "model": "gpt-4o",
                    "published_model": "gpt-4o"
                }
            }]
        })
    }

    #[test]
    fn price_without_target_still_carries_stated_rates_after_catalogue_fill() {
        let mut spec = price_flags(&[]);
        assert!(spec.targets.is_empty());
        assert_eq!(
            spec.price.as_ref().unwrap()["input_microdollars_per_million"],
            3_000_000
        );
        hydrate_from_catalogue(&mut spec, &scoped_catalogue("enabled")).expect("hydrate");
        let body = spec.envelope("update");
        assert_eq!(
            body["resource"]["targets"][0]["price"]["input_microdollars_per_million"],
            3_000_000
        );
        assert_eq!(
            body["resource"]["targets"][0]["price"]["output_microdollars_per_million"],
            12_000_000
        );
        assert_eq!(body["resource"]["targets"][0]["provider"], "openai");
    }

    #[test]
    fn price_preserves_the_alias_lifecycle() {
        let mut spec = price_flags(&[]);
        hydrate_from_catalogue(&mut spec, &scoped_catalogue("disabled")).expect("hydrate");
        assert_eq!(spec.state.as_deref(), Some("disabled"));
        assert_eq!(spec.envelope("update")["resource"]["state"], "disabled");
    }

    #[test]
    fn unique_alias_project_refuses_zero_or_several_projects() {
        let tenant_wide = serde_json::json!({
            "aliases": [{
                "slug": "gpt-4o",
                "scope": { "kind": "project", "project": "prj_1" }
            }],
            "entries": []
        });
        assert_eq!(
            unique_alias_project(&tenant_wide, "gpt-4o").expect("one project"),
            Some("prj_1".to_owned())
        );
        unique_alias_project(&serde_json::json!({ "aliases": [] }), "gpt-4o")
            .expect_err("not published");
        let two = serde_json::json!({
            "aliases": [
                { "slug": "gpt-4o", "scope": { "project": "prj_1" } },
                { "slug": "gpt-4o", "scope": { "project": "prj_2" } }
            ]
        });
        let error = unique_alias_project(&two, "gpt-4o").expect_err("ambiguous");
        assert!(error.to_string().contains("--project"), "{error}");
        assert!(
            target_from_catalogue(&tenant_wide, "gpt-4o").is_err(),
            "tenant-wide entries are not the binding target source"
        );
    }

    #[test]
    fn json_file_flattens_a_single_models_array() {
        let args = apply_argv(&["--target", "openai:unused"]);
        let (_, model) = nested(&args);
        let (_, apply) = nested(model);
        let spec = spec_from_json(
            r#"{
                "summary": "enable gpt-4o",
                "resource": {
                    "tenant": "ten_1",
                    "project": "prj_1",
                    "models": [{
                        "targets": [{
                            "provider": "openai",
                            "model": "gpt-4o",
                            "price": "observed"
                        }]
                    }]
                }
            }"#,
            apply,
        )
        .expect("models array");
        assert!(spec.name.is_none());
        assert_eq!(spec.alias, "gpt-4o");
        assert_eq!(spec.targets[0]["price"], "observed");
        spec_from_json(
            r#"{ "resource": { "tenant": "ten_1", "models": [] } }"#,
            apply,
        )
        .expect_err("zero models");
        spec_from_json(
            r#"{ "resource": { "tenant": "ten_1", "models": [
                { "targets": [{ "provider": "openai", "model": "gpt-4o" }] },
                { "targets": [{ "provider": "openai", "model": "gpt-4o-mini" }] }
            ] } }"#,
            apply,
        )
        .expect_err("more than one model");
    }

    #[test]
    fn catalog_refresh_outlives_the_import_timeout() {
        assert_eq!(call_timeout("catalogue/refresh"), CATALOG_REFRESH_TIMEOUT);
        assert_eq!(call_timeout("bindings"), ADMIN_CALL_TIMEOUT);
        assert!(CATALOG_REFRESH_TIMEOUT >= Duration::from_secs(60));
        assert!(CATALOG_REFRESH_TIMEOUT > ADMIN_CALL_TIMEOUT);
    }

    #[test]
    fn catalog_browse_is_an_imported_catalogue_read() {
        let args = matches(&[
            "admin",
            "catalog",
            "browse",
            "--tenant",
            "ten_1",
            "--provider",
            "openai",
            "--q",
            "gpt",
        ]);
        let (_, catalog) = nested(&args);
        let (_, browse) = nested(catalog);
        let call = browse_call(browse).expect("browse");
        assert_eq!(call.method, Method::GET);
        assert_eq!(call.path, "catalogue");
        assert!(
            call.query
                .contains(&("source".to_owned(), "imported".to_owned()))
        );
        assert!(
            call.query
                .contains(&("provider".to_owned(), "openai".to_owned()))
        );
        assert!(call.query.contains(&("q".to_owned(), "gpt".to_owned())));
        assert!(
            call.query
                .contains(&("tenant".to_owned(), "ten_1".to_owned()))
        );
    }

    #[test]
    fn catalog_browse_requires_provider_or_q() {
        command()
            .try_get_matches_from(["admin", "catalog", "browse", "--tenant", "ten_1"])
            .expect_err("imported browse is a search");
    }

    #[test]
    fn catalog_refresh_does_not_send_mutation_preconditions() {
        let env = HashMap::from([(TOKEN_ENV.to_owned(), "secret".to_owned())]);
        let args = matches(&["admin", "catalog", "refresh"]);
        let (_, catalog) = nested(&args);
        let (verb, refresh) = nested(catalog);
        assert_eq!(verb, "refresh");
        let sent = headers(&args, refresh, &env).expect("headers");
        assert!(!sent.contains_key(IDEMPOTENCY_KEY_HEADER));
        assert!(!sent.contains_key(EXPECTED_REVISION_HEADER));
        assert!(!sent.contains_key(DRY_RUN_HEADER));
    }

    #[test]
    fn model_apply_does_not_require_protocol_flags() {
        command()
            .try_get_matches_from([
                "admin",
                "model",
                "apply",
                "--tenant",
                "ten_1",
                "--target",
                "openai:gpt-4o",
                "--price-input",
                "2500000",
                "--price-output",
                "10000000",
            ])
            .expect("the CLI fills expected-revision and the idempotency key");
    }

    #[test]
    fn model_show_and_catalog_refresh_are_http_paths() {
        assert_eq!(catalogue_call("ten_1", None, Vec::new()).path, "catalogue");
        let refresh = Call {
            method: Method::POST,
            path: "catalogue/refresh".to_owned(),
            query: Vec::new(),
            body: None,
        };
        assert_eq!(refresh.path, "catalogue/refresh");
        assert!(refresh.body.is_none());
    }

    #[test]
    fn disable_and_price_fill_targets_from_catalogue_metadata() {
        let target = target_from_catalogue(&scoped_catalogue("enabled"), "gpt-4o").expect("target");
        assert_eq!(target["provider"], "openai");
        assert_eq!(target["model"], "gpt-4o");
        let disabled = target_from_catalogue(&scoped_catalogue("disabled"), "gpt-4o")
            .expect("disabled alias still names an enablement");
        assert_eq!(disabled["provider"], "openai");
    }

    #[test]
    fn help_does_not_teach_class_aliases() {
        let help = format!(
            "{}{}{}",
            help_text(&[]),
            help_text(&["model"]),
            help_text(&["model", "apply"])
        );
        assert!(!help.contains("standard"), "{help}");
        assert!(!help.contains("simple"), "{help}");
        assert!(help.contains("gpt-4o"), "{help}");
    }

    #[test]
    fn toml_namespace_is_accepted_and_ignored_for_scope() {
        let toml = r#"
[[model]]
namespace = "acme"
targets = [{ provider = "openai", model = "gpt-4o", price = "observed" }]
"#;
        let args = apply_argv(&["--target", "openai:unused"]);
        let (_, model) = nested(&args);
        let (_, apply) = nested(model);
        let spec = spec_from_toml(toml, apply).expect("namespace is not a table");
        assert_eq!(spec.tenant, "ten_1");
        assert_eq!(spec.project.as_deref(), Some("prj_1"));
        assert_eq!(spec.targets[0]["price"], "observed");
    }

    #[test]
    fn a_missing_credential_fails_before_model_apply_opens_a_connection() {
        let _args = apply_argv(&["--target", "openai:gpt-4o"]);
        let error = require_token(&HashMap::new()).expect_err("no credential");
        assert!(error.to_string().contains(TOKEN_ENV), "{error}");
    }
}
