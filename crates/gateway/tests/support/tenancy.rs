//! A multi-tenant deployment of the real binary, for the isolation suite (#225).
//!
//! The shipped fixture in [`gateway`](super::gateway) is one namespace, so it
//! cannot answer the question this harness exists for: with two tenants served
//! by one process, does either one ever see, invoke, spend against, or
//! authenticate with anything of the other's? That needs a deployment whose
//! tenants are genuinely disjoint — their own providers, their own credential
//! pools, their own gateway keys — and one tenant that has no credential at all
//! so platform fallback is a *decision* the config makes rather than a default.
//!
//! Every credential here is a fixture value pointing at the fake upstream, and
//! the upstream records what it was sent, so "the provider received only the
//! intended credential" is checked against the bytes that arrived rather than
//! against the gateway's own account of them.

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use super::gateway::{Axond, INPUT_PRICE, OUTPUT_PRICE};
use super::upstream::{FakeUpstream, target};

/// One tenant of the deployment: its namespace, the key that authenticates to
/// it, the alias only it can serve, and the upstream credential its requests
/// must arrive with.
pub struct Tenant {
    /// Tenant-qualified, the shape a project projects to (`acme/core`).
    pub namespace: &'static str,
    /// The inbound key value a caller presents.
    pub key: &'static str,
    /// The environment variable that key arrives in — also the usage record's
    /// `subject` for a statically authenticated caller.
    pub key_env: &'static str,
    /// The alias whose target provider only this tenant holds a credential for.
    pub alias: &'static str,
    /// This tenant's own provider id.
    pub provider: &'static str,
    /// The upstream key the gateway must inject for this tenant, and for no
    /// other.
    pub upstream_key: &'static str,
    /// The environment variable it arrives in.
    pub upstream_key_env: &'static str,
    /// The non-secret credential label the usage record attributes spend to.
    pub credential_id: &'static str,
}

/// A tenant with its own provider credential: BYOK, no fallback.
pub const ACME: Tenant = Tenant {
    namespace: "acme/core",
    key: "test-inbound-key-acme",
    key_env: "GW_KEY_ACME",
    alias: "acme-chat",
    provider: "acme-openai",
    upstream_key: "test-upstream-acme-key",
    upstream_key_env: "GW_FAKE_ACME_KEY",
    credential_id: "acme-openai-primary",
};

/// A second tenant, identically shaped, sharing the process and nothing else.
pub const GLOBEX: Tenant = Tenant {
    namespace: "globex/core",
    key: "test-inbound-key-globex",
    key_env: "GW_KEY_GLOBEX",
    alias: "globex-chat",
    provider: "globex-openai",
    upstream_key: "test-upstream-globex-key",
    upstream_key_env: "GW_FAKE_GLOBEX_KEY",
    credential_id: "globex-openai-primary",
};

/// The two tenants that hold their own credentials.
pub const TENANTS: [&Tenant; 2] = [&ACME, &GLOBEX];

/// A third namespace holding no credential of its own and opting in to the
/// platform pool: what `allow_platform_fallback` is for, and the only caller in
/// this deployment that may be served by a credential it does not own.
pub const FALLBACK_NAMESPACE: &str = "initech/core";
pub const FALLBACK_KEY: &str = "test-inbound-key-initech";
pub const FALLBACK_KEY_ENV: &str = "GW_KEY_INITECH";

/// The platform's own namespace, pool, and alias.
pub const PLATFORM_NAMESPACE: &str = "platform";
pub const PLATFORM_ALIAS: &str = "platform-chat";
pub const PLATFORM_PROVIDER: &str = "platform-openai";
pub const PLATFORM_CREDENTIAL_ID: &str = "platform-openai-primary";
pub const PLATFORM_UPSTREAM_KEY: &str = "test-upstream-platform-key";
const PLATFORM_UPSTREAM_KEY_ENV: &str = "GW_FAKE_PLATFORM_KEY";
/// The platform operator's own inbound key, so the platform namespace is a
/// caller too and not merely a pool other namespaces borrow from.
pub const PLATFORM_KEY: &str = "test-inbound-key-platform";
const PLATFORM_KEY_ENV: &str = "GW_KEY_PLATFORM";

/// Every alias the deployment declares, so a catalogue assertion can name what
/// a tenant must *not* see as well as what it must.
pub const ALL_ALIASES: [&str; 3] = [ACME.alias, GLOBEX.alias, PLATFORM_ALIAS];

/// A booted two-tenant deployment: the fake provider, the process, and the
/// durable tables it was pointed at when a Postgres was available.
pub struct Deployment {
    pub upstream: FakeUpstream,
    pub gateway: Axond,
    /// The Postgres DSN the sink and budget were configured with, when the
    /// suite is running with a datastore.
    pub dsn: Option<String>,
    /// Tables unique to this boot, so concurrent runs share a database without
    /// sharing rows.
    pub usage_table: String,
    pub budget_table: String,
}

/// The durable state this deployment keeps, when it keeps any.
#[derive(Clone, Copy)]
pub enum Durability {
    /// Usage on stdout only: no datastore, so the suite runs anywhere.
    None,
    /// A Postgres usage sink and a Postgres budget with a namespace-wide cap,
    /// which is what makes "one tenant's spend is its own" observable.
    Postgres { namespace_cap_microdollars: u64 },
}

/// Boot the deployment. `Durability::Postgres` yields `None` when no test
/// Postgres is configured, which is how the stateful cases skip.
pub async fn boot(durability: Durability) -> Option<Deployment> {
    let dsn = match durability {
        Durability::None => None,
        Durability::Postgres { .. } => Some(postgres_dsn()?),
    };
    let suffix = unique_suffix();
    let usage_table = format!("axond_usage_iso_{suffix}");
    let budget_table = format!("axond_budget_iso_{suffix}");

    let upstream = FakeUpstream::start().await;
    let render = |addr: SocketAddr| {
        config_toml(
            addr,
            &upstream.base_url,
            durability,
            &usage_table,
            &budget_table,
        )
    };
    let mut env = vec![
        (ACME.key_env.to_owned(), ACME.key.to_owned()),
        (GLOBEX.key_env.to_owned(), GLOBEX.key.to_owned()),
        (FALLBACK_KEY_ENV.to_owned(), FALLBACK_KEY.to_owned()),
        (PLATFORM_KEY_ENV.to_owned(), PLATFORM_KEY.to_owned()),
        (
            ACME.upstream_key_env.to_owned(),
            ACME.upstream_key.to_owned(),
        ),
        (
            GLOBEX.upstream_key_env.to_owned(),
            GLOBEX.upstream_key.to_owned(),
        ),
        (
            PLATFORM_UPSTREAM_KEY_ENV.to_owned(),
            PLATFORM_UPSTREAM_KEY.to_owned(),
        ),
    ];
    if let Some(dsn) = &dsn {
        env.push(("AXOND_ISOLATION_DSN".to_owned(), dsn.clone()));
    }

    Some(Deployment {
        gateway: Axond::start_custom(&render, &env).await,
        upstream,
        dsn,
        usage_table,
        budget_table,
    })
}

impl Deployment {
    /// Drop the tables this boot created. Called by the stateful cases so a
    /// long-lived CI database does not accumulate one table per run.
    pub async fn drop_tables(&self) {
        let Some(dsn) = &self.dsn else {
            return;
        };
        let client = connect(dsn).await;
        for table in [
            format!("{}_reservation", self.budget_table),
            format!("{}_namespace", self.budget_table),
            self.budget_table.clone(),
            self.usage_table.clone(),
        ] {
            client
                .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
                .await
                .expect("a table this boot created can be dropped");
        }
    }
}

/// The test Postgres, under the same variable the in-crate suites use: absent
/// means skip, unless CI has declared the service mandatory.
pub fn postgres_dsn() -> Option<String> {
    match std::env::var("AXOND_TEST_POSTGRES_DSN").ok() {
        Some(dsn) => Some(dsn),
        None if std::env::var("AXOND_TEST_REQUIRE_SERVICES").as_deref() == Ok("1") => panic!(
            "AXOND_TEST_POSTGRES_DSN is required when AXOND_TEST_REQUIRE_SERVICES=1; \
             CI requires the test service to be available"
        ),
        None => None,
    }
}

/// A connected client whose connection task is detached, as every caller here
/// wants: the queries are short and the client is dropped with the test.
pub async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("the test Postgres accepts a connection");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// A short per-boot table suffix.
///
/// Short deliberately: the budget DDL derives index and trigger names from the
/// table (`<table>_reservation_namespace_expires_idx` is 34 characters longer),
/// and Postgres truncates an identifier at 63 bytes — which does not fail the
/// `CREATE`, it fails the boot check that looks the derived object up again by
/// its untruncated name. So the base name stays well inside the budget the
/// longest derived one leaves.
fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos()
        % u128::from(u64::MAX);
    let nanos = u64::try_from(nanos).expect("reduced into range");
    format!("{:08x}", u64::from(std::process::id()) ^ nanos)
}

fn config_toml(
    bind: SocketAddr,
    upstream: &str,
    durability: Durability,
    usage_table: &str,
    budget_table: &str,
) -> String {
    let price = format!(
        "{{ input_microdollars_per_million = {INPUT_PRICE}, output_microdollars_per_million = {OUTPUT_PRICE} }}"
    );
    let model = |name: &str, provider: &str| {
        format!(
            "[[model]]\nname = \"{name}\"\ntargets = [ {{ provider = \"{provider}\", model = \"{target}\", price = {price} }} ]\n\n",
            target = target::CHAT,
        )
    };
    let provider = |id: &str| {
        format!("[[provider]]\nid = \"{id}\"\nkind = \"openai\"\nbase_url = \"{upstream}\"\n\n")
    };
    let credential = |namespace: &str, provider: &str, env: &str, id: &str| {
        format!(
            "[[credential]]\nnamespace = \"{namespace}\"\nprovider = \"{provider}\"\nenv = \"{env}\"\nid = \"{id}\"\n\n"
        )
    };
    let gateway_key = |env: &str, namespace: &str| {
        format!("[[gateway_key]]\nenv = \"{env}\"\nnamespace = \"{namespace}\"\n\n")
    };
    // Stdout stays declared alongside the durable sink: the suite reads records
    // off the process's own output, and a durable sink must not silence that.
    let durable = match durability {
        Durability::None => String::new(),
        Durability::Postgres {
            namespace_cap_microdollars,
        } => format!(
            r#"
[[usage_sink]]
kind = "postgres"
dsn_env = "AXOND_ISOLATION_DSN"
table = "{usage_table}"
create_table = true
max_batch = 1
flush_interval_ms = 50

[budget]
backend = "postgres"
limit_microdollars = {namespace_cap_microdollars}
namespace_limit_microdollars = {namespace_cap_microdollars}
dsn_env = "AXOND_ISOLATION_DSN"
table = "{budget_table}"
create_table = true
"#
        ),
    };

    format!(
        r#"
[server]
bind = "{bind}"

[[namespace]]
id = "{PLATFORM_NAMESPACE}"
default = true

[[namespace]]
id = "{acme_ns}"

[[namespace]]
id = "{globex_ns}"

# The one namespace allowed to be served by a credential it does not own.
[[namespace]]
id = "{FALLBACK_NAMESPACE}"
allow_platform_fallback = true

{providers}{credentials}{keys}
# Unique to this boot, so a request carrying it can only be answered by the
# process the harness started.
[[gateway_key]]
env = "GW_BOOT_KEY"
namespace = "{PLATFORM_NAMESPACE}"

[[usage_sink]]
kind = "stdout"

[failover]
max_attempts = 1
overall_timeout_ms = 30000
{durable}
{models}"#,
        acme_ns = ACME.namespace,
        globex_ns = GLOBEX.namespace,
        providers = [ACME.provider, GLOBEX.provider, PLATFORM_PROVIDER]
            .map(provider)
            .concat(),
        credentials = [
            credential(
                ACME.namespace,
                ACME.provider,
                ACME.upstream_key_env,
                ACME.credential_id
            ),
            credential(
                GLOBEX.namespace,
                GLOBEX.provider,
                GLOBEX.upstream_key_env,
                GLOBEX.credential_id
            ),
            credential(
                PLATFORM_NAMESPACE,
                PLATFORM_PROVIDER,
                PLATFORM_UPSTREAM_KEY_ENV,
                PLATFORM_CREDENTIAL_ID
            ),
        ]
        .concat(),
        keys = [
            gateway_key(ACME.key_env, ACME.namespace),
            gateway_key(GLOBEX.key_env, GLOBEX.namespace),
            gateway_key(FALLBACK_KEY_ENV, FALLBACK_NAMESPACE),
            gateway_key(PLATFORM_KEY_ENV, PLATFORM_NAMESPACE),
        ]
        .concat(),
        models = [
            model(ACME.alias, ACME.provider),
            model(GLOBEX.alias, GLOBEX.provider),
            model(PLATFORM_ALIAS, PLATFORM_PROVIDER),
        ]
        .concat(),
    )
}
