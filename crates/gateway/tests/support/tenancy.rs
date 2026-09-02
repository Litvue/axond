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
use super::upstream::FakeUpstream;

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
    alias: "acme-openai/fixture-chat",
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
    alias: "globex-openai/fixture-chat",
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
pub const PLATFORM_ALIAS: &str = "platform-openai/fixture-chat";
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
    /// The durable objects this boot was pointed at, present when the suite is
    /// running with a datastore. Declared last, so it is dropped — and its
    /// objects with it — after the process that writes to them is gone.
    objects: Option<Objects>,
}

impl Deployment {
    /// The durable objects of a stateful boot, for a case that has one.
    pub fn objects(&self) -> &Objects {
        self.objects
            .as_ref()
            .expect("a stateful deployment has durable objects")
    }
}

/// The per-boot Postgres objects, and the [`Drop`] that removes them.
///
/// Its own value rather than a part of [`Deployment`], because the child
/// *creates* these objects while booting (`create_table = true`) and a boot that
/// fails a post-`CREATE` check panics before a `Deployment` exists. Owning them
/// from before the process starts is what makes the cleanup unconditional.
pub struct Objects {
    /// The DSN the sink and budget were configured with.
    pub dsn: String,
    /// Names unique to this boot, so concurrent runs share a database without
    /// sharing rows.
    pub usage_table: String,
    pub budget_table: String,
    /// The schema the durable usage outbox lives in, for the boot that enables
    /// one. The outbox's table names are fixed, so a per-boot outbox is a
    /// per-boot schema.
    pub outbox_schema: Option<String>,
}

/// The durable state this deployment keeps, when it keeps any.
#[derive(Clone, Copy)]
pub enum Durability {
    /// Usage on stdout only: no datastore, so the suite runs anywhere.
    None,
    /// A Postgres usage sink and a Postgres budget with a namespace-wide cap,
    /// which is what makes "one tenant's spend is its own" observable.
    Postgres { namespace_cap_microdollars: u64 },
    /// The billing-grade durable usage outbox: every settled event is appended
    /// to a Postgres journal before the request is answered, and a delivery
    /// worker replays it into the sinks. A second durable usage path, so what
    /// the row sink partitions has to be asserted of the outbox too.
    Outbox,
}

/// Boot the deployment. `Durability::Postgres` yields `None` when no test
/// Postgres is configured, which is how the stateful cases skip.
pub async fn boot(durability: Durability) -> Option<Deployment> {
    start(durability, &unique_suffix(), Fate::Serve).await
}

/// The message the deliberately failed boot panics with, so the regression
/// cannot pass on some other panic.
const FAILED_BOOT: &str = "a post-boot check failed";

/// Run a boot that dies after the gateway came up, and report the names it
/// created so a caller can prove they did not outlive it. `None` when no test
/// Postgres is configured.
///
/// This is the failure the cleanup exists for: the child creates its usage and
/// budget objects while starting, so anything that panics between that and a
/// `Deployment` existing would leave them behind if the `Deployment` were what
/// owned them.
pub fn boot_that_fails_after_starting(durability: Durability) -> Option<Names> {
    postgres_dsn()?;
    let suffix = unique_suffix();
    let names = names(&suffix);
    // On a thread with a runtime of its own, because the boot has to unwind
    // without taking the calling test with it, and the caller's runtime cannot
    // be re-entered.
    let outcome = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime for the failing boot")
            // Dropped inside the runtime either way, so nothing of a boot that
            // unexpectedly succeeded outlives this.
            .block_on(async move { start(durability, &suffix, Fate::Fail).await.is_some() })
    })
    .join();
    let Err(failure) = outcome else {
        panic!("the arranged boot returned instead of failing");
    };
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains(FAILED_BOOT),
        "the boot failed for the reason the regression arranged, not another: {message}"
    );
    Some(names)
}

/// Run the outbox setup far enough to create its schema, then fail before any
/// later setup step can construct a deployment. The `Objects` guard must own
/// the schema already, or this deliberately arranged failure leaves it behind.
pub fn boot_that_fails_after_creating_outbox() -> Option<String> {
    postgres_dsn()?;
    let suffix = unique_suffix();
    let schema = names(&suffix).outbox_schema;
    let outcome = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime for the failing outbox boot")
            .block_on(async move {
                start(Durability::Outbox, &suffix, Fate::FailAfterOutboxSchema)
                    .await
                    .is_some()
            })
    })
    .join();
    let Err(failure) = outcome else {
        panic!("the arranged outbox boot returned instead of failing");
    };
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(
        message.contains(FAILED_BOOT),
        "the outbox boot failed for the arranged setup reason, not another: {message}"
    );
    Some(schema)
}

/// The per-boot object names, derived from the run's suffix so a caller can know
/// them without holding the boot that creates them.
pub struct Names {
    pub usage_table: String,
    pub budget_table: String,
    pub outbox_schema: String,
}

fn names(suffix: &str) -> Names {
    Names {
        usage_table: format!("axond_usage_iso_{suffix}"),
        budget_table: format!("axond_budget_iso_{suffix}"),
        outbox_schema: format!("axond_outbox_iso_{suffix}"),
    }
}

/// How far a boot is meant to get.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Fate {
    Serve,
    /// Panic once the gateway is up, the way a post-boot check would.
    Fail,
    /// Panic immediately after creating the outbox schema, before later setup.
    FailAfterOutboxSchema,
}

async fn start(durability: Durability, suffix: &str, fate: Fate) -> Option<Deployment> {
    let dsn = match durability {
        Durability::None => None,
        Durability::Postgres { .. } | Durability::Outbox => Some(postgres_dsn()?),
    };
    let Names {
        usage_table,
        budget_table,
        outbox_schema,
    } = names(suffix);

    // Before the boot, not after it: the child creates these objects as it comes
    // up, so a setup failure after any one is created must still take it with it.
    // In particular, this guard has to exist before the outbox schema below.
    let objects = dsn.as_ref().map(|dsn| Objects {
        dsn: dsn.clone(),
        usage_table: usage_table.clone(),
        budget_table: budget_table.clone(),
        outbox_schema: matches!(durability, Durability::Outbox).then(|| outbox_schema.clone()),
    });

    // The child applies the outbox DDL itself (`create_schema = true`), but the
    // schema it applies it into has to exist first, and it is this boot's own so
    // concurrent runs do not share an outbox.
    if let (Durability::Outbox, Some(dsn)) = (durability, dsn.as_deref()) {
        connect(dsn)
            .await
            .batch_execute(&format!("CREATE SCHEMA {outbox_schema}"))
            .await
            .expect("a schema for this boot's outbox");
    }

    if fate == Fate::FailAfterOutboxSchema {
        panic!("{FAILED_BOOT}");
    }

    let upstream = FakeUpstream::start().await;
    let render = |addr: SocketAddr| {
        config_toml(
            addr,
            &upstream.base_url,
            durability,
            &usage_table,
            &budget_table,
            &outbox_schema,
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

    let gateway = Axond::start_custom(&render, &env).await;

    if fate == Fate::Fail {
        let dsn = objects
            .as_ref()
            .map(|objects| objects.dsn.clone())
            .expect("a failing boot is only arranged for a stateful one");
        assert!(
            relation_exists(&dsn, &usage_table).await,
            "the child created its objects before the boot failed, or the case proves nothing"
        );
        // `objects` and `gateway` are still locals, so the unwind is what has to
        // clean up — the point of the case.
        panic!("{FAILED_BOOT}");
    }

    Some(Deployment {
        gateway,
        upstream,
        objects,
    })
}

/// Whether a relation of this name exists, for asserting on what a boot left.
pub async fn relation_exists(dsn: &str, name: &str) -> bool {
    let client = connect(dsn).await;
    let row = client
        .query_one(
            "SELECT count(*) FROM pg_class WHERE relname = $1 AND relkind = 'r'",
            &[&name],
        )
        .await
        .expect("a relation lookup");
    row.get::<_, i64>(0) > 0
}

/// Whether a schema of this boot's name exists in the shared test database.
pub async fn schema_exists(dsn: &str, name: &str) -> bool {
    let client = connect(dsn).await;
    let row = client
        .query_one(
            "SELECT count(*) FROM pg_namespace WHERE nspname = $1",
            &[&name],
        )
        .await
        .expect("a schema lookup");
    row.get::<_, i64>(0) > 0
}

/// Whether a function of this name exists: the budget DDL names one after its
/// table, and dropping the table does not take it.
pub async fn function_exists(dsn: &str, name: &str) -> bool {
    let client = connect(dsn).await;
    let row = client
        .query_one("SELECT count(*) FROM pg_proc WHERE proname = $1", &[&name])
        .await
        .expect("a function lookup");
    row.get::<_, i64>(0) > 0
}

/// End the process before its tables are dropped: it holds a Postgres pool,
/// batches usage every 50ms and settles detached, so dropping the tables under a
/// live child either waits on its lock or fails its next insert against a
/// relation that is no longer there. A field's own `Drop` runs after this body,
/// which is why the objects are a field and this is not their cleanup.
impl Drop for Deployment {
    fn drop(&mut self) {
        if self.objects.is_some() {
            self.gateway.shutdown();
        }
    }
}

/// Drop everything this boot created, so a long-lived CI database does not
/// accumulate a boot's worth of objects per run — the usage table, the three
/// budget relations, and the namespace fence function, which the budget DDL
/// also names after the table and which dropping the table leaves behind.
///
/// On [`Drop`] rather than at the end of each test, because the runs that most
/// need cleaning up are the ones that panicked, and those never reach a
/// trailing statement. It is a blocking cleanup on a thread of its own: the
/// tests run on a current-thread runtime, which cannot be re-entered from a
/// destructor.
impl Drop for Objects {
    fn drop(&mut self) {
        let dsn = self.dsn.clone();
        let mut objects = vec![
            format!("TABLE IF EXISTS {}_reservation", self.budget_table),
            format!("TABLE IF EXISTS {}_namespace", self.budget_table),
            format!("TABLE IF EXISTS {}", self.budget_table),
            format!("TABLE IF EXISTS {}", self.usage_table),
            format!("FUNCTION IF EXISTS {}_namespace_fence()", self.budget_table),
        ];
        if let Some(schema) = &self.outbox_schema {
            objects.push(format!("SCHEMA IF EXISTS {schema} CASCADE"));
        }
        let cleanup = std::thread::spawn(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a cleanup runtime")
                .block_on(async move {
                    let client = connect(&dsn).await;
                    for object in objects {
                        if let Err(error) = client.batch_execute(&format!("DROP {object}")).await {
                            eprintln!("isolation cleanup could not drop {object}: {error}");
                        }
                    }
                });
        });
        // Joined, so the objects are gone before the test process is; and
        // reported rather than panicked on, because a destructor that fails
        // during an unwind aborts the process and buries the assertion that
        // failed — which is the run whose cleanup matters most.
        if cleanup.join().is_err() {
            eprintln!("isolation cleanup thread panicked");
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

/// A short per-boot table suffix: eight hex characters, and the width is the
/// point.
///
/// The budget DDL derives index and trigger names from the table, the longest
/// being `<table>_reservation_namespace_expires_idx` at 34 characters more.
/// Postgres truncates an identifier at 63 bytes, which does not fail the
/// `CREATE` — it fails the boot check that looks the derived object up again by
/// its untruncated name. `axond_budget_iso_` plus eight leaves that longest
/// derived name at 59.
fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let folded = u32::try_from((nanos ^ u128::from(std::process::id())) & 0xffff_ffff)
        .expect("masked into range");
    format!("{folded:08x}")
}

fn config_toml(
    bind: SocketAddr,
    upstream: &str,
    durability: Durability,
    usage_table: &str,
    budget_table: &str,
    outbox_schema: &str,
) -> String {
    let model = |provider: &str| {
        format!(
            "[[price]]\nprovider = \"{provider}\"\nmodel = \"*\"\ninput_microdollars_per_million = {INPUT_PRICE}\noutput_microdollars_per_million = {OUTPUT_PRICE}\n\n"
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
        // No row sink and no budget: the outbox is the durable path under test,
        // and the stdout sink the delivery worker replays into is what tells the
        // suite an appended event was delivered.
        Durability::Outbox => format!(
            r#"
[usage_journal]
backend = "postgres"
dsn_env = "AXOND_ISOLATION_DSN"
schema = "{outbox_schema}"
create_schema = true
consumer = "isolation"
"#
        ),
        Durability::Postgres {
            namespace_cap_microdollars,
        } => {
            // The per-subject cap is required by the config, but it must not be
            // reachable: each tenant here has exactly one static subject, so a
            // subject limit equal to the namespace one is exhausted at the same
            // request, and a ledger keyed on the wrong one of the two would pass
            // the isolation test anyway. Out of reach, the namespace cap is the
            // only thing that can produce the 429 that test observes.
            let subject_cap = namespace_cap_microdollars * 1_000;
            format!(
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
limit_microdollars = {subject_cap}
namespace_limit_microdollars = {namespace_cap_microdollars}
dsn_env = "AXOND_ISOLATION_DSN"
table = "{budget_table}"
create_table = true
"#
            )
        }
    };

    format!(
        r#"
[server]
bind = "{bind}"

[storage]
backend = "sqlite"
path = "{sqlite}"

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
        sqlite = std::env::temp_dir()
            .join(format!(
                "axond-tenancy-{}-{}.sqlite",
                std::process::id(),
                bind.port()
            ))
            .display(),
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
            model(ACME.provider),
            model(GLOBEX.provider),
            model(PLATFORM_PROVIDER),
        ]
        .concat(),
    )
}
