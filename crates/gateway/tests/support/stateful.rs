//! Fixtures for the stateful integration suite: config files on disk, the
//! operator CLI, and a throwaway control-plane schema.
//!
//! Black-box like the rest of the harness — every helper here drives the shipped
//! binary through its command line and its environment, because the #160 release
//! gates are properties of a deployment rather than of a function.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

/// Distinguishes the fixtures of tests running in the same process.
static FIXTURES: AtomicU64 = AtomicU64::new(0);

/// The shipped binary, which is what these scenarios qualify.
pub fn axond() -> &'static str {
    env!("CARGO_BIN_EXE_axond")
}

/// Write `contents` to a private file and return its path.
///
/// Mode 0600 on purpose: `axond check preflight` gates on the ownership of a
/// file that names secret references, so a fixture written with the runner's
/// umask would fail a check about the deployment rather than about the config.
pub fn private_config(name: &str, contents: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "axond-stateful-{}-{}",
        std::process::id(),
        FIXTURES.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("fixture directory");
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("fixture config is written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("fixture config is private");
    }
    path
}

/// A loopback address nothing is listening on, so a scenario never depends on
/// one fixed port being free on the machine running it.
pub fn free_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("a free port");
    listener.local_addr().expect("a bound address")
}

/// One operator command, run against `config` with exactly `env` in scope.
///
/// The environment is replaced rather than extended: a control-plane DSN
/// leaking in from the runner would make a scenario pass for a reason it did
/// not state. The binary is invoked by absolute path, so nothing inherited is
/// needed to start it; `PATH` is restored because a command may shell out, and
/// `TMPDIR` because fixtures live under it.
pub fn run(config: &Path, args: &[&str], env: &BTreeMap<&str, String>) -> Run {
    let mut command = Command::new(axond());
    command.args(args).arg("--config").arg(config).env_clear();
    for key in ["PATH", "TMPDIR"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command.output().expect("the axond binary runs");
    Run {
        args: args.iter().map(|arg| (*arg).to_owned()).collect(),
        output,
    }
}

/// What a command did, with enough context to explain a failure.
pub struct Run {
    args: Vec<String>,
    output: Output,
}

impl Run {
    pub fn succeeded(&self) -> bool {
        self.output.status.success()
    }

    pub fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output.stdout).into_owned()
    }

    pub fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).into_owned()
    }

    /// stdout and stderr together, for asserting that *something* was reported
    /// without depending on which stream a report chose.
    pub fn reported(&self) -> String {
        format!("{}{}", self.stdout(), self.stderr())
    }

    /// A failure message naming the command, its exit status, and both streams.
    pub fn context(&self) -> String {
        format!(
            "`axond {}` exited with {:?}\n--- stdout ---\n{}--- stderr ---\n{}",
            self.args.join(" "),
            self.output.status.code(),
            self.stdout(),
            self.stderr()
        )
    }
}

/// The control-plane DSN the datastore scenarios need, or `None` to skip them.
///
/// Mirrors the crate's own `test_services` rule, which an integration test
/// cannot reach: absent configuration skips, and `AXOND_TEST_REQUIRE_SERVICES=1`
/// turns a skip into a panic so CI cannot report green for scenarios that never
/// ran.
pub fn postgres_dsn() -> Option<String> {
    match std::env::var("AXOND_TEST_POSTGRES_DSN") {
        Ok(dsn) if !dsn.is_empty() => Some(dsn),
        _ if std::env::var("AXOND_TEST_REQUIRE_SERVICES").as_deref() == Ok("1") => panic!(
            "AXOND_TEST_POSTGRES_DSN is required when AXOND_TEST_REQUIRE_SERVICES=1; the \
             stateful integration scenarios must not be skipped in CI"
        ),
        _ => None,
    }
}

/// A stateful bootstrap pointed at a schema of this test's own.
///
/// Every scenario owns a schema, so the journal's fixed table names do not make
/// the suite one test, and a crashed run leaks one empty schema rather than a
/// ledger the next run would adopt.
pub struct ControlPlane {
    pub dsn: String,
    pub schema: String,
    pub config: PathBuf,
    pub env: BTreeMap<&'static str, String>,
}

/// The environment variable names the fixture config refers to. References, not
/// values: the config never inlines a DSN, a KEK, or a credential.
pub const DSN_ENV: &str = "GW_INTEGRATION_CONTROL_PLANE_DSN";
pub const KEK_ENV: &str = "GW_INTEGRATION_KEK";
pub const BREAKGLASS_ENV: &str = "GW_INTEGRATION_BREAKGLASS";

impl ControlPlane {
    /// `None` when no test database is configured.
    pub async fn create() -> Option<Self> {
        let dsn = postgres_dsn()?;
        let schema = format!(
            "axond_it_{}_{}",
            std::process::id(),
            FIXTURES.fetch_add(1, Ordering::SeqCst)
        );
        client(&dsn)
            .await
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("create the scenario's schema");
        let config = private_config(
            "axond.toml",
            &format!(
                "mode = \"stateful\"\n\
                 [server]\n\
                 bind = \"127.0.0.1:0\"\n\
                 [control_plane]\n\
                 dsn_env = \"{DSN_ENV}\"\n\
                 schema = \"{schema}\"\n\
                 connect_timeout_ms = 5000\n\
                 operation_timeout_ms = 30000\n\
                 [secret_store]\n\
                 backend = \"postgres\"\n\
                 kek_env = \"{KEK_ENV}\"\n\
                 [[admin_breakglass]]\n\
                 env = \"{BREAKGLASS_ENV}\"\n\
                 id = \"breakglass\"\n"
            ),
        );
        let env = BTreeMap::from([
            (DSN_ENV, dsn.clone()),
            // Fixture values for references the commands only have to resolve.
            (KEK_ENV, "integration-test-kek-0123456789abcdef".to_owned()),
            (BREAKGLASS_ENV, "integration-test-breakglass".to_owned()),
        ]);
        Some(Self {
            dsn,
            schema,
            config,
            env,
        })
    }

    pub fn run(&self, args: &[&str]) -> Run {
        run(&self.config, args, &self.env)
    }

    /// Whether the migration ledger exists — observed on a connection of the
    /// test's own, so a read-only claim is checked from outside the command that
    /// made it.
    pub async fn ledger_exists(&self) -> bool {
        client(&self.dsn)
            .await
            .query_one(
                "SELECT to_regclass($1)::text",
                &[&format!("{}.axond_cp_schema_migration", self.schema)],
            )
            .await
            .expect("probe the ledger")
            .get::<_, Option<String>>(0)
            .is_some()
    }

    /// The applied migration versions, in order.
    pub async fn applied_versions(&self) -> Vec<i32> {
        client(&self.dsn)
            .await
            .query(
                &format!(
                    "SELECT version FROM {}.axond_cp_schema_migration ORDER BY version",
                    self.schema
                ),
                &[],
            )
            .await
            .expect("read the ledger")
            .iter()
            .map(|row| row.get::<_, i32>(0))
            .collect()
    }

    pub async fn drop_schema(&self) {
        client(&self.dsn)
            .await
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
            .await
            .expect("drop the scenario's schema");
    }
}

/// A plain connection to the test database. `NoTls` because the DSN is a local
/// test service; the gateway's own connector is not reachable from a black-box
/// test and is exercised by the crate's unit tests.
async fn client(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to the test database");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}
