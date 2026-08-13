//! Fixtures for the stateful integration suite: config files on disk, the
//! operator CLI, and a throwaway control-plane schema.
//!
//! Black-box like the rest of the harness — every helper here drives the shipped
//! binary through its command line and its environment, because the #160 release
//! gates are properties of a deployment rather than of a function.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
/// the suite one test, and the schema is dropped when the fixture goes out of
/// scope, however the scenario ends.
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

    /// A replica of this deployment, running until the fixture is dropped.
    ///
    /// The control plane must already be migrated: a replica opens it at boot,
    /// so an unprepared schema is a boot failure rather than a scenario.
    pub async fn serve(&self) -> Replica {
        let bind = free_addr();
        let log = self
            .config
            .parent()
            .expect("the fixture config has a directory")
            .join("replica.log");
        // A file rather than a pipe: a replica outlives the assertions made
        // against it, and a full pipe nobody is draining would block its next
        // log line and hang the scenario on an unrelated write.
        let sink = std::fs::File::create(&log).expect("the replica's log is writable");
        let mut command = Command::new(axond());
        command
            .env_clear()
            .env("AXOND_CONFIG", &self.config)
            // The fixture config binds port 0 so it never collides; a scenario
            // has to know the port to make a request against it.
            .env("AXOND_SERVER__BIND", bind.to_string())
            .stdout(sink.try_clone().expect("the log handle is shareable"))
            .stderr(sink);
        for key in ["PATH", "TMPDIR"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        for (key, value) in &self.env {
            command.env(key, value);
        }
        let child = command.spawn().expect("the axond binary runs");
        let replica = Replica { child, bind, log };
        replica.await_liveness().await;
        replica
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
}

/// A running replica, with the address to reach it and the log to explain it.
pub struct Replica {
    child: Child,
    bind: SocketAddr,
    log: PathBuf,
}

impl Replica {
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.bind)
    }

    /// Everything the replica has reported so far, for a failure message.
    pub fn output(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Wait until the process answers its liveness probe.
    ///
    /// `/healthz`, not `/readyz`: a stateful replica serves administration while
    /// reporting itself unready for inference, so readiness is the very thing a
    /// scenario here asserts rather than a precondition it waits on.
    async fn await_liveness(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let client = reqwest::Client::new();
        loop {
            if let Ok(response) = client.get(self.url("/healthz")).send().await
                && response.status().is_success()
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the replica never answered /healthz on {}:\n{}",
                self.bind,
                self.output()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// A replica left running would hold its schema open against the `DROP` the
/// control-plane fixture performs, and outlive the suite that started it.
impl Drop for Replica {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Cleanup a failing assertion cannot skip: a scenario that panics half-way
/// through a migration would otherwise leave a fully populated schema behind in
/// a database every other run shares.
///
/// The drop runs on a thread of its own with its own runtime, because a `Drop`
/// cannot await and the test's runtime may already be shutting down.
impl Drop for ControlPlane {
    fn drop(&mut self) {
        let dsn = self.dsn.clone();
        let schema = self.schema.clone();
        let cleanup = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a cleanup runtime");
            runtime.block_on(async {
                client(&dsn)
                    .await
                    .batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
                    .await
                    .expect("drop the scenario's schema");
            });
        });
        // Only report a cleanup failure when nothing worse is already unwinding.
        if cleanup.join().is_err() && !std::thread::panicking() {
            panic!("the scenario's schema was left behind");
        }
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
