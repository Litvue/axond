//! The heavy rollout's real stateful deployment.
//!
//! One schema owns the migration ledger, desired-state journal, encrypted
//! provider material, imported catalogue, and every serving replica. The
//! reduced lane deliberately does not instantiate this module: it remains a
//! cheap stateless diagnostic.

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::Bytes;
use serde_json::{Value, json};
use tokio::sync::oneshot;

use crate::support::gateway::alias;
use crate::support::schema::Schema;
use crate::support::upstream::target;

use super::fleet::Revision;
use super::manifest::ShutdownBounds;

pub const SLOW_PROMPT: &str = "rollout-behavior:slow-stream";
pub const BUFFERED_PROMPT: &str = "rollout-behavior:late-headers";
pub const STALLED_PROMPT: &str = "rollout-behavior:stall-after-bytes";

const DSN_ENV: &str = "GW_ROLLOUT_CONTROL_PLANE_DSN";
const KEK_ENV: &str = "GW_ROLLOUT_KEK";
const BREAKGLASS_ENV: &str = "GW_ROLLOUT_BREAKGLASS";
const BOOT_ATTEMPTS: usize = 4;
const BOOT_WAIT: Duration = Duration::from_secs(30);

const TENANT: &str = "ten_019ff9e0-0000-7000-8000-000000000001";
const PROJECT: &str = "prj_019ff9e0-0000-7000-8000-000000000002";
const PRINCIPAL: &str = "prn_019ff9e0-0000-7000-8000-000000000003";
const PROVIDER: &str = "res_019ff9e0-0000-7000-8000-000000000004";
const CREDENTIAL: &str = "res_019ff9e0-0000-7000-8000-000000000005";
const CATALOG: &str = "res_019ff9e0-0000-7000-8000-000000000006";
const ENABLEMENT: &str = "res_019ff9e0-0000-7000-8000-000000000007";
const PRICE_BOOK: &str = "res_019ff9e0-0000-7000-8000-000000000008";
const CHAT_ALIAS: &str = "res_019ff9e0-0000-7000-8000-000000000009";
const SLOW_ALIAS: &str = "res_019ff9e0-0000-7000-8000-00000000000a";
const BUFFERED_ALIAS: &str = "res_019ff9e0-0000-7000-8000-00000000000b";
const STALLED_ALIAS: &str = "res_019ff9e0-0000-7000-8000-00000000000c";

static FIXTURES: AtomicU64 = AtomicU64::new(0);

/// Everything an operator command needs to address the same schema the fleet
/// serves. Owned so the migration sequence can cross `await` points without
/// borrowing the fleet that remains live behind ingress.
#[derive(Clone)]
pub struct MigrationTarget {
    pub dsn: String,
    pub schema: String,
    pub env: Vec<(String, String)>,
}

/// The observed outcome of booting a retained executable from scratch against
/// the candidate-migrated schema. A live old snapshot is deliberately allowed
/// to finish serving; this separate process proves whether rollback may create
/// a new old replica.
pub struct ColdStartAttempt {
    pub reached_readiness: bool,
    pub exit_code: Option<i32>,
    pub output: String,
}

/// The stateful half of a heavy fleet. The publisher is retained until the end
/// so the exact process that authored the revision remains available in failure
/// diagnostics; `_schema` is declared last so it is dropped after that process.
pub struct Deployment {
    dsn: String,
    schema: String,
    env: Vec<(String, String)>,
    breakglass: String,
    workload_key: String,
    provider: BehaviorProxy,
    publisher: Option<Process>,
    revision: Option<String>,
    _schema: Schema,
}

impl Deployment {
    pub async fn create(upstream: &str) -> Self {
        let dsn = std::env::var("AXOND_TEST_POSTGRES_DSN")
            .expect("heavy stateful rollout requires AXOND_TEST_POSTGRES_DSN");
        let fixture = FIXTURES.fetch_add(1, Ordering::SeqCst);
        let schema = format!("axond_rollout_{}_{}", std::process::id(), fixture);
        let claim = Schema::create(&dsn, &schema).await;
        let breakglass = format!("rollout-breakglass-{}-{fixture}", std::process::id());
        let workload_key = format!("axw1.{}", "d0".repeat(32));
        let env = vec![
            (DSN_ENV.to_owned(), dsn.clone()),
            (
                KEK_ENV.to_owned(),
                crate::support::stateful::integration_kek(),
            ),
            (BREAKGLASS_ENV.to_owned(), breakglass.clone()),
        ];
        Self {
            dsn,
            schema,
            env,
            breakglass,
            workload_key,
            provider: BehaviorProxy::start(upstream).await,
            publisher: None,
            revision: None,
            _schema: claim,
        }
    }

    pub fn config(&self, bind: SocketAddr, shutdown: ShutdownBounds) -> String {
        render_bootstrap_config(bind, &self.schema, shutdown)
    }

    pub fn migration_target(&self) -> MigrationTarget {
        MigrationTarget {
            dsn: self.dsn.clone(),
            schema: self.schema.clone(),
            env: self.env.clone(),
        }
    }

    pub fn revision(&self) -> Option<&str> {
        self.revision.as_deref()
    }

    pub fn workload_key(&self) -> &str {
        &self.workload_key
    }

    pub async fn start_replica(&self, binary: &Path, shutdown: ShutdownBounds) -> Process {
        Process::start(
            binary,
            &|bind| self.config(bind, shutdown),
            &self.env,
            Boot::Ready(self.workload_key.clone()),
        )
        .await
    }

    pub async fn cold_start_attempt(
        &self,
        binary: &Path,
        shutdown: ShutdownBounds,
    ) -> ColdStartAttempt {
        let fixture = FIXTURES.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "axond-rollout-cold-start-{}-{fixture}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("the cold-start probe directory exists");
        let bind = free_addr();
        let config = dir.join("axond.toml");
        std::fs::write(&config, self.config(bind, shutdown))
            .expect("the cold-start probe config is written");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600))
                .expect("the cold-start probe config is private");
        }
        let stdout = dir.join("stdout.log");
        let stderr = dir.join("stderr.log");
        let stdout_sink = std::fs::File::create(&stdout).expect("cold-start stdout is writable");
        let stderr_sink = std::fs::File::create(&stderr).expect("cold-start stderr is writable");
        let mut command = Command::new(binary);
        command
            .env_clear()
            .env("AXOND_CONFIG", &config)
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(stdout_sink))
            .stderr(Stdio::from(stderr_sink));
        for key in ["PATH", "TMPDIR"] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        for (name, value) in &self.env {
            command.env(name, value);
        }
        let mut child = command
            .spawn()
            .expect("the retained cold-start probe process starts");
        let http = reqwest::Client::new();
        let base_url = format!("http://{bind}");
        let deadline = Instant::now() + BOOT_WAIT;
        let (reached_readiness, exit_code) = loop {
            if let Some(status) = child
                .try_wait()
                .expect("the retained cold-start probe can be polled")
            {
                break (false, status.code());
            }
            let ready = http
                .get(format!("{base_url}/readyz"))
                .send()
                .await
                .is_ok_and(|response| response.status() == 200);
            let authenticated = http
                .get(format!("{base_url}/v1/models"))
                .bearer_auth(&self.workload_key)
                .send()
                .await
                .is_ok_and(|response| response.status() == 200);
            if ready && authenticated {
                child
                    .kill()
                    .expect("the accepted cold-start probe is stopped");
                child
                    .wait()
                    .expect("the accepted cold-start probe is reaped");
                break (true, None);
            }
            if Instant::now() >= deadline {
                child
                    .kill()
                    .expect("the timed-out cold-start probe is stopped");
                child
                    .wait()
                    .expect("the timed-out cold-start probe is reaped");
                break (false, None);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let output = output_files(&stdout, &stderr);
        let _ = std::fs::remove_dir_all(&dir);
        ColdStartAttempt {
            reached_readiness,
            exit_code,
            output,
        }
    }

    /// Publish one complete serving revision through the retained binary's real
    /// administrative surface, then require that same binary to project and
    /// serve it. This requires the durable inbound-principal projection shipped
    /// in v0.3.40: an older retained build cannot participate in a promotable
    /// stateful mixed fleet.
    pub async fn prepare(&mut self, previous_binary: &Path, shutdown: ShutdownBounds) {
        assert!(
            self.publisher.is_none(),
            "the rollout revision is published once"
        );
        let mut publisher = Process::start(
            previous_binary,
            &|bind| self.config(bind, shutdown),
            &self.env,
            Boot::Admin(self.breakglass.clone()),
        )
        .await;
        let http = reqwest::Client::new();
        let provider_material = format!("rollout-provider-{}", self.schema);

        let staged = publisher
            .breakglass(
                http.post(publisher.admin_url("/secrets")).json(&json!({
                    "tenant": TENANT,
                    "project": PROJECT,
                    "material": provider_material,
                })),
                "rollout: stage provider secret",
            )
            .send()
            .await
            .expect("the rollout secret stage answers");
        let status = staged.status();
        let staged: Value = staged.json().await.expect("the staged secret body is JSON");
        assert_eq!(
            status,
            200,
            "the provider secret is staged: {staged}\n{}",
            publisher.output()
        );
        let reference = staged["reference"]
            .as_str()
            .expect("the staged provider secret names a version")
            .to_owned();
        let secret = reference
            .split_once('@')
            .expect("the provider secret reference is versioned")
            .0
            .to_owned();
        let activated = publisher
            .breakglass(
                http.post(publisher.admin_url("/secrets/lifecycle"))
                    .json(&json!({
                        "tenant": TENANT,
                        "project": PROJECT,
                        "reference": reference,
                        "lifecycle": "active",
                    })),
                "rollout: activate provider secret",
            )
            .send()
            .await
            .expect("the rollout secret activation answers");
        assert_eq!(activated.status(), 200, "{}", publisher.output());

        let (catalog_digest, catalog_content, catalog_size) = self.catalogue_identity().await;
        let offering = offering_id("openai", "gpt-4o");
        let key_digest = sha256_checksum(self.workload_key.as_bytes());
        let mut revision = String::from("empty");
        let documents = vec![
            (
                "/tenants",
                "rollout-tenant",
                json!({
                    "summary": "publish the rollout tenant",
                    "mutation": "create",
                    "resource": {
                        "tenant": TENANT,
                        "slug": "rollout",
                        "display_name": "Rollout qualification",
                    },
                }),
            ),
            (
                "/projects",
                "rollout-project",
                json!({
                    "summary": "publish the rollout project",
                    "mutation": "create",
                    "resource": {
                        "project": PROJECT,
                        "tenant": TENANT,
                        "slug": "serving",
                        "display_name": "Serving fleet",
                    },
                }),
            ),
            (
                "/principals",
                "rollout-principal",
                json!({
                    "principal": PRINCIPAL,
                    "tenant": TENANT,
                    "project": PROJECT,
                    "slug": "rollout-workload",
                    "display_name": "Rollout workload",
                    "key_digest": key_digest,
                    "roles": ["operator"],
                }),
            ),
            (
                "/providers",
                "rollout-provider",
                json!({
                    "provider": PROVIDER,
                    "tenant": TENANT,
                    "project": PROJECT,
                    "slug": "openai",
                    "display_name": "Rollout fixture OpenAI",
                    "wire_family": "openai-chat",
                    "endpoint": self.provider.base_url.clone(),
                }),
            ),
            (
                "/credentials",
                "rollout-credential",
                json!({
                    "credential": CREDENTIAL,
                    "tenant": TENANT,
                    "project": PROJECT,
                    "provider": PROVIDER,
                    "slug": "openai-primary",
                    "display_name": "Rollout fixture credential",
                    "secret": secret,
                    "secret_version": 1,
                    "lifecycle": "active",
                }),
            ),
            (
                "/catalogs",
                "rollout-catalog",
                json!({
                    "catalog": CATALOG,
                    "slug": "seed",
                    "digest": catalog_digest,
                    "size_bytes": catalog_size,
                }),
            ),
            (
                "/models",
                "rollout-model",
                json!({
                    "enablement": ENABLEMENT,
                    "tenant": TENANT,
                    "project": PROJECT,
                    "slug": "gpt-4o",
                    "offering": offering,
                    "catalog": CATALOG,
                    "snapshot": catalog_digest,
                    "wire_family": "openai-chat",
                    "state": "enabled",
                }),
            ),
            (
                "/prices",
                "rollout-price",
                json!({
                    "price_book": PRICE_BOOK,
                    "slug": "rollout-prices",
                    "catalog": catalog_content,
                    "catalog_version": 1,
                    "state": "approved",
                    "approved_at_millis": 1,
                    "approval_citation": "rollout qualification fixture",
                    "rules": [{
                        "provider": "openai",
                        "model": "gpt-4o",
                        "precedence": "baseline",
                        "from_millis": 0,
                        "input_nano_dollars_per_million": 2_500_000_000_u64,
                        "output_nano_dollars_per_million": 10_000_000_000_u64,
                        "origin": "operator",
                        "citation": "rollout qualification fixture",
                    }],
                }),
            ),
        ];
        for (path, idempotency, document) in documents {
            revision =
                publish_resource(&publisher, &http, path, idempotency, &revision, document).await;
        }
        for (id, slug) in [
            (CHAT_ALIAS, alias::CHAT),
            (SLOW_ALIAS, alias::CHAT_SLOW),
            (BUFFERED_ALIAS, alias::CHAT_LATE_HEADERS),
            (STALLED_ALIAS, alias::CHAT_STALL_AFTER_BYTES),
        ] {
            revision = publish_resource(
                &publisher,
                &http,
                "/aliases",
                &format!("rollout-alias-{slug}"),
                &revision,
                json!({
                    "alias": id,
                    "tenant": TENANT,
                    "project": PROJECT,
                    "slug": slug,
                    "wire_family": "openai-chat",
                    "state": "enabled",
                    "targets": [{ "enablement": ENABLEMENT }],
                }),
            )
            .await;
        }

        publisher
            .await_revision(&revision, &self.workload_key, &self.breakglass)
            .await;
        self.revision = Some(revision);
        self.publisher = Some(publisher);
    }

    async fn catalogue_identity(&self) -> (String, String, u64) {
        let client = connect(&self.dsn).await;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(row) = client
                .query_opt(
                    &format!(
                        "SELECT raw_digest, content_id, raw_bytes FROM {}.axond_catalog_snapshot \
                         ORDER BY imported_at LIMIT 1",
                        self.schema
                    ),
                    &[],
                )
                .await
                .expect("the rollout catalogue identity is readable")
            {
                return (row.get(0), row.get(1), row.get::<_, i64>(2) as u64);
            }
            assert!(
                Instant::now() < deadline,
                "the stateful rollout seed catalogue was not retained in {}",
                self.schema
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn render_bootstrap_config(bind: SocketAddr, schema: &str, shutdown: ShutdownBounds) -> String {
    let tuning = Revision::stateful_tuning(shutdown);
    format!(
        "mode = \"stateful\"\n\
         [server]\n\
         bind = \"{bind}\"\n\
         [control_plane]\n\
         dsn_env = \"{DSN_ENV}\"\n\
         schema = \"{schema}\"\n\
         connect_timeout_ms = 5000\n\
         operation_timeout_ms = 30000\n\
         [secret_store]\n\
         backend = \"postgres\"\n\
         kek_env = \"{KEK_ENV}\"\n\
         schema = \"{schema}\"\n\
         [catalog]\n\
         source = \"seed\"\n\
         store = \"postgres\"\n\
         schema = \"{schema}\"\n\
         bootstrap = \"seed\"\n\
         [[admin_breakglass]]\n\
         env = \"{BREAKGLASS_ENV}\"\n\
         id = \"breakglass\"\n\
         [[usage_sink]]\n\
         kind = \"stdout\"\n\
         {tuning}"
    )
}

#[cfg(test)]
mod tests {
    use figment::Figment;
    use figment::providers::{Format, Toml};

    use super::*;

    #[test]
    fn rendered_stateful_bootstrap_omits_control_plane_owned_sections() {
        let config = render_bootstrap_config(
            SocketAddr::from(([127, 0, 0, 1], 8080)),
            "rollout_schema",
            ShutdownBounds {
                drain_grace_ms: 100,
                deadline_ms: 200,
                flush_timeout_ms: 300,
            },
        );
        let parsed: serde_json::Value = Figment::from(Toml::string(&config))
            .extract()
            .expect("the rendered bootstrap is valid TOML");

        assert_eq!(parsed["mode"], "stateful");
        assert_eq!(parsed["control_plane"]["schema"], "rollout_schema");
        assert_eq!(parsed["transport"]["connect_timeout_ms"], 10_000);
        assert_eq!(parsed["shutdown"]["deadline_ms"], 200);
        assert!(parsed.get("failover").is_none());
        assert!(parsed.get("model").is_none());
    }
}

async fn publish_resource(
    publisher: &Process,
    http: &reqwest::Client,
    path: &str,
    idempotency: &str,
    expected: &str,
    document: Value,
) -> String {
    let document = if document.get("summary").is_some() {
        document
    } else {
        json!({
            "summary": "publish rollout serving state",
            "mutation": "create",
            "resource": document,
        })
    };
    let response = publisher
        .breakglass(
            http.post(publisher.admin_url(path))
                .header("idempotency-key", idempotency)
                .header("x-axond-expected-revision", expected)
                .json(&document),
            "rollout: publish serving revision",
        )
        .send()
        .await
        .expect("a rollout resource publication answers");
    let status = response.status();
    let body: Value = response.json().await.expect("the publication body is JSON");
    assert_eq!(
        status,
        200,
        "the rollout resource publishes ({path}, {idempotency}, expected {expected}): {body}\n{}",
        publisher.output()
    );
    body["revision"]
        .as_str()
        .unwrap_or_else(|| panic!("a rollout publication names its revision: {body}"))
        .to_owned()
}

fn sha256_checksum(bytes: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    let mut rendered = String::from("sha256:");
    for byte in digest.as_ref() {
        use std::fmt::Write;
        write!(&mut rendered, "{byte:02x}").expect("a string accepts formatting");
    }
    rendered
}

fn offering_id(provider: &str, model: &str) -> String {
    fn string(value: &str, output: &mut Vec<u8>) {
        output.push(0x03);
        output.extend_from_slice(&(value.len() as u64).to_be_bytes());
        output.extend_from_slice(value.as_bytes());
    }

    let mut canonical = b"axond.desired-state\0\x01".to_vec();
    canonical.push(0x07);
    canonical.extend_from_slice(&2_u64.to_be_bytes());
    string("model", &mut canonical);
    string(model, &mut canonical);
    string("provider", &mut canonical);
    string(provider, &mut canonical);
    format!("off_{}", &sha256_checksum(&canonical)[7..])
}

async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("the rollout connects to PostgreSQL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[derive(Clone)]
enum Boot {
    Ready(String),
    Admin(String),
}

/// A stateful replica launched from an arbitrary retained or candidate binary.
/// Output goes to files so a heavy run does not retain every JSON line in the
/// harness heap; usage records are parsed on demand and after exit.
pub struct Process {
    pub base_url: String,
    child: Child,
    admin_key: Option<String>,
    config_dir: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl Process {
    async fn start(
        binary: &Path,
        render: &dyn Fn(SocketAddr) -> String,
        env: &[(String, String)],
        boot: Boot,
    ) -> Self {
        let mut last = String::new();
        for _ in 0..BOOT_ATTEMPTS {
            let fixture = FIXTURES.fetch_add(1, Ordering::SeqCst);
            let dir = std::env::temp_dir().join(format!(
                "axond-rollout-stateful-{}-{fixture}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).expect("the stateful rollout config directory exists");
            let bind = free_addr();
            let config = dir.join("axond.toml");
            std::fs::write(&config, render(bind)).expect("the stateful rollout config is written");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&config, std::fs::Permissions::from_mode(0o600))
                    .expect("the stateful rollout config is private");
            }
            let stdout = dir.join("stdout.log");
            let stderr = dir.join("stderr.log");
            let stdout_sink = std::fs::File::create(&stdout).expect("stdout log is writable");
            let stderr_sink = std::fs::File::create(&stderr).expect("stderr log is writable");
            let mut command = Command::new(binary);
            command
                .env_clear()
                .env("AXOND_CONFIG", &config)
                .env("RUST_LOG", "info")
                .stdout(Stdio::from(stdout_sink))
                .stderr(Stdio::from(stderr_sink));
            for key in ["PATH", "TMPDIR"] {
                if let Some(value) = std::env::var_os(key) {
                    command.env(key, value);
                }
            }
            for (name, value) in env {
                command.env(name, value);
            }
            let child = command.spawn().expect("the stateful rollout binary starts");
            let admin_key = match &boot {
                Boot::Admin(key) => Some(key.clone()),
                Boot::Ready(_) => None,
            };
            let mut process = Self {
                base_url: format!("http://{bind}"),
                child,
                admin_key,
                config_dir: dir,
                stdout,
                stderr,
            };
            if process.await_boot(&boot, bind).await {
                return process;
            }
            last = process.output();
            let collision = last.contains("Address already in use");
            process.shutdown();
            if !collision {
                panic!(
                    "stateful rollout binary {} never reached {} on the migrated control-plane \
                     revision:\n{last}",
                    binary.display(),
                    boot.description()
                );
            }
        }
        panic!(
            "stateful rollout binary lost {BOOT_ATTEMPTS} ports before it reached {}:\n{last}",
            boot.description()
        );
    }

    async fn await_boot(&mut self, boot: &Boot, bind: SocketAddr) -> bool {
        let deadline = Instant::now() + BOOT_WAIT;
        let http = reqwest::Client::new();
        while Instant::now() < deadline {
            if !matches!(self.child.try_wait(), Ok(None)) {
                return false;
            }
            let listening = {
                let output = self.output();
                output.contains("axond listening") && output.contains(&bind.to_string())
            };
            let identified = match boot {
                Boot::Ready(key) => {
                    let ready = http
                        .get(self.url("/readyz"))
                        .send()
                        .await
                        .is_ok_and(|response| response.status() == 200);
                    let models = http
                        .get(self.url("/v1/models"))
                        .bearer_auth(key)
                        .send()
                        .await
                        .is_ok_and(|response| response.status() == 200);
                    ready && models
                }
                Boot::Admin(breakglass) => self
                    .admin_request(http.get(self.admin_url("/state")), breakglass, "identify")
                    .send()
                    .await
                    .is_ok_and(|response| matches!(response.status().as_u16(), 200 | 503)),
            };
            if listening && identified && matches!(self.child.try_wait(), Ok(None)) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    async fn await_revision(&mut self, revision: &str, key: &str, breakglass: &str) {
        let deadline = Instant::now() + BOOT_WAIT;
        let http = reqwest::Client::new();
        let mut last = Value::Null;
        loop {
            if let Ok(response) = self
                .admin_request(
                    http.get(self.admin_url("/convergence")),
                    breakglass,
                    "observe-convergence",
                )
                .send()
                .await
                && response.status() == 200
                && let Ok(body) = response.json::<Value>().await
            {
                let active = body["active"] == revision && body["loaded"] == revision;
                last = body;
                if active {
                    let ready = http
                        .get(self.url("/readyz"))
                        .send()
                        .await
                        .is_ok_and(|response| response.status() == 200);
                    let authenticated = http
                        .get(self.url("/v1/models"))
                        .bearer_auth(key)
                        .send()
                        .await
                        .is_ok_and(|response| response.status() == 200);
                    if ready && authenticated {
                        return;
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "the retained rollout binary did not project durable revision {revision}; last \
                 convergence report: {last}\n{}",
                self.output()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    fn admin_url(&self, path: &str) -> String {
        self.url(&format!("/admin/v1{path}"))
    }

    fn admin_request(
        &self,
        request: reqwest::RequestBuilder,
        breakglass: &str,
        reason: &str,
    ) -> reqwest::RequestBuilder {
        request
            .bearer_auth(breakglass)
            .header("x-axond-breakglass-operator", "rollout-harness")
            .header("x-axond-breakglass-reason", format!("rollout: {reason}"))
    }

    fn breakglass(
        &self,
        request: reqwest::RequestBuilder,
        reason: &str,
    ) -> reqwest::RequestBuilder {
        let breakglass = self
            .admin_key
            .as_deref()
            .expect("only the stateful publisher makes administrative requests");
        self.admin_request(request, breakglass, reason)
    }

    pub fn usage_records(&self) -> Vec<Value> {
        [&self.stdout, &self.stderr]
            .into_iter()
            .flat_map(|path| {
                std::fs::File::open(path)
                    .ok()
                    .into_iter()
                    .flat_map(|file| BufReader::new(file).lines().map_while(Result::ok))
            })
            .filter_map(|line| serde_json::from_str::<Value>(&line).ok())
            .filter(|value| value.get("schema_version").is_some())
            .collect()
    }

    pub fn output(&self) -> String {
        output_files(&self.stdout, &self.stderr)
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    pub fn terminate(&self) {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(self.pid().to_string())
            .status()
            .expect("kill(1) runs");
        assert!(status.success(), "SIGTERM was delivered");
    }

    pub async fn await_exit(&mut self, within: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + within;
        loop {
            match self
                .child
                .try_wait()
                .expect("the stateful child can be polled")
            {
                Some(status) => return Some(status),
                None if Instant::now() >= deadline => return None,
                None => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    }

    pub async fn settle_output(&self, _within: Duration) {
        // Files are written directly by the child. Once `await_exit` reaps it,
        // there are no detached pipe readers whose final line can still be in
        // flight.
    }

    fn shutdown(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn output_files(stdout: &Path, stderr: &Path) -> String {
    format!(
        "--- stdout ---\n{}\n--- stderr ---\n{}",
        std::fs::read_to_string(stdout).unwrap_or_default(),
        std::fs::read_to_string(stderr).unwrap_or_default(),
    )
}

impl Boot {
    fn description(&self) -> &'static str {
        match self {
            Self::Ready(_) => "authenticated readiness",
            Self::Admin(_) => "the authenticated administrative surface",
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.shutdown();
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }
}

fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free rollout port");
    listener.local_addr().expect("a bound rollout address")
}

#[derive(Clone)]
struct ProxyState {
    upstream: String,
    client: reqwest::Client,
}

/// Rewrites the catalogue's real `openai/gpt-4o` target into one of the fake
/// upstream's deterministic transport behaviours. The durable control plane
/// still owns the provider and model; this adapter only gives the fixture one
/// endpoint capable of late headers and never-ending streams.
struct BehaviorProxy {
    base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl BehaviorProxy {
    async fn start(upstream: &str) -> Self {
        let app = Router::new()
            .route("/chat/completions", post(proxy_request))
            .with_state(ProxyState {
                upstream: upstream.to_owned(),
                client: reqwest::Client::new(),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the rollout behavior proxy binds");
        let addr = listener
            .local_addr()
            .expect("the behavior proxy has an address");
        let (stop, stopped) = oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await;
        });
        Self {
            base_url: format!("http://{addr}"),
            shutdown: Some(stop),
        }
    }
}

impl Drop for BehaviorProxy {
    fn drop(&mut self) {
        if let Some(stop) = self.shutdown.take() {
            let _ = stop.send(());
        }
    }
}

async fn proxy_request(
    State(state): State<ProxyState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let mut document: Value = match serde_json::from_slice(&body) {
        Ok(document) => document,
        Err(error) => {
            return (
                axum::http::StatusCode::BAD_REQUEST,
                format!("invalid rollout fixture request: {error}"),
            )
                .into_response();
        }
    };
    let prompt = document["messages"]
        .as_array()
        .and_then(|messages| messages.first())
        .and_then(|message| message["content"].as_str())
        .unwrap_or_default();
    document["model"] = Value::String(
        match prompt {
            SLOW_PROMPT => target::SLOW_STREAM,
            BUFFERED_PROMPT => target::LATE_HEADERS,
            STALLED_PROMPT => target::STALL_AFTER_BYTES,
            _ => target::CHAT,
        }
        .to_owned(),
    );
    let mut request = state
        .client
        .post(format!("{}/chat/completions", state.upstream))
        .json(&document);
    for name in ["authorization", "x-api-key"] {
        if let Some(value) = headers.get(name) {
            request = request.header(name, value);
        }
    }
    let upstream = match request.send().await {
        Ok(response) => response,
        Err(error) => {
            return (
                axum::http::StatusCode::BAD_GATEWAY,
                format!("rollout fixture proxy failure: {error}"),
            )
                .into_response();
        }
    };
    let status = upstream.status();
    let content_type = upstream.headers().get("content-type").cloned();
    let mut response = Response::builder().status(status);
    if let Some(content_type) = content_type {
        response = response.header("content-type", content_type);
    }
    response
        .body(Body::from_stream(upstream.bytes_stream()))
        .expect("the proxy response is valid")
}
