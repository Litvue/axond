//! Boots the real `axond` binary against a [`FakeUpstream`].
//!
//! Black-box on purpose: these suites qualify the shipped process — its config
//! parsing, its inbound auth, its usage records on stdout — rather than a
//! router assembled in-process, so a regression in boot or wiring fails here
//! too.

use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::upstream::target;

/// The inbound gateway key every test authenticates with. A fixture value, not
/// a secret: the fake upstream is the only thing it can reach.
pub const GATEWAY_KEY: &str = "test-inbound-key";
/// The environment variable carrying a boot's private inbound key. Its value is
/// unique per boot, which is what lets a readiness probe tell this child apart
/// from a sibling test process serving the same loopback port.
const BOOT_KEY_ENV: &str = "GW_BOOT_KEY";
/// Upstream credentials the gateway is expected to inject, asserted on the
/// fake upstream's recorded requests.
pub const OPENAI_KEY: &str = "test-upstream-openai-key";
pub const ANTHROPIC_KEY: &str = "test-upstream-anthropic-key";
/// Second keys per provider, exported at boot but referenced by no shipped
/// config section: a suite that wants a credential *pool* declares the extra
/// `[[credential]]` entries in its own tuning and gets them from the
/// environment the process already has.
pub const OPENAI_KEY_SECONDARY: &str = "test-upstream-openai-key-secondary";
pub const ANTHROPIC_KEY_SECONDARY: &str = "test-upstream-anthropic-key-secondary";
/// The environment variables those second keys arrive in.
pub const OPENAI_SECONDARY_ENV: &str = "GW_FAKE_OPENAI_KEY_SECONDARY";
pub const ANTHROPIC_SECONDARY_ENV: &str = "GW_FAKE_ANTHROPIC_KEY_SECONDARY";

/// Caller-facing aliases the test config exposes.
pub mod alias {
    pub const CHAT: &str = "chat-golden";
    pub const CHAT_NO_HEADERS: &str = "chat-no-headers";
    pub const CHAT_LATE_HEADERS: &str = "chat-late-headers";
    pub const CHAT_SLOW_BODY: &str = "chat-slow-body";
    pub const CHAT_HUGE_BODY: &str = "chat-huge-body";
    pub const CHAT_HUGE_ERROR: &str = "chat-huge-error";
    pub const CHAT_STALL: &str = "chat-stall";
    pub const CHAT_STALL_AFTER_BYTES: &str = "chat-stall-after-bytes";
    pub const CHAT_LONG: &str = "chat-long";
    pub const CHAT_SLOW: &str = "chat-slow";
    pub const CHAT_DROP: &str = "chat-drop";
    pub const CHAT_FAIL: &str = "chat-fail";
    pub const MESSAGES: &str = "messages-golden";
    pub const MESSAGES_SLOW: &str = "messages-slow";
    pub const MESSAGES_DROP: &str = "messages-drop";
    pub const EMBEDDINGS: &str = "embeddings-golden";
    pub const RESPONSES: &str = "responses-golden";
    /// Buffered answers of a fixed size, for a response-size sweep.
    pub const CHAT_SIZED_SMALL: &str = "chat-sized-small";
    pub const CHAT_SIZED_MEDIUM: &str = "chat-sized-medium";
    pub const CHAT_SIZED_LARGE: &str = "chat-sized-large";
}

/// The `[failover]` and `[transport]` sections every suite gets unless it asks
/// for its own: bounds high enough that no golden-path test races them.
pub const DEFAULT_TUNING: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000
"#;

/// Micro-dollars per million tokens every test target is priced at, so an
/// expected charge can be computed from the tokens a fixture reports.
pub const INPUT_PRICE: u64 = 2_500_000;
pub const OUTPUT_PRICE: u64 = 10_000_000;

/// Distinguishes the config directories of gateways booted by the same test
/// process, so differently tuned boots cannot share a file.
static CONFIGS: AtomicU64 = AtomicU64::new(0);

/// How many ephemeral ports a boot may lose before the suite gives up.
const BOOT_ATTEMPTS: u32 = 4;

/// How many output lines a boot keeps. Failure output wants the recent past,
/// and a run long enough to overflow this is a soak whose evidence is its
/// artifact rather than its scrollback — retaining every line of a twelve-hour
/// run would make the harness the leak it is looking for.
const RETAINED_LINES: usize = 4096;

/// What the process has said, kept so a failing test can print it and a
/// harness can read what each request was charged.
#[derive(Default)]
struct Output {
    /// Everything the process wrote, usage records included, bounded to the
    /// most recent [`RETAINED_LINES`]. Records stay here as well as in
    /// [`Self::usage`] because assertions about what the process must *not*
    /// print — a prompt, a credential — read the raw text.
    lines: VecDeque<String>,
    /// How many lines were dropped to stay inside that bound, so truncated
    /// output says so rather than reading as everything the process wrote.
    dropped: u64,
    /// Usage records, parsed as they arrive. Unbounded by default — a suite
    /// asserts over all of them — and drained by the endurance harness, which
    /// reconciles each batch as it lands.
    usage: Vec<Value>,
}

impl Output {
    fn ingest(&mut self, line: String) {
        if let Ok(value) = serde_json::from_str::<Value>(&line)
            && value.get("schema_version").is_some()
        {
            self.usage.push(value);
        }
        if self.lines.len() == RETAINED_LINES {
            self.lines.pop_front();
            self.dropped += 1;
        }
        self.lines.push_back(line);
    }

    fn rendered(&self) -> String {
        let recent = self.lines.iter().cloned().collect::<Vec<_>>().join("\n");
        match self.dropped {
            0 => recent,
            dropped => format!("[{dropped} earlier lines dropped]\n{recent}"),
        }
    }
}

pub struct Axond {
    pub base_url: String,
    /// This boot's private inbound key: no other process has it, so a route that
    /// fails closed accepts it only from this child.
    boot_key: String,
    /// The config the process was booted with, kept so a harness can record
    /// exactly what it qualified.
    pub config: String,
    child: Child,
    output: Arc<Mutex<Output>>,
    /// The threads draining the child's pipes into `output`, kept so a failed
    /// boot can wait for them before quoting what the child said.
    readers: Vec<JoinHandle<()>>,
}

impl Axond {
    /// Boot the binary against `upstream_base_url` and wait until it serves.
    pub async fn start(upstream_base_url: &str) -> Self {
        Self::start_with(upstream_base_url, DEFAULT_TUNING).await
    }

    /// Boot with `tuning` — TOML replacing the default `[failover]` section and
    /// carrying any `[transport]` bounds the suite wants to exercise.
    pub async fn start_with(upstream_base_url: &str, tuning: &str) -> Self {
        // `free_addr` closes its listener before the binary binds it, so a
        // sibling test process can take the port in between. That race is the
        // ephemeral-port allocator's, not the gateway's, so a lost boot is
        // retried on a fresh port rather than failing the suite.
        let mut last = String::new();
        for _ in 0..BOOT_ATTEMPTS {
            match Self::try_start(upstream_base_url, tuning).await {
                Ok(gateway) => return gateway,
                Err(output) => last = output,
            }
        }
        panic!("{}", never_served(&last));
    }

    /// Boot from a config the caller renders, plus extra environment.
    ///
    /// For suites whose subject is the *shape* of a deployment rather than one
    /// section of the shipped fixture — several namespaces, several credential
    /// pools, a durable sink — where patching `tuning` into one namespace's
    /// config would not express it. `render` is called per attempt because a
    /// retried boot binds a different port, and the config carries the bind.
    ///
    /// The rendered config must declare a `[[gateway_key]]` reading
    /// `GW_BOOT_KEY`: that key is how readiness tells this child apart from a
    /// sibling that won the port (see [`Self::answers_for_this_boot`]).
    pub async fn start_custom(
        render: &dyn Fn(SocketAddr) -> String,
        env: &[(String, String)],
    ) -> Self {
        let mut last = String::new();
        for _ in 0..BOOT_ATTEMPTS {
            match Self::try_start_custom(render, env).await {
                Ok(gateway) => return gateway,
                Err(output) => last = output,
            }
        }
        panic!("{}", never_served(&last));
    }

    /// One boot attempt. The error is everything that attempt's child wrote,
    /// which on a loopback port usually means someone else won the bind — and
    /// when it means something else, it is the only account of what.
    async fn try_start(upstream_base_url: &str, tuning: &str) -> Result<Self, String> {
        Self::try_start_custom(&|addr| config_toml(addr, upstream_base_url, tuning), &[]).await
    }

    async fn try_start_custom(
        render: &dyn Fn(SocketAddr) -> String,
        extra_env: &[(String, String)],
    ) -> Result<Self, String> {
        // One reservation, used for both the config directory and the boot key:
        // re-reading the counter could hand two concurrent boots the same value,
        // and the key has to be unique by construction, not by timing.
        let boot = CONFIGS.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("axond-compat-{}-{boot}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("test config directory");
        let addr = free_addr();
        let path = dir.join(format!("axond-{}.toml", addr.port()));
        let boot_key = format!("test-boot-key-{}-{boot}", std::process::id());
        let config = render(addr);
        std::fs::write(&path, &config).expect("test config is written");

        let mut command = Command::new(env!("CARGO_BIN_EXE_axond"));
        for (name, value) in extra_env {
            command.env(name, value);
        }
        let mut child = command
            .env("AXOND_CONFIG", &path)
            .env("GW_INBOUND_KEY", GATEWAY_KEY)
            .env(BOOT_KEY_ENV, &boot_key)
            .env("GW_FAKE_OPENAI_KEY", OPENAI_KEY)
            .env("GW_FAKE_ANTHROPIC_KEY", ANTHROPIC_KEY)
            .env(OPENAI_SECONDARY_ENV, OPENAI_KEY_SECONDARY)
            .env(ANTHROPIC_SECONDARY_ENV, ANTHROPIC_KEY_SECONDARY)
            .env("RUST_LOG", "warn")
            .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("the axond binary starts");

        let output = Arc::new(Mutex::new(Output::default()));
        let mut readers = Vec::new();
        for stream in [
            child
                .stdout
                .take()
                .map(Box::new)
                .map(|s| s as Box<dyn std::io::Read + Send>),
            child
                .stderr
                .take()
                .map(Box::new)
                .map(|s| s as Box<dyn std::io::Read + Send>),
        ]
        .into_iter()
        .flatten()
        {
            let sink = output.clone();
            readers.push(std::thread::spawn(move || {
                for line in BufReader::new(stream).lines().map_while(Result::ok) {
                    sink.lock().expect("output lock").ingest(line);
                }
            }));
        }

        let mut gateway = Self {
            base_url: format!("http://{addr}"),
            boot_key,
            config,
            child,
            output,
            readers,
        };
        if gateway.await_ready().await {
            Ok(gateway)
        } else {
            Err(gateway.final_output())
        }
    }

    /// Whether the process is serving. A boot that loses its port is reported
    /// rather than panicked on, so the caller can retry it.
    async fn await_ready(&mut self) -> bool {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            // A lost bind is decided by this child's own output, not by whether
            // the port answers: when a sibling wins the port, that sibling's
            // gateway answers `/healthz` while this child is exiting, and taking
            // the probe as readiness would run the test against a process
            // configured for someone else.
            if self.lost_the_port() {
                return false;
            }
            if let Ok(response) = client
                .get(format!("{}/healthz", self.base_url))
                .send()
                .await
                && response.status().is_success()
            {
                // Health only proves *something* serves the port. Readiness is
                // this child's only if the server also accepts this boot's
                // private key, which no sibling was given.
                let base_url = self.base_url.clone();
                return self.answers_for_this_boot(&client, &base_url).await;
            }
            if let Ok(Some(_)) = self.child.try_wait() {
                // The process is gone. Every exit is retriable here, because the
                // one that is genuinely a race — a sibling took the port — is
                // not reliably distinguishable from the ones that are not: the
                // bind failure is a substring of output drained by detached
                // threads, so it may not have arrived yet. The attempt's output
                // travels with the failure instead, and the caller names the
                // cause if every attempt fails.
                return false;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        false
    }

    /// Whether the process serving this port is this child. `/v1/models` fails
    /// closed on an unknown gateway key, and this boot's key exists only in this
    /// child's environment, so a sibling that won the port answers 401 — the
    /// window between `spawn` and the child's own bind failure, where nothing has
    /// been logged yet, is covered by asking the server who it is rather than by
    /// waiting for this child to complain.
    async fn answers_for_this_boot(&mut self, client: &reqwest::Client, base_url: &str) -> bool {
        let identified = client
            .get(format!("{base_url}/v1/models"))
            .bearer_auth(&self.boot_key)
            .send()
            .await
            .is_ok_and(|response| response.status().is_success());
        identified && !self.lost_the_port() && matches!(self.child.try_wait(), Ok(None))
    }

    /// Whether `base_url` is served by this child, as `await_ready` decides it.
    /// Exposed so the identity rule can be tested against a foreign server
    /// deterministically, instead of only when the port race is actually lost.
    pub async fn serves_this_boot(&mut self, base_url: &str) -> bool {
        let client = reqwest::Client::new();
        self.answers_for_this_boot(&client, base_url).await
    }

    /// Whether this child reported that its listener address was taken. The
    /// ephemeral port `free_addr` picked is released before the binary binds it,
    /// so a sibling test process can win it in between; that is the allocator's
    /// race, and a lost boot is retried on a fresh port.
    fn lost_the_port(&self) -> bool {
        self.output().contains("Address already in use")
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// What the process has written to stdout/stderr, for failure output and
    /// for assertions over the raw text: its most recent lines.
    pub fn output(&self) -> String {
        self.output.lock().expect("output lock").rendered()
    }

    /// [`Self::output`], but complete. The pipes are drained by detached
    /// threads, so a child that has already exited may still have its last and
    /// most interesting line in flight; ending the child closes the pipes, which
    /// ends the readers, and joining them settles the record. For the abandoned
    /// boot whose output is about to become a failure message.
    fn final_output(&mut self) -> String {
        self.shutdown();
        self.output()
    }

    /// End the process and settle its output, ahead of the drop that would do
    /// it anyway. For a harness that must know the child is gone — because what
    /// it does next, dropping the tables the child writes to, would otherwise
    /// race the writes still in flight.
    pub fn shutdown(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
    }

    /// The usage records the process has emitted on its stdout sink — the
    /// black-box view of what each request was charged.
    pub fn usage_records(&self) -> Vec<Value> {
        self.output.lock().expect("output lock").usage.clone()
    }

    /// Take the usage records emitted since the last drain. A run long enough
    /// to settle millions of them reconciles each batch and lets it go, rather
    /// than holding every record until the end.
    pub fn drain_usage_records(&self) -> Vec<Value> {
        std::mem::take(&mut self.output.lock().expect("output lock").usage)
    }

    /// Wait until at least `count` usage records have been written. Settlement
    /// is detached from the request, so a record can land just after the
    /// client's last byte.
    pub async fn await_usage_records(&self, count: usize) -> Vec<Value> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let records = self.usage_records();
            if records.len() >= count {
                return records;
            }
            if Instant::now() >= deadline {
                panic!(
                    "expected {count} usage records, saw {}:\n{}",
                    records.len(),
                    self.output()
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Send `SIGTERM`, exactly as an orchestrator does when it takes a replica
    /// out of a rolling deployment. Spawning `kill(1)` keeps the harness free of
    /// an `unsafe` signal call for the sake of one line.
    pub fn terminate(&self) {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(self.pid().to_string())
            .status()
            .expect("kill(1) runs");
        assert!(status.success(), "SIGTERM was delivered");
    }

    /// Wait for the process to exit, up to `within`. `None` means it was still
    /// running — the failure mode a bounded shutdown exists to prevent.
    pub async fn await_exit(&mut self, within: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + within;
        loop {
            match self.child.try_wait().expect("the child can be polled") {
                Some(status) => return Some(status),
                None if Instant::now() >= deadline => return None,
                None => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    }

    /// Wait for `/readyz` to report the drain, up to `within`. Returns the last
    /// status seen, so a test can distinguish "still ready" from "gone".
    pub async fn await_not_ready(&self, within: Duration) -> Option<reqwest::StatusCode> {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + within;
        let mut last = None;
        while Instant::now() < deadline {
            last = client
                .get(self.url("/readyz"))
                .send()
                .await
                .ok()
                .map(|response| response.status());
            if last.is_some_and(|status| status == reqwest::StatusCode::SERVICE_UNAVAILABLE) {
                return last;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        last
    }

    /// Resident set size of the gateway process, in kibibytes. Used to assert
    /// a soak does not buffer stream bodies.
    pub fn resident_kib(&self) -> Option<u64> {
        resident_kib(self.pid())
    }
}

/// Resident set size of a process, in kibibytes; `None` off `/proc` platforms.
pub fn resident_kib(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

impl Drop for Axond {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// What to say when every attempt failed. The last child's own output is the
/// message: a lost port race says `Address already in use` and is the boring
/// answer, while a refused config or an unreachable datastore says what it was,
/// and either beats reporting only that nothing became healthy.
fn never_served(last_output: &str) -> String {
    format!(
        "axond never served on a free port in {BOOT_ATTEMPTS} attempts; \
         the last one said:\n{last_output}"
    )
}

/// A loopback address nothing is listening on. The listener is closed before
/// the gateway binds it, which is the usual small race and is fine for a test
/// process that owns its own port range for milliseconds.
fn free_addr() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
    listener.local_addr().expect("a bound address")
}

fn config_toml(bind: SocketAddr, upstream: &str, tuning: &str) -> String {
    let price = format!(
        "{{ input_microdollars_per_million = {INPUT_PRICE}, output_microdollars_per_million = {OUTPUT_PRICE} }}"
    );
    let model = |name: &str, provider: &str, target: &str| {
        format!(
            "[[model]]\nname = \"{name}\"\ntargets = [ {{ provider = \"{provider}\", model = \"{target}\", price = {price} }} ]\n\n"
        )
    };
    format!(
        r#"
[server]
bind = "{bind}"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "{upstream}"

[[provider]]
id = "fake-anthropic"
kind = "anthropic"
base_url = "{upstream}"

[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "GW_FAKE_OPENAI_KEY"
id = "fake-openai-primary"

[[credential]]
namespace = "platform"
provider = "fake-anthropic"
env = "GW_FAKE_ANTHROPIC_KEY"
id = "fake-anthropic-primary"

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"

# Unique to this boot, so a request carrying it can only be answered by the
# process the harness started; the suites authenticate with GW_INBOUND_KEY.
[[gateway_key]]
env = "GW_BOOT_KEY"
namespace = "platform"

{tuning}

{chat}{chat_sized_small}{chat_sized_medium}{chat_sized_large}{chat_no_headers}{chat_late_headers}{chat_slow_body}{chat_huge_body}{chat_huge_error}{chat_stall}{chat_stall_after_bytes}{chat_long}{chat_slow}{chat_drop}{chat_fail}{messages}{messages_slow}{messages_drop}{embeddings}{responses}"#,
        chat = model(alias::CHAT, "fake-openai", target::CHAT),
        chat_sized_small = model(
            alias::CHAT_SIZED_SMALL,
            "fake-openai",
            target::SIZED_BODY_SMALL
        ),
        chat_sized_medium = model(
            alias::CHAT_SIZED_MEDIUM,
            "fake-openai",
            target::SIZED_BODY_MEDIUM
        ),
        chat_sized_large = model(
            alias::CHAT_SIZED_LARGE,
            "fake-openai",
            target::SIZED_BODY_LARGE
        ),
        chat_no_headers = model(alias::CHAT_NO_HEADERS, "fake-openai", target::NO_HEADERS),
        chat_late_headers = model(
            alias::CHAT_LATE_HEADERS,
            "fake-openai",
            target::LATE_HEADERS
        ),
        chat_slow_body = model(alias::CHAT_SLOW_BODY, "fake-openai", target::SLOW_BODY),
        chat_huge_body = model(alias::CHAT_HUGE_BODY, "fake-openai", target::HUGE_BODY),
        chat_huge_error = model(alias::CHAT_HUGE_ERROR, "fake-openai", target::HUGE_ERROR),
        chat_stall = model(alias::CHAT_STALL, "fake-openai", target::STALL_STREAM),
        chat_stall_after_bytes = model(
            alias::CHAT_STALL_AFTER_BYTES,
            "fake-openai",
            target::STALL_AFTER_BYTES
        ),
        chat_long = model(alias::CHAT_LONG, "fake-openai", target::LONG_STREAM),
        chat_slow = model(alias::CHAT_SLOW, "fake-openai", target::SLOW_STREAM),
        chat_drop = model(alias::CHAT_DROP, "fake-openai", target::DROP_STREAM),
        chat_fail = model(alias::CHAT_FAIL, "fake-openai", target::FAIL),
        messages = model(alias::MESSAGES, "fake-anthropic", target::MESSAGES),
        messages_slow = model(alias::MESSAGES_SLOW, "fake-anthropic", target::SLOW_STREAM),
        messages_drop = model(alias::MESSAGES_DROP, "fake-anthropic", target::DROP_STREAM),
        embeddings = model(alias::EMBEDDINGS, "fake-openai", target::EMBEDDINGS),
        responses = model(alias::RESPONSES, "fake-openai", target::RESPONSES),
    )
}
