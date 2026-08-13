//! The deployment under test: a small fleet of real `axond` replicas, the
//! config revisions published to them under load, and the balancer in front.
//!
//! Everything here is the shipped binary reading a config file from disk with a
//! durable usage sink behind it. A revision is a new file plus `SIGHUP`
//! (ADR 0011) — which is what an operator does, and what a control plane will
//! do on the operator's behalf — so what the run qualifies is the process'
//! ability to take a change while serving, not a function's ability to return a
//! new struct.
//!
//! The four tenants are not alike, on purpose. The platform namespace owns the
//! credential pool; a BYOK namespace brings its own; a fallback namespace has
//! none and is allowed to borrow the platform's; and a probe namespace exists
//! only so tenant policy can be revised against a caller whose refusal cannot
//! be confused with the workload's planned failures.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::support::endurance::plan::Tenant;
use crate::support::gateway::{
    ANTHROPIC_SECONDARY_ENV, Axond, GATEWAY_KEY, INPUT_PRICE, OPENAI_SECONDARY_ENV, OUTPUT_PRICE,
    alias,
};
use crate::support::upstream::target;

/// The namespaces the run serves.
pub const PLATFORM: &str = "platform";
pub const BYOK: &str = "stateful-byok";
pub const FALLBACK: &str = "stateful-fallback";
/// The namespace tenant policy is revised against.
pub const PROBE: &str = "stateful-probe";

/// Inbound keys for the namespaces the shared fixture does not declare. They
/// are fixtures rather than secrets — everything they can reach is a fake
/// upstream on loopback — and they are delivered as files because the boot
/// environment belongs to the shared harness while `[[gateway_key]] file` is
/// config this profile owns.
const BYOK_KEY: &str = "stateful-endurance-byok-inbound-key";
const FALLBACK_KEY: &str = "stateful-endurance-fallback-inbound-key";
const PROBE_KEY: &str = "stateful-endurance-probe-inbound-key";

/// The alias only the catalogue revision serves. A request for it is how the
/// run observes that a published revision is the one answering.
pub const CATALOGUE_ALIAS: &str = "chat-catalogue-v2";

/// The credential labels a usage record may carry, before and after the
/// credential revision. Labels, not material: they are attribution, and the
/// artifact quotes them.
pub const OPENAI_PRIMARY_ID: &str = "fake-openai-primary";
pub const OPENAI_ROTATED_ID: &str = "fake-openai-rotated";
pub const ANTHROPIC_PRIMARY_ID: &str = "fake-anthropic-primary";
pub const ANTHROPIC_ROTATED_ID: &str = "fake-anthropic-rotated";
pub const BYOK_OPENAI_ID: &str = "stateful-byok-openai";
pub const BYOK_ANTHROPIC_ID: &str = "stateful-byok-anthropic";

/// The environment variable each replica reads its usage DSN from. The *name*
/// travels in the config and in the artifact; the value never does.
pub const USAGE_DSN_ENV: &str = "GW_USAGE_DSN";

/// The placeholder a tenant key file's directory is replaced by before a config
/// is hashed: the directory is per process, and an input hash that changed
/// every run would make every artifact incomparable.
pub const KEY_DIR_PLACEHOLDER: &str = "/TENANT_KEY_DIR";

/// The placeholder the run's own usage table is replaced by, for the same
/// reason: the schema is named after the moment the run started, and it is in
/// the config text the artifact hashes.
pub const USAGE_TABLE_PLACEHOLDER: &str = "QUALIFICATION_SCHEMA.axond_usage";

/// Distinguishes the replicas one run boots, live and retired alike.
static REPLICAS: AtomicU64 = AtomicU64::new(0);

/// Which revisions have been published. Cumulative: a credential rotation does
/// not withdraw the alias the catalogue revision added.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Revision {
    pub catalogue: bool,
    pub credential: bool,
    pub policy: bool,
}

impl Revision {
    /// A label for the timeline, so an event can name the revision it produced.
    pub fn label(self) -> String {
        let mut parts = Vec::new();
        if self.catalogue {
            parts.push("catalogue");
        }
        if self.credential {
            parts.push("credential");
        }
        if self.policy {
            parts.push("policy");
        }
        if parts.is_empty() {
            "baseline".to_owned()
        } else {
            parts.join("+")
        }
    }
}

/// Everything the config depends on that is not the revision: where the
/// upstream is, where the tenant keys are, and which table the durable sink
/// writes to.
pub struct Deployment {
    pub upstream_base_url: String,
    pub key_dir: PathBuf,
    pub usage_table: String,
}

impl Deployment {
    /// Write the tenant key files. Called once per run, before any replica
    /// boots: every replica reads the same keys, as a fleet does.
    pub fn stage_keys(&self) {
        std::fs::create_dir_all(&self.key_dir).expect("the tenant key directory is writable");
        for (name, key) in [
            ("byok.key", BYOK_KEY),
            ("fallback.key", FALLBACK_KEY),
            ("probe.key", PROBE_KEY),
        ] {
            // No trailing newline: a static key is exact bytes, and a newline
            // makes the file unusable as a bearer token.
            let path = self.key_dir.join(name);
            std::fs::write(&path, key).expect("a tenant key file is written");
            // Mode 0600, as the sibling stateful harness writes its secret-
            // naming fixtures: a live inbound bearer token under `target/`
            // should not be readable by every local user for the length of a
            // twelve-hour run.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                    .expect("a tenant key file is private");
            }
        }
    }

    /// The tenants the workload rotates over.
    pub fn tenants(&self) -> Vec<Tenant> {
        vec![
            Tenant {
                namespace: PLATFORM,
                key: GATEWAY_KEY.to_owned(),
                credential_source: "platform",
            },
            Tenant {
                namespace: BYOK,
                key: BYOK_KEY.to_owned(),
                credential_source: "byok",
            },
            Tenant {
                namespace: FALLBACK,
                key: FALLBACK_KEY.to_owned(),
                credential_source: "platform",
            },
        ]
    }

    /// The tenant policy is revised against. Not in the workload rotation: its
    /// refusal after the revision has to be unambiguous, and a namespace that
    /// is also offering planned faults cannot give that.
    pub fn probe_tenant(&self) -> Tenant {
        Tenant {
            namespace: PROBE,
            key: PROBE_KEY.to_owned(),
            credential_source: "platform",
        }
    }

    /// The config a replica serves at `revision`.
    pub fn config(&self, bind: SocketAddr, revision: Revision) -> String {
        let price = format!(
            "{{ input_microdollars_per_million = {INPUT_PRICE}, \
             output_microdollars_per_million = {OUTPUT_PRICE} }}"
        );
        let model = |name: &str, provider: &str, upstream_model: &str| {
            format!(
                "[[model]]\nname = \"{name}\"\ntargets = [ {{ provider = \"{provider}\", model = \
                 \"{upstream_model}\", price = {price} }} ]\n\n"
            )
        };
        let catalogue = if revision.catalogue {
            model(CATALOGUE_ALIAS, "fake-openai", target::CHAT)
        } else {
            String::new()
        };
        let (openai_id, anthropic_id, openai_env, anthropic_env) = if revision.credential {
            (
                OPENAI_ROTATED_ID,
                ANTHROPIC_ROTATED_ID,
                OPENAI_SECONDARY_ENV,
                ANTHROPIC_SECONDARY_ENV,
            )
        } else {
            (
                OPENAI_PRIMARY_ID,
                ANTHROPIC_PRIMARY_ID,
                "GW_FAKE_OPENAI_KEY",
                "GW_FAKE_ANTHROPIC_KEY",
            )
        };
        // The policy revision withdraws the probe namespace's permission to
        // borrow the platform pool. BYOK means BYOK, decided per tenant and
        // changed while the fleet serves.
        let probe_fallback = !revision.policy;

        format!(
            r#"
[server]
bind = "{bind}"

[[namespace]]
id = "{PLATFORM}"
default = true

[[namespace]]
id = "{BYOK}"

[[namespace]]
id = "{FALLBACK}"
allow_platform_fallback = true

[[namespace]]
id = "{PROBE}"
allow_platform_fallback = {probe_fallback}

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "{upstream}"

[[provider]]
id = "fake-anthropic"
kind = "anthropic"
base_url = "{upstream}"

[[credential]]
namespace = "{PLATFORM}"
provider = "fake-openai"
env = "{openai_env}"
id = "{openai_id}"

[[credential]]
namespace = "{PLATFORM}"
provider = "fake-anthropic"
env = "{anthropic_env}"
id = "{anthropic_id}"

[[credential]]
namespace = "{BYOK}"
provider = "fake-openai"
env = "{OPENAI_SECONDARY_ENV}"
id = "{BYOK_OPENAI_ID}"

[[credential]]
namespace = "{BYOK}"
provider = "fake-anthropic"
env = "{ANTHROPIC_SECONDARY_ENV}"
id = "{BYOK_ANTHROPIC_ID}"

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "{PLATFORM}"

# Unique to this boot, so a request carrying it can only be answered by the
# process the harness started.
[[gateway_key]]
env = "GW_BOOT_KEY"
namespace = "{PLATFORM}"

[[gateway_key]]
file = "{byok_key}"
namespace = "{BYOK}"

[[gateway_key]]
file = "{fallback_key}"
namespace = "{FALLBACK}"

[[gateway_key]]
file = "{probe_key}"
namespace = "{PROBE}"

# Both sinks, deliberately. The stdout record is what the harness reconciles
# against, request by request, as it is emitted; the durable row is what the
# deployment is qualified on. Comparing the two is the only way to tell a record
# the gateway never made from a record the database never got.
[[usage_sink]]
kind = "stdout"

[[usage_sink]]
kind = "postgres"
dsn_env = "{USAGE_DSN_ENV}"
table = "{table}"
create_table = true
buffer_capacity = 100000
max_batch = 500
flush_interval_ms = 250

# `max_attempts = 1` keeps the planned faults planned: a retry would turn a
# deliberate upstream failure into a success and one accounting row into two.
#
# The breaker bounds are written out because the run is judged on recovery. A
# declared provider outage trips both breakers — that is what they are for — and
# what the qualification asks is that they close again promptly once the
# provider is back, so the cooldown is short, explicit, and pinned by the
# recorded config hash rather than left to the default.
[failover]
max_attempts = 1
overall_timeout_ms = 60000
failure_threshold = 5
cooldown_seconds = 5

[credential_pool]
failure_threshold = 5
cooldown_seconds = 5

[transport]
connect_timeout_ms = 10000
response_header_timeout_ms = 30000
buffered_body_timeout_ms = 30000
stream_idle_timeout_ms = 30000
max_response_bytes = 33554432
max_error_bytes = 65536

# Ceilings far above the offered concurrency: shedding has its own suite, and a
# `503` here is a finding rather than the subject.
[admission]
max_request_bytes = 1048576
max_in_flight = 8192
max_in_flight_streams = 8192
max_in_flight_per_tenant = 0
queue_capacity = 0
queue_wait_ms = 0
max_prompt_tokens = 0
max_output_tokens = 0
max_stream_duration_ms = 0
max_stream_bytes = 0

# A restart is only rolling if the replica leaving actually leaves. Written out
# so the recorded config hash pins the bound the drain is judged against.
[shutdown]
drain_grace_ms = 2000
deadline_ms = 8000
flush_timeout_ms = 5000

{chat}{chat_slow}{chat_drop}{chat_fail}{messages}{messages_slow}{messages_drop}{embeddings}{responses}{catalogue}"#,
            upstream = self.upstream_base_url,
            table = self.usage_table,
            byok_key = self.key_dir.join("byok.key").display(),
            fallback_key = self.key_dir.join("fallback.key").display(),
            probe_key = self.key_dir.join("probe.key").display(),
            chat = model(alias::CHAT, "fake-openai", target::CHAT),
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

    /// The config with this run's own names taken out of it: the temporary key
    /// directory and the ephemeral usage schema. The ports are normalised by
    /// the shared provenance collector. Both of these change every run, and an
    /// input hash that changed every run would make every artifact
    /// incomparable.
    pub fn portable(&self, config: &str) -> String {
        config
            .replace(&self.key_dir.display().to_string(), KEY_DIR_PLACEHOLDER)
            .replace(&self.usage_table, USAGE_TABLE_PLACEHOLDER)
    }
}

/// One running replica, and how the balancer sees it.
pub struct Replica {
    pub id: String,
    pub process: Axond,
    /// The revision this process is serving, which during a rolling restart is
    /// not necessarily the one its neighbour is serving.
    pub revision: Revision,
    /// Whether the balancer may send it work. A replica taken out of rotation
    /// before it is signalled is the whole point of a drain.
    pub in_rotation: bool,
}

impl Replica {
    pub fn base_url(&self) -> &str {
        &self.process.base_url
    }
}

/// The fleet, plus the records of the replicas that have left it.
pub struct Fleet {
    pub deployment: Deployment,
    pub replicas: Vec<Replica>,
    /// Usage records harvested from replicas that no longer exist. A retiring
    /// replica's records are the ones most likely to be lost, so they are taken
    /// from its stdout the moment it exits and kept here.
    pub retired_records: Vec<serde_json::Value>,
    /// The same for the drop reports of replicas that no longer exist: what a
    /// retiring process abandoned is exactly what a rolling restart is asked
    /// not to lose.
    retired_drops: Vec<serde_json::Value>,
    usage_dsn: String,
}

impl Fleet {
    pub async fn start(deployment: Deployment, usage_dsn: String, replicas: usize) -> Self {
        deployment.stage_keys();
        let mut fleet = Self {
            deployment,
            replicas: Vec::new(),
            retired_records: Vec::new(),
            retired_drops: Vec::new(),
            usage_dsn,
        };
        for _ in 0..replicas {
            fleet.admit(Revision::default()).await;
        }
        fleet
    }

    /// Boot one more replica at `revision` and put it in rotation. The id counts
    /// booted processes rather than live ones, so a replacement is never
    /// confused with the replica it replaced.
    pub async fn admit(&mut self, revision: Revision) -> String {
        let id = format!("replica-{}", REPLICAS.fetch_add(1, Ordering::SeqCst));
        let deployment = &self.deployment;
        let process = Axond::start_custom(
            &|addr| deployment.config(addr, revision),
            &[(USAGE_DSN_ENV.to_owned(), self.usage_dsn.clone())],
        )
        .await;
        self.replicas.push(Replica {
            id: id.clone(),
            process,
            revision,
            in_rotation: true,
        });
        id
    }

    /// Publish `revision` to every live replica and reload it in place.
    pub fn publish(&mut self, revision: Revision) {
        for replica in &mut self.replicas {
            let bind: SocketAddr = replica
                .process
                .bind()
                .parse()
                .expect("a replica is bound to a loopback address");
            let config = self.deployment.config(bind, revision);
            replica.process.publish(&config);
            replica.revision = revision;
        }
    }

    /// The base URLs the balancer may use, in a stable order.
    pub fn rotation(&self) -> Vec<String> {
        self.replicas
            .iter()
            .filter(|replica| replica.in_rotation)
            .map(|replica| replica.base_url().to_owned())
            .collect()
    }

    /// Take one replica out of rotation, `SIGTERM` it, wait for it to go, and
    /// keep what it flushed on the way out.
    pub async fn retire(&mut self, id: &str, within: Duration) -> Retired {
        let index = self
            .replicas
            .iter()
            .position(|replica| replica.id == id)
            .unwrap_or_else(|| panic!("{id} is not a live replica"));
        self.replicas[index].in_rotation = false;
        let mut replica = self.replicas.remove(index);
        let signalled = std::time::Instant::now();
        replica.process.terminate();
        let status = replica.process.await_exit(within).await;
        // The records a replica flushes are written just before it exits, so the
        // pipe is drained before the buffer is read.
        replica.process.settle_output(Duration::from_secs(5)).await;
        // Everything the process ever emitted, minus what was already drained
        // from it while it was live, so a record is counted once.
        let records = replica.process.drain_usage_records();
        self.retired_records.extend(records.iter().cloned());
        self.retired_drops
            .extend(replica.process.drain_usage_drops());
        Retired {
            id: replica.id,
            took: status.map(|_| signalled.elapsed()),
            clean: status.is_some_and(|status| status.success()),
            flushed: records.len() as u64,
        }
    }

    /// Take the usage records the fleet has emitted since the last drain: the
    /// live replicas', and whatever a replica flushed on its way out. A run long
    /// enough to settle millions of them reconciles each batch and lets it go.
    ///
    /// The retired replicas' records belong here rather than in a final sweep.
    /// They are the ones a rolling restart is most likely to lose, and a
    /// reconciliation that only ever reads live processes would report every one
    /// of them missing.
    pub fn drain_usage_records(&mut self) -> Vec<serde_json::Value> {
        let mut drained = std::mem::take(&mut self.retired_records);
        drained.extend(
            self.replicas
                .iter()
                .flat_map(|replica| replica.process.drain_usage_records()),
        );
        drained
    }

    /// Take the fleet's reports of usage batches it dropped since the last
    /// drain. A durable row that never arrived is either one of these — the
    /// process saying which sink lost it and why — or a finding.
    pub fn drain_usage_drops(&mut self) -> Vec<serde_json::Value> {
        let mut drained = std::mem::take(&mut self.retired_drops);
        drained.extend(
            self.replicas
                .iter()
                .flat_map(|replica| replica.process.drain_usage_drops()),
        );
        drained
    }

    /// The replicas that exited without being asked to. An abort condition
    /// rather than a slow request: the rest of a run whose fleet is shrinking
    /// measures something other than what it set out to.
    pub async fn departed(&mut self) -> Vec<String> {
        let mut departed = Vec::new();
        for replica in &mut self.replicas {
            if replica.process.await_exit(Duration::ZERO).await.is_some() {
                departed.push(replica.id.clone());
            }
        }
        departed
    }

    pub fn shutdown(&mut self) {
        for replica in &mut self.replicas {
            replica.process.shutdown();
        }
    }
}

/// What a replica's departure cost, and what it flushed.
#[derive(Debug, Clone)]
pub struct Retired {
    pub id: String,
    /// How long the process took to exit; `None` if it outlived the bound.
    pub took: Option<Duration>,
    pub clean: bool,
    pub flushed: u64,
}

/// Whether a replica may be reached at all: `/readyz`, as an orchestrator asks
/// it.
pub async fn ready(client: &reqwest::Client, base_url: &str) -> bool {
    client
        .get(format!("{base_url}/readyz"))
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
}

/// Wait until `base_url` answers `/readyz`, or `within` passes.
pub async fn await_ready(client: &reqwest::Client, base_url: &str, within: Duration) -> bool {
    let deadline = std::time::Instant::now() + within;
    while std::time::Instant::now() < deadline {
        if ready(client, base_url).await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}

/// The directory an artifact, its samples, and its tenant keys are written to.
/// Under `target/` rather than under a temporary directory, so a soak's inputs
/// are still readable after it ends and a wiped `/tmp` cannot take a running
/// replica's inbound keys with it.
pub fn artifact_dir(tier: &str) -> PathBuf {
    let dir = crate::support::capacity::manifest::workspace_root()
        .join("target/stateful-endurance")
        .join(tier);
    std::fs::create_dir_all(&dir).expect("the stateful endurance artifact directory is writable");
    dir
}

/// Where the tenant key files for one run live.
pub fn key_dir(tier: &str, stem: &str) -> PathBuf {
    artifact_dir(tier).join(format!("{stem}-keys"))
}

/// A path is only useful in an artifact if it is relative to the workspace.
pub fn relative(path: &Path) -> String {
    let root = crate::support::capacity::manifest::workspace_root();
    path.strip_prefix(&root)
        .unwrap_or(path)
        .display()
        .to_string()
}
