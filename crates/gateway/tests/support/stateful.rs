//! Fixtures for the stateful integration suite: config files on disk, the
//! operator CLI, and a throwaway control-plane schema.
//!
//! Black-box like the rest of the harness — every helper here drives the shipped
//! binary through its command line and its environment, because the #160 release
//! gates are properties of a deployment rather than of a function.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::oidc::OidcProvider;
use super::schema::Schema;

/// Distinguishes the fixtures of tests running in the same process.
static FIXTURES: AtomicU64 = AtomicU64::new(0);

/// How many ports a replica may lose to a sibling before its boot is the
/// scenario's failure. Eight, because a collision is rare and independent: a run
/// that lost this many is reporting something other than bad luck.
const BOOT_ATTEMPTS: usize = 8;

const AXOND_BIN: &str = env!("CARGO_BIN_EXE_axond");

/// The immutable executable identity captured before any process is spawned.
///
/// Every command and replica resolves through this value. Evidence therefore
/// records the bytes selected for execution, rather than hashing a path for the
/// first time after the scenario has already finished.
struct AxondExecution {
    path: PathBuf,
    sha256: String,
    cargo_profile: &'static str,
}

static AXOND_EXECUTION: OnceLock<AxondExecution> = OnceLock::new();

fn axond_execution() -> &'static AxondExecution {
    AXOND_EXECUTION.get_or_init(|| {
        let path = PathBuf::from(AXOND_BIN);
        assert!(
            path.is_file(),
            "{}: CARGO_BIN_EXE_axond is not a file",
            path.display()
        );
        assert!(
            path.is_absolute(),
            "{}: CARGO_BIN_EXE_axond must be an absolute execution identity",
            path.display()
        );
        let cargo_profile = executable_cargo_profile(&path);
        let sha256 = super::capacity::manifest::sha256_file(&path);
        AxondExecution {
            path,
            sha256,
            cargo_profile,
        }
    })
}

/// The shipped binary, which is what these scenarios qualify.
pub fn axond() -> &'static str {
    axond_execution()
        .path
        .to_str()
        .expect("CARGO_BIN_EXE_axond is valid UTF-8")
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
/// scope, however the scenario ends. The SecretStore is pointed at the same
/// schema rather than left to its default: a replica opens it at boot, and a
/// store on `public` would put every concurrent scenario's material in one table
/// that no fixture's teardown removes.
pub struct ControlPlane {
    pub dsn: String,
    pub schema: String,
    pub config: PathBuf,
    pub cache_path: PathBuf,
    pub env: BTreeMap<&'static str, String>,
    /// Real process-fixture timing retained by recovery evidence. Both values
    /// are captured before schema creation or migration can begin.
    evidence_started_at_unix_ms: u64,
    evidence_started: Instant,
    /// The breakglass credential this fixture's replica accepts, and no other
    /// fixture's does. See [`ControlPlane::spawn`].
    breakglass: String,
    /// The claim on [`ControlPlane::schema`], held from before the schema was
    /// created so a setup step that fails after the `CREATE` still takes it
    /// with it. Declared last, so it is dropped after everything above that was
    /// granted on it.
    _schema: Schema,
}

/// The environment variable names the fixture config refers to. References, not
/// values: the config never inlines a DSN, a KEK, or a credential.
pub const DSN_ENV: &str = "GW_INTEGRATION_CONTROL_PLANE_DSN";
pub const KEK_ENV: &str = "GW_INTEGRATION_KEK";
pub const BREAKGLASS_ENV: &str = "GW_INTEGRATION_BREAKGLASS";
pub const CACHE_KEY_ENV: &str = "GW_INTEGRATION_LKG_KEY";

/// A non-secret fixture value for the deployment KEK reference.
///
/// The configured SecretStore validates the referenced value during boot, so a
/// stateful integration fixture must provide the same shape as an operator's
/// value: base64 encoding of exactly 32 bytes. Encode it at runtime instead of
/// committing the encoded material to the repository or emitting it in config.
pub fn integration_kek() -> String {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    STANDARD.encode([7u8; 32])
}

pub fn integration_cache_key() -> String {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    STANDARD.encode([11u8; 32])
}

/// Retain process-level recovery evidence in the same small JSON contract as
/// The artifact deliberately accepts only caller-supplied observations and
/// bounded status strings; callers cannot pass provider material through this
/// helper by accident.
const RECOVERY_GATE_NAMES: [&str; 6] = [
    "max_serving_error_fraction",
    "max_convergence_lag_seconds",
    "max_data_loss_revisions",
    "readiness",
    "admin_writes",
    "max_unauthenticated_admin_successes",
];

fn recovery_gate_bound(scenario: &str, gate: &str) -> &'static str {
    match gate {
        "max_serving_error_fraction" => "0.0",
        "max_convergence_lag_seconds" => match scenario {
            "backup-restore" | "point-in-time-recovery" => "120",
            _ => "60",
        },
        "max_data_loss_revisions" | "max_unauthenticated_admin_successes" => "0",
        "readiness" => match scenario {
            "cold-boot-no-cache" | "cold-boot-invalid-cache" => "refuses",
            _ => "serves",
        },
        "admin_writes" => match scenario {
            "control-plane-outage"
            | "cold-boot-valid-cache"
            | "cold-boot-no-cache"
            | "cold-boot-invalid-cache" => "unavailable",
            _ => "accepted",
        },
        _ => panic!("{gate}: unknown recovery scenario gate"),
    }
}

fn recovery_gate_evidence(gate: &str) -> &'static [&'static str] {
    match gate {
        "max_serving_error_fraction" => &["serving_behavior"],
        "max_convergence_lag_seconds" => &["convergence_lag"],
        "max_data_loss_revisions" => &["revision_loss_boundary"],
        "readiness" => &["serving_behavior", "fail_open_closed", "cold_start"],
        "admin_writes" => &[
            "audit_auth",
            "outage_timeline",
            "revisions",
            "fail_open_closed",
        ],
        "max_unauthenticated_admin_successes" => &["audit_auth"],
        _ => panic!("{gate}: unknown recovery scenario gate"),
    }
}

fn process_recovery_observations(scenario: &str, stage: &str) -> &'static [&'static str] {
    match (scenario, stage) {
        ("control-plane-outage", "journal-outage") => &[
            "revision",
            "active_revision",
            "convergence_rejection_reason",
            "convergence_lag_ms",
            "proxy_severed_connections",
            "admin_write_status",
            "admin_write_error",
        ],
        ("control-plane-outage", "serving") => &[
            "revision",
            "proxy_severed_connections",
            "inference_status",
            "ready_status",
        ],
        ("control-plane-outage", "administration") => &[
            "authenticated_state_status",
            "mutation_status",
            "anonymous_state_status",
        ],
        ("cold-boot-valid-cache", "serving") => &[
            "ready_status",
            "chat_status",
            "anonymous_models_status",
            "admin_catalogue_status",
            "admin_mutation_status",
            "anonymous_admin_status",
        ],
        ("cold-boot-valid-cache", "cold-boot") => &[
            "boot_note",
            "cold_start_outcome",
            "cached_revision",
            "restored_revision",
            "active_revision",
            "snapshot_source",
            "ready_status",
        ],
        ("cold-boot-no-cache", "cold-boot") => &[
            "boot_note",
            "cold_start_outcome",
            "refusal",
            "snapshot_generation_after_cold_boot",
            "ready_status",
            "active_revision",
            "anonymous_models_status",
        ],
        ("cold-boot-invalid-cache", "cold-boot") => &[
            "boot_note",
            "cold_start_outcome",
            "unauthentic_cache_variants_refused",
            "ready_status",
            "edited_record_refused",
            "truncated_file_refused",
            "foreign_signing_key_refused",
        ],
        ("cold-boot-no-cache" | "cold-boot-invalid-cache", "readiness") => &[
            "ready_status",
            "admin_state_status",
            "admin_mutation_status",
            "anonymous_admin_status",
            "anonymous_models_status",
        ],
        ("recovery-convergence", "serving") => &[
            "revision",
            "source",
            "converged",
            "chat_status",
            "ready_status",
        ],
        ("recovery-convergence", "journal-recovery") => &[
            "outage_revision",
            "unseen_revision",
            "recovered_head_revision",
            "active_revision",
            "direct_replica_active_revision",
            "loaded_unseen_revision",
            "snapshot_source",
            "converged",
            "residual_lag_ms",
            "recovery_seconds",
            "recovered_history_revisions",
            "recovered_history_contains_required_revisions",
            "post_recovery_write_accepted",
        ],
        ("recovery-convergence", "administration") => {
            &["audit_status", "actor", "anonymous_admin_status"]
        }
        ("secret-rotation", "rotation") => &[
            "revision",
            "active_revision",
            "source",
            "converged",
            "rotation_seconds",
            "publication_accepted",
            "rotated_revision_published",
            "rotation_history_contains_required_revisions",
            "same_replica_before_and_after_rotation",
        ],
        ("secret-rotation", "serving") => &[
            "chat_status",
            "ready_status",
            "audit_status",
            "audit_actor",
            "credential",
            "anonymous_admin_status",
            "rotated_material_authenticated_upstream",
        ],
        _ => panic!("{scenario}/{stage}: no process-backed recovery observation contract"),
    }
}

/// The complete check vocabulary for each process-backed stage.
///
/// This is intentionally separate from the caller's slice. A producer cannot
/// make an inconvenient check disappear, invent a new check, or satisfy a
/// named check with two equal strings that were never retained as observations.
fn process_recovery_checks(scenario: &str, stage: &str) -> &'static [&'static str] {
    match (scenario, stage) {
        ("control-plane-outage", "journal-outage") => &[
            "active_revision_survives_the_cut",
            "convergence_reports_unavailable",
            "administrative_write_is_typed",
        ],
        ("control-plane-outage", "serving") => {
            &["inference_remains_available", "postgres_path_was_severed"]
        }
        ("control-plane-outage", "administration") => &[
            "authenticated_administration_refused",
            "mutation_refused",
            "anonymous_administration_refused",
        ],
        ("recovery-convergence", "journal-recovery") => &[
            "unseen_revision_loaded",
            "recovered_head_active",
            "fleet_reaches_one_head",
            "recovered_history_is_whole",
        ],
        ("recovery-convergence", "serving") => {
            &["recovered_revision_loaded", "recovered_inference_served"]
        }
        ("recovery-convergence", "administration") => &[
            "recovered_audit_is_readable",
            "recovered_audit_is_authenticated",
        ],
        ("secret-rotation", "rotation") => &["rotated_revision_published", "no_restart"],
        ("secret-rotation", "serving") => &[
            "rotated_material_authenticated_upstream",
            "authenticated_audit_attribution",
        ],
        ("cold-boot-valid-cache", "cold-boot") => &[
            "restored_revision_is_cached_revision",
            "snapshot_source_is_last_known_good",
            "cold_process_is_ready",
        ],
        ("cold-boot-valid-cache", "serving") => &[
            "readiness_serves_cached_snapshot",
            "anonymous_inference_refused",
            "administrative_read_refused_without_control_plane",
        ],
        ("cold-boot-invalid-cache", "cold-boot") => &[
            "edited_record_refused",
            "truncated_file_refused",
            "foreign_signing_key_refused",
        ],
        ("cold-boot-no-cache", "cold-boot") => &[
            "no_active_revision",
            "readiness_refuses_without_cache",
            "authentication_remains_first",
        ],
        ("cold-boot-no-cache", "readiness") => &[
            "readiness_refuses_without_cache",
            "authenticated_administration_refuses_without_control_plane",
            "administration_requires_authentication",
        ],
        ("cold-boot-invalid-cache", "readiness") => &[
            "readiness_refuses_with_invalid_cache",
            "authenticated_administration_refuses_without_control_plane",
            "administration_requires_authentication",
        ],
        _ => panic!("{scenario}/{stage}: no process-backed recovery check contract"),
    }
}

fn recovery_observation<'a>(
    observations: &'a BTreeMap<String, serde_json::Value>,
    scenario: &str,
    stage: &str,
    check: &str,
    key: &str,
) -> &'a serde_json::Value {
    observations.get(key).unwrap_or_else(|| {
        panic!("{scenario}/{stage}: check {check:?} requires observation {key:?}")
    })
}

fn recovery_observation_text(
    observations: &BTreeMap<String, serde_json::Value>,
    scenario: &str,
    stage: &str,
    check: &str,
    key: &str,
) -> String {
    match recovery_observation(observations, scenario, stage, check, key) {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        value => panic!(
            "{scenario}/{stage}: check {check:?} observation {key:?} must be a scalar, got {value}"
        ),
    }
}

fn recovery_boolean_label(
    observations: &BTreeMap<String, serde_json::Value>,
    scenario: &str,
    stage: &str,
    check: &str,
    key: &str,
    met: &str,
    unmet: &str,
) -> String {
    match recovery_observation(observations, scenario, stage, check, key).as_bool() {
        Some(true) => met.to_owned(),
        Some(false) => unmet.to_owned(),
        None => panic!("{scenario}/{stage}: check {check:?} observation {key:?} must be a boolean"),
    }
}

/// Reconstruct a check's comparison exclusively from retained observations and
/// committed invariants. The strings supplied by the call site are assertions
/// about this result, not evidence in their own right.
fn recovery_check_comparison(
    scenario: &str,
    stage: &str,
    check: &str,
    observations: &BTreeMap<String, serde_json::Value>,
) -> (String, String) {
    let literal_observation = |expected: &str, key: &str| {
        (
            expected.to_owned(),
            recovery_observation_text(observations, scenario, stage, check, key),
        )
    };
    let observation_pair = |expected_key: &str, observed_key: &str| {
        (
            recovery_observation_text(observations, scenario, stage, check, expected_key),
            recovery_observation_text(observations, scenario, stage, check, observed_key),
        )
    };
    let boolean_invariant = |key: &str| {
        (
            "true".to_owned(),
            recovery_boolean_label(observations, scenario, stage, check, key, "true", "false"),
        )
    };

    match (scenario, stage, check) {
        ("control-plane-outage", "journal-outage", "active_revision_survives_the_cut") => {
            observation_pair("revision", "active_revision")
        }
        ("control-plane-outage", "journal-outage", "convergence_reports_unavailable") => {
            literal_observation("unavailable", "convergence_rejection_reason")
        }
        ("control-plane-outage", "journal-outage", "administrative_write_is_typed") => {
            literal_observation("control_plane_unavailable", "admin_write_error")
        }
        ("control-plane-outage", "serving", "inference_remains_available") => {
            literal_observation("200", "inference_status")
        }
        ("control-plane-outage", "serving", "postgres_path_was_severed") => {
            let severed = recovery_observation(
                observations,
                scenario,
                stage,
                check,
                "proxy_severed_connections",
            )
            .as_u64()
            .unwrap_or_else(|| {
                panic!(
                    "{scenario}/{stage}: check {check:?} observation \"proxy_severed_connections\" must be an unsigned integer"
                )
            });
            (
                "at-least-one".to_owned(),
                if severed > 0 {
                    "at-least-one".to_owned()
                } else {
                    "none".to_owned()
                },
            )
        }
        ("control-plane-outage", "administration", "authenticated_administration_refused") => {
            literal_observation("503", "authenticated_state_status")
        }
        ("control-plane-outage", "administration", "mutation_refused") => {
            literal_observation("503", "mutation_status")
        }
        ("control-plane-outage", "administration", "anonymous_administration_refused") => {
            literal_observation("401", "anonymous_state_status")
        }
        ("recovery-convergence", "journal-recovery", "unseen_revision_loaded") => {
            observation_pair("unseen_revision", "loaded_unseen_revision")
        }
        ("recovery-convergence", "journal-recovery", "recovered_head_active") => {
            observation_pair("recovered_head_revision", "active_revision")
        }
        ("recovery-convergence", "journal-recovery", "fleet_reaches_one_head") => {
            observation_pair("recovered_head_revision", "direct_replica_active_revision")
        }
        ("recovery-convergence", "journal-recovery", "recovered_history_is_whole") => (
            "three-required-revisions".to_owned(),
            recovery_boolean_label(
                observations,
                scenario,
                stage,
                check,
                "recovered_history_contains_required_revisions",
                "three-required-revisions",
                "incomplete-history",
            ),
        ),
        ("recovery-convergence", "serving", "recovered_revision_loaded") => {
            boolean_invariant("converged")
        }
        ("recovery-convergence", "serving", "recovered_inference_served") => {
            literal_observation("200", "chat_status")
        }
        ("recovery-convergence", "administration", "recovered_audit_is_readable") => {
            literal_observation("200", "audit_status")
        }
        ("recovery-convergence", "administration", "recovered_audit_is_authenticated") => {
            literal_observation("breakglass", "actor")
        }
        ("secret-rotation", "rotation", "rotated_revision_published") => {
            boolean_invariant("rotated_revision_published")
        }
        ("secret-rotation", "rotation", "no_restart") => {
            boolean_invariant("same_replica_before_and_after_rotation")
        }
        ("secret-rotation", "serving", "rotated_material_authenticated_upstream") => {
            boolean_invariant("rotated_material_authenticated_upstream")
        }
        ("secret-rotation", "serving", "authenticated_audit_attribution") => {
            literal_observation("breakglass", "audit_actor")
        }
        ("cold-boot-valid-cache", "cold-boot", "restored_revision_is_cached_revision") => {
            observation_pair("cached_revision", "active_revision")
        }
        ("cold-boot-valid-cache", "cold-boot", "snapshot_source_is_last_known_good") => {
            literal_observation("last-known-good", "snapshot_source")
        }
        ("cold-boot-valid-cache", "cold-boot", "cold_process_is_ready") => {
            literal_observation("200", "ready_status")
        }
        ("cold-boot-valid-cache", "serving", "readiness_serves_cached_snapshot") => {
            literal_observation("200", "ready_status")
        }
        ("cold-boot-valid-cache", "serving", "anonymous_inference_refused") => {
            literal_observation("401", "anonymous_models_status")
        }
        (
            "cold-boot-valid-cache",
            "serving",
            "administrative_read_refused_without_control_plane",
        ) => literal_observation("503", "admin_catalogue_status"),
        ("cold-boot-invalid-cache", "cold-boot", "edited_record_refused") => (
            "refused".to_owned(),
            recovery_boolean_label(
                observations,
                scenario,
                stage,
                check,
                "edited_record_refused",
                "refused",
                "accepted",
            ),
        ),
        ("cold-boot-invalid-cache", "cold-boot", "truncated_file_refused") => (
            "refused".to_owned(),
            recovery_boolean_label(
                observations,
                scenario,
                stage,
                check,
                "truncated_file_refused",
                "refused",
                "accepted",
            ),
        ),
        ("cold-boot-invalid-cache", "cold-boot", "foreign_signing_key_refused") => (
            "refused".to_owned(),
            recovery_boolean_label(
                observations,
                scenario,
                stage,
                check,
                "foreign_signing_key_refused",
                "refused",
                "accepted",
            ),
        ),
        ("cold-boot-no-cache", "cold-boot", "no_active_revision") => {
            let active =
                recovery_observation(observations, scenario, stage, check, "active_revision");
            (
                "none".to_owned(),
                if active.is_null() {
                    "none".to_owned()
                } else {
                    "present".to_owned()
                },
            )
        }
        ("cold-boot-no-cache", "cold-boot", "readiness_refuses_without_cache") => {
            literal_observation("503", "ready_status")
        }
        ("cold-boot-no-cache", "cold-boot", "authentication_remains_first") => {
            literal_observation("401", "anonymous_models_status")
        }
        ("cold-boot-no-cache", "readiness", "readiness_refuses_without_cache") => {
            literal_observation("503", "ready_status")
        }
        (
            "cold-boot-no-cache" | "cold-boot-invalid-cache",
            "readiness",
            "authenticated_administration_refuses_without_control_plane",
        ) => literal_observation("503", "admin_state_status"),
        (
            "cold-boot-no-cache" | "cold-boot-invalid-cache",
            "readiness",
            "administration_requires_authentication",
        ) => literal_observation("401", "anonymous_admin_status"),
        ("cold-boot-invalid-cache", "readiness", "readiness_refuses_with_invalid_cache") => {
            literal_observation("503", "ready_status")
        }
        _ => panic!("{scenario}/{stage}: check {check:?} has no observation binding"),
    }
}

/// Reconstruct a process gate from the retained black-box observation that
/// measured it. This makes the caller's tuple an assertion about evidence,
/// never an independently supplied passing verdict.
fn recovery_gate_observation(
    scenario: &str,
    stage: &str,
    gate: &str,
    observations: &BTreeMap<String, serde_json::Value>,
) -> String {
    let text = |key: &str| recovery_observation_text(observations, scenario, stage, gate, key);
    let status = |key: &str| {
        recovery_observation(observations, scenario, stage, gate, key)
            .as_u64()
            .unwrap_or_else(|| {
                panic!(
                    "{scenario}/{stage}: gate {gate:?} observation {key:?} must be an HTTP status"
                )
            })
    };
    let boolean = |key: &str| {
        recovery_observation(observations, scenario, stage, gate, key)
            .as_bool()
            .unwrap_or_else(|| {
                panic!("{scenario}/{stage}: gate {gate:?} observation {key:?} must be boolean")
            })
    };
    let zero_if_equal = |expected: &str, actual: &str| {
        if text(expected) == text(actual) {
            "0"
        } else {
            "1"
        }
        .to_owned()
    };
    let zero_error_fraction = |key: &str| {
        if (200..300).contains(&status(key)) {
            "0.0".to_owned()
        } else {
            "1.0".to_owned()
        }
    };
    let readiness = |key: &str| {
        if status(key) == 200 {
            "serves"
        } else {
            "refuses"
        }
        .to_owned()
    };
    let administration = |key: &str| {
        if (200..300).contains(&status(key)) {
            "accepted"
        } else {
            "unavailable"
        }
        .to_owned()
    };
    let unauthenticated_successes = |key: &str| {
        if (200..300).contains(&status(key)) {
            "1"
        } else {
            "0"
        }
        .to_owned()
    };

    match (scenario, stage, gate) {
        ("control-plane-outage", "journal-outage", "max_data_loss_revisions") => {
            zero_if_equal("revision", "active_revision")
        }
        ("control-plane-outage", "journal-outage", "admin_writes") => {
            administration("admin_write_status")
        }
        ("control-plane-outage", "serving", "max_serving_error_fraction") => {
            zero_error_fraction("inference_status")
        }
        ("control-plane-outage", "serving", "readiness") => readiness("ready_status"),
        ("control-plane-outage", "administration", "max_unauthenticated_admin_successes") => {
            unauthenticated_successes("anonymous_state_status")
        }
        ("recovery-convergence", "journal-recovery", "max_convergence_lag_seconds") => {
            text("recovery_seconds")
        }
        ("recovery-convergence", "journal-recovery", "max_data_loss_revisions") => {
            if boolean("recovered_history_contains_required_revisions") {
                "0"
            } else {
                "1"
            }
            .to_owned()
        }
        ("recovery-convergence", "journal-recovery", "admin_writes") => {
            if boolean("post_recovery_write_accepted") {
                "accepted"
            } else {
                "unavailable"
            }
            .to_owned()
        }
        ("recovery-convergence", "serving", "max_serving_error_fraction") => {
            zero_error_fraction("chat_status")
        }
        ("recovery-convergence", "serving", "readiness") => readiness("ready_status"),
        ("recovery-convergence", "administration", "max_unauthenticated_admin_successes") => {
            unauthenticated_successes("anonymous_admin_status")
        }
        ("secret-rotation", "rotation", "max_convergence_lag_seconds") => text("rotation_seconds"),
        ("secret-rotation", "rotation", "max_data_loss_revisions") => {
            if boolean("rotation_history_contains_required_revisions") {
                "0"
            } else {
                "1"
            }
            .to_owned()
        }
        ("secret-rotation", "rotation", "admin_writes") => if boolean("publication_accepted") {
            "accepted"
        } else {
            "unavailable"
        }
        .to_owned(),
        ("secret-rotation", "serving", "max_serving_error_fraction") => {
            zero_error_fraction("chat_status")
        }
        ("secret-rotation", "serving", "readiness") => readiness("ready_status"),
        ("secret-rotation", "serving", "max_unauthenticated_admin_successes") => {
            unauthenticated_successes("anonymous_admin_status")
        }
        ("cold-boot-valid-cache", "cold-boot", "max_data_loss_revisions") => {
            zero_if_equal("cached_revision", "active_revision")
        }
        ("cold-boot-valid-cache", "cold-boot", "readiness") => readiness("ready_status"),
        ("cold-boot-valid-cache", "serving", "max_serving_error_fraction") => {
            zero_error_fraction("chat_status")
        }
        ("cold-boot-valid-cache", "serving", "admin_writes") => {
            administration("admin_mutation_status")
        }
        ("cold-boot-valid-cache", "serving", "max_unauthenticated_admin_successes") => {
            unauthenticated_successes("anonymous_admin_status")
        }
        ("cold-boot-no-cache", "cold-boot", "readiness") => readiness("ready_status"),
        ("cold-boot-invalid-cache", "cold-boot", "readiness") => readiness("ready_status"),
        ("cold-boot-no-cache", "readiness", "admin_writes")
        | ("cold-boot-invalid-cache", "readiness", "admin_writes") => {
            administration("admin_mutation_status")
        }
        (
            "cold-boot-no-cache" | "cold-boot-invalid-cache",
            "readiness",
            "max_unauthenticated_admin_successes",
        ) => unauthenticated_successes("anonymous_admin_status"),
        _ => panic!("{scenario}/{stage}: gate {gate:?} has no observation binding"),
    }
}

fn executable_cargo_profile(path: &Path) -> &'static str {
    let profile =
        path.components()
            .rev()
            .find_map(|component| match component.as_os_str().to_str() {
                Some("release") => Some("release"),
                Some("debug") => Some("debug"),
                _ => None,
            });
    profile.unwrap_or_else(|| {
        panic!(
            "{}: CARGO_BIN_EXE_axond does not identify a Cargo release/debug profile",
            path.display()
        )
    })
}

fn numeric_gate_met(bound: &str, observed: &str) -> bool {
    let bound: f64 = bound.parse().expect("a numeric recovery gate bound");
    let observed: f64 = observed
        .parse()
        .expect("a numeric recovery gate observation");
    bound.is_finite() && observed.is_finite() && observed <= bound
}

#[allow(clippy::too_many_arguments)]
pub fn write_recovery_artifact(
    control_plane: &ControlPlane,
    scenario: &str,
    stage: &str,
    capability: &str,
    evidence: &[&str],
    events: &[(&str, &str)],
    observations: &[(&str, serde_json::Value)],
    gates: &[(&str, &str, &str, &str)],
    checks: &[(&str, &str, &str, &str)],
) {
    let started_at_unix_ms = control_plane.evidence_started_at_unix_ms;
    let elapsed_ms = u64::try_from(control_plane.evidence_started.elapsed().as_millis())
        .expect("the recovery fixture duration fits in u64 milliseconds");
    let timeline: Vec<serde_json::Value> = events
        .iter()
        .map(|(event, detail)| {
            serde_json::json!({
                // Event labels are reduced after the black-box assertions have
                // run. Record their real capture offset instead of inventing a
                // one-millisecond sequence that never occurred.
                "at_ms": elapsed_ms,
                "event": event,
                "detail": detail,
            })
        })
        .collect();
    assert!(
        !events.is_empty(),
        "{scenario}/{stage}: recovery timeline is empty"
    );
    let mut observation_map = BTreeMap::new();
    for (key, value) in observations {
        assert!(
            observation_map
                .insert((*key).to_owned(), value.clone())
                .is_none(),
            "{scenario}/{stage}: observation {key:?} is duplicated"
        );
    }
    let observations = observation_map;
    for required in process_recovery_observations(scenario, stage) {
        let value = observations.get(*required).unwrap_or_else(|| {
            panic!("{scenario}/{stage}: required observation {required:?} is missing")
        });
        if (scenario, stage, *required) == ("cold-boot-no-cache", "cold-boot", "active_revision") {
            assert!(
                value.is_null(),
                "{scenario}/{stage}: active_revision must be null to prove no snapshot was published"
            );
        } else {
            assert!(
                !value.is_null(),
                "{scenario}/{stage}: required observation {required:?} is null"
            );
        }
    }

    let mut recorded_gates = std::collections::BTreeSet::new();
    let mut gate_verdicts = Vec::new();
    for (gate, bound, observed, detail) in gates {
        assert!(
            recorded_gates.insert(*gate),
            "{scenario}/{stage}: gate {gate:?} is duplicated"
        );
        assert!(
            RECOVERY_GATE_NAMES.contains(gate),
            "{scenario}/{stage}: gate {gate:?} is not a scenario gate"
        );
        let expected_bound = recovery_gate_bound(scenario, gate);
        let bound_matches = if gate.starts_with("max_") {
            let expected: f64 = expected_bound.parse().expect("a numeric manifest gate");
            let actual: f64 = bound.parse().expect("a numeric recorded gate");
            expected == actual
        } else {
            *bound == expected_bound
        };
        assert!(
            bound_matches,
            "{scenario}/{stage}: gate {gate:?} bound {bound:?} does not match {expected_bound:?}"
        );
        assert!(
            recovery_gate_evidence(gate)
                .iter()
                .any(|class| evidence.contains(class)),
            "{scenario}/{stage}: gate {gate:?} has no supporting evidence class"
        );
        let reconstructed = recovery_gate_observation(scenario, stage, gate, &observations);
        assert_eq!(
            *observed, reconstructed,
            "{scenario}/{stage}: gate {gate:?} is not derived from its retained observation"
        );
        let met = if gate.starts_with("max_") {
            numeric_gate_met(bound, observed)
        } else {
            *bound == *observed
        };
        assert!(
            met,
            "{scenario}/{stage}: gate {gate:?} does not follow from {observed:?} against {bound:?}"
        );
        gate_verdicts.push(serde_json::json!({
            "gate": gate,
            "bound": bound,
            "observed": observed,
            "outcome": "met",
            "detail": detail,
        }));
    }
    for gate in RECOVERY_GATE_NAMES {
        if recorded_gates.contains(gate) {
            continue;
        }
        let required = recovery_gate_evidence(gate).join(", ");
        let retained = evidence.join(", ");
        gate_verdicts.push(serde_json::json!({
            "gate": gate,
            "bound": recovery_gate_bound(scenario, gate),
            "observed": "not measured",
            "outcome": "not_evaluated",
            "detail": format!(
                "requires evidence class [{required}]; this stage retains [{retained}]. \
                 The process-backed stage did not reduce this observation to the scenario gate."
            ),
        }));
    }

    let required_checks = process_recovery_checks(scenario, stage);
    let mut recorded_checks = std::collections::BTreeSet::new();
    let check_verdicts = checks
        .iter()
        .map(|(check, bound, observed, detail)| {
            assert!(
                recorded_checks.insert(*check),
                "{scenario}/{stage}: check {check:?} is duplicated"
            );
            assert!(
                required_checks.contains(check),
                "{scenario}/{stage}: check {check:?} is not in the committed stage contract"
            );
            let (reconstructed_bound, reconstructed_observed) =
                recovery_check_comparison(scenario, stage, check, &observations);
            assert_eq!(
                *bound,
                reconstructed_bound.as_str(),
                "{scenario}/{stage}: check {check:?} supplied bound is unrelated to its retained observations"
            );
            assert_eq!(
                *observed,
                reconstructed_observed.as_str(),
                "{scenario}/{stage}: check {check:?} supplied observation is unrelated to its retained observations"
            );
            assert_eq!(
                bound, observed,
                "{scenario}/{stage}: check {check:?} does not follow from its comparison"
            );
            serde_json::json!({
                "gate": check,
                "bound": bound,
                "observed": observed,
                "outcome": "met",
                "detail": detail,
            })
        })
        .collect::<Vec<_>>();
    for required in required_checks {
        assert!(
            recorded_checks.contains(required),
            "{scenario}/{stage}: required check {required:?} is missing"
        );
    }

    let execution = axond_execution();
    let cargo_profile = execution.cargo_profile;
    assert_eq!(
        cargo_profile, "release",
        "{scenario}/{stage}: promotable process-backed recovery evidence requires a release executable"
    );
    let executable_sha256 = super::capacity::manifest::sha256_file(&execution.path);
    assert_eq!(
        executable_sha256, execution.sha256,
        "{scenario}/{stage}: the axond executable changed after its pre-spawn identity was captured"
    );
    let executed_sha256 = execution.sha256.clone();
    let executable_path = execution.path.to_string_lossy().into_owned();
    let schema_identity = control_plane.recovery_schema_identity();
    let artifact = serde_json::json!({
        "schema_version": 2,
        "scenario": scenario,
        "stage": stage,
        "runner": "stateful-tests",
        "capability": capability,
        "evidence": evidence,
        "run": {
            "started_at_unix_ms": started_at_unix_ms,
            "elapsed_ms": elapsed_ms,
            "axond_version": env!("CARGO_PKG_VERSION"),
            "control_plane": "postgres",
            "schema": control_plane.schema,
            "schema_identity": schema_identity,
            "axond_executable_sha256": executable_sha256,
            "axond_executed_sha256": executed_sha256,
            "axond_executable_path": executable_path,
            "axond_execution_bound": true,
            "cargo_profile": cargo_profile,
        },
        "timeline": timeline,
        "observations": observations,
        "gates": gate_verdicts,
        "checks": check_verdicts,
    });
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/recovery");
    std::fs::create_dir_all(&directory).expect("the recovery evidence directory is writable");
    let path = directory.join(format!("{scenario}.{stage}.json"));
    let text = serde_json::to_string_pretty(&artifact).expect("the recovery artifact serializes");
    std::fs::write(&path, format!("{text}\n")).expect("the recovery artifact is writable");
}

impl ControlPlane {
    /// `None` when no test database is configured.
    pub async fn create() -> Option<Self> {
        let evidence_started_at_unix_ms = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("the clock is after the epoch")
                .as_millis(),
        )
        .expect("the current Unix time fits in u64 milliseconds");
        let evidence_started = Instant::now();
        let dsn = postgres_dsn()?;
        let fixture = format!(
            "{}_{}",
            std::process::id(),
            FIXTURES.fetch_add(1, Ordering::SeqCst)
        );
        let schema = format!("axond_it_{fixture}");
        let breakglass = format!("integration-test-breakglass-{fixture}");
        let cache_path = std::env::temp_dir().join(format!("axond-stateful-{fixture}.snapshot"));
        let compiled_cache_path = cache_path.with_extension("serving");
        // Pids can be reused between test processes. Remove only this fixture's
        // exact cache paths so a prior process cannot turn a no-cache boot into a
        // cache-refusal scenario.
        let _ = std::fs::remove_file(&cache_path);
        let _ = std::fs::remove_file(compiled_cache_path);
        let cache_path_text = cache_path.display().to_string();
        // Before the config is rendered and before any later step can fail: the
        // cleanup has to be owned from the moment the schema exists.
        let claim = Schema::create(&dsn, &schema).await;
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
                 schema = \"{schema}\"\n\
                 [convergence]\n\
                 cache_path = \"{cache_path_text}\"\n\
                 cache_key_env = \"{CACHE_KEY_ENV}\"\n\
                 [catalog]\n\
                 source = \"seed\"\n\
                 store = \"postgres\"\n\
                 schema = \"{schema}\"\n\
                 bootstrap = \"seed\"\n\
                 [[admin_breakglass]]\n\
                 env = \"{BREAKGLASS_ENV}\"\n\
                 id = \"breakglass\"\n"
            ),
        );
        let env = BTreeMap::from([
            (DSN_ENV, dsn.clone()),
            // Fixture values satisfy reference validation without being logged or inlined.
            (KEK_ENV, integration_kek()),
            (CACHE_KEY_ENV, integration_cache_key()),
            (BREAKGLASS_ENV, breakglass.clone()),
        ]);
        Some(Self {
            dsn,
            schema,
            config,
            cache_path,
            env,
            evidence_started_at_unix_ms,
            evidence_started,
            breakglass,
            _schema: claim,
        })
    }

    pub fn run(&self, args: &[&str]) -> Run {
        run(&self.config, args, &self.env)
    }

    fn recovery_schema_identity(&self) -> String {
        let status = self.run(&["migrate", "status"]);
        assert!(
            status.succeeded(),
            "recovery evidence could not read the migrated schema identity:\n{}",
            status.context()
        );
        let reported = status.stdout();
        let normalized = reported.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            !normalized.is_empty(),
            "recovery evidence received an empty `migrate status` identity"
        );
        format!("{}: {normalized}", self.schema)
    }

    /// Add the explicit local OIDC verifier boundary to this fixture's
    /// bootstrap. Breakglass remains configured as the recovery credential.
    pub fn enable_oidc(&self, provider: &OidcProvider) {
        let mut config = std::fs::read_to_string(&self.config).expect("read fixture config");
        config.push_str(&format!(
            "\n[admin_oidc]\nissuer = \"{}\"\naudience = \"{}\"\njwks_url = \"{}\"\n",
            provider.issuer(),
            provider.audience(),
            provider.jwks_url(),
        ));
        std::fs::write(&self.config, config).expect("write OIDC fixture config");
    }

    /// Whether the migration ledger exists — observed on a connection of the
    /// test's own, so a read-only claim is checked from outside the command that
    /// made it.
    pub async fn ledger_exists(&self) -> bool {
        self.table_exists("axond_cp_schema_migration").await
    }

    /// Whether `table` exists *in this scenario's schema*, observed the way
    /// [`Self::ledger_exists`] is.
    pub async fn table_exists(&self, table: &str) -> bool {
        client(&self.dsn)
            .await
            .query_one(
                "SELECT to_regclass($1)::text",
                &[&format!("{}.{table}", self.schema)],
            )
            .await
            .expect("probe a table")
            .get::<_, Option<String>>(0)
            .is_some()
    }

    /// The raw payload and normalized content identities imported from the
    /// bundled seed. Admin revisions pin the former; approved prices pin the
    /// latter.
    pub async fn catalogue_identity(&self) -> Option<(String, String, u64)> {
        client(&self.dsn)
            .await
            .query_opt(
                &format!(
                    "SELECT raw_digest, content_id, raw_bytes FROM {}.axond_catalog_snapshot ORDER BY imported_at LIMIT 1",
                    self.schema
                ),
                &[],
            )
            .await
            .expect("read the seeded catalogue identity")
            .map(|row| (row.get(0), row.get(1), row.get::<_, i64>(2) as u64))
    }

    /// A replica of this deployment, running until the fixture is dropped.
    ///
    /// The control plane must already be migrated: a replica opens it at boot,
    /// so an unprepared schema is a boot failure rather than a scenario.
    ///
    /// Retried rather than attempted once, because an ephemeral port is free the
    /// moment [`free_addr`] hands it over: a sibling scenario booting its own
    /// replica can take it before this child binds it, and that child then exits
    /// on the bind. A retry gives up the lost port and asks for another one, so a
    /// collision costs an attempt instead of failing a scenario for a reason it
    /// does not state.
    pub async fn serve(&self) -> Replica {
        self.serve_with_dsn_mode(&self.dsn, false).await
    }

    /// Start a replica through a caller-controlled TCP path to the same control
    /// plane. The stateful recovery integration uses this to sever live
    /// Postgres connections while leaving the database and the direct admin
    /// replica available for the recovery publication.
    pub async fn serve_with_dsn(&self, dsn: &str) -> Replica {
        self.serve_with_dsn_mode(dsn, false).await
    }

    /// Start a replacement replica with the control plane unreachable. This is
    /// local fault injection: it avoids stopping the shared test Postgres while
    /// exercising the deferred-store and encrypted-cache recovery path.
    pub async fn serve_without_control_plane(&self) -> Replica {
        self.serve_with_dsn_mode("postgres://axond@127.0.0.1:1/axond", true)
            .await
    }

    /// Start a replica without a serving cache while the control plane is
    /// reachable. The administrative surface remains available so the
    /// black-box readiness stages can distinguish a booted, fail-closed process
    /// from a process that never started.
    pub async fn serve_unready(&self) -> Replica {
        self.serve_with_dsn_mode(&self.dsn, false).await
    }

    /// Start a cacheless replica while the control plane is unreachable. The
    /// listener remains alive, but authenticated administrative reads refuse
    /// with dependency-unavailable and readiness stays closed.
    pub async fn serve_without_control_plane_unready(&self) -> Replica {
        self.serve_with_dsn_mode("postgres://axond@127.0.0.1:1/axond", false)
            .await
    }

    async fn serve_with_dsn_mode(&self, dsn: &str, recovery: bool) -> Replica {
        let mut reported = String::new();
        for attempt in 0..BOOT_ATTEMPTS {
            match self.spawn(attempt, dsn, recovery).await {
                Ok(replica) => return replica,
                Err(output) => reported = output,
            }
        }
        panic!(
            "a stateful replica did not serve in {BOOT_ATTEMPTS} attempts; the last one \
             reported:\n{reported}"
        );
    }

    /// One boot attempt on a port of its own: the running replica, or what the
    /// child said instead of serving.
    async fn spawn(&self, attempt: usize, dsn: &str, recovery: bool) -> Result<Replica, String> {
        let bind = free_addr();
        let log = self
            .config
            .parent()
            .expect("the fixture config has a directory")
            // Per attempt, so a retry's diagnostics are its own.
            .join(format!("replica-{attempt}.log"));
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
        command.env(DSN_ENV, dsn);
        let child = command.spawn().expect("the axond binary runs");
        let mut replica = Replica {
            child,
            bind,
            log,
            breakglass: self.breakglass.clone(),
        };
        if replica.serving(recovery).await {
            Ok(replica)
        } else {
            Err(replica.output())
        }
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
    breakglass: String,
}

impl Replica {
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.bind)
    }

    /// An `/admin/v1` URL, spelt through the prefix the binary serves.
    pub fn admin_url(&self, path: &str) -> String {
        self.url(&format!("/admin/v1{path}"))
    }

    /// Everything the replica has reported so far, for a failure message.
    pub fn output(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// Whether *this* child is serving, rather than whether something answers on
    /// its port.
    ///
    /// Liveness is necessary and not sufficient. `/healthz` is unauthenticated,
    /// so a sibling replica that won the same port would answer it, and no wait
    /// settles which process did: a child still parsing its config is neither
    /// bound nor exited. The credential settles it. Each fixture's replica accepts
    /// only its own breakglass value, so a `200` from an authenticated admin read
    /// identifies the answerer as this child; a sibling answers `401`, and the
    /// boot is retried on a fresh port.
    ///
    /// An admin read rather than `/readyz`: a stateful replica serves
    /// administration while reporting itself unready for inference, so readiness
    /// is the very thing a scenario here asserts rather than a precondition it
    /// waits on.
    async fn serving(&mut self, recovery: bool) -> bool {
        let deadline = Instant::now() + Duration::from_secs(30);
        let client = reqwest::Client::new();
        while Instant::now() < deadline {
            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return false;
            }
            let path = if recovery { "/convergence" } else { "/state" };
            if let Ok(response) = self
                .breakglass(
                    client.get(self.admin_url(path)),
                    "fixture: identify the replica",
                )
                .send()
                .await
            {
                let status = response.status();
                if recovery && status == reqwest::StatusCode::OK {
                    let body = response.json::<serde_json::Value>().await;
                    if body
                        .ok()
                        .and_then(|body| body.get("active").cloned())
                        .is_some_and(|active| active.is_string())
                    {
                        return true;
                    }
                } else {
                    match status {
                        reqwest::StatusCode::OK => return true,
                        reqwest::StatusCode::SERVICE_UNAVAILABLE if !recovery => return true,
                        // Another process holds this port. This child will exit on its
                        // own failed bind; the caller takes a different port.
                        reqwest::StatusCode::UNAUTHORIZED => return false,
                        // Still starting: nothing is listening yet, or the admin
                        // surface is not mounted yet.
                        _ => {}
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    /// Present this replica's breakglass credential, attributed.
    ///
    /// Attribution is required rather than convenient: the surface refuses an
    /// unattributed breakglass request, and the audit row records who and why — so
    /// a helper that omitted it would be a helper for a `401`.
    pub fn breakglass(
        &self,
        request: reqwest::RequestBuilder,
        reason: &str,
    ) -> reqwest::RequestBuilder {
        request
            .bearer_auth(&self.breakglass)
            .header("x-axond-breakglass-operator", "integration-harness")
            .header("x-axond-breakglass-reason", reason)
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
