//! `axond check preflight`: everything a stateful replica would fail at boot,
//! decided before a replica exists.
//!
//! A boot failure in a rollout is expensive in a way a command-line failure is
//! not: the listener is already gone, the old replica is already terminating, and
//! the operator finds out from a crash loop. Every check here is one a boot
//! performs anyway — the config parses and validates, the file it came from is
//! not one anybody can rewrite, the references it makes resolve, the control-plane
//! database answers, and its schema is the one this build writes — run in the
//! order a boot would hit them and reported all at once rather than one per
//! restart.
//!
//! Strictly read-only. Nothing here creates, migrates, or writes: the control
//! plane is opened for maintenance with migration off, and the schema is read
//! inside a `READ ONLY` transaction. `axond migrate apply` is the mutation, and it
//! is a separate command an operator types on purpose.
//!
//! Stateless mode is not a degraded case of stateful mode: the database checks are
//! reported `skipped` because there is nothing to check, and a stateless install
//! passes preflight with no PostgreSQL anywhere.

use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use super::{
    CONTROL_PLANE, control_plane, control_plane_dsn_env, control_plane_error, dsn,
    open_control_plane,
};
use crate::backends::control_plane::schema::{self, SchemaStatus};
use crate::config::{
    BudgetBackend, Config, Mode, RateLimitBackend, RevocationBackend, UsageSinkKind,
};

/// One check's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The check applies and passed. The detail is what was established, not a
    /// restatement of the check's name.
    Passed(String),
    /// The check does not apply to this configuration, and why. A pass for exit
    /// purposes: a stateless install skipping the control-plane checks is correct
    /// rather than unverified.
    Skipped(String),
    /// The check applies and failed. A replica would fail to boot for this
    /// reason, so the detail is written as the thing to fix.
    Failed(String),
}

impl Outcome {
    pub fn is_ok(&self) -> bool {
        !matches!(self, Self::Failed(_))
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Passed(_) => "ok",
            Self::Skipped(_) => "skipped",
            Self::Failed(_) => "FAILED",
        }
    }

    fn detail(&self) -> &str {
        match self {
            Self::Passed(detail) | Self::Skipped(detail) | Self::Failed(detail) => detail,
        }
    }
}

/// A named check and what it found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub name: &'static str,
    pub outcome: Outcome,
}

impl fmt::Display for Check {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:<28} {:<8} {}",
            self.name,
            self.outcome.label(),
            self.outcome.detail()
        )
    }
}

/// Every check, in the order a boot would reach them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub checks: Vec<Check>,
}

impl Report {
    fn passed(&mut self, name: &'static str, detail: impl Into<String>) {
        self.push(name, Outcome::Passed(detail.into()));
    }

    fn skipped(&mut self, name: &'static str, detail: impl Into<String>) {
        self.push(name, Outcome::Skipped(detail.into()));
    }

    fn failed(&mut self, name: &'static str, detail: impl Into<String>) {
        self.push(name, Outcome::Failed(detail.into()));
    }

    fn push(&mut self, name: &'static str, outcome: Outcome) {
        self.checks.push(Check { name, outcome });
    }

    /// Whether a replica would boot against this configuration, as far as
    /// anything outside a replica can tell.
    pub fn is_ok(&self) -> bool {
        self.checks.iter().all(|check| check.outcome.is_ok())
    }

    pub fn failures(&self) -> impl Iterator<Item = &Check> {
        self.checks.iter().filter(|check| !check.outcome.is_ok())
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for check in &self.checks {
            writeln!(f, "{check}")?;
        }
        Ok(())
    }
}

/// Run every check against an already-loaded config.
///
/// The config is a parameter rather than a path because loading it *is* the first
/// check: a config that does not parse or does not validate has no preflight to
/// run, and the loader's own error is the better message. The CLI reports that as
/// [`super::OpsError::Config`] and never reaches here.
pub async fn run(config: &Config, config_path: &Path, env: &HashMap<String, String>) -> Report {
    let mut report = Report { checks: Vec::new() };
    report.passed(
        "config",
        format!(
            "`{}` parses and validates in {} mode",
            config_path.display(),
            match config.mode {
                Mode::Stateless => "stateless",
                Mode::Stateful => "stateful",
            }
        ),
    );
    check_file_ownership(&mut report, config_path);
    check_references(&mut report, config, env);
    check_control_plane(&mut report, config, env).await;
    report
}

/// The config file names every secret the process will read. A file another
/// account can rewrite is a file that can redirect the control plane, so
/// ownership and mode are a boot property rather than a lint.
#[cfg(unix)]
fn check_file_ownership(report: &mut Report, path: &Path) {
    use std::os::unix::fs::MetadataExt;

    const NAME: &str = "config ownership";
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) => {
            report.failed(
                NAME,
                format!("`{}` cannot be read: {error}", path.display()),
            );
            return;
        }
    };
    let mode = metadata.mode() & 0o777;
    let owner = metadata.uid();
    let mut problems = Vec::new();
    // Group- or world-writable: anyone in that set can point this process at a
    // different control plane, which is the whole configuration's integrity.
    if mode & 0o022 != 0 {
        problems.push(format!(
            "mode {mode:04o} is writable beyond its owner; use 0600 or 0640"
        ));
    }
    // Owned by neither this user nor root: the file's contents are not under the
    // control of the identity that is about to act on them. Only checked where
    // the process' own uid can be established without writing anything — this
    // command is read-only, so it does not create a probe file to find out.
    match process_uid() {
        Some(effective) if owner != effective && owner != 0 => problems.push(format!(
            "owner uid {owner} is neither this process (uid {effective}) nor root"
        )),
        _ => {}
    }
    if problems.is_empty() {
        report.passed(
            NAME,
            format!(
                "`{}` is mode {mode:04o} and owned by uid {owner}",
                path.display()
            ),
        );
    } else {
        report.failed(
            NAME,
            format!("`{}`: {}", path.display(), problems.join("; ")),
        );
    }
}

/// This process' own uid, where the platform exposes it without a syscall
/// crate: `/proc/self` is owned by the process it describes.
#[cfg(unix)]
fn process_uid() -> Option<u32> {
    use std::os::unix::fs::MetadataExt;

    std::fs::metadata("/proc/self")
        .ok()
        .map(|metadata| metadata.uid())
}

#[cfg(not(unix))]
fn check_file_ownership(report: &mut Report, path: &Path) {
    const NAME: &str = "config ownership";
    match std::fs::metadata(path) {
        Ok(_) => report.skipped(
            NAME,
            format!(
                "`{}` exists; ownership and mode are not checked on this platform",
                path.display()
            ),
        ),
        Err(error) => report.failed(
            NAME,
            format!("`{}` cannot be read: {error}", path.display()),
        ),
    }
}

/// Every reference the config makes, resolved against the environment and the
/// filesystem.
///
/// A reference is a name, not a value: the check is that something answers to the
/// name, and no resolved value is ever reported. This covers the stateful
/// bootstrap set (control plane, secret store KEK, breakglass credential) and the
/// opt-in stores, because an unset `dsn_env` fails a boot whichever section it is
/// in.
fn check_references(report: &mut Report, config: &Config, env: &HashMap<String, String>) {
    let mut references: Vec<(String, Reference)> = Vec::new();
    if let Some(control_plane) = config.control_plane.as_ref() {
        references.push((
            "[control_plane] dsn_env".to_owned(),
            Reference::Env(control_plane_dsn_env(control_plane)),
        ));
    }
    if let Some(secret_store) = config.secret_store.as_ref() {
        // The store's own DSN is optional: omitting it reuses the control plane's
        // reference, which is the single-database deployment.
        if let Some(dsn_env) = non_empty(secret_store.dsn_env.as_deref()) {
            references.push((
                "[secret_store] dsn_env".to_owned(),
                Reference::Env(dsn_env.to_owned()),
            ));
        }
        match secret_store.kek_reference() {
            Some(("kek_env", name)) => references.push((
                "[secret_store] kek_env".to_owned(),
                Reference::Env(name.to_owned()),
            )),
            Some((_, path)) => references.push((
                "[secret_store] kek_file".to_owned(),
                Reference::File(path.to_owned()),
            )),
            None => {}
        }
    }
    for (index, breakglass) in config.admin_breakglass.iter().enumerate() {
        let key = format!("[[admin_breakglass]] #{}", index + 1);
        if let Some(name) = non_empty(breakglass.env.as_deref()) {
            references.push((key, Reference::Env(name.to_owned())));
        } else if let Some(path) = non_empty(breakglass.file.as_deref()) {
            references.push((key, Reference::File(path.to_owned())));
        }
    }
    for (index, key) in config.gateway_key.iter().enumerate() {
        let label = format!("[[gateway_key]] #{}", index + 1);
        if let Some(name) = non_empty(key.env.as_deref()) {
            references.push((label, Reference::Env(name.to_owned())));
        } else if let Some(path) = non_empty(key.file.as_deref()) {
            references.push((label, Reference::File(path.to_owned())));
        }
    }
    for (index, sink) in config.usage_sink.iter().enumerate() {
        if sink.kind != UsageSinkKind::Postgres {
            continue;
        }
        if let Some(name) = non_empty(sink.dsn_env.as_deref()) {
            references.push((
                format!("[[usage_sink]] #{} dsn_env", index + 1),
                Reference::Env(name.to_owned()),
            ));
        }
    }
    // A DSN reference only has to resolve when the backend that reads it is
    // selected: an in-memory budget with a leftover `dsn_env` is not a boot
    // failure, so it must not be a preflight failure either.
    let admission = [
        (
            "[budget] dsn_env",
            matches!(
                config.budget.backend,
                BudgetBackend::Redis | BudgetBackend::Postgres
            ),
            config.budget.dsn_env.as_deref(),
        ),
        (
            "[rate_limit] dsn_env",
            matches!(config.rate_limit.backend, RateLimitBackend::Redis),
            config.rate_limit.dsn_env.as_deref(),
        ),
        (
            "[revocation] dsn_env",
            matches!(
                config.revocation.backend,
                RevocationBackend::Redis | RevocationBackend::Postgres
            ),
            config.revocation.dsn_env.as_deref(),
        ),
    ];
    for (key, selected, dsn_env) in admission {
        if let (true, Some(name)) = (selected, non_empty(dsn_env)) {
            references.push((key.to_owned(), Reference::Env(name.to_owned())));
        }
    }

    const NAME: &str = "bootstrap references";
    if references.is_empty() {
        report.skipped(NAME, "this configuration references no secrets");
        return;
    }
    let unsatisfied: Vec<String> = references
        .iter()
        .filter_map(|(key, reference)| {
            reference
                .unsatisfied(env)
                .map(|why| format!("{key}: {why}"))
        })
        .collect();
    if unsatisfied.is_empty() {
        report.passed(NAME, format!("{} reference(s) resolve", references.len()));
    } else {
        report.failed(NAME, unsatisfied.join("; "));
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// A secret's location: the name of an environment variable, or a path.
enum Reference {
    Env(String),
    File(String),
}

impl Reference {
    /// Why this reference would fail a boot, or `None` if it resolves. The
    /// *value* is never part of the answer.
    fn unsatisfied(&self, env: &HashMap<String, String>) -> Option<String> {
        match self {
            Self::Env(name) => {
                if name.is_empty() {
                    return Some("no environment variable is named".to_owned());
                }
                match env.get(name) {
                    Some(value) if !value.trim().is_empty() => None,
                    Some(_) => Some(format!("`{name}` is set but empty")),
                    None => Some(format!("`{name}` is unset")),
                }
            }
            Self::File(path) => match std::fs::metadata(path) {
                Ok(metadata) if metadata.is_file() => None,
                Ok(_) => Some(format!("`{path}` is not a file")),
                Err(error) => Some(format!("`{path}` cannot be read: {error}")),
            },
        }
    }
}

/// Connectivity and schema compatibility, as two separately reported checks.
///
/// Separate because they fail for different reasons and are fixed by different
/// people: an unreachable database is network or credentials, while an
/// incompatible schema is a release-ordering decision.
async fn check_control_plane(report: &mut Report, config: &Config, env: &HashMap<String, String>) {
    const CONNECTIVITY: &str = "control-plane database";
    const COMPATIBILITY: &str = "control-plane schema";
    let Some(control_plane) = control_plane(config) else {
        let reason = "stateless mode owns no durable resources, so no control plane is read";
        report.skipped(CONNECTIVITY, reason);
        report.skipped(COMPATIBILITY, reason);
        return;
    };
    let dsn_env = control_plane_dsn_env(control_plane);
    // Resolved separately from the connection so an unset variable is reported as
    // the reference problem it is, rather than as a failure to connect.
    if let Err(error) = dsn(env, CONTROL_PLANE, &dsn_env) {
        report.failed(CONNECTIVITY, error.to_string());
        report.skipped(
            COMPATIBILITY,
            format!("not checked: `{dsn_env}` did not resolve to a connection string"),
        );
        return;
    }
    let store = match open_control_plane(control_plane, env).await {
        Ok(store) => store,
        Err(error) => {
            report.failed(CONNECTIVITY, error.to_string());
            report.skipped(
                COMPATIBILITY,
                "not checked: the control-plane database was not reached",
            );
            return;
        }
    };
    report.passed(
        CONNECTIVITY,
        format!(
            "connected using `${dsn_env}`; server meets the PostgreSQL {} minimum",
            schema::MINIMUM_SERVER_VERSION_NUM / 10_000
        ),
    );
    match store.schema_status().await {
        Ok(status) if accepts(&status) => report.passed(COMPATIBILITY, status.to_string()),
        // Migratable is not a pass: a replica booting against it would either
        // refuse or migrate under a rollout, and both are the operator's call.
        Ok(status) if status.is_migratable() => report.failed(
            COMPATIBILITY,
            format!("{status} (run `axond migrate apply` first)"),
        ),
        Ok(status) => report.failed(COMPATIBILITY, status.to_string()),
        Err(error) => report.failed(COMPATIBILITY, control_plane_error(error).to_string()),
    }
}

/// Whether preflight treats a schema as ready to serve.
///
/// Only a current schema is. Preflight is a boot rehearsal, and a replica booted
/// against a migratable schema either refuses or migrates mid-rollout — both of
/// which are the operator's decision to make with `axond migrate apply`, before
/// any replica starts.
fn accepts(status: &SchemaStatus) -> bool {
    status.is_current()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::desired_state::Checksum;
    use crate::ops::tests::{stateful_toml, stateless_toml};

    /// Fixtures use the temp directory directly, the way the rest of this crate's
    /// file-backed tests do: no dev-dependency is added for a config file.
    fn write(name: &str, contents: &str) -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "axond-preflight-{}-{}-{name}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::write(&path, contents).expect("write fixture");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .expect("tighten fixture");
        }
        path
    }

    #[tokio::test]
    async fn a_stateless_install_passes_with_no_postgres_anywhere() {
        let path = write("axond.toml", stateless_toml());
        let config = Config::from_toml_str(stateless_toml()).expect("valid stateless config");
        let env = HashMap::from([("GW_KEY".to_owned(), "secret".to_owned())]);
        let report = run(&config, &path, &env).await;
        assert!(report.is_ok(), "{report}");
        let skipped: Vec<&str> = report
            .checks
            .iter()
            .filter(|check| matches!(check.outcome, Outcome::Skipped(_)))
            .map(|check| check.name)
            .collect();
        assert!(
            skipped.contains(&"control-plane database")
                && skipped.contains(&"control-plane schema"),
            "stateless mode must not require a control plane: {report}"
        );
    }

    #[tokio::test]
    async fn an_unset_control_plane_reference_fails_without_connecting() {
        let path = write("axond.toml", stateful_toml());
        let config = Config::from_toml_str(stateful_toml()).expect("valid stateful config");
        // Every reference unsatisfied, and no database reachable at any address:
        // the report is still produced, and every failure names a variable.
        let report = run(&config, &path, &HashMap::new()).await;
        assert!(!report.is_ok(), "{report}");
        let rendered = report.to_string();
        assert!(rendered.contains("GW_CONTROL_PLANE_DSN"), "{rendered}");
        assert!(rendered.contains("GW_KEK"), "{rendered}");
        assert!(rendered.contains("GW_BREAKGLASS"), "{rendered}");
        let schema_check = report
            .checks
            .iter()
            .find(|check| check.name == "control-plane schema")
            .expect("the schema check is always reported");
        assert!(
            matches!(schema_check.outcome, Outcome::Skipped(_)),
            "an unresolvable reference cannot be a schema verdict: {schema_check}"
        );
    }

    #[tokio::test]
    async fn a_reference_that_resolves_is_reported_without_its_value() {
        let path = write("axond.toml", stateful_toml());
        let config = Config::from_toml_str(stateful_toml()).expect("valid stateful config");
        let env = HashMap::from([
            (
                "GW_CONTROL_PLANE_DSN".to_owned(),
                // Port 1 is not a PostgreSQL server: the connectivity check has to
                // fail, which is what makes this a deterministic missing-database
                // case rather than one that depends on a local server.
                "postgres://axond:hunter2@127.0.0.1:1/axond?connect_timeout=1".to_owned(),
            ),
            ("GW_KEK".to_owned(), "kek".to_owned()),
            ("GW_BREAKGLASS".to_owned(), "breakglass".to_owned()),
        ]);
        let report = run(&config, &path, &env).await;
        let rendered = report.to_string();
        assert!(!report.is_ok(), "an unreachable database is a failure");
        assert!(
            !rendered.contains("hunter2") && !rendered.contains("postgres://"),
            "a DSN must never be echoed: {rendered}"
        );
        let references = report
            .checks
            .iter()
            .find(|check| check.name == "bootstrap references")
            .expect("references are checked");
        assert!(
            matches!(references.outcome, Outcome::Passed(_)),
            "every reference is satisfied here: {references}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_world_writable_config_fails_ownership() {
        use std::os::unix::fs::PermissionsExt;

        let path = write("axond.toml", stateless_toml());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666))
            .expect("loosen fixture");
        let config = Config::from_toml_str(stateless_toml()).expect("valid stateless config");
        let env = HashMap::from([("GW_KEY".to_owned(), "secret".to_owned())]);
        let report = run(&config, &path, &env).await;
        assert!(!report.is_ok(), "{report}");
        let ownership = report
            .checks
            .iter()
            .find(|check| check.name == "config ownership")
            .expect("ownership is checked");
        assert!(
            ownership
                .outcome
                .detail()
                .contains("writable beyond its owner"),
            "{ownership}"
        );

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640))
            .expect("tighten fixture");
        let report = run(&config, &path, &env).await;
        assert!(report.is_ok(), "0640 is acceptable: {report}");
    }

    /// Preflight is a boot rehearsal, so only a schema a replica could serve
    /// as-is passes: a migratable one still needs an operator to migrate it.
    #[test]
    fn only_a_current_schema_passes_preflight() {
        assert!(accepts(&SchemaStatus::Current {
            version: schema::required_version()
        }));
        for rejected in [
            SchemaStatus::Absent,
            SchemaStatus::Behind {
                applied: 0,
                required: 1,
            },
            SchemaStatus::Ahead {
                applied: 9,
                required: 1,
            },
            SchemaStatus::Drifted {
                version: 1,
                expected: schema::MIGRATIONS[0].checksum(),
                found: Checksum::of(b"edited"),
            },
            SchemaStatus::Incomplete {
                applied: 3,
                missing: vec![2],
            },
            SchemaStatus::Renamed {
                version: 1,
                expected: schema::MIGRATIONS[0].name,
                found: "renamed".to_owned(),
            },
            SchemaStatus::Malformed {
                message: "not this ledger".to_owned(),
            },
        ] {
            assert!(!accepts(&rejected), "{rejected}");
        }
    }

    #[test]
    fn a_skipped_check_is_not_a_failure_and_a_failure_is_listed() {
        let report = Report {
            checks: vec![
                Check {
                    name: "config",
                    outcome: Outcome::Passed("fine".to_owned()),
                },
                Check {
                    name: "control-plane schema",
                    outcome: Outcome::Skipped("stateless".to_owned()),
                },
            ],
        };
        assert!(report.is_ok());
        assert_eq!(report.failures().count(), 0);

        let mut failing = report.clone();
        failing.failed("control-plane database", "connection refused");
        assert!(!failing.is_ok());
        assert_eq!(
            failing
                .failures()
                .map(|check| check.name)
                .collect::<Vec<_>>(),
            vec!["control-plane database"]
        );
    }

    #[tokio::test]
    async fn a_missing_config_file_fails_ownership_rather_than_panicking() {
        let config = Config::from_toml_str(stateless_toml()).expect("valid stateless config");
        let env = HashMap::from([("GW_KEY".to_owned(), "secret".to_owned())]);
        let report = run(&config, Path::new("/nonexistent/axond.toml"), &env).await;
        assert!(!report.is_ok(), "{report}");
    }
}
