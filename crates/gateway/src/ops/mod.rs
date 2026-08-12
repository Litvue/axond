//! Operator commands that run *before* replicas do.
//!
//! Two commands live here, and both exist because a stateful installation has a
//! step a rolling restart cannot perform: someone has to decide that the
//! database this build is about to serve from is the database this build wrote.
//!
//! - [`preflight`] answers "would a replica boot against this?" without booting
//!   one. It checks the config file's ownership and mode, that the bootstrap
//!   *references* the config makes are satisfied by the environment, that the
//!   control-plane database is reachable, and what its schema is relative to the
//!   version this build requires.
//! - [`migrate`] answers "what schema does this database have, and what would
//!   this build apply to it?", and applies it on request. Forward-only: a journal
//!   a newer build owns, one whose applied migration was edited in place, and one
//!   whose recorded history has a hole in it are reported and refused rather than
//!   written over.
//!
//! Three properties are the point, and each is enforced rather than intended:
//!
//! 1. **`preflight` and `migrate status` cannot change anything.** They open the
//!    control plane through [`PostgresControlPlane::connect_for_maintenance`],
//!    which does not prepare a schema, with `migrate` forced off, and they read
//!    the ledger inside a `READ ONLY` transaction. A status command an operator
//!    runs against production is a thing that *cannot* write to production.
//! 2. **`migrate apply` is the only mutation**, it is explicit, and it is
//!    idempotent: it takes the journal's advisory lock, re-reads the ledger under
//!    it, applies only the versions above the recorded prefix, and commits once.
//!    Running it twice, or from two hosts at once, is not two migrations.
//! 3. **Nothing here serves.** No snapshot is compiled, no request path reads the
//!    control plane, and the existing refusal to serve statefully is untouched:
//!    these commands are what an operator runs *around* that refusal, and lifting
//!    it is later wiring.
//!
//! Stateless deployments are unaffected: `mode = "stateless"` has no control
//! plane, so [`preflight`] reports the database checks as skipped and
//! [`migrate`] has nothing to do. Neither command requires PostgreSQL to exist.
//!
//! Scope note: the control-plane journal is the only store with a migration
//! ledger, so it is the only store these commands migrate. The usage, budget,
//! and revocation stores ship DDL without a ledger — no recorded version, no
//! checksum — so orchestrating them here would mean inventing bookkeeping for
//! databases adopters already own, and inventing it silently. `preflight` still
//! resolves their references (an unset `dsn_env` is a boot failure whichever
//! store it belongs to), and giving them ledgers of their own is follow-up work.

pub mod migrate;
pub mod preflight;

use std::collections::HashMap;

use crate::backends::control_plane::ControlPlaneError;
use crate::backends::control_plane::postgres::{ControlPlaneSettings, PostgresControlPlane};
use crate::config::{Config, Mode};

/// Why an operator command could not do what was asked.
///
/// Typed rather than a string, because the three categories need different
/// actions: a `Config` mistake is fixed in a file, an `Unreachable` database is
/// worth retrying once the network or credentials are fixed, and a `Refused`
/// schema needs a human decision and will refuse identically forever.
///
/// No variant can carry a DSN. Every message names the *reference* — the
/// environment variable, the store — because a connection string carries a
/// password and these messages end up in shells, logs, and issue reports.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OpsError {
    /// The config file is missing, unparsable, invalid, or not owned the way a
    /// file holding secret references must be.
    #[error("{0}")]
    Config(String),
    /// A `dsn_env` reference the config makes is not satisfied by the process
    /// environment.
    #[error(
        "{target}: `{dsn_env}` is unset or empty in the environment; export it before running \
         this command"
    )]
    MissingDsn { target: String, dsn_env: String },
    /// The database could not be reached, or refused the connection.
    #[error("{target}: {message}")]
    Unreachable { target: String, message: String },
    /// The database was reached and this build must not write to it.
    #[error("{target}: {message}")]
    Refused { target: String, message: String },
}

impl OpsError {
    /// Whether an operator can fix this by retrying rather than by deciding
    /// something. Only an outage qualifies.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unreachable { .. })
    }
}

/// Load and validate the config an operator command was pointed at.
///
/// Loading is the first check every command performs, and its failure is a
/// configuration failure rather than a database one: the loader's own message
/// already names the file and the invalid key, so it is carried through intact.
pub fn load(path: &str) -> Result<Config, OpsError> {
    Config::load(path)
        .map_err(|error| OpsError::Config(format!("failed to load config from `{path}`: {error}")))
}

/// How a report names the control-plane database in output.
pub(crate) const CONTROL_PLANE: &str = "control plane";

/// The `[control_plane]` section a command should act on, if any.
///
/// `None` in stateless mode even if a section were somehow present: stateless
/// mode does not read one, so a command that connected anyway would make
/// PostgreSQL a stateless-mode dependency. Config validation already rejects the
/// section in that mode; this is the second half of the same rule, stated where
/// the connection would otherwise be made.
pub(crate) fn control_plane(config: &Config) -> Option<&crate::config::ControlPlane> {
    match config.mode {
        Mode::Stateless => None,
        Mode::Stateful => config.control_plane.as_ref(),
    }
}

/// The env var name a `[control_plane]` section references.
pub(crate) fn control_plane_dsn_env(control_plane: &crate::config::ControlPlane) -> String {
    control_plane
        .dsn_env
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_owned()
}

/// Resolve a `dsn_env` reference, or say which variable is missing.
///
/// Returns the name of the variable rather than its value in the error: an
/// operator needs to know *which* reference is unsatisfied, and the value is a
/// password.
pub(crate) fn dsn<'a>(
    env: &'a HashMap<String, String>,
    target: &str,
    dsn_env: &str,
) -> Result<&'a str, OpsError> {
    env.get(dsn_env)
        .map(String::as_str)
        .filter(|dsn| !dsn.trim().is_empty())
        .ok_or_else(|| OpsError::MissingDsn {
            target: target.to_owned(),
            dsn_env: dsn_env.to_owned(),
        })
}

/// Open the control plane for maintenance: connected, server-version checked,
/// and with no opinion yet about the schema.
///
/// `migrate` is forced off regardless of what the config allows boot to do, so
/// opening a connection can never be the thing that migrates a database. The
/// commands that mutate say so in their own name.
pub(crate) async fn open_control_plane(
    control_plane: &crate::config::ControlPlane,
    env: &HashMap<String, String>,
) -> Result<PostgresControlPlane, OpsError> {
    let dsn_env = control_plane_dsn_env(control_plane);
    let dsn = dsn(env, CONTROL_PLANE, &dsn_env)?;
    PostgresControlPlane::connect_for_maintenance(
        dsn,
        ControlPlaneSettings::for_maintenance(control_plane),
    )
    .await
    .map_err(control_plane_error)
}

/// Translate a control-plane failure into the operator-facing category.
///
/// `Unavailable` is the retryable one; everything else — a denial, a conflict,
/// unreadable storage — is a decision an operator has to make, so it is a
/// refusal here rather than something a wrapper script should loop on.
pub(crate) fn control_plane_error(error: ControlPlaneError) -> OpsError {
    match error {
        ControlPlaneError::Unavailable { message, .. } => OpsError::Unreachable {
            target: CONTROL_PLANE.to_owned(),
            message,
        },
        other => OpsError::Refused {
            target: CONTROL_PLANE.to_owned(),
            message: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn stateful_toml() -> &'static str {
        "mode = \"stateful\"\n\
         [control_plane]\n\
         dsn_env = \"GW_CONTROL_PLANE_DSN\"\n\
         [secret_store]\n\
         kek_env = \"GW_KEK\"\n\
         [[admin_breakglass]]\n\
         env = \"GW_BREAKGLASS\"\n"
    }

    pub(super) fn stateless_toml() -> &'static str {
        "[[gateway_key]]\nenv = \"GW_KEY\"\nnamespace = \"platform\"\n\
         [[namespace]]\nid = \"platform\"\ndefault = true\n"
    }

    #[test]
    fn stateless_mode_has_no_control_plane_to_act_on() {
        let config = Config::from_toml_str(stateless_toml()).expect("valid stateless config");
        assert!(
            control_plane(&config).is_none(),
            "a stateless install must not acquire a PostgreSQL dependency from an ops command"
        );
    }

    #[test]
    fn stateful_mode_acts_on_the_configured_reference() {
        let config = Config::from_toml_str(stateful_toml()).expect("valid stateful config");
        let control_plane = control_plane(&config).expect("stateful mode requires a control plane");
        assert_eq!(control_plane_dsn_env(control_plane), "GW_CONTROL_PLANE_DSN");
    }

    #[test]
    fn an_unsatisfied_reference_names_the_variable_and_never_a_value() {
        let mut env = HashMap::new();
        env.insert("GW_CONTROL_PLANE_DSN".to_owned(), "   ".to_owned());
        let error = dsn(&env, CONTROL_PLANE, "GW_CONTROL_PLANE_DSN")
            .expect_err("whitespace is not a connection string");
        assert_eq!(
            error,
            OpsError::MissingDsn {
                target: CONTROL_PLANE.to_owned(),
                dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
            }
        );
        let rendered = error.to_string();
        assert!(rendered.contains("GW_CONTROL_PLANE_DSN"), "{rendered}");
        assert!(!error.is_retryable(), "exporting a variable is not a retry");

        env.insert(
            "GW_CONTROL_PLANE_DSN".to_owned(),
            "postgres://axond:hunter2@db/axond".to_owned(),
        );
        assert_eq!(
            dsn(&env, CONTROL_PLANE, "GW_CONTROL_PLANE_DSN").expect("resolved"),
            "postgres://axond:hunter2@db/axond"
        );
    }

    #[test]
    fn a_missing_variable_is_reported_without_connecting() {
        let error = dsn(&HashMap::new(), CONTROL_PLANE, "GW_CONTROL_PLANE_DSN")
            .expect_err("an unset variable cannot be resolved");
        assert!(matches!(error, OpsError::MissingDsn { .. }));
    }

    #[test]
    fn an_outage_is_retryable_and_a_denial_is_not() {
        let outage = control_plane_error(ControlPlaneError::Unavailable {
            backend: "postgres",
            message: "connection refused".to_owned(),
        });
        assert!(outage.is_retryable(), "{outage}");
        let denial = control_plane_error(ControlPlaneError::Denied {
            backend: "postgres",
            message: "a newer gateway owns this database".to_owned(),
        });
        assert!(!denial.is_retryable(), "{denial}");
        assert!(denial.to_string().contains("newer gateway"));
    }
}
