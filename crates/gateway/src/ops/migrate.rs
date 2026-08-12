//! `axond migrate status` and `axond migrate apply`: the control-plane journal's
//! schema, reported and moved forward.
//!
//! One database, one ledger, one direction. The journal records every migration
//! it has applied — version, shipped file name, and a checksum of that file's
//! text — so "what does this database contain?" is answered from the database
//! rather than guessed from the binary's version. [`status`] reads that ledger
//! without writing to it; [`apply`] is the only thing here that writes, and what
//! it writes is the versions above the recorded prefix and nothing else.
//!
//! Forward-only is not a convention here, it is the absence of a downgrade path:
//! there is no `revert`, applied files are immutable, and a database whose ledger
//! this build cannot account for is refused rather than repaired. The refusals are
//! deliberately specific — a future version, an edited file, a hole in the
//! history, a renamed migration, a ledger that is not this ledger — because each
//! one implies a different thing for the operator to do.

use std::collections::HashMap;
use std::fmt;

use super::{
    OpsError, control_plane, control_plane_dsn_env, control_plane_error, open_control_plane,
};
use crate::backends::control_plane::schema::{self, SchemaStatus};
use crate::config::Config;

/// What one migration target's schema is, or what was done to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// The schema is the one this build requires. `apply` leaves it alone.
    Current { version: i32 },
    /// Migrations are missing and this build can apply them. What `status`
    /// reports before an upgrade, and what `apply` acts on.
    Pending { pending: Vec<(i32, &'static str)> },
    /// `apply` applied these. Empty is impossible: an `apply` that had nothing
    /// to do reports [`State::Current`], so re-running it is visibly a no-op
    /// rather than an indistinguishable success.
    Applied { applied: Vec<(i32, &'static str)> },
    /// This build must not write to this database, and why.
    Refused { reason: String },
}

impl State {
    /// The status a schema read implies, before anything has been applied.
    fn from_status(status: &SchemaStatus) -> Self {
        match status {
            SchemaStatus::Current { version } => Self::Current { version: *version },
            SchemaStatus::Absent | SchemaStatus::Behind { .. } => Self::Pending {
                pending: named(&schema::pending(status)),
            },
            // Everything else is a decision an operator has to make. The status'
            // own message is the explanation: it is written for exactly this.
            refused => Self::Refused {
                reason: refused.to_string(),
            },
        }
    }

    /// Whether this state is a success for exit-code purposes.
    pub fn is_ok(&self) -> bool {
        !matches!(self, Self::Refused { .. })
    }

    /// Whether an operator still has an `apply` to run. `status` exits non-zero
    /// on a pending schema so a deployment gate can be `axond migrate status`.
    pub fn is_settled(&self) -> bool {
        !matches!(self, Self::Pending { .. } | Self::Refused { .. })
    }
}

/// Pair each version with the file it ships as, so a report names something an
/// operator can find in `ops/postgres/`.
fn named(versions: &[i32]) -> Vec<(i32, &'static str)> {
    versions
        .iter()
        .filter_map(|version| {
            schema::MIGRATIONS
                .iter()
                .find(|migration| migration.version == *version)
                .map(|migration| (migration.version, migration.name))
        })
        .collect()
}

/// What a command found, in the form the CLI prints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Report {
    /// Stateless mode: no control plane, so no schema, so nothing to migrate.
    /// A success — a stateless install has no migration step to forget.
    NoControlPlane,
    ControlPlane {
        /// The env var the config references. The name, never the DSN.
        dsn_env: String,
        state: State,
    },
}

impl Report {
    pub fn state(&self) -> Option<&State> {
        match self {
            Self::NoControlPlane => None,
            Self::ControlPlane { state, .. } => Some(state),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.state().is_none_or(State::is_ok)
    }

    pub fn is_settled(&self) -> bool {
        self.state().is_none_or(State::is_settled)
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self::ControlPlane { dsn_env, state } = self else {
            return write!(
                f,
                "stateless mode: no control plane is configured, so there is no schema to migrate"
            );
        };
        write!(f, "control plane (${dsn_env}): ")?;
        match state {
            State::Current { version } => write!(f, "schema v{version} is current"),
            State::Pending { pending } => {
                write!(
                    f,
                    "{} migration(s) pending: {}",
                    pending.len(),
                    list(pending)
                )
            }
            State::Applied { applied } => {
                write!(
                    f,
                    "applied {} migration(s): {}",
                    applied.len(),
                    list(applied)
                )
            }
            State::Refused { reason } => write!(f, "refused: {reason}"),
        }
    }
}

fn list(migrations: &[(i32, &'static str)]) -> String {
    migrations
        .iter()
        .map(|(version, name)| format!("v{version} {name}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Report the control-plane schema without touching it.
///
/// Read-only twice over: the store is opened for maintenance, so nothing prepares
/// a schema, and the ledger is read inside a `READ ONLY` transaction, so the
/// server itself would reject a write. A refusal is a *reported state* rather than
/// an error, because "your schema is one a newer build owns" is the answer to the
/// question rather than a failure to answer it. The CLI still exits non-zero.
pub async fn status(config: &Config, env: &HashMap<String, String>) -> Result<Report, OpsError> {
    let Some(control_plane) = control_plane(config) else {
        return Ok(Report::NoControlPlane);
    };
    let dsn_env = control_plane_dsn_env(control_plane);
    let store = open_control_plane(control_plane, env).await?;
    let status = store.schema_status().await.map_err(control_plane_error)?;
    Ok(Report::ControlPlane {
        dsn_env,
        state: State::from_status(&status),
    })
}

/// Apply every migration the control-plane journal is missing.
///
/// Idempotent, and safe to run while replicas are starting: the read and the
/// writes are one transaction under the journal's advisory lock, so a second
/// invocation — or a second host — finds the schema current and applies nothing.
/// Forward-only: a database this build cannot account for is refused with
/// [`OpsError::Refused`] rather than written over.
pub async fn apply(config: &Config, env: &HashMap<String, String>) -> Result<Report, OpsError> {
    let Some(control_plane) = control_plane(config) else {
        return Ok(Report::NoControlPlane);
    };
    let dsn_env = control_plane_dsn_env(control_plane);
    let store = open_control_plane(control_plane, env).await?;
    let applied = store
        .apply_migrations()
        .await
        .map_err(control_plane_error)?;
    let state = if applied.is_empty() {
        State::Current {
            version: schema::required_version(),
        }
    } else {
        State::Applied {
            applied: named(&applied),
        }
    };
    Ok(Report::ControlPlane { dsn_env, state })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    use crate::desired_state::Checksum;
    use crate::ops::tests::{stateful_toml, stateless_toml};

    fn state(status: &SchemaStatus) -> State {
        State::from_status(status)
    }

    #[tokio::test]
    async fn a_stateless_install_has_nothing_to_migrate_and_needs_no_postgres() {
        let config = Config::from_toml_str(stateless_toml()).expect("valid stateless config");
        // No DSN in the environment, and no database anywhere: both commands
        // still succeed, because a stateless install has no schema.
        let env = HashMap::new();
        for report in [
            status(&config, &env).await.expect("status"),
            apply(&config, &env).await.expect("apply"),
        ] {
            assert_eq!(report, Report::NoControlPlane);
            assert!(report.is_ok() && report.is_settled(), "{report}");
            assert!(report.to_string().contains("no control plane"), "{report}");
        }
    }

    /// The whole point of the missing-Postgres case: it is decided before a
    /// socket is opened, so it is deterministic without a database.
    #[tokio::test]
    async fn an_unset_reference_fails_before_connecting_and_names_the_variable() {
        let config = Config::from_toml_str(stateful_toml()).expect("valid stateful config");
        let env = HashMap::new();
        for error in [
            status(&config, &env)
                .await
                .expect_err("no DSN to connect with"),
            apply(&config, &env)
                .await
                .expect_err("no DSN to connect with"),
        ] {
            assert_eq!(
                error,
                OpsError::MissingDsn {
                    target: crate::ops::CONTROL_PLANE.to_owned(),
                    dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
                }
            );
            assert!(!error.is_retryable(), "exporting a variable is not a retry");
        }
    }

    #[test]
    fn an_absent_schema_is_pending_every_shipped_migration() {
        let State::Pending { pending } = state(&SchemaStatus::Absent) else {
            panic!("a fresh install has migrations to apply");
        };
        assert_eq!(
            pending,
            schema::MIGRATIONS
                .iter()
                .map(|migration| (migration.version, migration.name))
                .collect::<Vec<_>>()
        );
        let state = State::Pending { pending };
        assert!(state.is_ok(), "pending is not a failure to report");
        assert!(
            !state.is_settled(),
            "a deployment gate must not pass while a migration is outstanding"
        );
    }

    #[test]
    fn an_already_migrated_schema_is_current_and_has_nothing_pending() {
        let status = SchemaStatus::Current {
            version: schema::required_version(),
        };
        let state = state(&status);
        assert_eq!(
            state,
            State::Current {
                version: schema::required_version()
            }
        );
        assert!(state.is_ok() && state.is_settled());
        assert!(schema::pending(&status).is_empty());
    }

    #[test]
    fn a_future_schema_is_refused_and_says_a_newer_build_owns_it() {
        let state = state(&SchemaStatus::Ahead {
            applied: 99,
            required: schema::required_version(),
        });
        let State::Refused { reason } = &state else {
            panic!("a schema a newer build wrote is not one this build may migrate: {state:?}");
        };
        assert!(reason.contains("newer gateway"), "{reason}");
        assert!(!state.is_ok() && !state.is_settled());
    }

    #[test]
    fn drift_is_refused_and_names_the_version_that_was_edited() {
        let state = state(&SchemaStatus::Drifted {
            version: 1,
            expected: schema::MIGRATIONS[0].checksum(),
            found: Checksum::of(b"edited in place"),
        });
        let State::Refused { reason } = &state else {
            panic!("an edited applied migration is not migratable: {state:?}");
        };
        assert!(reason.contains("v1"), "{reason}");
        assert!(reason.contains("edited in place"), "{reason}");
    }

    #[test]
    fn a_hole_in_the_history_and_a_renamed_migration_are_refused_separately() {
        let incomplete = state(&SchemaStatus::Incomplete {
            applied: 3,
            missing: vec![2],
        });
        let State::Refused { reason } = &incomplete else {
            panic!("an incomplete prefix is not a history this build can extend");
        };
        assert!(reason.contains("missing v2"), "{reason}");

        let renamed = state(&SchemaStatus::Renamed {
            version: 1,
            expected: schema::MIGRATIONS[0].name,
            found: "control_plane_0001_initial_patched".to_owned(),
        });
        let State::Refused { reason } = &renamed else {
            panic!("a renamed migration is not the one this build ships");
        };
        assert!(
            reason.contains("control_plane_0001_initial_patched"),
            "{reason}"
        );
        assert_ne!(incomplete, renamed, "the two refusals are distinguishable");
    }

    #[test]
    fn a_ledger_this_build_did_not_write_is_refused_rather_than_migrated() {
        let state = state(&SchemaStatus::Malformed {
            message: "column `checksum` does not exist".to_owned(),
        });
        assert!(!state.is_ok(), "{state:?}");
        let State::Refused { reason } = &state else {
            panic!("a foreign ledger is a refusal");
        };
        assert!(reason.contains("checksum"), "{reason}");
    }

    /// Output is what an operator pastes into an issue, so it names the
    /// *reference* and never the connection string behind it.
    #[test]
    fn reports_print_the_reference_and_never_a_dsn() {
        let reports = [
            Report::NoControlPlane,
            Report::ControlPlane {
                dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
                state: State::Pending {
                    pending: named(&[1]),
                },
            },
            Report::ControlPlane {
                dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
                state: State::Applied {
                    applied: named(&[1]),
                },
            },
            Report::ControlPlane {
                dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
                state: State::Current { version: 1 },
            },
        ];
        for report in reports {
            let rendered = report.to_string();
            assert!(!rendered.contains("postgres://"), "{rendered}");
            assert!(!rendered.contains("hunter2"), "{rendered}");
        }
    }

    /// A dedicated schema in the test database, with the config and environment
    /// an operator command would be given.
    ///
    /// Each test owns a schema, so the ledger's fixed table name does not make
    /// every test one test. `None` when no Postgres is configured, which is what
    /// keeps this suite runnable without a database — `AXOND_TEST_REQUIRE_SERVICES`
    /// turns that into a panic for CI.
    async fn fixture() -> Option<Fixture> {
        let dsn = crate::test_services::postgres_dsn()?;
        let schema = format!(
            "cp_ops_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let client = client(&dsn).await;
        client
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("create the test schema");
        let config = Config::from_toml_str(&format!(
            "mode = \"stateful\"\n\
             [control_plane]\n\
             dsn_env = \"GW_CONTROL_PLANE_DSN\"\n\
             schema = \"{schema}\"\n\
             [secret_store]\n\
             kek_env = \"GW_KEK\"\n\
             [[admin_breakglass]]\n\
             env = \"GW_BREAKGLASS\"\n"
        ))
        .expect("valid stateful config");
        let env = HashMap::from([("GW_CONTROL_PLANE_DSN".to_owned(), dsn.clone())]);
        Some(Fixture {
            config,
            env,
            schema,
            dsn,
        })
    }

    struct Fixture {
        config: Config,
        env: HashMap<String, String>,
        schema: String,
        dsn: String,
    }

    impl Fixture {
        /// A connection of the test's own, so what the commands did is observed
        /// from outside them.
        async fn observe(&self) -> tokio_postgres::Client {
            let client = client(&self.dsn).await;
            client
                .batch_execute(&format!("SET search_path TO {}", self.schema))
                .await
                .expect("set the test search path");
            client
        }

        async fn ledger_exists(&self) -> bool {
            self.observe()
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

        async fn ledger(&self) -> Vec<(i32, String, String)> {
            self.observe()
                .await
                .query(
                    "SELECT version, name, checksum FROM axond_cp_schema_migration ORDER BY \
                     version",
                    &[],
                )
                .await
                .expect("read the ledger")
                .iter()
                .map(|row| (row.get(0), row.get(1), row.get(2)))
                .collect()
        }
    }

    async fn client(dsn: &str) -> tokio_postgres::Client {
        let (client, connection) = tokio_postgres::Config::from_str(dsn)
            .expect("test dsn")
            .connect(crate::usage::tls_connector())
            .await
            .expect("connect to the test database");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// The read-only guarantee, observed rather than asserted: a `status` against
    /// a database with no journal must leave it with no journal. A command that
    /// created its bookkeeping table "just to look" would be a command that
    /// changed production.
    #[tokio::test]
    async fn status_reports_a_fresh_database_without_creating_anything_in_it() {
        let Some(fixture) = fixture().await else {
            return;
        };
        let report = status(&fixture.config, &fixture.env)
            .await
            .expect("a reachable database has a status");
        assert!(
            matches!(report.state(), Some(State::Pending { .. })),
            "{report}"
        );
        assert!(!report.is_settled(), "a fresh install has an apply to run");
        assert!(
            !fixture.ledger_exists().await,
            "`migrate status` must not create the ledger it reads"
        );
    }

    /// Idempotence, and the reason `apply` distinguishes `Applied` from `Current`:
    /// the second run is visibly a no-op rather than an indistinguishable success.
    #[tokio::test]
    async fn a_second_apply_is_current_rather_than_a_second_migration() {
        let Some(fixture) = fixture().await else {
            return;
        };
        let first = apply(&fixture.config, &fixture.env).await.expect("apply");
        assert_eq!(
            first.state(),
            Some(&State::Applied {
                applied: named(
                    &schema::MIGRATIONS
                        .iter()
                        .map(|m| m.version)
                        .collect::<Vec<_>>()
                ),
            }),
            "{first}"
        );
        let ledger = fixture.ledger().await;
        assert_eq!(ledger.len(), schema::MIGRATIONS.len());

        let second = apply(&fixture.config, &fixture.env)
            .await
            .expect("a second apply is a no-op, not a failure");
        assert_eq!(
            second.state(),
            Some(&State::Current {
                version: schema::required_version()
            }),
            "{second}"
        );
        assert_eq!(
            fixture.ledger().await,
            ledger,
            "a repeated apply must not record a migration twice"
        );

        let status = status(&fixture.config, &fixture.env).await.expect("status");
        assert!(status.is_ok() && status.is_settled(), "{status}");
    }

    /// Idempotence as *not executing the SQL again*, rather than as an unchanged
    /// ledger. The shipped v1 file is written with `IF NOT EXISTS` throughout, so
    /// a re-run leaves the same ledger and the same tables and no assertion on
    /// either can tell the difference — while the first `ALTER TABLE` or backfill
    /// to ship would corrupt a current database. A table the migration creates is
    /// dropped behind the ledger's back, which makes execution observable: if the
    /// second apply runs the file, the table comes back.
    #[tokio::test]
    async fn a_current_database_is_not_migrated_again() {
        let Some(fixture) = fixture().await else {
            return;
        };
        apply(&fixture.config, &fixture.env)
            .await
            .expect("the first apply migrates");
        fixture
            .observe()
            .await
            .batch_execute("DROP TABLE axond_cp_idempotency CASCADE")
            .await
            .expect("drop a table the migration creates");

        let second = apply(&fixture.config, &fixture.env)
            .await
            .expect("a current database is a no-op, not a failure");
        assert_eq!(
            second.state(),
            Some(&State::Current {
                version: schema::required_version()
            }),
            "{second}"
        );
        let recreated = fixture
            .observe()
            .await
            .query_one(
                "SELECT to_regclass($1)::text",
                &[&format!("{}.axond_cp_idempotency", fixture.schema)],
            )
            .await
            .expect("probe the dropped table")
            .get::<_, Option<String>>(0)
            .is_some();
        assert!(
            !recreated,
            "applying to a current schema re-executed the shipped migration SQL"
        );
    }

    /// Safe before replicas start includes safe *while another operator is doing
    /// the same thing*: the advisory lock is what makes two applies one migration.
    #[tokio::test]
    async fn concurrent_applies_migrate_the_database_once() {
        let Some(fixture) = fixture().await else {
            return;
        };
        let (left, right) = tokio::join!(
            apply(&fixture.config, &fixture.env),
            apply(&fixture.config, &fixture.env)
        );
        let states = [
            left.expect("the first apply").state().cloned(),
            right.expect("the second apply").state().cloned(),
        ];
        assert_eq!(
            states
                .iter()
                .filter(|state| matches!(state, Some(State::Applied { .. })))
                .count(),
            1,
            "exactly one of two concurrent applies migrates: {states:?}"
        );
        assert!(
            states
                .iter()
                .any(|state| matches!(state, Some(State::Current { .. }))),
            "the apply that lost the race finds the schema current: {states:?}"
        );
        assert_eq!(
            fixture.ledger().await.len(),
            schema::MIGRATIONS.len(),
            "each migration is recorded once however many applies ran"
        );
    }

    /// A database a newer build owns: both commands must report it, and `apply`
    /// must refuse rather than write more DDL over a history it cannot read.
    #[tokio::test]
    async fn a_future_ledger_is_reported_by_status_and_refused_by_apply() {
        let Some(fixture) = fixture().await else {
            return;
        };
        apply(&fixture.config, &fixture.env)
            .await
            .expect("migrate to current first");
        fixture
            .observe()
            .await
            .execute(
                "INSERT INTO axond_cp_schema_migration (version, name, checksum) VALUES ($1, $2, \
                 $3)",
                &[
                    &999_i32,
                    &"control_plane_0999_from_the_future",
                    &Checksum::of(b"a newer build wrote this").to_string(),
                ],
            )
            .await
            .expect("record a future migration");

        let report = status(&fixture.config, &fixture.env)
            .await
            .expect("a future schema is a state to report, not a failure to read");
        let Some(State::Refused { reason }) = report.state() else {
            panic!("a future ledger is refused: {report}");
        };
        assert!(reason.contains("newer gateway"), "{reason}");
        assert!(!report.is_ok(), "the CLI exits non-zero on this");

        let error = apply(&fixture.config, &fixture.env)
            .await
            .expect_err("a future ledger must not be migrated");
        assert!(
            matches!(error, OpsError::Refused { .. }) && !error.is_retryable(),
            "{error}"
        );
    }

    /// An applied migration edited in place. The version still matches, so only
    /// the checksum catches it — and it must be caught before any DDL is applied
    /// on top of a file the database does not actually contain.
    #[tokio::test]
    async fn a_drifted_ledger_is_refused_by_both_commands() {
        let Some(fixture) = fixture().await else {
            return;
        };
        apply(&fixture.config, &fixture.env)
            .await
            .expect("migrate to current first");
        fixture
            .observe()
            .await
            .execute(
                "UPDATE axond_cp_schema_migration SET checksum = $1 WHERE version = 1",
                &[&Checksum::of(b"edited in place").to_string()],
            )
            .await
            .expect("edit the recorded checksum");

        let report = status(&fixture.config, &fixture.env).await.expect("status");
        let Some(State::Refused { reason }) = report.state() else {
            panic!("drift is refused: {report}");
        };
        assert!(reason.contains("edited in place"), "{reason}");
        assert!(
            apply(&fixture.config, &fixture.env)
                .await
                .is_err_and(|error| matches!(error, OpsError::Refused { .. })),
            "drift is not something an apply resolves"
        );
    }

    /// A ledger that is not this ledger: the table name is taken by something
    /// else. Reported as a schema disagreement rather than as an outage, because
    /// retrying it forever is not the fix.
    #[tokio::test]
    async fn a_foreign_ledger_is_refused_rather_than_treated_as_absent() {
        let Some(fixture) = fixture().await else {
            return;
        };
        fixture
            .observe()
            .await
            .batch_execute("CREATE TABLE axond_cp_schema_migration (id int primary key)")
            .await
            .expect("take the ledger's name");

        let report = status(&fixture.config, &fixture.env).await.expect("status");
        let Some(State::Refused { reason }) = report.state() else {
            panic!("a foreign table under the ledger's name is refused: {report}");
        };
        assert!(
            reason.contains("is not the one this build writes"),
            "{reason}"
        );
        assert!(
            apply(&fixture.config, &fixture.env).await.is_err(),
            "an apply must not write into a table it cannot account for"
        );
    }

    /// The missing-database case end to end: a reference that resolves to a
    /// database nothing answers at is an outage, is worth retrying, and still
    /// never prints the DSN it failed to connect with.
    #[tokio::test]
    async fn an_unreachable_database_is_retryable_and_never_echoes_the_dsn() {
        let config = Config::from_toml_str(
            "mode = \"stateful\"\n\
             [control_plane]\n\
             dsn_env = \"GW_CONTROL_PLANE_DSN\"\n\
             connect_timeout_ms = 500\n\
             [secret_store]\n\
             kek_env = \"GW_KEK\"\n\
             [[admin_breakglass]]\n\
             env = \"GW_BREAKGLASS\"\n",
        )
        .expect("valid stateful config");
        // Port 1 on the loopback: refused immediately rather than waiting for a
        // timeout, so the test is fast and deterministic.
        let env = HashMap::from([(
            "GW_CONTROL_PLANE_DSN".to_owned(),
            "postgres://axond:hunter2@127.0.0.1:1/axond".to_owned(),
        )]);
        for error in [
            status(&config, &env).await.expect_err("nothing answers"),
            apply(&config, &env).await.expect_err("nothing answers"),
        ] {
            assert!(error.is_retryable(), "{error}");
            let rendered = error.to_string();
            assert!(!rendered.contains("hunter2"), "{rendered}");
            assert!(!rendered.contains("postgres://"), "{rendered}");
        }
    }

    #[test]
    fn an_applied_report_names_the_files_that_ran() {
        let report = Report::ControlPlane {
            dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
            state: State::Applied {
                applied: named(&[1]),
            },
        };
        let rendered = report.to_string();
        assert!(
            rendered.contains("v1 control_plane_0001_initial"),
            "{rendered}"
        );
        assert!(report.is_ok() && report.is_settled(), "{rendered}");
    }
}
