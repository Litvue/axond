//! `axond migrate status`, `axond migrate apply`, and `axond migrate adopt`: the
//! control-plane journal's schema, reported and moved forward.
//!
//! One database, one ledger, one direction. The journal records every migration
//! it has applied — version, shipped file name, and a checksum of that file's
//! text — so "what does this database contain?" is answered from the database
//! rather than guessed from the binary's version. [`status`] reads that ledger
//! without writing to it; [`apply`] writes the versions above the recorded prefix
//! and nothing else; [`adopt`] writes ledger rows for DDL an operator applied out
//! of band, and only for the versions whose objects the database actually holds.
//! No command here executes a migration file twice, and none of them repairs a
//! history: adoption is how an *unrecorded* history becomes a recorded one, which
//! is a different question from a recorded history that disagrees with this build.
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
use crate::backends::control_plane::postgres::Adoption;
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
    /// `adopt` recorded these versions as already applied, on the evidence of
    /// the tables they declare, and `pending` is what an `apply` must still add.
    /// Never empty for the same reason [`State::Applied`] is not: an `adopt`
    /// that recorded nothing reports the state it found instead.
    Adopted {
        adopted: Vec<(i32, &'static str)>,
        pending: Vec<(i32, &'static str)>,
    },
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
        match self {
            Self::Pending { .. } | Self::Refused { .. } => false,
            // An adoption that left versions above the baseline is a schema no
            // replica may serve yet: the operator's next command is an `apply`.
            Self::Adopted { pending, .. } => pending.is_empty(),
            Self::Current { .. } | Self::Applied { .. } => true,
        }
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
            State::Adopted { adopted, pending } => {
                write!(
                    f,
                    "adopted {} migration(s) as already applied: {}",
                    adopted.len(),
                    list(adopted)
                )?;
                if pending.is_empty() {
                    return write!(f, "; the schema is now current");
                }
                write!(
                    f,
                    "; {} migration(s) still pending: {} (run `axond migrate apply`)",
                    pending.len(),
                    list(pending)
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

/// Record the baseline a hand-applied schema left unrecorded.
///
/// The operator-explicit half of the empty-ledger contract: applying the shipped
/// DDL with `psql` creates the ledger without recording anything in it, and the
/// ledger is the only record of what ran, so this build will neither serve that
/// database nor migrate it from zero. `adopt` is how an operator says "this DDL
/// was applied" — and it is checked rather than believed. The baseline recorded is
/// the longest prefix of shipped migrations whose statements are *all* confirmed
/// — tables and indexes present, idempotent seed rows written; a prefix that is
/// empty, interrupted, or not a prefix is refused with
/// [`OpsError::Refused`] and writes nothing.
///
/// It executes no migration SQL, so it can never double-apply a file. It is
/// idempotent: run against a ledger that already records a history, it writes
/// nothing and reports what is there.
pub async fn adopt(config: &Config, env: &HashMap<String, String>) -> Result<Report, OpsError> {
    let Some(control_plane) = control_plane(config) else {
        return Ok(Report::NoControlPlane);
    };
    let dsn_env = control_plane_dsn_env(control_plane);
    let store = open_control_plane(control_plane, env).await?;
    let state = match store.adopt_ledger().await.map_err(control_plane_error)? {
        Adoption::Recorded { versions, status } => State::Adopted {
            adopted: named(&versions),
            pending: named(&schema::pending(&status)),
        },
        // Nothing was written, so the report is the state that made writing
        // unnecessary: current, or behind with an `apply` outstanding.
        Adoption::AlreadyRecorded { status } => State::from_status(&status),
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
            adopt(&config, &env).await.expect("adopt"),
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

        /// The database an operator gets from `psql -f`: every object the shipped
        /// migration declares, including the ledger table, and no ledger row.
        async fn hand_applied(&self) {
            let client = self.observe().await;
            for migration in schema::MIGRATIONS.iter() {
                client
                    .batch_execute(migration.sql)
                    .await
                    .expect("apply the shipped DDL the way an operator would");
            }
            assert!(
                self.ledger().await.is_empty(),
                "applying the shipped DDL by hand must not record anything: that is the whole \
                 problem adoption exists for"
            );
        }

        async fn relation_exists(&self, relation: &str) -> bool {
            self.observe()
                .await
                .query_one(
                    "SELECT to_regclass($1)::text",
                    &[&format!("{}.{relation}", self.schema)],
                )
                .await
                .expect("probe a relation")
                .get::<_, Option<String>>(0)
                .is_some()
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

    /// A ledger table with no rows is the database an operator gets from applying
    /// the shipped SQL with `psql`, and it is indistinguishable from an untouched
    /// one: the ledger is the only record of what ran. Migrating it from zero
    /// would replay every file over objects that are already there, so it is
    /// refused with the baseline to state instead — and refused *without*
    /// touching the database, which is what the empty ledger still being empty
    /// proves.
    #[tokio::test]
    async fn an_empty_ledger_is_refused_rather_than_migrated_from_zero() {
        let Some(fixture) = fixture().await else {
            return;
        };
        // The ledger, by hand, exactly as the shipped migration declares it.
        fixture
            .observe()
            .await
            .batch_execute(
                "CREATE TABLE axond_cp_schema_migration (
                     version     integer     PRIMARY KEY,
                     name        text        NOT NULL,
                     checksum    text        NOT NULL,
                     applied_at  timestamptz NOT NULL DEFAULT now()
                 )",
            )
            .await
            .expect("create an empty ledger");

        let reported = status(&fixture.config, &fixture.env)
            .await
            .expect("an empty ledger has a status");
        let Some(State::Refused { reason }) = reported.state() else {
            panic!("an empty ledger is not something to migrate from zero: {reported}");
        };
        assert!(
            reason.contains("records no migrations")
                && reason.contains("axond migrate adopt")
                && reason.contains("drop the empty"),
            "the refusal names both ways out of an empty ledger: {reason}"
        );

        let error = apply(&fixture.config, &fixture.env)
            .await
            .expect_err("apply must refuse an empty ledger");
        assert!(
            matches!(error, OpsError::Refused { .. }) && !error.is_retryable(),
            "an operator decision, not an outage: {error:?}"
        );
        assert!(
            fixture.ledger().await.is_empty(),
            "a refused apply must not record a migration"
        );
        let created = fixture
            .observe()
            .await
            .query_one(
                "SELECT to_regclass($1)::text",
                &[&format!("{}.axond_cp_blob", fixture.schema)],
            )
            .await
            .expect("probe a table the migration would create")
            .get::<_, Option<String>>(0)
            .is_some();
        assert!(
            !created,
            "a refused apply executed the shipped migration SQL anyway"
        );

        // Adoption is refused here too, and for the opposite reason `apply` is:
        // there is no applied schema to adopt. A ledger nobody applied DDL beside
        // is a database whose objects say "nothing ran", so recording a baseline
        // would be recording a fiction that every later decision then trusts.
        let error = adopt(&fixture.config, &fixture.env)
            .await
            .expect_err("there is no baseline to adopt when no object is present");
        assert!(
            matches!(error, OpsError::Refused { .. }) && !error.is_retryable(),
            "an operator decision, not an outage: {error:?}"
        );
        assert!(
            error.to_string().contains("drop the empty")
                && error.to_string().contains("axond migrate apply"),
            "the refusal names the way forward for an unapplied database: {error}"
        );
        assert!(
            fixture.ledger().await.is_empty(),
            "a refused adoption must not record a baseline"
        );
        assert!(
            !fixture.relation_exists("axond_cp_blob").await,
            "adoption must never execute migration SQL"
        );

        // The baseline, stated by hand: still supported, and still classified the
        // same way, so `adopt` is a convenience over the manual `INSERT` rather
        // than a replacement for a contract it changed.
        let client = fixture.observe().await;
        for migration in schema::MIGRATIONS.iter() {
            client
                .execute(
                    "INSERT INTO axond_cp_schema_migration (version, name, checksum) VALUES ($1, \
                     $2, $3)",
                    &[
                        &migration.version,
                        &migration.name,
                        &migration.checksum().to_string(),
                    ],
                )
                .await
                .expect("record the baseline the DDL corresponds to");
        }
        let adopted = apply(&fixture.config, &fixture.env)
            .await
            .expect("a recorded baseline is current");
        assert_eq!(
            adopted.state(),
            Some(&State::Current {
                version: schema::required_version()
            }),
            "{adopted}"
        );
    }

    /// A `psql -f` that stopped one statement short of the end.
    ///
    /// The shipped file ends by seeding the singleton head row, and `psql` without
    /// a wrapping transaction can abort before it: every table present, no head
    /// row. Adoption records what it confirmed, and a seed row is part of what a
    /// migration did, so this is the partly-applied refusal rather than a baseline
    /// — otherwise the ledger would call v1 applied and the next `apply` would
    /// never write the anchor publication needs.
    #[tokio::test]
    async fn a_hand_applied_schema_missing_its_seed_row_is_refused_rather_than_adopted() {
        let Some(fixture) = fixture().await else {
            return;
        };
        fixture.hand_applied().await;
        fixture
            .observe()
            .await
            .batch_execute("DELETE FROM axond_cp_head")
            .await
            .expect("undo the seed the shipped file ends with");

        let error = adopt(&fixture.config, &fixture.env)
            .await
            .expect_err("a migration that did not finish is not a baseline");
        assert!(
            matches!(error, OpsError::Refused { .. }) && !error.is_retryable(),
            "an operator decision, not an outage: {error:?}"
        );
        assert!(
            error.to_string().contains("only partly applied")
                && error.to_string().contains("axond_cp_head"),
            "the refusal has to name what is not there: {error}"
        );
        assert!(
            fixture.ledger().await.is_empty(),
            "a refused adoption must not record a baseline"
        );
    }

    /// Another install's journal on the same search path is not evidence about
    /// *this* schema.
    ///
    /// With `[control_plane] schema` unset the DSN's own `search_path` applies, and
    /// it may well end in `public`. A relation probe that resolved down that path
    /// would read the neighbour's tables as proof that this schema's DDL was
    /// applied and record a baseline for objects it cannot even see — the one way
    /// adoption could write a ledger row for a migration that never ran here. So
    /// the probe is qualified to `current_schema()`, the schema an `apply` would
    /// have created these tables in.
    #[tokio::test]
    async fn objects_in_another_schema_on_the_path_are_not_evidence_of_an_applied_baseline() {
        let Some(fixture) = fixture().await else {
            return;
        };
        // The neighbour: a complete, hand-applied journal, ledger row and all.
        let neighbour = format!("{}_neighbour", fixture.schema);
        let client = client(&fixture.dsn).await;
        client
            .batch_execute(&format!(
                "CREATE SCHEMA {neighbour}; SET search_path TO {neighbour}"
            ))
            .await
            .expect("create the neighbouring schema");
        for migration in schema::MIGRATIONS.iter() {
            client
                .batch_execute(migration.sql)
                .await
                .expect("apply the shipped DDL into the neighbour");
        }

        // This schema: the empty ledger and nothing else, on a search path that
        // reaches the neighbour's tables.
        fixture
            .observe()
            .await
            .batch_execute(
                "CREATE TABLE axond_cp_schema_migration (
                     version     integer     PRIMARY KEY,
                     name        text        NOT NULL,
                     checksum    text        NOT NULL,
                     applied_at  timestamptz NOT NULL DEFAULT now()
                 )",
            )
            .await
            .expect("create an empty ledger");
        let config = Config::from_toml_str(
            "mode = \"stateful\"\n\
             [control_plane]\n\
             dsn_env = \"GW_CONTROL_PLANE_DSN\"\n\
             [secret_store]\n\
             kek_env = \"GW_KEK\"\n\
             [[admin_breakglass]]\n\
             env = \"GW_BREAKGLASS\"\n",
        )
        .expect("valid stateful config without a schema of its own");
        let separator = if fixture.dsn.contains('?') { '&' } else { '?' };
        let env = HashMap::from([(
            "GW_CONTROL_PLANE_DSN".to_owned(),
            format!(
                "{}{separator}options=-c%20search_path%3D{},{neighbour}",
                fixture.dsn, fixture.schema
            ),
        )]);

        let error = adopt(&config, &env)
            .await
            .expect_err("a neighbour's tables are not this schema's baseline");
        assert!(
            matches!(error, OpsError::Refused { .. }) && !error.is_retryable(),
            "an operator decision, not an outage: {error:?}"
        );
        assert!(
            error.to_string().contains("drop the empty"),
            "the refusal is the one for a database where nothing was applied: {error}"
        );
        assert!(
            fixture.ledger().await.is_empty(),
            "a baseline was recorded for objects that live in another schema"
        );
    }

    /// The commands create neither the database nor the schema, so a configured
    /// `[control_plane] schema` that does not exist is an operator error — and
    /// `SET search_path` accepts a missing schema, so it arrives as the server
    /// rejecting the first `CREATE TABLE` rather than as a connection failure.
    /// A retryable classification there would have a rollout gate looping on
    /// something no retry can clear.
    #[tokio::test]
    async fn a_missing_schema_refuses_the_apply_rather_than_advising_a_retry() {
        let Some(mut fixture) = fixture().await else {
            return;
        };
        // The same fixture, pointed at a schema nothing created.
        let missing = format!("{}_absent", fixture.schema);
        fixture.config = Config::from_toml_str(&format!(
            "mode = \"stateful\"\n\
             [control_plane]\n\
             dsn_env = \"GW_CONTROL_PLANE_DSN\"\n\
             schema = \"{missing}\"\n\
             [secret_store]\n\
             kek_env = \"GW_KEK\"\n\
             [[admin_breakglass]]\n\
             env = \"GW_BREAKGLASS\"\n"
        ))
        .expect("valid stateful config");

        let error = apply(&fixture.config, &fixture.env)
            .await
            .expect_err("a schema that does not exist cannot be migrated");
        assert!(
            matches!(error, OpsError::Refused { .. }),
            "the server rejected the DDL, which is an operator's to fix: {error:?}"
        );
        assert!(
            !error.is_retryable(),
            "a rollout gate must stop rather than loop: {error}"
        );
        assert!(
            error.to_string().contains("schema exists"),
            "the refusal names what to check: {error}"
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

    /// The same-names-wrong-types case: a foreign table that answers to `version`,
    /// `name`, and `checksum` makes the ledger query *succeed*, so the disagreement
    /// only shows up while decoding. That has to be the reported refusal too,
    /// rather than a panic in the middle of an operator's command.
    #[tokio::test]
    async fn a_ledger_shaped_table_with_other_column_types_is_refused_not_a_panic() {
        let Some(fixture) = fixture().await else {
            return;
        };
        fixture
            .observe()
            .await
            .batch_execute(
                "CREATE TABLE axond_cp_schema_migration \
                 (version text primary key, name text, checksum bytea)",
            )
            .await
            .expect("take the ledger's name with other types");
        fixture
            .observe()
            .await
            .batch_execute(
                "INSERT INTO axond_cp_schema_migration VALUES ('one', 'whatever', '\\x00')",
            )
            .await
            .expect("give it a row to decode");

        let report = status(&fixture.config, &fixture.env)
            .await
            .expect("a decode disagreement is a status, not an error");
        let Some(State::Refused { reason }) = report.state() else {
            panic!("a ledger this build cannot read is refused: {report}");
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

    /// A bad moment is not a broken history. Every server-reported error carries a
    /// SQLSTATE, so classifying the ledger read by "did the server answer with a
    /// code?" would tell an operator to go and repair a history that is fine — and
    /// would drop the retryable classification. Class 42 means the name is not this
    /// build's ledger; a serialization failure means try again.
    #[tokio::test]
    async fn a_transient_ledger_read_failure_stays_retryable() {
        let Some(fixture) = fixture().await else {
            return;
        };
        // A view over a function that raises a chosen SQLSTATE: the ledger's name
        // resolves and its columns type-check, so the only thing under test is how
        // the error is classified.
        let raise = |code: &str| {
            format!(
                "CREATE FUNCTION ledger_{code}() RETURNS TABLE(version integer, name text, \
                 checksum text) AS $$ BEGIN RAISE EXCEPTION 'simulated' USING ERRCODE = \
                 '{code}'; END $$ LANGUAGE plpgsql;\n\
                 CREATE VIEW axond_cp_schema_migration AS SELECT * FROM ledger_{code}();"
            )
        };
        fixture
            .observe()
            .await
            .batch_execute(&raise("40001"))
            .await
            .expect("stand in for a serialization failure");
        let error = status(&fixture.config, &fixture.env)
            .await
            .expect_err("a serialization failure is an outage, not a verdict");
        assert!(
            error.is_retryable(),
            "a transient server error must stay retryable: {error}"
        );

        // The same shape with a class-42 code is the permanent verdict it looks
        // like: this table is not the ledger.
        let client = fixture.observe().await;
        client
            .batch_execute("DROP VIEW axond_cp_schema_migration")
            .await
            .expect("drop the stand-in");
        client
            .batch_execute(&raise("42703"))
            .await
            .expect("stand in for an undefined column");
        let report = status(&fixture.config, &fixture.env)
            .await
            .expect("a schema disagreement is a status, not an error");
        assert!(
            matches!(report.state(), Some(State::Refused { .. })),
            "{report}"
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
            adopt(&config, &env).await.expect_err("nothing answers"),
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

    #[test]
    fn an_adopted_report_names_the_baseline_and_what_is_still_pending() {
        let whole = Report::ControlPlane {
            dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
            state: State::Adopted {
                adopted: named(&[1]),
                pending: Vec::new(),
            },
        };
        let rendered = whole.to_string();
        assert!(
            rendered.contains("adopted 1 migration(s) as already applied")
                && rendered.contains("v1 control_plane_0001_initial")
                && rendered.contains("now current"),
            "{rendered}"
        );
        assert!(whole.is_ok() && whole.is_settled(), "{rendered}");

        // A baseline below the required version is a success that is not a
        // finished deployment: the exit code has to keep a rollout gate honest.
        let partial = Report::ControlPlane {
            dsn_env: "GW_CONTROL_PLANE_DSN".to_owned(),
            state: State::Adopted {
                adopted: named(&[1]),
                pending: named(&[1]),
            },
        };
        assert!(partial.is_ok() && !partial.is_settled(), "{partial}");
        assert!(
            partial.to_string().contains("axond migrate apply"),
            "{partial}"
        );
    }

    /// The database `psql -f ops/postgres/control_plane_0001_initial.sql` leaves:
    /// every object present, the ledger present, and nothing recorded in it. What
    /// `adopt` is for — and the recording is on the evidence of the objects, so
    /// afterwards the database is byte-for-byte the ledger an `apply` would have
    /// written, which is what makes every later classification the same for both
    /// paths.
    #[tokio::test]
    async fn a_hand_applied_schema_is_adopted_as_the_baseline_its_objects_prove() {
        let Some(fixture) = fixture().await else {
            return;
        };
        fixture.hand_applied().await;

        let refused = status(&fixture.config, &fixture.env)
            .await
            .expect("an unrecorded schema has a status");
        assert!(
            matches!(refused.state(), Some(State::Refused { .. })),
            "an unrecorded schema is refused until it is adopted: {refused}"
        );

        let report = adopt(&fixture.config, &fixture.env)
            .await
            .expect("the objects the shipped DDL declares are all present");
        assert_eq!(
            report.state(),
            Some(&State::Adopted {
                adopted: named(
                    &schema::MIGRATIONS
                        .iter()
                        .map(|migration| migration.version)
                        .collect::<Vec<_>>()
                ),
                pending: Vec::new(),
            }),
            "{report}"
        );
        assert_eq!(
            fixture.ledger().await,
            schema::MIGRATIONS
                .iter()
                .map(|migration| (
                    migration.version,
                    migration.name.to_owned(),
                    migration.checksum().to_string()
                ))
                .collect::<Vec<_>>(),
            "an adopted baseline is the ledger an apply would have written"
        );

        let settled = status(&fixture.config, &fixture.env)
            .await
            .expect("an adopted schema has a status");
        assert_eq!(
            settled.state(),
            Some(&State::Current {
                version: schema::required_version()
            }),
            "{settled}"
        );

        // The point of the whole exercise: the shipped SQL is never executed over
        // objects that are already there. A table dropped after adoption stays
        // dropped, because an apply against a current schema applies nothing.
        fixture
            .observe()
            .await
            .batch_execute("DROP TABLE axond_cp_idempotency CASCADE")
            .await
            .expect("drop a table the migration creates");
        let applied = apply(&fixture.config, &fixture.env)
            .await
            .expect("an adopted schema is current, so an apply is a no-op");
        assert_eq!(
            applied.state(),
            Some(&State::Current {
                version: schema::required_version()
            }),
            "{applied}"
        );
        assert!(
            !fixture.relation_exists("axond_cp_idempotency").await,
            "applying after an adoption replayed the shipped migration SQL"
        );
    }

    /// Adoption is idempotent, and it is idempotent the way `apply` is: the second
    /// run reports the state it found rather than recording a second baseline.
    #[tokio::test]
    async fn a_second_adopt_reports_the_recorded_history_rather_than_writing_again() {
        let Some(fixture) = fixture().await else {
            return;
        };
        fixture.hand_applied().await;
        adopt(&fixture.config, &fixture.env)
            .await
            .expect("the first adoption records the baseline");
        let first = fixture.ledger().await;

        let second = adopt(&fixture.config, &fixture.env)
            .await
            .expect("a recorded history is not a refusal");
        assert_eq!(
            second.state(),
            Some(&State::Current {
                version: schema::required_version()
            }),
            "a second adoption reports the history it found: {second}"
        );
        assert_eq!(
            fixture.ledger().await,
            first,
            "a second adoption rewrote the ledger it should have left alone"
        );
    }

    /// An ordinary migrated database, for the same reason a twice-adopted one is a
    /// no-op: `adopt` answers "what is this *unrecorded* schema?", so a database
    /// that already has a history is reported rather than written to. That is what
    /// keeps a mistaken `adopt` in a rollout from being a ledger edit.
    #[tokio::test]
    async fn adopting_a_migrated_database_reports_it_and_records_nothing() {
        let Some(fixture) = fixture().await else {
            return;
        };
        apply(&fixture.config, &fixture.env)
            .await
            .expect("migrate normally");
        let recorded = fixture.ledger().await;

        let report = adopt(&fixture.config, &fixture.env)
            .await
            .expect("a migrated database is current, not adoptable");
        assert_eq!(
            report.state(),
            Some(&State::Current {
                version: schema::required_version()
            }),
            "{report}"
        );
        assert_eq!(fixture.ledger().await, recorded, "{report}");
    }

    /// A half-applied migration is the case adoption must not paper over: one of
    /// the tables the file declares is missing, so neither "it was applied" nor
    /// "it was not" is true. Recording it would promise a schema the database does
    /// not have, and the failure has to leave the ledger exactly as empty as it
    /// found it — a partial baseline would be worse than none.
    #[tokio::test]
    async fn a_partly_applied_schema_is_refused_without_recording_anything() {
        let Some(fixture) = fixture().await else {
            return;
        };
        fixture.hand_applied().await;
        fixture
            .observe()
            .await
            .batch_execute("DROP TABLE axond_cp_head CASCADE")
            .await
            .expect("leave the hand-applied schema incomplete");

        let error = adopt(&fixture.config, &fixture.env)
            .await
            .expect_err("an incomplete schema has no baseline");
        assert!(
            matches!(error, OpsError::Refused { .. }) && !error.is_retryable(),
            "an operator decision, not an outage: {error:?}"
        );
        let rendered = error.to_string();
        assert!(
            rendered.contains("only partly applied") && rendered.contains("axond_cp_head"),
            "the refusal names the object that is missing: {rendered}"
        );
        assert!(
            fixture.ledger().await.is_empty(),
            "a refused adoption must record no version at all, not the ones it got through"
        );
        assert!(
            !fixture.relation_exists("axond_cp_head").await,
            "adoption executed DDL to repair what it should have refused"
        );

        // Still refused by the read-only command and by `apply`, unchanged: the
        // schema is unrecorded, and a failed adoption did not make it anything else.
        let report = status(&fixture.config, &fixture.env)
            .await
            .expect("status still reads");
        assert!(
            matches!(report.state(), Some(State::Refused { .. })),
            "{report}"
        );
        assert!(
            apply(&fixture.config, &fixture.env)
                .await
                .expect_err("apply still refuses an unrecorded schema")
                .to_string()
                .contains("records no migrations")
        );
    }

    /// A database with no ledger at all is `apply`'s job, not adoption's, and a
    /// ledger this build cannot account for is nobody's: adoption is one narrow
    /// operation on one status, so every other status it is pointed at is a typed
    /// refusal that writes nothing.
    #[tokio::test]
    async fn adoption_refuses_every_schema_that_is_not_an_empty_ledger() {
        let Some(fixture) = fixture().await else {
            return;
        };
        // Absent: no ledger to reconcile, and `apply` is the command for it.
        let error = adopt(&fixture.config, &fixture.env)
            .await
            .expect_err("an absent schema is not adoptable");
        assert!(
            matches!(error, OpsError::Refused { .. }) && !error.is_retryable(),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("existing but empty"),
            "the refusal says what adoption is for: {error}"
        );
        assert!(
            !fixture.ledger_exists().await,
            "a refused adoption created the ledger it refused to reconcile"
        );

        // Drifted: a recorded history whose text is not this build's. Adoption
        // must not "fix" it by recording the checksum this build ships.
        apply(&fixture.config, &fixture.env)
            .await
            .expect("migrate to current first");
        fixture
            .observe()
            .await
            .execute(
                "UPDATE axond_cp_schema_migration SET checksum = $1 WHERE version = $2",
                &[&Checksum::of(b"an edited migration").to_string(), &1_i32],
            )
            .await
            .expect("drift the recorded checksum");
        let error = adopt(&fixture.config, &fixture.env)
            .await
            .expect_err("a drifted history is not adoptable");
        assert!(
            matches!(error, OpsError::Refused { .. }) && !error.is_retryable(),
            "{error:?}"
        );
        assert_eq!(
            fixture.ledger().await.first().map(|row| row.2.clone()),
            Some(Checksum::of(b"an edited migration").to_string()),
            "a refused adoption rewrote a recorded checksum"
        );
    }

    /// Two operators, one database: adoption takes the journal's advisory lock and
    /// re-reads the ledger under it, so a race records one baseline and the loser
    /// reports the history the winner wrote.
    #[tokio::test]
    async fn concurrent_adoptions_record_the_baseline_once() {
        let Some(fixture) = fixture().await else {
            return;
        };
        fixture.hand_applied().await;
        let (left, right) = tokio::join!(
            adopt(&fixture.config, &fixture.env),
            adopt(&fixture.config, &fixture.env)
        );
        let states = [
            left.expect("the first adoption").state().cloned(),
            right.expect("the second adoption").state().cloned(),
        ];
        assert_eq!(
            states
                .iter()
                .filter(|state| matches!(state, Some(State::Adopted { .. })))
                .count(),
            1,
            "exactly one of two concurrent adoptions records a baseline: {states:?}"
        );
        assert!(
            states
                .iter()
                .any(|state| matches!(state, Some(State::Current { .. }))),
            "the adoption that lost the race finds a recorded history: {states:?}"
        );
        assert_eq!(
            fixture.ledger().await.len(),
            schema::MIGRATIONS.len(),
            "each migration is recorded once however many adoptions ran"
        );
    }
}
