//! Forward-only migrations, and the status a boot refuses or accepts on.
//!
//! Two properties matter more than the mechanism:
//!
//! - **Forward-only.** A migration is applied once, recorded with a checksum of
//!   its own text, and never edited afterwards. Editing an applied file is the
//!   failure mode a version number alone does not catch — the version still
//!   matches, so the database is silently not the schema the build expects — so
//!   [`SchemaStatus::Drifted`] reports it instead.
//! - **A status is a decision, not a log line.** A gateway that finds a database
//!   [`SchemaStatus::Behind`] must either migrate it or refuse to serve the
//!   control plane; one that finds it [`SchemaStatus::Ahead`] must always refuse,
//!   because a newer writer owns that database and "migrating backwards" is not a
//!   thing. Returning a typed status rather than a boolean is what lets the caller
//!   tell those apart.
//!
//! The DDL itself is the operator contract in `ops/postgres/`, embedded here from
//! the package-local copy under `crates/gateway/sql/`; `tests/shipped_ddl.rs`
//! gates the two against drift.

use std::fmt;

use tokio_postgres::Transaction;

use crate::desired_state::Checksum;

/// One versioned, forward-only migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Migration {
    /// Monotonic, gapless, and never reused.
    pub version: i32,
    /// The shipped file's stem, so a failure names something greppable.
    pub name: &'static str,
    /// The file's text, applied verbatim.
    pub sql: &'static str,
}

impl Migration {
    /// The checksum recorded when this migration is applied.
    pub fn checksum(&self) -> Checksum {
        Checksum::of(self.sql.as_bytes())
    }
}

/// Every migration this build ships, in application order.
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "control_plane_0001_initial",
    sql: include_str!("../../../sql/control_plane_0001_initial.sql"),
}];

/// The schema version this build requires.
pub fn required_version() -> i32 {
    MIGRATIONS
        .last()
        .expect("at least one migration ships")
        .version
}

/// The minimum PostgreSQL the DDL is written against, as `server_version_num`.
///
/// 14 is the floor because the journal uses identity columns and `ON CONFLICT`
/// against partial unique indexes. CI exercises 17.
pub const MINIMUM_SERVER_VERSION_NUM: i32 = 140_000;

/// What a database holds relative to what this build requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaStatus {
    /// No journal at all: the migration bookkeeping table does not exist.
    Absent,
    Current {
        version: i32,
    },
    Behind {
        applied: i32,
        required: i32,
    },
    /// A newer build has migrated this database. Always a refusal.
    Ahead {
        applied: i32,
        required: i32,
    },
    /// A recorded migration's text is not the text this build ships.
    Drifted {
        version: i32,
        expected: Checksum,
        found: Checksum,
    },
}

impl SchemaStatus {
    /// Whether the control plane may be used as-is.
    pub fn is_current(&self) -> bool {
        matches!(self, Self::Current { .. })
    }

    /// Whether applying this build's migrations would make it current.
    ///
    /// False for [`SchemaStatus::Ahead`] and [`SchemaStatus::Drifted`]: both mean
    /// the database is not this schema's history, and writing more DDL over it
    /// would make that worse rather than better.
    pub fn is_migratable(&self) -> bool {
        matches!(self, Self::Absent | Self::Behind { .. })
    }
}

impl fmt::Display for SchemaStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => write!(
                f,
                "the control-plane schema is not present; apply \
                 ops/postgres/control_plane_0001_initial.sql"
            ),
            Self::Current { version } => write!(f, "control-plane schema v{version} is current"),
            Self::Behind { applied, required } => write!(
                f,
                "control-plane schema is v{applied}, but this build requires v{required}; \
                 apply the pending migrations in ops/postgres/"
            ),
            Self::Ahead { applied, required } => write!(
                f,
                "control-plane schema is v{applied}, which is newer than the v{required} this \
                 build knows; a newer gateway owns this database"
            ),
            Self::Drifted {
                version,
                expected,
                found,
            } => write!(
                f,
                "control-plane migration v{version} was applied as {found}, but this build ships \
                 {expected}; an applied migration was edited in place"
            ),
        }
    }
}

/// The bookkeeping table's name, unqualified: the caller's `search_path` decides
/// which schema it is read from.
const MIGRATION_TABLE: &str = "axond_cp_schema_migration";

/// Read the status inside a transaction the caller controls.
///
/// Takes a transaction rather than a client so a status read and the migration
/// that follows it see the same snapshot under the same advisory lock.
pub(super) async fn status(
    transaction: &Transaction<'_>,
) -> Result<SchemaStatus, tokio_postgres::Error> {
    let present: Option<String> = transaction
        .query_one("SELECT to_regclass($1)::text", &[&MIGRATION_TABLE])
        .await?
        .get(0);
    if present.is_none() {
        return Ok(SchemaStatus::Absent);
    }
    let recorded: Vec<(i32, String)> = transaction
        .query(
            &format!("SELECT version, checksum FROM {MIGRATION_TABLE} ORDER BY version"),
            &[],
        )
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    Ok(classify(&recorded))
}

/// The status a set of recorded `(version, checksum)` rows implies.
///
/// Separated from the query so the decision table is testable without a database:
/// it is the part that has to be right.
fn classify(recorded: &[(i32, String)]) -> SchemaStatus {
    let required = required_version();
    let applied = recorded.iter().map(|(version, _)| *version).max();
    let Some(applied) = applied else {
        return SchemaStatus::Behind {
            applied: 0,
            required,
        };
    };
    for (version, checksum) in recorded {
        let Some(migration) = MIGRATIONS.iter().find(|m| m.version == *version) else {
            // A version this build has never heard of: the database's history is
            // longer than ours, whatever the maximum happens to be.
            return SchemaStatus::Ahead {
                applied: *version,
                required,
            };
        };
        let expected = migration.checksum();
        if checksum != &expected.to_string() {
            return SchemaStatus::Drifted {
                version: *version,
                expected,
                found: Checksum::parse(checksum).unwrap_or(expected),
            };
        }
    }
    match applied.cmp(&required) {
        std::cmp::Ordering::Equal => SchemaStatus::Current { version: applied },
        std::cmp::Ordering::Less => SchemaStatus::Behind { applied, required },
        std::cmp::Ordering::Greater => SchemaStatus::Ahead { applied, required },
    }
}

/// Apply every migration the database is missing, recording each one.
///
/// The caller holds the advisory lock, so two gateways booting against one empty
/// database serialize here rather than both running the DDL.
pub(super) async fn migrate(
    transaction: &Transaction<'_>,
    from: &SchemaStatus,
) -> Result<(), tokio_postgres::Error> {
    let applied = match from {
        SchemaStatus::Behind { applied, .. } => *applied,
        _ => 0,
    };
    for migration in MIGRATIONS.iter().filter(|m| m.version > applied) {
        transaction.batch_execute(migration.sql).await?;
        transaction
            .execute(
                &format!(
                    "INSERT INTO {MIGRATION_TABLE} (version, name, checksum) VALUES ($1, $2, $3) \
                     ON CONFLICT (version) DO NOTHING"
                ),
                &[
                    &migration.version,
                    &migration.name,
                    &migration.checksum().to_string(),
                ],
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded(version: i32) -> (i32, String) {
        let migration = MIGRATIONS
            .iter()
            .find(|m| m.version == version)
            .expect("shipped migration");
        (version, migration.checksum().to_string())
    }

    #[test]
    fn migrations_are_gapless_and_never_reordered() {
        for (index, migration) in MIGRATIONS.iter().enumerate() {
            assert_eq!(
                migration.version,
                i32::try_from(index + 1).expect("small"),
                "migrations are numbered from 1 without gaps, so `applied` is a version count"
            );
            assert!(
                migration.name.starts_with("control_plane_"),
                "a migration's name is its shipped file's stem"
            );
            assert!(!migration.sql.trim().is_empty());
        }
        assert_eq!(required_version(), MIGRATIONS.len() as i32);
    }

    #[test]
    fn an_empty_or_partial_history_is_behind_and_a_complete_one_is_current() {
        assert_eq!(
            classify(&[]),
            SchemaStatus::Behind {
                applied: 0,
                required: required_version()
            }
        );
        let complete: Vec<_> = MIGRATIONS.iter().map(|m| recorded(m.version)).collect();
        assert_eq!(
            classify(&complete),
            SchemaStatus::Current {
                version: required_version()
            }
        );
        assert!(classify(&complete).is_current());
    }

    #[test]
    fn an_unknown_version_is_ahead_and_never_migratable() {
        let status = classify(&[recorded(1), (99, Checksum::of(b"newer").to_string())]);
        assert_eq!(
            status,
            SchemaStatus::Ahead {
                applied: 99,
                required: required_version()
            }
        );
        assert!(!status.is_migratable());
        assert!(status.to_string().contains("newer gateway"));
    }

    #[test]
    fn an_edited_applied_migration_is_drift_rather_than_a_matching_version() {
        let status = classify(&[(1, Checksum::of(b"edited in place").to_string())]);
        let SchemaStatus::Drifted {
            version,
            expected,
            found,
        } = status.clone()
        else {
            panic!("an edited migration must be reported as drift, got {status:?}");
        };
        assert_eq!(version, 1);
        assert_eq!(expected, MIGRATIONS[0].checksum());
        assert_eq!(found, Checksum::of(b"edited in place"));
        assert!(!status.is_migratable());
        assert!(!status.is_current());
    }

    #[test]
    fn the_shipped_ddl_is_the_migration_this_build_applies() {
        let ddl = MIGRATIONS[0].sql;
        for object in [
            "axond_cp_schema_migration",
            "axond_cp_blob",
            "axond_cp_resource_version",
            "axond_cp_resource_dependency",
            "axond_cp_mutation",
            "axond_cp_revision",
            "axond_cp_revision_entry",
            "axond_cp_revision_blob",
            "axond_cp_audit_event",
            "axond_cp_idempotency",
            "axond_cp_head",
        ] {
            assert!(
                ddl.contains(&format!("CREATE TABLE IF NOT EXISTS {object}")),
                "the journal's {object} table is missing from the shipped DDL"
            );
        }
    }
}
