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
///
/// The variants are the decision, so they are as fine-grained as the decisions
/// are: an operator told "the schema is wrong" has to go find out *how*, whereas
/// an operator told a version prefix has a hole in it knows a migration was
/// applied out of order or a row was deleted, and one told a name does not match
/// knows a file was renumbered rather than edited.
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
    /// The applied versions do not form a complete prefix: something applied
    /// v3 without v2, or a ledger row was deleted. Never migratable, because
    /// "apply everything after the maximum" would leave the hole behind.
    Incomplete {
        applied: i32,
        missing: Vec<i32>,
    },
    /// A version is recorded under a name this build does not ship it as. The
    /// checksum may still match — a renumbered or renamed file is the usual
    /// cause — so it is reported separately from drift.
    Renamed {
        version: i32,
        expected: &'static str,
        found: String,
    },
    /// The ledger exists but is not the ledger this build writes: a column is
    /// missing, a version is not a version, or the rows cannot be read as the
    /// journal's own bookkeeping.
    Malformed {
        message: String,
    },
}

impl SchemaStatus {
    /// Whether the control plane may be used as-is.
    pub fn is_current(&self) -> bool {
        matches!(self, Self::Current { .. })
    }

    /// Whether applying this build's migrations would make it current.
    ///
    /// True only for [`SchemaStatus::Absent`] and [`SchemaStatus::Behind`].
    /// Every other status means the database is not this schema's history, and
    /// writing more DDL over it would make that worse rather than better.
    pub fn is_migratable(&self) -> bool {
        matches!(self, Self::Absent | Self::Behind { .. })
    }
}

impl fmt::Display for SchemaStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Absent => write!(
                f,
                "the control-plane schema is not present; run `axond migrate apply` (or apply \
                 ops/postgres/control_plane_0001_initial.sql)"
            ),
            Self::Current { version } => write!(f, "control-plane schema v{version} is current"),
            Self::Behind { applied, required } => write!(
                f,
                "control-plane schema is v{applied}, but this build requires v{required}; run \
                 `axond migrate apply` before starting replicas"
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
            Self::Incomplete { applied, missing } => write!(
                f,
                "control-plane schema records v{applied} but is missing {}; the applied versions \
                 are not a complete history, so this build cannot tell what the database contains",
                missing
                    .iter()
                    .map(|version| format!("v{version}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Renamed {
                version,
                expected,
                found,
            } => write!(
                f,
                "control-plane migration v{version} is recorded as `{found}`, but this build ships \
                 v{version} as `{expected}`; a migration was renumbered or renamed rather than \
                 added"
            ),
            Self::Malformed { message } => write!(
                f,
                "the control-plane migration ledger is not the one this build writes: {message}"
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
    // A ledger table that will not answer the ledger's own query is a
    // [`SchemaStatus::Malformed`] rather than an error: the table exists, so this
    // is a schema disagreement an operator has to resolve, not a database that
    // could not be reached. Something else owns that name.
    let rows = match transaction
        .query(
            &format!("SELECT version, name, checksum FROM {MIGRATION_TABLE} ORDER BY version"),
            &[],
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) if error.code().is_some() => {
            return Ok(SchemaStatus::Malformed {
                message: format!(
                    "reading `{MIGRATION_TABLE}` as (version, name, checksum) failed: {error}"
                ),
            });
        }
        Err(error) => return Err(error),
    };
    let recorded: Vec<Recorded> = rows
        .iter()
        .map(|row| Recorded {
            version: row.get(0),
            name: row.get(1),
            checksum: row.get(2),
        })
        .collect();
    Ok(classify(&recorded))
}

/// One row of the migration ledger, as the database holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recorded {
    pub version: i32,
    pub name: String,
    pub checksum: String,
}

/// The status a set of ledger rows implies.
///
/// Separated from the query so the decision table is testable without a database:
/// it is the part that has to be right. The checks are ordered by how much they
/// tell an operator — an unknown version outranks a name mismatch, which outranks
/// a checksum mismatch — and the prefix check comes last because it is only
/// meaningful once every recorded row is one this build recognises.
fn classify(recorded: &[Recorded]) -> SchemaStatus {
    let required = required_version();
    if let Some(row) = recorded.iter().find(|row| row.version < 1) {
        return SchemaStatus::Malformed {
            message: format!(
                "v{} is recorded, but migration versions start at 1",
                row.version
            ),
        };
    }
    let mut versions: Vec<i32> = recorded.iter().map(|row| row.version).collect();
    versions.sort_unstable();
    versions.dedup();
    if versions.len() != recorded.len() {
        return SchemaStatus::Malformed {
            message: "a version is recorded more than once, so the ledger's primary key is not \
                      the one this build writes"
                .to_owned(),
        };
    }
    let Some(applied) = versions.last().copied() else {
        // The table exists and is empty: the ledger of a database whose DDL was
        // applied by hand without recording it, or one migration away from
        // existing at all. Either way, forward.
        return SchemaStatus::Behind {
            applied: 0,
            required,
        };
    };
    for row in recorded {
        let Some(migration) = MIGRATIONS.iter().find(|m| m.version == row.version) else {
            // A version this build has never heard of: the database's history is
            // longer than ours, whatever the maximum happens to be.
            return SchemaStatus::Ahead {
                applied: row.version,
                required,
            };
        };
        if row.name != migration.name {
            return SchemaStatus::Renamed {
                version: row.version,
                expected: migration.name,
                found: row.name.clone(),
            };
        }
        let expected = migration.checksum();
        if row.checksum != expected.to_string() {
            return SchemaStatus::Drifted {
                version: row.version,
                expected,
                found: Checksum::parse(&row.checksum).unwrap_or(expected),
            };
        }
    }
    // Versions are gapless by construction, so the applied set must be the whole
    // prefix `1..=applied`. A hole means a migration was skipped or a row was
    // deleted, and "apply everything above the maximum" would silently keep it.
    let missing: Vec<i32> = (1..=applied)
        .filter(|version| !versions.contains(version))
        .collect();
    if !missing.is_empty() {
        return SchemaStatus::Incomplete { applied, missing };
    }
    match applied.cmp(&required) {
        std::cmp::Ordering::Equal => SchemaStatus::Current { version: applied },
        std::cmp::Ordering::Less => SchemaStatus::Behind { applied, required },
        std::cmp::Ordering::Greater => SchemaStatus::Ahead { applied, required },
    }
}

/// The versions [`migrate`] would apply from this status, in application order.
///
/// Empty for a status that is already current, and empty for one that must be
/// refused: an operator asking "what would `apply` do?" gets the same answer the
/// apply itself would act on rather than a separately computed guess.
pub fn pending(from: &SchemaStatus) -> Vec<i32> {
    let applied = match from {
        SchemaStatus::Absent => 0,
        SchemaStatus::Behind { applied, .. } => *applied,
        // Everything else is either done or a refusal, and a refusal has no
        // pending set: what an operator has to do about it is not "apply files".
        SchemaStatus::Current { .. }
        | SchemaStatus::Ahead { .. }
        | SchemaStatus::Drifted { .. }
        | SchemaStatus::Incomplete { .. }
        | SchemaStatus::Renamed { .. }
        | SchemaStatus::Malformed { .. } => return Vec::new(),
    };
    MIGRATIONS
        .iter()
        .filter(|migration| migration.version > applied)
        .map(|migration| migration.version)
        .collect()
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

    fn recorded(version: i32) -> Recorded {
        let migration = MIGRATIONS
            .iter()
            .find(|m| m.version == version)
            .expect("shipped migration");
        Recorded {
            version,
            name: migration.name.to_owned(),
            checksum: migration.checksum().to_string(),
        }
    }

    /// A row nothing shipped wrote: the ledger as a restored backup, a manual
    /// `INSERT`, or a newer build left it.
    fn foreign(version: i32, name: &str, checksum: &str) -> Recorded {
        Recorded {
            version,
            name: name.to_owned(),
            checksum: checksum.to_owned(),
        }
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
        let status = classify(&[
            recorded(1),
            foreign(
                99,
                "control_plane_0099_future",
                &Checksum::of(b"newer").to_string(),
            ),
        ]);
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
        let status = classify(&[foreign(
            1,
            MIGRATIONS[0].name,
            &Checksum::of(b"edited in place").to_string(),
        )]);
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
    fn a_renamed_migration_is_reported_as_a_rename_even_when_its_text_matches() {
        let mut row = recorded(1);
        row.name = "control_plane_0001_initial_v2".to_owned();
        let status = classify(&[row]);
        assert_eq!(
            status,
            SchemaStatus::Renamed {
                version: 1,
                expected: MIGRATIONS[0].name,
                found: "control_plane_0001_initial_v2".to_owned(),
            }
        );
        assert!(
            !status.is_migratable(),
            "a version this build ships under another name is not a history it can extend"
        );
        assert!(status.to_string().contains("renumbered or renamed"));
    }

    /// A hole in the prefix is the failure `max(version)` alone cannot see: the
    /// maximum is right, so a version-count check would call the database current.
    #[test]
    fn a_hole_in_the_applied_prefix_is_incomplete_rather_than_current_or_behind() {
        let status = classify(&[foreign(
            2,
            "control_plane_0002_later",
            &Checksum::of(b"later").to_string(),
        )]);
        assert_eq!(
            status,
            SchemaStatus::Ahead {
                applied: 2,
                required: required_version()
            },
            "this build ships one migration, so v2 is a future version before it is a hole"
        );

        // With v2 shipped, the same ledger is a hole: v1 is missing.
        let versions = [2];
        let missing: Vec<i32> = (1..=2)
            .filter(|version| !versions.contains(version))
            .collect();
        let status = SchemaStatus::Incomplete {
            applied: 2,
            missing,
        };
        assert!(!status.is_migratable());
        assert!(!status.is_current());
        assert!(status.to_string().contains("missing v1"), "{status}");
    }

    #[test]
    fn a_ledger_that_is_not_this_ledger_is_malformed_rather_than_behind() {
        let duplicated = classify(&[recorded(1), recorded(1)]);
        assert!(
            matches!(duplicated, SchemaStatus::Malformed { .. }),
            "two rows for one version is not a history: {duplicated:?}"
        );
        assert!(!duplicated.is_migratable());
        let zeroed = classify(&[foreign(0, "control_plane_0000", "sha256:0")]);
        assert!(
            matches!(zeroed, SchemaStatus::Malformed { .. }),
            "versions start at 1: {zeroed:?}"
        );
        assert!(
            zeroed.to_string().contains("not the one this build writes"),
            "{zeroed}"
        );
    }

    /// An empty ledger is the one case where a table that exists still means
    /// "apply everything": the DDL may have been run without recording it.
    #[test]
    fn an_empty_ledger_is_behind_and_pending_names_every_shipped_version() {
        let status = classify(&[]);
        assert_eq!(
            status,
            SchemaStatus::Behind {
                applied: 0,
                required: required_version()
            }
        );
        assert_eq!(
            pending(&status),
            MIGRATIONS.iter().map(|m| m.version).collect::<Vec<_>>()
        );
        for refused in [
            SchemaStatus::Ahead {
                applied: 9,
                required: 1,
            },
            SchemaStatus::Drifted {
                version: 1,
                expected: MIGRATIONS[0].checksum(),
                found: Checksum::of(b"edited"),
            },
            SchemaStatus::Current { version: 1 },
        ] {
            assert!(
                pending(&refused).is_empty(),
                "nothing is pending against {refused:?}: an apply must not write there"
            );
        }
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
