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

use std::collections::HashSet;
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

    /// The tables this migration's own text declares, in declaration order.
    ///
    /// Read from the embedded SQL rather than listed beside it, so a migration
    /// that adds a table cannot forget to say so: the evidence adoption checks
    /// for is derived from the file adoption claims was applied.
    pub fn relations(&self) -> Vec<&'static str> {
        let mut relations = Vec::new();
        for statement in statements(self.sql) {
            if let Some(Statement::Table(name)) = statement_kind(statement)
                && !relations.contains(&name)
            {
                relations.push(name);
            }
        }
        relations
    }
}

/// What one statement of a migration did, when that is something a later
/// connection can be asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Statement {
    Table(&'static str),
    Index(&'static str),
    /// An `INSERT ... ON CONFLICT DO NOTHING`: a seed row, whose presence is
    /// checked by the target table not being empty. Idempotent by construction,
    /// which is what makes it checkable at all.
    Seed(&'static str),
}

/// The migration's text as statements, with comments and string literals ignored
/// while looking for the separators.
///
/// A `;` inside `'...'` is not a statement boundary and `--` starts a comment, so
/// a plain `split(';')` would both cut statements in half and find keywords in
/// prose. The slices point into the embedded SQL, so every name parsed out of one
/// is `'static`.
///
/// A chunk with no word outside its comments is not a statement and is dropped: a
/// file that ends with an explanatory comment, or that has a stray `;;`, is
/// otherwise read as a statement nothing can confirm, which would withdraw
/// adoption from the whole history over a comment.
fn statements(sql: &'static str) -> Vec<&'static str> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut commented = false;
    let bytes = sql.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        match byte {
            b'\n' if commented => commented = false,
            _ if commented => {}
            b'\'' => quoted = !quoted,
            _ if quoted => {}
            b'-' if bytes.get(index + 1) == Some(&b'-') => commented = true,
            b';' => {
                let statement = sql[start..index].trim();
                if !words(statement).is_empty() {
                    statements.push(statement);
                }
                start = index + 1;
            }
            _ => {}
        }
    }
    let tail = sql[start..].trim();
    if !words(tail).is_empty() {
        statements.push(tail);
    }
    statements
}

/// The words of a statement, with comments and string literals skipped.
fn words(statement: &'static str) -> Vec<&'static str> {
    let mut words = Vec::new();
    let mut start: Option<usize> = None;
    let mut quoted = false;
    let mut commented = false;
    let bytes = statement.as_bytes();
    for (index, byte) in bytes.iter().enumerate() {
        let word = byte.is_ascii_alphanumeric() || *byte == b'_';
        match byte {
            b'\n' if commented => commented = false,
            _ if commented => {}
            // A quote or a comment marker ends the word running up to it as much
            // as a space does: `EXISTS foo--why` names `foo`, and dropping the
            // word would make the next one answer for it.
            b'\'' => {
                if let Some(from) = start.take() {
                    words.push(&statement[from..index]);
                }
                quoted = !quoted;
            }
            _ if quoted => {}
            b'-' if bytes.get(index + 1) == Some(&b'-') => {
                if let Some(from) = start.take() {
                    words.push(&statement[from..index]);
                }
                commented = true;
            }
            _ if word => start = start.or(Some(index)),
            _ => {
                if let Some(from) = start.take() {
                    words.push(&statement[from..index]);
                }
            }
        }
    }
    if let Some(from) = start {
        words.push(&statement[from..]);
    }
    words
}

/// What a statement created, or `None` for one whose effect the catalogue cannot
/// be asked about — an `ALTER`, an `UPDATE`, a backfill, a non-idempotent
/// `INSERT`, a `DROP`.
fn statement_kind(statement: &'static str) -> Option<Statement> {
    let words = words(statement);
    let keyword = |position: usize, expected: &str| {
        words
            .get(position)
            .is_some_and(|word| word.eq_ignore_ascii_case(expected))
    };
    /// Past `IF NOT EXISTS` and `CONCURRENTLY`, to the object's own name.
    fn name(words: &[&'static str], mut at: usize) -> Option<&'static str> {
        for skipped in ["CONCURRENTLY", "IF", "NOT", "EXISTS"] {
            if words
                .get(at)
                .is_some_and(|word| word.eq_ignore_ascii_case(skipped))
            {
                at += 1;
            }
        }
        words.get(at).copied()
    }
    if keyword(0, "CREATE") && keyword(1, "TABLE") {
        return name(&words, 2).map(Statement::Table);
    }
    if keyword(0, "CREATE") && keyword(1, "INDEX") {
        return name(&words, 2).map(Statement::Index);
    }
    if keyword(0, "CREATE") && keyword(1, "UNIQUE") && keyword(2, "INDEX") {
        return name(&words, 3).map(Statement::Index);
    }
    if keyword(0, "INSERT") && keyword(1, "INTO") {
        // Only the idempotent form: a plain `INSERT` cannot be told apart from
        // one that never ran, and re-running it would double the rows.
        let idempotent = words.windows(2).any(|pair| {
            pair[0].eq_ignore_ascii_case("DO") && pair[1].eq_ignore_ascii_case("NOTHING")
        });
        return words
            .get(2)
            .copied()
            .filter(|_| idempotent)
            .map(Statement::Seed);
    }
    None
}

/// Every migration this build ships, in application order.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "control_plane_0001_initial",
        sql: include_str!("../../../sql/control_plane_0001_initial.sql"),
    },
    Migration {
        version: 2,
        name: "control_plane_0002_tenancy_access",
        sql: include_str!("../../../sql/control_plane_0002_tenancy_access.sql"),
    },
    Migration {
        version: 3,
        name: "control_plane_0003_tenancy_constraints",
        sql: include_str!("../../../sql/control_plane_0003_tenancy_constraints.sql"),
    },
];

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
    /// The ledger exists and records nothing.
    ///
    /// Not migratable, and not the same thing as [`SchemaStatus::Absent`]: an
    /// empty ledger is indistinguishable from a database whose objects were
    /// created by hand — the ledger is the only record of what was applied, so
    /// with no rows this build cannot tell an untouched database from a fully
    /// populated one. Migrating from 0 would re-run every shipped file over
    /// objects that may already exist, which survives only as long as every
    /// statement is `IF NOT EXISTS`; the first `ALTER TABLE` or backfill would
    /// double-apply. The baseline is adopted deliberately instead —
    /// [`baseline`] reconciles it against the objects the database actually
    /// holds, and `axond migrate adopt` is the operator command that records it.
    Unrecorded,
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
    /// True only for [`SchemaStatus::Absent`] and [`SchemaStatus::Behind`], both
    /// of which say what the database already contains: nothing, or a recorded
    /// prefix. Every other status means the database is not this schema's history, and
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
            Self::Unrecorded => write!(
                f,
                "`{MIGRATION_TABLE}` exists but records no migrations, so this build cannot tell \
                 whether the schema it describes was ever applied and will not migrate from zero \
                 over objects that may already exist; if the DDL was applied out of band, run \
                 `axond migrate adopt` to record the baseline the database's own objects account \
                 for, and if nothing was applied, drop the empty `{MIGRATION_TABLE}` table and \
                 run `axond migrate apply`"
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
pub(crate) const MIGRATION_TABLE: &str = "axond_cp_schema_migration";

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
    //
    // Only errors that say *that*, though. Every server-reported error carries a
    // SQLSTATE, including `57014 query_canceled` and `40001 serialization_failure`,
    // and calling a cancelled statement a broken schema would tell an operator to
    // go and fix a history that is fine — and would strip the retryable
    // classification the error type exists to carry. Class 42 (syntax and access
    // rules: undefined table, undefined column, insufficient privilege) is the
    // class that means the name is not this build's ledger.
    let rows = match transaction
        .query(
            &format!("SELECT version, name, checksum FROM {MIGRATION_TABLE} ORDER BY version"),
            &[],
        )
        .await
    {
        Ok(rows) => rows,
        Err(error) if is_schema_disagreement(&error) => {
            return Ok(SchemaStatus::Malformed {
                message: format!(
                    "reading `{MIGRATION_TABLE}` as (version, name, checksum) failed: {error}"
                ),
            });
        }
        Err(error) => return Err(error),
    };
    // Decoded fallibly for the same reason: a table that answers to those three
    // column *names* with other types (`version text`, `checksum bytea`) makes the
    // query succeed, and `Row::get` would panic on it. That is the documented
    // `Malformed` case — a version that is not a version — not a crash.
    let mut recorded = Vec::with_capacity(rows.len());
    for row in &rows {
        let decoded = row
            .try_get(0)
            .and_then(|version| {
                Ok(Recorded {
                    version,
                    name: row.try_get(1)?,
                    checksum: row.try_get(2)?,
                })
            })
            .map_err(|error| format!("`{MIGRATION_TABLE}` holds a row this build cannot read as (version integer, name text, checksum text): {error}"));
        match decoded {
            Ok(row) => recorded.push(row),
            Err(message) => return Ok(SchemaStatus::Malformed { message }),
        }
    }
    Ok(classify(&recorded))
}

/// Whether a failed ledger read means the table is not this build's ledger, as
/// opposed to a database that had a bad moment.
///
/// SQLSTATE class 42 is "syntax error or access rule violation": `42P01`
/// undefined table, `42703` undefined column, `42501` insufficient privilege.
/// Anything else — a cancelled statement, a serialization failure, a deadlock —
/// stays an error, so it keeps its retryable classification.
fn is_schema_disagreement(error: &tokio_postgres::Error) -> bool {
    error
        .code()
        .is_some_and(|code| code.code().starts_with("42"))
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
        // The table exists and is empty, which is not the same question as
        // "has anything been applied?". It is what a database whose DDL was
        // applied by hand looks like, and also what a database with a
        // hand-created ledger and nothing else looks like, and the ledger is
        // the only thing that could tell them apart. Migrating from 0 would
        // replay every file over whatever is there.
        return SchemaStatus::Unrecorded;
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
        | SchemaStatus::Unrecorded
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

/// What recording a baseline into an empty ledger would be asserting.
///
/// Adoption exists for one database: a [`SchemaStatus::Unrecorded`] ledger, which
/// is what applying the shipped DDL with `psql` leaves behind. The ledger cannot
/// answer "was this applied?", so the objects are asked instead — and the answer
/// is only ever a *prefix* of the shipped history, because that is the only shape
/// a forward-only sequence of files can produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Baseline {
    /// Every table versions `1..=n` declare is present, so recording them states
    /// what the database already contains rather than guessing it.
    Applied { versions: Vec<i32> },
    /// No shipped migration's tables are there: nothing was applied out of band,
    /// so there is no baseline to adopt and the empty ledger is the only thing
    /// standing between this database and an ordinary `apply`.
    Nothing,
    /// No baseline is adoptable: a migration is half applied, a later one's tables
    /// exist without an earlier one's, or the history contains a migration that
    /// creates no table and so cannot be reconciled against objects at all. Never
    /// adopted — recording a version whose objects are incomplete would promise a
    /// schema the database does not have, and recording a prefix under an
    /// unobservable version would leave `apply` to re-run it.
    Inconsistent { message: String },
}

/// One thing a migration did that a later connection can be asked to confirm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Evidence {
    /// A table or index the migration creates, present in this schema.
    Relation(&'static str),
    /// A seed row an idempotent `INSERT ... ON CONFLICT DO NOTHING` writes,
    /// confirmed by the target table not being empty. A migration whose tables
    /// exist and whose seed row does not is a `psql` run that stopped in the
    /// middle, which is why the row counts as evidence rather than as detail.
    Seed(&'static str),
}

/// Everything a migration must be able to show for itself to be adoptable, or
/// `None` when the file contains a statement whose effect cannot be confirmed.
///
/// Derived from the migration's own text, so a statement cannot ship without
/// adoption accounting for it — and the accounting is deliberately total: an
/// `ALTER`, a backfill, a `DROP`, or a plain `INSERT` makes the whole migration
/// unconfirmable rather than being passed over, because "every table is there"
/// says nothing about a column or a row.
///
/// The ledger table is excluded: adoption only runs against a database whose
/// ledger exists, so its presence is the precondition rather than evidence.
/// Counting it would make a bare ledger look half-applied rather than untouched.
fn evidence(migration: &Migration) -> Option<Vec<Evidence>> {
    let mut evidence = Vec::new();
    for statement in statements(migration.sql) {
        let item = match statement_kind(statement)? {
            Statement::Table(name) | Statement::Index(name) => Evidence::Relation(name),
            Statement::Seed(name) => Evidence::Seed(name),
        };
        // Relations de-duplicate safely: the same object declared twice is one
        // probe with one answer. A seed does not — the target having a row confirms
        // one insert, not two — so a repeated seed stays in the list, where the
        // shared-seed refusal can see that this table is seeded more than once.
        let duplicate = match item {
            Evidence::Relation(_) => evidence.contains(&item),
            Evidence::Seed(_) => false,
        };
        if item == Evidence::Relation(MIGRATION_TABLE) || duplicate {
            continue;
        }
        evidence.push(item);
    }
    Some(evidence)
}

/// Reconcile an empty ledger against what the database can show.
///
/// Read-only: this is evidence gathering, and the caller decides what to do with
/// it. Deliberately strict — a version is adoptable only when *everything* it
/// declares is confirmed, and a version confirmed after one that is not is a
/// refusal rather than a hole to paper over.
pub(super) async fn baseline(
    transaction: &Transaction<'_>,
) -> Result<Baseline, tokio_postgres::Error> {
    let mut confirmed: HashSet<Evidence> = HashSet::new();
    for item in MIGRATIONS.iter().filter_map(evidence).flatten() {
        // Every probe is qualified to the one schema this connection writes in —
        // the schema `[control_plane] schema` selected, or the first on the DSN's
        // own search path, which is where `apply` would have created these
        // objects. An unqualified probe would resolve down the whole search path,
        // so another install's journal sitting in `public` would be read as
        // evidence that *this* schema's DDL was applied.
        let found: bool = match item {
            Evidence::Relation(name) => transaction
                .query_one(
                    "SELECT EXISTS (\
                       SELECT 1 FROM pg_catalog.pg_class class \
                         JOIN pg_catalog.pg_namespace namespace \
                           ON namespace.oid = class.relnamespace \
                        WHERE class.relname = $1 \
                          AND class.relkind IN ('r', 'p', 'i', 'I') \
                          AND namespace.nspname = current_schema())",
                    &[&name],
                )
                .await?
                .get(0),
            // The table is confirmed by its own `Relation` probe first, and a
            // table in `current_schema()` shadows one of the same name further
            // down the path, so the row this finds is this schema's. No table
            // means no seed row either.
            Evidence::Seed(name) => {
                if !confirmed.contains(&Evidence::Relation(name)) {
                    false
                } else {
                    transaction
                        .query_one(&format!("SELECT EXISTS (SELECT 1 FROM {name})"), &[])
                        .await?
                        .get(0)
                }
            }
        };
        if found {
            confirmed.insert(item);
        }
    }
    Ok(reconcile(MIGRATIONS, &confirmed))
}

/// The prefix a set of confirmed objects and rows accounts for, and nothing more.
///
/// Separated from the probing so the shape of the history — a whole version, a
/// half-applied one, a hole, a version nothing can confirm — is decided by a
/// function that can be examined directly.
fn reconcile(migrations: &[Migration], confirmed: &HashSet<Evidence>) -> Baseline {
    // A seed is confirmed by its target not being empty, which is one answer for
    // the whole table: two migrations seeding the same one would each be confirmed
    // by whichever row is there, so a database that only ever had the first applied
    // would have the second recorded and `apply` would never write its row. The
    // second seed into a table is therefore evidence of nothing, and treated like
    // any other statement adoption cannot confirm.
    let seeded: Vec<&'static str> = migrations
        .iter()
        .filter_map(evidence)
        .flatten()
        .filter_map(|item| match item {
            Evidence::Seed(name) => Some(name),
            Evidence::Relation(_) => None,
        })
        .collect();
    let mut adoptable: Vec<i32> = Vec::new();
    let mut absent: Option<i32> = None;
    for migration in migrations {
        // A migration containing a statement whose effect nothing can be asked
        // about — an `ALTER TABLE`, a backfill, a `DROP`, a non-idempotent
        // `INSERT` — blocks adoption of this database wherever in the history it
        // sits, including the versions below it.
        //
        // Fail-closed on purpose, in both directions. Recording it on the strength
        // of the objects it happens to create would claim a column or a row that
        // may never have been written; recording only the prefix underneath would
        // have the ledger call it pending, so the next `apply` would run it over a
        // database that may already have had it applied out of band. That rerun is
        // precisely the non-idempotent replay adoption exists to prevent, and no
        // ledger row both accounts for the objects and keeps `apply` away.
        let Some(declared) = evidence(migration).filter(|declared| !declared.is_empty()) else {
            return Baseline::Inconsistent {
                message: format!(
                    "v{} `{}` contains a statement whose effect this database cannot be asked \
                     about, so whether it was applied is not something adoption can confirm — and \
                     recording a baseline below it would leave `axond migrate apply` to re-run it \
                     over a schema that may already have it. No baseline is adoptable while it \
                     ships unrecorded: state the history with `INSERT INTO {MIGRATION_TABLE} \
                     (version, name, checksum)` if you own the change that applied it, or drop the \
                     empty ledger and apply from zero if nothing was.",
                    migration.version, migration.name,
                ),
            };
        };
        if let Some(shared) = declared.iter().find_map(|item| match item {
            Evidence::Seed(name) if seeded.iter().filter(|target| *target == name).count() > 1 => {
                Some(*name)
            }
            _ => None,
        }) {
            return Baseline::Inconsistent {
                message: format!(
                    "v{} `{}` seeds `{shared}`, which the shipped history seeds more than once, \
                     so a row in it proves at most one of those inserts and not this one: whether \
                     this version ran is not something adoption can confirm. No baseline is \
                     adoptable while they all ship: state the history with `INSERT INTO \
                     {MIGRATION_TABLE} (version, name, checksum)` if you own the change that \
                     applied it, or drop the empty ledger and apply from zero if nothing was.",
                    migration.version, migration.name,
                ),
            };
        }
        // Named by what is actually wrong with each one: a table that is not there
        // and a table that is there without its seed row are different repairs, and
        // an operator told "`axond_cp_head` is not present" about a table that
        // exists would go looking for the wrong thing.
        let missing: Vec<String> = declared
            .iter()
            .filter(|item| !confirmed.contains(item))
            .map(|item| match item {
                Evidence::Relation(name) => format!("`{name}` is not present"),
                Evidence::Seed(name) => format!("`{name}` has no seeded row"),
            })
            .collect();
        if !missing.is_empty() && missing.len() < declared.len() {
            return Baseline::Inconsistent {
                message: format!(
                    "v{} `{}` is only partly applied: {}, so this build cannot record it as \
                     applied and cannot apply it over what is there either. Finish or undo that \
                     migration by hand, then re-run.",
                    migration.version,
                    migration.name,
                    missing.join(", "),
                ),
            };
        }
        match (missing.is_empty(), absent) {
            // Still extending the prefix of versions the database can account for.
            (true, None) => adoptable.push(migration.version),
            // Objects for a version above one that is absent: the database is
            // not any prefix of this history, so nothing about it can be
            // recorded as a baseline.
            (true, Some(hole)) => {
                return Baseline::Inconsistent {
                    message: format!(
                        "v{} `{}` declares objects that are present while v{hole} declares objects \
                         that are not; this database is not a prefix of the shipped migration \
                         history, so no baseline describes it",
                        migration.version, migration.name,
                    ),
                };
            }
            (false, _) => absent = absent.or(Some(migration.version)),
        }
    }
    if adoptable.is_empty() {
        return Baseline::Nothing;
    }
    Baseline::Applied {
        versions: adoptable,
    }
}

/// Record an adopted baseline: the versions whose objects are already there.
///
/// The same rows [`migrate`] writes, with the same checksums, so a database that
/// was adopted and one that was migrated are afterwards the same database as far
/// as every other classification is concerned. `ON CONFLICT DO NOTHING` for the
/// reason `migrate` has it: the caller holds the advisory lock, and a row that
/// appeared anyway is not one to overwrite.
pub(super) async fn record_baseline(
    transaction: &Transaction<'_>,
    versions: &[i32],
) -> Result<(), tokio_postgres::Error> {
    for migration in MIGRATIONS
        .iter()
        .filter(|migration| versions.contains(&migration.version))
    {
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
    fn an_unrecorded_or_partial_history_is_not_current_and_a_complete_one_is() {
        assert_eq!(classify(&[]), SchemaStatus::Unrecorded);
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
        // A version past the newest this build ships is a future version before
        // it is a hole: the ledger is ahead, and this build cannot judge what it
        // has never seen.
        let beyond = required_version() + 1;
        let status = classify(&[foreign(
            beyond,
            "control_plane_9999_later",
            &Checksum::of(b"later").to_string(),
        )]);
        assert_eq!(
            status,
            SchemaStatus::Ahead {
                applied: beyond,
                required: required_version()
            },
        );

        // Within the shipped range, the same shape of ledger is a hole: the
        // newest is right, and an earlier version never ran.
        let applied = required_version();
        let missing: Vec<i32> = (1..applied).collect();
        let status = SchemaStatus::Incomplete { applied, missing };
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

    /// An empty ledger is not "apply everything": the files it would replay may
    /// already be in the database, and the ledger is the only thing that could
    /// have said so. An *absent* ledger is "apply everything" — nothing has run.
    #[test]
    fn an_empty_ledger_is_refused_while_an_absent_one_pends_every_shipped_version() {
        let empty = classify(&[]);
        assert_eq!(empty, SchemaStatus::Unrecorded);
        assert!(
            !empty.is_migratable() && !empty.is_current(),
            "an empty ledger must not be migrated from zero: {empty:?}"
        );
        assert!(
            pending(&empty).is_empty(),
            "nothing is pending against an empty ledger, or an apply would replay every file"
        );
        let rendered = empty.to_string();
        for expected in [
            "records no migrations",
            "drop the empty",
            "axond migrate adopt",
            "axond migrate apply",
        ] {
            assert!(
                rendered.contains(expected),
                "the refusal has to name the action to take, missing `{expected}`: {rendered}"
            );
        }

        assert_eq!(
            pending(&SchemaStatus::Absent),
            MIGRATIONS.iter().map(|m| m.version).collect::<Vec<_>>()
        );
        for refused in [
            SchemaStatus::Unrecorded,
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

    /// The evidence adoption reconciles an empty ledger against is read out of the
    /// migration's own text, so a migration that adds a table cannot ship without
    /// adoption knowing to look for it. A parse that silently found nothing would
    /// turn every adoption into an unchecked assertion, which is exactly the
    /// failure the operation exists to prevent — so the count is asserted too.
    #[test]
    fn a_migrations_declared_tables_are_read_out_of_the_shipped_ddl() {
        let declared = MIGRATIONS[0].relations();
        assert_eq!(
            declared,
            vec![
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
            ],
            "the tables adoption looks for are the ones the shipped file creates"
        );
        // Every statement of the shipped file is accounted for — its indexes and
        // its one idempotent seed row as well as its tables — and the ledger is
        // excluded, being an adoption's precondition rather than evidence for it.
        // A statement adoption cannot confirm withdraws `adopt` outright, so this
        // assertion is where that release decision surfaces.
        assert_eq!(
            evidence(&MIGRATIONS[0]),
            Some(vec![
                Evidence::Relation("axond_cp_blob"),
                Evidence::Relation("axond_cp_resource_version"),
                Evidence::Relation("axond_cp_resource_version_tenant_idx"),
                Evidence::Relation("axond_cp_resource_dependency"),
                Evidence::Relation("axond_cp_mutation"),
                Evidence::Relation("axond_cp_revision"),
                Evidence::Relation("axond_cp_revision_single_root_idx"),
                Evidence::Relation("axond_cp_revision_entry"),
                Evidence::Relation("axond_cp_revision_blob"),
                Evidence::Relation("axond_cp_audit_event"),
                Evidence::Relation("axond_cp_audit_event_revision_idx"),
                Evidence::Relation("axond_cp_idempotency"),
                Evidence::Relation("axond_cp_idempotency_expires_at_idx"),
                Evidence::Relation("axond_cp_head"),
                Evidence::Seed("axond_cp_head"),
            ]),
            "every statement of the shipped file has to be something adoption confirms"
        );
        for migration in MIGRATIONS.iter() {
            assert!(
                evidence(migration).is_some_and(|declared| !declared.is_empty()),
                "v{} contains a statement adoption cannot confirm, so no database can be adopted \
                 while it ships",
                migration.version
            );
        }
    }

    /// Statement kinds, and what each one leaves for adoption to check. The mixed
    /// case is the one worth pinning: a migration that creates a table *and* alters
    /// another must not be adoptable on the strength of the table, because a `psql`
    /// run that stopped between the two leaves exactly that catalogue.
    #[test]
    fn a_statement_whose_effect_cannot_be_confirmed_makes_its_migration_unadoptable() {
        const CONFIRMABLE: Migration = Migration {
            version: 1,
            name: "confirmable",
            sql: "CREATE TABLE IF NOT EXISTS first (id integer);\n\
                  -- A comment mentioning ALTER TABLE and a ';' should not matter.\n\
                  CREATE UNIQUE INDEX IF NOT EXISTS first_id ON first ((id IS NULL));\n\
                  INSERT INTO first (id) VALUES (1) ON CONFLICT (id) DO NOTHING;\n",
        };
        assert_eq!(
            evidence(&CONFIRMABLE),
            Some(vec![
                Evidence::Relation("first"),
                Evidence::Relation("first_id"),
                Evidence::Seed("first"),
            ])
        );

        for sql in [
            // A table beside an `ALTER`: the reviewed mixed case.
            "CREATE TABLE IF NOT EXISTS first (id integer);\nALTER TABLE second ADD COLUMN n text;\n",
            // A backfill beside a table.
            "CREATE TABLE IF NOT EXISTS first (id integer);\nUPDATE second SET n = 1;\n",
            // An `INSERT` that is not idempotent: indistinguishable from one that
            // never ran, and doubling on a rerun.
            "CREATE TABLE IF NOT EXISTS first (id integer);\nINSERT INTO first (id) VALUES (1);\n",
            "DROP TABLE second;\n",
        ] {
            let migration = Migration {
                version: 2,
                name: "mixed",
                sql,
            };
            assert_eq!(
                evidence(&migration),
                None,
                "a statement whose effect nothing can confirm must void the whole migration: {sql}"
            );
        }

        // Prose is not a statement. A file that ends with an explanation, or that
        // has a stray separator, would otherwise withdraw adoption from the entire
        // history over a comment.
        const COMMENTED: Migration = Migration {
            version: 3,
            name: "commented",
            sql: "CREATE TABLE IF NOT EXISTS first (id integer);;\n\
                  -- Why this table exists, after the last statement.\n\
                  -- And a second line of it.\n",
        };
        assert_eq!(
            evidence(&COMMENTED),
            Some(vec![Evidence::Relation("first")])
        );

        // A word ending against a quote or a `--` is still that word. Losing it
        // would have the following one answer for it: the table below would be
        // looked for under the name of its column, and the seed would read as a
        // plain `INSERT` and withdraw adoption from the whole history.
        const TIGHT: Migration = Migration {
            version: 4,
            name: "tight",
            sql: "CREATE TABLE IF NOT EXISTS second--the only row holder\n\
                  (id integer PRIMARY KEY, note text);\n\
                  INSERT INTO second (id, note) VALUES (1, 'only')\n\
                  ON CONFLICT (id) DO NOTHING--idempotent by construction\n\
                  ;\n",
        };
        assert_eq!(
            evidence(&TIGHT),
            Some(vec![Evidence::Relation("second"), Evidence::Seed("second")])
        );
    }

    /// `CREATE TABLE` with and without `IF NOT EXISTS`, a name followed by a
    /// newline rather than a paren, and an index or an `ALTER` that creates no
    /// table: the parse has to be the file's tables and nothing else, because a
    /// name it invents is a table adoption looks for and never finds.
    #[test]
    fn declared_tables_are_parsed_from_either_create_form_and_nothing_else() {
        const MIXED: Migration = Migration {
            version: 7,
            name: "fixture",
            sql: "CREATE TABLE IF NOT EXISTS first (id integer);\n\
                  CREATE TABLE second\n(id integer);\n\
                  CREATE INDEX IF NOT EXISTS second_id ON second (id);\n\
                  ALTER TABLE first ADD COLUMN note text;\n\
                  CREATE TABLE IF NOT EXISTS first (id integer);\n",
        };
        assert_eq!(MIXED.relations(), vec!["first", "second"]);
    }

    /// A migration that creates no table — an `ALTER`-only or backfill migration,
    /// the first non-idempotent kind — blocks adoption of the whole database,
    /// wherever in the shipped history it sits. Adopting the prefix underneath it
    /// would look safe and would not be: the ledger would then report the opaque
    /// version as pending, and the next `apply` would run it over a schema that may
    /// already have had it applied out of band, which is exactly the replay
    /// adoption exists to prevent.
    #[test]
    fn a_migration_no_object_can_account_for_blocks_adoption_of_the_whole_history() {
        const V1: Migration = Migration {
            version: 1,
            name: "first",
            sql: "CREATE TABLE IF NOT EXISTS axond_cp_schema_migration (version integer);\n\
                  CREATE TABLE IF NOT EXISTS one (id integer);\n",
        };
        const V2: Migration = Migration {
            version: 2,
            name: "backfill",
            sql: "ALTER TABLE one ADD COLUMN note text;\n",
        };
        const V3: Migration = Migration {
            version: 3,
            name: "third",
            sql: "CREATE TABLE IF NOT EXISTS three (id integer);\n",
        };
        let shipped = &[V1, V2, V3];
        let one = Evidence::Relation("one");
        let three = Evidence::Relation("three");

        // Every state of such a database refuses, including the one where the
        // prefix below the opaque migration is entirely accounted for.
        for confirmed in [
            HashSet::from([one]),
            HashSet::from([one, three]),
            HashSet::from([three]),
            HashSet::new(),
        ] {
            let Baseline::Inconsistent { message } = reconcile(shipped, &confirmed) else {
                panic!("a history with an unconfirmable migration has no adoptable baseline");
            };
            assert!(
                message.contains("v2 `backfill` contains a statement")
                    && message.contains("re-run it"),
                "the refusal has to name the version and why nothing under it is safe: {message}"
            );
        }

        // The same reconciliation without that migration still adopts the prefix
        // its objects prove, so the refusals above are the opaque version's doing
        // rather than a blanket one.
        let confirmable = &[V1, V3];
        assert_eq!(
            reconcile(confirmable, &HashSet::from([one])),
            Baseline::Applied { versions: vec![1] }
        );
        assert_eq!(reconcile(confirmable, &HashSet::new()), Baseline::Nothing);
        let Baseline::Inconsistent { message } = reconcile(confirmable, &HashSet::from([three]))
        else {
            panic!("a hole in the applied prefix is not a baseline");
        };
        assert!(
            message.contains("not a prefix"),
            "the refusal has to say why the objects describe no baseline: {message}"
        );
    }

    /// A seed is confirmed by its target having a row, which is one answer for the
    /// whole table. Two migrations seeding the same table would both read as
    /// applied off whichever row is there, so a database that only ever had the
    /// first would have the second recorded and `apply` would never write its row —
    /// the fail-open direction, and the one this refuses.
    #[test]
    fn a_second_seed_into_an_already_seeded_table_blocks_adoption() {
        const V1: Migration = Migration {
            version: 1,
            name: "first",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n\
                  INSERT INTO one (id) VALUES (1) ON CONFLICT (id) DO NOTHING;\n",
        };
        const V2: Migration = Migration {
            version: 2,
            name: "second seed",
            sql: "INSERT INTO one (id) VALUES (2) ON CONFLICT (id) DO NOTHING;\n",
        };
        let one = Evidence::Relation("one");
        let seeded = Evidence::Seed("one");

        // Including the state where the table has a row: that row is v1's, and
        // nothing here can tell whether v2's is beside it.
        for confirmed in [
            HashSet::from([one, seeded]),
            HashSet::from([one]),
            HashSet::new(),
        ] {
            let Baseline::Inconsistent { message } = reconcile(&[V1, V2], &confirmed) else {
                panic!("a seed no row can be attributed to has no adoptable baseline");
            };
            assert!(
                message.contains("which the shipped history seeds more than once"),
                "the refusal has to say why a row in it proves nothing: {message}"
            );
        }

        // The same table seeded twice inside one file, which a `psql -f` can stop
        // between: the row that is there is the first insert's, so the second is
        // unprovable for exactly the same reason and must not be de-duplicated
        // away before the refusal can see it.
        const TWICE: Migration = Migration {
            version: 1,
            name: "twice",
            sql: "CREATE TABLE IF NOT EXISTS one (id integer);\n\
                  INSERT INTO one (id) VALUES (1) ON CONFLICT (id) DO NOTHING;\n\
                  INSERT INTO one (id) VALUES (2) ON CONFLICT (id) DO NOTHING;\n",
        };
        assert_eq!(
            evidence(&TWICE),
            Some(vec![one, seeded, seeded]),
            "a repeated seed is two expectations, unlike a relation declared twice"
        );
        let Baseline::Inconsistent { message } = reconcile(&[TWICE], &HashSet::from([one, seeded]))
        else {
            panic!("a file seeding one table twice has no adoptable baseline");
        };
        assert!(
            message.contains("which the shipped history seeds more than once"),
            "the refusal has to say why a row in it proves nothing: {message}"
        );

        // One migration seeding it once is still evidence, so the refusals above
        // are the second seed's doing rather than a withdrawal of seed evidence.
        assert_eq!(
            reconcile(&[V1], &HashSet::from([one, seeded])),
            Baseline::Applied { versions: vec![1] }
        );
    }
}
