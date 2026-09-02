//! The shipped DDL exists twice, and the two copies must never diverge.
//!
//! `ops/postgres/` is the operator contract: it is what the deployment docs,
//! the ADRs, and every `psql -f` in an operator's runbook point at, and ADR 0009
//! forbids editing a shipped file in place — a row-shape change is a new
//! `*_v<N>.sql`. That directory is outside `crates/gateway/`, so it cannot be
//! part of the `axond` package, and the `include_str!`ed DDL the gateway applies
//! with `create_table = true` therefore reads package-local copies under
//! `crates/gateway/sql/`.
//!
//! Two copies of an interface is a drift hazard: an operator applying
//! `ops/postgres/usage_v2.sql` by hand and a gateway applying its embedded copy
//! must produce the same table. This test is the gate. It fails on a file added
//! to one side only — including a future `budget_v2.sql` — and on any byte
//! difference, so `just fmt`-style forgetfulness cannot ship a split schema.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// `ops/postgres/`, the operator-facing location.
fn operator_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../ops/postgres")
}

/// `crates/gateway/sql/`, the copies the binary embeds and the package ships.
fn packaged_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("sql")
}

/// File name to contents, sorted, so a failure names the same file every run.
fn sql_files(directory: &Path) -> BTreeMap<String, Vec<u8>> {
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", directory.display()));
    entries
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "sql"))
        .map(|path| {
            let name = path
                .file_name()
                .expect("file name")
                .to_string_lossy()
                .into_owned();
            let contents = fs::read(&path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            (name, contents)
        })
        .collect()
}

#[test]
fn every_shipped_ddl_file_exists_in_both_locations() {
    let operator = sql_files(&operator_dir());
    let packaged = sql_files(&packaged_dir());

    assert!(
        !operator.is_empty(),
        "ops/postgres holds the shipped DDL; an empty directory means the operator contract was removed"
    );

    let missing_from_package: Vec<&String> = operator
        .keys()
        .filter(|name| !packaged.contains_key(*name))
        .collect();
    assert!(
        missing_from_package.is_empty(),
        "these ops/postgres files have no crates/gateway/sql copy, so the published axond package \
         cannot embed them: {missing_from_package:?}. Copy each one into crates/gateway/sql/."
    );

    let missing_from_operator: Vec<&String> = packaged
        .keys()
        .filter(|name| !operator.contains_key(*name))
        .collect();
    assert!(
        missing_from_operator.is_empty(),
        "these crates/gateway/sql files are not in ops/postgres, so operators applying the schema \
         by hand would never see them: {missing_from_operator:?}"
    );
}

#[test]
fn the_two_copies_of_each_shipped_ddl_file_are_byte_identical() {
    let operator = sql_files(&operator_dir());
    let packaged = sql_files(&packaged_dir());

    for (name, operator_contents) in &operator {
        let Some(packaged_contents) = packaged.get(name) else {
            continue; // Reported by the file-set test, with a better message.
        };
        assert!(
            operator_contents == packaged_contents,
            "ops/postgres/{name} and crates/gateway/sql/{name} differ. The gateway embeds the \
             second and operators apply the first, so they would build different tables. Copy \
             ops/postgres/{name} over crates/gateway/sql/{name}."
        );
    }
}

/// An index an operator applies is an index they keep paying for on every write,
/// so the shipped schema only carries indexes some statement actually plans
/// against. The secret store keys every read on its primary key and verifies the
/// owner columns the row returns, so it declares no owner index — whoever adds
/// one (an owner-scoped administrative listing is the plausible reason) has to
/// add the predicate that uses it, and updating this gate is the reminder.
#[test]
fn the_secret_store_declares_no_index_its_statements_would_not_plan_against() {
    let (name, contents) = sql_files(&operator_dir())
        .into_iter()
        .find(|(name, _)| name == "secret_store_v1.sql")
        .expect("the secret store's DDL is shipped");
    let text = String::from_utf8(contents).unwrap_or_else(|_| panic!("{name} is not UTF-8"));

    let declared: Vec<&str> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("--"))
        .filter(|line| line.to_ascii_uppercase().contains("CREATE INDEX"))
        .collect();

    assert!(
        declared.is_empty(),
        "ops/postgres/{name} declares {declared:?}. Every read keys on (secret_id, version) and \
         checks the owner columns it read, so an unused index only costs writes: either add the \
         statement that plans against it and say so in the header, or drop it."
    );
}

/// Store budget tables must not reuse the withdrawn `[budget]` Postgres
/// backend names. `CREATE TABLE IF NOT EXISTS axond_budget` would leave a
/// leftover `budget_v1.sql` ledger in place and fail `probe_schema`.
#[test]
fn store_budget_tables_do_not_reuse_withdrawn_budget_backend_names() {
    let (_, contents) = sql_files(&operator_dir())
        .into_iter()
        .find(|(name, _)| name == "store_budget_v1.sql")
        .expect("store budget DDL is shipped");
    let text =
        String::from_utf8(contents).unwrap_or_else(|_| panic!("store_budget_v1.sql is not UTF-8"));
    assert!(text.contains("CREATE TABLE IF NOT EXISTS axond_store_budget ("));
    assert!(text.contains("CREATE TABLE IF NOT EXISTS axond_store_budget_active ("));
    assert!(text.contains("CREATE TABLE IF NOT EXISTS axond_store_budget_reservation ("));
    assert!(text.contains("CREATE INDEX IF NOT EXISTS axond_store_budget_reservation_scope_idx"));
    assert!(!text.contains("CREATE TABLE IF NOT EXISTS axond_budget ("));
    assert!(!text.contains("CREATE TABLE IF NOT EXISTS axond_budget_active ("));
    assert!(!text.contains("CREATE TABLE IF NOT EXISTS axond_budget_reservation ("));
}

/// A shipped DDL header is an operator's route into the reasoning behind the
/// schema, and ADR 0009 forbids editing the file once it has been applied — so a
/// pointer at an ADR that does not exist (a number two branches both claimed, a
/// renamed file) is a mistake this gate has to catch before the file freezes.
#[test]
fn every_adr_a_shipped_ddl_file_cites_exists() {
    let adr_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/adr");

    for (name, contents) in sql_files(&operator_dir()) {
        let text = String::from_utf8(contents).unwrap_or_else(|_| panic!("{name} is not UTF-8"));
        for cited in text
            .split("docs/adr/")
            .skip(1)
            .filter_map(|rest| rest.split(|c: char| c.is_whitespace() || c == ')').next())
            .map(|cited| cited.trim_end_matches(['.', ',']))
        {
            assert!(
                adr_dir.join(cited).exists(),
                "ops/postgres/{name} cites docs/adr/{cited}, which does not exist"
            );
        }
    }
}
