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
