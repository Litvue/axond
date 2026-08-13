//! The fuzz seam is a second target over the binary's own module sources, and
//! the two module lists must never diverge.
//!
//! `src/main.rs` and `src/fuzz_seam.rs` each declare the crate's modules,
//! because a Rust target's root is the only place a module can be declared.
//! Cargo builds the library target only with the `fuzzing` feature on, so a
//! module added to the binary and forgotten here would break the fuzz project
//! rather than CI — long after the change. This test moves that failure to the
//! commit that causes it, and it needs neither the feature nor a build: both
//! lists are read as text.

use std::fs;
use std::path::PathBuf;

fn module_names(relative: &str) -> Vec<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative);
    let source = fs::read_to_string(&path).expect("target root is readable");
    let mut names: Vec<String> = source
        .lines()
        .zip(std::iter::once("").chain(source.lines()))
        .filter_map(|(line, previous)| {
            // Only top-level declarations of a *file* module: a nested `mod` is
            // indented, an inline `mod tests {` carries its own body rather than
            // a source file, and a `#[cfg(test)]` one belongs to whichever
            // target runs the tests rather than to the seam.
            if previous.trim_start().starts_with("#[cfg(test)]") {
                return None;
            }
            let rest = line.strip_prefix("mod ")?;
            Some(rest.strip_suffix(';')?.to_owned())
        })
        .collect();
    names.sort();
    assert!(!names.is_empty(), "{relative} declares no modules");
    names
}

#[test]
fn the_fuzz_seam_declares_every_module_the_binary_does() {
    let mut binary = module_names("src/main.rs");
    // Test support has no place in a target that carries no tests.
    binary.retain(|name| name != "test_services");
    assert_eq!(
        binary,
        module_names("src/fuzz_seam.rs"),
        "src/fuzz_seam.rs and src/main.rs declare different modules; \
         the fuzz seam only compiles when they match"
    );
}
