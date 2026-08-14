//! Coverage-guided fuzzing of the models.dev catalogue import: decoding, schema
//! validation, normalization, content identity, semantic classification, and
//! admission over a last-known-good catalogue.
#![no_main]

use axond_fuzz::CatalogInput;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: CatalogInput<'_>| {
    axond_fuzz::catalog_import(&input);
});
