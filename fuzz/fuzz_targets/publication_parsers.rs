//! Coverage-guided fuzzing of object-store publication documents: bounded
//! canonical head JSON and deterministic immutable revision-manifest CBOR.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    axond_fuzz::publication_parsers(data);
});
