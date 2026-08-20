//! Coverage-guided fuzzing of object-store publication documents: bounded,
//! signed canonical head JSON and signed deterministic revision-manifest CBOR.
//! The shared target reaches schema/algorithm/key selection and real Ed25519
//! verification under synthetic bootstrap trust as well as structural parsing.
//! Independently mutable expectations drive digest/sequence/environment/parent,
//! tuple-equivocation, active-revision matching, and final version fencing.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    axond_fuzz::publication_parsers(data);
});
