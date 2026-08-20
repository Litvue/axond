//! Coverage-guided parsing of namespace-native immutable secret envelopes.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    axond_fuzz::blob_secret_envelope(input);
});
