//! Coverage-guided fuzzing for ADR 0062 durable body decoding.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    axond_fuzz::flat_v2_body_target(data);
});
