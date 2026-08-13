//! Coverage-guided fuzzing of credential-status query parsing, including
//! malformed percent-encoding, duplicate keys, empty values, and oversized
//! inputs.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    axond_fuzz::credentials_query(data);
});
