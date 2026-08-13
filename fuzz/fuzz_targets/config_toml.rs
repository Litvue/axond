//! Coverage-guided fuzzing of `Config::from_toml_str` and its validation.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    axond_fuzz::config_toml(data);
});
