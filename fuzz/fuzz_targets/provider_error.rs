//! Coverage-guided fuzzing of `ProviderError::from_upstream` and the
//! classification behind it: which typed error a status and body become, and
//! what the diagnostic that comes with it may contain.
#![no_main]

use axond_fuzz::UpstreamFailure;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: UpstreamFailure<'_>| {
    axond_fuzz::provider_error(&input);
});
