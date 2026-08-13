//! Coverage-guided fuzzing of `gateway-core`'s SSE decoder: arbitrary bodies
//! split on arbitrary chunk boundaries, truncated final events, and a buffer
//! limit small enough to reach.
#![no_main]

use axond_fuzz::SseInput;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: SseInput<'_>| {
    axond_fuzz::sse_decode(&input);
});
