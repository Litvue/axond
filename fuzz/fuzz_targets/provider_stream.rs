//! Coverage-guided fuzzing of the provider stream decoders: OpenAI chat and
//! Responses, Azure AI Foundry, Anthropic translated into OpenAI chunks, and a
//! native Anthropic relay.
#![no_main]

use axond_fuzz::ProviderStreamInput;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: ProviderStreamInput<'_>| {
    axond_fuzz::provider_stream(&input);
});
