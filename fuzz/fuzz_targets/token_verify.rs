//! Coverage-guided fuzzing of minted-token verification: JWS decoding, key
//! selection, signature checking, and every claim check behind them.
#![no_main]

use axond_fuzz::TokenInput;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: TokenInput<'_>| {
    axond_fuzz::token_verify(&input);
});
