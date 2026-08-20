//! Coverage-guided namespace secret sealing, opening, and rotation invariants.
#![no_main]

use axond_fuzz::BlobSecretCryptoInput;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: BlobSecretCryptoInput<'_>| {
    axond_fuzz::blob_secret_crypto(&input);
});
