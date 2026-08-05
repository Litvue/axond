//! Black-box test harness: a fake provider upstream plus a real `axond`
//! process configured to talk to it (ADR 0014).
//!
//! Every suite that uses it is hermetic — no network, no secrets, no provider
//! account — so a wire-fidelity regression fails deterministically in CI.

// Each test binary compiles the whole harness and uses part of it.
#![allow(dead_code, unused_imports)]

pub mod gateway;
pub mod upstream;

use std::time::Duration;

pub use gateway::{Axond, GATEWAY_KEY, alias};
pub use upstream::{FakeUpstream, target};

/// Boot a fake upstream and a gateway wired to it.
pub async fn boot() -> (FakeUpstream, Axond) {
    let upstream = FakeUpstream::start().await;
    let gateway = Axond::start(&upstream.base_url).await;
    (upstream, gateway)
}

/// A client with no timeout, because the soak holds streams open deliberately.
pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .expect("test client builds")
}
