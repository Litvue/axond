//! Black-box test harness: a fake provider upstream plus a real `axond`
//! process configured to talk to it (ADR 0014).
//!
//! Every suite that uses it is hermetic — no network, no secrets, no provider
//! account — so a wire-fidelity regression fails deterministically in CI.

// Each test binary compiles the whole harness and uses part of it.
#![allow(dead_code, unused_imports)]

pub mod capacity;
pub mod endurance;
pub mod fault;
pub mod gateway;
pub mod oidc;
pub mod packet;
pub mod recovery;
pub mod rollout;
pub mod schema;
pub mod stateful;
pub mod stateful_endurance;
pub mod tenancy;
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

/// Boot a fake upstream and a gateway whose `[failover]`/`[transport]` bounds
/// come from `tuning`.
pub async fn boot_with(tuning: &str) -> (FakeUpstream, Axond) {
    let upstream = FakeUpstream::start().await;
    let gateway = Axond::start_with(&upstream.base_url, tuning).await;
    (upstream, gateway)
}

/// A client with no whole-request deadline: the soak holds streams open
/// deliberately, and a deadline would cut one off and look like a client
/// hang-up. Connecting is still bounded, since that cannot legitimately hang.
pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("test client builds")
}
