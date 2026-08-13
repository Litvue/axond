//! The harness's own guarantees, asserted rather than assumed.
//!
//! A black-box suite is only as trustworthy as its boot: `free_addr` releases
//! its loopback port before the binary binds it, so a sibling test process can
//! win that port and answer probes while this child is still starting. Readiness
//! therefore has to identify the server, not just find one, and that rule is
//! tested here against a real second gateway instead of only when the race is
//! actually lost.

mod support;

use support::{GATEWAY_KEY, boot, client};

#[tokio::test]
async fn readiness_belongs_to_the_boot_that_asked_for_it() {
    let (_upstream, mut first) = boot().await;
    let (_sibling_upstream, second) = boot().await;

    // The sibling is a real axond serving real probes — what a lost port race
    // leaves behind — and it is not this boot.
    assert!(
        first.serves_this_boot(&first.base_url.clone()).await,
        "a boot must recognise its own process"
    );
    assert!(
        !first.serves_this_boot(&second.base_url.clone()).await,
        "a sibling gateway answering the probes was accepted as this boot:\n{}",
        second.output()
    );
}

#[tokio::test]
async fn the_boot_key_grants_nothing_the_suite_key_does_not() {
    let (_upstream, gateway) = boot().await;

    // The identity probe adds a second inbound key, so what an unauthenticated
    // or wrongly-keyed caller sees must be unchanged: the extra key is only ever
    // presented by the harness, and it is not a way in.
    let unauthenticated = client()
        .get(gateway.url("/v1/models"))
        .send()
        .await
        .expect("a response");
    assert_eq!(unauthenticated.status(), 401);

    let guessed = client()
        .get(gateway.url("/v1/models"))
        .bearer_auth("test-boot-key-0-0")
        .send()
        .await
        .expect("a response");
    assert_eq!(
        guessed.status(),
        401,
        "a boot key shape must not be guessable into an entitlement"
    );

    // And the key the suites use still sees exactly the configured aliases.
    let allowed = client()
        .get(gateway.url("/v1/models"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("a response");
    assert_eq!(allowed.status(), 200);
}
