//! ADR 0063 slice 1: store, `/ns/{ns}/v1`, static key, namespace API.

use serde_json::{Value, json};
use support::{GATEWAY_KEY, alias, boot, client};

mod support;

#[tokio::test]
async fn namespaced_completion_and_namespace_api() {
    let (_upstream, gateway) = boot().await;
    let http = client();

    let created = http
        .post(gateway.url("/api/v1/namespaces"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"id": "wsp_x", "attrs": {"org": "acme"}}))
        .send()
        .await
        .expect("create");
    assert_eq!(created.status(), 201, "{}", created.text().await.unwrap());

    let dup = http
        .post(gateway.url("/api/v1/namespaces"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"id": "wsp_x"}))
        .send()
        .await
        .expect("dup");
    assert_eq!(dup.status(), 409);
    let body: Value = dup.json().await.unwrap();
    assert_eq!(body["error"]["type"], "namespace_conflict");

    let completion = http
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("completion");
    assert_eq!(
        completion.status(),
        200,
        "{}",
        completion.text().await.unwrap()
    );

    let unprefixed = http
        .post(format!("{}/v1/chat/completions", gateway.base_url))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("unprefixed");
    assert_eq!(unprefixed.status(), 404);

    let minted = http
        .get(gateway.url("/api/v1/namespaces"))
        .bearer_auth("axt1.not-a-token")
        .send()
        .await
        .expect("minted");
    assert_eq!(minted.status(), 401);

    let missing = http
        .post(format!("{}/ns/ghost/v1/chat/completions", gateway.base_url))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("missing ns");
    assert_eq!(missing.status(), 404);
    let body: Value = missing.json().await.unwrap();
    assert_eq!(body["error"]["type"], "unknown_namespace");
}
