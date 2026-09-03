//! ADR 0063 slice 5: DELETE /api/v1/namespaces/{ns}.

use std::time::Duration;

use serde_json::{Value, json};
use support::{GATEWAY_KEY, alias, boot, client};

mod support;

async fn create_namespace(http: &reqwest::Client, gateway: &support::Axond, id: &str) {
    let created = http
        .post(gateway.url("/api/v1/namespaces"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"id": id, "attrs": {"org": "acme"}}))
        .send()
        .await
        .expect("create");
    assert_eq!(created.status(), 201, "{}", created.text().await.unwrap());
}

#[tokio::test]
async fn delete_namespace_is_idempotent_and_fail_closed_on_recreate() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    create_namespace(&http, &gateway, "wsp_del").await;

    let budget = http
        .put(gateway.url("/api/v1/namespaces/wsp_del/budgets/2026-09"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"limit_microdollars": 1_000_000_000}))
        .send()
        .await
        .expect("budget");
    assert_eq!(budget.status(), 200, "{}", budget.text().await.unwrap());

    let ok = http
        .post(format!(
            "{}/ns/wsp_del/v1/chat/completions",
            gateway.base_url
        ))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 16
        }))
        .send()
        .await
        .expect("completion");
    assert_eq!(ok.status(), 200, "{}", ok.text().await.unwrap());
    gateway.await_usage_records(1).await;

    let deleted = http
        .delete(gateway.url("/api/v1/namespaces/wsp_del"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("delete");
    assert_eq!(deleted.status(), 204, "{}", deleted.text().await.unwrap());

    let again = http
        .delete(gateway.url("/api/v1/namespaces/wsp_del"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("delete again");
    assert_eq!(again.status(), 204);

    let missing = http
        .delete(gateway.url("/api/v1/namespaces/ghost"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("missing");
    assert_eq!(missing.status(), 204);

    let configured = http
        .delete(gateway.url("/api/v1/namespaces/platform"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("configured");
    assert_eq!(configured.status(), 409);
    let body: Value = configured.json().await.expect("configured body");
    assert_eq!(body["error"]["type"], "namespace_conflict");

    for path in [
        "/api/v1/namespaces/wsp_del",
        "/api/v1/namespaces/wsp_del/budgets/2026-09",
        "/api/v1/namespaces/wsp_del/usage?period=2026-09",
    ] {
        let response = http
            .get(gateway.url(path))
            .bearer_auth(GATEWAY_KEY)
            .send()
            .await
            .expect(path);
        let status = response.status();
        let body: Value = response.json().await.expect(path);
        assert_eq!(status, 404, "{path} {body}");
        assert_eq!(body["error"]["type"], "unknown_namespace", "{path} {body}");
    }

    let completion = http
        .post(format!(
            "{}/ns/wsp_del/v1/chat/completions",
            gateway.base_url
        ))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("completion after delete");
    assert_eq!(
        completion.status(),
        404,
        "{}",
        completion.text().await.unwrap()
    );
    let body: Value = completion.json().await.unwrap();
    assert_eq!(body["error"]["type"], "unknown_namespace");

    create_namespace(&http, &gateway, "wsp_del").await;

    let budget = http
        .get(gateway.url("/api/v1/namespaces/wsp_del/budgets/2026-09"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("budget after recreate");
    assert_eq!(budget.status(), 404, "{}", budget.text().await.unwrap());
    let body: Value = budget.json().await.unwrap();
    assert_eq!(body["error"]["type"], "unknown_budget");

    let denied = http
        .post(format!(
            "{}/ns/wsp_del/v1/chat/completions",
            gateway.base_url
        ))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("fail closed");
    assert_eq!(denied.status(), 429, "{}", denied.text().await.unwrap());
    let body: Value = denied.json().await.unwrap();
    assert_eq!(body["error"]["type"], "budget_exceeded");
}

#[tokio::test]
async fn delete_requires_the_gateway_key() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    let unauth = http
        .delete(gateway.url("/api/v1/namespaces/platform"))
        .send()
        .await
        .expect("unauth");
    assert_eq!(unauth.status(), 401);
}

#[tokio::test]
async fn in_flight_completion_finishes_after_delete() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    create_namespace(&http, &gateway, "wsp_live").await;
    let budget = http
        .put(gateway.url("/api/v1/namespaces/wsp_live/budgets/p"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"limit_microdollars": 1_000_000_000}))
        .send()
        .await
        .expect("budget");
    assert_eq!(budget.status(), 200, "{}", budget.text().await.unwrap());

    let stream = http
        .post(format!(
            "{}/ns/wsp_live/v1/chat/completions",
            gateway.base_url
        ))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT_LONG,
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true,
            "max_tokens": 32
        }))
        .send()
        .await
        .expect("stream headers");
    assert_eq!(stream.status(), 200, "{}", stream.status());

    let deleted = http
        .delete(gateway.url("/api/v1/namespaces/wsp_live"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("delete");
    assert_eq!(deleted.status(), 204, "{}", deleted.text().await.unwrap());

    let mut stream = stream;
    let mut saw_body = false;
    let read_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < read_deadline {
        match stream.chunk().await {
            Ok(Some(chunk)) if !chunk.is_empty() => {
                saw_body = true;
                break;
            }
            Ok(None) => break,
            Ok(Some(_)) => {}
            Err(_) => break,
        }
    }
    assert!(saw_body, "in-flight stream produced no bytes after delete");

    let denied = http
        .post(format!(
            "{}/ns/wsp_live/v1/chat/completions",
            gateway.base_url
        ))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("new completion");
    assert_eq!(denied.status(), 404, "{}", denied.text().await.unwrap());
    let body: Value = denied.json().await.unwrap();
    assert_eq!(body["error"]["type"], "unknown_namespace");
}
