//! ADR 0063 slice 3 / ADR 0064: per-namespace per-period admit/charge.

use std::time::Duration;

use serde_json::{Value, json};
use support::{Axond, GATEWAY_KEY, alias, boot, client};

mod support;

fn chat_body() -> Value {
    json!({
        "model": alias::CHAT,
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 16
    })
}

async fn create_namespace(http: &reqwest::Client, gateway: &Axond, id: &str) {
    let created = http
        .post(gateway.url("/api/v1/namespaces"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"id": id, "attrs": {"org": "acme"}}))
        .send()
        .await
        .expect("create");
    assert_eq!(created.status(), 201, "{}", created.text().await.unwrap());
}

async fn put_budget(
    http: &reqwest::Client,
    gateway: &Axond,
    ns: &str,
    period: &str,
    limit: u64,
) -> Value {
    let response = http
        .put(gateway.url(&format!("/api/v1/namespaces/{ns}/budgets/{period}")))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"limit_microdollars": limit}))
        .send()
        .await
        .expect("put budget");
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    response.json().await.expect("budget json")
}

async fn get_budget(http: &reqwest::Client, gateway: &Axond, ns: &str, period: &str) -> Value {
    let response = http
        .get(gateway.url(&format!("/api/v1/namespaces/{ns}/budgets/{period}")))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("get budget");
    assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    response.json().await.expect("budget json")
}

async fn complete(
    http: &reqwest::Client,
    gateway: &Axond,
    ns: &str,
    body: &Value,
) -> reqwest::Response {
    http.post(format!("{}/ns/{ns}/v1/chat/completions", gateway.base_url))
        .bearer_auth(GATEWAY_KEY)
        .json(body)
        .send()
        .await
        .expect("completion")
}

#[tokio::test]
async fn post_put_then_fitting_completion_is_admitted_over_cap_is_not() {
    let (upstream, gateway) = boot().await;
    let http = client();
    create_namespace(&http, &gateway, "wsp_fit").await;

    let none = complete(&http, &gateway, "wsp_fit", &chat_body()).await;
    assert_eq!(none.status(), 429, "{}", none.text().await.unwrap());
    let body: Value = none.json().await.unwrap();
    assert_eq!(body["error"]["type"], "budget_exceeded");
    assert_eq!(upstream.state.received(), 0);

    let published = put_budget(&http, &gateway, "wsp_fit", "2026-09", 1_000_000_000).await;
    assert_eq!(published["reserved_microdollars"], json!(0));
    assert_eq!(published["remaining_microdollars"], json!(1_000_000_000));
    let ok = complete(&http, &gateway, "wsp_fit", &chat_body()).await;
    assert_eq!(ok.status(), 200, "{}", ok.text().await.unwrap());
    assert!(upstream.state.received() >= 1);
    let records = gateway.await_usage_records(1).await;
    assert_eq!(records[0]["period"], json!("2026-09"));

    put_budget(&http, &gateway, "wsp_fit", "2026-09", 1).await;
    let before = upstream.state.received();
    let denied = complete(&http, &gateway, "wsp_fit", &chat_body()).await;
    assert_eq!(denied.status(), 429, "{}", denied.text().await.unwrap());
    let body: Value = denied.json().await.unwrap();
    assert_eq!(body["error"]["type"], "budget_exceeded");
    assert_eq!(upstream.state.received(), before);
}

#[tokio::test]
async fn put_new_period_switches_admission_and_keeps_old_spend() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    create_namespace(&http, &gateway, "wsp_period").await;
    put_budget(&http, &gateway, "wsp_period", "old", 1_000_000_000).await;
    let ok = complete(&http, &gateway, "wsp_period", &chat_body()).await;
    assert_eq!(ok.status(), 200, "{}", ok.text().await.unwrap());
    gateway.await_usage_records(1).await;
    let old = get_budget(&http, &gateway, "wsp_period", "old").await;
    assert!(old["spent_microdollars"].as_u64().unwrap() > 0);
    assert_eq!(old["active"], json!(true));

    let neu = put_budget(&http, &gateway, "wsp_period", "new", 1_000_000_000).await;
    assert_eq!(neu["spent_microdollars"], json!(0));
    assert_eq!(neu["active"], json!(true));
    let old = get_budget(&http, &gateway, "wsp_period", "old").await;
    assert!(old["spent_microdollars"].as_u64().unwrap() > 0);
    assert_eq!(old["active"], json!(false));

    let ok = complete(&http, &gateway, "wsp_period", &chat_body()).await;
    assert_eq!(ok.status(), 200, "{}", ok.text().await.unwrap());
    gateway.await_usage_records(2).await;
    let neu = get_budget(&http, &gateway, "wsp_period", "new").await;
    assert!(neu["spent_microdollars"].as_u64().unwrap() > 0);
    let records = gateway.usage_records();
    assert_eq!(records.last().unwrap()["period"], json!("new"));
}

#[tokio::test]
async fn put_same_period_new_limit_does_not_zero_spend() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    create_namespace(&http, &gateway, "wsp_limit").await;
    put_budget(&http, &gateway, "wsp_limit", "p", 1_000_000_000).await;
    let ok = complete(&http, &gateway, "wsp_limit", &chat_body()).await;
    assert_eq!(ok.status(), 200, "{}", ok.text().await.unwrap());
    gateway.await_usage_records(1).await;
    let before = get_budget(&http, &gateway, "wsp_limit", "p").await;
    let spent = before["spent_microdollars"].as_u64().unwrap();
    assert!(spent > 0);
    let after = put_budget(&http, &gateway, "wsp_limit", "p", spent + 10).await;
    assert_eq!(after["spent_microdollars"].as_u64().unwrap(), spent);
    assert_eq!(after["limit_microdollars"].as_u64().unwrap(), spent + 10);
    assert_eq!(after["reserved_microdollars"], json!(0));
    assert_eq!(after["remaining_microdollars"].as_u64().unwrap(), 10);
}

#[tokio::test]
async fn in_flight_does_not_hold_cancel_charges_consumed() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    create_namespace(&http, &gateway, "wsp_hold").await;
    put_budget(&http, &gateway, "wsp_hold", "p", 1_000_000_000).await;

    let stream = http
        .post(format!(
            "{}/ns/wsp_hold/v1/chat/completions",
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

    let inflight = get_budget(&http, &gateway, "wsp_hold", "p").await;
    assert_eq!(inflight["reserved_microdollars"], json!(0));

    let concurrent = complete(&http, &gateway, "wsp_hold", &chat_body()).await;
    assert_eq!(
        concurrent.status(),
        200,
        "in-flight work does not occupy remaining: {}",
        concurrent.text().await.unwrap()
    );

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
    assert!(saw_body, "stream produced no bytes to cancel");
    drop(stream);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let got = get_budget(&http, &gateway, "wsp_hold", "p").await;
        if got["reserved_microdollars"].as_u64() == Some(0)
            && got["spent_microdollars"].as_u64().unwrap_or(0) > 0
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("charge never landed: {got}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let records = gateway.await_usage_records(2).await;
    let cancelled = records
        .iter()
        .find(|record| record["status"] == json!("client_cancelled"))
        .expect("cancelled usage");
    assert!(cancelled["cost_microdollars"].as_u64().unwrap_or(0) > 0);
}

#[tokio::test]
async fn budget_routes_require_the_gateway_key() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    let unauth = http
        .get(gateway.url("/api/v1/namespaces/platform/budgets/harness"))
        .send()
        .await
        .expect("unauth");
    assert_eq!(unauth.status(), 401);
}

#[tokio::test]
async fn unknown_namespace_budget_is_404() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    let missing = http
        .put(gateway.url("/api/v1/namespaces/ghost/budgets/p"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"limit_microdollars": 1}))
        .send()
        .await
        .expect("missing ns");
    assert_eq!(missing.status(), 404);
    let body: Value = missing.json().await.unwrap();
    assert_eq!(body["error"]["type"], "unknown_namespace");
}
