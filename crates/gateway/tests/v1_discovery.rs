//! ADR 0063 slice 6: cached provider model discovery.

use std::time::Duration;

use serde_json::{Value, json};
use support::gateway::{Axond, DEFAULT_TUNING, Options};
use support::{GATEWAY_KEY, alias, boot, client};

mod support;

async fn wait_for_listing(http: &reqwest::Client, gateway: &Axond, provider: &str) -> Value {
    let url = gateway.url(&format!("/api/v1/providers/{provider}/models"));
    let mut last = Value::Null;
    for _ in 0..100 {
        let response = http
            .get(&url)
            .bearer_auth(GATEWAY_KEY)
            .send()
            .await
            .expect("listing");
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
        last = response.json().await.expect("json");
        if last["fetched_at"].as_str().is_some() && last["stale"] == false {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("listing did not become fresh: {last}");
}

fn model_ids(body: &Value) -> Vec<&str> {
    body["data"]
        .as_array()
        .expect("data")
        .iter()
        .map(|item| item["id"].as_str().expect("id"))
        .collect()
}

#[tokio::test]
async fn per_provider_and_fan_out_return_fetched_at() {
    let (_upstream, gateway) = boot().await;
    let http = client();

    let openai = wait_for_listing(&http, &gateway, "fake-openai").await;
    assert!(
        openai["fetched_at"]
            .as_str()
            .expect("fetched_at")
            .contains('T'),
        "{openai}"
    );
    assert_eq!(openai["stale"], false);
    assert_eq!(openai["provider"], "fake-openai");
    let ids = model_ids(&openai);
    assert!(ids.contains(&"gpt-4o"), "{ids:?}");
    assert!(ids.contains(&"fixture-chat"), "{ids:?}");

    let fan = http
        .get(gateway.url("/api/v1/providers/models"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("fan-out");
    assert_eq!(fan.status(), 200, "{}", fan.text().await.unwrap());
    let fan: Value = fan.json().await.expect("json");
    let data = fan["data"].as_array().expect("data");
    assert_eq!(data.len(), 2, "{fan}");
    for row in data {
        assert!(row["fetched_at"].as_str().is_some(), "{row}");
        assert_eq!(row["stale"], false, "{row}");
        assert!(!row["data"].as_array().expect("models").is_empty(), "{row}");
    }
}

#[tokio::test]
async fn upstream_5xx_is_stale_with_previous_cache_or_empty() {
    let upstream = support::upstream::FakeUpstream::start().await;
    upstream.state.set_models_status(500);
    let gateway = Axond::start(&upstream.base_url).await;
    let http = client();

    let mut empty = Value::Null;
    for _ in 0..100 {
        let response = http
            .get(gateway.url("/api/v1/providers/fake-openai/models"))
            .bearer_auth(GATEWAY_KEY)
            .send()
            .await
            .expect("empty stale");
        assert_eq!(response.status(), 200);
        empty = response.json().await.expect("json");
        if empty["stale"] == true {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(empty["stale"], true, "{empty}");
    assert_eq!(empty["data"], json!([]), "{empty}");
    assert!(
        empty["fetched_at"].is_null() || empty.get("fetched_at").is_none(),
        "{empty}"
    );

    drop(gateway);

    let upstream = support::upstream::FakeUpstream::start().await;
    let gateway = Axond::start_with_options(
        &upstream.base_url,
        Options::new(DEFAULT_TUNING).with_config("[discovery]\nrefresh_interval_seconds = 1\n"),
    )
    .await;
    let fresh = wait_for_listing(&http, &gateway, "fake-openai").await;
    let previous = model_ids(&fresh);
    assert!(previous.contains(&"gpt-4o"), "{previous:?}");
    let fetched_at = fresh["fetched_at"].clone();

    upstream.state.set_models_status(500);
    let mut stale = Value::Null;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(250)).await;
        let response = http
            .get(gateway.url("/api/v1/providers/fake-openai/models"))
            .bearer_auth(GATEWAY_KEY)
            .send()
            .await
            .expect("stale");
        assert_eq!(response.status(), 200);
        stale = response.json().await.expect("json");
        if stale["stale"] == true {
            break;
        }
    }
    assert_eq!(stale["stale"], true, "{stale}");
    assert_eq!(stale["fetched_at"], fetched_at, "{stale}");
    assert_eq!(model_ids(&stale), previous, "{stale}");
}

#[tokio::test]
async fn namespaced_models_prefix_provider_id_and_omit_blocklist() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    wait_for_listing(&http, &gateway, "fake-openai").await;

    let created = http
        .post(gateway.url("/api/v1/namespaces"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "id": "wsp_disc",
            "attrs": {"org": "acme"},
            "blocklist": ["*-preview"]
        }))
        .send()
        .await
        .expect("create");
    assert_eq!(created.status(), 201, "{}", created.text().await.unwrap());

    let listed = http
        .get(format!("{}/ns/wsp_disc/v1/models", gateway.base_url))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("ns models");
    assert_eq!(listed.status(), 200, "{}", listed.text().await.unwrap());
    let body: Value = listed.json().await.expect("json");
    let ids = model_ids(&body);
    assert!(
        ids.iter().all(|id| id.contains('/')),
        "ids must be provider-id/model-id: {ids:?}"
    );
    assert!(ids.contains(&"fake-openai/gpt-4o"), "{ids:?}");
    assert!(ids.contains(&"fake-openai/fixture-chat"), "{ids:?}");
    assert!(
        ids.iter().all(|id| !id.ends_with("-preview")),
        "blocklist must omit *-preview: {ids:?}"
    );
}

#[tokio::test]
async fn a_completion_does_not_hit_discovery() {
    let (upstream, gateway) = boot().await;
    let http = client();
    wait_for_listing(&http, &gateway, "fake-openai").await;
    let hits = upstream.state.models_hits();
    assert!(hits >= 1, "discovery should have listed once");

    let completion = http
        .post(format!(
            "{}/ns/platform/v1/chat/completions",
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
    assert_eq!(
        completion.status(),
        200,
        "{}",
        completion.text().await.unwrap()
    );
    assert_eq!(
        upstream.state.models_hits(),
        hits,
        "inference must not call GET /models"
    );
}
