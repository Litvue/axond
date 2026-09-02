//! ADR 0063 slice 4: OpenAPI 3.1 and usage summary.

use std::collections::BTreeMap;

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
async fn openapi_json_is_31_covers_mounted_routes_and_requires_the_key() {
    let (_upstream, gateway) = boot().await;
    let http = client();

    let unauth = http
        .get(gateway.url("/api/v1/openapi.json"))
        .send()
        .await
        .expect("unauth");
    assert_eq!(unauth.status(), 401);

    let wrong = http
        .get(gateway.url("/api/v1/openapi.json"))
        .bearer_auth("wrong-key")
        .send()
        .await
        .expect("wrong key");
    assert_eq!(wrong.status(), 401);

    let ok = http
        .get(gateway.url("/api/v1/openapi.json"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("spec");
    assert_eq!(ok.status(), 200, "{}", ok.text().await.unwrap());
    let spec: Value = ok.json().await.expect("json");
    let version = spec["openapi"].as_str().expect("openapi");
    assert!(version.starts_with("3.1"), "{version}");

    let paths = spec["paths"].as_object().expect("paths");
    assert!(paths["/api/v1/namespaces"].get("post").is_some());
    assert!(paths["/api/v1/namespaces"].get("get").is_some());
    assert!(paths["/api/v1/namespaces/{ns}"].get("get").is_some());
    assert!(paths["/api/v1/namespaces/{ns}"].get("put").is_some());
    assert!(paths["/api/v1/namespaces/{ns}"].get("delete").is_none());
    assert!(
        paths["/api/v1/namespaces/{ns}/budgets/{period}"]
            .get("put")
            .is_some()
    );
    assert!(
        paths["/api/v1/namespaces/{ns}/budgets/{period}"]
            .get("get")
            .is_some()
    );
    assert!(paths["/api/v1/namespaces/{ns}/usage"].get("get").is_some());
    assert!(
        paths.keys().all(|path| !path.contains("/providers")),
        "discovery is unmounted: {paths:?}"
    );
    let scheme = &spec["components"]["securitySchemes"]["gateway_key"];
    assert_eq!(scheme["type"], "http");
    assert_eq!(scheme["scheme"], "bearer");
}

#[tokio::test]
async fn usage_summary_matches_rows_for_namespace_and_period() {
    let (_upstream, gateway) = boot().await;
    let http = client();
    create_namespace(&http, &gateway, "wsp_use").await;

    let budget = http
        .put(gateway.url("/api/v1/namespaces/wsp_use/budgets/2026-09"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({"limit_microdollars": 1_000_000_000}))
        .send()
        .await
        .expect("budget");
    assert_eq!(budget.status(), 200, "{}", budget.text().await.unwrap());

    let missing = http
        .get(gateway.url("/api/v1/namespaces/wsp_use/usage"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("missing period");
    assert_eq!(missing.status(), 400, "{}", missing.text().await.unwrap());
    let body: Value = missing.json().await.unwrap();
    assert_eq!(body["error"]["type"], "bad_request");

    for _ in 0..2 {
        let ok = http
            .post(format!(
                "{}/ns/wsp_use/v1/chat/completions",
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
    }

    let records = gateway.await_usage_records(2).await;
    let scoped: Vec<_> = records
        .iter()
        .filter(|row| row["namespace"] == "wsp_use" && row["period"] == json!("2026-09"))
        .cloned()
        .collect();
    assert!(!scoped.is_empty(), "{records:?}");

    let mut expected: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
    for row in &scoped {
        let model = row["model"].as_str().expect("model").to_owned();
        let status = row["status"].as_str().expect("status").to_owned();
        let cost = row["cost_microdollars"].as_u64().unwrap_or(0);
        let entry = expected.entry((model, status)).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += cost;
    }

    let summary = http
        .get(gateway.url("/api/v1/namespaces/wsp_use/usage?period=2026-09"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("summary");
    assert_eq!(summary.status(), 200, "{}", summary.text().await.unwrap());
    let body: Value = summary.json().await.unwrap();
    assert_eq!(body["namespace"], "wsp_use");
    assert_eq!(body["period"], "2026-09");
    let data = body["data"].as_array().expect("data");
    assert_eq!(data.len(), expected.len(), "{body} vs {expected:?}");
    for row in data {
        let key = (
            row["model"].as_str().unwrap().to_owned(),
            row["status"].as_str().unwrap().to_owned(),
        );
        let got = (
            row["count"].as_u64().unwrap(),
            row["cost_microdollars"].as_u64().unwrap(),
        );
        assert_eq!(expected.get(&key), Some(&got), "{row} vs {expected:?}");
    }

    let other = http
        .get(gateway.url("/api/v1/namespaces/wsp_use/usage?period=other"))
        .bearer_auth(GATEWAY_KEY)
        .send()
        .await
        .expect("other period");
    assert_eq!(other.status(), 200);
    let body: Value = other.json().await.unwrap();
    assert_eq!(body["data"], json!([]));
}
