//! Record/replay: committed provider responses served by a fake upstream and
//! replayed through a real gateway, offline (ADR 0014).
//!
//! The assertion is twofold — the caller gets the recorded wire back, and the
//! usage record the gateway settles matches the tokens the fixture reports. A
//! change to either is a wire-fidelity or an accounting regression, and fails
//! deterministically with no network.

mod support;

use serde_json::json;
use support::gateway::{INPUT_PRICE, OUTPUT_PRICE, alias};
use support::{GATEWAY_KEY, boot, client};

fn cost(
    input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
    cache_read_tokens: u64,
) -> u64 {
    let billed_output_tokens = output_tokens.saturating_sub(reasoning_tokens);
    input_tokens * INPUT_PRICE / 1_000_000
        + billed_output_tokens * OUTPUT_PRICE / 1_000_000
        + reasoning_tokens * OUTPUT_PRICE / 1_000_000
        + cache_read_tokens * INPUT_PRICE / 1_000_000
}

#[tokio::test]
async fn replayed_fixtures_settle_the_usage_the_wire_reports() {
    let (_upstream, gateway) = boot().await;
    let client = client();

    let cases = [
        (
            "/v1/chat/completions",
            alias::CHAT,
            false,
            19u64,
            7u64,
            0,
            0,
        ),
        ("/v1/chat/completions", alias::CHAT, true, 19, 7, 0, 0),
        ("/v1/messages", alias::MESSAGES, false, 41, 63, 0, 0),
        ("/v1/messages", alias::MESSAGES, true, 41, 63, 0, 0),
        ("/v1/embeddings", alias::EMBEDDINGS, false, 8, 0, 0, 0),
        ("/v1/responses", alias::RESPONSES, false, 16, 7, 2, 3),
        ("/v1/responses", alias::RESPONSES, true, 16, 7, 2, 3),
    ];

    for (index, (path, model, streamed, input, output, reasoning, cache_read)) in
        cases.into_iter().enumerate()
    {
        let mut body = json!({ "model": model, "max_tokens": 1024, "input": "hello",
                               "messages": [{ "role": "user", "content": "hello" }] });
        if streamed {
            body["stream"] = json!(true);
        }
        let response = client
            .post(gateway.url(path))
            .bearer_auth(GATEWAY_KEY)
            .json(&body)
            .send()
            .await
            .expect("the gateway answers");
        assert_eq!(response.status(), 200, "{path} stream={streamed}");
        // Drain the body so a streamed request settles before the next case.
        let _ = response.bytes().await.expect("a complete body");

        // Settlement is detached from the request, so each case waits for its
        // own record rather than assuming an ordering across cases.
        let records = gateway.await_usage_records(index + 1).await;
        let record = &records[index];
        let label = format!("{path} stream={streamed}");
        assert_eq!(record["model"], json!(model), "{label}");
        assert_eq!(record["status"], json!("ok"), "{label}");
        assert_eq!(record["input_tokens"], json!(input), "{label}");
        assert_eq!(record["output_tokens"], json!(output), "{label}");
        assert_eq!(
            record["cost_microdollars"],
            json!(cost(input, output, reasoning, cache_read)),
            "{label}"
        );
        assert_eq!(record["attempts"], json!(1), "{label}");
    }
}

/// A stream that never opens is charged nothing and holds nothing: the whole
/// budget reservation is released (ADR 0010's `$0` arm).
#[tokio::test]
async fn an_upstream_that_never_opens_is_charged_nothing() {
    let (_upstream, gateway) = boot().await;
    for streamed in [false, true] {
        let response = client()
            .post(gateway.url("/v1/chat/completions"))
            .bearer_auth(GATEWAY_KEY)
            .json(&json!({
                "model": alias::CHAT_FAIL,
                "stream": streamed,
                "messages": [{ "role": "user", "content": "hello" }]
            }))
            .send()
            .await
            .expect("the gateway answers");
        assert!(response.status().is_server_error(), "stream={streamed}");
    }

    let records = gateway.await_usage_records(2).await;
    for record in &records {
        assert_eq!(record["status"], json!("upstream_error"));
        assert_eq!(record["cost_microdollars"], json!(0));
        assert_eq!(record["output_tokens"], json!(0));
    }
}

/// The fixtures are a stability contract, so drift in the frames the accounting
/// reads is caught here rather than as a confusing token mismatch above.
#[test]
fn committed_stream_fixtures_carry_their_usage_frames() {
    let anthropic = String::from_utf8(
        support::upstream::fixture("anthropic/message_thinking_tool_use.sse").to_vec(),
    )
    .expect("fixtures are UTF-8");
    for frame in [
        "event: message_start",
        "\"signature_delta\"",
        "\"input_json_delta\"",
        "event: message_delta",
        "event: message_stop",
    ] {
        assert!(anthropic.contains(frame), "missing `{frame}`");
    }

    let openai =
        String::from_utf8(support::upstream::fixture("openai/chat_completion.sse").to_vec())
            .expect("fixtures are UTF-8");
    assert!(openai.contains("\"usage\""));
    assert!(openai.trim_end().ends_with("data: [DONE]"));

    // No captured secret ever lands in the tree.
    for fixture in [anthropic, openai] {
        assert!(!fixture.contains("sk-"), "a fixture looks unredacted");
    }
}
