"""The real OpenAI SDK, pointed at axond instead of at OpenAI."""

from __future__ import annotations

import json

import openai
import pytest
from openai import OpenAI

from conftest import GATEWAY_KEY, NAMESPACE, UNGRANTED_NAMESPACE, UPSTREAM_OPENAI_KEY
from fake_upstream import CHAT, fixture, RESPONSES


@pytest.fixture
def sdk_base_url(gateway) -> str:
    """ADR 0063: inference is served only at `/ns/{ns}/v1`."""
    return f"{gateway}/ns/{NAMESPACE}/v1"


@pytest.fixture
def client(sdk_base_url) -> OpenAI:
    return OpenAI(base_url=sdk_base_url, api_key=GATEWAY_KEY)


def test_buffered_chat_completion(client, upstream):
    completion = client.chat.completions.create(
        model="chat-golden",
        messages=[{"role": "user", "content": "What is the capital of France?"}],
    )
    expected = json.loads(fixture("openai/chat_completion.json"))
    assert completion.choices[0].message.content == expected["choices"][0]["message"]["content"]
    assert completion.usage.prompt_tokens == expected["usage"]["prompt_tokens"]
    assert completion.usage.completion_tokens == expected["usage"]["completion_tokens"]

    sent = upstream.requests[-1]
    # The alias is rewritten to the target model, and the caller's gateway key
    # never travels upstream.
    assert sent["path"] == "/chat/completions"
    assert sent["model"] == CHAT
    assert sent["authorization"] == f"Bearer {UPSTREAM_OPENAI_KEY}"
    assert sent["body"]["messages"][0]["content"] == "What is the capital of France?"


def test_streamed_chat_completion(client, upstream):
    stream = client.chat.completions.create(
        model="chat-golden",
        messages=[{"role": "user", "content": "What is the capital of France?"}],
        stream=True,
    )
    text = "".join(
        chunk.choices[0].delta.content or ""
        for chunk in stream
        if chunk.choices
    )
    assert text == "The capital of France is Paris."
    sent = upstream.requests[-1]
    assert sent["path"] == "/chat/completions"
    assert sent["model"] == CHAT
    assert sent["body"]["stream"] is True
    assert sent["body"]["messages"] == [
        {"role": "user", "content": "What is the capital of France?"}
    ]


def test_embeddings(client, upstream):
    response = client.embeddings.create(model="embeddings-golden", input="hello")
    expected = json.loads(fixture("openai/embeddings.json"))
    assert response.data[0].embedding == expected["data"][0]["embedding"]
    assert response.usage.prompt_tokens == expected["usage"]["prompt_tokens"]
    sent = upstream.requests[-1]
    assert sent["path"] == "/embeddings"
    assert sent["body"]["input"] == "hello"
    assert sent["authorization"] == f"Bearer {UPSTREAM_OPENAI_KEY}"


def test_buffered_responses(client, upstream):
    response = client.responses.create(
        model="responses-golden",
        input="What is the capital of France?",
    )
    expected = json.loads(fixture("openai/responses.json"))
    assert response.id == expected["id"]
    assert response.output[0].content[0].text == expected["output"][0]["content"][0]["text"]
    assert response.usage.input_tokens == expected["usage"]["input_tokens"]
    sent = upstream.requests[-1]
    assert sent["path"] == "/responses"
    assert sent["model"] == RESPONSES
    assert sent["body"]["input"] == "What is the capital of France?"
    assert sent["authorization"] == f"Bearer {UPSTREAM_OPENAI_KEY}"


def test_streamed_responses(client, upstream):
    stream = client.responses.create(
        model="responses-golden",
        input="What is the capital of France?",
        stream=True,
    )
    text = "".join(event.delta for event in stream if getattr(event, "delta", None))
    assert text == "The capital of France is Paris."
    sent = upstream.requests[-1]
    assert sent["path"] == "/responses"
    assert sent["model"] == RESPONSES
    assert sent["body"]["stream"] is True
    assert sent["body"]["input"] == "What is the capital of France?"


def test_models_are_listed(client):
    assert {model.id for model in client.models.list()} >= {
        "chat-golden",
        "messages-golden",
        "embeddings-golden",
        "responses-golden",
    }


def test_an_unknown_gateway_key_is_rejected(sdk_base_url):
    stranger = OpenAI(base_url=sdk_base_url, api_key="not-a-gateway-key", max_retries=0)
    with pytest.raises(openai.AuthenticationError):
        stranger.chat.completions.create(
            model="chat-golden",
            messages=[{"role": "user", "content": "hi"}],
        )


def _models_refusal(gateway: str, namespace: str) -> tuple[int, dict]:
    candidate = OpenAI(
        base_url=f"{gateway}/ns/{namespace}/v1",
        api_key=GATEWAY_KEY,
        max_retries=0,
    )
    with pytest.raises(openai.APIStatusError) as caught:
        candidate.models.list()
    return caught.value.status_code, caught.value.response.json()


def test_store_backed_namespace_is_addressable_and_absent_is_unknown(
    gateway, upstream
):
    before = len(upstream.requests)
    existing = OpenAI(
        base_url=f"{gateway}/ns/{UNGRANTED_NAMESPACE}/v1",
        api_key=GATEWAY_KEY,
        max_retries=0,
    )
    listed = existing.models.list()
    assert listed.object == "list"
    assert len(upstream.requests) == before

    status, body = _models_refusal(gateway, "ghost")
    assert status == 404
    assert body == {
        "error": {
            "type": "unknown_namespace",
            "message": "unknown namespace",
        }
    }
    assert len(upstream.requests) == before


def test_noncanonical_namespace_path_is_a_generic_non_disclosing_refusal(
    gateway, upstream
):
    before = len(upstream.requests)
    status, body = _models_refusal(gateway, "%70latform")

    assert status == 400
    assert body == {
        "error": {
            "type": "invalid_namespace",
            "message": "namespace identifier is invalid",
        }
    }
    assert "%70latform" not in json.dumps(body)
    assert len(upstream.requests) == before
