"""The real OpenAI SDK, pointed at axond instead of at OpenAI."""

from __future__ import annotations

import json

import pytest
from openai import OpenAI

from conftest import GATEWAY_KEY, UPSTREAM_OPENAI_KEY
from fake_upstream import CHAT, fixture, RESPONSES


@pytest.fixture
def client(gateway) -> OpenAI:
    return OpenAI(base_url=f"{gateway}/v1", api_key=GATEWAY_KEY)


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
    assert sent["model"] == CHAT
    assert sent["authorization"] == f"Bearer {UPSTREAM_OPENAI_KEY}"
    assert sent["body"]["messages"][0]["content"] == "What is the capital of France?"


def test_streamed_chat_completion(client):
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


def test_embeddings(client):
    response = client.embeddings.create(model="embeddings-golden", input="hello")
    expected = json.loads(fixture("openai/embeddings.json"))
    assert response.data[0].embedding == expected["data"][0]["embedding"]
    assert response.usage.prompt_tokens == expected["usage"]["prompt_tokens"]


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
    assert sent["model"] == RESPONSES
    assert sent["authorization"] == f"Bearer {UPSTREAM_OPENAI_KEY}"


def test_streamed_responses(client):
    stream = client.responses.create(
        model="responses-golden",
        input="What is the capital of France?",
        stream=True,
    )
    text = "".join(event.delta for event in stream if getattr(event, "delta", None))
    assert text == "The capital of France is Paris."


def test_models_are_listed(client):
    assert {model.id for model in client.models.list()} >= {
        "chat-golden",
        "messages-golden",
        "embeddings-golden",
        "responses-golden",
    }


def test_an_unknown_gateway_key_is_rejected(gateway):
    import openai

    stranger = OpenAI(base_url=f"{gateway}/v1", api_key="not-a-gateway-key", max_retries=0)
    with pytest.raises(openai.AuthenticationError):
        stranger.chat.completions.create(
            model="chat-golden",
            messages=[{"role": "user", "content": "hi"}],
        )
