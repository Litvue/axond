"""The real Anthropic SDK, pointed at axond's native /v1/messages.

The SDK sends its gateway key as ``x-api-key``, which is the reason inbound
auth accepts both schemes (ADR 0012/0013), and it parses thinking and tool-use
blocks strictly — so it passing is the byte-fidelity claim, verified by a client
that was not written for this gateway.
"""

from __future__ import annotations

import json

import pytest
from anthropic import Anthropic

from conftest import GATEWAY_KEY, NAMESPACE, UPSTREAM_ANTHROPIC_KEY
from fake_upstream import MESSAGES, fixture


@pytest.fixture
def sdk_base_url(gateway) -> str:
    """Anthropic appends `/v1/messages`; its base must stop at `/ns/{ns}`."""
    return f"{gateway}/ns/{NAMESPACE}"


@pytest.fixture
def client(sdk_base_url) -> Anthropic:
    return Anthropic(base_url=sdk_base_url, api_key=GATEWAY_KEY)


def test_buffered_message_preserves_thinking_and_tool_use(client, upstream):
    message = client.messages.create(
        model=f"fake-anthropic/{MESSAGES}",
        max_tokens=1024,
        thinking={"type": "enabled", "budget_tokens": 1024},
        messages=[{"role": "user", "content": "Weather in Paris?"}],
    )
    expected = json.loads(fixture("anthropic/message_thinking_tool_use.json"))

    thinking, text, tool_use = message.content
    assert thinking.type == "thinking"
    assert thinking.signature == expected["content"][0]["signature"]
    assert text.text == expected["content"][1]["text"]
    assert tool_use.type == "tool_use"
    assert tool_use.input == expected["content"][2]["input"]
    assert message.usage.input_tokens == expected["usage"]["input_tokens"]
    assert message.usage.output_tokens == expected["usage"]["output_tokens"]

    sent = upstream.requests[-1]
    assert sent["path"] == "/messages"
    assert sent["model"] == MESSAGES
    assert sent["x-api-key"] == UPSTREAM_ANTHROPIC_KEY
    assert sent["anthropic-version"]
    assert sent["body"]["messages"] == [
        {"role": "user", "content": "Weather in Paris?"}
    ]
    assert sent["body"]["thinking"] == {
        "type": "enabled",
        "budget_tokens": 1024,
    }


def test_streamed_message_reassembles_thinking_and_tool_use(client, upstream):
    with client.messages.stream(
        model=f"fake-anthropic/{MESSAGES}",
        max_tokens=1024,
        thinking={"type": "enabled", "budget_tokens": 1024},
        messages=[{"role": "user", "content": "Weather in Paris?"}],
    ) as stream:
        final = stream.get_final_message()

    thinking, text, tool_use = final.content
    # The signature survives the relay byte-for-byte, which is what makes a
    # thinking block replayable back to the provider on the next turn.
    assert thinking.signature == "REDACTED_THINKING_SIGNATURE_0002"
    assert thinking.thinking == "The user wants the weather in Paris."
    assert text.text == "Let me look that up."
    assert tool_use.name == "get_weather"
    assert tool_use.input == {"location": "Paris, France"}
    assert final.stop_reason == "tool_use"
    assert (final.usage.input_tokens, final.usage.output_tokens) == (41, 63)

    sent = upstream.requests[-1]
    # `/ns/{ns}` must cause the SDK to add `/v1/messages` exactly once; axond
    # then removes the outer namespace mount before provider dispatch.
    assert sent["path"] == "/messages"
    assert sent["model"] == MESSAGES
    assert sent["body"]["stream"] is True
    assert sent["body"]["messages"] == [
        {"role": "user", "content": "Weather in Paris?"}
    ]


def test_an_unknown_gateway_key_is_rejected(sdk_base_url):
    import anthropic

    stranger = Anthropic(
        base_url=sdk_base_url,
        api_key="not-a-gateway-key",
        max_retries=0,
    )
    with pytest.raises(anthropic.AuthenticationError):
        stranger.messages.create(
            model=f"fake-anthropic/{MESSAGES}",
            max_tokens=16,
            messages=[{"role": "user", "content": "hi"}],
        )
