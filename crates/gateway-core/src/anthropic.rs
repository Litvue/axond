use std::collections::BTreeMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::{
    Capabilities, ModelUsage, ProviderAdapter, ProviderError, ProviderRequest, ProviderResponse,
    ProviderStreamDecoder, ProviderStreamEvent, SseEvent, Surface, is_rate_limit_payload,
};

const DEFAULT_MAX_TOKENS: u64 = 4096;
const MIN_THINKING_BUDGET: u64 = 1024;
pub const REASONING_DETAIL_FORMAT: &str = "anthropic-claude-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignedThinking {
    pub thinking: String,
    pub signature: String,
}

pub fn encrypted_reasoning_detail(tool_call_id: &str, blocks: &[SignedThinking]) -> Value {
    let data = BASE64.encode(serde_json::to_vec(blocks).unwrap_or_default());
    json!({
        "type": "reasoning.encrypted",
        "id": tool_call_id,
        "data": data,
        "format": REASONING_DETAIL_FORMAT,
    })
}

pub fn signed_thinking_from_details(message: &Value) -> Vec<SignedThinking> {
    message
        .get("reasoning_details")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|detail| {
            detail.get("type").and_then(Value::as_str) == Some("reasoning.encrypted")
                && detail
                    .get("format")
                    .and_then(Value::as_str)
                    .is_none_or(|format| format == REASONING_DETAIL_FORMAT)
        })
        .filter_map(|detail| detail.get("data").and_then(Value::as_str))
        .filter_map(|data| BASE64.decode(data).ok())
        .filter_map(|decoded| serde_json::from_slice::<Vec<SignedThinking>>(&decoded).ok())
        .flatten()
        .collect()
}

pub fn thinking_blocks_from_details(message: &Value) -> Vec<Value> {
    signed_thinking_from_details(message)
        .into_iter()
        .map(|block| {
            json!({
                "type": "thinking",
                "thinking": block.thinking,
                "signature": block.signature,
            })
        })
        .collect()
}

pub struct AnthropicAdapter;

impl AnthropicAdapter {
    pub const VERSION: &'static str = "2023-06-01";

    pub fn new() -> Self {
        Self
    }
}

impl Default for AnthropicAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl ProviderAdapter for AnthropicAdapter {
    fn name(&self) -> &'static str {
        "anthropic"
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            chat: true,
            responses: false,
            vision: true,
            reasoning: true,
            embeddings: false,
        }
    }

    fn encode_request(
        &self,
        surface: Surface,
        request: ProviderRequest,
    ) -> Result<Value, ProviderError> {
        if surface != Surface::ChatCompletions {
            return Err(ProviderError::Unsupported(
                "Anthropic supports chat completions through the native Messages adapter".into(),
            ));
        }
        Ok(build_request(&request.model, &request.body))
    }

    fn decode_response(
        &self,
        surface: Surface,
        response: Value,
    ) -> Result<ProviderResponse, ProviderError> {
        if surface != Surface::ChatCompletions {
            return Err(ProviderError::Unsupported("Anthropic Responses API".into()));
        }
        let usage = anthropic_usage(response.get("usage").unwrap_or(&Value::Null));
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut signed_thinking = Vec::new();
        let mut tool_calls = Vec::new();
        for block in response
            .get("content")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            match block.get("type").and_then(Value::as_str) {
                Some("text") => text.push_str(block.get("text").and_then(Value::as_str).unwrap_or_default()),
                Some("thinking") => {
                    let thinking = block
                        .get("thinking")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    reasoning.push_str(thinking);
                    if let Some(signature) = block.get("signature").and_then(Value::as_str) {
                        signed_thinking.push(SignedThinking {
                            thinking: thinking.to_owned(),
                            signature: signature.to_owned(),
                        });
                    }
                }
                Some("tool_use") => tool_calls.push(json!({
                    "id": block.get("id").cloned().unwrap_or(Value::Null),
                    "type": "function",
                    "function": {
                        "name": block.get("name").cloned().unwrap_or(Value::Null),
                        "arguments": serde_json::to_string(block.get("input").unwrap_or(&Value::Null)).unwrap_or_else(|_| "{}".into())
                    }
                })),
                _ => {}
            }
        }
        let finish_reason = match response.get("stop_reason").and_then(Value::as_str) {
            Some("tool_use") => "tool_calls",
            Some("max_tokens") => "length",
            _ => "stop",
        };
        let mut message = json!({
            "role": "assistant",
            "content": if text.is_empty() { Value::Null } else { json!(text) }
        });
        if !reasoning.is_empty() {
            message["reasoning_content"] = json!(reasoning);
        }
        if !tool_calls.is_empty() {
            if !signed_thinking.is_empty() {
                let tool_call_id = tool_calls[0]
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                message["reasoning_details"] =
                    json!([encrypted_reasoning_detail(tool_call_id, &signed_thinking)]);
            }
            message["tool_calls"] = Value::Array(tool_calls);
        }
        Ok(ProviderResponse {
            body: json!({
                "id": response.get("id").cloned().unwrap_or(Value::Null),
                "object": "chat.completion",
                "model": response.get("model").cloned().unwrap_or(Value::Null),
                "choices": [{ "index": 0, "message": message, "finish_reason": finish_reason }],
                "usage": openai_usage(usage)
            }),
            usage,
        })
    }

    fn stream_decoder(
        &self,
        surface: Surface,
    ) -> Result<Box<dyn ProviderStreamDecoder>, ProviderError> {
        if surface != Surface::ChatCompletions {
            return Err(ProviderError::Unsupported("Anthropic Responses API".into()));
        }
        Ok(Box::new(AnthropicStreamDecoder::default()))
    }
}

#[derive(Default)]
struct AnthropicStreamDecoder {
    usage: ModelUsage,
    first_delta: bool,
    pending_thinking: BTreeMap<u64, SignedThinking>,
    completed_thinking: Vec<SignedThinking>,
    tool_blocks: BTreeMap<u64, u64>,
    next_tool_index: u64,
    thinking_attached: bool,
    saw_tool_call: bool,
    stop_reason: Option<String>,
    terminal_emitted: bool,
    done: bool,
}

impl AnthropicStreamDecoder {
    fn data(&mut self, mut delta: Value) -> ProviderStreamEvent {
        if !self.first_delta {
            if let Some(delta) = delta.as_object_mut() {
                delta.insert("role".into(), json!("assistant"));
            }
            self.first_delta = true;
        }
        ProviderStreamEvent::Data {
            event: None,
            data: json!({
                "choices": [{
                    "index": 0,
                    "delta": delta,
                    "finish_reason": null
                }]
            }),
        }
    }

    fn terminal(&self) -> ProviderStreamEvent {
        let finish_reason = map_stop_reason(self.stop_reason.as_deref(), self.saw_tool_call);
        ProviderStreamEvent::Data {
            event: None,
            data: json!({
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": finish_reason
                }],
                "usage": openai_usage(self.usage)
            }),
        }
    }
}

impl ProviderStreamDecoder for AnthropicStreamDecoder {
    fn decode(&mut self, event: SseEvent) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        let data: Value = serde_json::from_str(&event.data)
            .map_err(|error| ProviderError::InvalidStream(error.to_string()))?;
        let kind = event
            .event
            .as_deref()
            .or_else(|| data.get("type").and_then(Value::as_str));
        match kind {
            Some("message_start") => {
                merge_anthropic_usage(
                    &mut self.usage,
                    data.pointer("/message/usage").unwrap_or(&Value::Null),
                );
                Ok(Vec::new())
            }
            Some("content_block_start") => {
                let index = data.get("index").and_then(Value::as_u64).unwrap_or(0);
                let block = data.get("content_block").unwrap_or(&Value::Null);
                match block.get("type").and_then(Value::as_str) {
                    Some("thinking") => {
                        self.pending_thinking.insert(
                            index,
                            SignedThinking {
                                thinking: block
                                    .get("thinking")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_owned(),
                                signature: block
                                    .get("signature")
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_owned(),
                            },
                        );
                        Ok(Vec::new())
                    }
                    Some("tool_use") => {
                        let tool_index = self.next_tool_index;
                        self.next_tool_index = self.next_tool_index.saturating_add(1);
                        self.tool_blocks.insert(index, tool_index);
                        self.saw_tool_call = true;
                        let id = block.get("id").and_then(Value::as_str).unwrap_or_default();
                        let mut delta = json!({
                            "tool_calls": [{
                                "index": tool_index,
                                "id": id,
                                "type": "function",
                                "function": {
                                    "name": block
                                        .get("name")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default(),
                                    "arguments": ""
                                }
                            }]
                        });
                        if !self.thinking_attached && !self.completed_thinking.is_empty() {
                            delta["reasoning_details"] =
                                json!([encrypted_reasoning_detail(id, &self.completed_thinking)]);
                            self.thinking_attached = true;
                        }
                        Ok(vec![self.data(delta)])
                    }
                    _ => Ok(Vec::new()),
                }
            }
            Some("content_block_delta") => {
                let index = data.get("index").and_then(Value::as_u64).unwrap_or(0);
                let delta = data.get("delta").unwrap_or(&Value::Null);
                match delta.get("type").and_then(Value::as_str) {
                    Some("text_delta") => Ok(vec![self.data(json!({
                        "content": delta.get("text").and_then(Value::as_str).unwrap_or_default()
                    }))]),
                    Some("thinking_delta") => {
                        let thinking = delta
                            .get("thinking")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        self.pending_thinking
                            .entry(index)
                            .or_insert_with(|| SignedThinking {
                                thinking: String::new(),
                                signature: String::new(),
                            })
                            .thinking
                            .push_str(thinking);
                        Ok(vec![self.data(json!({ "reasoning_content": thinking }))])
                    }
                    Some("signature_delta") => {
                        let signature = delta
                            .get("signature")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        self.pending_thinking
                            .entry(index)
                            .or_insert_with(|| SignedThinking {
                                thinking: String::new(),
                                signature: String::new(),
                            })
                            .signature
                            .push_str(signature);
                        Ok(Vec::new())
                    }
                    Some("input_json_delta") => {
                        let Some(tool_index) = self.tool_blocks.get(&index).copied() else {
                            return Err(ProviderError::InvalidStream(format!(
                                "input_json_delta for unknown content block {index}"
                            )));
                        };
                        Ok(vec![self.data(json!({
                            "tool_calls": [{
                                "index": tool_index,
                                "function": {
                                    "arguments": delta
                                        .get("partial_json")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default()
                                }
                            }]
                        }))])
                    }
                    _ => Ok(Vec::new()),
                }
            }
            Some("content_block_stop") => {
                let index = data.get("index").and_then(Value::as_u64).unwrap_or(0);
                if let Some(thinking) = self.pending_thinking.remove(&index)
                    && !thinking.signature.is_empty()
                {
                    self.completed_thinking.push(thinking);
                }
                self.tool_blocks.remove(&index);
                Ok(Vec::new())
            }
            Some("message_delta") => {
                merge_anthropic_usage(&mut self.usage, data.get("usage").unwrap_or(&Value::Null));
                if let Some(reason) = data.pointer("/delta/stop_reason").and_then(Value::as_str) {
                    self.stop_reason = Some(reason.to_owned());
                }
                self.terminal_emitted = true;
                Ok(vec![self.terminal()])
            }
            Some("message_stop") => {
                let mut events = Vec::with_capacity(2);
                if !self.terminal_emitted {
                    events.push(self.terminal());
                    self.terminal_emitted = true;
                }
                if !self.done {
                    events.push(ProviderStreamEvent::Done(self.usage));
                    self.done = true;
                }
                Ok(events)
            }
            Some("error") => {
                let message = data
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("Anthropic stream error")
                    .to_owned();
                if crate::is_rate_limit_payload(&data) {
                    Err(ProviderError::RateLimitedStream(message))
                } else {
                    Err(ProviderError::InvalidStream(message))
                }
            }
            _ => Ok(Vec::new()),
        }
    }

    fn finish(&mut self) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        if self.done {
            return Ok(Vec::new());
        }
        let mut events = Vec::with_capacity(2);
        if !self.terminal_emitted {
            events.push(self.terminal());
            self.terminal_emitted = true;
        }
        events.push(ProviderStreamEvent::Done(self.usage));
        self.done = true;
        Ok(events)
    }
}

/// Usage from a native Messages response, mapped onto the canonical
/// [`ModelUsage`]. Anthropic reports `input_tokens`/`output_tokens` alongside
/// separate cache counters, so a native response needs this rather than the
/// OpenAI-shaped `prompt_tokens`/`completion_tokens` reader.
pub fn native_message_usage(response: &Value) -> ModelUsage {
    anthropic_usage(response.get("usage").unwrap_or(&Value::Null))
}

/// Decoder for a native Messages stream that is relayed to the caller
/// unchanged: every event is handed back verbatim, and the only work done is
/// folding Anthropic's split usage reporting — input tokens on `message_start`,
/// output tokens on `message_delta` — into one [`ModelUsage`].
#[derive(Default)]
pub struct NativeMessagesDecoder {
    usage: ModelUsage,
    done: bool,
}

impl NativeMessagesDecoder {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ProviderStreamDecoder for NativeMessagesDecoder {
    fn decode(&mut self, event: SseEvent) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        let data: Value = serde_json::from_str(&event.data)
            .map_err(|error| ProviderError::InvalidStream(error.to_string()))?;
        let kind = event
            .event
            .clone()
            .or_else(|| data.get("type").and_then(Value::as_str).map(str::to_owned));
        if is_rate_limit_payload(&data) {
            return Err(ProviderError::RateLimitedStream(
                data.pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("Anthropic stream rate limited")
                    .to_owned(),
            ));
        }
        match kind.as_deref() {
            Some("message_start") => merge_anthropic_usage(
                &mut self.usage,
                data.pointer("/message/usage").unwrap_or(&Value::Null),
            ),
            Some("message_delta") => {
                merge_anthropic_usage(&mut self.usage, data.get("usage").unwrap_or(&Value::Null))
            }
            Some("error") => {
                return Err(ProviderError::InvalidStream(
                    data.pointer("/error/message")
                        .and_then(Value::as_str)
                        .unwrap_or("Anthropic stream error")
                        .to_owned(),
                ));
            }
            _ => {}
        }
        let terminal = kind.as_deref() == Some("message_stop");
        let mut events = vec![ProviderStreamEvent::Data { event: kind, data }];
        if terminal && !self.done {
            self.done = true;
            events.push(ProviderStreamEvent::Done(self.usage));
        }
        Ok(events)
    }

    fn finish(&mut self) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        if self.done {
            return Ok(Vec::new());
        }
        self.done = true;
        Ok(vec![ProviderStreamEvent::Done(self.usage)])
    }
}

fn build_request(model: &str, body: &Value) -> Value {
    let max_tokens = body
        .get("max_tokens")
        .or_else(|| body.get("max_completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_TOKENS);
    let thinking = thinking_budget(body, max_tokens);
    let mut request = Map::from_iter([
        ("model".into(), json!(model)),
        ("max_tokens".into(), json!(max_tokens)),
        ("messages".into(), Value::Array(Vec::new())),
    ]);
    let (system, messages) = build_messages(body, thinking.is_some());
    request.insert("messages".into(), Value::Array(messages));
    if !system.is_empty() {
        request.insert("system".into(), json!(system));
    }
    if let Some(tools) = parse_tools(body) {
        request.insert("tools".into(), Value::Array(tools));
        if let Some(choice) = parse_tool_choice(body) {
            request.insert("tool_choice".into(), choice);
        }
    }
    if let Some(budget) = thinking {
        request.insert(
            "thinking".into(),
            json!({ "type": "enabled", "budget_tokens": budget }),
        );
    } else {
        if let Some(value) = body.get("temperature") {
            request.insert("temperature".into(), value.clone());
        }
        if let Some(value) = body.get("top_p") {
            request.insert("top_p".into(), value.clone());
        }
    }
    if let Some(stops) = parse_stop_sequences(body) {
        request.insert("stop_sequences".into(), Value::Array(stops));
    }
    request.insert(
        "stream".into(),
        body.get("stream").cloned().unwrap_or(json!(false)),
    );
    Value::Object(request)
}

fn thinking_budget(body: &Value, max_tokens: u64) -> Option<u64> {
    let fraction = match body.get("reasoning_effort").and_then(Value::as_str)? {
        "minimal" => 10,
        "low" => 20,
        "medium" => 50,
        "high" | "xhigh" => 80,
        _ => return None,
    };
    let cap = max_tokens.checked_sub(MIN_THINKING_BUDGET)?;
    if cap < MIN_THINKING_BUDGET {
        return None;
    }
    let scaled = max_tokens.saturating_mul(fraction) / 100;
    Some(scaled.clamp(MIN_THINKING_BUDGET, cap))
}

fn parse_tools(body: &Value) -> Option<Vec<Value>> {
    let tools = body
        .get("tools")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(|tool| {
            let function = tool.get("function")?;
            let name = function.get("name")?.as_str()?;
            let mut translated = Map::new();
            translated.insert("name".into(), json!(name));
            if let Some(description) = function.get("description").and_then(Value::as_str) {
                translated.insert("description".into(), json!(description));
            }
            translated.insert(
                "input_schema".into(),
                function
                    .get("parameters")
                    .cloned()
                    .unwrap_or_else(|| json!({ "type": "object" })),
            );
            Some(Value::Object(translated))
        })
        .collect::<Vec<_>>();
    (!tools.is_empty()).then_some(tools)
}

fn parse_tool_choice(body: &Value) -> Option<Value> {
    match body.get("tool_choice")? {
        Value::String(choice) => match choice.as_str() {
            "auto" => Some(json!({ "type": "auto" })),
            "required" => Some(json!({ "type": "any" })),
            "none" => Some(json!({ "type": "none" })),
            _ => None,
        },
        choice @ Value::Object(_) => Some(json!({
            "type": "tool",
            "name": choice.pointer("/function/name")?.as_str()?
        })),
        _ => None,
    }
}

fn parse_stop_sequences(body: &Value) -> Option<Vec<Value>> {
    match body.get("stop")? {
        Value::String(stop) => Some(vec![json!(stop)]),
        Value::Array(stops) => {
            let stops = stops
                .iter()
                .filter(|stop| stop.is_string())
                .cloned()
                .collect::<Vec<_>>();
            (!stops.is_empty()).then_some(stops)
        }
        _ => None,
    }
}

fn build_messages(body: &Value, thinking_enabled: bool) -> (String, Vec<Value>) {
    let mut system = String::new();
    let mut messages: Vec<Value> = Vec::new();
    for message in body
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("user");
        if matches!(role, "system" | "developer") {
            let text = content_text(message.get("content"));
            if !text.is_empty() {
                if !system.is_empty() {
                    system.push('\n');
                }
                system.push_str(&text);
            }
            continue;
        }
        let output_role = if role == "assistant" {
            "assistant"
        } else {
            "user"
        };
        let mut blocks = if role == "tool" {
            vec![json!({
                "type": "tool_result",
                "tool_use_id": message.get("tool_call_id").cloned().unwrap_or(Value::Null),
                "content": content_text(message.get("content"))
            })]
        } else {
            content_blocks(message.get("content"))
        };
        if role == "assistant" {
            if thinking_enabled {
                let mut thinking = thinking_blocks_from_details(message);
                thinking.append(&mut blocks);
                blocks = thinking;
            }
            for call in message
                .get("tool_calls")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let function = call.get("function").unwrap_or(&Value::Null);
                let input = function
                    .get("arguments")
                    .and_then(Value::as_str)
                    .and_then(|value| serde_json::from_str(value).ok())
                    .unwrap_or_else(|| json!({}));
                blocks.push(json!({
                    "type": "tool_use",
                    "id": call.get("id").cloned().unwrap_or(Value::Null),
                    "name": function.get("name").cloned().unwrap_or(Value::Null),
                    "input": input
                }));
            }
        }
        if blocks.is_empty() {
            continue;
        }
        if let Some(previous) = messages.last_mut()
            && previous.get("role").and_then(Value::as_str) == Some(output_role)
            && let Some(content) = previous.get_mut("content").and_then(Value::as_array_mut)
        {
            content.extend(blocks);
        } else {
            messages.push(json!({ "role": output_role, "content": blocks }));
        }
    }
    (system, messages)
}

fn content_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| part.get("text").and_then(Value::as_str))
            .collect(),
        _ => String::new(),
    }
}

fn content_blocks(content: Option<&Value>) -> Vec<Value> {
    match content {
        Some(Value::String(value)) if !value.is_empty() => {
            vec![json!({ "type": "text", "text": value })]
        }
        Some(Value::Array(parts)) => parts
            .iter()
            .filter_map(|part| match part.get("type").and_then(Value::as_str) {
                Some("text") => Some(json!({ "type": "text", "text": part.get("text")? })),
                Some("image_url") => {
                    let url = part.pointer("/image_url/url").and_then(Value::as_str)?;
                    Some(image_block(url))
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn image_block(url: &str) -> Value {
    if let Some(data) = url.strip_prefix("data:")
        && let Some((metadata, payload)) = data.split_once(',')
    {
        return json!({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": metadata.split(';').next().unwrap_or("image/png"),
                "data": payload
            }
        });
    }
    json!({ "type": "image", "source": { "type": "url", "url": url } })
}

fn map_stop_reason(stop_reason: Option<&str>, saw_tool_call: bool) -> &'static str {
    match stop_reason {
        Some("tool_use") => "tool_calls",
        Some("max_tokens") => "length",
        Some(_) | None if saw_tool_call => "tool_calls",
        _ => "stop",
    }
}

fn openai_usage(usage: ModelUsage) -> Value {
    json!({
        "prompt_tokens": usage.input_tokens,
        "completion_tokens": usage.output_tokens,
        "total_tokens": usage.total_tokens(),
        "completion_tokens_details": {
            "reasoning_tokens": usage.reasoning_tokens
        },
        "prompt_tokens_details": {
            "cached_tokens": usage.cache_read_tokens,
            "cache_write_tokens": usage.cache_write_tokens
        }
    })
}

fn merge_anthropic_usage(usage: &mut ModelUsage, value: &Value) {
    let next = anthropic_usage(value);
    if value.get("input_tokens").is_some() {
        usage.input_tokens = next.input_tokens;
    }
    if value.get("output_tokens").is_some() {
        usage.output_tokens = next.output_tokens;
    }
    if value.get("reasoning_tokens").is_some() {
        usage.reasoning_tokens = next.reasoning_tokens;
    }
    if value.get("cache_read_input_tokens").is_some() {
        usage.cache_read_tokens = next.cache_read_tokens;
    }
    if value.get("cache_creation_input_tokens").is_some() {
        usage.cache_write_tokens = next.cache_write_tokens;
    }
}

fn anthropic_usage(value: &Value) -> ModelUsage {
    ModelUsage {
        input_tokens: value
            .get("input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output_tokens: value
            .get("output_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        reasoning_tokens: value
            .get("reasoning_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        cache_read_tokens: value
            .get("cache_read_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        cache_write_tokens: value
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_openai_messages_tools_and_usage() {
        let adapter = AnthropicAdapter::new();
        let request = adapter
            .encode_request(
                Surface::ChatCompletions,
                ProviderRequest {
                    model: "claude-sonnet".into(),
                    body: json!({
                        "messages": [
                            { "role": "system", "content": "safe" },
                            { "role": "user", "content": "hello" }
                        ],
                        "tools": [{ "type": "function", "function": { "name": "lookup", "parameters": { "type": "object" } } }]
                    }),
                },
            )
            .unwrap();
        assert_eq!(request["system"], "safe");
        assert_eq!(request["messages"][0]["content"][0]["text"], "hello");
        assert_eq!(request["tools"][0]["name"], "lookup");

        let response = adapter
            .decode_response(
                Surface::ChatCompletions,
                json!({
                    "id": "msg_1",
                    "model": "claude-sonnet",
                    "content": [{ "type": "text", "text": "answer" }],
                    "stop_reason": "end_turn",
                    "usage": { "input_tokens": 10, "output_tokens": 4 }
                }),
            )
            .unwrap();
        assert_eq!(response.body["choices"][0]["message"]["content"], "answer");
        assert_eq!(response.usage.total_tokens(), 14);
    }

    #[test]
    fn translates_reasoning_tool_choice_and_stop_sequences() {
        let request = build_request(
            "claude",
            &json!({
                "messages": [{ "role": "user", "content": "hello" }],
                "max_tokens": 8000,
                "reasoning_effort": "medium",
                "temperature": 0.2,
                "top_p": 0.9,
                "stop": ["END", 42],
                "tools": [{
                    "type": "function",
                    "function": { "name": "lookup", "parameters": { "type": "object" } }
                }],
                "tool_choice": {
                    "type": "function",
                    "function": { "name": "lookup" }
                }
            }),
        );
        assert_eq!(request["thinking"]["budget_tokens"], 4000);
        assert!(request.get("temperature").is_none());
        assert!(request.get("top_p").is_none());
        assert_eq!(
            request["tool_choice"],
            json!({ "type": "tool", "name": "lookup" })
        );
        assert_eq!(request["stop_sequences"], json!(["END"]));
        assert_eq!(
            thinking_budget(&json!({ "reasoning_effort": "minimal" }), 4096),
            Some(1024)
        );
        assert_eq!(
            thinking_budget(&json!({ "reasoning_effort": "high" }), 4096),
            Some(3072)
        );
        assert_eq!(
            thinking_budget(&json!({ "reasoning_effort": "medium" }), 1500),
            None
        );
    }

    #[test]
    fn signed_thinking_round_trips_through_encrypted_details() {
        let adapter = AnthropicAdapter::new();
        let response = adapter
            .decode_response(
                Surface::ChatCompletions,
                json!({
                    "content": [
                        { "type": "thinking", "thinking": "check", "signature": "sig" },
                        { "type": "tool_use", "id": "toolu_1", "name": "lookup", "input": {} }
                    ],
                    "stop_reason": "tool_use"
                }),
            )
            .unwrap();
        let message = &response.body["choices"][0]["message"];
        assert_eq!(
            signed_thinking_from_details(message),
            vec![SignedThinking {
                thinking: "check".into(),
                signature: "sig".into()
            }]
        );
        let request = build_request(
            "claude",
            &json!({
                "reasoning_effort": "low",
                "max_tokens": 8000,
                "messages": [message]
            }),
        );
        assert_eq!(
            request["messages"][0]["content"][0],
            json!({ "type": "thinking", "thinking": "check", "signature": "sig" })
        );
        assert_eq!(request["messages"][0]["content"][1]["type"], "tool_use");
    }

    fn sse(data: Value) -> SseEvent {
        SseEvent {
            event: None,
            data: data.to_string(),
        }
    }

    #[test]
    fn stream_preserves_fragmented_tools_signed_reasoning_and_terminal_usage() {
        let adapter = AnthropicAdapter::new();
        let mut decoder = adapter.stream_decoder(Surface::ChatCompletions).unwrap();
        let upstream = [
            json!({ "type": "message_start", "message": { "usage": {
                "input_tokens": 12,
                "output_tokens": 0,
                "cache_read_input_tokens": 3,
                "cache_creation_input_tokens": 2
            }}}),
            json!({ "type": "content_block_start", "index": 0, "content_block": { "type": "thinking" }}),
            json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "thinking_delta", "thinking": "check " }}),
            json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "thinking_delta", "thinking": "weather" }}),
            json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "signature_delta", "signature": "sig-" }}),
            json!({ "type": "content_block_delta", "index": 0, "delta": { "type": "signature_delta", "signature": "1" }}),
            json!({ "type": "content_block_stop", "index": 0 }),
            json!({ "type": "content_block_start", "index": 2, "content_block": {
                "type": "tool_use", "id": "toolu_1", "name": "lookup"
            }}),
            json!({ "type": "content_block_delta", "index": 2, "delta": {
                "type": "input_json_delta", "partial_json": "{\"city\":"
            }}),
            json!({ "type": "content_block_delta", "index": 2, "delta": {
                "type": "input_json_delta", "partial_json": "\"Paris\"}"
            }}),
            json!({ "type": "content_block_stop", "index": 2 }),
            json!({ "type": "message_delta", "delta": { "stop_reason": "tool_use" }, "usage": {
                "output_tokens": 5, "reasoning_tokens": 2
            }}),
            json!({ "type": "message_stop" }),
        ];
        let mut events = Vec::new();
        for event in upstream {
            events.extend(decoder.decode(sse(event)).unwrap());
        }

        let mut assembler = crate::ToolCallAssembler::new();
        for event in &events {
            assembler.push_event(event).unwrap();
        }
        let calls = assembler.finish().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "toolu_1");
        assert_eq!(calls[0].name, "lookup");
        assert_eq!(
            calls[0].arguments_json().unwrap(),
            json!({ "city": "Paris" })
        );

        let details = events.iter().find_map(|event| match event {
            ProviderStreamEvent::Data { data, .. }
                if data.pointer("/choices/0/delta/reasoning_details/0/type")
                    == Some(&json!("reasoning.encrypted")) =>
            {
                Some(&data["choices"][0]["delta"])
            }
            _ => None,
        });
        let details = details.expect("encrypted reasoning detail on tool start");
        assert_eq!(
            signed_thinking_from_details(details),
            vec![SignedThinking {
                thinking: "check weather".into(),
                signature: "sig-1".into()
            }]
        );

        let terminal = events.iter().find_map(|event| match event {
            ProviderStreamEvent::Data { data, .. }
                if data.pointer("/choices/0/finish_reason") == Some(&json!("tool_calls")) =>
            {
                Some(data)
            }
            _ => None,
        });
        let terminal = terminal.expect("terminal chunk");
        assert_eq!(terminal["usage"]["prompt_tokens"], 12);
        assert_eq!(terminal["usage"]["completion_tokens"], 5);
        assert_eq!(terminal["usage"]["total_tokens"], 22);
        assert_eq!(
            terminal["usage"]["completion_tokens_details"]["reasoning_tokens"],
            2
        );
        assert_eq!(
            terminal["usage"]["prompt_tokens_details"]["cache_write_tokens"],
            2
        );
        assert!(matches!(
            events.last(),
            Some(ProviderStreamEvent::Done(ModelUsage {
                input_tokens: 12,
                output_tokens: 5,
                reasoning_tokens: 2,
                cache_read_tokens: 3,
                cache_write_tokens: 2,
            }))
        ));
    }

    #[test]
    fn stream_finish_closes_an_upstream_stream_without_message_stop() {
        let mut decoder = AnthropicAdapter::new()
            .stream_decoder(Surface::ChatCompletions)
            .unwrap();
        decoder
            .decode(sse(json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": { "type": "text_delta", "text": "answer" }
            })))
            .unwrap();
        let terminal = decoder.finish().unwrap();
        assert_eq!(terminal.len(), 2);
        assert!(matches!(terminal[0], ProviderStreamEvent::Data { .. }));
        assert!(matches!(terminal[1], ProviderStreamEvent::Done(_)));
        assert!(decoder.finish().unwrap().is_empty());
    }

    #[test]
    fn native_response_usage_maps_cache_counters() {
        assert_eq!(
            native_message_usage(&json!({
                "id": "msg_1",
                "content": [{ "type": "text", "text": "answer" }],
                "usage": {
                    "input_tokens": 11,
                    "output_tokens": 4,
                    "cache_creation_input_tokens": 7,
                    "cache_read_input_tokens": 5
                }
            })),
            ModelUsage {
                input_tokens: 11,
                output_tokens: 4,
                reasoning_tokens: 0,
                cache_read_tokens: 5,
                cache_write_tokens: 7,
            }
        );
        assert_eq!(native_message_usage(&json!({})), ModelUsage::default());
    }

    #[test]
    fn native_stream_forwards_events_verbatim_and_folds_split_usage() {
        let mut decoder = NativeMessagesDecoder::new();
        let thinking = json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": { "type": "thinking", "thinking": "why", "signature": "sig-1" }
        });
        let upstream = vec![
            json!({ "type": "message_start", "message": { "usage": {
                "input_tokens": 12,
                "cache_read_input_tokens": 3,
                "cache_creation_input_tokens": 2
            }}}),
            thinking.clone(),
            json!({ "type": "content_block_stop", "index": 0 }),
            json!({ "type": "message_delta", "delta": { "stop_reason": "end_turn" }, "usage": {
                "output_tokens": 9
            }}),
            json!({ "type": "message_stop" }),
        ];
        let mut events = Vec::new();
        for event in &upstream {
            events.extend(decoder.decode(sse(event.clone())).unwrap());
        }

        let forwarded: Vec<&Value> = events
            .iter()
            .filter_map(|event| match event {
                ProviderStreamEvent::Data { data, .. } => Some(data),
                ProviderStreamEvent::Done(_) => None,
            })
            .collect();
        assert_eq!(forwarded, upstream.iter().collect::<Vec<_>>());
        // The signed thinking block survives untouched, which is the whole point
        // of serving the native wire rather than translating it.
        assert_eq!(forwarded[1]["content_block"], thinking["content_block"]);
        assert_eq!(
            events.last(),
            Some(&ProviderStreamEvent::Done(ModelUsage {
                input_tokens: 12,
                output_tokens: 9,
                reasoning_tokens: 0,
                cache_read_tokens: 3,
                cache_write_tokens: 2,
            }))
        );
        assert!(decoder.finish().unwrap().is_empty());
    }

    #[test]
    fn native_stream_reports_an_error_event_and_closes_a_truncated_stream() {
        let mut decoder = NativeMessagesDecoder::new();
        let error = decoder
            .decode(sse(
                json!({ "type": "error", "error": { "message": "overloaded" } }),
            ))
            .unwrap_err();
        assert!(matches!(error, ProviderError::InvalidStream(message) if message == "overloaded"));

        let mut truncated = NativeMessagesDecoder::new();
        truncated
            .decode(sse(
                json!({ "type": "message_start", "message": { "usage": { "input_tokens": 4 } } }),
            ))
            .unwrap();
        assert_eq!(
            truncated.finish().unwrap(),
            vec![ProviderStreamEvent::Done(ModelUsage {
                input_tokens: 4,
                ..ModelUsage::default()
            })]
        );
    }
}
