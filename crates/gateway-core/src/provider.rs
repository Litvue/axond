use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ProviderError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Surface {
    ChatCompletions,
    Responses,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub chat: bool,
    pub responses: bool,
    pub vision: bool,
    pub reasoning: bool,
    pub embeddings: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub reasoning_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
}

impl ModelUsage {
    pub fn total_tokens(self) -> u64 {
        self.input_tokens
            .saturating_add(self.output_tokens)
            .saturating_add(self.cache_read_tokens)
            .saturating_add(self.cache_write_tokens)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderRequest {
    pub model: String,
    pub body: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderResponse {
    pub body: Value,
    pub usage: ModelUsage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ProviderStreamEvent {
    Data { event: Option<String>, data: Value },
    Usage(ModelUsage),
    Done(ModelUsage),
}

pub trait ProviderStreamDecoder: Send {
    fn decode(&mut self, event: crate::SseEvent)
    -> Result<Vec<ProviderStreamEvent>, ProviderError>;

    fn finish(&mut self) -> Result<Vec<ProviderStreamEvent>, ProviderError> {
        Ok(Vec::new())
    }
}

pub trait ProviderAdapter: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> Capabilities;
    fn encode_request(
        &self,
        surface: Surface,
        request: ProviderRequest,
    ) -> Result<Value, ProviderError>;
    fn decode_response(
        &self,
        surface: Surface,
        response: Value,
    ) -> Result<ProviderResponse, ProviderError>;
    fn stream_decoder(
        &self,
        surface: Surface,
    ) -> Result<Box<dyn ProviderStreamDecoder>, ProviderError>;
}

pub(crate) fn chat_usage(value: &Value) -> ModelUsage {
    let usage = value.get("usage").unwrap_or(value);
    ModelUsage {
        input_tokens: usage
            .get("prompt_tokens")
            .or_else(|| usage.get("input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        output_tokens: usage
            .get("completion_tokens")
            .or_else(|| usage.get("output_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        reasoning_tokens: usage
            .pointer("/completion_tokens_details/reasoning_tokens")
            .or_else(|| usage.pointer("/output_tokens_details/reasoning_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        cache_read_tokens: usage
            .pointer("/prompt_tokens_details/cached_tokens")
            .or_else(|| usage.pointer("/input_tokens_details/cached_tokens"))
            .or_else(|| usage.get("cache_read_input_tokens"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        cache_write_tokens: usage
            .get("cache_creation_input_tokens")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
    }
}
