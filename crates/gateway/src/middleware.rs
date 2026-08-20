//! Gateway-owned middleware chain orchestration.
//!
//! `gateway-core` defines the I/O-free contract.  This module owns the parts
//! that must remain in the gateway: registration validation, fixed chain order,
//! invocation bounds, failure posture, and mapping to the gateway's stable
//! refusal envelope. Typed policy registration compiles one chain per namespace
//! into the immutable serving snapshot, so hot reload and rollback use the same
//! atomic publication path as routing, pricing, and policy limits.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use gateway_core::{
    DeterministicGuardrail, Middleware, MiddlewareDeclaration, MiddlewareError,
    MiddlewareFailurePosture, MiddlewareNeed, MiddlewareOutcome, MiddlewarePhase,
    MiddlewareRefusal, MiddlewareScope, MiddlewareStateBag, MiddlewareSurface, MiddlewareVerdict,
    ProviderRequest, ProviderResponse, ProviderStreamEvent,
};
use ring::hmac;
use secrecy::zeroize::Zeroize;
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::budget::{Admission, BudgetKey, Denial, Reservation};
use crate::config::Config;
use crate::desired_state::ContentMiddlewareRegistration;
use crate::error::GatewayError;
use crate::rate_limit::{RateLimitError, RateLimitKey, RateLimitPermit};
use crate::state::AppState;

/// Keep abandoned synchronous invocations from consuming Tokio's entire
/// process-wide blocking pool. A timed-out task retains its permit until the
/// middleware actually returns; later calls wait for a slot within their own
/// end-to-end invocation bound instead of spawning unbounded blocked threads.
const MAX_BLOCKING_MIDDLEWARE_INVOCATIONS: usize = 64;

/// No one middleware implementation may retain more than this many blocking
/// workers after concurrent requests cross their invocation deadlines.
const MAX_BLOCKING_INVOCATIONS_PER_MIDDLEWARE: usize = 4;

/// Replica-owned execution bounds shared by every chain in one serving state.
///
/// The global semaphore bounds the blocking work for the replica. A weak gate
/// per middleware id adds a tighter bound and isolates abandoned invocations:
/// while a timed-out call is still running, only that id applies its failure
/// posture. The weak registry forgets inactive ids instead of growing with
/// policy revisions.
#[derive(Clone)]
pub(crate) struct MiddlewareRuntime {
    slots: Arc<Semaphore>,
    gates: Arc<Mutex<BTreeMap<String, Weak<MiddlewareGate>>>>,
}

impl Default for MiddlewareRuntime {
    fn default() -> Self {
        Self {
            slots: Arc::new(Semaphore::new(MAX_BLOCKING_MIDDLEWARE_INVOCATIONS)),
            gates: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl MiddlewareRuntime {
    fn gate(&self, id: &str) -> Arc<MiddlewareGate> {
        let mut gates = self
            .gates
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(id).and_then(Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(MiddlewareGate {
            slots: Arc::new(Semaphore::new(MAX_BLOCKING_INVOCATIONS_PER_MIDDLEWARE)),
            abandoned: AtomicUsize::new(0),
        });
        gates.insert(id.to_owned(), Arc::downgrade(&gate));
        gate
    }

    #[cfg(test)]
    fn with_slots(slots: Arc<Semaphore>) -> Self {
        Self {
            slots,
            gates: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[cfg(test)]
    pub(crate) fn abandoned_for_test(&self, id: &str) -> usize {
        self.gate(id).abandoned.load(Ordering::Acquire)
    }
}

struct MiddlewareGate {
    slots: Arc<Semaphore>,
    abandoned: AtomicUsize,
}

#[derive(Clone, Copy)]
enum InvocationState {
    Running,
    TimedOut,
    Completed,
}

/// Resolves a temporary id quarantine even when middleware panics. Tokio cannot
/// cancel `spawn_blocking`, so this guard has to live inside the closure.
struct InvocationGuard {
    state: Arc<Mutex<InvocationState>>,
    gate: Arc<MiddlewareGate>,
}

impl Drop for InvocationGuard {
    fn drop(&mut self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if matches!(*state, InvocationState::TimedOut) {
            let previous = self.gate.abandoned.fetch_sub(1, Ordering::AcqRel);
            debug_assert!(previous > 0);
        }
        *state = InvocationState::Completed;
    }
}

/// Marks blocking work as abandoned if its async caller is cancelled while
/// awaiting the join. The closure-owned [`InvocationGuard`] clears the
/// quarantine when that work eventually exits.
struct InvocationCancellationGuard {
    state: Arc<Mutex<InvocationState>>,
    gate: Arc<MiddlewareGate>,
    armed: bool,
}

impl InvocationCancellationGuard {
    fn new(state: Arc<Mutex<InvocationState>>, gate: Arc<MiddlewareGate>) -> Self {
        Self {
            state,
            gate,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for InvocationCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            mark_invocation_abandoned(&self.state, &self.gate);
        }
    }
}

/// Permanently marks a response-lifetime state slot unavailable if its owner is
/// moved into blocking work and the awaiting future is cancelled. The blocking
/// invocation may still be mutating that opaque state, so it must never be
/// re-entered even after the id quarantine clears.
struct StateSlotCancellationGuard<'a> {
    stranded: &'a mut bool,
    armed: bool,
}

impl<'a> StateSlotCancellationGuard<'a> {
    fn new(stranded: &'a mut bool) -> Self {
        Self {
            stranded,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StateSlotCancellationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            *self.stranded = true;
        }
    }
}

fn mark_invocation_abandoned(state: &Arc<Mutex<InvocationState>>, gate: &Arc<MiddlewareGate>) {
    let mut state = state.lock().unwrap_or_else(|poison| poison.into_inner());
    if matches!(*state, InvocationState::Running) {
        gate.abandoned.fetch_add(1, Ordering::AcqRel);
        *state = InvocationState::TimedOut;
    }
}

/// A registered, ordered set of content middleware.
#[derive(Clone, Default)]
pub struct MiddlewareChain {
    entries: Arc<[Arc<dyn Middleware>]>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MiddlewareChainError {
    #[error("middleware id must not be empty")]
    EmptyId,
    #[error("middleware `{0}` declares no scope")]
    NoScope(String),
    #[error("middleware `{0}` declares a zero invocation bound")]
    ZeroBound(String),
    #[error("middleware `{0}` declares response mutation without response or stream-event scope")]
    ResponseMutationWithoutScope(String),
    #[error("middleware id `{0}` is registered more than once")]
    DuplicateId(String),
    #[error("middleware `{0}` requests network access, which v1 does not provide")]
    NetworkNeedUnsupported(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MiddlewarePolicyError {
    #[error("content middleware `{id}` is not compiled into this axond build")]
    Unknown { id: String },
    #[error("compiled declaration for content middleware `{id}` does not match its policy")]
    DeclarationMismatch { id: String },
    #[error("content middleware `{id}` has invalid configuration: {detail}")]
    InvalidConfiguration { id: String, detail: String },
    #[error("content middleware `{id}` key reference `{env}` is unset or empty")]
    MissingKey { id: String, env: String },
    #[error(
        "content middleware `{id}` key reference `{env}` must contain canonical padded base64 encoding of exactly 32 bytes"
    )]
    InvalidKey { id: String, env: String },
    #[error(transparent)]
    Chain(#[from] MiddlewareChainError),
}

#[derive(Debug, Error, PartialEq, Eq)]
#[error("namespace `{namespace}` cannot activate its content middleware: {source}")]
pub struct MiddlewarePlanError {
    pub namespace: String,
    #[source]
    pub source: MiddlewarePolicyError,
}

struct MiddlewareBuildContext<'a> {
    namespace: &'a str,
    env: &'a HashMap<String, String>,
    max_request_bytes: usize,
}

type MiddlewareFactory = fn(
    &ContentMiddlewareRegistration,
    &MiddlewareBuildContext<'_>,
) -> Result<Arc<dyn Middleware>, MiddlewarePolicyError>;

/// The in-process implementations this binary knows how to materialize.
///
/// Policy selects from this registry; it cannot load code, grant I/O, or name a
/// core request stage. Unknown or malformed registrations are compile refusals
/// that leave the last-known-good snapshot serving.
struct MiddlewareRegistry {
    factories: BTreeMap<&'static str, MiddlewareFactory>,
}

impl MiddlewareRegistry {
    fn builtins() -> Self {
        let factories =
            BTreeMap::from([("axond.redact", deterministic_guardrail as MiddlewareFactory)]);
        #[cfg(test)]
        let mut factories = factories;
        #[cfg(test)]
        factories.insert(
            "test.policy-marker",
            test_policy_marker as MiddlewareFactory,
        );
        Self { factories }
    }

    fn compile(
        &self,
        registrations: &[ContentMiddlewareRegistration],
        context: &MiddlewareBuildContext<'_>,
    ) -> Result<MiddlewareChain, MiddlewarePolicyError> {
        self.validate(registrations)?;
        let entries = registrations
            .iter()
            .map(|registration| {
                let factory = self.factories.get(registration.id()).ok_or_else(|| {
                    MiddlewarePolicyError::Unknown {
                        id: registration.id().to_owned(),
                    }
                })?;
                let entry = factory(registration, context)?;
                let declaration = entry.declaration();
                if declaration.id != registration.id()
                    || declaration.scopes != registration.scopes()
                    || declaration.failure_posture != registration.failure_posture()
                    || declaration.max_duration
                        != Duration::from_millis(registration.max_duration_milliseconds())
                {
                    return Err(MiddlewarePolicyError::DeclarationMismatch {
                        id: registration.id().to_owned(),
                    });
                }
                Ok(entry)
            })
            .collect::<Result<Vec<_>, _>>()?;
        MiddlewareChain::new(entries).map_err(Into::into)
    }

    fn validate(
        &self,
        registrations: &[ContentMiddlewareRegistration],
    ) -> Result<(), MiddlewarePolicyError> {
        for registration in registrations {
            if !self.factories.contains_key(registration.id()) {
                return Err(MiddlewarePolicyError::Unknown {
                    id: registration.id().to_owned(),
                });
            }
            if registration.id() == "axond.redact" {
                validate_deterministic_guardrail(registration)?;
            } else if registration.guardrail().is_some() {
                return Err(MiddlewarePolicyError::InvalidConfiguration {
                    id: registration.id().to_owned(),
                    detail: "guardrail configuration belongs only to `axond.redact`".to_owned(),
                });
            }
        }
        Ok(())
    }
}

/// Validate one policy's registrations against the implementations and scopes
/// compiled into this binary before an administrative candidate is published.
/// Snapshot compilation calls the same registry again as a defence-in-depth
/// check when a revision is hydrated on another replica.
pub(crate) fn validate_content_middleware(
    registrations: &[ContentMiddlewareRegistration],
) -> Result<(), MiddlewarePolicyError> {
    MiddlewareRegistry::builtins().validate(registrations)
}

/// Every namespace's compiled chain in one serving snapshot.
#[derive(Clone, Default)]
pub struct MiddlewarePlan {
    by_namespace: BTreeMap<String, MiddlewareChain>,
    guardrail_key_fingerprints: BTreeMap<String, String>,
    empty: MiddlewareChain,
}

impl MiddlewarePlan {
    pub fn compile(
        config: &Config,
        env: &HashMap<String, String>,
    ) -> Result<Self, MiddlewarePlanError> {
        let registry = MiddlewareRegistry::builtins();
        let mut by_namespace = BTreeMap::new();
        let mut guardrail_key_fingerprints = BTreeMap::new();
        for namespace in &config.namespace {
            let registrations = namespace.content_middleware();
            if registrations.is_empty() {
                continue;
            }
            // Projected namespaces keep a durable identity across an operator
            // rename. File-declared namespaces have no such identity, so their
            // configured id is the stable namespace boundary available here.
            let namespace_identity = namespace.project.as_ref().map_or_else(
                || namespace.id.clone(),
                |project| format!("{}/{}", project.tenant, project.project),
            );
            let chain = registry
                .compile(
                    registrations,
                    &MiddlewareBuildContext {
                        namespace: &namespace_identity,
                        env,
                        max_request_bytes: config.admission.max_request_bytes,
                    },
                )
                .map_err(|source| MiddlewarePlanError {
                    namespace: namespace.id.clone(),
                    source,
                })?;
            if let Some(guardrail) = registrations
                .iter()
                .find(|registration| registration.id() == "axond.redact")
                .and_then(ContentMiddlewareRegistration::guardrail)
            {
                let fingerprint =
                    guardrail_key_fingerprint(&namespace_identity, guardrail.key_env(), env)
                        .map_err(|source| MiddlewarePlanError {
                            namespace: namespace.id.clone(),
                            source,
                        })?;
                guardrail_key_fingerprints.insert(namespace.id.clone(), fingerprint);
            }
            for middleware in chain.response_only_ids() {
                tracing::warn!(
                    namespace = %namespace.id,
                    middleware,
                    "response-scoped content middleware does not run on streamed requests; declare stream_event scope for streaming coverage"
                );
            }
            by_namespace.insert(namespace.id.clone(), chain);
        }
        Ok(Self {
            by_namespace,
            guardrail_key_fingerprints,
            empty: MiddlewareChain::empty(),
        })
    }

    pub fn for_namespace(&self, namespace: &str) -> &MiddlewareChain {
        self.by_namespace.get(namespace).unwrap_or(&self.empty)
    }

    pub(crate) fn guardrail_key_fingerprint(&self, namespace: &str) -> Option<&str> {
        self.guardrail_key_fingerprints
            .get(namespace)
            .map(String::as_str)
    }
}

fn declaration(registration: &ContentMiddlewareRegistration) -> MiddlewareDeclaration {
    let mut declaration =
        MiddlewareDeclaration::new(registration.id(), registration.scopes().iter().copied());
    declaration.failure_posture = registration.failure_posture();
    declaration.max_duration = Duration::from_millis(registration.max_duration_milliseconds());
    declaration
}

fn validate_deterministic_guardrail(
    registration: &ContentMiddlewareRegistration,
) -> Result<(), MiddlewarePolicyError> {
    if registration.failure_posture() != MiddlewareFailurePosture::FailClosed {
        return Err(MiddlewarePolicyError::InvalidConfiguration {
            id: registration.id().to_owned(),
            detail: "requires failure posture `fail_closed`".to_owned(),
        });
    }
    let required = [
        MiddlewareScope::Request,
        MiddlewareScope::Response,
        MiddlewareScope::StreamEvent,
    ];
    if registration.scopes() != required {
        return Err(MiddlewarePolicyError::InvalidConfiguration {
            id: registration.id().to_owned(),
            detail: "requires request, response, and stream_event scopes".to_owned(),
        });
    }
    let guardrail =
        registration
            .guardrail()
            .ok_or_else(|| MiddlewarePolicyError::InvalidConfiguration {
                id: registration.id().to_owned(),
                detail: "missing guardrail key reference and rules".to_owned(),
            })?;
    let mut declaration = declaration(registration);
    declaration.mutates_response = guardrail
        .rules()
        .iter()
        .any(|rule| rule.action == gateway_core::GuardrailAction::Redact);
    DeterministicGuardrail::compile(declaration, &[0; 32], guardrail.rules()).map_err(|error| {
        MiddlewarePolicyError::InvalidConfiguration {
            id: registration.id().to_owned(),
            detail: error.to_string(),
        }
    })?;
    Ok(())
}

fn deterministic_guardrail(
    registration: &ContentMiddlewareRegistration,
    context: &MiddlewareBuildContext<'_>,
) -> Result<Arc<dyn Middleware>, MiddlewarePolicyError> {
    validate_deterministic_guardrail(registration)?;
    let guardrail = registration
        .guardrail()
        .expect("validated redaction middleware has guardrail state");
    let mut namespace_key = resolve_guardrail_key(
        registration.id(),
        context.namespace,
        guardrail.key_env(),
        context.env,
    )?;
    let mut declaration = declaration(registration);
    declaration.mutates_response = guardrail
        .rules()
        .iter()
        .any(|rule| rule.action == gateway_core::GuardrailAction::Redact);
    let middleware = DeterministicGuardrail::compile_with_request_limit(
        declaration,
        &namespace_key,
        guardrail.rules(),
        context.max_request_bytes,
    );
    namespace_key.zeroize();
    let middleware = middleware.map_err(|error| MiddlewarePolicyError::InvalidConfiguration {
        id: registration.id().to_owned(),
        detail: error.to_string(),
    })?;
    Ok(Arc::new(middleware))
}

fn resolve_guardrail_key(
    id: &str,
    namespace: &str,
    key_env: &str,
    env: &HashMap<String, String>,
) -> Result<[u8; 32], MiddlewarePolicyError> {
    let encoded = env
        .get(key_env)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| MiddlewarePolicyError::MissingKey {
            id: id.to_owned(),
            env: key_env.to_owned(),
        })?;
    let mut decoded = STANDARD
        .decode(encoded)
        .map_err(|_| MiddlewarePolicyError::InvalidKey {
            id: id.to_owned(),
            env: key_env.to_owned(),
        })?;
    if decoded.len() != 32 || STANDARD.encode(&decoded) != *encoded {
        decoded.zeroize();
        return Err(MiddlewarePolicyError::InvalidKey {
            id: id.to_owned(),
            env: key_env.to_owned(),
        });
    }
    let master = hmac::Key::new(hmac::HMAC_SHA256, &decoded);
    decoded.zeroize();
    let mut namespace_context = b"axond.guardrail.namespace.v1\0".to_vec();
    namespace_context.extend_from_slice(namespace.as_bytes());
    let tag = hmac::sign(&master, &namespace_context);
    let mut namespace_key = [0_u8; 32];
    namespace_key.copy_from_slice(tag.as_ref());
    Ok(namespace_key)
}

pub(crate) fn guardrail_key_fingerprint(
    namespace: &str,
    key_env: &str,
    env: &HashMap<String, String>,
) -> Result<String, MiddlewarePolicyError> {
    let mut namespace_key = resolve_guardrail_key("axond.redact", namespace, key_env, env)?;
    let fingerprint_key = hmac::Key::new(hmac::HMAC_SHA256, &namespace_key);
    namespace_key.zeroize();
    Ok(STANDARD
        .encode(hmac::sign(&fingerprint_key, b"axond.guardrail.key-fingerprint.v1\0").as_ref()))
}

#[cfg(test)]
struct PolicyMarker {
    declaration: MiddlewareDeclaration,
}

#[cfg(test)]
impl Middleware for PolicyMarker {
    fn declaration(&self) -> &MiddlewareDeclaration {
        &self.declaration
    }

    fn apply(
        &self,
        phase: MiddlewarePhase<'_>,
        _state: Option<&mut gateway_core::MiddlewareState>,
    ) -> gateway_core::MiddlewareResult {
        if let MiddlewarePhase::Request(request) = phase {
            request.body["policy_middleware"] =
                serde_json::Value::String(self.declaration.id.clone());
        }
        Ok(MiddlewareOutcome::continue_without_state())
    }
}

#[cfg(test)]
fn test_policy_marker(
    registration: &ContentMiddlewareRegistration,
    _context: &MiddlewareBuildContext<'_>,
) -> Result<Arc<dyn Middleware>, MiddlewarePolicyError> {
    Ok(Arc::new(PolicyMarker {
        declaration: declaration(registration),
    }))
}

impl MiddlewareChain {
    /// Build the fixed-order chain.  Registration order is execution order;
    /// policy documents can select content middleware later but cannot reorder
    /// authentication, admission, accounting, or provider failover stages.
    pub fn new(entries: Vec<Arc<dyn Middleware>>) -> Result<Self, MiddlewareChainError> {
        let mut ids = std::collections::BTreeSet::new();
        for entry in &entries {
            let declaration = entry.declaration();
            if declaration.id.is_empty() {
                return Err(MiddlewareChainError::EmptyId);
            }
            if !ids.insert(declaration.id.clone()) {
                return Err(MiddlewareChainError::DuplicateId(declaration.id.clone()));
            }
            if declaration.scopes.is_empty() {
                return Err(MiddlewareChainError::NoScope(declaration.id.clone()));
            }
            if declaration.max_duration.is_zero() {
                return Err(MiddlewareChainError::ZeroBound(declaration.id.clone()));
            }
            if declaration.mutates_response
                && !declaration.has_scope(MiddlewareScope::Response)
                && !declaration.has_scope(MiddlewareScope::StreamEvent)
            {
                return Err(MiddlewareChainError::ResponseMutationWithoutScope(
                    declaration.id.clone(),
                ));
            }
            if declaration.needs.contains(&MiddlewareNeed::Network) {
                return Err(MiddlewareChainError::NetworkNeedUnsupported(
                    declaration.id.clone(),
                ));
            }
        }
        Ok(Self {
            entries: entries.into(),
        })
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether any registered middleware applies to this execution scope,
    /// regardless of whether it declares output mutation.
    pub fn has_scope(&self, scope: MiddlewareScope) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.declaration().has_scope(scope))
    }

    /// Registrations whose output policy covers buffered responses but not
    /// streams. This is valid phase selection, but it is easy to mistake for a
    /// fail-closed guardrail over both request shapes, so snapshot compilation
    /// names every gap to the operator.
    fn response_only_ids(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().filter_map(|entry| {
            let declaration = entry.declaration();
            (declaration.has_scope(MiddlewareScope::Response)
                && !declaration.has_scope(MiddlewareScope::StreamEvent))
            .then_some(declaration.id.as_str())
        })
    }

    /// Whether this chain can change output in the phase this request will
    /// actually execute. A buffered response and a decoded stream are distinct
    /// scopes; selecting one must not make the other look active.
    pub fn has_response_mutator(&self, scope: MiddlewareScope) -> bool {
        self.entries.iter().any(|entry| {
            let declaration = entry.declaration();
            declaration.mutates_response && declaration.has_scope(scope)
        })
    }

    /// Invoke request-scope middleware once, returning the state owner that
    /// must be retained until the response ends.
    pub async fn request(
        &self,
        runtime: &MiddlewareRuntime,
        request: &mut ProviderRequest,
    ) -> Result<MiddlewareStateBag, GatewayError> {
        self.request_with_runtime(request, runtime, &[], None).await
    }

    #[cfg(test)]
    pub(crate) async fn request_isolated(
        &self,
        request: &mut ProviderRequest,
    ) -> Result<MiddlewareStateBag, GatewayError> {
        self.request(&MiddlewareRuntime::default(), request).await
    }

    /// Start a response-lifetime execution using this exact chain generation.
    ///
    /// The returned owner must remain attached to the buffered response or be
    /// moved into the streaming response body. It is the sole owner of every
    /// middleware state slot for the request. This surface-neutral entry point
    /// is suitable for middleware that does not declassify route-shaped output;
    /// authenticated HTTP routes use `start_with_protected_values` so a
    /// declassifying guardrail receives the trusted surface identity.
    pub async fn start(
        &self,
        runtime: &MiddlewareRuntime,
        request: &mut ProviderRequest,
    ) -> Result<MiddlewareExecution, GatewayError> {
        let mut execution = self.execution(runtime, None);
        execution.request(request, &[]).await?;
        Ok(execution)
    }

    /// Start one execution while inspecting caller-controlled provider wire
    /// values that cannot safely be rewritten as content.
    pub async fn start_with_protected_values(
        &self,
        runtime: &MiddlewareRuntime,
        request: &mut ProviderRequest,
        protected_values: &[(String, String)],
        surface: MiddlewareSurface,
    ) -> Result<MiddlewareExecution, GatewayError> {
        let mut execution = self.execution(runtime, Some(surface));
        execution.request(request, protected_values).await?;
        Ok(execution)
    }

    /// Create the response-lifetime owner before fixed core middleware runs.
    /// The rate-limit permit can therefore enter the same owner before content
    /// callbacks execute, while the exact chain generation remains pinned.
    pub(crate) fn execution(
        &self,
        runtime: &MiddlewareRuntime,
        surface: Option<MiddlewareSurface>,
    ) -> MiddlewareExecution {
        MiddlewareExecution::new(
            self.clone(),
            runtime.clone(),
            MiddlewareStateBag::new(self.len()),
            surface,
        )
    }

    #[cfg(test)]
    async fn start_isolated(
        &self,
        request: &mut ProviderRequest,
    ) -> Result<MiddlewareExecution, GatewayError> {
        self.start(&MiddlewareRuntime::default(), request).await
    }

    #[cfg(test)]
    async fn request_with_slots(
        &self,
        request: &mut ProviderRequest,
        slots: Arc<Semaphore>,
    ) -> Result<MiddlewareStateBag, GatewayError> {
        self.request_with_runtime(request, &MiddlewareRuntime::with_slots(slots), &[], None)
            .await
    }

    async fn request_with_runtime(
        &self,
        request: &mut ProviderRequest,
        runtime: &MiddlewareRuntime,
        protected_values: &[(String, String)],
        surface: Option<MiddlewareSurface>,
    ) -> Result<MiddlewareStateBag, GatewayError> {
        let mut states = MiddlewareStateBag::new(self.len());
        for (index, entry) in self.entries.iter().enumerate() {
            let declaration = entry.declaration();
            if !declaration.has_scope(MiddlewareScope::Request) {
                continue;
            }
            let gate = runtime.gate(&declaration.id);
            if gate.abandoned.load(Ordering::Acquire) > 0 {
                self.failure(
                    index,
                    "middleware id quarantined while an abandoned invocation is still running",
                )?;
                continue;
            }
            let bound = declaration.max_duration;
            let capacity_started = tokio::time::Instant::now();
            let deadline = capacity_started + bound;
            let middleware_slot =
                match tokio::time::timeout_at(deadline, Arc::clone(&gate.slots).acquire_owned())
                    .await
                {
                    Ok(Ok(slot)) => slot,
                    Ok(Err(_)) => {
                        self.failure(index, "middleware invocation capacity closed")?;
                        continue;
                    }
                    Err(_) => {
                        crate::telemetry::metrics::record_middleware_capacity_wait(
                            capacity_started.elapsed().as_secs_f64() * 1_000.0,
                            true,
                        );
                        self.failure(
                            index,
                            "invocation bound exceeded waiting for per-middleware capacity",
                        )?;
                        continue;
                    }
                };
            let process_slot =
                match tokio::time::timeout_at(deadline, Arc::clone(&runtime.slots).acquire_owned())
                    .await
                {
                    Ok(Ok(slot)) => slot,
                    Ok(Err(_)) => {
                        self.failure(index, "process invocation capacity closed")?;
                        continue;
                    }
                    Err(_) => {
                        crate::telemetry::metrics::record_middleware_capacity_wait(
                            capacity_started.elapsed().as_secs_f64() * 1_000.0,
                            true,
                        );
                        self.failure(index, "invocation bound exceeded waiting for capacity")?;
                        continue;
                    }
                };
            if invocation_deadline_expired(deadline) {
                crate::telemetry::metrics::record_middleware_capacity_wait(
                    capacity_started.elapsed().as_secs_f64() * 1_000.0,
                    true,
                );
                drop(process_slot);
                drop(middleware_slot);
                self.failure(index, "invocation bound exhausted waiting for capacity")?;
                continue;
            }
            crate::telemetry::metrics::record_middleware_capacity_wait(
                capacity_started.elapsed().as_secs_f64() * 1_000.0,
                false,
            );
            let mut candidate = request.clone();
            let middleware = Arc::clone(entry);
            let protected_values = protected_values.to_vec();
            let invocation_state = Arc::new(Mutex::new(InvocationState::Running));
            let closure_state = Arc::clone(&invocation_state);
            let closure_gate = Arc::clone(&gate);
            let mut cancellation_guard =
                InvocationCancellationGuard::new(Arc::clone(&invocation_state), Arc::clone(&gate));
            let invoked = tokio::time::timeout_at(
                deadline,
                tokio::task::spawn_blocking(move || {
                    let _process_slot = process_slot;
                    let _middleware_slot = middleware_slot;
                    let _invocation_guard = InvocationGuard {
                        state: closure_state,
                        gate: closure_gate,
                    };
                    let result = match middleware.inspect_protected_request(
                        surface,
                        &candidate,
                        &protected_values,
                    ) {
                        Ok(Some(reason)) => Ok(MiddlewareOutcome::refuse(reason)),
                        Ok(None) => middleware.apply_for_surface(
                            surface,
                            MiddlewarePhase::Request(&mut candidate),
                            None,
                        ),
                        Err(error) => Err(error),
                    };
                    (candidate, result)
                }),
            )
            .await;
            cancellation_guard.disarm();
            let (candidate, mut result) = match invoked {
                Ok(Ok(invoked)) => invoked,
                Ok(Err(_)) => {
                    self.failure(index, "invocation task failed")?;
                    continue;
                }
                Err(_) => {
                    mark_invocation_abandoned(&invocation_state, &gate);
                    self.failure(index, "invocation bound exceeded")?;
                    continue;
                }
            };
            let continues = matches!(
                result.as_ref().map(|outcome| outcome.verdict),
                Ok(MiddlewareVerdict::Continue)
            );
            if continues && !routing_fields_unchanged(request, &candidate) {
                result = Err(MiddlewareError::Failed);
            }
            if self.finish(index, result, &mut states, true)? {
                *request = candidate;
            }
        }
        Ok(states)
    }

    fn finish(
        &self,
        index: usize,
        result: Result<MiddlewareOutcome, MiddlewareError>,
        states: &mut MiddlewareStateBag,
        accepts_state: bool,
    ) -> Result<bool, GatewayError> {
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(_) => return self.failure(index, "invocation failed"),
        };
        match outcome.verdict {
            MiddlewareVerdict::Continue => {
                if let Some(state) = outcome.state {
                    if !accepts_state {
                        return self.failure(index, "state returned outside request scope");
                    }
                    states.insert(index, state);
                }
                Ok(true)
            }
            MiddlewareVerdict::Refuse(reason) => Err(GatewayError::MiddlewareRefused {
                reason: stable_refusal_reason(reason),
            }),
        }
    }

    fn failure(&self, index: usize, detail: &'static str) -> Result<bool, GatewayError> {
        let declaration = self.entries[index].declaration();
        tracing::warn!(middleware = %declaration.id, detail, "content middleware invocation failed");
        match declaration.failure_posture {
            MiddlewareFailurePosture::FailOpen => Ok(false),
            MiddlewareFailurePosture::FailClosed => Err(GatewayError::MiddlewareUnavailable),
        }
    }
}

fn invocation_deadline_expired(deadline: tokio::time::Instant) -> bool {
    tokio::time::Instant::now() >= deadline
}

/// Pins one request's middleware chain and owns its response-lifetime state.
///
/// Methods take `&mut self`, so two callbacks cannot borrow the state bag at
/// once. A callback that outlives its bound keeps its state in the abandoned
/// blocking task; that slot is then permanently stranded in this owner. Later
/// callbacks skip a fail-open slot or reject through the stable fail-closed
/// result without spawning concurrent work for the same slot.
pub struct MiddlewareExecution {
    chain: MiddlewareChain,
    runtime: MiddlewareRuntime,
    states: MiddlewareStateBag,
    stranded: Vec<bool>,
    failure_logged: Vec<bool>,
    stream_finished: bool,
    stream_finalized: Vec<bool>,
    surface: Option<MiddlewareSurface>,
    core_rate_limit_permit: Option<RateLimitPermit>,
    core_budget: Option<CoreBudgetHold>,
}

struct InvocationCapacity {
    deadline: tokio::time::Instant,
    process_slot: OwnedSemaphorePermit,
    middleware_slot: OwnedSemaphorePermit,
    invocation_state: Arc<Mutex<InvocationState>>,
    gate: Arc<MiddlewareGate>,
}

impl Default for MiddlewareExecution {
    fn default() -> Self {
        Self::new(
            MiddlewareChain::empty(),
            MiddlewareRuntime::default(),
            MiddlewareStateBag::default(),
            None,
        )
    }
}

impl MiddlewareExecution {
    fn new(
        chain: MiddlewareChain,
        runtime: MiddlewareRuntime,
        states: MiddlewareStateBag,
        surface: Option<MiddlewareSurface>,
    ) -> Self {
        let stranded = vec![false; chain.len()];
        let failure_logged = vec![false; chain.len()];
        let stream_finalized = vec![false; chain.len()];
        Self {
            chain,
            runtime,
            states,
            stranded,
            failure_logged,
            stream_finished: false,
            stream_finalized,
            surface,
            core_rate_limit_permit: None,
            core_budget: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_state_bag_for_test(states: MiddlewareStateBag) -> Self {
        Self {
            chain: MiddlewareChain::empty(),
            runtime: MiddlewareRuntime::default(),
            states,
            stranded: Vec::new(),
            failure_logged: Vec::new(),
            stream_finished: false,
            stream_finalized: Vec::new(),
            surface: None,
            core_rate_limit_permit: None,
            core_budget: None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.chain.is_empty()
    }

    /// Run the pinned configurable request chain into this owner.
    pub(crate) async fn request(
        &mut self,
        request: &mut ProviderRequest,
        protected_values: &[(String, String)],
    ) -> Result<(), GatewayError> {
        self.states = self
            .chain
            .request_with_runtime(request, &self.runtime, protected_values, self.surface)
            .await?;
        Ok(())
    }

    /// Fixed core rate-limit middleware. The permit remains in this execution
    /// until the buffered response body ends or streaming accounting drops.
    pub(crate) async fn acquire_rate_limit(
        &mut self,
        state: &AppState,
        key: &RateLimitKey,
    ) -> Result<(), GatewayError> {
        debug_assert!(self.core_rate_limit_permit.is_none());
        let permit = state
            .0
            .rate_limiter
            .acquire(key)
            .await
            .map_err(|error| match error {
                RateLimitError::StoreUnavailable => GatewayError::RateLimitUnavailable,
                RateLimitError::Exceeded | RateLimitError::SubjectCapacityExceeded => {
                    GatewayError::RateLimitExceeded {
                        retry_after_seconds: None,
                    }
                }
            })?;
        self.core_rate_limit_permit = Some(permit);
        Ok(())
    }

    /// Fixed core budget middleware. This runs only after content mutation and
    /// authoritative estimate recomputation. An armed hold releases on drop
    /// unless an outcome transfers it into terminal accounting first.
    pub(crate) async fn reserve_budget(
        &mut self,
        state: &AppState,
        key: BudgetKey,
        estimated_microdollars: u64,
        estimated_input_tokens: u64,
        alias: &str,
    ) -> Result<(), GatewayError> {
        debug_assert!(self.core_budget.is_none());
        let reservation = match state.0.budget.reserve(&key, estimated_microdollars).await {
            Admission::Allowed(reservation) => reservation,
            Admission::Denied(Denial::Exceeded) => {
                return Err(GatewayError::BudgetExceeded(alias.to_owned()));
            }
            Admission::Denied(Denial::StoreUnavailable) => {
                return Err(GatewayError::BudgetUnavailable);
            }
        };
        self.core_budget = Some(CoreBudgetHold {
            state: state.clone(),
            key,
            reservation: Some(reservation),
            estimated_input_tokens,
        });
        Ok(())
    }

    pub(crate) fn core_budget_context(&self) -> Option<(&BudgetKey, &Reservation, u64)> {
        self.core_budget.as_ref().map(|hold| {
            (
                &hold.key,
                hold.reservation
                    .as_ref()
                    .expect("core budget hold is armed"),
                hold.estimated_input_tokens,
            )
        })
    }

    pub(crate) fn take_core_budget(&mut self) -> Option<CoreBudgetHold> {
        self.core_budget.take()
    }

    pub(crate) async fn release_core_budget(&mut self) -> bool {
        let Some(hold) = self.core_budget.take() else {
            return false;
        };
        hold.release().await;
        true
    }

    /// Whether this pinned execution has any stream-event middleware whose
    /// response-lifetime state must observe strict stream completion.
    pub fn has_stream_event_scope(&self) -> bool {
        self.chain.has_scope(MiddlewareScope::StreamEvent)
    }

    /// Whether this pinned stream execution may change rendered event payloads.
    /// Validation-only middleware still requires strict finalization, but it must
    /// not silently turn a disabled ordinary stream-size ceiling back on.
    pub fn has_stream_event_mutator(&self) -> bool {
        self.chain
            .has_response_mutator(MiddlewareScope::StreamEvent)
    }

    /// Run buffered-response scopes in reverse registration order.
    pub async fn response(&mut self, response: &mut ProviderResponse) -> Result<(), GatewayError> {
        for index in (0..self.chain.len()).rev() {
            if !self.chain.entries[index]
                .declaration()
                .has_scope(MiddlewareScope::Response)
            {
                continue;
            }
            if self.stranded[index] {
                self.failure(index, "state unavailable after abandoned invocation")?;
                continue;
            }

            let capacity = match self.acquire(index).await? {
                Some(capacity) => capacity,
                None => continue,
            };
            let InvocationCapacity {
                deadline,
                process_slot,
                middleware_slot,
                invocation_state,
                gate,
            } = capacity;
            let middleware = Arc::clone(&self.chain.entries[index]);
            let surface = self.surface;
            let mut candidate = response.clone();
            let original_usage = response.usage;
            let mut state = self.states.take(index);
            let closure_state = Arc::clone(&invocation_state);
            let closure_gate = Arc::clone(&gate);
            let mut cancellation_guard =
                InvocationCancellationGuard::new(Arc::clone(&invocation_state), Arc::clone(&gate));
            let invoked = {
                let mut state_guard = StateSlotCancellationGuard::new(&mut self.stranded[index]);
                let invoked = tokio::time::timeout_at(
                    deadline,
                    tokio::task::spawn_blocking(move || {
                        let _process_slot = process_slot;
                        let _middleware_slot = middleware_slot;
                        let _invocation_guard = InvocationGuard {
                            state: closure_state,
                            gate: closure_gate,
                        };
                        let result = middleware.apply_for_surface(
                            surface,
                            MiddlewarePhase::Response(&mut candidate),
                            state.as_mut(),
                        );
                        (candidate, state, result)
                    }),
                )
                .await;
                cancellation_guard.disarm();
                if matches!(&invoked, Ok(Ok(_))) {
                    state_guard.disarm();
                }
                invoked
            };
            let (candidate, state, mut result) = match invoked {
                Ok(Ok(invoked)) => invoked,
                Ok(Err(_)) => {
                    self.stranded[index] = true;
                    self.failure(index, "invocation task failed")?;
                    continue;
                }
                Err(_) => {
                    mark_invocation_abandoned(&invocation_state, &gate);
                    self.stranded[index] = true;
                    self.failure(index, "invocation bound exceeded")?;
                    continue;
                }
            };
            if matches!(
                result.as_ref().map(|outcome| outcome.verdict),
                Ok(MiddlewareVerdict::Continue)
            ) && (candidate.usage != original_usage
                || (!self.chain.entries[index].declaration().mutates_response
                    && candidate.body != response.body))
            {
                result = Err(MiddlewareError::Failed);
            }
            let restore_state = matches!(
                result.as_ref(),
                Ok(outcome)
                    if outcome.verdict == MiddlewareVerdict::Continue
                        && outcome.state.is_none()
            );
            if restore_state {
                if let Some(state) = state {
                    self.states.insert(index, state);
                }
            } else if state.is_some() {
                // A failed callback may have partially mutated opaque state.
                // It cannot be rolled back, so disable this slot rather than
                // expose that partial mutation to a later response event.
                self.stranded[index] = true;
            }
            if self.finish(index, result)? {
                *response = candidate;
            }
        }
        Ok(())
    }

    /// Run decoded stream-event scopes in reverse registration order.
    ///
    /// Terminal usage events are gateway-owned and are never handed to content
    /// middleware. Only decoded data events are transformable.
    pub async fn stream_event(
        &mut self,
        event: &mut ProviderStreamEvent,
    ) -> Result<(), GatewayError> {
        if matches!(event, ProviderStreamEvent::Done(_)) {
            return Ok(());
        }
        for index in (0..self.chain.len()).rev() {
            if !self.chain.entries[index]
                .declaration()
                .has_scope(MiddlewareScope::StreamEvent)
            {
                continue;
            }
            if self.stranded[index] {
                self.failure(index, "state unavailable after abandoned invocation")?;
                continue;
            }

            let capacity = match self.acquire(index).await? {
                Some(capacity) => capacity,
                None => continue,
            };
            let InvocationCapacity {
                deadline,
                process_slot,
                middleware_slot,
                invocation_state,
                gate,
            } = capacity;
            let middleware = Arc::clone(&self.chain.entries[index]);
            let surface = self.surface;
            let mut candidate = event.clone();
            let mut state = self.states.take(index);
            let closure_state = Arc::clone(&invocation_state);
            let closure_gate = Arc::clone(&gate);
            let mut cancellation_guard =
                InvocationCancellationGuard::new(Arc::clone(&invocation_state), Arc::clone(&gate));
            let invoked = {
                let mut state_guard = StateSlotCancellationGuard::new(&mut self.stranded[index]);
                let invoked = tokio::time::timeout_at(
                    deadline,
                    tokio::task::spawn_blocking(move || {
                        let _process_slot = process_slot;
                        let _middleware_slot = middleware_slot;
                        let _invocation_guard = InvocationGuard {
                            state: closure_state,
                            gate: closure_gate,
                        };
                        let result = middleware.apply_for_surface(
                            surface,
                            MiddlewarePhase::StreamEvent(&mut candidate),
                            state.as_mut(),
                        );
                        (candidate, state, result)
                    }),
                )
                .await;
                cancellation_guard.disarm();
                if matches!(&invoked, Ok(Ok(_))) {
                    state_guard.disarm();
                }
                invoked
            };
            let (candidate, state, mut result) = match invoked {
                Ok(Ok(invoked)) => invoked,
                Ok(Err(_)) => {
                    self.stranded[index] = true;
                    self.failure(index, "invocation task failed")?;
                    continue;
                }
                Err(_) => {
                    mark_invocation_abandoned(&invocation_state, &gate);
                    self.stranded[index] = true;
                    self.failure(index, "invocation bound exceeded")?;
                    continue;
                }
            };
            if matches!(
                result.as_ref().map(|outcome| outcome.verdict),
                Ok(MiddlewareVerdict::Continue)
            ) && (!matches!(&candidate, ProviderStreamEvent::Data { .. })
                || (!self.chain.entries[index].declaration().mutates_response
                    && candidate != *event))
            {
                result = Err(MiddlewareError::Failed);
            }
            let restore_state = matches!(
                result.as_ref(),
                Ok(outcome)
                    if outcome.verdict == MiddlewareVerdict::Continue
                        && outcome.state.is_none()
            );
            if restore_state {
                if let Some(state) = state {
                    self.states.insert(index, state);
                }
            } else if state.is_some() {
                self.stranded[index] = true;
            }
            if self.finish(index, result)? {
                *event = candidate;
            }
        }
        Ok(())
    }

    /// Finalize stream-event middleware once, in reverse registration order.
    ///
    /// The callback carries no terminal event or usage payload: accounting owns
    /// terminal usage, while content middleware may only validate and release
    /// its response-lifetime state. Marking the execution finished before the
    /// first await prevents cancellation from retrying a partially completed
    /// reverse chain or double-finalizing state still owned by blocking work.
    pub async fn finish_stream(&mut self) -> Result<(), GatewayError> {
        if self.stream_finished {
            for index in (0..self.chain.len()).rev() {
                if self.chain.entries[index]
                    .declaration()
                    .has_scope(MiddlewareScope::StreamEvent)
                    && !self.stream_finalized[index]
                {
                    self.failure(index, "stream finalizer did not complete")?;
                }
            }
            return Ok(());
        }
        self.stream_finished = true;

        for index in (0..self.chain.len()).rev() {
            if !self.chain.entries[index]
                .declaration()
                .has_scope(MiddlewareScope::StreamEvent)
            {
                continue;
            }
            if self.stranded[index] {
                self.failure(index, "state unavailable after abandoned invocation")?;
                continue;
            }

            let capacity = match self.acquire(index).await? {
                Some(capacity) => capacity,
                None => continue,
            };
            let InvocationCapacity {
                deadline,
                process_slot,
                middleware_slot,
                invocation_state,
                gate,
            } = capacity;
            let middleware = Arc::clone(&self.chain.entries[index]);
            let mut state = self.states.take(index);
            let closure_state = Arc::clone(&invocation_state);
            let closure_gate = Arc::clone(&gate);
            let mut cancellation_guard =
                InvocationCancellationGuard::new(Arc::clone(&invocation_state), Arc::clone(&gate));
            let invoked = {
                let mut state_guard = StateSlotCancellationGuard::new(&mut self.stranded[index]);
                let invoked = tokio::time::timeout_at(
                    deadline,
                    tokio::task::spawn_blocking(move || {
                        let _process_slot = process_slot;
                        let _middleware_slot = middleware_slot;
                        let _invocation_guard = InvocationGuard {
                            state: closure_state,
                            gate: closure_gate,
                        };
                        let result = middleware.finish_stream(state.as_mut());
                        (state, result)
                    }),
                )
                .await;
                cancellation_guard.disarm();
                if matches!(&invoked, Ok(Ok(_))) {
                    state_guard.disarm();
                }
                invoked
            };
            let (state, result) = match invoked {
                Ok(Ok(invoked)) => invoked,
                Ok(Err(_)) => {
                    self.stranded[index] = true;
                    self.failure(index, "invocation task failed")?;
                    continue;
                }
                Err(_) => {
                    mark_invocation_abandoned(&invocation_state, &gate);
                    self.stranded[index] = true;
                    self.failure(index, "invocation bound exceeded")?;
                    continue;
                }
            };
            if result.is_err() {
                self.stranded[index] = true;
                self.failure(index, "invocation failed")?;
            } else {
                if let Some(state) = state {
                    self.states.insert(index, state);
                }
                self.stream_finalized[index] = true;
            }
        }
        Ok(())
    }

    async fn acquire(&mut self, index: usize) -> Result<Option<InvocationCapacity>, GatewayError> {
        let declaration = self.chain.entries[index].declaration();
        let gate = self.runtime.gate(&declaration.id);
        let bound = declaration.max_duration;
        if gate.abandoned.load(Ordering::Acquire) > 0 {
            self.failure(
                index,
                "middleware id quarantined while an abandoned invocation is still running",
            )?;
            return Ok(None);
        }
        let capacity_started = tokio::time::Instant::now();
        let deadline = capacity_started + bound;
        let middleware_slot = match tokio::time::timeout_at(
            deadline,
            Arc::clone(&gate.slots).acquire_owned(),
        )
        .await
        {
            Ok(Ok(slot)) => slot,
            Ok(Err(_)) => {
                self.failure(index, "middleware invocation capacity closed")?;
                return Ok(None);
            }
            Err(_) => {
                crate::telemetry::metrics::record_middleware_capacity_wait(
                    capacity_started.elapsed().as_secs_f64() * 1_000.0,
                    true,
                );
                self.failure(
                    index,
                    "invocation bound exceeded waiting for per-middleware capacity",
                )?;
                return Ok(None);
            }
        };
        let process_slot = match tokio::time::timeout_at(
            deadline,
            Arc::clone(&self.runtime.slots).acquire_owned(),
        )
        .await
        {
            Ok(Ok(slot)) => slot,
            Ok(Err(_)) => {
                self.failure(index, "process invocation capacity closed")?;
                return Ok(None);
            }
            Err(_) => {
                crate::telemetry::metrics::record_middleware_capacity_wait(
                    capacity_started.elapsed().as_secs_f64() * 1_000.0,
                    true,
                );
                self.failure(index, "invocation bound exceeded waiting for capacity")?;
                return Ok(None);
            }
        };
        if invocation_deadline_expired(deadline) {
            crate::telemetry::metrics::record_middleware_capacity_wait(
                capacity_started.elapsed().as_secs_f64() * 1_000.0,
                true,
            );
            drop(process_slot);
            drop(middleware_slot);
            self.failure(index, "invocation bound exhausted waiting for capacity")?;
            return Ok(None);
        }
        crate::telemetry::metrics::record_middleware_capacity_wait(
            capacity_started.elapsed().as_secs_f64() * 1_000.0,
            false,
        );
        Ok(Some(InvocationCapacity {
            deadline,
            process_slot,
            middleware_slot,
            invocation_state: Arc::new(Mutex::new(InvocationState::Running)),
            gate,
        }))
    }

    /// Apply one slot's failure posture on every event, but emit its operator
    /// warning at most once for this response-lifetime execution. Counters keep
    /// carrying volume; a quarantined fail-open add-on must not turn every chunk
    /// of one long stream into an identical log line.
    fn failure(&mut self, index: usize, detail: &'static str) -> Result<bool, GatewayError> {
        if !self.failure_logged[index] {
            self.failure_logged[index] = true;
            return self.chain.failure(index, detail);
        }
        match self.chain.entries[index].declaration().failure_posture {
            MiddlewareFailurePosture::FailOpen => Ok(false),
            MiddlewareFailurePosture::FailClosed => Err(GatewayError::MiddlewareUnavailable),
        }
    }

    fn finish(
        &mut self,
        index: usize,
        result: Result<MiddlewareOutcome, MiddlewareError>,
    ) -> Result<bool, GatewayError> {
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(_) => return self.failure(index, "invocation failed"),
        };
        match outcome.verdict {
            MiddlewareVerdict::Continue => {
                if outcome.state.is_some() {
                    return self.failure(index, "state returned outside request scope");
                }
                Ok(true)
            }
            MiddlewareVerdict::Refuse(reason) => Err(GatewayError::MiddlewareRefused {
                reason: stable_refusal_reason(reason),
            }),
        }
    }
}

/// Gateway-owned state for the fixed budget middleware. It is deliberately not
/// a `gateway-core` middleware state: core remains I/O-free, while this owner
/// performs the asynchronous reserve/settle contract at fixed source positions.
pub(crate) struct CoreBudgetHold {
    state: AppState,
    key: BudgetKey,
    reservation: Option<Reservation>,
    estimated_input_tokens: u64,
}

impl CoreBudgetHold {
    pub(crate) async fn settle(mut self, actual_microdollars: u64) {
        let reservation = self
            .reservation
            .take()
            .expect("core budget hold must be armed");
        self.state
            .0
            .budget
            .settle(&self.key, &reservation, actual_microdollars)
            .await;
    }

    pub(crate) async fn release(mut self) {
        let reservation = self
            .reservation
            .take()
            .expect("core budget hold must be armed");
        self.state.0.budget.release(&self.key, &reservation).await;
    }
}

impl Drop for CoreBudgetHold {
    fn drop(&mut self) {
        let Some(reservation) = self.reservation.take() else {
            return;
        };
        let state = self.state.clone();
        let key = self.key.clone();
        crate::streaming::spawn_settlement(async move {
            state.0.budget.release(&key, &reservation).await;
        });
    }
}

fn routing_fields_unchanged(before: &ProviderRequest, after: &ProviderRequest) -> bool {
    before.model == after.model
        && before.body.get("model") == after.body.get("model")
        && before.body.get("stream") == after.body.get("stream")
        && before.body.get("previous_response_id") == after.body.get("previous_response_id")
}

fn stable_refusal_reason(reason: MiddlewareRefusal) -> &'static str {
    match reason {
        MiddlewareRefusal::Policy => "policy",
        MiddlewareRefusal::InvalidRequest => "invalid_request",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{NamespacePolicy, ProjectIdentity};
    use crate::desired_state::fixtures::{policy_body, project_id, revision_id, tenant_id};
    use crate::desired_state::policy::ContentGuardrailRegistration;
    use crate::desired_state::{PolicyEpoch, PolicyScope};
    use gateway_core::{
        GuardrailAction, GuardrailRule, MiddlewareDeclaration, MiddlewareFailurePosture,
        MiddlewareOutcome, MiddlewareState, ModelUsage,
    };
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct MiddlewareLogWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for MiddlewareLogWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("middleware log")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn registration(
        id: &str,
        scopes: impl IntoIterator<Item = MiddlewareScope>,
    ) -> ContentMiddlewareRegistration {
        ContentMiddlewareRegistration::new(id, scopes, MiddlewareFailurePosture::FailClosed, 25)
            .expect("valid registration")
    }

    fn config_with_registration(registration: ContentMiddlewareRegistration) -> Config {
        let mut config = Config::from_toml_str(
            r#"
[[namespace]]
id = "alpha"
default = true

[[namespace]]
id = "beta"

[[gateway_key]]
env = "ALPHA_KEY"
namespace = "alpha"
"#,
        )
        .expect("valid namespace fixture");
        let body = policy_body(PolicyScope::Tenant(tenant_id(1)), PolicyEpoch::FIRST.get())
            .with_content_middleware(vec![registration])
            .expect("registration attaches");
        let generation = body.generation(revision_id(1));
        config.namespace[0].policy = Some(NamespacePolicy { body, generation });
        config
    }

    fn guardrail_registration() -> ContentMiddlewareRegistration {
        registration(
            "axond.redact",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        )
        .with_guardrail(
            ContentGuardrailRegistration::new(
                "GW_GUARDRAIL_KEY",
                vec![GuardrailRule {
                    id: "email".to_owned(),
                    pattern: r"[a-z]+@example\.com".to_owned(),
                    action: GuardrailAction::Redact,
                }],
            )
            .expect("guardrail configuration"),
        )
        .expect("guardrail attaches")
    }

    struct Noop {
        declaration: MiddlewareDeclaration,
    }

    impl Middleware for Noop {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            _phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    struct Mutator {
        declaration: MiddlewareDeclaration,
    }

    impl Middleware for Mutator {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if let MiddlewarePhase::Request(request) = phase {
                request.body["changed"] = json!(true);
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    struct Refuser {
        declaration: MiddlewareDeclaration,
    }

    impl Middleware for Refuser {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            _phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            Ok(MiddlewareOutcome::refuse(MiddlewareRefusal::Policy))
        }
    }

    #[tokio::test]
    async fn empty_chain_is_byte_neutral() {
        let chain = MiddlewareChain::empty();
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"prompt": "unchanged"}),
        };
        let original = request.clone();
        let states = chain
            .request_isolated(&mut request)
            .await
            .expect("empty chain");
        assert_eq!(request, original);
        assert!(states.is_empty());
    }

    #[tokio::test]
    async fn request_chain_runs_in_registration_order_and_maps_refusal() {
        let first = Arc::new(Mutator {
            declaration: MiddlewareDeclaration::new("first", [MiddlewareScope::Request]),
        });
        let second = Arc::new(Refuser {
            declaration: MiddlewareDeclaration::new("second", [MiddlewareScope::Request]),
        });
        let chain = MiddlewareChain::new(vec![first, second]).expect("valid chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let error = match chain.request_isolated(&mut request).await {
            Ok(_) => panic!("policy refusal"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            GatewayError::MiddlewareRefused { reason: "policy" }
        ));
        assert_eq!(request.body["changed"], json!(true));
    }

    #[tokio::test]
    async fn fail_open_internal_error_discards_partial_mutation() {
        let mut declaration = MiddlewareDeclaration::new("optional", [MiddlewareScope::Request]);
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        struct Broken {
            declaration: MiddlewareDeclaration,
        }
        impl Middleware for Broken {
            fn declaration(&self) -> &MiddlewareDeclaration {
                &self.declaration
            }
            fn apply(
                &self,
                phase: MiddlewarePhase<'_>,
                _state: Option<&mut gateway_core::MiddlewareState>,
            ) -> gateway_core::MiddlewareResult {
                if let MiddlewarePhase::Request(request) = phase {
                    request.body["must_not_escape"] = json!(true);
                }
                Err(MiddlewareError::Failed)
            }
        }
        let chain =
            MiddlewareChain::new(vec![Arc::new(Broken { declaration })]).expect("valid chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let original = request.clone();
        chain
            .request_isolated(&mut request)
            .await
            .expect("fail open");
        assert_eq!(request, original);
    }

    struct RoutingMutator {
        declaration: MiddlewareDeclaration,
    }

    impl Middleware for RoutingMutator {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if let MiddlewarePhase::Request(request) = phase {
                request.model = "rerouted".to_owned();
                request.body["model"] = json!("rerouted");
                request.body["stream"] = json!(false);
                request.body["previous_response_id"] = json!("rerouted-continuation");
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    #[tokio::test]
    async fn routing_field_mutation_obeys_failure_posture_and_never_escapes() {
        let mut fail_open = MiddlewareDeclaration::new("optional", [MiddlewareScope::Request]);
        fail_open.failure_posture = MiddlewareFailurePosture::FailOpen;
        let open_chain = MiddlewareChain::new(vec![Arc::new(RoutingMutator {
            declaration: fail_open,
        })])
        .expect("valid chain");
        let mut open_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({
                "model": "alias",
                "stream": true,
                "previous_response_id": "pinned-continuation",
            }),
        };
        let open_original = open_request.clone();
        open_chain
            .request_isolated(&mut open_request)
            .await
            .expect("fail-open routing mutation is discarded");
        assert_eq!(open_request, open_original);

        let closed_chain = MiddlewareChain::new(vec![Arc::new(RoutingMutator {
            declaration: MiddlewareDeclaration::new("required", [MiddlewareScope::Request]),
        })])
        .expect("valid chain");
        let mut closed_request = open_original.clone();
        assert!(matches!(
            closed_chain.request_isolated(&mut closed_request).await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
        assert_eq!(closed_request, open_original);
    }

    #[tokio::test]
    async fn explicit_refusal_survives_routing_mutation_even_when_fail_open() {
        struct RoutingRefuser {
            declaration: MiddlewareDeclaration,
        }
        impl Middleware for RoutingRefuser {
            fn declaration(&self) -> &MiddlewareDeclaration {
                &self.declaration
            }
            fn apply(
                &self,
                phase: MiddlewarePhase<'_>,
                _state: Option<&mut gateway_core::MiddlewareState>,
            ) -> gateway_core::MiddlewareResult {
                if let MiddlewarePhase::Request(request) = phase {
                    request.model = "must-not-escape".to_owned();
                    request.body["stream"] = json!(false);
                }
                Ok(MiddlewareOutcome::refuse(MiddlewareRefusal::InvalidRequest))
            }
        }

        let mut declaration = MiddlewareDeclaration::new("guard", [MiddlewareScope::Request]);
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        let chain = MiddlewareChain::new(vec![Arc::new(RoutingRefuser { declaration })])
            .expect("valid chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({"stream": true}),
        };
        let original = request.clone();
        assert!(matches!(
            chain.request_isolated(&mut request).await,
            Err(GatewayError::MiddlewareRefused {
                reason: "invalid_request"
            })
        ));
        assert_eq!(request, original);
    }

    #[tokio::test]
    async fn invocation_bound_is_enforced_before_a_mutation_can_commit() {
        struct Slow {
            declaration: MiddlewareDeclaration,
        }
        impl Middleware for Slow {
            fn declaration(&self) -> &MiddlewareDeclaration {
                &self.declaration
            }
            fn apply(
                &self,
                phase: MiddlewarePhase<'_>,
                _state: Option<&mut gateway_core::MiddlewareState>,
            ) -> gateway_core::MiddlewareResult {
                std::thread::sleep(Duration::from_millis(50));
                if let MiddlewarePhase::Request(request) = phase {
                    request.body["late"] = json!(true);
                }
                Ok(MiddlewareOutcome::continue_without_state())
            }
        }

        let mut declaration = MiddlewareDeclaration::new("slow", [MiddlewareScope::Request]);
        declaration.max_duration = Duration::from_millis(1);
        let chain =
            MiddlewareChain::new(vec![Arc::new(Slow { declaration })]).expect("bounded chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let original = request.clone();
        assert!(matches!(
            chain
                .request_with_slots(&mut request, Arc::new(Semaphore::new(1)))
                .await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
        assert_eq!(request, original);
    }

    #[tokio::test]
    async fn healthy_invocation_waits_for_blocking_capacity_within_its_bound() {
        let mut declaration = MiddlewareDeclaration::new("queued", [MiddlewareScope::Request]);
        declaration.max_duration = Duration::from_secs(1);
        let chain = Arc::new(
            MiddlewareChain::new(vec![Arc::new(Noop { declaration })]).expect("bounded chain"),
        );
        let slots = Arc::new(Semaphore::new(1));
        let held = Arc::clone(&slots).acquire_owned().await.expect("test slot");
        let waiting = tokio::spawn({
            let chain = Arc::clone(&chain);
            let slots = Arc::clone(&slots);
            async move {
                let mut request = ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({}),
                };
                chain.request_with_slots(&mut request, slots).await
            }
        });

        tokio::task::yield_now().await;
        assert!(
            !waiting.is_finished(),
            "capacity contention must wait instead of refusing immediately"
        );
        drop(held);
        tokio::time::timeout(Duration::from_millis(250), waiting)
            .await
            .expect("queued invocation should complete")
            .expect("invocation task")
            .expect("healthy middleware should continue");
    }

    #[tokio::test]
    async fn capacity_wait_is_part_of_the_declared_end_to_end_bound() {
        let mut declaration = MiddlewareDeclaration::new("queued", [MiddlewareScope::Request]);
        declaration.max_duration = Duration::from_millis(1);
        let chain =
            MiddlewareChain::new(vec![Arc::new(Noop { declaration })]).expect("bounded chain");
        let slots = Arc::new(Semaphore::new(1));
        let _held = Arc::clone(&slots).acquire_owned().await.expect("test slot");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };

        assert!(matches!(
            chain.request_with_slots(&mut request, slots).await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
    }

    #[tokio::test]
    async fn an_abandoned_invocation_quarantines_only_its_id_until_it_returns() {
        struct Slow {
            declaration: MiddlewareDeclaration,
            calls: Arc<AtomicUsize>,
        }
        impl Middleware for Slow {
            fn declaration(&self) -> &MiddlewareDeclaration {
                &self.declaration
            }
            fn apply(
                &self,
                _phase: MiddlewarePhase<'_>,
                _state: Option<&mut gateway_core::MiddlewareState>,
            ) -> gateway_core::MiddlewareResult {
                self.calls.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(500));
                Ok(MiddlewareOutcome::continue_without_state())
            }
        }

        let mut declaration = MiddlewareDeclaration::new("stuck", [MiddlewareScope::Request]);
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        // Leave ample executor-scheduling time so this specifically exercises
        // a closure abandoned after it starts, not the pre-spawn deadline gate.
        declaration.max_duration = Duration::from_millis(100);
        let calls = Arc::new(AtomicUsize::new(0));
        let chain = MiddlewareChain::new(vec![
            Arc::new(Slow {
                declaration,
                calls: Arc::clone(&calls),
            }),
            Arc::new(Mutator {
                declaration: MiddlewareDeclaration::new(
                    "required-peer",
                    [MiddlewareScope::Request],
                ),
            }),
        ])
        .expect("bounded chain");
        let runtime = MiddlewareRuntime::with_slots(Arc::new(Semaphore::new(2)));
        let mut first = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };

        chain
            .request(&runtime, &mut first)
            .await
            .expect("the timed-out optional middleware must not disable its required peer");
        assert_eq!(first.body["changed"], json!(true));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(runtime.gate("stuck").abandoned.load(Ordering::Acquire), 1);

        let mut second = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        tokio::time::timeout(
            Duration::from_millis(75),
            chain.request(&runtime, &mut second),
        )
        .await
        .expect("the quarantined id must be skipped without waiting")
        .expect("the required peer must still run");
        assert_eq!(second.body["changed"], json!(true));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        tokio::time::sleep(Duration::from_millis(450)).await;
        let mut recovered = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        chain
            .request(&runtime, &mut recovered)
            .await
            .expect("the id is retried after its abandoned invocation returns");
        assert_eq!(recovered.body["changed"], json!(true));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn an_exhausted_deadline_is_detected_before_blocking_work_is_spawned() {
        assert!(invocation_deadline_expired(tokio::time::Instant::now()));
        assert!(!invocation_deadline_expired(
            tokio::time::Instant::now() + Duration::from_secs(1)
        ));
    }

    #[test]
    fn invalid_registration_is_rejected_before_activation() {
        let mut declaration = MiddlewareDeclaration::new("bad", [MiddlewareScope::Request]);
        declaration.mutates_response = true;
        let error = match MiddlewareChain::new(vec![Arc::new(Noop { declaration })]) {
            Ok(_) => panic!("response mutation must declare response scope"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            MiddlewareChainError::ResponseMutationWithoutScope(_)
        ));
    }

    #[test]
    fn all_implemented_scopes_are_accepted_at_registration() {
        let declaration = MiddlewareDeclaration::new(
            "all-scopes",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        );
        let chain = MiddlewareChain::new(vec![Arc::new(Noop { declaration })])
            .expect("every declared scope has an invocation path");
        assert_eq!(chain.len(), 1);
    }

    #[test]
    fn response_mutation_is_selected_for_the_executed_scope_only() {
        let mut declaration =
            MiddlewareDeclaration::new("response-only", [MiddlewareScope::Response]);
        declaration.mutates_response = true;
        let chain =
            MiddlewareChain::new(vec![Arc::new(Noop { declaration })]).expect("response chain");

        assert!(chain.has_response_mutator(MiddlewareScope::Response));
        assert!(!chain.has_response_mutator(MiddlewareScope::StreamEvent));
        assert_eq!(
            chain.response_only_ids().collect::<Vec<_>>(),
            ["response-only"]
        );
    }

    #[test]
    fn scope_presence_does_not_require_response_mutation() {
        let declaration =
            MiddlewareDeclaration::new("stream-observer", [MiddlewareScope::StreamEvent]);
        let chain = MiddlewareChain::new(vec![Arc::new(Noop { declaration })])
            .expect("stream observer chain");

        assert!(chain.has_scope(MiddlewareScope::StreamEvent));
        assert!(!chain.has_response_mutator(MiddlewareScope::StreamEvent));
        assert!(!chain.has_scope(MiddlewareScope::Response));
    }

    #[derive(Debug)]
    struct PhaseCounts {
        responses: usize,
        stream_events: usize,
    }

    struct OrderedStateful {
        declaration: MiddlewareDeclaration,
        marker: &'static str,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl Middleware for OrderedStateful {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            match phase {
                MiddlewarePhase::Request(_) => {
                    self.calls
                        .lock()
                        .expect("call log")
                        .push(format!("{}:request", self.marker));
                    Ok(MiddlewareOutcome::continue_with_state(
                        MiddlewareState::new(PhaseCounts {
                            responses: 0,
                            stream_events: 0,
                        }),
                    ))
                }
                MiddlewarePhase::Response(response) => {
                    let counts = state
                        .and_then(|state| state.downcast_mut::<PhaseCounts>())
                        .expect("request state is reused for response");
                    counts.responses += 1;
                    response.body["order"]
                        .as_array_mut()
                        .expect("response order array")
                        .push(json!(self.marker));
                    self.calls
                        .lock()
                        .expect("call log")
                        .push(format!("{}:response", self.marker));
                    Ok(MiddlewareOutcome::continue_without_state())
                }
                MiddlewarePhase::StreamEvent(event) => {
                    let counts = state
                        .and_then(|state| state.downcast_mut::<PhaseCounts>())
                        .expect("request state is reused for stream event");
                    counts.stream_events += 1;
                    let ProviderStreamEvent::Data { data, .. } = event else {
                        panic!("terminal usage is not dispatched");
                    };
                    data["order"]
                        .as_array_mut()
                        .expect("event order array")
                        .push(json!(self.marker));
                    self.calls
                        .lock()
                        .expect("call log")
                        .push(format!("{}:stream", self.marker));
                    Ok(MiddlewareOutcome::continue_without_state())
                }
            }
        }
    }

    fn ordered_stateful(
        marker: &'static str,
        calls: Arc<Mutex<Vec<String>>>,
    ) -> Arc<dyn Middleware> {
        let mut declaration = MiddlewareDeclaration::new(
            marker,
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        );
        declaration.mutates_response = true;
        Arc::new(OrderedStateful {
            declaration,
            marker,
            calls,
        })
    }

    #[tokio::test]
    async fn execution_pins_state_and_unwinds_response_scopes_in_reverse_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let chain = MiddlewareChain::new(vec![
            ordered_stateful("first", Arc::clone(&calls)),
            ordered_stateful("second", Arc::clone(&calls)),
        ])
        .expect("stateful chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start_isolated(&mut request)
            .await
            .expect("request scopes");
        let mut response = ProviderResponse {
            body: json!({"order": []}),
            usage: ModelUsage::default(),
        };
        execution
            .response(&mut response)
            .await
            .expect("response scopes");
        let mut event = ProviderStreamEvent::Data {
            event: Some("delta".to_owned()),
            data: json!({"order": []}),
        };
        execution
            .stream_event(&mut event)
            .await
            .expect("stream scopes");

        assert_eq!(response.body["order"], json!(["second", "first"]));
        let ProviderStreamEvent::Data { data, .. } = event else {
            panic!("data event remains data");
        };
        assert_eq!(data["order"], json!(["second", "first"]));
        assert_eq!(
            *calls.lock().expect("call log"),
            vec![
                "first:request",
                "second:request",
                "second:response",
                "first:response",
                "second:stream",
                "first:stream",
            ]
        );
        for index in 0..2 {
            let counts = execution
                .states
                .get_mut(index)
                .and_then(|state| state.downcast_ref::<PhaseCounts>());
            let counts = counts.expect("state remains in its original slot");
            assert_eq!(counts.responses, 1);
            assert_eq!(counts.stream_events, 1);
        }
    }

    struct FailingOutputMutator {
        declaration: MiddlewareDeclaration,
    }

    impl Middleware for FailingOutputMutator {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            match phase {
                MiddlewarePhase::Response(response) => response.body["escaped"] = json!(true),
                MiddlewarePhase::StreamEvent(ProviderStreamEvent::Data { data, .. }) => {
                    data["escaped"] = json!(true);
                }
                _ => {}
            }
            Err(MiddlewareError::Failed)
        }
    }

    #[tokio::test]
    async fn fail_open_response_errors_discard_every_partial_output_mutation() {
        let mut declaration = MiddlewareDeclaration::new(
            "optional-output",
            [MiddlewareScope::Response, MiddlewareScope::StreamEvent],
        );
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        declaration.mutates_response = true;
        let chain = MiddlewareChain::new(vec![Arc::new(FailingOutputMutator { declaration })])
            .expect("output chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain.start_isolated(&mut request).await.expect("execution");

        let mut response = ProviderResponse {
            body: json!({"original": true}),
            usage: ModelUsage::default(),
        };
        let original_response = response.clone();
        execution.response(&mut response).await.expect("fail open");
        assert_eq!(response, original_response);

        let mut event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"original": true}),
        };
        let original_event = event.clone();
        execution.stream_event(&mut event).await.expect("fail open");
        assert_eq!(event, original_event);
    }

    struct FailingStatefulOutput {
        declaration: MiddlewareDeclaration,
        output_calls: Arc<AtomicUsize>,
    }

    impl Middleware for FailingStatefulOutput {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            match phase {
                MiddlewarePhase::Request(_) => Ok(MiddlewareOutcome::continue_with_state(
                    MiddlewareState::new(0_usize),
                )),
                MiddlewarePhase::Response(response) => {
                    self.output_calls.fetch_add(1, Ordering::SeqCst);
                    *state
                        .and_then(|state| state.downcast_mut::<usize>())
                        .expect("request state") += 1;
                    response.body["must_not_escape"] = json!(true);
                    Err(MiddlewareError::Failed)
                }
                MiddlewarePhase::StreamEvent(ProviderStreamEvent::Data { data, .. }) => {
                    self.output_calls.fetch_add(1, Ordering::SeqCst);
                    data["must_not_escape"] = json!(true);
                    Err(MiddlewareError::Failed)
                }
                MiddlewarePhase::StreamEvent(ProviderStreamEvent::Done(_)) => {
                    panic!("terminal usage is not dispatched")
                }
            }
        }
    }

    #[tokio::test]
    async fn failed_stateful_callback_is_stranded_instead_of_reused_after_fail_open() {
        let output_calls = Arc::new(AtomicUsize::new(0));
        let mut declaration = MiddlewareDeclaration::new(
            "failing-stateful-output",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        );
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        declaration.mutates_response = true;
        let chain = MiddlewareChain::new(vec![Arc::new(FailingStatefulOutput {
            declaration,
            output_calls: Arc::clone(&output_calls),
        })])
        .expect("stateful output chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start_isolated(&mut request)
            .await
            .expect("request state");
        let mut response = ProviderResponse {
            body: json!({"original": true}),
            usage: ModelUsage::default(),
        };
        execution.response(&mut response).await.expect("fail open");
        assert_eq!(response.body, json!({"original": true}));
        assert!(execution.stranded[0]);

        let mut event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"original": true}),
        };
        execution
            .stream_event(&mut event)
            .await
            .expect("stranded fail-open slot is skipped");
        assert_eq!(
            event,
            ProviderStreamEvent::Data {
                event: None,
                data: json!({"original": true}),
            }
        );
        assert_eq!(output_calls.load(Ordering::SeqCst), 1);
    }

    struct RefusingOutputMutator {
        declaration: MiddlewareDeclaration,
    }

    impl Middleware for RefusingOutputMutator {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if let MiddlewarePhase::Response(response) = phase {
                response.body["must_not_escape"] = json!(true);
            }
            Ok(MiddlewareOutcome::refuse(MiddlewareRefusal::Policy))
        }
    }

    #[tokio::test]
    async fn explicit_output_refusal_is_never_weakened_by_fail_open() {
        let mut declaration =
            MiddlewareDeclaration::new("output-guard", [MiddlewareScope::Response]);
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        declaration.mutates_response = true;
        let chain = MiddlewareChain::new(vec![Arc::new(RefusingOutputMutator { declaration })])
            .expect("output chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain.start_isolated(&mut request).await.expect("execution");
        let mut response = ProviderResponse {
            body: json!({"original": true}),
            usage: ModelUsage::default(),
        };
        let original = response.clone();

        assert!(matches!(
            execution.response(&mut response).await,
            Err(GatewayError::MiddlewareRefused { reason: "policy" })
        ));
        assert_eq!(response, original);
    }

    struct CancellableResponse {
        declaration: MiddlewareDeclaration,
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
        release: Arc<AtomicBool>,
    }

    impl Middleware for CancellableResponse {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if matches!(
                phase,
                MiddlewarePhase::Request(_)
                    | MiddlewarePhase::Response(_)
                    | MiddlewarePhase::StreamEvent(_)
            ) {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.maximum.fetch_max(active, Ordering::SeqCst);
                for _ in 0..2_000 {
                    if self.release.load(Ordering::Acquire) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    struct ResponsePeer {
        declaration: MiddlewareDeclaration,
        calls: Arc<AtomicUsize>,
    }

    impl Middleware for ResponsePeer {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if let MiddlewarePhase::Response(response) = phase {
                self.calls.fetch_add(1, Ordering::SeqCst);
                response.body["peer"] = json!(true);
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    #[tokio::test]
    async fn cancelled_request_quarantines_only_its_id_until_blocking_work_exits() {
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));

        let mut cancelled_declaration =
            MiddlewareDeclaration::new("cancelled-request", [MiddlewareScope::Request]);
        cancelled_declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        cancelled_declaration.max_duration = Duration::from_secs(5);
        let chain = Arc::new(
            MiddlewareChain::new(vec![
                Arc::new(CancellableResponse {
                    declaration: cancelled_declaration,
                    calls: Arc::clone(&calls),
                    active: Arc::clone(&active),
                    maximum: Arc::clone(&maximum),
                    release: Arc::clone(&release),
                }),
                Arc::new(Mutator {
                    declaration: MiddlewareDeclaration::new(
                        "request-peer",
                        [MiddlewareScope::Request],
                    ),
                }),
            ])
            .expect("request chain"),
        );
        let runtime = MiddlewareRuntime::with_slots(Arc::new(Semaphore::new(2)));
        let cancelled = tokio::spawn({
            let chain = Arc::clone(&chain);
            let runtime = runtime.clone();
            async move {
                let mut request = ProviderRequest {
                    model: "alias".to_owned(),
                    body: json!({}),
                };
                chain.request(&runtime, &mut request).await
            }
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking request callback starts");
        cancelled.abort();
        let join_error = cancelled.await.err().expect("request future is cancelled");
        assert!(join_error.is_cancelled());
        let gate = runtime.gate("cancelled-request");
        assert_eq!(gate.abandoned.load(Ordering::Acquire), 1);

        let mut quarantined = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        tokio::time::timeout(
            Duration::from_millis(100),
            chain.request(&runtime, &mut quarantined),
        )
        .await
        .expect("quarantined request id is skipped without waiting")
        .expect("fail-open quarantine leaves the peer available");
        assert_eq!(quarantined.body["changed"], json!(true));
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(maximum.load(Ordering::Acquire), 1);

        release.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.abandoned.load(Ordering::Acquire) != 0 || active.load(Ordering::Acquire) != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking request callback exits and clears quarantine");

        let mut recovered = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        chain
            .request(&runtime, &mut recovered)
            .await
            .expect("request middleware recovers after callback exit");
        assert_eq!(recovered.body["changed"], json!(true));
        assert_eq!(calls.load(Ordering::Acquire), 2);
        assert_eq!(maximum.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn cancelled_response_quarantines_only_its_id_until_blocking_work_exits() {
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let peer_calls = Arc::new(AtomicUsize::new(0));

        let mut peer_declaration =
            MiddlewareDeclaration::new("response-peer", [MiddlewareScope::Response]);
        peer_declaration.mutates_response = true;
        let mut cancelled_declaration =
            MiddlewareDeclaration::new("cancelled-response", [MiddlewareScope::Response]);
        cancelled_declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        cancelled_declaration.max_duration = Duration::from_secs(5);
        let chain = MiddlewareChain::new(vec![
            Arc::new(ResponsePeer {
                declaration: peer_declaration,
                calls: Arc::clone(&peer_calls),
            }),
            Arc::new(CancellableResponse {
                declaration: cancelled_declaration,
                calls: Arc::clone(&calls),
                active: Arc::clone(&active),
                maximum: Arc::clone(&maximum),
                release: Arc::clone(&release),
            }),
        ])
        .expect("cancellable response chain");
        let runtime = MiddlewareRuntime::with_slots(Arc::new(Semaphore::new(2)));

        let mut first_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut first_execution = chain
            .start(&runtime, &mut first_request)
            .await
            .expect("first execution");
        let first = tokio::spawn(async move {
            let mut response = ProviderResponse {
                body: json!({}),
                usage: ModelUsage::default(),
            };
            first_execution.response(&mut response).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking response middleware starts");

        first.abort();
        assert!(
            first
                .await
                .expect_err("response future is cancelled")
                .is_cancelled()
        );
        let cancelled_gate = runtime.gate("cancelled-response");
        assert_eq!(cancelled_gate.abandoned.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(active.load(Ordering::SeqCst), 1);

        let mut second_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut second_execution = chain
            .start(&runtime, &mut second_request)
            .await
            .expect("second execution");
        let mut second_response = ProviderResponse {
            body: json!({}),
            usage: ModelUsage::default(),
        };
        tokio::time::timeout(
            Duration::from_millis(100),
            second_execution.response(&mut second_response),
        )
        .await
        .expect("quarantined id is skipped without waiting")
        .expect("fail-open quarantine leaves peers available");
        assert_eq!(second_response.body["peer"], json!(true));
        assert_eq!(peer_calls.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);

        release.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while cancelled_gate.abandoned.load(Ordering::Acquire) != 0
                || active.load(Ordering::Acquire) != 0
                || cancelled_gate.slots.available_permits()
                    != MAX_BLOCKING_INVOCATIONS_PER_MIDDLEWARE
                || runtime.slots.available_permits() != 2
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closure exit clears quarantine");
        assert_eq!(
            cancelled_gate.slots.available_permits(),
            MAX_BLOCKING_INVOCATIONS_PER_MIDDLEWARE
        );
        assert_eq!(runtime.slots.available_permits(), 2);

        let mut recovered_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut recovered_execution = chain
            .start(&runtime, &mut recovered_request)
            .await
            .expect("recovered execution");
        let mut recovered_response = ProviderResponse {
            body: json!({}),
            usage: ModelUsage::default(),
        };
        recovered_execution
            .response(&mut recovered_response)
            .await
            .expect("middleware id is retried after recovery");
        assert_eq!(recovered_response.body["peer"], json!(true));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(peer_calls.load(Ordering::SeqCst), 2);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_stream_event_quarantines_and_recovers_without_overlapping_calls() {
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));

        let mut declaration =
            MiddlewareDeclaration::new("cancelled-stream", [MiddlewareScope::StreamEvent]);
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        declaration.max_duration = Duration::from_secs(5);
        let chain = MiddlewareChain::new(vec![Arc::new(CancellableResponse {
            declaration,
            calls: Arc::clone(&calls),
            active: Arc::clone(&active),
            maximum: Arc::clone(&maximum),
            release: Arc::clone(&release),
        })])
        .expect("cancellable stream chain");
        let runtime = MiddlewareRuntime::with_slots(Arc::new(Semaphore::new(1)));

        let mut first_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut first_execution = chain
            .start(&runtime, &mut first_request)
            .await
            .expect("first execution");
        let first = tokio::spawn(async move {
            let mut event = ProviderStreamEvent::Data {
                event: None,
                data: json!({"delta": "first"}),
            };
            first_execution.stream_event(&mut event).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking stream middleware starts");

        first.abort();
        assert!(
            first
                .await
                .expect_err("stream future is cancelled")
                .is_cancelled()
        );
        let gate = runtime.gate("cancelled-stream");
        assert_eq!(gate.abandoned.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut second_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut second_execution = chain
            .start(&runtime, &mut second_request)
            .await
            .expect("second execution");
        let mut second_event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"delta": "second"}),
        };
        tokio::time::timeout(
            Duration::from_millis(100),
            second_execution.stream_event(&mut second_event),
        )
        .await
        .expect("quarantined stream id is skipped without waiting")
        .expect("fail-open quarantine preserves the stream");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);

        release.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.abandoned.load(Ordering::Acquire) != 0
                || active.load(Ordering::Acquire) != 0
                || gate.slots.available_permits() != MAX_BLOCKING_INVOCATIONS_PER_MIDDLEWARE
                || runtime.slots.available_permits() != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closure exit clears stream quarantine");
        assert_eq!(
            gate.slots.available_permits(),
            MAX_BLOCKING_INVOCATIONS_PER_MIDDLEWARE
        );
        assert_eq!(runtime.slots.available_permits(), 1);

        let mut recovered_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut recovered_execution = chain
            .start(&runtime, &mut recovered_request)
            .await
            .expect("recovered execution");
        let mut recovered_event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"delta": "recovered"}),
        };
        recovered_execution
            .stream_event(&mut recovered_event)
            .await
            .expect("middleware id is retried after recovery");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn quarantined_stream_addon_warns_once_per_execution() {
        crate::telemetry::testing::keep_callsites_answerable();
        let logged = Arc::new(Mutex::new(Vec::<u8>::new()));
        let sink = Arc::clone(&logged);
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || MiddlewareLogWriter(Arc::clone(&sink)))
            .with_ansi(false)
            .finish();
        let _log = tracing::subscriber::set_default(subscriber);

        let mut declaration =
            MiddlewareDeclaration::new("quarantined-stream", [MiddlewareScope::StreamEvent]);
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        let chain = MiddlewareChain::new(vec![Arc::new(Noop { declaration })])
            .expect("stream middleware chain");
        let runtime = MiddlewareRuntime::default();
        let gate = runtime.gate("quarantined-stream");
        gate.abandoned.store(1, Ordering::Release);
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start(&runtime, &mut request)
            .await
            .expect("response-lifetime execution");

        for sequence in 0..4 {
            let mut event = ProviderStreamEvent::Data {
                event: None,
                data: json!({"sequence": sequence}),
            };
            execution
                .stream_event(&mut event)
                .await
                .expect("fail-open quarantine preserves every event");
        }

        let log = String::from_utf8(logged.lock().expect("middleware log").clone())
            .expect("utf-8 middleware log");
        assert_eq!(
            log.matches("content middleware invocation failed").count(),
            1,
            "one quarantined add-on must produce one warning per execution: {log}"
        );
        assert!(
            log.contains("quarantined-stream"),
            "warning names the add-on: {log}"
        );
    }

    struct CapacityMeasuredStreamEvent {
        declaration: MiddlewareDeclaration,
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
        release: Arc<AtomicBool>,
    }

    impl Middleware for CapacityMeasuredStreamEvent {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if matches!(phase, MiddlewarePhase::StreamEvent(_)) {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.maximum.fetch_max(active, Ordering::SeqCst);
                while !self.release.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    #[tokio::test]
    async fn concurrent_stream_events_use_bounded_parallel_capacity_without_global_serialisation() {
        const EVENTS: usize = 8;
        const SLOTS: usize = 2;
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let mut declaration =
            MiddlewareDeclaration::new("capacity-stream", [MiddlewareScope::StreamEvent]);
        declaration.max_duration = Duration::from_secs(2);
        let chain = MiddlewareChain::new(vec![Arc::new(CapacityMeasuredStreamEvent {
            declaration,
            calls: Arc::clone(&calls),
            active: Arc::clone(&active),
            maximum: Arc::clone(&maximum),
            release: Arc::clone(&release),
        })])
        .expect("stream-event chain");
        let slots = Arc::new(Semaphore::new(SLOTS));
        let runtime = MiddlewareRuntime::with_slots(Arc::clone(&slots));
        let gate = runtime.gate("capacity-stream");
        let mut tasks = Vec::with_capacity(EVENTS);

        for sequence in 0..EVENTS {
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body: json!({}),
            };
            let mut execution = chain
                .start(&runtime, &mut request)
                .await
                .expect("independent stream execution");
            tasks.push(tokio::spawn(async move {
                let mut event = ProviderStreamEvent::Data {
                    event: None,
                    data: json!({"sequence": sequence}),
                };
                execution.stream_event(&mut event).await
            }));
        }

        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) != SLOTS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("two streams occupy the declared process capacity");
        assert_eq!(calls.load(Ordering::SeqCst), SLOTS);
        assert_eq!(maximum.load(Ordering::SeqCst), SLOTS);
        assert_eq!(slots.available_permits(), 0);

        release.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            for task in tasks {
                task.await
                    .expect("stream-event task")
                    .expect("event is processed within its bound");
            }
        })
        .await
        .expect("queued stream events drain through bounded capacity");

        assert_eq!(calls.load(Ordering::SeqCst), EVENTS);
        assert_eq!(maximum.load(Ordering::SeqCst), SLOTS);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(slots.available_permits(), SLOTS);
        assert_eq!(
            gate.slots.available_permits(),
            MAX_BLOCKING_INVOCATIONS_PER_MIDDLEWARE
        );
    }

    #[tokio::test]
    async fn cancelled_fail_closed_stream_event_refuses_until_the_id_recovers() {
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let mut declaration =
            MiddlewareDeclaration::new("cancelled-closed", [MiddlewareScope::StreamEvent]);
        declaration.max_duration = Duration::from_secs(5);
        let chain = MiddlewareChain::new(vec![Arc::new(CancellableResponse {
            declaration,
            calls: Arc::clone(&calls),
            active: Arc::clone(&active),
            maximum: Arc::clone(&maximum),
            release: Arc::clone(&release),
        })])
        .expect("fail-closed stream chain");
        let runtime = MiddlewareRuntime::with_slots(Arc::new(Semaphore::new(1)));

        let mut first_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut first_execution = chain
            .start(&runtime, &mut first_request)
            .await
            .expect("first execution");
        let first = tokio::spawn(async move {
            let mut event = ProviderStreamEvent::Data {
                event: None,
                data: json!({"delta": "first"}),
            };
            first_execution.stream_event(&mut event).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking stream middleware starts");
        first.abort();
        assert!(
            first
                .await
                .expect_err("stream future is cancelled")
                .is_cancelled()
        );
        let gate = runtime.gate("cancelled-closed");
        assert_eq!(gate.abandoned.load(Ordering::Acquire), 1);

        let mut refused_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut refused_execution = chain
            .start(&runtime, &mut refused_request)
            .await
            .expect("refused execution owner");
        let mut refused_event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"delta": "refused"}),
        };
        assert!(matches!(
            refused_execution.stream_event(&mut refused_event).await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        release.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while gate.abandoned.load(Ordering::Acquire) != 0
                || active.load(Ordering::Acquire) != 0
                || gate.slots.available_permits() != MAX_BLOCKING_INVOCATIONS_PER_MIDDLEWARE
                || runtime.slots.available_permits() != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("fail-closed id recovers after closure exit");

        let mut recovered_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut recovered_execution = chain
            .start(&runtime, &mut recovered_request)
            .await
            .expect("recovered execution");
        let mut recovered_event = ProviderStreamEvent::Data {
            event: None,
            data: json!({"delta": "recovered"}),
        };
        recovered_execution
            .stream_event(&mut recovered_event)
            .await
            .expect("recovered fail-closed id runs again");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }

    struct SlowStatefulOutput {
        declaration: MiddlewareDeclaration,
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
        response_calls: Arc<AtomicUsize>,
    }

    impl Middleware for SlowStatefulOutput {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            match phase {
                MiddlewarePhase::Request(_) => Ok(MiddlewareOutcome::continue_with_state(
                    MiddlewareState::new(0_usize),
                )),
                MiddlewarePhase::Response(response) => {
                    self.response_calls.fetch_add(1, Ordering::SeqCst);
                    let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                    self.maximum.fetch_max(active, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(50));
                    *state
                        .and_then(|state| state.downcast_mut::<usize>())
                        .expect("request state") += 1;
                    response.body["late"] = json!(true);
                    self.active.fetch_sub(1, Ordering::SeqCst);
                    Ok(MiddlewareOutcome::continue_without_state())
                }
                MiddlewarePhase::StreamEvent(_) => Ok(MiddlewareOutcome::continue_without_state()),
            }
        }
    }

    fn slow_stateful_output(
        posture: MiddlewareFailurePosture,
    ) -> (Arc<dyn Middleware>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let response_calls = Arc::new(AtomicUsize::new(0));
        let mut declaration = MiddlewareDeclaration::new(
            "slow-stateful",
            [MiddlewareScope::Request, MiddlewareScope::Response],
        );
        declaration.failure_posture = posture;
        declaration.max_duration = Duration::from_millis(5);
        (
            Arc::new(SlowStatefulOutput {
                declaration,
                active,
                maximum: Arc::clone(&maximum),
                response_calls: Arc::clone(&response_calls),
            }),
            maximum,
            response_calls,
        )
    }

    #[tokio::test]
    async fn timed_out_fail_open_state_is_stranded_and_never_invoked_concurrently() {
        let (entry, maximum, response_calls) =
            slow_stateful_output(MiddlewareFailurePosture::FailOpen);
        let chain = MiddlewareChain::new(vec![entry]).expect("slow chain");
        let runtime = MiddlewareRuntime::default();
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start(&runtime, &mut request)
            .await
            .expect("request state");
        let mut first = ProviderResponse {
            body: json!({}),
            usage: ModelUsage::default(),
        };
        execution.response(&mut first).await.expect("fail open");
        assert_eq!(first.body, json!({}));
        assert!(execution.stranded[0]);

        let mut second = first.clone();
        execution
            .response(&mut second)
            .await
            .expect("stranded fail-open slot is disabled");
        assert_eq!(response_calls.load(Ordering::SeqCst), 1);
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
        let gate = runtime.gate("slow-stateful");
        assert_eq!(gate.abandoned.load(Ordering::Acquire), 1);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(gate.abandoned.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn timed_out_fail_closed_state_remains_failed_for_the_response_lifetime() {
        let (entry, _maximum, response_calls) =
            slow_stateful_output(MiddlewareFailurePosture::FailClosed);
        let chain = MiddlewareChain::new(vec![entry]).expect("slow chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start_isolated(&mut request)
            .await
            .expect("request state");
        let mut response = ProviderResponse {
            body: json!({}),
            usage: ModelUsage::default(),
        };
        assert!(matches!(
            execution.response(&mut response).await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
        assert!(matches!(
            execution.response(&mut response).await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
        assert_eq!(response_calls.load(Ordering::SeqCst), 1);
        tokio::time::sleep(Duration::from_millis(60)).await;
    }

    struct FinalizerProbe {
        declaration: MiddlewareDeclaration,
        order: Arc<Mutex<Vec<String>>>,
        calls: Arc<AtomicUsize>,
    }

    struct FinalizerOwnedState(Arc<AtomicUsize>);

    impl Drop for FinalizerOwnedState {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct StatefulFinalizerProbe {
        declaration: MiddlewareDeclaration,
        drops: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
    }

    impl Middleware for StatefulFinalizerProbe {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if matches!(phase, MiddlewarePhase::Request(_)) {
                return Ok(MiddlewareOutcome::continue_with_state(
                    MiddlewareState::new(FinalizerOwnedState(Arc::clone(&self.drops))),
                ));
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }

        fn finish_stream(
            &self,
            state: Option<&mut MiddlewareState>,
        ) -> Result<(), MiddlewareError> {
            assert!(
                state
                    .and_then(|state| state.downcast_mut::<FinalizerOwnedState>())
                    .is_some(),
                "finalizer receives its request-owned state"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct StatefulBlockingFinalizer {
        declaration: MiddlewareDeclaration,
        drops: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        release: Arc<AtomicBool>,
    }

    impl Middleware for StatefulBlockingFinalizer {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if matches!(phase, MiddlewarePhase::Request(_)) {
                return Ok(MiddlewareOutcome::continue_with_state(
                    MiddlewareState::new(FinalizerOwnedState(Arc::clone(&self.drops))),
                ));
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }

        fn finish_stream(
            &self,
            state: Option<&mut MiddlewareState>,
        ) -> Result<(), MiddlewareError> {
            assert!(
                state
                    .and_then(|state| state.downcast_mut::<FinalizerOwnedState>())
                    .is_some(),
                "blocking finalizer receives its request-owned state"
            );
            self.active.fetch_add(1, Ordering::SeqCst);
            while !self.release.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl Middleware for FinalizerProbe {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            _phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            Ok(MiddlewareOutcome::continue_without_state())
        }

        fn finish_stream(
            &self,
            _state: Option<&mut MiddlewareState>,
        ) -> Result<(), MiddlewareError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.order
                .lock()
                .expect("finalizer order")
                .push(self.declaration.id.clone());
            Ok(())
        }
    }

    struct BlockingFinalizer {
        declaration: MiddlewareDeclaration,
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        maximum: Arc<AtomicUsize>,
        release: Arc<AtomicBool>,
    }

    struct BlockingReleaseGuard(Arc<AtomicBool>);

    impl BlockingReleaseGuard {
        fn new(release: Arc<AtomicBool>) -> Self {
            Self(release)
        }

        fn release(&self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl Drop for BlockingReleaseGuard {
        fn drop(&mut self) {
            self.release();
        }
    }

    impl Middleware for BlockingFinalizer {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            _phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            Ok(MiddlewareOutcome::continue_without_state())
        }

        fn finish_stream(
            &self,
            _state: Option<&mut MiddlewareState>,
        ) -> Result<(), MiddlewareError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.maximum.fetch_max(active, Ordering::SeqCst);
            while !self.release.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    type BlockingFinalizerFixture = (
        Arc<dyn Middleware>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        Arc<AtomicBool>,
    );

    fn blocking_finalizer(
        id: &str,
        posture: MiddlewareFailurePosture,
        bound: Duration,
    ) -> BlockingFinalizerFixture {
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let mut declaration = MiddlewareDeclaration::new(id, [MiddlewareScope::StreamEvent]);
        declaration.failure_posture = posture;
        declaration.max_duration = bound;
        (
            Arc::new(BlockingFinalizer {
                declaration,
                calls: Arc::clone(&calls),
                active: Arc::clone(&active),
                maximum: Arc::clone(&maximum),
                release: Arc::clone(&release),
            }),
            calls,
            active,
            maximum,
            release,
        )
    }

    #[tokio::test]
    async fn stream_finalizer_defaults_to_noop_and_scope_is_explicit() {
        let mut empty = MiddlewareExecution::default();
        assert!(!empty.has_stream_event_scope());
        empty.finish_stream().await.expect("empty finalizer");
        empty
            .finish_stream()
            .await
            .expect("empty finalizer is once-only");

        let response_only = Arc::new(Noop {
            declaration: MiddlewareDeclaration::new(
                "response-only-finalizer",
                [MiddlewareScope::Response],
            ),
        });
        let chain = MiddlewareChain::new(vec![response_only]).expect("response-only chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain.start_isolated(&mut request).await.expect("execution");
        assert!(!execution.has_stream_event_scope());
        execution
            .finish_stream()
            .await
            .expect("no stream callbacks");
    }

    #[tokio::test]
    async fn stream_finalizer_runs_stream_scope_in_reverse_order_at_most_once() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let first = Arc::new(FinalizerProbe {
            declaration: MiddlewareDeclaration::new("first", [MiddlewareScope::StreamEvent]),
            order: Arc::clone(&order),
            calls: Arc::clone(&first_calls),
        });
        let response_only = Arc::new(Noop {
            declaration: MiddlewareDeclaration::new("middle", [MiddlewareScope::Response]),
        });
        let second = Arc::new(FinalizerProbe {
            declaration: MiddlewareDeclaration::new("second", [MiddlewareScope::StreamEvent]),
            order: Arc::clone(&order),
            calls: Arc::clone(&second_calls),
        });
        let chain = MiddlewareChain::new(vec![first, response_only, second]).expect("finalizers");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain.start_isolated(&mut request).await.expect("execution");
        assert!(execution.has_stream_event_scope());
        execution.finish_stream().await.expect("first completion");
        execution
            .finish_stream()
            .await
            .expect("duplicate completion is inert");
        assert_eq!(*order.lock().expect("finalizer order"), ["second", "first"]);
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn successful_finalization_retains_request_state_until_execution_drops() {
        let drops = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let entry = Arc::new(StatefulFinalizerProbe {
            declaration: MiddlewareDeclaration::new(
                "stateful-finalizer",
                [MiddlewareScope::Request, MiddlewareScope::StreamEvent],
            ),
            drops: Arc::clone(&drops),
            calls: Arc::clone(&calls),
        });
        let chain = MiddlewareChain::new(vec![entry]).expect("stateful finalizer chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain.start_isolated(&mut request).await.expect("execution");
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        execution
            .finish_stream()
            .await
            .expect("finalization succeeds");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "successful finalization returns state to the response owner"
        );
        drop(execution);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_finalization_drops_request_state_once_after_blocking_work_exits() {
        let drops = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let release_guard = BlockingReleaseGuard::new(Arc::clone(&release));
        let mut declaration = MiddlewareDeclaration::new(
            "stateful-blocking-finalizer",
            [MiddlewareScope::Request, MiddlewareScope::StreamEvent],
        );
        declaration.max_duration = Duration::from_secs(5);
        let entry = Arc::new(StatefulBlockingFinalizer {
            declaration,
            drops: Arc::clone(&drops),
            active: Arc::clone(&active),
            release: Arc::clone(&release),
        });
        let chain = MiddlewareChain::new(vec![entry]).expect("blocking stateful finalizer chain");
        let runtime = MiddlewareRuntime::default();
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start(&runtime, &mut request)
            .await
            .expect("execution");
        let mut finishing = Box::pin(execution.finish_stream());
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = &mut finishing => panic!("finalizer unexpectedly completed: {result:?}"),
                _ = async {
                    while active.load(Ordering::Acquire) == 0 {
                        tokio::task::yield_now().await;
                    }
                } => {}
            }
        })
        .await
        .expect("blocking finalizer becomes active");
        drop(finishing);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(execution.stranded[0]);
        let gate = runtime.gate("stateful-blocking-finalizer");
        assert_eq!(gate.abandoned.load(Ordering::Acquire), 1);
        release_guard.release();
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) != 0
                || gate.abandoned.load(Ordering::Acquire) != 0
                || drops.load(Ordering::Acquire) != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("abandoned finalizer exits and drops its state");
        drop(execution);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_stream_finalizers_share_bounded_process_and_id_capacity() {
        const EXECUTIONS: usize = 6;
        const SLOTS: usize = 2;
        let (entry, calls, active, maximum, release) = blocking_finalizer(
            "bounded-finalizer",
            MiddlewareFailurePosture::FailClosed,
            Duration::from_secs(2),
        );
        let chain = MiddlewareChain::new(vec![entry]).expect("finalizer chain");
        let slots = Arc::new(Semaphore::new(SLOTS));
        let runtime = MiddlewareRuntime::with_slots(Arc::clone(&slots));
        let mut tasks = Vec::new();
        for _ in 0..EXECUTIONS {
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body: json!({}),
            };
            let mut execution = chain
                .start(&runtime, &mut request)
                .await
                .expect("execution");
            tasks.push(tokio::spawn(async move { execution.finish_stream().await }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) != SLOTS {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("finalizers occupy bounded process capacity");
        assert_eq!(calls.load(Ordering::SeqCst), SLOTS);
        assert_eq!(maximum.load(Ordering::SeqCst), SLOTS);
        assert_eq!(slots.available_permits(), 0);
        release.store(true, Ordering::Release);
        for task in tasks {
            task.await
                .expect("finalizer task")
                .expect("finalizer completes within bound");
        }
        assert_eq!(calls.load(Ordering::SeqCst), EXECUTIONS);
        assert_eq!(maximum.load(Ordering::SeqCst), SLOTS);
        assert_eq!(slots.available_permits(), SLOTS);
    }

    #[tokio::test]
    async fn timed_out_finalizer_quarantines_and_never_double_finalizes() {
        let (entry, calls, active, _maximum, release) = blocking_finalizer(
            "timed-finalizer",
            MiddlewareFailurePosture::FailClosed,
            Duration::from_millis(5),
        );
        let chain = MiddlewareChain::new(vec![entry]).expect("finalizer chain");
        let runtime = MiddlewareRuntime::default();
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start(&runtime, &mut request)
            .await
            .expect("execution");
        assert!(matches!(
            execution.finish_stream().await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
        assert!(execution.stranded[0]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let gate = runtime.gate("timed-finalizer");
        assert_eq!(gate.abandoned.load(Ordering::Acquire), 1);
        assert!(matches!(
            execution.finish_stream().await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) != 0 || gate.abandoned.load(Ordering::Acquire) != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed-out finalizer releases quarantine");
    }

    #[tokio::test]
    async fn cancelled_finalizer_strands_state_and_quarantined_posture_is_enforced() {
        let (entry, calls, active, _maximum, release) = blocking_finalizer(
            "cancelled-finalizer",
            MiddlewareFailurePosture::FailClosed,
            Duration::from_secs(5),
        );
        let release_guard = BlockingReleaseGuard::new(Arc::clone(&release));
        let chain = MiddlewareChain::new(vec![entry]).expect("finalizer chain");
        let runtime = MiddlewareRuntime::default();
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start(&runtime, &mut request)
            .await
            .expect("execution");
        let mut finishing = Box::pin(execution.finish_stream());
        tokio::time::timeout(Duration::from_secs(1), async {
            tokio::select! {
                result = &mut finishing => panic!("finalizer unexpectedly completed: {result:?}"),
                _ = async {
                    while active.load(Ordering::Acquire) == 0 {
                        tokio::task::yield_now().await;
                    }
                } => {}
            }
        })
        .await
        .expect("blocking finalizer becomes active");
        drop(finishing);
        assert!(execution.stranded[0]);
        let gate = runtime.gate("cancelled-finalizer");
        assert_eq!(gate.abandoned.load(Ordering::Acquire), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            execution.finish_stream().await,
            Err(GatewayError::MiddlewareUnavailable)
        ));

        let mut refused_request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut refused = chain
            .start(&runtime, &mut refused_request)
            .await
            .expect("second execution");
        assert!(matches!(
            refused.finish_stream().await,
            Err(GatewayError::MiddlewareUnavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release_guard.release();
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) != 0 || gate.abandoned.load(Ordering::Acquire) != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled finalizer exits and releases quarantine");
    }

    #[tokio::test]
    async fn quarantined_fail_open_finalizer_is_skipped() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut declaration =
            MiddlewareDeclaration::new("optional-finalizer", [MiddlewareScope::StreamEvent]);
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        let chain = MiddlewareChain::new(vec![Arc::new(FinalizerProbe {
            declaration,
            order: Arc::new(Mutex::new(Vec::new())),
            calls: Arc::clone(&calls),
        })])
        .expect("optional finalizer");
        let runtime = MiddlewareRuntime::default();
        let gate = runtime.gate("optional-finalizer");
        gate.abandoned.store(1, Ordering::Release);
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start(&runtime, &mut request)
            .await
            .expect("execution");
        execution
            .finish_stream()
            .await
            .expect("fail-open quarantine skips finalizer");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    struct UsageMutator {
        declaration: MiddlewareDeclaration,
        stream_calls: Arc<AtomicUsize>,
    }

    impl Middleware for UsageMutator {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            match phase {
                MiddlewarePhase::Response(response) => {
                    response.body["changed"] = json!(true);
                    response.usage.output_tokens = 999;
                }
                MiddlewarePhase::StreamEvent(event) => {
                    self.stream_calls.fetch_add(1, Ordering::SeqCst);
                    *event = ProviderStreamEvent::Done(ModelUsage {
                        output_tokens: 999,
                        ..ModelUsage::default()
                    });
                }
                MiddlewarePhase::Request(_) => {}
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    #[tokio::test]
    async fn provider_usage_and_terminal_stream_usage_remain_gateway_owned() {
        let stream_calls = Arc::new(AtomicUsize::new(0));
        let mut declaration = MiddlewareDeclaration::new(
            "usage-mutator",
            [MiddlewareScope::Response, MiddlewareScope::StreamEvent],
        );
        declaration.failure_posture = MiddlewareFailurePosture::FailOpen;
        declaration.mutates_response = true;
        let chain = MiddlewareChain::new(vec![Arc::new(UsageMutator {
            declaration,
            stream_calls: Arc::clone(&stream_calls),
        })])
        .expect("output chain");
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain.start_isolated(&mut request).await.expect("execution");
        let mut response = ProviderResponse {
            body: json!({"original": true}),
            usage: ModelUsage {
                input_tokens: 3,
                output_tokens: 5,
                ..ModelUsage::default()
            },
        };
        let original = response.clone();
        execution
            .response(&mut response)
            .await
            .expect("usage mutation follows fail-open posture");
        assert_eq!(response, original);

        let mut data = ProviderStreamEvent::Data {
            event: Some("delta".to_owned()),
            data: json!({"original": true}),
        };
        let original_data = data.clone();
        execution
            .stream_event(&mut data)
            .await
            .expect("synthetic terminal usage follows fail-open posture");
        assert_eq!(data, original_data);
        assert_eq!(stream_calls.load(Ordering::SeqCst), 1);

        let usage = ModelUsage {
            output_tokens: 8,
            ..ModelUsage::default()
        };
        let mut done = ProviderStreamEvent::Done(usage);
        execution
            .stream_event(&mut done)
            .await
            .expect("terminal event bypasses middleware");
        assert_eq!(done, ProviderStreamEvent::Done(usage));
        assert_eq!(stream_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn policy_plan_is_namespace_scoped_and_preserves_empty_siblings() {
        let config = config_with_registration(registration(
            "test.policy-marker",
            [MiddlewareScope::Request],
        ));
        let plan =
            MiddlewarePlan::compile(&config, &HashMap::new()).expect("known registration compiles");
        let runtime = MiddlewareRuntime::default();

        let mut alpha = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        plan.for_namespace("alpha")
            .request(&runtime, &mut alpha)
            .await
            .expect("alpha chain runs");
        assert_eq!(alpha.body["policy_middleware"], "test.policy-marker");

        let mut beta = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        plan.for_namespace("beta")
            .request(&runtime, &mut beta)
            .await
            .expect("sibling stays empty");
        assert_eq!(beta.body, json!({}));
    }

    #[test]
    fn unactivatable_policy_is_rejected_during_snapshot_compilation() {
        let unknown = config_with_registration(registration(
            "test.not-compiled",
            [MiddlewareScope::Request],
        ));
        assert!(matches!(
            MiddlewarePlan::compile(&unknown, &HashMap::new()),
            Err(MiddlewarePlanError {
                source: MiddlewarePolicyError::Unknown { .. },
                ..
            })
        ));
    }

    #[test]
    fn policy_registration_accepts_response_and_stream_event_scopes() {
        let config = config_with_registration(registration(
            "test.policy-marker",
            [MiddlewareScope::Response, MiddlewareScope::StreamEvent],
        ));
        let plan =
            MiddlewarePlan::compile(&config, &HashMap::new()).expect("implemented scopes compile");
        assert_eq!(plan.for_namespace("alpha").len(), 1);
    }

    #[tokio::test]
    async fn production_guardrail_is_stable_across_replicas_and_renames_and_restores_output() {
        let mut config = config_with_registration(guardrail_registration());
        config.namespace[0].project = Some(ProjectIdentity {
            tenant: tenant_id(1),
            project: project_id(1),
        });
        let body = policy_body(PolicyScope::Tenant(tenant_id(2)), PolicyEpoch::FIRST.get())
            .with_content_middleware(vec![guardrail_registration()])
            .expect("registration attaches");
        let generation = body.generation(revision_id(2));
        config.namespace[1].policy = Some(NamespacePolicy { body, generation });
        let env = HashMap::from([("GW_GUARDRAIL_KEY".to_owned(), STANDARD.encode([9_u8; 32]))]);
        let plan = MiddlewarePlan::compile(&config, &env).expect("guardrail compiles");
        let independent_plan =
            MiddlewarePlan::compile(&config, &env).expect("a second replica compiles");
        let mut renamed = config.clone();
        renamed.namespace[0].id = "alpha-renamed".to_owned();
        let renamed_plan =
            MiddlewarePlan::compile(&renamed, &env).expect("the renamed projection compiles");
        let runtime = MiddlewareRuntime::default();

        async fn mask(
            plan: &MiddlewarePlan,
            runtime: &MiddlewareRuntime,
            namespace: &str,
        ) -> (String, ProviderResponse) {
            let mut request = ProviderRequest {
                model: "alias".to_owned(),
                body: json!({"messages": [{
                    "role": "user",
                    "content": "alice@example.com"
                }]}),
            };
            let mut execution = plan
                .for_namespace(namespace)
                .start_with_protected_values(
                    runtime,
                    &mut request,
                    &[],
                    MiddlewareSurface::ChatCompletions,
                )
                .await
                .expect("request is masked");
            let token = request.body["messages"][0]["content"]
                .as_str()
                .unwrap()
                .to_owned();
            let mut response = ProviderResponse {
                body: json!({"choices": [{"message": {"content": token}}]}),
                usage: ModelUsage::default(),
            };
            execution
                .response(&mut response)
                .await
                .expect("response is restored");
            (
                request.body["messages"][0]["content"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                response,
            )
        }

        let (alpha_first, alpha_response) = mask(&plan, &runtime, "alpha").await;
        let (alpha_second, _) = mask(&plan, &runtime, "alpha").await;
        let (alpha_other_replica, _) = mask(&independent_plan, &runtime, "alpha").await;
        let (alpha_after_rename, _) = mask(&renamed_plan, &runtime, "alpha-renamed").await;
        let (beta, beta_response) = mask(&plan, &runtime, "beta").await;
        assert_eq!(alpha_first, alpha_second);
        assert_eq!(alpha_first, alpha_other_replica);
        assert_eq!(alpha_first, alpha_after_rename);
        assert_ne!(alpha_first, beta);
        assert_eq!(
            alpha_response.body["choices"][0]["message"]["content"],
            "alice@example.com"
        );
        assert_eq!(
            beta_response.body["choices"][0]["message"]["content"],
            "alice@example.com"
        );
        assert!(
            plan.for_namespace("alpha")
                .has_response_mutator(MiddlewareScope::Response)
        );
        assert!(
            plan.for_namespace("alpha")
                .has_response_mutator(MiddlewareScope::StreamEvent)
        );
    }

    #[test]
    fn block_only_guardrail_declares_validation_without_response_mutation() {
        let registration = registration(
            "axond.redact",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        )
        .with_guardrail(
            ContentGuardrailRegistration::new(
                "GW_GUARDRAIL_KEY",
                vec![GuardrailRule {
                    id: "deny".to_owned(),
                    pattern: "forbidden".to_owned(),
                    action: GuardrailAction::Block,
                }],
            )
            .unwrap(),
        )
        .unwrap();
        let config = config_with_registration(registration);
        let env = HashMap::from([("GW_GUARDRAIL_KEY".to_owned(), STANDARD.encode([9_u8; 32]))]);
        let plan = MiddlewarePlan::compile(&config, &env).expect("block guardrail compiles");
        let chain = plan.for_namespace("alpha");
        assert!(chain.has_scope(MiddlewareScope::StreamEvent));
        assert!(!chain.has_response_mutator(MiddlewareScope::Response));
        assert!(!chain.has_response_mutator(MiddlewareScope::StreamEvent));
    }

    #[test]
    fn production_guardrail_fails_closed_on_key_rule_and_posture_configuration() {
        let config = config_with_registration(guardrail_registration());
        assert!(matches!(
            MiddlewarePlan::compile(&config, &HashMap::new()),
            Err(MiddlewarePlanError {
                source: MiddlewarePolicyError::MissingKey { .. },
                ..
            })
        ));
        let invalid =
            HashMap::from([("GW_GUARDRAIL_KEY".to_owned(), "not-key-material".to_owned())]);
        assert!(matches!(
            MiddlewarePlan::compile(&config, &invalid),
            Err(MiddlewarePlanError {
                source: MiddlewarePolicyError::InvalidKey { .. },
                ..
            })
        ));

        let broad = registration(
            "axond.redact",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        )
        .with_guardrail(
            ContentGuardrailRegistration::new(
                "GW_GUARDRAIL_KEY",
                vec![GuardrailRule {
                    id: "empty".to_owned(),
                    pattern: ".*".to_owned(),
                    action: GuardrailAction::Redact,
                }],
            )
            .unwrap(),
        )
        .unwrap();
        assert!(matches!(
            validate_content_middleware(&[broad]),
            Err(MiddlewarePolicyError::InvalidConfiguration { .. })
        ));

        let missing_guardrail = registration(
            "axond.redact",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        );
        assert!(matches!(
            validate_content_middleware(&[missing_guardrail]),
            Err(MiddlewarePolicyError::InvalidConfiguration { .. })
        ));

        let guardrail = guardrail_registration()
            .guardrail()
            .expect("guardrail fixture")
            .clone();
        let incomplete_scopes = registration(
            "axond.redact",
            [MiddlewareScope::Request, MiddlewareScope::Response],
        )
        .with_guardrail(guardrail.clone())
        .unwrap();
        assert!(matches!(
            validate_content_middleware(&[incomplete_scopes]),
            Err(MiddlewarePolicyError::InvalidConfiguration { .. })
        ));

        let misplaced = registration("test.policy-marker", [MiddlewareScope::Request])
            .with_guardrail(guardrail)
            .unwrap();
        assert!(matches!(
            validate_content_middleware(&[misplaced]),
            Err(MiddlewarePolicyError::InvalidConfiguration { .. })
        ));

        assert_eq!(
            ContentMiddlewareRegistration::new(
                "axond.redact",
                [
                    MiddlewareScope::Request,
                    MiddlewareScope::Response,
                    MiddlewareScope::StreamEvent,
                ],
                MiddlewareFailurePosture::FailOpen,
                25,
            ),
            Err(crate::desired_state::InvalidContentMiddleware::RedactionRequiresFailClosed)
        );
    }
}
