//! Gateway-owned middleware chain orchestration.
//!
//! `gateway-core` defines the I/O-free contract.  This module owns the parts
//! that must remain in the gateway: registration validation, fixed chain order,
//! invocation bounds, failure posture, and mapping to the gateway's stable
//! refusal envelope. Typed policy registration compiles one chain per namespace
//! into the immutable serving snapshot, so hot reload and rollback use the same
//! atomic publication path as routing, pricing, and policy limits.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[cfg(test)]
use gateway_core::MiddlewareDeclaration;
use gateway_core::{
    Middleware, MiddlewareError, MiddlewareFailurePosture, MiddlewareNeed, MiddlewareOutcome,
    MiddlewarePhase, MiddlewareRefusal, MiddlewareScope, MiddlewareStateBag, MiddlewareVerdict,
    ProviderRequest, ProviderResponse, ProviderStreamEvent,
};
use thiserror::Error;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::Config;
use crate::desired_state::ContentMiddlewareRegistration;
use crate::error::GatewayError;

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

type MiddlewareFactory =
    fn(&ContentMiddlewareRegistration) -> Result<Arc<dyn Middleware>, MiddlewarePolicyError>;

/// The in-process implementations this binary knows how to materialize.
///
/// Policy selects from this registry; it cannot load code, grant I/O, or name a
/// core request stage. The first production entry is added by #358. Keeping an
/// empty production registry here makes an early/unknown registration a compile
/// refusal that leaves the last-known-good snapshot serving.
struct MiddlewareRegistry {
    factories: BTreeMap<&'static str, MiddlewareFactory>,
}

impl MiddlewareRegistry {
    fn builtins() -> Self {
        let factories = BTreeMap::new();
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
    ) -> Result<MiddlewareChain, MiddlewarePolicyError> {
        let entries = registrations
            .iter()
            .map(|registration| {
                let factory = self.factories.get(registration.id()).ok_or_else(|| {
                    MiddlewarePolicyError::Unknown {
                        id: registration.id().to_owned(),
                    }
                })?;
                let entry = factory(registration)?;
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
}

/// Validate one policy's registrations against the implementations and scopes
/// compiled into this binary before an administrative candidate is published.
/// Snapshot compilation calls the same registry again as a defence-in-depth
/// check when a revision is hydrated on another replica.
pub(crate) fn validate_content_middleware(
    registrations: &[ContentMiddlewareRegistration],
) -> Result<(), MiddlewarePolicyError> {
    MiddlewareRegistry::builtins()
        .compile(registrations)
        .map(|_| ())
}

/// Every namespace's compiled chain in one serving snapshot.
#[derive(Clone, Default)]
pub struct MiddlewarePlan {
    by_namespace: BTreeMap<String, MiddlewareChain>,
    empty: MiddlewareChain,
}

impl MiddlewarePlan {
    pub fn compile(config: &Config) -> Result<Self, MiddlewarePlanError> {
        let registry = MiddlewareRegistry::builtins();
        let mut by_namespace = BTreeMap::new();
        for namespace in &config.namespace {
            let Some(policy) = &namespace.policy else {
                continue;
            };
            if policy.body.content_middleware().is_empty() {
                continue;
            }
            let chain = registry
                .compile(policy.body.content_middleware())
                .map_err(|source| MiddlewarePlanError {
                    namespace: namespace.id.clone(),
                    source,
                })?;
            by_namespace.insert(namespace.id.clone(), chain);
        }
        Ok(Self {
            by_namespace,
            empty: MiddlewareChain::empty(),
        })
    }

    pub fn for_namespace(&self, namespace: &str) -> &MiddlewareChain {
        self.by_namespace.get(namespace).unwrap_or(&self.empty)
    }
}

#[cfg(test)]
fn declaration(registration: &ContentMiddlewareRegistration) -> MiddlewareDeclaration {
    let mut declaration =
        MiddlewareDeclaration::new(registration.id(), registration.scopes().iter().copied());
    declaration.failure_posture = registration.failure_posture();
    declaration.max_duration = Duration::from_millis(registration.max_duration_milliseconds());
    declaration
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
        self.request_with_runtime(request, runtime).await
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
    /// middleware state slot for the request.
    pub async fn start(
        &self,
        runtime: &MiddlewareRuntime,
        request: &mut ProviderRequest,
    ) -> Result<MiddlewareExecution, GatewayError> {
        let states = self.request(runtime, request).await?;
        Ok(MiddlewareExecution::new(
            self.clone(),
            runtime.clone(),
            states,
        ))
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
        self.request_with_runtime(request, &MiddlewareRuntime::with_slots(slots))
            .await
    }

    async fn request_with_runtime(
        &self,
        request: &mut ProviderRequest,
        runtime: &MiddlewareRuntime,
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
            let invocation_state = Arc::new(Mutex::new(InvocationState::Running));
            let closure_state = Arc::clone(&invocation_state);
            let closure_gate = Arc::clone(&gate);
            let invoked = tokio::time::timeout_at(
                deadline,
                tokio::task::spawn_blocking(move || {
                    let _process_slot = process_slot;
                    let _middleware_slot = middleware_slot;
                    let _invocation_guard = InvocationGuard {
                        state: closure_state,
                        gate: closure_gate,
                    };
                    let result = middleware.apply(MiddlewarePhase::Request(&mut candidate), None);
                    (candidate, result)
                }),
            )
            .await;
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
        )
    }
}

impl MiddlewareExecution {
    fn new(chain: MiddlewareChain, runtime: MiddlewareRuntime, states: MiddlewareStateBag) -> Self {
        let stranded = vec![false; chain.len()];
        Self {
            chain,
            runtime,
            states,
            stranded,
        }
    }

    #[cfg(test)]
    pub(crate) fn from_state_bag_for_test(states: MiddlewareStateBag) -> Self {
        Self {
            chain: MiddlewareChain::empty(),
            runtime: MiddlewareRuntime::default(),
            states,
            stranded: Vec::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.chain.is_empty()
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
                self.chain
                    .failure(index, "state unavailable after abandoned invocation")?;
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
            let mut candidate = response.clone();
            let original_usage = response.usage;
            let mut state = self.states.take(index);
            let closure_state = Arc::clone(&invocation_state);
            let closure_gate = Arc::clone(&gate);
            let invoked = tokio::time::timeout_at(
                deadline,
                tokio::task::spawn_blocking(move || {
                    let _process_slot = process_slot;
                    let _middleware_slot = middleware_slot;
                    let _invocation_guard = InvocationGuard {
                        state: closure_state,
                        gate: closure_gate,
                    };
                    let result =
                        middleware.apply(MiddlewarePhase::Response(&mut candidate), state.as_mut());
                    (candidate, state, result)
                }),
            )
            .await;
            let (candidate, state, mut result) = match invoked {
                Ok(Ok(invoked)) => invoked,
                Ok(Err(_)) => {
                    self.stranded[index] = true;
                    self.chain.failure(index, "invocation task failed")?;
                    continue;
                }
                Err(_) => {
                    mark_invocation_abandoned(&invocation_state, &gate);
                    self.stranded[index] = true;
                    self.chain.failure(index, "invocation bound exceeded")?;
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
            if self.chain.finish(index, result, &mut self.states, false)? {
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
                self.chain
                    .failure(index, "state unavailable after abandoned invocation")?;
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
            let mut candidate = event.clone();
            let mut state = self.states.take(index);
            let closure_state = Arc::clone(&invocation_state);
            let closure_gate = Arc::clone(&gate);
            let invoked = tokio::time::timeout_at(
                deadline,
                tokio::task::spawn_blocking(move || {
                    let _process_slot = process_slot;
                    let _middleware_slot = middleware_slot;
                    let _invocation_guard = InvocationGuard {
                        state: closure_state,
                        gate: closure_gate,
                    };
                    let result = middleware
                        .apply(MiddlewarePhase::StreamEvent(&mut candidate), state.as_mut());
                    (candidate, state, result)
                }),
            )
            .await;
            let (candidate, state, mut result) = match invoked {
                Ok(Ok(invoked)) => invoked,
                Ok(Err(_)) => {
                    self.stranded[index] = true;
                    self.chain.failure(index, "invocation task failed")?;
                    continue;
                }
                Err(_) => {
                    mark_invocation_abandoned(&invocation_state, &gate);
                    self.stranded[index] = true;
                    self.chain.failure(index, "invocation bound exceeded")?;
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
            if self.chain.finish(index, result, &mut self.states, false)? {
                *event = candidate;
            }
        }
        Ok(())
    }

    async fn acquire(&mut self, index: usize) -> Result<Option<InvocationCapacity>, GatewayError> {
        let declaration = self.chain.entries[index].declaration();
        let gate = self.runtime.gate(&declaration.id);
        if gate.abandoned.load(Ordering::Acquire) > 0 {
            self.chain.failure(
                index,
                "middleware id quarantined while an abandoned invocation is still running",
            )?;
            return Ok(None);
        }
        let bound = declaration.max_duration;
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
                self.chain
                    .failure(index, "middleware invocation capacity closed")?;
                return Ok(None);
            }
            Err(_) => {
                crate::telemetry::metrics::record_middleware_capacity_wait(
                    capacity_started.elapsed().as_secs_f64() * 1_000.0,
                    true,
                );
                self.chain.failure(
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
                self.chain
                    .failure(index, "process invocation capacity closed")?;
                return Ok(None);
            }
            Err(_) => {
                crate::telemetry::metrics::record_middleware_capacity_wait(
                    capacity_started.elapsed().as_secs_f64() * 1_000.0,
                    true,
                );
                self.chain
                    .failure(index, "invocation bound exceeded waiting for capacity")?;
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
            self.chain
                .failure(index, "invocation bound exhausted waiting for capacity")?;
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
    use crate::config::NamespacePolicy;
    use crate::desired_state::fixtures::{policy_body, revision_id, tenant_id};
    use crate::desired_state::{PolicyEpoch, PolicyScope};
    use gateway_core::{
        MiddlewareDeclaration, MiddlewareFailurePosture, MiddlewareOutcome, MiddlewareState,
        ModelUsage,
    };
    use serde_json::json;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let mut execution = chain
            .start_isolated(&mut request)
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
        tokio::time::sleep(Duration::from_millis(60)).await;
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
        let plan = MiddlewarePlan::compile(&config).expect("known registration compiles");
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
            MiddlewarePlan::compile(&unknown),
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
        let plan = MiddlewarePlan::compile(&config).expect("implemented scopes compile");
        assert_eq!(plan.for_namespace("alpha").len(), 1);
    }
}
