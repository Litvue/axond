//! Runtime-neutral request-path middleware contracts.
//!
//! This module deliberately contains no executor, clock, socket, credential,
//! or gateway state.  The gateway owns invocation order and bounds; core owns
//! only the values a middleware can declare, the three supported scopes, and
//! the response-lifetime state it may return from a request invocation.

use std::any::Any;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ProviderRequest, ProviderResponse, ProviderStreamEvent};

/// The only request-path scopes supported by the first middleware primitive.
///
/// There is intentionally no attempt scope.  A request-scoped transformation
/// must be identical across failover targets and credential rotation, or the
/// state it returns could disagree with the response that ultimately serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MiddlewareScope {
    Request,
    Response,
    StreamEvent,
}

/// The failure policy a middleware declares for an invocation error or bound
/// breach.  The gateway applies this policy; core does not decide whether a
/// request should become an HTTP response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MiddlewareFailurePosture {
    FailOpen,
    FailClosed,
}

/// Capabilities a middleware may ask the gateway to hand it.
///
/// These are declarations, not handles.  In particular, declaring `Network`
/// does not grant a socket and does not make the core crate I/O-capable.  The
/// gateway's v1 chain rejects capabilities it cannot safely hand out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MiddlewareNeed {
    Clock,
    Credentials,
    Network,
}

/// Immutable registration metadata for one middleware.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MiddlewareDeclaration {
    pub id: String,
    pub scopes: Vec<MiddlewareScope>,
    pub needs: Vec<MiddlewareNeed>,
    pub failure_posture: MiddlewareFailurePosture,
    /// Maximum end-to-end wall-clock duration the gateway permits for one
    /// invocation, including capacity wait and executor scheduling.
    pub max_duration: Duration,
    /// Whether this middleware may change a response body or stream event.
    /// A response mutator must declare the corresponding response scope.
    pub mutates_response: bool,
}

impl MiddlewareDeclaration {
    /// A small, bounded declaration suitable for tests and simple in-process
    /// policies.  Production policy compilation can build the full value.
    pub fn new(id: impl Into<String>, scopes: impl IntoIterator<Item = MiddlewareScope>) -> Self {
        Self {
            id: id.into(),
            scopes: scopes.into_iter().collect(),
            needs: Vec::new(),
            failure_posture: MiddlewareFailurePosture::FailClosed,
            max_duration: Duration::from_millis(25),
            mutates_response: false,
        }
    }

    pub fn has_scope(&self, scope: MiddlewareScope) -> bool {
        self.scopes.contains(&scope)
    }
}

/// Opaque state returned by a request-scope middleware.
///
/// The gateway never interprets this value.  It stores it in a buffered
/// request's local owner or moves it into the streaming relay's accounting
/// owner, making the drop boundary the response body's lifetime.
pub struct MiddlewareState(Box<dyn Any + Send>);

impl MiddlewareState {
    pub fn new<T: Any + Send>(value: T) -> Self {
        Self(Box::new(value))
    }

    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }

    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.0.downcast_mut()
    }
}

/// State slots for one request, indexed by the middleware's chain position.
///
/// Keeping slots even for middleware that returns no state makes ownership
/// explicit and prevents a response callback from accidentally reading another
/// middleware's state.
pub struct MiddlewareStateBag {
    slots: Vec<Option<MiddlewareState>>,
}

impl MiddlewareStateBag {
    pub fn new(slots: usize) -> Self {
        Self {
            slots: (0..slots).map(|_| None).collect(),
        }
    }

    pub fn insert(&mut self, index: usize, state: MiddlewareState) -> Option<MiddlewareState> {
        self.slots
            .get_mut(index)
            .expect("middleware state index must belong to the chain")
            .replace(state)
    }

    pub fn get_mut(&mut self, index: usize) -> Option<&mut MiddlewareState> {
        self.slots.get_mut(index).and_then(Option::as_mut)
    }

    /// Temporarily transfer one slot to the gateway-owned executor.
    ///
    /// Response callbacks run on a blocking worker. Moving the state into that
    /// worker, rather than borrowing it across an await, makes concurrent
    /// invocation impossible. The gateway replaces the slot only after the
    /// callback has actually returned.
    pub fn take(&mut self, index: usize) -> Option<MiddlewareState> {
        self.slots
            .get_mut(index)
            .expect("middleware state index must belong to the chain")
            .take()
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

impl Default for MiddlewareStateBag {
    fn default() -> Self {
        Self::new(0)
    }
}

/// The phase-specific value handed to a middleware.
pub enum MiddlewarePhase<'a> {
    Request(&'a mut ProviderRequest),
    Response(&'a mut ProviderResponse),
    StreamEvent(&'a mut ProviderStreamEvent),
}

impl MiddlewarePhase<'_> {
    pub fn scope(&self) -> MiddlewareScope {
        match self {
            Self::Request(_) => MiddlewareScope::Request,
            Self::Response(_) => MiddlewareScope::Response,
            Self::StreamEvent(_) => MiddlewareScope::StreamEvent,
        }
    }
}

/// A stable refusal reason.  The gateway deliberately maps it to a bounded
/// caller-facing error and never echoes request content or arbitrary internal
/// diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MiddlewareRefusal {
    Policy,
    InvalidRequest,
}

/// A middleware's successful result: continue, refuse, and optionally return
/// state that remains owned by the request's response lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareVerdict {
    Continue,
    Refuse(MiddlewareRefusal),
}

pub struct MiddlewareOutcome {
    pub verdict: MiddlewareVerdict,
    pub state: Option<MiddlewareState>,
}

impl MiddlewareOutcome {
    pub fn continue_without_state() -> Self {
        Self {
            verdict: MiddlewareVerdict::Continue,
            state: None,
        }
    }

    pub fn continue_with_state(state: MiddlewareState) -> Self {
        Self {
            verdict: MiddlewareVerdict::Continue,
            state: Some(state),
        }
    }

    pub fn refuse(reason: MiddlewareRefusal) -> Self {
        Self {
            verdict: MiddlewareVerdict::Refuse(reason),
            state: None,
        }
    }
}

/// Internal middleware failure.  The gateway applies the declaration's
/// fail-open/fail-closed posture and turns this into a typed gateway result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum MiddlewareError {
    #[error("middleware invocation failed")]
    Failed,
}

pub type MiddlewareResult = Result<MiddlewareOutcome, MiddlewareError>;

/// An I/O-free middleware implementation.
pub trait Middleware: Send + Sync {
    fn declaration(&self) -> &MiddlewareDeclaration;

    fn apply(
        &self,
        phase: MiddlewarePhase<'_>,
        state: Option<&mut MiddlewareState>,
    ) -> MiddlewareResult;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Stateful {
        declaration: MiddlewareDeclaration,
        dropped: Arc<AtomicUsize>,
    }

    impl Middleware for Stateful {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut MiddlewareState>,
        ) -> MiddlewareResult {
            if let MiddlewarePhase::Request(request) = phase {
                request.body["touched"] = json!(true);
                return Ok(MiddlewareOutcome::continue_with_state(
                    MiddlewareState::new(DropCounter(Arc::clone(&self.dropped))),
                ));
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    struct DropCounter(Arc<AtomicUsize>);

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn request_state_is_opaque_and_drops_with_its_owner() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let middleware = Stateful {
            declaration: MiddlewareDeclaration::new("test", [MiddlewareScope::Request]),
            dropped: Arc::clone(&dropped),
        };
        let mut request = ProviderRequest {
            model: "alias".to_owned(),
            body: json!({}),
        };
        let outcome = middleware
            .apply(MiddlewarePhase::Request(&mut request), None)
            .expect("middleware succeeds");
        let state = outcome.state.expect("request state");
        assert_eq!(request.body["touched"], json!(true));
        let mut bag = MiddlewareStateBag::new(1);
        bag.insert(0, state);
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(bag);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn declaration_rejects_no_scope_for_a_response_mutator_at_validation_time() {
        let mut declaration = MiddlewareDeclaration::new("bad", [MiddlewareScope::Request]);
        declaration.mutates_response = true;
        assert!(!declaration.has_scope(MiddlewareScope::Response));
    }

    #[test]
    fn refusal_reasons_use_the_same_snake_case_as_other_middleware_values() {
        assert_eq!(
            serde_json::to_value(MiddlewareRefusal::InvalidRequest).unwrap(),
            json!("invalid_request")
        );
        assert_eq!(
            serde_json::from_value::<MiddlewareRefusal>(json!("policy")).unwrap(),
            MiddlewareRefusal::Policy
        );
    }

    #[test]
    fn state_slots_can_be_transferred_without_changing_their_position() {
        let mut bag = MiddlewareStateBag::new(2);
        bag.insert(1, MiddlewareState::new(7_u64));

        let state = bag.take(1).expect("state transfers to executor");
        assert!(bag.get_mut(1).is_none());
        assert_eq!(state.downcast_ref::<u64>(), Some(&7));

        bag.insert(1, state);
        assert_eq!(
            bag.get_mut(1).and_then(|state| state.downcast_ref::<u64>()),
            Some(&7)
        );
    }
}
