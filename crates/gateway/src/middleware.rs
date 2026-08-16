//! Gateway-owned middleware chain orchestration.
//!
//! `gateway-core` defines the I/O-free contract.  This module owns the parts
//! that must remain in the gateway: registration validation, fixed chain order,
//! invocation bounds, failure posture, and mapping to the gateway's stable
//! refusal envelope.  The chain is deliberately not attached to `AppState` yet;
//! an empty chain is the production default until typed policy delivery lands.

use std::sync::{Arc, OnceLock};
#[cfg(test)]
use std::time::Duration;

use gateway_core::{
    Middleware, MiddlewareError, MiddlewareFailurePosture, MiddlewareNeed, MiddlewareOutcome,
    MiddlewarePhase, MiddlewareRefusal, MiddlewareScope, MiddlewareStateBag, MiddlewareVerdict,
    ProviderRequest,
};
use thiserror::Error;
use tokio::sync::Semaphore;

use crate::error::GatewayError;

/// Keep abandoned synchronous invocations from consuming Tokio's entire
/// process-wide blocking pool. A timed-out task retains its permit until the
/// middleware actually returns; later calls wait for a slot within their own
/// end-to-end invocation bound instead of spawning unbounded blocked threads.
const MAX_BLOCKING_MIDDLEWARE_INVOCATIONS: usize = 64;

fn blocking_middleware_slots() -> &'static Arc<Semaphore> {
    static SLOTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    SLOTS.get_or_init(|| Arc::new(Semaphore::new(MAX_BLOCKING_MIDDLEWARE_INVOCATIONS)))
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
    #[error("middleware `{id}` declares `{scope}` scope, which is not invoked in v1")]
    ScopeUnsupported { id: String, scope: &'static str },
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
            if let Some(scope) = declaration
                .scopes
                .iter()
                .find(|scope| **scope != MiddlewareScope::Request)
            {
                return Err(MiddlewareChainError::ScopeUnsupported {
                    id: declaration.id.clone(),
                    scope: middleware_scope_name(*scope),
                });
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

    /// Invoke request-scope middleware once, returning the state owner that
    /// must be retained until the response ends.
    pub async fn request(
        &self,
        request: &mut ProviderRequest,
    ) -> Result<MiddlewareStateBag, GatewayError> {
        self.request_with_slots(request, Arc::clone(blocking_middleware_slots()))
            .await
    }

    async fn request_with_slots(
        &self,
        request: &mut ProviderRequest,
        slots: Arc<Semaphore>,
    ) -> Result<MiddlewareStateBag, GatewayError> {
        let mut states = MiddlewareStateBag::new(self.len());
        for (index, entry) in self.entries.iter().enumerate() {
            if !entry.declaration().has_scope(MiddlewareScope::Request) {
                continue;
            }
            let bound = entry.declaration().max_duration;
            let deadline = tokio::time::Instant::now() + bound;
            let slot =
                match tokio::time::timeout_at(deadline, Arc::clone(&slots).acquire_owned()).await {
                    Ok(Ok(slot)) => slot,
                    Ok(Err(_)) => {
                        self.failure(index, "invocation capacity closed")?;
                        continue;
                    }
                    Err(_) => {
                        self.failure(index, "invocation bound exceeded waiting for capacity")?;
                        continue;
                    }
                };
            let mut candidate = request.clone();
            let middleware = Arc::clone(entry);
            let invoked = tokio::time::timeout_at(
                deadline,
                tokio::task::spawn_blocking(move || {
                    let _slot = slot;
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
        tracing::warn!(middleware = %declaration.id, detail, "request middleware invocation failed");
        match declaration.failure_posture {
            MiddlewareFailurePosture::FailOpen => Ok(false),
            MiddlewareFailurePosture::FailClosed => Err(GatewayError::MiddlewareUnavailable),
        }
    }
}

fn routing_fields_unchanged(before: &ProviderRequest, after: &ProviderRequest) -> bool {
    before.model == after.model
        && before.body.get("model") == after.body.get("model")
        && before.body.get("stream") == after.body.get("stream")
}

fn middleware_scope_name(scope: MiddlewareScope) -> &'static str {
    match scope {
        MiddlewareScope::Request => "request",
        MiddlewareScope::Response => "response",
        MiddlewareScope::StreamEvent => "stream_event",
    }
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
    use gateway_core::{MiddlewareDeclaration, MiddlewareFailurePosture, MiddlewareOutcome};
    use serde_json::json;

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
        let states = chain.request(&mut request).await.expect("empty chain");
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
        let error = match chain.request(&mut request).await {
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
        chain.request(&mut request).await.expect("fail open");
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
            body: json!({"model": "alias", "stream": true}),
        };
        let open_original = open_request.clone();
        open_chain
            .request(&mut open_request)
            .await
            .expect("fail-open routing mutation is discarded");
        assert_eq!(open_request, open_original);

        let closed_chain = MiddlewareChain::new(vec![Arc::new(RoutingMutator {
            declaration: MiddlewareDeclaration::new("required", [MiddlewareScope::Request]),
        })])
        .expect("valid chain");
        let mut closed_request = open_original.clone();
        assert!(matches!(
            closed_chain.request(&mut closed_request).await,
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
            chain.request(&mut request).await,
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
            chain.request(&mut request).await,
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
    fn scopes_without_an_invocation_path_are_rejected_before_activation() {
        for (scope, expected) in [
            (MiddlewareScope::Response, "response"),
            (MiddlewareScope::StreamEvent, "stream_event"),
        ] {
            let declaration = MiddlewareDeclaration::new(expected, [scope]);
            assert!(matches!(
                MiddlewareChain::new(vec![Arc::new(Noop { declaration })]),
                Err(MiddlewareChainError::ScopeUnsupported { scope, .. }) if scope == expected
            ));
        }
    }
}
