//! Process-wide admission control: bounded resources and load shedding.
//!
//! Three ceilings, checked in one place before a request is allowed to consume
//! a socket, a buffer, or upstream capacity:
//!
//! * a **tenant** ceiling on concurrent requests for one namespace, checked
//!   first and never queued, so a saturated tenant is refused at its own gate
//!   rather than occupying the shared queue;
//! * a **global** ceiling on concurrent requests, with an optional *bounded*
//!   queue: a request may wait only while both a queue slot and the configured
//!   wait remain, and is shed with a typed error otherwise;
//! * a **stream** ceiling on concurrent open relays, because a stream holds a
//!   socket for as long as the model talks, taken last so a request still
//!   waiting for global capacity does not occupy a stream slot.
//!
//! This is a separate layer from [`crate::rate_limit`], which bounds one
//! authenticated *subject*. Admission bounds the process and the tenant; the
//! per-subject limiter still runs, and both must admit a request.
//!
//! Permits are owned values released in `Drop`, so every exit path — success,
//! upstream failure, client cancellation, timeout, and process teardown —
//! returns capacity without an explicit release call. A streamed request moves
//! its permit into the relay's accounting, which the response body owns, so
//! capacity is held for exactly as long as the stream is open.
//!
//! State is process-local and per-replica, matching the stateless default
//! posture (ADR 0002): with N replicas behind a load balancer each admits up to
//! its own configured ceiling. A fleet-wide ceiling needs shared policy state,
//! which is #150's subject; the seam is [`AdmissionControl::admit`] and the
//! owned [`AdmissionPermit`] it hands back — a distributed limiter can satisfy
//! the same shape without changing a call site.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::AdmissionConfig;
use crate::telemetry::metrics;

/// Resource labels for the admission metrics. A closed vocabulary: admission
/// metrics carry no tenant, subject, or request dimension, so their cardinality
/// is fixed at build time.
pub const RESOURCE_REQUEST: &str = "request";
pub const RESOURCE_STREAM: &str = "stream";
pub const RESOURCE_TENANT: &str = "tenant";
pub const RESOURCE_QUEUE: &str = "queue";
pub const RESOURCE_DIAGNOSTIC: &str = "diagnostic";
/// The ceiling on *authenticating* a diagnostic, which is a different ceiling
/// with a different size: one read that reaches its handler holds one of each,
/// so publishing both under one label would report every reader twice against a
/// denominator that is neither bound.
pub const RESOURCE_DIAGNOSTIC_AUTH: &str = "diagnostic_auth";

/// Concurrent diagnostic reads one replica will answer.
///
/// Not configurable, and deliberately far below any served-traffic ceiling: a
/// diagnostic answers from memory in microseconds, so this is a bound on abuse
/// rather than a capacity dial. It is separate from `max_in_flight` so that a
/// replica saturated by served traffic can still be asked what is wrong with
/// it, and so that polling the diagnostic cannot consume the capacity served
/// traffic needs.
pub const MAX_IN_FLIGHT_DIAGNOSTICS: usize = 8;

/// Concurrent diagnostic *authentications* one replica will attempt.
///
/// Authenticating is the expensive half of a status read — a signature
/// verification, and a revocation-store round trip for a minted token — and it
/// happens before a caller has proved anything, so it cannot be bounded by the
/// ceiling above without letting an anonymous flood hold that ceiling closed
/// against the operators it exists for. Hence two ceilings: a wide one here
/// covering the unauthenticated work, and the narrow one above covering the
/// answer.
///
/// Wide on purpose. It is sized so that filling it takes a flood rather than a
/// few slow credentials — the eight authenticated readers the inner ceiling
/// allows, plus room for the verification of callers who turn out not to be
/// any of them — while still being a number rather than the process's memory.
/// Refusing here costs no I/O, so the refusal is cheap even when the flood is
/// not.
pub const MAX_AUTHENTICATING_DIAGNOSTICS: usize = 64;

/// Largest ceiling a semaphore-backed bound may carry. Config validation refuses
/// anything larger, so an absurd number is a typed boot error rather than an
/// assertion inside the semaphore.
pub const MAX_PERMITS: usize = Semaphore::MAX_PERMITS;

/// Whether the request will hold a stream open, which is what the stream
/// ceiling counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestKind {
    Buffered,
    Streamed,
}

/// Why a request was shed. Each variant maps to one stable error code, and the
/// distinction between "come back" (429) and "the process is full" (503) is
/// made here rather than at the HTTP boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmissionRejection {
    #[error("tenant concurrency limit exceeded")]
    Tenant,
    /// The tenant table is full of *active* tenants. A new tenant is refused
    /// rather than admitted without a ceiling — the same fail-closed choice the
    /// in-memory rate limiter makes for subjects.
    #[error("admission tenant capacity exhausted")]
    TenantCapacity,
    #[error("concurrent stream limit exceeded")]
    Streams,
    #[error("gateway is at its concurrent request limit")]
    Global,
    #[error("admission queue is full")]
    QueueFull,
    #[error("admission queue wait expired")]
    QueueTimeout,
    #[error("concurrent diagnostic limit exceeded")]
    Diagnostics,
}

impl AdmissionRejection {
    /// Every rejection this replica can record, so the metric catalogue can
    /// assert it declares each one rather than discovering the drift in a
    /// dashboard that a new refusal silently falls outside of.
    #[cfg(test)]
    pub const ALL: [Self; 7] = [
        Self::Tenant,
        Self::TenantCapacity,
        Self::Streams,
        Self::Global,
        Self::QueueFull,
        Self::QueueTimeout,
        Self::Diagnostics,
    ];

    /// The stable machine-readable error type a caller matches on.
    pub fn code(self) -> &'static str {
        match self {
            Self::Tenant => "tenant_concurrency_exceeded",
            Self::TenantCapacity => "admission_tenant_capacity_exhausted",
            Self::Streams => "stream_capacity_exhausted",
            Self::Global => "gateway_overloaded",
            Self::QueueFull => "admission_queue_full",
            Self::QueueTimeout => "admission_queue_timeout",
            Self::Diagnostics => "diagnostic_concurrency_exceeded",
        }
    }

    /// A tenant over its own ceiling is a caller-side condition, so it answers
    /// `429`. Every other rejection is the process refusing work it cannot do
    /// right now, which is what `503` means.
    pub fn is_caller_limit(self) -> bool {
        matches!(self, Self::Tenant)
    }

    /// Retry guidance only where it is honest: concurrency frees as in-flight
    /// work completes, so a second is a truthful lower bound. Tenant-table
    /// capacity frees when some *other* tenant goes idle, which this replica
    /// cannot predict, so it advertises nothing.
    pub fn retry_after_seconds(self) -> Option<u64> {
        match self {
            Self::Tenant
            | Self::Streams
            | Self::Global
            | Self::QueueFull
            | Self::QueueTimeout
            | Self::Diagnostics => Some(1),
            Self::TenantCapacity => None,
        }
    }

    /// The bounded metric dimension for this rejection.
    pub(crate) fn scope(self) -> &'static str {
        match self {
            Self::Tenant | Self::TenantCapacity => RESOURCE_TENANT,
            Self::Streams => RESOURCE_STREAM,
            Self::Global => RESOURCE_REQUEST,
            Self::QueueFull | Self::QueueTimeout => RESOURCE_QUEUE,
            Self::Diagnostics => RESOURCE_DIAGNOSTIC,
        }
    }
}

/// The resolved bounds, in the units the request path uses. `None` is
/// deliberately "unbounded" rather than "zero": a bound of 0 in config means
/// the operator turned that ceiling off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdmissionLimits {
    pub max_request_bytes: usize,
    pub max_in_flight: Option<usize>,
    pub max_in_flight_streams: Option<usize>,
    pub max_in_flight_per_tenant: Option<usize>,
    pub max_tenants: usize,
    pub queue_capacity: Option<usize>,
    pub queue_wait: Duration,
    pub max_stream_duration: Option<Duration>,
    pub max_prompt_tokens: Option<u64>,
    pub max_output_tokens: Option<u64>,
    pub max_stream_bytes: Option<u64>,
}

impl From<&AdmissionConfig> for AdmissionLimits {
    fn from(config: &AdmissionConfig) -> Self {
        let bound = |value: usize| (value > 0).then_some(value);
        let bound64 = |value: u64| (value > 0).then_some(value);
        Self {
            max_request_bytes: config.max_request_bytes,
            max_in_flight: bound(config.max_in_flight),
            max_in_flight_streams: bound(config.max_in_flight_streams),
            max_in_flight_per_tenant: bound(config.max_in_flight_per_tenant),
            max_tenants: config.max_tenants,
            queue_capacity: bound(config.queue_capacity),
            queue_wait: Duration::from_millis(config.queue_wait_ms),
            max_stream_duration: (config.max_stream_duration_ms > 0)
                .then(|| Duration::from_millis(config.max_stream_duration_ms)),
            max_prompt_tokens: bound64(config.max_prompt_tokens),
            max_output_tokens: bound64(config.max_output_tokens),
            max_stream_bytes: bound64(config.max_stream_bytes),
        }
    }
}

/// The process's admission gate. Built once at boot and shared by every
/// request; the bounds it was built with are fixed for the process lifetime, so
/// a reloaded `[admission]` section is validated but applied on restart.
pub struct AdmissionControl {
    limits: AdmissionLimits,
    global: Option<Arc<Semaphore>>,
    streams: Option<Arc<Semaphore>>,
    queue: Option<Arc<Semaphore>>,
    diagnostics: Arc<Semaphore>,
    authenticating_diagnostics: Arc<Semaphore>,
    tenants: Arc<TenantTable>,
}

impl AdmissionControl {
    pub fn new(limits: AdmissionLimits) -> Self {
        Self {
            global: limits.max_in_flight.map(|n| Arc::new(Semaphore::new(n))),
            streams: limits
                .max_in_flight_streams
                .map(|n| Arc::new(Semaphore::new(n))),
            queue: limits.queue_capacity.map(|n| Arc::new(Semaphore::new(n))),
            diagnostics: Arc::new(Semaphore::new(MAX_IN_FLIGHT_DIAGNOSTICS)),
            authenticating_diagnostics: Arc::new(Semaphore::new(MAX_AUTHENTICATING_DIAGNOSTICS)),
            tenants: Arc::new(TenantTable {
                limit: limits.max_in_flight_per_tenant,
                max_tenants: limits.max_tenants,
                active: Mutex::new(HashMap::new()),
            }),
            limits,
        }
    }

    pub fn from_config(config: &AdmissionConfig) -> Self {
        Self::new(AdmissionLimits::from(config))
    }

    pub fn limits(&self) -> AdmissionLimits {
        self.limits
    }

    /// Admit one request, or shed it with a typed rejection. The tenant gate is
    /// first and never waits: a tenant at its ceiling must not consume the
    /// shared queue that other tenants are waiting in. The stream gate is last,
    /// so a stream slot is only ever held by a request about to open a stream.
    pub async fn admit(
        &self,
        tenant: &str,
        kind: RequestKind,
    ) -> Result<AdmissionPermit, AdmissionRejection> {
        // Built incrementally so a ceiling refused later releases the ones
        // already taken through the permit's own `Drop`, rather than through a
        // second unwind path that could drift from it.
        let mut permit = AdmissionPermit {
            global: None,
            stream: None,
            tenant: self.tenants.reserve(tenant).map_err(reject)?,
        };
        if let Some(global) = &self.global {
            permit.global = Some(self.acquire_global(global).await?);
            metrics::record_admission_acquired(RESOURCE_REQUEST);
        }
        // Taken after the global wait: a request queued for capacity is not
        // streaming yet, and holding a stream slot while it waits would turn
        // away a caller that could start streaming now.
        if let (RequestKind::Streamed, Some(streams)) = (kind, &self.streams) {
            permit.stream = Some(
                Arc::clone(streams)
                    .try_acquire_owned()
                    .map_err(|_| reject(AdmissionRejection::Streams))?,
            );
            metrics::record_admission_acquired(RESOURCE_STREAM);
        }
        Ok(permit)
    }

    /// Admit one diagnostic read, or shed it.
    ///
    /// Never waits and never queues: a diagnostic that has to queue has stopped
    /// being a diagnostic. Takes no tenant, global, or stream slot, so a caller
    /// polling it cannot displace served traffic — and served traffic at its own
    /// ceiling cannot make the replica unanswerable.
    pub fn admit_diagnostic(&self) -> Result<DiagnosticPermit, AdmissionRejection> {
        let permit = Arc::clone(&self.diagnostics)
            .try_acquire_owned()
            .map_err(|_| reject(AdmissionRejection::Diagnostics))?;
        metrics::record_admission_acquired(RESOURCE_DIAGNOSTIC);
        Ok(DiagnosticPermit {
            resource: RESOURCE_DIAGNOSTIC,
            _permit: Some(permit),
        })
    }

    /// Admit one diagnostic *authentication*, or shed it before any of the work
    /// authenticating costs is done.
    ///
    /// The permit is held across authentication and released when the response
    /// is, so what it bounds is concurrent signature verification and revocation
    /// lookups on a route that admission does not cover. Refused with the same
    /// [`AdmissionRejection::Diagnostics`] as the inner ceiling: both mean "this
    /// replica is answering as many status reads as it will", and a caller has
    /// no action that distinguishes them.
    pub fn admit_diagnostic_authentication(&self) -> Result<DiagnosticPermit, AdmissionRejection> {
        let permit = Arc::clone(&self.authenticating_diagnostics)
            .try_acquire_owned()
            .map_err(|_| reject(AdmissionRejection::Diagnostics))?;
        metrics::record_admission_acquired(RESOURCE_DIAGNOSTIC_AUTH);
        Ok(DiagnosticPermit {
            resource: RESOURCE_DIAGNOSTIC_AUTH,
            _permit: Some(permit),
        })
    }

    /// The global ceiling, with the bounded queue behind it. Without a queue
    /// (the default) saturation is refused immediately, which is the bounded
    /// behavior: a caller learns now rather than after an unbounded wait.
    async fn acquire_global(
        &self,
        global: &Arc<Semaphore>,
    ) -> Result<OwnedSemaphorePermit, AdmissionRejection> {
        if let Ok(permit) = Arc::clone(global).try_acquire_owned() {
            return Ok(permit);
        }
        let Some(queue) = &self.queue else {
            return Err(reject(AdmissionRejection::Global));
        };
        let Ok(slot) = Arc::clone(queue).try_acquire_owned() else {
            return Err(reject(AdmissionRejection::QueueFull));
        };
        let _queued = QueuedRequest { _slot: slot };
        metrics::record_admission_acquired(RESOURCE_QUEUE);
        match tokio::time::timeout(self.limits.queue_wait, Arc::clone(global).acquire_owned()).await
        {
            Ok(Ok(permit)) => Ok(permit),
            // The semaphore is never closed while the process serves; treat a
            // closed gate as saturation rather than admitting past the ceiling.
            Ok(Err(_)) => Err(reject(AdmissionRejection::Global)),
            Err(_) => Err(reject(AdmissionRejection::QueueTimeout)),
        }
    }
}

fn reject(rejection: AdmissionRejection) -> AdmissionRejection {
    metrics::record_admission_rejection(rejection.scope(), rejection.code());
    rejection
}

/// The diagnostic slot one in-flight diagnostic read holds, released on every
/// exit path by `Drop` exactly as a served request's permit is.
pub struct DiagnosticPermit {
    /// The ceiling this permit came from, so it is released on the dimension it
    /// was acquired on rather than on whichever one is named at the drop site.
    resource: &'static str,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Drop for DiagnosticPermit {
    fn drop(&mut self) {
        if self._permit.take().is_some() {
            metrics::record_admission_released(self.resource);
        }
    }
}

/// A request waiting for the global ceiling. Exists to keep the queue gauge and
/// the queue slot symmetric on every exit, including the timeout path.
struct QueuedRequest {
    _slot: OwnedSemaphorePermit,
}

impl Drop for QueuedRequest {
    fn drop(&mut self) {
        metrics::record_admission_released(RESOURCE_QUEUE);
    }
}

/// The capacity one admitted request holds. Dropping it releases every ceiling
/// it took, in any order, exactly once.
pub struct AdmissionPermit {
    global: Option<OwnedSemaphorePermit>,
    stream: Option<OwnedSemaphorePermit>,
    tenant: Option<TenantSlot>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        if self.global.take().is_some() {
            metrics::record_admission_released(RESOURCE_REQUEST);
        }
        if self.stream.take().is_some() {
            metrics::record_admission_released(RESOURCE_STREAM);
        }
        drop(self.tenant.take());
    }
}

/// Per-tenant in-flight counts. Bounded in both dimensions: how many requests
/// one tenant may have in flight, and how many tenants are tracked at once.
/// An entry exists only while a tenant has work in flight, so the table cannot
/// grow with the number of tenants ever seen.
struct TenantTable {
    limit: Option<usize>,
    max_tenants: usize,
    active: Mutex<HashMap<String, usize>>,
}

impl TenantTable {
    fn reserve(self: &Arc<Self>, tenant: &str) -> Result<Option<TenantSlot>, AdmissionRejection> {
        let Some(limit) = self.limit else {
            return Ok(None);
        };
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let in_flight = match active.get_mut(tenant) {
            Some(in_flight) => in_flight,
            None => {
                if active.len() >= self.max_tenants {
                    return Err(AdmissionRejection::TenantCapacity);
                }
                active.entry(tenant.to_owned()).or_insert(0)
            }
        };
        if *in_flight >= limit {
            // A tenant that has never been under its ceiling leaves no entry
            // behind, so a refused first request cannot occupy the table.
            if *in_flight == 0 {
                active.remove(tenant);
            }
            return Err(AdmissionRejection::Tenant);
        }
        *in_flight += 1;
        drop(active);
        metrics::record_admission_acquired(RESOURCE_TENANT);
        Ok(Some(TenantSlot {
            table: Arc::clone(self),
            tenant: tenant.to_owned(),
        }))
    }

    fn release(&self, tenant: &str) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(in_flight) = active.get_mut(tenant) {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                active.remove(tenant);
            }
        }
    }
}

/// One tenant's in-flight slot, released synchronously on drop.
struct TenantSlot {
    table: Arc<TenantTable>,
    tenant: String,
}

impl Drop for TenantSlot {
    fn drop(&mut self) {
        self.table.release(&self.tenant);
        metrics::record_admission_released(RESOURCE_TENANT);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rejection a shed request carried. Permits are not comparable — they
    /// are live capacity — so a test asserts on the error rather than the result.
    fn shed(result: Result<AdmissionPermit, AdmissionRejection>) -> AdmissionRejection {
        result.err().expect("the request is shed")
    }

    fn limits() -> AdmissionLimits {
        AdmissionLimits {
            max_request_bytes: 1024,
            max_in_flight: None,
            max_in_flight_streams: None,
            max_in_flight_per_tenant: None,
            max_tenants: 8,
            queue_capacity: None,
            queue_wait: Duration::ZERO,
            max_stream_duration: None,
            max_prompt_tokens: None,
            max_output_tokens: None,
            max_stream_bytes: None,
        }
    }

    #[tokio::test]
    async fn unbounded_admission_always_admits() {
        let control = AdmissionControl::new(limits());
        for _ in 0..64 {
            control
                .admit("tenant", RequestKind::Streamed)
                .await
                .expect("admit");
        }
    }

    #[tokio::test]
    async fn global_saturation_sheds_with_a_typed_rejection_and_recovers() {
        let control = AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(1),
            ..limits()
        });
        let held = control
            .admit("tenant", RequestKind::Buffered)
            .await
            .expect("first request is admitted");
        assert_eq!(
            shed(control.admit("tenant", RequestKind::Buffered).await),
            AdmissionRejection::Global
        );
        drop(held);
        control
            .admit("tenant", RequestKind::Buffered)
            .await
            .expect("capacity returns when the permit drops");
    }

    #[tokio::test]
    async fn a_saturated_tenant_leaves_other_tenants_their_capacity() {
        let control = AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(4),
            max_in_flight_per_tenant: Some(1),
            ..limits()
        });
        let _noisy = control
            .admit("noisy", RequestKind::Buffered)
            .await
            .expect("first request of the noisy tenant");
        assert_eq!(
            shed(control.admit("noisy", RequestKind::Buffered).await),
            AdmissionRejection::Tenant
        );
        control
            .admit("quiet", RequestKind::Buffered)
            .await
            .expect("another tenant keeps its own capacity");
    }

    #[tokio::test]
    async fn a_refused_tenant_does_not_consume_the_global_ceiling() {
        let control = AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(2),
            max_in_flight_per_tenant: Some(1),
            ..limits()
        });
        let _first = control
            .admit("noisy", RequestKind::Buffered)
            .await
            .expect("admit");
        for _ in 0..8 {
            assert_eq!(
                shed(control.admit("noisy", RequestKind::Buffered).await),
                AdmissionRejection::Tenant
            );
        }
        control
            .admit("quiet", RequestKind::Buffered)
            .await
            .expect("the shed requests never took a global permit");
    }

    #[tokio::test]
    async fn tenant_table_capacity_refuses_new_tenants_rather_than_unbounding() {
        let control = AdmissionControl::new(AdmissionLimits {
            max_in_flight_per_tenant: Some(1),
            max_tenants: 1,
            ..limits()
        });
        let held = control
            .admit("first", RequestKind::Buffered)
            .await
            .expect("admit");
        assert_eq!(
            shed(control.admit("second", RequestKind::Buffered).await),
            AdmissionRejection::TenantCapacity
        );
        drop(held);
        control
            .admit("second", RequestKind::Buffered)
            .await
            .expect("an idle tenant leaves no entry behind");
    }

    #[tokio::test]
    async fn streams_have_their_own_ceiling() {
        let control = AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(8),
            max_in_flight_streams: Some(1),
            ..limits()
        });
        let _open = control
            .admit("tenant", RequestKind::Streamed)
            .await
            .expect("admit");
        assert_eq!(
            shed(control.admit("tenant", RequestKind::Streamed).await),
            AdmissionRejection::Streams
        );
        control
            .admit("tenant", RequestKind::Buffered)
            .await
            .expect("a buffered request is not bound by the stream ceiling");
    }

    /// A request waiting in the queue is not streaming yet, so it must not hold
    /// a stream slot: otherwise callers are told there is no stream capacity
    /// while no stream is open.
    #[tokio::test(start_paused = true)]
    async fn a_queued_request_does_not_occupy_a_stream_slot_while_it_waits() {
        let control = Arc::new(AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(1),
            max_in_flight_streams: Some(1),
            max_in_flight_per_tenant: None,
            queue_capacity: Some(4),
            queue_wait: Duration::from_secs(30),
            ..limits()
        }));
        let held = control
            .admit("first", RequestKind::Buffered)
            .await
            .expect("admit");
        let queued = tokio::spawn({
            let control = Arc::clone(&control);
            async move { control.admit("second", RequestKind::Streamed).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        // The waiter is parked on the global ceiling, not on the stream ceiling,
        // so the slot is still there for whichever request gets capacity first.
        assert_eq!(
            control
                .streams
                .as_ref()
                .expect("a stream ceiling")
                .available_permits(),
            1,
            "a request that is only waiting for capacity holds no stream slot"
        );
        drop(held);
        queued
            .await
            .expect("task")
            .expect("the queued stream takes the free slot once it has capacity");
    }

    #[tokio::test(start_paused = true)]
    async fn a_queued_request_is_admitted_when_capacity_frees() {
        let control = Arc::new(AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(1),
            queue_capacity: Some(1),
            queue_wait: Duration::from_secs(5),
            ..limits()
        }));
        let held = control
            .admit("tenant", RequestKind::Buffered)
            .await
            .expect("admit");
        let queued = tokio::spawn({
            let control = Arc::clone(&control);
            async move { control.admit("tenant", RequestKind::Buffered).await }
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        drop(held);
        queued
            .await
            .expect("task")
            .expect("the queued request is admitted");
    }

    #[tokio::test(start_paused = true)]
    async fn a_queued_request_expires_rather_than_waiting_forever() {
        let control = AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(1),
            queue_capacity: Some(1),
            queue_wait: Duration::from_secs(2),
            ..limits()
        });
        let _held = control
            .admit("tenant", RequestKind::Buffered)
            .await
            .expect("admit");
        let started = tokio::time::Instant::now();
        assert_eq!(
            shed(control.admit("tenant", RequestKind::Buffered).await),
            AdmissionRejection::QueueTimeout
        );
        assert!(started.elapsed() >= Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn the_queue_itself_is_bounded() {
        let control = Arc::new(AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(1),
            queue_capacity: Some(1),
            queue_wait: Duration::from_secs(30),
            ..limits()
        }));
        let _held = control
            .admit("tenant", RequestKind::Buffered)
            .await
            .expect("admit");
        let queued = tokio::spawn({
            let control = Arc::clone(&control);
            async move { control.admit("tenant", RequestKind::Buffered).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            shed(control.admit("tenant", RequestKind::Buffered).await),
            AdmissionRejection::QueueFull
        );
        queued.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn an_abandoned_queued_request_frees_its_queue_slot() {
        let control = Arc::new(AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(1),
            queue_capacity: Some(1),
            queue_wait: Duration::from_secs(2),
            ..limits()
        }));
        let _held = control
            .admit("tenant", RequestKind::Buffered)
            .await
            .expect("admit");
        let queued = tokio::spawn({
            let control = Arc::clone(&control);
            async move { control.admit("tenant", RequestKind::Buffered).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        queued.abort();
        let _ = queued.await;
        // The cancelled waiter released its slot, so the queue admits again
        // rather than staying permanently full.
        // A queue slot the waiter still held would refuse this with `QueueFull`;
        // waiting out `queue_wait` instead proves the slot came back.
        assert_eq!(
            shed(control.admit("tenant", RequestKind::Buffered).await),
            AdmissionRejection::QueueTimeout
        );
    }

    #[tokio::test]
    async fn concurrent_admission_never_exceeds_the_ceiling() {
        let control = Arc::new(AdmissionControl::new(AdmissionLimits {
            max_in_flight: Some(3),
            ..limits()
        }));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let control = Arc::clone(&control);
            tasks.push(tokio::spawn(async move {
                control.admit("tenant", RequestKind::Buffered).await
            }));
        }
        // Permits are held until every task has raced for one, so the count is
        // the ceiling rather than a sequence of admit-and-release.
        let mut held = Vec::new();
        for task in tasks {
            if let Ok(permit) = task.await.expect("task") {
                held.push(permit);
            }
        }
        assert_eq!(held.len(), 3);
    }

    #[test]
    fn rejections_separate_caller_limits_from_process_saturation() {
        assert!(AdmissionRejection::Tenant.is_caller_limit());
        for rejection in [
            AdmissionRejection::Global,
            AdmissionRejection::QueueFull,
            AdmissionRejection::QueueTimeout,
            AdmissionRejection::Streams,
            AdmissionRejection::TenantCapacity,
        ] {
            assert!(!rejection.is_caller_limit(), "{rejection}");
        }
        assert_eq!(
            AdmissionRejection::TenantCapacity.retry_after_seconds(),
            None
        );
        assert_eq!(AdmissionRejection::Global.retry_after_seconds(), Some(1));
    }

    #[test]
    fn zero_means_unbounded_when_limits_are_resolved() {
        let config = AdmissionConfig {
            max_in_flight: 0,
            max_in_flight_streams: 0,
            max_in_flight_per_tenant: 0,
            queue_capacity: 0,
            max_stream_duration_ms: 0,
            max_prompt_tokens: 0,
            max_output_tokens: 0,
            max_stream_bytes: 0,
            ..AdmissionConfig::default()
        };
        let limits = AdmissionLimits::from(&config);
        assert_eq!(limits.max_in_flight, None);
        assert_eq!(limits.max_in_flight_streams, None);
        assert_eq!(limits.max_in_flight_per_tenant, None);
        assert_eq!(limits.queue_capacity, None);
        assert_eq!(limits.max_stream_duration, None);
        assert_eq!(limits.max_prompt_tokens, None);
        assert_eq!(limits.max_output_tokens, None);
        assert_eq!(limits.max_stream_bytes, None);
    }
}
