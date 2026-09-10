//! Response accounting: budget holds, buffered-response settlement, and usage
//! records.
//!
//! Permit and reservation ownership is here. A [`BudgetReservation`] drop
//! releases a hold that the handler never settled. [`BufferedResponseAccounting`]
//! finishes one durable `ok` / `rejected` / `client_cancelled` decision.
//! Streaming relay accounting stays in [`crate::streaming`]. Settlement
//! capacity is [`crate::settlement`]; this module only spends a reservation
//! taken at admission.

use gateway_core::{Usage, serialized_json_len};
use serde_json::Value;

use crate::admission::AdmissionPermit;
use crate::budget::{BudgetKey, Reservation};
use crate::credentials::CredentialSource;
use crate::error::GatewayError;
use crate::middleware::CoreBudgetHold;
use crate::pricing::RequestPrice;
use crate::rate_limit::RateLimitPermit;
use crate::settlement::{SettlementReservation, SettlementSlot};
use crate::state::{AppState, InboundKey};
use crate::telemetry;
use crate::usage::identity::EventIdentity;
use crate::usage::{Status, UsageRecord};

pub(super) struct BoundedJsonCounter {
    bytes: u64,
    limit: u64,
}

impl std::io::Write for BoundedJsonCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let bytes = u64::try_from(buffer.len())
            .map_err(|_| std::io::Error::other("serialized response length overflow"))?;
        let next = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| std::io::Error::other("serialized response length overflow"))?;
        if next > self.limit {
            return Err(std::io::Error::other(
                "serialized response exceeds configured limit",
            ));
        }
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) fn json_fits_response_limit(body: &Value, limit: u64) -> bool {
    serde_json::to_writer(BoundedJsonCounter { bytes: 0, limit }, body).is_ok()
}
/// The budget reservation a request is dispatched under, plus the input-token
/// estimate it was priced from. The streaming relay needs both: the hold to
/// settle, and the estimate to price a stream that ends before the provider
/// reports authoritative usage.
pub(super) struct BudgetHold {
    pub(super) key: BudgetKey,
    pub(super) reservation: Reservation,
    pub(super) estimated_input_tokens: u64,
    pub(super) permit: Option<RateLimitPermit>,
    /// The admission capacity the request was let in under. Moved into the
    /// stream context that ends up owning the relay, so an open stream keeps
    /// occupying a slot for exactly as long as it is open — and a walk that
    /// never opens one drops it here.
    pub(super) admission: Option<AdmissionPermit>,
}

/// A buffered request's reservation must be reconciled even when its handler is
/// dropped while the upstream request is in flight. Streaming `Accounting`
/// covers cancellation once the relay exists; this guard covers the buffered path.
pub(super) struct BudgetReservation {
    state: AppState,
    key: BudgetKey,
    reservation: Option<Reservation>,
    settlement: SettlementSlot,
}

impl BudgetReservation {
    pub(super) fn new(
        state: AppState,
        key: BudgetKey,
        reservation: Reservation,
        settlement: SettlementSlot,
    ) -> Self {
        Self {
            state,
            key,
            reservation: Some(reservation),
            settlement,
        }
    }

    /// Disarm before awaiting so the explicit release and the drop fallback
    /// cannot both reconcile the same hold.
    pub(super) async fn release(mut self) {
        let reservation = self
            .reservation
            .take()
            .expect("budget reservation guard must be armed");
        self.state.0.budget.release(&self.key, &reservation).await;
    }

    pub(super) fn disarm(mut self) {
        self.reservation.take();
    }

    pub(super) fn into_response_accounting(
        mut self,
        record: UsageRecord,
        ttft_ms: Option<u64>,
        attempts: u32,
        settlement: Option<SettlementReservation>,
    ) -> BufferedResponseAccounting {
        BufferedResponseAccounting {
            state: self.state.clone(),
            hold: Some(BufferedBudgetHold::Legacy {
                key: self.key.clone(),
                reservation: self
                    .reservation
                    .take()
                    .expect("budget reservation guard must be armed"),
            }),
            record: Some(record),
            ttft_ms,
            attempts,
            settlement,
        }
    }
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        let Some(reservation) = self.reservation.take() else {
            return;
        };
        let state = self.state.clone();
        let key = self.key.clone();
        // Same transfer as `CoreBudgetHold`: consume the request's reserved slot
        // so a zero-charge release is not refused because that slot still
        // occupies capacity.
        self.state.0.settlements.spawn_reserved(
            self.settlement.take(),
            async move {
                state.0.budget.release(&key, &reservation).await;
            },
            "zero-charge budget release",
        );
    }
}

/// Owns known provider spend while buffered response middleware runs.
///
/// `client_cancelled` is recorded when this owner drops before middleware
/// produces a terminal outcome. Once middleware has returned, `finish` makes one durable
/// `ok`/`rejected` decision before any accounting await. That status describes
/// the request outcome, not an unknowable proof that the peer received the HTTP
/// response: changing it after an ambiguously acknowledged durable commit would
/// conflict with the immutable event under the same request identity.
pub(super) struct BufferedResponseAccounting {
    state: AppState,
    hold: Option<BufferedBudgetHold>,
    record: Option<UsageRecord>,
    ttft_ms: Option<u64>,
    attempts: u32,
    /// The settlement capacity reserved at admission, spent by whichever of
    /// `finish` and `Drop` spawns the one settlement.
    settlement: Option<SettlementReservation>,
}

pub(super) enum BufferedBudgetHold {
    Legacy {
        key: BudgetKey,
        reservation: Reservation,
    },
    Core(CoreBudgetHold),
}

impl BufferedResponseAccounting {
    pub(super) fn from_core(
        state: AppState,
        hold: CoreBudgetHold,
        record: UsageRecord,
        ttft_ms: Option<u64>,
        attempts: u32,
        settlement: Option<SettlementReservation>,
    ) -> Self {
        Self {
            state,
            hold: Some(BufferedBudgetHold::Core(hold)),
            record: Some(record),
            ttft_ms,
            attempts,
            settlement,
        }
    }

    pub(super) async fn finish(mut self, status: Status) -> Result<(), GatewayError> {
        let hold = self
            .hold
            .take()
            .expect("buffered response accounting must own its budget hold");
        let mut record = self
            .record
            .take()
            .expect("buffered response accounting must own its record");
        record.status = status;
        let decided = spawn_buffered_response_accounting(
            self.state.clone(),
            hold,
            record,
            self.ttft_ms,
            self.attempts,
            self.settlement.take(),
        );
        match decided.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(GatewayError::UsageNotDurable {
                reason: error.reason,
            }),
            // The settlement was abandoned before it reported: its execution
            // deadline expired, or the runtime stopped under it.
            Err(_) => Err(GatewayError::UsageNotDurable {
                reason: "the settlement did not report before it was abandoned",
            }),
        }
    }
}

impl Drop for BufferedResponseAccounting {
    fn drop(&mut self) {
        let Some(hold) = self.hold.take() else {
            return;
        };
        let mut record = self
            .record
            .take()
            .expect("armed buffered response accounting must own its record");
        record.status = Status::ClientCancelled;
        drop(spawn_buffered_response_accounting(
            self.state.clone(),
            hold,
            record,
            self.ttft_ms,
            self.attempts,
            self.settlement.take(),
        ));
    }
}

pub(super) fn spawn_buffered_response_accounting(
    state: AppState,
    hold: BufferedBudgetHold,
    record: UsageRecord,
    ttft_ms: Option<u64>,
    attempts: u32,
    settlement: Option<SettlementReservation>,
) -> tokio::sync::oneshot::Receiver<Result<(), crate::usage::NotDurable>> {
    let (verdict, decided) = tokio::sync::oneshot::channel();
    let settlements = state.0.settlements.clone();
    let accounting = async move {
        match hold {
            BufferedBudgetHold::Legacy { key, reservation } => {
                state
                    .0
                    .budget
                    .settle(&key, &reservation, record.settle_cost())
                    .await;
            }
            BufferedBudgetHold::Core(hold) => {
                hold.settle(record.settle_cost()).await;
            }
        }
        telemetry::record_request(&record, ttft_ms, attempts);
        let result = state.0.usage.record(&record).await;
        if let Err(Err(unheard)) = verdict.send(result) {
            state.0.usage.count_unheard_refusal(&unheard);
        }
    };
    // Known provider spend: the reservation taken at admission is what lets it
    // be spawned unconditionally. Without one (a caller that never reserved),
    // the work is refused loudly rather than run past the bound; the dropped
    // verdict then answers `usage_not_durable`.
    match settlement {
        Some(reserved) => settlements.spawn(reserved, accounting),
        None => {
            if settlements.try_spawn(accounting).is_err() {
                settlements.refuse("buffered response accounting");
            }
        }
    }
    decided
}
pub(super) fn to_usage(u: &gateway_core::ModelUsage) -> Usage {
    Usage {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        reasoning_tokens: u.reasoning_tokens,
        cache_read_tokens: u.cache_read_tokens,
        cache_write_tokens: u.cache_write_tokens,
    }
}

/// Conservative pre-dispatch usage estimate: input tokens from the request body
/// (~4 chars/token) plus an output allowance (`max_tokens` when present, else a
/// default). Used for `max_request_microdollars` and as a fallback when the
/// provider reports no usage. Not held against the namespace cap (ADR 0064).
pub(super) fn estimate_usage(body: &Value) -> (Usage, usize) {
    const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 1_024;
    let body_bytes = serialized_json_len(body).unwrap_or(0);
    let input_tokens = (body_bytes / 4) as u64;
    let output_tokens = requested_output_tokens(body).unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS);
    (
        Usage {
            input_tokens,
            output_tokens,
            reasoning_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        },
        body_bytes,
    )
}

/// Apply the request-derived prompt/output ceilings to one estimate. This is
/// called twice by `serve`: once before admission as a cheap fail-fast for the
/// arriving body, and once after the middleware chain as the authoritative
/// check for the body that will actually be sent upstream.
pub(super) fn check_estimate_bounds(
    body: &Value,
    estimate: Usage,
    limits: crate::admission::AdmissionLimits,
) -> Result<(), GatewayError> {
    if let Some(limit_tokens) = limits.max_prompt_tokens
        && estimate.input_tokens > limit_tokens
    {
        return Err(GatewayError::PromptTooLarge { limit_tokens });
    }
    if let Some(limit_tokens) = limits.max_output_tokens
        && let Some(requested_tokens) = requested_output_tokens(body)
        && requested_tokens > limit_tokens
    {
        return Err(GatewayError::OutputLimitExceeded {
            requested_tokens,
            limit_tokens,
        });
    }
    Ok(())
}

/// The output allowance a request asked for, in whichever spelling its surface
/// uses. `None` when the caller left it to the provider.
///
/// A body carrying several spellings takes the largest of them, because a
/// present-but-unusable field (`null`, a string) must not hide a usable one from
/// the ceiling or from the hold: whichever field the provider honors, the answer
/// here is never below it.
pub(super) fn requested_output_tokens(body: &Value) -> Option<u64> {
    ["max_tokens", "max_completion_tokens", "max_output_tokens"]
        .into_iter()
        .filter_map(|field| body.get(field).and_then(Value::as_u64))
        .max()
}

pub(super) struct RecordArgs<'a> {
    /// The event identity minted when the request was accepted, so the record
    /// carries the id the rest of the request already referred to rather than
    /// one invented at settlement.
    pub(super) identity: &'a EventIdentity,
    pub(super) caller: &'a InboundKey,
    pub(super) alias: &'a str,
    pub(super) target_provider: &'a str,
    pub(super) target_model: &'a str,
    pub(super) source: CredentialSource,
    pub(super) credential_id: &'a str,
    pub(super) status: Status,
    pub(super) input_tokens: u64,
    pub(super) cache_read_tokens: u64,
    pub(super) cache_write_tokens: u64,
    pub(super) output_tokens: u64,
    pub(super) cost_microdollars: Option<u64>,
    /// The pricing the cost was computed at, so the row names the immutable
    /// state it was charged against rather than "whatever is approved now".
    pub(super) price: RequestPrice,
    pub(super) latency_ms: u64,
    /// Time to the first token, when one was produced.
    pub(super) ttft_ms: Option<u64>,
    /// Upstream attempts made; the retry count is one less.
    pub(super) attempts: u32,
    pub(super) attrs: Option<serde_json::Value>,
    pub(super) period: Option<String>,
}

/// Record where the request is already ending for another reason, so a failure
/// to journal can only be reported and counted.
pub(super) async fn record_usage_terminal(
    state: &AppState,
    settlement: Option<SettlementReservation>,
    args: RecordArgs<'_>,
) {
    let (record, ttft_ms, attempts) = build_record(args);
    telemetry::record_request(&record, ttft_ms, attempts);
    if !state.0.usage.appends() {
        state.0.usage.record_terminal(&record).await;
        return;
    }
    // Detached for the same reason [`record_usage`] is: the request this
    // describes already failed, so nothing here changes the response, but a
    // caller hanging up must not be what decides whether the attempt was
    // recorded. Awaited anyway while the handler lives, so an uncancelled
    // request still reaches its sinks before it answers.
    let (done, recorded) = tokio::sync::oneshot::channel();
    let recording = state.clone();
    let record_terminal = async move {
        recording.0.usage.record_terminal(&record).await;
        let _ = done.send(());
    };
    match settlement {
        Some(reserved) => state.0.settlements.spawn(reserved, record_terminal),
        None => {
            // Nothing was reserved for this record. At capacity it is written
            // inline instead of dropped: the handler is awaiting it anyway, and
            // a cancelled caller losing a zero-charge failure record is the
            // lesser loss.
            if let Err(record_terminal) = state.0.settlements.try_spawn(record_terminal) {
                record_terminal.await;
                return;
            }
        }
    }
    let _ = recorded.await;
}

pub(super) fn build_record(args: RecordArgs<'_>) -> (UsageRecord, Option<u64>, u32) {
    let ttft_ms = args.ttft_ms;
    let attempts = args.attempts;
    let record = UsageRecord {
        schema_version: UsageRecord::SCHEMA_VERSION,
        request_id: args.identity.request_id.to_string(),
        trace_id: args.identity.trace_id.clone(),
        namespace: args.caller.namespace.clone(),
        attrs: args.attrs.clone().or_else(|| args.caller.attrs.clone()),
        period: args.period.clone(),
        subject: args.caller.subject.clone(),
        signer_kid: args.caller.signer_kid.clone(),
        model: args.alias.to_string(),
        target_provider: args.target_provider.to_string(),
        target_model: args.target_model.to_string(),
        credential_source: UsageRecord::credential_source_str(args.source),
        credential_id: args.credential_id.to_string(),
        status: args.status,
        input_tokens: args.input_tokens,
        cache_read_tokens: args.cache_read_tokens,
        cache_write_tokens: args.cache_write_tokens,
        output_tokens: args.output_tokens,
        cost_microdollars: args.cost_microdollars,
        catalog_version: args.price.catalog_version(),
        price_book: args.price.identity().map(|id| id.book()),
        price_book_checksum: args.price.identity().map(|id| id.checksum()),
        price_catalog: args.price.identity().map(|id| id.catalog()),
        latency_ms: args.latency_ms,
        attempts,
    };
    (record, ttft_ms, attempts)
}
