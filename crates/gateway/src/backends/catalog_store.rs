//! Durable catalogue snapshots: the [`CatalogStore`] contract.
//!
//! [`LastKnownGoodCatalog`](super::catalog::LastKnownGoodCatalog) makes a
//! refused import unable to disturb the active catalogue *within a process*. It
//! cannot make an admitted one survive the process: a replica that restarts
//! having imported yesterday holds nothing, so it must either re-import before
//! it can answer anything about models, or fall back to the compiled-in seed and
//! quietly serve a four-provider excerpt. This contract is the other half —
//! what a deployment has imported, kept where a restart, a rollback, and a
//! second replica can all read it.
//!
//! Three rules shape it, and each one is a property of the storage rather than
//! of a caller's diligence:
//!
//! - **A snapshot is written once, under its own identity.** The key is the
//!   [`CatalogContentId`], the payload is keyed by the digest of the exact bytes
//!   that were accepted, and nothing updates either. Re-importing an unchanged
//!   catalogue therefore stores nothing new
//!   ([`Retention::AlreadyRetained`]) — idempotence is the table's shape, not a
//!   check someone remembered to write — and a
//!   [`CatalogOffering`](crate::desired_state::models::CatalogOffering) that
//!   pinned an older snapshot keeps resolving the content it was published
//!   against.
//! - **What is active is a pointer, not a copy.** Activation moves one
//!   reference; it never rewrites a snapshot row. So a refresh cannot mutate
//!   history, and rolling back to a retained catalogue is moving the pointer
//!   back rather than re-fetching an upstream that has moved on.
//! - **Provenance moves without the content.** A `304` states validators and a
//!   check time about content already held, so [`CatalogStore::confirm`] writes
//!   those onto the active pointer and leaves the immutable import row alone.
//!   [`CatalogStore::load`] then answers with the *current* validators and the
//!   last confirmation time, which is what makes an active snapshot's age mean
//!   "last confirmed current" across a restart instead of resetting to the
//!   moment the process booted.
//!
//! # Rehydration re-parses; it does not deserialize
//!
//! [`hydrate`] turns a retained record back into a
//! [`CatalogSnapshot`] by running the stored
//! bytes through the same parser that accepted them, then checking that the
//! content it produces still has the identity the record names. There is
//! deliberately no second serialization of the normalized domain: a stored form
//! of [`CatalogContent`](super::catalog::CatalogContent) would be a second
//! definition of what a catalogue *is*, free to drift from the parser's, and the
//! drift would be invisible — a decoder happily reading a field the normalizer
//! stopped producing. Re-parsing has neither problem, and it makes the
//! interesting failure loud: if a parser change would now normalize yesterday's
//! bytes differently, boot says so ([`HydrationError::Drift`]) instead of
//! serving content nobody can reproduce.
//!
//! Nothing here is on the request path. The store is used by the background
//! refresher and by snapshot compilation, both before a candidate is published.
//! An unavailable database costs a refused refresh or a refused candidate; the
//! catalogue and serving snapshot already active remain untouched.

pub mod postgres;

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::SystemTime;

use async_trait::async_trait;

use super::catalog::{
    CatalogContentId, RawPayload, Refusable, Refusal, RefusalReason, SchemaVersion, SourceSnapshot,
    SourceValidators,
};
use super::models_dev::{ModelsDevAdapter, ModelsDevError};
use super::{BackendFailure, Capabilities, FailureCategory};
use crate::backends::catalog::CatalogSnapshot;
use crate::desired_state::BlobError;

/// One import exactly as it was retained: its provenance, and the bytes it was
/// parsed from.
///
/// The bytes are part of the record rather than an optional extra, because the
/// provenance alone cannot be rehydrated: [`SourceSnapshot::raw`] names a
/// payload, and a store that kept only the name would hold a catalogue it could
/// describe and not reconstruct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedCatalog {
    pub source: SourceSnapshot,
    pub payload: RawPayload,
}

impl RetainedCatalog {
    pub fn content_id(&self) -> CatalogContentId {
        self.source.content_id
    }
}

/// What a store holds about this deployment's catalogue.
///
/// The refusal fields are here, and not only in memory, because a refusal
/// outlives the process that observed it: a replica that restarts into an
/// upstream that is still refusing must not report a fresh, healthy catalogue
/// that has simply forgotten it is stuck.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredCatalogState {
    /// The active import, with the validators and check time currently recorded
    /// for it — not necessarily the ones it was imported with.
    pub active: Option<RetainedCatalog>,
    pub consecutive_refusals: u32,
    pub last_refusal: Option<RefusalReason>,
}

/// Whether an activation had to retain the bytes, or found them already stored.
///
/// Returned rather than discarded so idempotence is *observable*: a test, and an
/// operator reading a log line, can tell a re-imported unchanged catalogue from
/// a new one without comparing table sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retention {
    /// The store did not hold this content, and now does.
    Retained,
    /// The store already held exactly this content under this identity.
    AlreadyRetained,
}

/// Why a catalogue store could not answer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogStoreError {
    #[error("catalogue store `{backend}` unavailable: {message}")]
    Unavailable {
        backend: &'static str,
        message: String,
    },
    /// A stored record that does not add up: an active pointer to a snapshot
    /// that is not there, a payload row missing under a snapshot that names it.
    /// Always an operator alert, never a retry.
    #[error("catalogue store `{backend}` holds a record it cannot answer with: {message}")]
    Corrupt {
        backend: &'static str,
        message: String,
    },
    #[error("catalogue store `{backend}` refused the operation: {message}")]
    Denied {
        backend: &'static str,
        message: String,
    },
}

impl CatalogStoreError {
    pub fn unavailable(backend: &'static str, message: impl Into<String>) -> Self {
        Self::Unavailable {
            backend,
            message: message.into(),
        }
    }

    pub fn corrupt(backend: &'static str, message: impl Into<String>) -> Self {
        Self::Corrupt {
            backend,
            message: message.into(),
        }
    }

    pub fn denied(backend: &'static str, message: impl Into<String>) -> Self {
        Self::Denied {
            backend,
            message: message.into(),
        }
    }
}

impl BackendFailure for CatalogStoreError {
    fn category(&self) -> FailureCategory {
        match self {
            Self::Unavailable { .. } => FailureCategory::Unavailable,
            Self::Corrupt { .. } => FailureCategory::Corrupt,
            Self::Denied { .. } => FailureCategory::Denied,
        }
    }
}

/// Every storage failure refuses the *import*, whatever went wrong underneath.
///
/// The upstream document is blameless in all three arms, so they share
/// [`RefusalReason::NotRetained`]: what an operator needs from the label is
/// "look at the deployment's own database, not at models.dev", and the arm that
/// says which of the three it was travels in the log line beside it.
impl Refusable for CatalogStoreError {
    fn refusal(&self) -> Refusal {
        Refusal::new(RefusalReason::NotRetained)
    }
}

/// Durable retention of imported catalogue snapshots.
#[async_trait]
pub trait CatalogStore: Send + Sync {
    fn name(&self) -> &'static str;

    fn capabilities(&self) -> Capabilities;

    /// What this deployment has imported, as of now.
    ///
    /// The active record carries the validators and check time from the active
    /// pointer rather than from the import, so a catalogue confirmed by a `304`
    /// an hour ago reads as an hour old and not as however long ago its bytes
    /// were first transferred.
    async fn load(&self) -> Result<StoredCatalogState, CatalogStoreError>;

    /// A retained snapshot by identity, for a record that pinned one.
    ///
    /// Nothing in this slice deletes, so this answers for every catalogue the
    /// deployment ever admitted. A retention policy is a later decision, and it
    /// is one that has to consider the pins.
    async fn retained(
        &self,
        content_id: CatalogContentId,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError>;

    /// A retained snapshot by the raw payload digest an enablement pins.
    ///
    /// Desired state carries the digest of the exact document an operator
    /// approved, while the active catalogue is keyed by normalized content.
    /// Keeping this lookup here preserves both identities instead of asking
    /// convergence to guess one from the other.
    async fn retained_by_raw_digest(
        &self,
        digest: crate::desired_state::Checksum,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError>;

    /// Retain `import` if it is new, and make it the active catalogue as of
    /// `activated_at`.
    ///
    /// One operation rather than a retain-then-activate pair: a replica that
    /// crashed between the two would either lose the import or point at bytes it
    /// had not stored, and the second is exactly the state
    /// [`CatalogStoreError::Corrupt`] exists to report.
    async fn activate(
        &self,
        import: &RetainedCatalog,
        activated_at: SystemTime,
    ) -> Result<Retention, CatalogStoreError>;

    /// Record that the source confirmed the active content unchanged.
    ///
    /// Answers whether there was an active pointer to move: confirming before a
    /// first import is not an error, it is an answer about nothing. The stated
    /// validators are carried over the held ones rather than replacing them
    /// wholesale, for the reason
    /// [`SourceValidators::carry_over`] documents.
    async fn confirm(
        &self,
        content_id: CatalogContentId,
        validators: &SourceValidators,
        confirmed_at: SystemTime,
    ) -> Result<bool, CatalogStoreError>;

    /// Count a refused import durably, so staleness survives a restart.
    async fn refuse(
        &self,
        reason: RefusalReason,
        refused_at: SystemTime,
    ) -> Result<(), CatalogStoreError>;
}

/// A shared reference to a store is a store, so one durable store can back the
/// refresher and whatever else reads what the deployment retained without an
/// `Arc` in the contract.
#[async_trait]
impl<T: CatalogStore + ?Sized> CatalogStore for &T {
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn capabilities(&self) -> Capabilities {
        (**self).capabilities()
    }

    async fn load(&self) -> Result<StoredCatalogState, CatalogStoreError> {
        (**self).load().await
    }

    async fn retained(
        &self,
        content_id: CatalogContentId,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        (**self).retained(content_id).await
    }

    async fn retained_by_raw_digest(
        &self,
        digest: crate::desired_state::Checksum,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        (**self).retained_by_raw_digest(digest).await
    }

    async fn activate(
        &self,
        import: &RetainedCatalog,
        activated_at: SystemTime,
    ) -> Result<Retention, CatalogStoreError> {
        (**self).activate(import, activated_at).await
    }

    async fn confirm(
        &self,
        content_id: CatalogContentId,
        validators: &SourceValidators,
        confirmed_at: SystemTime,
    ) -> Result<bool, CatalogStoreError> {
        (**self).confirm(content_id, validators, confirmed_at).await
    }

    async fn refuse(
        &self,
        reason: RefusalReason,
        refused_at: SystemTime,
    ) -> Result<(), CatalogStoreError> {
        (**self).refuse(reason, refused_at).await
    }
}

#[async_trait]
impl<T: CatalogStore + ?Sized> CatalogStore for std::sync::Arc<T> {
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn capabilities(&self) -> Capabilities {
        (**self).capabilities()
    }

    async fn load(&self) -> Result<StoredCatalogState, CatalogStoreError> {
        (**self).load().await
    }

    async fn retained(
        &self,
        content_id: CatalogContentId,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        (**self).retained(content_id).await
    }

    async fn retained_by_raw_digest(
        &self,
        digest: crate::desired_state::Checksum,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        (**self).retained_by_raw_digest(digest).await
    }

    async fn activate(
        &self,
        import: &RetainedCatalog,
        activated_at: SystemTime,
    ) -> Result<Retention, CatalogStoreError> {
        (**self).activate(import, activated_at).await
    }

    async fn confirm(
        &self,
        content_id: CatalogContentId,
        validators: &SourceValidators,
        confirmed_at: SystemTime,
    ) -> Result<bool, CatalogStoreError> {
        (**self).confirm(content_id, validators, confirmed_at).await
    }

    async fn refuse(
        &self,
        reason: RefusalReason,
        refused_at: SystemTime,
    ) -> Result<(), CatalogStoreError> {
        (**self).refuse(reason, refused_at).await
    }
}

/// Why a retained record could not be turned back into a snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HydrationError {
    #[error(
        "catalogue {content_id} was imported under schema `{}`, which this build does not read",
        schema.as_str()
    )]
    UnknownSchema {
        content_id: CatalogContentId,
        schema: SchemaVersion,
    },
    #[error("the stored payload for catalogue {content_id} is not the bytes it names: {source}")]
    Payload {
        content_id: CatalogContentId,
        #[source]
        source: BlobError,
    },
    #[error("the stored payload for catalogue {content_id} no longer parses: {source}")]
    Parse {
        content_id: CatalogContentId,
        #[source]
        source: ModelsDevError,
    },
    /// The bytes parse, and this build normalizes them into something else. A
    /// normalizer changed under a stored import: the content an operator
    /// approved a price book against is not the content that would be served, so
    /// the record is refused rather than silently re-identified.
    #[error("the stored payload for catalogue {content_id} now normalizes to {recomputed}")]
    Drift {
        content_id: CatalogContentId,
        recomputed: CatalogContentId,
    },
}

impl Refusable for HydrationError {
    fn refusal(&self) -> Refusal {
        match self {
            // The payload is a document this deployment stored, so a parse
            // failure keeps the parser's own reason and pointer: it names the
            // field, which is the only thing that makes a stored-payload
            // refusal actionable.
            Self::Parse { source, .. } => source.refusal(),
            Self::UnknownSchema { .. } | Self::Payload { .. } | Self::Drift { .. } => {
                Refusal::new(RefusalReason::NotRetained)
            }
        }
    }
}

/// Turn a retained record back into the snapshot it was, by re-parsing it.
///
/// Three checks, in the order that makes a failure name the cheapest true
/// discrepancy: the bytes are the bytes the record names, they parse, and they
/// still normalize to the identity the record was stored under.
pub fn hydrate(retained: &RetainedCatalog) -> Result<CatalogSnapshot, HydrationError> {
    let content_id = retained.source.content_id;
    if retained.source.schema_version != SchemaVersion::MODELS_DEV_CATALOG_V1 {
        return Err(HydrationError::UnknownSchema {
            content_id,
            schema: retained.source.schema_version,
        });
    }
    retained
        .source
        .raw
        .verify(retained.payload.as_bytes())
        .map_err(|source| HydrationError::Payload { content_id, source })?;
    let adapter = ModelsDevAdapter::new(retained.source.source_url.clone())
        .map_err(|source| HydrationError::Parse { content_id, source })?;
    let snapshot = adapter
        .parse(
            retained.payload.as_bytes(),
            retained.source.validators.clone(),
            retained.source.fetched_at,
        )
        .map_err(|source| HydrationError::Parse { content_id, source })?;
    if snapshot.source.content_id != content_id {
        return Err(HydrationError::Drift {
            content_id,
            recomputed: snapshot.source.content_id,
        });
    }
    Ok(snapshot)
}

/// The active pointer: which content is active, and what is currently known
/// about it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ActivePointer {
    content_id: CatalogContentId,
    validators: SourceValidators,
    confirmed_at: SystemTime,
}

#[derive(Debug, Default)]
struct InMemoryState {
    retained: BTreeMap<CatalogContentId, RetainedCatalog>,
    active: Option<ActivePointer>,
    consecutive_refusals: u32,
    last_refusal: Option<RefusalReason>,
}

/// A [`CatalogStore`] in this process's memory.
///
/// Not a durable backend, and it does not pretend to be one: it exists so the
/// refresh orchestration can be tested against the contract's semantics —
/// write-once retention, an active pointer, provenance that moves without
/// content — without a database, and so a single-replica development run can
/// exercise the same code path a deployment runs.
#[derive(Debug, Default)]
pub struct InMemoryCatalogStore {
    state: Mutex<InMemoryState>,
}

const IN_MEMORY: &str = "in-memory";

impl InMemoryCatalogStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many distinct catalogues have been retained. The evidence that a
    /// re-import of unchanged content stored nothing.
    pub fn retained_count(&self) -> usize {
        self.state
            .lock()
            .expect("catalogue store lock")
            .retained
            .len()
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, InMemoryState> {
        self.state.lock().expect("catalogue store lock")
    }
}

#[async_trait]
impl CatalogStore for InMemoryCatalogStore {
    fn name(&self) -> &'static str {
        IN_MEMORY
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities::NONE
    }

    async fn load(&self) -> Result<StoredCatalogState, CatalogStoreError> {
        let state = self.locked();
        let active = state
            .active
            .as_ref()
            .map(|pointer| {
                let retained = state.retained.get(&pointer.content_id).ok_or_else(|| {
                    CatalogStoreError::corrupt(
                        IN_MEMORY,
                        format!("active catalogue {} is not retained", pointer.content_id),
                    )
                })?;
                let mut retained = retained.clone();
                retained.source.validators = pointer.validators.clone();
                retained.source.fetched_at = pointer.confirmed_at;
                Ok::<_, CatalogStoreError>(retained)
            })
            .transpose()?;
        Ok(StoredCatalogState {
            active,
            consecutive_refusals: state.consecutive_refusals,
            last_refusal: state.last_refusal,
        })
    }

    async fn retained(
        &self,
        content_id: CatalogContentId,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        Ok(self.locked().retained.get(&content_id).cloned())
    }

    async fn retained_by_raw_digest(
        &self,
        digest: crate::desired_state::Checksum,
    ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
        Ok(self
            .locked()
            .retained
            .values()
            .find(|retained| retained.source.raw.digest == digest)
            .cloned())
    }

    async fn activate(
        &self,
        import: &RetainedCatalog,
        activated_at: SystemTime,
    ) -> Result<Retention, CatalogStoreError> {
        let mut state = self.locked();
        let content_id = import.content_id();
        let retention = match state.retained.entry(content_id) {
            std::collections::btree_map::Entry::Occupied(_) => Retention::AlreadyRetained,
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(import.clone());
                Retention::Retained
            }
        };
        // Re-activating the content already active is the `304`-with-a-body
        // case, so its validators are carried over the held ones for the reason
        // [`SourceValidators::carry_over`] documents. New content replaces them
        // wholesale: a validator describes a document, not a deployment.
        let mut validators = import.source.validators.clone();
        if let Some(active) = state
            .active
            .as_ref()
            .filter(|active| active.content_id == content_id)
        {
            let mut held = active.validators.clone();
            held.carry_over(validators);
            validators = held;
        }
        state.active = Some(ActivePointer {
            content_id,
            validators,
            confirmed_at: activated_at,
        });
        state.consecutive_refusals = 0;
        state.last_refusal = None;
        Ok(retention)
    }

    async fn confirm(
        &self,
        content_id: CatalogContentId,
        validators: &SourceValidators,
        confirmed_at: SystemTime,
    ) -> Result<bool, CatalogStoreError> {
        let mut state = self.locked();
        let Some(pointer) = state
            .active
            .as_mut()
            .filter(|pointer| pointer.content_id == content_id)
        else {
            return Ok(false);
        };
        pointer.validators.carry_over(validators.clone());
        pointer.confirmed_at = confirmed_at;
        state.consecutive_refusals = 0;
        state.last_refusal = None;
        Ok(true)
    }

    async fn refuse(
        &self,
        reason: RefusalReason,
        _refused_at: SystemTime,
    ) -> Result<(), CatalogStoreError> {
        let mut state = self.locked();
        state.consecutive_refusals = state.consecutive_refusals.saturating_add(1);
        state.last_refusal = Some(reason);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::super::models_dev::{SEED_PAYLOAD, seed_snapshot};
    use super::*;

    fn seed_import() -> RetainedCatalog {
        RetainedCatalog {
            source: seed_snapshot().source,
            payload: RawPayload::new(SEED_PAYLOAD.as_bytes()),
        }
    }

    fn at(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    #[tokio::test]
    async fn a_retained_import_rehydrates_into_the_snapshot_it_was() {
        let import = seed_import();
        let hydrated = hydrate(&import).expect("the seed rehydrates");
        assert_eq!(hydrated, seed_snapshot());
    }

    #[tokio::test]
    async fn a_payload_that_is_not_the_bytes_it_names_is_refused() {
        let mut import = seed_import();
        import.payload = RawPayload::new(&b"{\"models\":{},\"providers\":{}}"[..]);
        let error = hydrate(&import).expect_err("the digest no longer matches");
        assert!(matches!(error, HydrationError::Payload { .. }));
        assert_eq!(error.refusal().reason(), RefusalReason::NotRetained);
    }

    /// The record's identity is the one thing rehydration may not take on
    /// trust: it is what a price book pins and what a diff is computed against.
    #[tokio::test]
    async fn a_record_stored_under_an_identity_its_bytes_do_not_produce_is_refused() {
        let mut import = seed_import();
        let elsewhere = crate::desired_state::Checksum::of(b"another catalogue");
        import.source.content_id = CatalogContentId::from_checksum(elsewhere);
        let error = hydrate(&import).expect_err("the content id does not match the bytes");
        let HydrationError::Drift { recomputed, .. } = error else {
            panic!("a mismatched identity is drift, not a parse failure: {error}");
        };
        assert_eq!(recomputed, seed_snapshot().source.content_id);
    }

    #[tokio::test]
    async fn an_import_this_build_cannot_parse_is_named_by_its_schema() {
        let mut import = seed_import();
        import.source.source_url = "https://models.dev/api.json".to_owned();
        let error = hydrate(&import).expect_err("the URL is not a catalogue document");
        assert!(matches!(error, HydrationError::Parse { .. }));
    }

    #[tokio::test]
    async fn re_importing_unchanged_content_retains_nothing_new() {
        let store = InMemoryCatalogStore::new();
        let import = seed_import();

        assert_eq!(
            store.activate(&import, at(10)).await.expect("activate"),
            Retention::Retained
        );
        assert_eq!(
            store.activate(&import, at(20)).await.expect("re-activate"),
            Retention::AlreadyRetained
        );
        assert_eq!(store.retained_count(), 1);

        let state = store.load().await.expect("load");
        let active = state.active.expect("an active catalogue");
        assert_eq!(active.content_id(), import.content_id());
        assert_eq!(
            active.source.fetched_at,
            at(20),
            "the second import is when the content was last confirmed"
        );
    }

    /// A `304` moves provenance and not content, and the store has to keep that
    /// distinction across a restart: an age that reset to the import would tell
    /// an operator the catalogue is a week stale while it is being confirmed
    /// every minute.
    #[tokio::test]
    async fn confirming_moves_the_check_time_without_touching_the_import() {
        let store = InMemoryCatalogStore::new();
        let import = seed_import();
        store.activate(&import, at(10)).await.expect("activate");

        let confirmed = store
            .confirm(
                import.content_id(),
                &SourceValidators::etag("\"later\""),
                at(600),
            )
            .await
            .expect("confirm");
        assert!(confirmed);

        let state = store.load().await.expect("load");
        let active = state.active.expect("an active catalogue");
        assert_eq!(active.source.fetched_at, at(600));
        assert_eq!(
            active.source.validators,
            SourceValidators::etag("\"later\"")
        );
        assert_eq!(
            store
                .retained(import.content_id())
                .await
                .expect("retained")
                .expect("the import itself")
                .source
                .validators,
            import.source.validators,
            "the immutable import keeps the validators it arrived with"
        );
    }

    #[tokio::test]
    async fn a_validator_the_answer_does_not_state_is_kept() {
        let store = InMemoryCatalogStore::new();
        let mut import = seed_import();
        import.source.validators = SourceValidators::etag("\"held\"");
        store.activate(&import, at(10)).await.expect("activate");

        store
            .confirm(import.content_id(), &SourceValidators::default(), at(20))
            .await
            .expect("confirm");

        let state = store.load().await.expect("load");
        assert_eq!(
            state.active.expect("active").source.validators,
            SourceValidators::etag("\"held\""),
            "an unstated validator is not a withdrawn one"
        );
    }

    /// Re-activation is the `304` case with a body, so it obeys the same rule
    /// as [`CatalogStore::confirm`]: an answer that states no validator has not
    /// withdrawn the held one, and dropping it would make every later refresh
    /// transfer a document the deployment already has.
    #[tokio::test]
    async fn re_activating_the_active_content_without_a_validator_keeps_the_held_one() {
        let store = InMemoryCatalogStore::new();
        let mut import = seed_import();
        import.source.validators = SourceValidators::etag("\"held\"");
        store.activate(&import, at(10)).await.expect("activate");

        let mut stripped = import.clone();
        stripped.source.validators = SourceValidators::default();
        store
            .activate(&stripped, at(20))
            .await
            .expect("re-activate");

        let state = store.load().await.expect("load");
        assert_eq!(
            state.active.expect("active").source.validators,
            SourceValidators::etag("\"held\""),
        );
    }

    /// The carry-over is scoped to the content it describes: a validator names
    /// a document, so new content arriving without one holds none.
    #[tokio::test]
    async fn activating_new_content_does_not_inherit_the_previous_validator() {
        let store = InMemoryCatalogStore::new();
        let mut first = seed_import();
        first.source.validators = SourceValidators::etag("\"held\"");
        store.activate(&first, at(10)).await.expect("activate");

        let mut second = seed_import();
        second.source.content_id =
            CatalogContentId::from_checksum(crate::desired_state::Checksum::of(b"other"));
        second.source.validators = SourceValidators::default();
        store.activate(&second, at(20)).await.expect("activate");

        let state = store.load().await.expect("load");
        let active = state.active.expect("active");
        assert_eq!(active.content_id(), second.content_id());
        assert_eq!(active.source.validators, SourceValidators::default());
    }

    #[tokio::test]
    async fn confirming_content_that_is_not_active_records_nothing() {
        let store = InMemoryCatalogStore::new();
        let confirmed = store
            .confirm(
                seed_import().content_id(),
                &SourceValidators::etag("\"any\""),
                at(10),
            )
            .await
            .expect("confirm");
        assert!(!confirmed, "there was no active pointer to move");
        assert_eq!(
            store.load().await.expect("load"),
            StoredCatalogState::default()
        );
    }

    /// Staleness has to outlive the process that observed it: a restarted
    /// replica whose upstream is still refusing must not read as healthy.
    #[tokio::test]
    async fn refusals_are_counted_durably_and_cleared_by_an_import() {
        let store = InMemoryCatalogStore::new();
        store
            .refuse(RefusalReason::Unreachable, at(10))
            .await
            .expect("refuse");
        store
            .refuse(RefusalReason::Schema, at(20))
            .await
            .expect("refuse");

        let state = store.load().await.expect("load");
        assert_eq!(state.consecutive_refusals, 2);
        assert_eq!(state.last_refusal, Some(RefusalReason::Schema));
        assert!(state.active.is_none(), "nothing was ever imported");

        store
            .activate(&seed_import(), at(30))
            .await
            .expect("activate");
        let state = store.load().await.expect("load");
        assert_eq!(state.consecutive_refusals, 0);
        assert_eq!(state.last_refusal, None);
    }

    #[test]
    fn every_storage_failure_refuses_the_import_without_blaming_the_payload() {
        for error in [
            CatalogStoreError::unavailable(IN_MEMORY, "connection refused"),
            CatalogStoreError::corrupt(IN_MEMORY, "active row names an absent snapshot"),
            CatalogStoreError::denied(IN_MEMORY, "no privilege on axond_catalog_snapshot"),
        ] {
            assert_eq!(error.refusal().reason(), RefusalReason::NotRetained);
        }
        assert!(CatalogStoreError::unavailable(IN_MEMORY, "down").retryable());
        assert!(!CatalogStoreError::corrupt(IN_MEMORY, "damaged").retryable());
        assert!(!CatalogStoreError::denied(IN_MEMORY, "refused").retryable());
    }
}
