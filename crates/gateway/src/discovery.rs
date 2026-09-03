//! Cached upstream provider model listing (ADR 0063).
//!
//! `GET /api/v1/providers/{id}/models` and the fan-out route read the Store.
//! A background timer in [`run`] is the only writer. Inference never lists
//! upstream models. Azure Foundry's data-plane listing omits deployments;
//! this still stores whatever `GET /models` returned.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use gateway_core::AnthropicAdapter;
use gateway_transport::{AuthScheme, Deadline, Upstream};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::config::ProviderKind;
use crate::state::AppState;
use crate::store::ProviderModels;

/// Anthropic `GET /v1/models` default page is 20; 20 pages is a hard ceiling
/// so a `has_more` loop cannot run unbounded.
const MAX_PAGES: usize = 20;
const EMPTY_BACKOFF_START: Duration = Duration::from_secs(1);
const EMPTY_BACKOFF_CAP: Duration = Duration::from_secs(30);

/// Refresh every configured provider, then sleep the live discovery interval,
/// until `stop`.
///
/// The first round runs immediately so a GET after boot can see a cache. An
/// empty serving snapshot (stateful boot before convergence publishes
/// providers) retries with short backoff instead of the configured interval.
pub async fn run(state: AppState, mut stop: oneshot::Receiver<()>) {
    let mut empty_backoff = EMPTY_BACKOFF_START;
    let mut waiting_for_providers = true;
    // First completed round may stale+replace a foreign cache row so a
    // `base_url` change lands. Later rounds skip a fresh foreign row so a
    // lagged replica cannot mark it stale and CAS-put the old listing.
    let mut first_round = true;
    loop {
        let providers = state.config().config.provider.len();
        if waiting_for_providers && providers == 0 {
            tokio::select! {
                biased;
                _ = &mut stop => {
                    tracing::debug!("provider model discovery stopped");
                    return;
                }
                _ = tokio::time::sleep(empty_backoff) => {}
            }
            empty_backoff = next_empty_backoff(empty_backoff);
            continue;
        }
        waiting_for_providers = false;
        match refresh_all(&state, &mut stop, first_round).await {
            Round::Stopped => {
                tracing::debug!("provider model discovery stopped");
                return;
            }
            Round::Restart => continue,
            Round::Done => first_round = false,
        }
        let interval = state.config().config.discovery.interval();
        tokio::select! {
            biased;
            _ = &mut stop => {
                tracing::debug!("provider model discovery stopped");
                return;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Round {
    Done,
    Stopped,
    Restart,
}

enum RefreshError {
    Failed(String),
    Stopped,
    SnapshotChanged,
}

async fn refresh_all(
    state: &AppState,
    stop: &mut oneshot::Receiver<()>,
    allow_replace: bool,
) -> Round {
    let snapshot = state.config();
    let providers: Vec<(String, ProviderKind, String)> = snapshot
        .config
        .provider
        .iter()
        .map(|provider| {
            (
                provider.id.clone(),
                provider.kind,
                provider.base_url.clone(),
            )
        })
        .collect();
    for (id, kind, base_url) in &providers {
        if stopped(stop) {
            return Round::Stopped;
        }
        if let Err(error) =
            refresh_one(state, &snapshot, id, *kind, base_url, allow_replace, stop).await
        {
            match error {
                RefreshError::Stopped => return Round::Stopped,
                RefreshError::SnapshotChanged => return Round::Restart,
                RefreshError::Failed(error) => {
                    tracing::warn!(
                        provider = %id,
                        error = %error,
                        "provider model discovery failed"
                    );
                    mark_stale(state, id, base_url).await;
                }
            }
        }
    }
    Round::Done
}

async fn refresh_one(
    state: &AppState,
    snapshot: &std::sync::Arc<crate::state::ConfigSnapshot>,
    provider_id: &str,
    kind: ProviderKind,
    base_url: &str,
    allow_replace: bool,
    stop: &mut oneshot::Receiver<()>,
) -> Result<(), RefreshError> {
    if stopped(stop) {
        return Err(RefreshError::Stopped);
    }
    if allow_replace {
        stale_if_source_changed(state, provider_id, base_url).await;
    } else if skip_fresh_foreign(state, provider_id, base_url).await {
        return Ok(());
    }

    let leases = snapshot
        .credentials
        .discovery_leases(&snapshot.config, provider_id);
    if leases.is_empty() {
        return Err(RefreshError::Failed("no credential".into()));
    };
    let mut headers: Vec<(&'static str, String)> = Vec::new();
    if kind == ProviderKind::Anthropic {
        headers.push(("anthropic-version", AnthropicAdapter::VERSION.to_owned()));
    }

    let mut last_error = None;
    for lease in leases {
        if stopped(stop) {
            return Err(RefreshError::Stopped);
        }
        let upstream = Upstream {
            base_url: base_url.to_owned(),
            api_key: lease.secret.clone(),
            auth: match kind {
                ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
                ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
            },
        };
        match fetch_listing(state, provider_id, &upstream, &headers, stop).await {
            Ok(data) => {
                let current = state.config();
                if current.generation != snapshot.generation {
                    return Err(RefreshError::SnapshotChanged);
                }
                let row = ProviderModels {
                    provider: provider_id.to_owned(),
                    fetched_at: Some(rfc3339_utc(SystemTime::now())),
                    stale: false,
                    data,
                    source: Some(base_url.to_owned()),
                };
                state
                    .store()
                    .ok_or_else(|| RefreshError::Failed("store unavailable".to_owned()))?
                    .put_provider_models(row)
                    .await
                    .map_err(|error| RefreshError::Failed(error.to_string()))?;
                return Ok(());
            }
            Err(RefreshError::Stopped) => return Err(RefreshError::Stopped),
            Err(RefreshError::SnapshotChanged) => return Err(RefreshError::SnapshotChanged),
            Err(RefreshError::Failed(error)) => {
                tracing::debug!(
                    provider = provider_id,
                    credential = %lease.id,
                    error = %error,
                    "discovery credential failed; trying next"
                );
                last_error = Some(error);
            }
        }
    }
    Err(RefreshError::Failed(last_error.unwrap_or_else(|| {
        "every discovery credential failed".into()
    })))
}

async fn fetch_listing(
    state: &AppState,
    provider_id: &str,
    upstream: &Upstream,
    headers: &[(&'static str, String)],
    stop: &mut oneshot::Receiver<()>,
) -> Result<Vec<Value>, RefreshError> {
    let mut data = Vec::new();
    let mut after_id: Option<String> = None;
    for page in 0..MAX_PAGES {
        if stopped(stop) {
            return Err(RefreshError::Stopped);
        }
        let path = models_path(after_id.as_deref());
        let body = tokio::select! {
            biased;
            _ = &mut *stop => return Err(RefreshError::Stopped),
            result = state.0.dispatcher.get_json(
                provider_id,
                upstream,
                &path,
                headers,
                Deadline::at(Instant::now() + Duration::from_secs(30)),
            ) => result.map_err(|error| RefreshError::Failed(error.to_string()))?,
        };
        let page_body = parse_page(&body).ok_or_else(|| {
            RefreshError::Failed("upstream listing is not an OpenAI-shaped `data` array".into())
        })?;
        data.extend(page_body.data);
        match page_body.next_after {
            Some(next) => {
                if page + 1 == MAX_PAGES {
                    return Err(RefreshError::Failed(
                        "provider model listing exceeded the page bound".into(),
                    ));
                }
                after_id = Some(next);
            }
            None => break,
        }
    }
    Ok(data)
}

async fn stale_if_source_changed(state: &AppState, provider: &str, base_url: &str) {
    let Some(store) = state.store() else {
        return;
    };
    if let Err(error) = store
        .mark_provider_models_stale_unless_source(provider, base_url)
        .await
    {
        tracing::warn!(
            provider,
            error = %error,
            "could not persist stale provider model cache"
        );
    }
}

/// Later rounds: a fresh row for another URL is someone else's successful
/// listing. Do not mark it stale (that would open the put CAS) and do not
/// fetch. A GET error falls through so put's source CAS still decides.
async fn skip_fresh_foreign(state: &AppState, provider: &str, base_url: &str) -> bool {
    let Some(store) = state.store() else {
        return false;
    };
    match store.get_provider_models(provider).await {
        Ok(existing) => is_fresh_foreign(existing.as_ref(), base_url),
        Err(error) => {
            tracing::warn!(
                provider,
                error = %error,
                "could not read provider model cache before refresh"
            );
            false
        }
    }
}

fn is_fresh_foreign(existing: Option<&ProviderModels>, my_source: &str) -> bool {
    existing.is_some_and(|row| {
        !row.stale
            && row
                .source
                .as_deref()
                .is_some_and(|source| source != my_source)
    })
}

async fn mark_stale(state: &AppState, provider: &str, source: &str) {
    let Some(store) = state.store() else {
        return;
    };
    if let Err(error) = store
        .mark_provider_models_stale_if_source(provider, source)
        .await
    {
        tracing::warn!(
            provider,
            error = %error,
            "could not persist stale provider model cache"
        );
    }
}

fn stopped(stop: &mut oneshot::Receiver<()>) -> bool {
    match stop.try_recv() {
        Ok(()) | Err(oneshot::error::TryRecvError::Closed) => true,
        Err(oneshot::error::TryRecvError::Empty) => false,
    }
}

fn next_empty_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(EMPTY_BACKOFF_CAP)
}

struct ListingPage {
    data: Vec<Value>,
    next_after: Option<String>,
}

fn parse_listing(body: &Value) -> Option<Vec<Value>> {
    let data = body.get("data")?.as_array()?;
    Some(
        data.iter()
            .filter(|item| item.get("id").and_then(Value::as_str).is_some())
            .cloned()
            .collect(),
    )
}

/// OpenAI is a single `data` array. Anthropic adds `has_more` / `last_id`;
/// the next request uses `after_id`.
fn parse_page(body: &Value) -> Option<ListingPage> {
    let data = parse_listing(body)?;
    let has_more = body
        .get("has_more")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !has_more {
        return Some(ListingPage {
            data,
            next_after: None,
        });
    }
    let last_id = body
        .get("last_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())?;
    Some(ListingPage {
        data,
        next_after: Some(last_id.to_owned()),
    })
}

fn models_path(after_id: Option<&str>) -> String {
    match after_id {
        None => "/models".to_owned(),
        Some(id) => format!("/models?after_id={}", encode_query_component(id)),
    }
}

fn encode_query_component(value: &str) -> String {
    let mut out = String::new();
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char);
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn rfc3339_utc(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = i64::try_from(secs / 86_400).unwrap_or(0);
    let tod = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    let hour = tod / 3_600;
    let minute = (tod % 3_600) / 60;
    let second = tod % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

/// Howard Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = u32::try_from(z - era * 146_097).unwrap_or(0);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_listing_keeps_objects_with_ids() {
        let body = json!({
            "object": "list",
            "data": [
                {"id": "gpt-4o", "object": "model"},
                {"object": "model"},
                {"id": "gpt-4o-preview"}
            ]
        });
        let models = parse_listing(&body).expect("data");
        assert_eq!(models.len(), 2);
        assert_eq!(models[0]["id"], "gpt-4o");
        assert_eq!(models[1]["id"], "gpt-4o-preview");
    }

    #[test]
    fn parse_listing_rejects_a_non_list() {
        assert!(parse_listing(&json!({"models": []})).is_none());
        assert!(parse_listing(&json!([])).is_none());
    }

    #[test]
    fn parse_page_openai_is_single_page() {
        let page = parse_page(&json!({
            "object": "list",
            "data": [{"id": "gpt-4o"}]
        }))
        .expect("page");
        assert_eq!(page.data.len(), 1);
        assert!(page.next_after.is_none());
    }

    #[test]
    fn parse_page_follows_anthropic_has_more() {
        let page = parse_page(&json!({
            "data": [{"id": "claude-3"}],
            "has_more": true,
            "last_id": "claude-3"
        }))
        .expect("page");
        assert_eq!(page.next_after.as_deref(), Some("claude-3"));
        assert!(
            parse_page(&json!({
                "data": [{"id": "claude-3"}],
                "has_more": true
            }))
            .is_none(),
            "has_more without last_id is a failed page"
        );
    }

    #[test]
    fn models_path_encodes_after_id() {
        assert_eq!(models_path(None), "/models");
        assert_eq!(
            models_path(Some("claude-3 opus")),
            "/models?after_id=claude-3%20opus"
        );
    }

    #[test]
    fn later_round_skips_a_fresh_foreign_source() {
        let fresh = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("t".into()),
            stale: false,
            data: vec![json!({"id": "gpt-4o"})],
            source: Some("https://new.example/v1".into()),
        };
        assert!(is_fresh_foreign(Some(&fresh), "https://old.example/v1"));
        assert!(
            !is_fresh_foreign(Some(&fresh), "https://new.example/v1"),
            "same source still refreshes"
        );
        let mut stale = fresh.clone();
        stale.stale = true;
        assert!(
            !is_fresh_foreign(Some(&stale), "https://old.example/v1"),
            "stale foreign may be replaced after a URL change"
        );
        assert!(!is_fresh_foreign(None, "https://old.example/v1"));
        let mut missing_source = fresh.clone();
        missing_source.source = None;
        assert!(!is_fresh_foreign(
            Some(&missing_source),
            "https://old.example/v1"
        ));
    }

    #[test]
    fn against_source_marks_a_url_change_stale() {
        let row = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("t".into()),
            stale: false,
            data: vec![json!({"id": "gpt-4o"})],
            source: Some("https://old.example/v1".into()),
        };
        let stale = row.clone().against_source("https://new.example/v1");
        assert!(stale.stale);
        assert_eq!(stale.data, row.data);
        assert!(!row.clone().against_source("https://old.example/v1").stale);
        assert!(
            row.data_if_source("https://new.example/v1").is_empty(),
            "namespaced /v1/models must not advertise another URL's ids"
        );
        assert_eq!(row.data_if_source("https://old.example/v1").len(), 1);
    }

    #[test]
    fn empty_backoff_doubles_then_caps() {
        assert_eq!(
            next_empty_backoff(Duration::from_secs(1)),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_empty_backoff(Duration::from_secs(16)),
            Duration::from_secs(30)
        );
        assert_eq!(
            next_empty_backoff(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn rfc3339_unix_epoch() {
        assert_eq!(rfc3339_utc(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            rfc3339_utc(UNIX_EPOCH + Duration::from_secs(86_400 + 3661)),
            "1970-01-02T01:01:01Z"
        );
    }
}
