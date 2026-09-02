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
use crate::store::{ProviderModels, StoreError};

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
        if refresh_all(&state, &mut stop).await == Round::Stopped {
            tracing::debug!("provider model discovery stopped");
            return;
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
}

enum RefreshError {
    Failed(String),
    Stopped,
}

async fn refresh_all(state: &AppState, stop: &mut oneshot::Receiver<()>) -> Round {
    let providers: Vec<(String, ProviderKind, String)> = {
        let snapshot = state.config();
        snapshot
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
            .collect()
    };
    for (id, kind, base_url) in &providers {
        if stopped(stop) {
            return Round::Stopped;
        }
        if let Err(error) = refresh_one(state, id, *kind, base_url, stop).await {
            match error {
                RefreshError::Stopped => return Round::Stopped,
                RefreshError::Failed(error) => {
                    tracing::warn!(
                        provider = %id,
                        error = %error,
                        "provider model discovery failed"
                    );
                    mark_stale(state, id).await;
                }
            }
        }
    }
    Round::Done
}

async fn refresh_one(
    state: &AppState,
    provider_id: &str,
    kind: ProviderKind,
    base_url: &str,
    stop: &mut oneshot::Receiver<()>,
) -> Result<(), RefreshError> {
    if stopped(stop) {
        return Err(RefreshError::Stopped);
    }
    stale_if_source_changed(state, provider_id, base_url).await;

    let snapshot = state.config();
    let Some(lease) = snapshot
        .credentials
        .discovery_lease(&snapshot.config, provider_id)
    else {
        return Err(RefreshError::Failed("no credential".into()));
    };
    let mut headers = Vec::new();
    if kind == ProviderKind::Anthropic {
        headers.push(("anthropic-version", AnthropicAdapter::VERSION.to_owned()));
    }
    let upstream = Upstream {
        base_url: base_url.to_owned(),
        api_key: lease.secret.clone(),
        auth: match kind {
            ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
            ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
        },
    };
    drop(snapshot);

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
                &upstream,
                &path,
                &headers,
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
                    tracing::warn!(
                        provider = provider_id,
                        pages = MAX_PAGES,
                        "provider model listing hit the page bound"
                    );
                    break;
                }
                after_id = Some(next);
            }
            None => break,
        }
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
    Ok(())
}

async fn stale_if_source_changed(state: &AppState, provider: &str, base_url: &str) {
    let Some(store) = state.store() else {
        return;
    };
    match store.get_provider_models(provider).await {
        Ok(Some(mut row)) if row.source.as_deref() != Some(base_url) => {
            row.stale = true;
            if let Err(error) = store.put_provider_models(row).await {
                tracing::warn!(
                    provider,
                    error = %error,
                    "could not persist stale provider model cache"
                );
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                provider,
                error = %error,
                "could not read provider model cache"
            );
        }
    }
}

async fn mark_stale(state: &AppState, provider: &str) {
    let Some(store) = state.store() else {
        return;
    };
    let existing = store.get_provider_models(provider).await;
    let Some(row) = stale_row(existing, provider) else {
        return;
    };
    if let Err(error) = store.put_provider_models(row).await {
        tracing::warn!(
            provider,
            error = %error,
            "could not persist stale provider model cache"
        );
    }
}

/// `Err` must not overwrite last-good. Only `Ok(None)` writes empty+stale.
fn stale_row(
    existing: Result<Option<ProviderModels>, StoreError>,
    provider: &str,
) -> Option<ProviderModels> {
    match existing {
        Ok(Some(mut row)) => {
            row.stale = true;
            Some(row)
        }
        Ok(None) => Some(ProviderModels::empty_stale(provider)),
        Err(error) => {
            tracing::warn!(
                provider,
                error = %error,
                "could not read provider model cache to mark stale"
            );
            None
        }
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
    fn stale_row_preserves_last_good_and_ignores_read_errors() {
        let good = ProviderModels {
            provider: "openai".into(),
            fetched_at: Some("2026-09-02T12:00:00Z".into()),
            stale: false,
            data: vec![json!({"id": "gpt-4o"})],
            source: Some("https://api.openai.com/v1".into()),
        };
        let marked = stale_row(Ok(Some(good.clone())), "openai").expect("row");
        assert!(marked.stale);
        assert_eq!(marked.data, good.data);
        assert_eq!(marked.fetched_at, good.fetched_at);
        assert_eq!(marked.source, good.source);

        let empty = stale_row(Ok(None), "openai").expect("empty");
        assert!(empty.stale);
        assert!(empty.data.is_empty());
        assert!(empty.fetched_at.is_none());

        assert!(stale_row(Err(StoreError::Unavailable("down".into())), "openai").is_none());
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
