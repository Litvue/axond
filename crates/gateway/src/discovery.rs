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

/// Refresh every configured provider, then sleep `interval`, until `stop`.
/// The first round runs immediately so a GET after boot can see a cache.
pub async fn run(state: AppState, interval: Duration, mut stop: oneshot::Receiver<()>) {
    loop {
        refresh_all(&state).await;
        tokio::select! {
            _ = &mut stop => {
                tracing::debug!("provider model discovery stopped");
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

pub async fn refresh_all(state: &AppState) {
    let snapshot = state.config();
    for provider in &snapshot.config.provider {
        if let Err(error) = refresh_one(
            state,
            provider.id.as_str(),
            provider.kind,
            &provider.base_url,
        )
        .await
        {
            tracing::warn!(
                provider = %provider.id,
                error = %error,
                "provider model discovery failed"
            );
            mark_stale(state, &provider.id).await;
        }
    }
}

async fn refresh_one(
    state: &AppState,
    provider_id: &str,
    kind: ProviderKind,
    base_url: &str,
) -> Result<(), String> {
    let snapshot = state.config();
    let Some(lease) = snapshot
        .credentials
        .discovery_lease(&snapshot.config, provider_id)
    else {
        return Err("no credential".into());
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
    let body = state
        .0
        .dispatcher
        .get_json(
            provider_id,
            &upstream,
            "/models",
            &headers,
            Deadline::at(Instant::now() + Duration::from_secs(30)),
        )
        .await
        .map_err(|error| error.to_string())?;
    let Some(data) = parse_listing(&body) else {
        return Err("upstream listing is not an OpenAI-shaped `data` array".into());
    };
    let row = ProviderModels {
        provider: provider_id.to_owned(),
        fetched_at: Some(rfc3339_utc(SystemTime::now())),
        stale: false,
        data,
    };
    state
        .store()
        .ok_or_else(|| "store unavailable".to_owned())?
        .put_provider_models(row)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn mark_stale(state: &AppState, provider: &str) {
    let Some(store) = state.store() else {
        return;
    };
    let row = match store.get_provider_models(provider).await {
        Ok(Some(mut row)) => {
            row.stale = true;
            row
        }
        Ok(None) | Err(StoreError::Unavailable(_)) | Err(StoreError::Invalid(_)) => {
            ProviderModels::empty_stale(provider)
        }
        Err(StoreError::Duplicate(_) | StoreError::NotFound(_)) => {
            ProviderModels::empty_stale(provider)
        }
    };
    if let Err(error) = store.put_provider_models(row).await {
        tracing::warn!(
            provider,
            error = %error,
            "could not persist stale provider model cache"
        );
    }
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
    fn rfc3339_unix_epoch() {
        assert_eq!(rfc3339_utc(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            rfc3339_utc(UNIX_EPOCH + Duration::from_secs(86_400 + 3661)),
            "1970-01-02T01:01:01Z"
        );
    }
}
