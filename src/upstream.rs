use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Weak},
};

use chrono::{DateTime, Utc};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    auth_file::{AuthFile, AuthTokens, claims_from_auth},
    config::Config,
    crypto::TokenCrypto,
    db,
    error::{AppError, AppResult},
    models::{Account, SelectedAccount, UsageData, UsageSnapshot},
};

type RefreshMutex = tokio::sync::Mutex<()>;
static REFRESH_LOCKS: LazyLock<tokio::sync::Mutex<HashMap<Uuid, Weak<RefreshMutex>>>> =
    LazyLock::new(|| tokio::sync::Mutex::new(HashMap::new()));

#[derive(Debug, Deserialize)]
struct OAuthTokenPayload {
    access_token: Option<String>,
    refresh_token: Option<String>,
    id_token: Option<String>,
    error: Option<Value>,
    error_description: Option<String>,
    message: Option<String>,
    code: Option<String>,
}

pub async fn refresh_account_tokens(
    pool: &sqlx::SqlitePool,
    crypto: &TokenCrypto,
    client: &reqwest::Client,
    config: &Config,
    account_id: Uuid,
) -> AppResult<Account> {
    let observed = db::get_account(pool, account_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("account {account_id} not found")))?;
    let refresh_lock = account_refresh_lock(account_id).await;
    let _refresh_guard = refresh_lock.lock().await;
    let account = db::get_account(pool, account_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("account {account_id} not found")))?;
    if account.encrypted_refresh_token != observed.encrypted_refresh_token
        || account.encrypted_access_token != observed.encrypted_access_token
    {
        return Ok(account);
    }
    let refresh_token = crypto.decrypt(&account.encrypted_refresh_token)?;

    let response = client
        .post(config.token_refresh_url())
        .json(&serde_json::json!({
            "grant_type": "refresh_token",
            "client_id": config.oauth_client_id,
            "refresh_token": refresh_token,
            "scope": config.oauth_scope,
        }))
        .send()
        .await
        .map_err(|err| AppError::Upstream(format!("token refresh request failed: {err}")))?;
    let status = response.status();
    let payload: OAuthTokenPayload = response
        .json()
        .await
        .map_err(|err| AppError::Upstream(format!("invalid token refresh response: {err}")))?;

    if !status.is_success() {
        let message = oauth_error_message(&payload)
            .unwrap_or_else(|| format!("token refresh failed ({status})"));
        return Err(AppError::Upstream(message));
    }

    let access_token = payload
        .access_token
        .ok_or_else(|| AppError::Upstream("refresh response missing access_token".into()))?;
    let refresh_token = payload
        .refresh_token
        .ok_or_else(|| AppError::Upstream("refresh response missing refresh_token".into()))?;
    let id_token = payload
        .id_token
        .ok_or_else(|| AppError::Upstream("refresh response missing id_token".into()))?;

    let auth = AuthFile {
        openai_api_key: None,
        tokens: AuthTokens {
            access_token: access_token.clone(),
            refresh_token: refresh_token.clone(),
            id_token: id_token.clone(),
            account_id: None,
        },
        last_refresh_at: Some(Utc::now()),
    };
    let claims = claims_from_auth(&auth);

    db::update_account_tokens(pool, crypto, account_id, &auth, claims).await
}

async fn account_refresh_lock(account_id: Uuid) -> Arc<RefreshMutex> {
    let mut locks = REFRESH_LOCKS.lock().await;
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(&account_id).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(RefreshMutex::new(()));
    locks.insert(account_id, Arc::downgrade(&lock));
    lock
}

pub async fn refresh_account_usage(
    pool: &sqlx::SqlitePool,
    crypto: &TokenCrypto,
    client: &reqwest::Client,
    config: &Config,
    account_id: Uuid,
) -> AppResult<UsageSnapshot> {
    let account = db::get_account(pool, account_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("account {account_id} not found")))?;
    let access_token = crypto.decrypt(&account.encrypted_access_token)?;
    let raw = match fetch_usage_raw(
        client,
        config,
        &access_token,
        account.chatgpt_account_id.as_deref(),
    )
    .await
    {
        Ok(raw) => raw,
        Err(error) if is_auth_error(&error) => {
            let refreshed =
                refresh_account_tokens(pool, crypto, client, config, account_id).await?;
            let access_token = crypto.decrypt(&refreshed.encrypted_access_token)?;
            fetch_usage_raw(
                client,
                config,
                &access_token,
                refreshed.chatgpt_account_id.as_deref(),
            )
            .await?
        }
        Err(error) => return Err(error),
    };
    let fetched_at = Utc::now();
    let windows = crate::usage::parse_usage_windows(&raw, fetched_at);
    let plan_type = raw
        .get("plan_type")
        .or_else(|| raw.get("plan"))
        .and_then(Value::as_str);
    db::replace_usage_windows(pool, account.id, plan_type, fetched_at, &windows).await?;
    let (used_percent, reset_at) = parse_primary_usage_window(&raw);
    db::insert_usage_snapshot(pool, account.id, used_percent, None, None, reset_at, raw).await
}

pub async fn refresh_all_usage(
    pool: &sqlx::SqlitePool,
    crypto: &TokenCrypto,
    client: &reqwest::Client,
    config: &Config,
) -> AppResult<Vec<UsageSnapshot>> {
    let accounts = db::active_accounts(pool).await?;
    let mut snapshots = Vec::with_capacity(accounts.len());
    for account in accounts {
        match refresh_account_usage(pool, crypto, client, config, account.id).await {
            Ok(snapshot) => snapshots.push(snapshot),
            Err(error) => {
                db::mark_usage_error(pool, account.id, &error.to_string())
                    .await
                    .ok();
                tracing::warn!(
                    account_id = %account.id,
                    error = %error,
                    "usage refresh failed"
                );
            }
        }
    }
    Ok(snapshots)
}

fn is_auth_error(error: &AppError) -> bool {
    matches!(error, AppError::Upstream(message) if message.contains("401") || message.contains("403"))
}

pub fn build_upstream_responses_request<'a>(
    client: &'a reqwest::Client,
    config: &'a Config,
    selected: &'a SelectedAccount,
    body: bytes::Bytes,
    request_id: &'a str,
    incoming_headers: &HeaderMap,
    compact: bool,
) -> reqwest::RequestBuilder {
    let url = if compact {
        config.upstream_codex_compact_url()
    } else {
        config.upstream_codex_responses_url()
    };
    let mut request = client
        .post(url)
        .timeout(config.request_timeout)
        .header(
            AUTHORIZATION,
            format!("Bearer {}", selected.tokens.access_token),
        )
        .header(
            ACCEPT,
            if compact {
                "application/json"
            } else {
                "text/event-stream"
            },
        )
        .header(CONTENT_TYPE, "application/json")
        .header("x-request-id", request_id)
        .body(body);
    for (name, value) in incoming_headers {
        if should_forward_request_header(name.as_str()) {
            request = request.header(name, value);
        }
    }
    if let Some(account_id) = selected.account.chatgpt_account_id.as_deref() {
        request = request.header("chatgpt-account-id", account_id);
    }
    request
}

pub fn build_upstream_models_request<'a>(
    client: &'a reqwest::Client,
    config: &'a Config,
    selected: &'a SelectedAccount,
    request_id: &'a str,
    incoming_headers: &HeaderMap,
    raw_query: Option<&str>,
) -> reqwest::RequestBuilder {
    let mut url = config.upstream_codex_models_url();
    if let Some(query) = raw_query.filter(|query| !query.is_empty()) {
        url.push('?');
        url.push_str(query);
    }
    let mut request = client
        .get(url)
        .timeout(config.request_timeout)
        .header(
            AUTHORIZATION,
            format!("Bearer {}", selected.tokens.access_token),
        )
        .header(ACCEPT, "application/json")
        .header("x-request-id", request_id);
    for (name, value) in incoming_headers {
        if should_forward_request_header(name.as_str()) {
            request = request.header(name, value);
        }
    }
    if let Some(account_id) = selected.account.chatgpt_account_id.as_deref() {
        request = request.header("chatgpt-account-id", account_id);
    }
    request
}

fn should_forward_request_header(name: &str) -> bool {
    matches!(
        name,
        "user-agent"
            | "originator"
            | "openai-beta"
            | "accept-language"
            | "session_id"
            | "x-session-id"
            | "x-request-id"
            | "x-codex-session-id"
            | "x-codex-conversation-id"
            | "x-codex-turn-state"
            | "x-codex-turn-metadata"
            | "x-codex-client-version"
            | "x-codex-service-tier"
            | "x-openai-client-user-agent"
    ) || name.starts_with("x-stainless-")
}

pub fn model_from_body(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
}

pub fn extract_usage_from_sse(buffer: &str) -> UsageData {
    let mut usage = UsageData::default();
    for line in buffer.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(data) {
            merge_usage(&mut usage, &value);
        }
    }
    if usage.input_tokens.is_none()
        && usage.output_tokens.is_none()
        && let Some(value) = last_named_json_object(buffer, "usage")
    {
        merge_usage_fields(&mut usage, &value);
    }
    usage
}

pub fn extract_usage_from_json(value: &Value) -> UsageData {
    let mut usage = UsageData::default();
    merge_usage(&mut usage, value);
    usage
}

fn merge_usage(usage: &mut UsageData, value: &Value) {
    let candidates = [
        value.get("usage"),
        value.get("response").and_then(|v| v.get("usage")),
        value.get("item").and_then(|v| v.get("usage")),
    ];
    for candidate in candidates.into_iter().flatten() {
        merge_usage_fields(usage, candidate);
    }
}

fn merge_usage_fields(usage: &mut UsageData, candidate: &Value) {
    if let Some(input) = candidate.get("input_tokens").and_then(Value::as_i64) {
        usage.input_tokens = Some(input);
    }
    if let Some(output) = candidate.get("output_tokens").and_then(Value::as_i64) {
        usage.output_tokens = Some(output);
    }
    if let Some(cached) = candidate
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(Value::as_i64)
    {
        usage.cached_input_tokens = Some(cached);
    }
    if let Some(reasoning) = candidate
        .pointer("/output_tokens_details/reasoning_tokens")
        .and_then(Value::as_i64)
    {
        usage.reasoning_tokens = Some(reasoning);
    }
}

fn last_named_json_object(buffer: &str, name: &str) -> Option<Value> {
    let needle = format!("\"{name}\"");
    for (key_start, _) in buffer.rmatch_indices(&needle) {
        let after_key = &buffer[key_start + needle.len()..];
        let Some(colon) = after_key.find(':') else {
            continue;
        };
        let Some(relative_object) = after_key[colon + 1..].find('{') else {
            continue;
        };
        let object_offset = relative_object + colon + 1;
        let object_start = key_start + needle.len() + object_offset;
        let bytes = buffer.as_bytes();
        let mut depth = 0_u32;
        let mut in_string = false;
        let mut escaped = false;
        for index in object_start..bytes.len() {
            let byte = bytes[index];
            if in_string {
                if escaped {
                    escaped = false;
                } else if byte == b'\\' {
                    escaped = true;
                } else if byte == b'"' {
                    in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => in_string = true,
                b'{' => depth += 1,
                b'}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        if let Ok(value) = serde_json::from_slice(&bytes[object_start..=index]) {
                            return Some(value);
                        }
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    None
}

async fn fetch_usage_raw(
    client: &reqwest::Client,
    config: &Config,
    access_token: &str,
    account_id: Option<&str>,
) -> AppResult<Value> {
    let mut request = client
        .get(config.upstream_usage_url())
        .header(AUTHORIZATION, format!("Bearer {access_token}"))
        .header(ACCEPT, "application/json");
    if let Some(account_id) = account_id {
        request = request.header("chatgpt-account-id", account_id);
    }
    let response = request
        .send()
        .await
        .map_err(|err| AppError::Upstream(format!("usage request failed: {err}")))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|err| AppError::Upstream(format!("failed to read usage response: {err}")))?;
    let raw = serde_json::from_str::<Value>(&text)
        .unwrap_or_else(|_| Value::String(text.chars().take(1_000).collect()));
    if !status.is_success() {
        return Err(AppError::Upstream(format!(
            "usage request failed ({status}): {}",
            raw
        )));
    }
    if raw.is_string() {
        return Err(AppError::Upstream(
            "usage response was not valid JSON".to_string(),
        ));
    }
    Ok(raw)
}

fn parse_primary_usage_window(raw: &Value) -> (Option<f64>, Option<DateTime<Utc>>) {
    let primary = raw.pointer("/rate_limit/primary_window");
    let used_percent = primary
        .and_then(|value| value.get("used_percent"))
        .and_then(Value::as_f64);
    let reset_at = primary
        .and_then(|value| value.get("reset_at"))
        .and_then(Value::as_i64)
        .and_then(|epoch| DateTime::<Utc>::from_timestamp(epoch, 0));
    (used_percent, reset_at)
}

fn oauth_error_message(payload: &OAuthTokenPayload) -> Option<String> {
    if let Some(message) = payload.message.clone() {
        return Some(message);
    }
    if let Some(description) = payload.error_description.clone() {
        return Some(description);
    }
    if let Some(code) = payload.code.clone() {
        return Some(code);
    }
    match payload.error.as_ref()? {
        Value::String(value) => Some(value.clone()),
        Value::Object(map) => map
            .get("message")
            .or_else(|| map.get("error_description"))
            .or_else(|| map.get("code"))
            .and_then(Value::as_str)
            .map(str::to_string),
        _ => None,
    }
}

pub fn should_refresh(last_refresh_at: DateTime<Utc>, interval_days: i64) -> bool {
    Utc::now().signed_duration_since(last_refresh_at).num_days() >= interval_days
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{extract_usage_from_sse, model_from_body, parse_primary_usage_window};

    #[test]
    fn model_from_body_reads_model_field() {
        let model = model_from_body(br#"{"model":"gpt-5.1-codex-mini","input":"hi"}"#);

        assert_eq!(model.as_deref(), Some("gpt-5.1-codex-mini"));
    }

    #[test]
    fn extract_usage_from_sse_reads_nested_response_usage() {
        let usage = extract_usage_from_sse(concat!(
            "event: response.completed\n",
            "data: {\"response\":{\"usage\":{\"input_tokens\":12,\"output_tokens\":5,",
            "\"input_tokens_details\":{\"cached_tokens\":3},",
            "\"output_tokens_details\":{\"reasoning_tokens\":2}}}}\n\n",
            "data: [DONE]\n\n",
        ));

        assert_eq!(usage.input_tokens, Some(12));
        assert_eq!(usage.output_tokens, Some(5));
        assert_eq!(usage.cached_input_tokens, Some(3));
        assert_eq!(usage.reasoning_tokens, Some(2));
    }

    #[test]
    fn extract_usage_from_truncated_terminal_event_uses_usage_tail() {
        let usage = extract_usage_from_sse(concat!(
            "...truncated response output...\"usage\":{",
            "\"input_tokens\":101,\"output_tokens\":7,",
            "\"input_tokens_details\":{\"cached_tokens\":80},",
            "\"output_tokens_details\":{\"reasoning_tokens\":4}}}}\n\n",
            "data: [DONE]\n\n",
        ));

        assert_eq!(usage.input_tokens, Some(101));
        assert_eq!(usage.output_tokens, Some(7));
        assert_eq!(usage.cached_input_tokens, Some(80));
        assert_eq!(usage.reasoning_tokens, Some(4));
    }

    #[test]
    fn parse_primary_usage_window_reads_used_percent_and_reset() {
        let (used_percent, reset_at) = parse_primary_usage_window(&json!({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 42.5,
                    "reset_at": 1_700_000_000
                }
            }
        }));

        assert_eq!(used_percent, Some(42.5));
        assert_eq!(reset_at.unwrap().timestamp(), 1_700_000_000);
    }
}
