use chrono::{DateTime, Utc};
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
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
    pool: &sqlx::PgPool,
    crypto: &TokenCrypto,
    client: &reqwest::Client,
    config: &Config,
    account_id: Uuid,
) -> AppResult<Account> {
    let account = db::get_account(pool, account_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("account {account_id} not found")))?;
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

    db::update_account_tokens(
        pool,
        crypto,
        account_id,
        &access_token,
        &refresh_token,
        &id_token,
        claims.chatgpt_account_id,
        Some(claims.email),
        Some(claims.plan_type),
    )
    .await
}

pub async fn refresh_account_usage(
    pool: &sqlx::PgPool,
    crypto: &TokenCrypto,
    client: &reqwest::Client,
    config: &Config,
    account_id: Uuid,
) -> AppResult<UsageSnapshot> {
    let account = db::get_account(pool, account_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("account {account_id} not found")))?;
    let access_token = crypto.decrypt(&account.encrypted_access_token)?;
    let raw = fetch_usage_raw(
        client,
        config,
        &access_token,
        account.chatgpt_account_id.as_deref(),
    )
    .await?;
    let (used_percent, reset_at) = parse_primary_usage_window(&raw);
    db::insert_usage_snapshot(pool, account.id, used_percent, None, None, reset_at, raw).await
}

pub async fn refresh_all_usage(
    pool: &sqlx::PgPool,
    crypto: &TokenCrypto,
    client: &reqwest::Client,
    config: &Config,
) -> AppResult<Vec<UsageSnapshot>> {
    let accounts = sqlx::query_as::<_, Account>(
        "SELECT * FROM accounts WHERE status = 'active' ORDER BY created_at ASC",
    )
    .fetch_all(pool)
    .await?;
    let mut snapshots = Vec::with_capacity(accounts.len());
    for account in accounts {
        let access_token = crypto.decrypt(&account.encrypted_access_token)?;
        let raw = fetch_usage_raw(
            client,
            config,
            &access_token,
            account.chatgpt_account_id.as_deref(),
        )
        .await?;
        let (used_percent, reset_at) = parse_primary_usage_window(&raw);
        snapshots.push(
            db::insert_usage_snapshot(pool, account.id, used_percent, None, None, reset_at, raw)
                .await?,
        );
    }
    Ok(snapshots)
}

pub fn build_upstream_responses_request<'a>(
    client: &'a reqwest::Client,
    config: &'a Config,
    selected: &'a SelectedAccount,
    body: bytes::Bytes,
    request_id: &'a str,
) -> reqwest::RequestBuilder {
    let mut request = client
        .post(config.upstream_codex_responses_url())
        .timeout(config.request_timeout)
        .header(
            AUTHORIZATION,
            format!("Bearer {}", selected.tokens.access_token),
        )
        .header(ACCEPT, "text/event-stream")
        .header(CONTENT_TYPE, "application/json")
        .header("x-request-id", request_id)
        .body(body);
    if let Some(account_id) = selected.account.chatgpt_account_id.as_deref() {
        request = request.header("chatgpt-account-id", account_id);
    }
    request
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
    usage
}

fn merge_usage(usage: &mut UsageData, value: &Value) {
    let candidates = [
        value.get("usage"),
        value.get("response").and_then(|v| v.get("usage")),
        value.get("item").and_then(|v| v.get("usage")),
    ];
    for candidate in candidates.into_iter().flatten() {
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
    let raw = response
        .json::<Value>()
        .await
        .map_err(|err| AppError::Upstream(format!("invalid usage response: {err}")))?;
    if !status.is_success() {
        return Err(AppError::Upstream(format!(
            "usage request failed ({status}): {}",
            raw
        )));
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
