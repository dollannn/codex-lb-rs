use std::{io, time::Instant};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, Response, StatusCode, header},
    routing::{get, post},
};
use futures_util::StreamExt;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    db,
    error::{AppError, AppResult},
    models::{AccountTokens, SelectedAccount, UsageData},
    state::AppState,
    upstream,
};

const SSE_CAPTURE_LIMIT: usize = 4 * 1024 * 1024;
const RATE_LIMIT_COOLDOWN_SECONDS: i64 = 60;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/backend-api/codex/responses", post(proxy_responses))
        .route("/backend-api/codex/models", get(codex_models))
        .route("/v1/responses", post(proxy_responses))
        .route("/v1/models", get(v1_models))
}

async fn proxy_responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response<Body>> {
    validate_proxy_auth(&state, &headers)?;
    let request_id = request_id(&headers);
    let model = upstream::model_from_body(&body);
    let active_count = db::account_count(&state.pool).await?.max(1);
    let attempts = active_count.min(2);
    let mut last_error: Option<AppError> = None;

    for _ in 0..attempts {
        let mut selected = db::select_account(&state.pool, &state.crypto).await?;
        maybe_refresh_selected(&state, &mut selected).await;

        let started = Instant::now();
        let response = upstream::build_upstream_responses_request(
            &state.http,
            &state.config,
            &selected,
            body.clone(),
            &request_id,
        )
        .send()
        .await;

        let response = match response {
            Ok(response) => response,
            Err(err) => {
                db::cooldown_account(
                    &state.pool,
                    selected.account.id,
                    10,
                    "upstream request failed",
                )
                .await
                .ok();
                db::insert_request_log(
                    &state.pool,
                    &request_id,
                    Some(selected.account.id),
                    model.as_deref(),
                    "error",
                    Some("upstream_request_failed"),
                    Some(&err.to_string()),
                    UsageData::default(),
                    Some(started.elapsed().as_millis().min(i32::MAX as u128) as i32),
                )
                .await
                .ok();
                last_error = Some(AppError::Upstream(err.to_string()));
                continue;
            }
        };

        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "rate limited".to_string());
            db::cooldown_account(
                &state.pool,
                selected.account.id,
                RATE_LIMIT_COOLDOWN_SECONDS,
                "upstream rate limited",
            )
            .await
            .ok();
            db::insert_request_log(
                &state.pool,
                &request_id,
                Some(selected.account.id),
                model.as_deref(),
                "error",
                Some("rate_limited"),
                Some(&message),
                UsageData::default(),
                Some(started.elapsed().as_millis().min(i32::MAX as u128) as i32),
            )
            .await
            .ok();
            last_error = Some(AppError::Upstream(
                "selected account was rate limited".to_string(),
            ));
            continue;
        }

        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "auth failed".to_string());
            db::mark_auth_failed(&state.pool, selected.account.id, &message)
                .await
                .ok();
            db::insert_request_log(
                &state.pool,
                &request_id,
                Some(selected.account.id),
                model.as_deref(),
                "error",
                Some("auth_failed"),
                Some(&message),
                UsageData::default(),
                Some(started.elapsed().as_millis().min(i32::MAX as u128) as i32),
            )
            .await
            .ok();
            last_error = Some(AppError::Upstream(
                "selected account auth failed".to_string(),
            ));
            continue;
        }

        if !status.is_success() {
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| format!("upstream returned {status}"));
            db::insert_request_log(
                &state.pool,
                &request_id,
                Some(selected.account.id),
                model.as_deref(),
                "error",
                Some("upstream_error"),
                Some(&message),
                UsageData::default(),
                Some(started.elapsed().as_millis().min(i32::MAX as u128) as i32),
            )
            .await
            .ok();
            return Err(AppError::Upstream(message));
        }

        let stream = logged_sse_stream(
            state,
            response,
            selected.account.id,
            request_id,
            model,
            started,
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .body(Body::from_stream(stream))
            .map_err(|err| AppError::Internal(err.to_string()));
    }

    Err(last_error
        .unwrap_or_else(|| AppError::BadRequest("no account could handle request".to_string())))
}

fn logged_sse_stream(
    state: AppState,
    response: reqwest::Response,
    account_id: Uuid,
    request_id: String,
    model: Option<String>,
    started: Instant,
) -> impl futures_util::Stream<Item = Result<Bytes, io::Error>> {
    async_stream::try_stream! {
        let mut upstream = response.bytes_stream();
        let mut capture = String::new();
        let mut failure: Option<String> = None;

        while let Some(chunk) = upstream.next().await {
            match chunk {
                Ok(bytes) => {
                    if capture.len() < SSE_CAPTURE_LIMIT {
                        let remaining = SSE_CAPTURE_LIMIT - capture.len();
                        let slice = if bytes.len() > remaining { &bytes[..remaining] } else { &bytes[..] };
                        capture.push_str(&String::from_utf8_lossy(slice));
                    }
                    yield bytes;
                }
                Err(err) => {
                    failure = Some(err.to_string());
                    break;
                }
            }
        }

        let usage = upstream::extract_usage_from_sse(&capture);
        let latency_ms = Some(started.elapsed().as_millis().min(i32::MAX as u128) as i32);
        let (status, error_code, error_message) = if let Some(message) = failure.as_deref() {
            ("error", Some("upstream_stream_error"), Some(message))
        } else {
            ("success", None, None)
        };
        if let Err(err) = db::insert_request_log(
            &state.pool,
            &request_id,
            Some(account_id),
            model.as_deref(),
            status,
            error_code,
            error_message,
            usage,
            latency_ms,
        ).await {
            tracing::warn!(error = %err, "failed to persist request log");
        }
    }
}

async fn codex_models(
    State(_state): State<AppState>,
    headers: HeaderMap,
) -> AppResult<Json<Value>> {
    validate_proxy_auth(&_state, &headers)?;
    Ok(Json(serde_json::json!({
        "models": [
            { "id": "gpt-5.3-codex", "name": "GPT-5.3 Codex" },
            { "id": "gpt-5.1-codex-mini", "name": "GPT-5.1 Codex Mini" },
            { "id": "gpt-5.5", "name": "GPT-5.5" }
        ]
    })))
}

async fn v1_models(State(_state): State<AppState>, headers: HeaderMap) -> AppResult<Json<Value>> {
    validate_proxy_auth(&_state, &headers)?;
    Ok(Json(serde_json::json!({
        "object": "list",
        "data": [
            { "id": "gpt-5.3-codex", "object": "model", "owned_by": "codex-lb-rs" },
            { "id": "gpt-5.1-codex-mini", "object": "model", "owned_by": "codex-lb-rs" },
            { "id": "gpt-5.5", "object": "model", "owned_by": "codex-lb-rs" }
        ]
    })))
}

fn validate_proxy_auth(state: &AppState, headers: &HeaderMap) -> AppResult<()> {
    let Some(expected) = state.config.proxy_api_token.as_deref() else {
        return Ok(());
    };
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if provided == Some(expected) {
        Ok(())
    } else {
        Err(AppError::Unauthorized("invalid proxy token".to_string()))
    }
}

async fn maybe_refresh_selected(state: &AppState, selected: &mut SelectedAccount) {
    if !upstream::should_refresh(
        selected.account.last_refresh_at,
        state.config.token_refresh_interval_days,
    ) {
        return;
    }
    match upstream::refresh_account_tokens(
        &state.pool,
        &state.crypto,
        &state.http,
        &state.config,
        selected.account.id,
    )
    .await
    {
        Ok(account) => {
            let tokens = AccountTokens {
                access_token: state
                    .crypto
                    .decrypt(&account.encrypted_access_token)
                    .unwrap_or_else(|_| selected.tokens.access_token.clone()),
                refresh_token: state
                    .crypto
                    .decrypt(&account.encrypted_refresh_token)
                    .unwrap_or_else(|_| selected.tokens.refresh_token.clone()),
                id_token: state
                    .crypto
                    .decrypt(&account.encrypted_id_token)
                    .unwrap_or_else(|_| selected.tokens.id_token.clone()),
            };
            selected.account = account;
            selected.tokens = tokens;
        }
        Err(err) => {
            tracing::warn!(account_id = %selected.account.id, error = %err, "token refresh failed; trying existing token");
        }
    }
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}
