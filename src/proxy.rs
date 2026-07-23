use std::{collections::HashSet, io, time::Instant};

use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{RawQuery, State},
    http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode, header},
    routing::{get, post},
};
use chrono::{Duration as ChronoDuration, Utc};
use futures_util::StreamExt;
use serde_json::Value;
use sqlx::SqlitePool;
use uuid::Uuid;

use crate::{
    db,
    error::{AppError, AppResult},
    models::{
        AccountTokens, NewRequestLog, SelectedAccount, SelectionReason, SessionRouteEpoch,
        SessionRouteKey, UsageData,
    },
    state::AppState,
    upstream,
};

const SSE_CAPTURE_LIMIT: usize = 1024 * 1024;
const LOG_MESSAGE_LIMIT: usize = 2_000;
const ERROR_BODY_LIMIT: usize = 1024 * 1024;
const AFFINITY_VALUE_LIMIT: usize = 64 * 1024;

pub fn router() -> Router<AppState> {
    Router::new()
        .route(
            "/backend-api/codex/responses",
            get(crate::websocket::proxy_responses).post(proxy_responses),
        )
        .route(
            "/backend-api/codex/responses/compact",
            post(proxy_responses_compact),
        )
        .route("/backend-api/codex/models", get(codex_models))
        .route(
            "/v1/responses",
            get(crate::websocket::proxy_responses).post(proxy_responses),
        )
        .route("/v1/responses/compact", post(proxy_responses_compact))
        .route("/v1/models", get(v1_models))
}

async fn proxy_responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response<Body>> {
    proxy_request(state, headers, body, false).await
}

async fn proxy_responses_compact(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> AppResult<Response<Body>> {
    proxy_request(state, headers, body, true).await
}

async fn proxy_request(
    state: AppState,
    headers: HeaderMap,
    body: Bytes,
    compact: bool,
) -> AppResult<Response<Body>> {
    validate_proxy_auth(&state, &headers)?;
    let request_id = request_id(&headers);
    let model = upstream::model_from_body(&body);
    let affinities = request_affinities(&headers, &body);
    let route_epoch = db::resolve_session_route_epoch(&state.pool, &affinities).await?;
    let affinity = selection_affinity(&affinities, route_epoch.as_ref());
    let settings = db::runtime_settings(&state.pool).await?;
    let active_count = db::account_count(&state.pool).await?;
    let attempts = proxy_attempts(active_count, settings.proxy_max_attempts);
    let mut excluded = HashSet::with_capacity(attempts);
    let mut last_error: Option<AppError> = None;
    let mut last_response: Option<Response<Body>> = None;

    for _ in 0..attempts {
        let selected = db::select_account_for_request(
            &state.pool,
            &state.crypto,
            affinity.map(|affinity| (affinity.key_hash.as_str(), affinity.kind.as_str())),
            &excluded,
            &settings,
            state.config.usage_refresh_interval,
        )
        .await;
        let mut selected = match selected {
            Ok(selected) => selected,
            Err(error) if !excluded.is_empty() => {
                last_error.get_or_insert(error);
                break;
            }
            Err(error) => return Err(error),
        };
        let account_id = selected.account.id;
        let selection_reason = selected.selection_reason;
        let lease = AccountLease::new(state.pool.clone(), account_id);
        maybe_refresh_selected(&state, &mut selected).await;

        let started = Instant::now();
        let response = send_upstream(
            &state,
            &selected,
            body.clone(),
            &headers,
            &request_id,
            compact,
        )
        .await;

        let mut response = match response {
            Ok(response) => response,
            Err(error) => {
                let message = error.to_string();
                db::cooldown_account(&state.pool, account_id, 10, "upstream request failed")
                    .await
                    .ok();
                log_request(
                    &state,
                    &request_id,
                    account_id,
                    selection_reason,
                    model.as_deref(),
                    RequestOutcome::error("upstream_request_failed", &message),
                    started,
                )
                .await;
                lease.release();
                excluded.insert(account_id);
                last_error = Some(AppError::Upstream(message));
                continue;
            }
        };

        // Access tokens can expire before the scheduled refresh. Refresh once on an
        // auth response, then retry the same request on the same account.
        if is_auth_status(response.status()) {
            let first_auth_response = BufferedUpstream::from_response(response).await;
            match refresh_selected(&state, &mut selected).await {
                Ok(()) => {
                    response = match send_upstream(
                        &state,
                        &selected,
                        body.clone(),
                        &headers,
                        &request_id,
                        compact,
                    )
                    .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            let message = error.to_string();
                            db::cooldown_account(
                                &state.pool,
                                account_id,
                                10,
                                "upstream retry failed",
                            )
                            .await
                            .ok();
                            log_request(
                                &state,
                                &request_id,
                                account_id,
                                selection_reason,
                                model.as_deref(),
                                RequestOutcome::error("upstream_retry_failed", &message),
                                started,
                            )
                            .await;
                            lease.release();
                            excluded.insert(account_id);
                            last_error = Some(AppError::Upstream(message));
                            continue;
                        }
                    };
                }
                Err(error) => {
                    let message = format!(
                        "token refresh failed: {error}; upstream: {}",
                        first_auth_response.message()
                    );
                    db::mark_auth_failed(&state.pool, account_id, &message)
                        .await
                        .ok();
                    log_request(
                        &state,
                        &request_id,
                        account_id,
                        selection_reason,
                        model.as_deref(),
                        RequestOutcome::error("auth_refresh_failed", &message),
                        started,
                    )
                    .await;
                    lease.release();
                    excluded.insert(account_id);
                    last_response = Some(first_auth_response.into_response()?);
                    last_error = Some(AppError::Upstream(message));
                    continue;
                }
            }
        }

        let status = response.status();
        if is_auth_status(status) {
            let buffered = BufferedUpstream::from_response(response).await;
            let message = buffered.message();
            db::mark_auth_failed(&state.pool, account_id, &message)
                .await
                .ok();
            log_request(
                &state,
                &request_id,
                account_id,
                selection_reason,
                model.as_deref(),
                RequestOutcome::error("auth_failed", &message),
                started,
            )
            .await;
            lease.release();
            excluded.insert(account_id);
            last_response = Some(buffered.into_response()?);
            last_error = Some(AppError::Upstream(
                "selected account auth failed".to_string(),
            ));
            continue;
        }

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let cooldown =
                retry_after_seconds(response.headers(), settings.rate_limit_cooldown_seconds);
            let buffered = BufferedUpstream::from_response(response).await;
            let message = buffered.message();
            db::cooldown_account(&state.pool, account_id, cooldown, "upstream rate limited")
                .await
                .ok();
            log_request(
                &state,
                &request_id,
                account_id,
                selection_reason,
                model.as_deref(),
                RequestOutcome::error("rate_limited", &message),
                started,
            )
            .await;
            lease.release();
            excluded.insert(account_id);
            last_response = Some(buffered.into_response()?);
            last_error = Some(AppError::Upstream(
                "all selected accounts were rate limited".to_string(),
            ));
            continue;
        }

        if status.is_server_error() {
            let buffered = BufferedUpstream::from_response(response).await;
            let message = buffered.message();
            db::cooldown_account(&state.pool, account_id, 10, "transient upstream error")
                .await
                .ok();
            log_request(
                &state,
                &request_id,
                account_id,
                selection_reason,
                model.as_deref(),
                RequestOutcome::error("upstream_server_error", &message),
                started,
            )
            .await;
            lease.release();
            excluded.insert(account_id);
            last_response = Some(buffered.into_response()?);
            last_error = Some(AppError::Upstream(message));
            continue;
        }

        if !status.is_success() {
            let buffered = BufferedUpstream::from_response(response).await;
            let message = buffered.message();
            log_request(
                &state,
                &request_id,
                account_id,
                selection_reason,
                model.as_deref(),
                RequestOutcome::error("upstream_error", &message),
                started,
            )
            .await;
            lease.release();
            return buffered.into_response();
        }

        let response_headers = forwarded_response_headers(response.headers());
        let event_stream = is_event_stream(response.headers());
        if !affinities.is_empty() {
            bind_affinities(&state, account_id, &affinities, route_epoch.as_ref()).await;
        }
        let stream = logged_response_stream(
            state,
            response,
            lease,
            StreamLogContext {
                request_id,
                model,
                selection_reason,
                started,
                event_stream,
                affinities,
                route_epoch,
            },
        );
        return build_response(status, response_headers, Body::from_stream(stream));
    }

    if let Some(response) = last_response {
        return Ok(response);
    }
    Err(last_error
        .unwrap_or_else(|| AppError::Unavailable("no account could handle request".to_string())))
}

async fn send_upstream(
    state: &AppState,
    selected: &SelectedAccount,
    body: Bytes,
    headers: &HeaderMap,
    request_id: &str,
    compact: bool,
) -> reqwest::Result<reqwest::Response> {
    upstream::build_upstream_responses_request(
        &state.http,
        &state.config,
        selected,
        body,
        request_id,
        headers,
        compact,
    )
    .send()
    .await
}

fn proxy_attempts(active_count: i64, max_attempts: usize) -> usize {
    if active_count <= 0 {
        1
    } else {
        (active_count as usize).min(max_attempts.max(1))
    }
}

struct StreamLogContext {
    request_id: String,
    model: Option<String>,
    selection_reason: SelectionReason,
    started: Instant,
    event_stream: bool,
    affinities: Vec<SessionRouteKey>,
    route_epoch: Option<SessionRouteEpoch>,
}

fn logged_response_stream(
    state: AppState,
    response: reqwest::Response,
    lease: AccountLease,
    context: StreamLogContext,
) -> impl futures_util::Stream<Item = Result<Bytes, io::Error>> {
    async_stream::try_stream! {
        let mut upstream = response.bytes_stream();
        let mut capture = TailCapture::new(SSE_CAPTURE_LIMIT);
        let mut failure: Option<String> = None;
        let account_id = lease.account_id;
        let mut lease = Some(lease);
        let mut detector = SseTerminalDetector::default();
        let mut settled = false;

        while let Some(chunk) = upstream.next().await {
            match chunk {
                Ok(bytes) => {
                    capture.push(&bytes);
                    if !settled
                        && let Some(terminal) = detector.observe(&bytes)
                    {
                        let (usage, response_id) = capture_metadata(&capture, true);
                        if let Some(response_id) = response_id {
                            bind_response_affinity(
                                &state,
                                account_id,
                                &response_id,
                                &context.affinities,
                                context.route_epoch.as_ref(),
                            ).await;
                        }
                        let (status, error_code) = match terminal {
                            SseTerminal::Completed => ("success", None),
                            SseTerminal::Incomplete => ("error", Some("response_incomplete")),
                            SseTerminal::Failed => ("error", Some("response_failed")),
                            SseTerminal::Error => ("error", Some("upstream_error")),
                        };
                        log_request(
                            &state,
                            &context.request_id,
                            account_id,
                            context.selection_reason,
                            context.model.as_deref(),
                            RequestOutcome {
                                status,
                                error_code,
                                error_message: None,
                                usage,
                            },
                            context.started,
                        ).await;
                        if let Some(lease) = lease.take() {
                            lease.release();
                        }
                        settled = true;
                    }
                    yield bytes;
                }
                Err(error) => {
                    failure = Some(error.to_string());
                    break;
                }
            }
        }

        if !settled {
            let (usage, response_id) = capture_metadata(&capture, context.event_stream);
            if let Some(response_id) = response_id {
                bind_response_affinity(
                    &state,
                    account_id,
                    &response_id,
                    &context.affinities,
                    context.route_epoch.as_ref(),
                ).await;
            }
            let (status, error_code, error_message) = if let Some(message) = failure.as_deref() {
                ("error", Some("upstream_stream_error"), Some(message))
            } else {
                ("success", None, None)
            };
            log_request(
                &state,
                &context.request_id,
                account_id,
                context.selection_reason,
                context.model.as_deref(),
                RequestOutcome {
                    status,
                    error_code,
                    error_message,
                    usage,
                },
                context.started,
            ).await;
            if let Some(lease) = lease.take() {
                lease.release();
            }
        }

        if let Some(message) = failure {
            Err(io::Error::other(message))?;
        }
    }
}

fn capture_metadata(capture: &TailCapture, assume_sse: bool) -> (UsageData, Option<String>) {
    let capture = capture.to_vec();
    let captured = String::from_utf8_lossy(&capture);
    let parse_as_sse = assume_sse || captured.starts_with("data:") || captured.contains("\ndata:");
    if parse_as_sse {
        (
            upstream::extract_usage_from_sse(&captured),
            response_id_from_sse(&captured),
        )
    } else {
        let json = serde_json::from_slice::<Value>(&capture).ok();
        (
            json.as_ref()
                .map(upstream::extract_usage_from_json)
                .unwrap_or_default(),
            json.as_ref()
                .and_then(response_id_from_value)
                .map(str::to_string),
        )
    }
}

struct RequestOutcome<'a> {
    status: &'a str,
    error_code: Option<&'a str>,
    error_message: Option<&'a str>,
    usage: UsageData,
}

impl<'a> RequestOutcome<'a> {
    fn error(code: &'a str, message: &'a str) -> Self {
        Self {
            status: "error",
            error_code: Some(code),
            error_message: Some(message),
            usage: UsageData::default(),
        }
    }
}

async fn log_request(
    state: &AppState,
    request_id: &str,
    account_id: Uuid,
    selection_reason: SelectionReason,
    model: Option<&str>,
    outcome: RequestOutcome<'_>,
    started: Instant,
) {
    if let Err(error) = db::insert_request_log(
        &state.pool,
        NewRequestLog {
            request_id,
            account_id: Some(account_id),
            model,
            status: outcome.status,
            selection_reason: Some(selection_reason),
            error_code: outcome.error_code,
            error_message: outcome.error_message,
            usage: outcome.usage,
            latency_ms: Some(started.elapsed().as_millis().min(i32::MAX as u128) as i32),
        },
    )
    .await
    {
        tracing::warn!(%error, "failed to persist request log");
    }
}

fn request_affinities(headers: &HeaderMap, body: &[u8]) -> Vec<SessionRouteKey> {
    const HEADER_KEYS: [(&str, &str); 6] = [
        ("x-codex-turn-state", "turn_state"),
        ("session_id", "session_id"),
        ("x-session-id", "session_id"),
        ("x-codex-session-id", "session_id"),
        ("x-codex-conversation-id", "conversation_id"),
        ("conversation_id", "conversation_id"),
    ];
    let mut seen = HashSet::new();
    let mut affinities = Vec::with_capacity(HEADER_KEYS.len() + 6);
    for (header_name, kind) in HEADER_KEYS {
        if let Some(value) = headers
            .get(header_name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .filter(|value| value.len() <= AFFINITY_VALUE_LIMIT)
        {
            let affinity = session_route_key(kind, value);
            if seen.insert(affinity.key_hash.clone()) {
                affinities.push(affinity);
            }
        }
    }

    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return affinities;
    };
    const BODY_KEYS: [(&str, &str); 6] = [
        ("/prompt_cache_key", "prompt_cache_key"),
        ("/session_id", "session_id"),
        ("/conversation_id", "conversation_id"),
        ("/previous_response_id", "response_id"),
        ("/metadata/session_id", "session_id"),
        ("/metadata/conversation_id", "conversation_id"),
    ];
    for (pointer, kind) in BODY_KEYS {
        if let Some(value) = value
            .pointer(pointer)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .filter(|value| value.len() <= AFFINITY_VALUE_LIMIT)
        {
            let affinity = session_route_key(kind, value);
            if seen.insert(affinity.key_hash.clone()) {
                affinities.push(affinity);
            }
        }
    }
    affinities
}

fn session_route_key(kind: &str, value: &str) -> SessionRouteKey {
    SessionRouteKey {
        key_hash: db::affinity_hash(kind, value),
        kind: kind.to_string(),
    }
}

fn selection_affinity<'a>(
    affinities: &'a [SessionRouteKey],
    route_epoch: Option<&SessionRouteEpoch>,
) -> Option<&'a SessionRouteKey> {
    route_epoch
        .and_then(|epoch| {
            affinities
                .iter()
                .find(|affinity| affinity.key_hash == epoch.selection_key_hash)
        })
        .or_else(|| affinities.first())
}

async fn bind_response_affinity(
    state: &AppState,
    account_id: Uuid,
    response_id: &str,
    affinities: &[SessionRouteKey],
    route_epoch: Option<&SessionRouteEpoch>,
) {
    let mut response_affinities = Vec::with_capacity(affinities.len() + 1);
    response_affinities.extend_from_slice(affinities);
    let response_affinity = session_route_key("response_id", response_id);
    if !response_affinities
        .iter()
        .any(|affinity| affinity.key_hash == response_affinity.key_hash)
    {
        response_affinities.push(response_affinity);
    }
    bind_affinities(state, account_id, &response_affinities, route_epoch).await;
}

async fn bind_affinities(
    state: &AppState,
    account_id: Uuid,
    affinities: &[SessionRouteKey],
    route_epoch: Option<&SessionRouteEpoch>,
) {
    match db::bind_affinities_at_epoch(&state.pool, affinities, account_id, route_epoch).await {
        Ok(true) => {}
        Ok(false) => tracing::debug!(
            %account_id,
            "ignored affinities from an obsolete session route generation"
        ),
        Err(error) => tracing::warn!(%error, "failed to persist request affinities"),
    }
}

fn response_id_from_sse(buffer: &str) -> Option<String> {
    buffer
        .lines()
        .rev()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != "[DONE]")
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|value| response_id_from_value(&value).map(str::to_string))
}

fn response_id_from_value(value: &Value) -> Option<&str> {
    value
        .get("id")
        .and_then(Value::as_str)
        .or_else(|| value.pointer("/response/id").and_then(Value::as_str))
}

struct TailCapture {
    buffer: Vec<u8>,
    capacity: usize,
    start: usize,
}

impl TailCapture {
    fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity.min(64 * 1024)),
            capacity,
            start: 0,
        }
    }

    fn push(&mut self, mut bytes: &[u8]) {
        if self.capacity == 0 {
            return;
        }
        if bytes.len() >= self.capacity {
            self.buffer.clear();
            self.buffer
                .extend_from_slice(&bytes[bytes.len() - self.capacity..]);
            self.start = 0;
            return;
        }

        if self.buffer.len() < self.capacity {
            let append = bytes.len().min(self.capacity - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..append]);
            bytes = &bytes[append..];
            if bytes.is_empty() {
                return;
            }
        }

        let first = bytes.len().min(self.capacity - self.start);
        self.buffer[self.start..self.start + first].copy_from_slice(&bytes[..first]);
        let remaining = bytes.len() - first;
        if remaining > 0 {
            self.buffer[..remaining].copy_from_slice(&bytes[first..]);
        }
        self.start = (self.start + bytes.len()) % self.capacity;
    }

    fn to_vec(&self) -> Vec<u8> {
        if self.buffer.len() < self.capacity || self.start == 0 {
            return self.buffer.clone();
        }
        let mut ordered = Vec::with_capacity(self.capacity);
        ordered.extend_from_slice(&self.buffer[self.start..]);
        ordered.extend_from_slice(&self.buffer[..self.start]);
        ordered
    }
}

#[derive(Clone, Copy)]
enum SseTerminal {
    Completed,
    Incomplete,
    Failed,
    Error,
}

#[derive(Default)]
struct SseTerminalDetector {
    carry: Vec<u8>,
    pending: Option<SseTerminal>,
}

impl SseTerminalDetector {
    fn observe(&mut self, bytes: &[u8]) -> Option<SseTerminal> {
        let mut combined = Vec::with_capacity(self.carry.len() + bytes.len());
        combined.extend_from_slice(&self.carry);
        combined.extend_from_slice(bytes);

        if let Some(terminal) = self.pending {
            if contains_sse_boundary(&combined) {
                self.pending = None;
                self.carry.clear();
                return Some(terminal);
            }
            self.carry = tail_bytes(&combined, 3);
            return None;
        }

        if let Some((position, terminal)) = find_sse_terminal(&combined) {
            let after_marker = &combined[position..];
            if contains_sse_boundary(after_marker) {
                self.carry.clear();
                return Some(terminal);
            }
            self.pending = Some(terminal);
            self.carry = tail_bytes(after_marker, 3);
            return None;
        }

        self.carry = tail_bytes(&combined, 64);
        None
    }
}

fn find_sse_terminal(bytes: &[u8]) -> Option<(usize, SseTerminal)> {
    const PATTERNS: [(&[u8], SseTerminal); 8] = [
        (b"\"type\":\"response.completed\"", SseTerminal::Completed),
        (b"\"type\": \"response.completed\"", SseTerminal::Completed),
        (b"\"type\":\"response.incomplete\"", SseTerminal::Incomplete),
        (
            b"\"type\": \"response.incomplete\"",
            SseTerminal::Incomplete,
        ),
        (b"\"type\":\"response.failed\"", SseTerminal::Failed),
        (b"\"type\": \"response.failed\"", SseTerminal::Failed),
        (b"\"type\":\"error\"", SseTerminal::Error),
        (b"\"type\": \"error\"", SseTerminal::Error),
    ];
    PATTERNS
        .into_iter()
        .filter_map(|(pattern, terminal)| {
            bytes
                .windows(pattern.len())
                .position(|window| window == pattern)
                .map(|position| (position, terminal))
        })
        .min_by_key(|(position, _)| *position)
}

fn contains_sse_boundary(bytes: &[u8]) -> bool {
    bytes.windows(2).any(|window| window == b"\n\n")
        || bytes.windows(4).any(|window| window == b"\r\n\r\n")
}

fn tail_bytes(bytes: &[u8], limit: usize) -> Vec<u8> {
    bytes[bytes.len().saturating_sub(limit)..].to_vec()
}

struct AccountLease {
    pool: SqlitePool,
    account_id: Uuid,
    armed: bool,
}

impl AccountLease {
    fn new(pool: SqlitePool, account_id: Uuid) -> Self {
        Self {
            pool,
            account_id,
            armed: true,
        }
    }

    fn release(mut self) {
        self.armed = false;
        spawn_release(self.pool.clone(), self.account_id);
    }
}

impl Drop for AccountLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        spawn_release(self.pool.clone(), self.account_id);
    }
}

fn spawn_release(pool: SqlitePool, account_id: Uuid) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(async move {
            if let Err(error) = db::release_account(&pool, account_id).await {
                tracing::warn!(%account_id, %error, "failed to release account lease");
            }
        });
    }
}

struct BufferedUpstream {
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    body: Bytes,
}

impl BufferedUpstream {
    async fn from_response(response: reqwest::Response) -> Self {
        let status = response.status();
        let headers = forwarded_response_headers(response.headers());
        let mut body = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    let remaining = ERROR_BODY_LIMIT.saturating_sub(body.len());
                    if remaining == 0 {
                        break;
                    }
                    body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                }
                Err(error) => {
                    if body.is_empty() {
                        body.extend_from_slice(
                            format!("failed to read upstream response: {error}").as_bytes(),
                        );
                    }
                    break;
                }
            }
        }
        Self {
            status,
            headers,
            body: Bytes::from(body),
        }
    }

    fn message(&self) -> String {
        String::from_utf8_lossy(&self.body)
            .chars()
            .take(LOG_MESSAGE_LIMIT)
            .collect()
    }

    fn into_response(self) -> AppResult<Response<Body>> {
        build_response(self.status, self.headers, Body::from(self.body))
    }
}

fn forwarded_response_headers(headers: &HeaderMap) -> Vec<(HeaderName, HeaderValue)> {
    headers
        .iter()
        .filter(|(name, _)| should_forward_response_header(name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

fn should_forward_response_header(name: &str) -> bool {
    matches!(
        name,
        "content-type"
            | "cache-control"
            | "etag"
            | "last-modified"
            | "vary"
            | "retry-after"
            | "x-request-id"
            | "openai-processing-ms"
            | "x-ratelimit-limit-requests"
            | "x-ratelimit-remaining-requests"
            | "x-ratelimit-reset-requests"
            | "x-codex-turn-state"
    ) || name.starts_with("x-codex-")
}

fn build_response(
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    body: Body,
) -> AppResult<Response<Body>> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        builder = builder.header(name, value);
    }
    builder
        .body(body)
        .map_err(|error| AppError::Internal(error.to_string()))
}

fn is_event_stream(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().contains("text/event-stream"))
}

fn is_auth_status(status: StatusCode) -> bool {
    status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN
}

fn retry_after_seconds(headers: &HeaderMap, fallback: i64) -> i64 {
    headers
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(fallback)
        .clamp(1, 60 * 60)
}

async fn codex_models(
    State(state): State<AppState>,
    RawQuery(raw_query): RawQuery,
    headers: HeaderMap,
) -> AppResult<Response<Body>> {
    validate_proxy_auth(&state, &headers)?;
    proxy_models(state, headers, raw_query.as_deref()).await
}

async fn proxy_models(
    state: AppState,
    headers: HeaderMap,
    raw_query: Option<&str>,
) -> AppResult<Response<Body>> {
    let request_id = request_id(&headers);
    let settings = db::runtime_settings(&state.pool).await?;
    let attempts = proxy_attempts(
        db::account_count(&state.pool).await?,
        settings.proxy_max_attempts,
    );
    let mut excluded = HashSet::with_capacity(attempts);
    let mut last_response = None;
    let mut last_error = None;

    for _ in 0..attempts {
        let selected = db::select_account_for_request(
            &state.pool,
            &state.crypto,
            None,
            &excluded,
            &settings,
            state.config.usage_refresh_interval,
        )
        .await;
        let mut selected = match selected {
            Ok(selected) => selected,
            Err(error) if !excluded.is_empty() => {
                last_error.get_or_insert(error);
                break;
            }
            Err(error) => return Err(error),
        };
        let account_id = selected.account.id;
        let lease = AccountLease::new(state.pool.clone(), account_id);
        maybe_refresh_selected(&state, &mut selected).await;

        let mut response =
            match send_models(&state, &selected, &headers, &request_id, raw_query).await {
                Ok(response) => response,
                Err(error) => {
                    db::cooldown_account(&state.pool, account_id, 10, "models request failed")
                        .await
                        .ok();
                    lease.release();
                    excluded.insert(account_id);
                    last_error = Some(AppError::Upstream(error.to_string()));
                    continue;
                }
            };

        if is_auth_status(response.status()) {
            let first_auth_response = BufferedUpstream::from_response(response).await;
            match refresh_selected(&state, &mut selected).await {
                Ok(()) => {
                    response = match send_models(
                        &state,
                        &selected,
                        &headers,
                        &request_id,
                        raw_query,
                    )
                    .await
                    {
                        Ok(response) => response,
                        Err(error) => {
                            lease.release();
                            excluded.insert(account_id);
                            last_error = Some(AppError::Upstream(error.to_string()));
                            continue;
                        }
                    };
                }
                Err(error) => {
                    let message = format!("model-list token refresh failed: {error}");
                    db::mark_auth_failed(&state.pool, account_id, &message)
                        .await
                        .ok();
                    lease.release();
                    excluded.insert(account_id);
                    last_response = Some(first_auth_response.into_response()?);
                    last_error = Some(AppError::Upstream(message));
                    continue;
                }
            }
        }

        let status = response.status();
        if is_auth_status(status)
            || status == StatusCode::TOO_MANY_REQUESTS
            || status.is_server_error()
        {
            let buffered = BufferedUpstream::from_response(response).await;
            if is_auth_status(status) {
                db::mark_auth_failed(&state.pool, account_id, &buffered.message())
                    .await
                    .ok();
            } else {
                let cooldown = if status == StatusCode::TOO_MANY_REQUESTS {
                    settings.rate_limit_cooldown_seconds
                } else {
                    10
                };
                db::cooldown_account(&state.pool, account_id, cooldown, "models request failed")
                    .await
                    .ok();
            }
            lease.release();
            excluded.insert(account_id);
            last_response = Some(buffered.into_response()?);
            last_error = Some(AppError::Upstream(format!(
                "upstream models request returned {status}"
            )));
            continue;
        }

        let response_headers = forwarded_response_headers(response.headers());
        let stream = leased_passthrough_stream(response, lease);
        return build_response(status, response_headers, Body::from_stream(stream));
    }

    if let Some(response) = last_response {
        return Ok(response);
    }
    Err(last_error
        .unwrap_or_else(|| AppError::Unavailable("no account could list models".to_string())))
}

async fn send_models(
    state: &AppState,
    selected: &SelectedAccount,
    headers: &HeaderMap,
    request_id: &str,
    raw_query: Option<&str>,
) -> reqwest::Result<reqwest::Response> {
    upstream::build_upstream_models_request(
        &state.http,
        &state.config,
        selected,
        request_id,
        headers,
        raw_query,
    )
    .send()
    .await
}

fn leased_passthrough_stream(
    response: reqwest::Response,
    lease: AccountLease,
) -> impl futures_util::Stream<Item = Result<Bytes, io::Error>> {
    async_stream::try_stream! {
        let mut upstream = response.bytes_stream();
        let mut lease = Some(lease);
        while let Some(chunk) = upstream.next().await {
            match chunk {
                Ok(bytes) => yield bytes,
                Err(error) => {
                    if let Some(lease) = lease.take() {
                        lease.release();
                    }
                    Err(io::Error::other(error.to_string()))?;
                }
            }
        }
        if let Some(lease) = lease.take() {
            lease.release();
        }
    }
}

async fn v1_models(State(state): State<AppState>, headers: HeaderMap) -> AppResult<Json<Value>> {
    validate_proxy_auth(&state, &headers)?;
    Ok(Json(serde_json::json!({
        "object": "list",
        "data": [
            { "id": "gpt-5.6-terra", "object": "model", "owned_by": "codex-lb-rs" },
            { "id": "gpt-5.6-sol", "object": "model", "owned_by": "codex-lb-rs" },
            { "id": "gpt-5.5", "object": "model", "owned_by": "codex-lb-rs" },
            { "id": "gpt-5.3-codex", "object": "model", "owned_by": "codex-lb-rs" },
            { "id": "gpt-5.1-codex-mini", "object": "model", "owned_by": "codex-lb-rs" }
        ]
    })))
}

pub(crate) fn validate_proxy_auth(state: &AppState, headers: &HeaderMap) -> AppResult<()> {
    // Browser WebSockets are not protected by CORS, and simple cross-origin
    // HTTP requests can still reach loopback services. Native Codex/OpenCode
    // clients do not send Origin, so reject browser-originated proxy traffic.
    if headers.contains_key(header::ORIGIN) {
        return Err(AppError::Unauthorized(
            "browser-origin proxy requests are not allowed".to_string(),
        ));
    }
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

pub(crate) async fn maybe_refresh_selected(state: &AppState, selected: &mut SelectedAccount) {
    let expires_soon = selected
        .account
        .access_token_expires_at
        .is_some_and(|expires_at| expires_at <= Utc::now() + ChronoDuration::minutes(15));
    if !expires_soon
        && !upstream::should_refresh(
            selected.account.last_refresh_at,
            state.config.token_refresh_interval_days,
        )
    {
        return;
    }
    if let Err(error) = refresh_selected(state, selected).await {
        tracing::warn!(account_id = %selected.account.id, %error, "proactive token refresh failed; trying existing token");
    }
}

pub(crate) async fn refresh_selected(
    state: &AppState,
    selected: &mut SelectedAccount,
) -> AppResult<()> {
    let account = upstream::refresh_account_tokens(
        &state.pool,
        &state.crypto,
        &state.http,
        &state.config,
        selected.account.id,
    )
    .await?;
    let tokens = AccountTokens {
        access_token: state.crypto.decrypt(&account.encrypted_access_token)?,
        refresh_token: state.crypto.decrypt(&account.encrypted_refresh_token)?,
        id_token: state.crypto.decrypt(&account.encrypted_id_token)?,
    };
    selected.account = account;
    selected.tokens = tokens;
    Ok(())
}

fn request_id(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;

    use super::{
        SSE_CAPTURE_LIMIT, SseTerminal, SseTerminalDetector, TailCapture, proxy_attempts,
        request_affinities,
    };

    #[test]
    fn proxy_attempts_use_one_attempt_when_no_accounts_so_error_is_reported() {
        assert_eq!(proxy_attempts(0, 3), 1);
    }

    #[test]
    fn proxy_attempts_limit_by_active_accounts_and_setting() {
        assert_eq!(proxy_attempts(5, 3), 3);
        assert_eq!(proxy_attempts(2, 5), 2);
    }

    #[test]
    fn proxy_attempts_never_use_zero_setting() {
        assert_eq!(proxy_attempts(2, 0), 1);
    }

    #[test]
    fn affinity_values_are_hashed_and_stable() {
        let first = request_affinities(
            &HeaderMap::new(),
            br#"{"prompt_cache_key":"secret-session"}"#,
        );
        let second = request_affinities(
            &HeaderMap::new(),
            br#"{"prompt_cache_key":"secret-session"}"#,
        );

        assert_eq!(first, second);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].key_hash.len(), 64);
        assert!(!first[0].key_hash.contains("secret-session"));
    }

    #[test]
    fn collects_and_deduplicates_all_request_affinity_aliases() {
        let mut headers = HeaderMap::new();
        headers.insert("x-session-id", "session-a".parse().unwrap());
        headers.insert("x-codex-session-id", "session-a".parse().unwrap());

        let affinities = request_affinities(
            &headers,
            br#"{"prompt_cache_key":"cache-b","session_id":"session-a"}"#,
        );

        assert_eq!(affinities.len(), 2);
        assert_eq!(affinities[0].kind, "session_id");
        assert_eq!(affinities[1].kind, "prompt_cache_key");
    }

    #[test]
    fn capture_keeps_only_the_tail() {
        let mut capture = TailCapture::new(SSE_CAPTURE_LIMIT);
        capture.push(&vec![b'a'; SSE_CAPTURE_LIMIT - 2]);
        capture.push(b"bcde");
        let capture = capture.to_vec();

        assert_eq!(capture.len(), SSE_CAPTURE_LIMIT);
        assert_eq!(&capture[..2], b"aa");
        assert_eq!(&capture[capture.len() - 4..], b"bcde");
    }

    #[test]
    fn terminal_detector_handles_split_marker_and_boundary() {
        let mut detector = SseTerminalDetector::default();
        assert!(
            detector
                .observe(b"data: {\"type\":\"response.com")
                .is_none()
        );
        assert!(
            detector
                .observe(b"pleted\",\"response\":{\"usage\":{}}}\n")
                .is_none()
        );
        assert!(matches!(
            detector.observe(b"\n"),
            Some(SseTerminal::Completed)
        ));
    }
}
