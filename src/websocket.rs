use std::{collections::HashSet, time::Instant};

use axum::{
    extract::{
        State,
        ws::{
            CloseFrame as ClientCloseFrame, Message as ClientMessage, WebSocket, WebSocketUpgrade,
        },
    },
    http::{HeaderMap, HeaderName, HeaderValue, Response, StatusCode, header},
    response::Response as AxumResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use sqlx::SqlitePool;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{
        Error as WebSocketError, Message as UpstreamMessage, client::IntoClientRequest,
        protocol::CloseFrame as UpstreamCloseFrame,
    },
};
use uuid::Uuid;

use crate::{
    db,
    error::{AppError, AppResult},
    models::{NewRequestLog, SelectedAccount, UsageData},
    proxy,
    state::AppState,
    upstream,
};

const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;
const CONNECT_TIMEOUT_SECONDS: u64 = 10;
const OPENAI_BETA_WEBSOCKET_V2: &str = "responses_websockets=2026-02-06";

type UpstreamSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub async fn proxy_responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> AppResult<AxumResponse> {
    proxy::validate_proxy_auth(&state, &headers)?;
    let affinity = websocket_affinity(&headers);
    let prepared = select_and_connect(&state, &headers, affinity.as_ref()).await?;
    let account_id = prepared.selected.account.id;
    let upstream_headers = prepared.response_headers;
    let upstream_socket = prepared.socket;

    let mut response = upgrade
        .max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |client| bridge(state, client, upstream_socket, account_id, affinity));
    copy_upstream_handshake_headers(&upstream_headers, response.headers_mut());
    Ok(response)
}

struct PreparedWebSocket {
    selected: SelectedAccount,
    socket: UpstreamSocket,
    response_headers: HeaderMap,
}

async fn select_and_connect(
    state: &AppState,
    incoming_headers: &HeaderMap,
    affinity: Option<&AffinityKey>,
) -> AppResult<PreparedWebSocket> {
    let settings = db::runtime_settings(&state.pool).await?;
    let active_count = db::account_count(&state.pool).await?;
    let attempts = if active_count <= 0 {
        1
    } else {
        (active_count as usize).min(settings.proxy_max_attempts.max(1))
    };
    let mut excluded = HashSet::with_capacity(attempts);
    let mut last_error = None;

    for _ in 0..attempts {
        let selected = db::select_account_for_request(
            &state.pool,
            &state.crypto,
            affinity.map(|item| (item.hash.as_str(), item.kind.as_str())),
            &excluded,
            &settings,
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
        let reservation = AccountReservation::new(state.pool.clone(), account_id);
        proxy::maybe_refresh_selected(state, &mut selected).await;

        let mut refreshed = false;
        let connection = loop {
            match connect_upstream(state, &selected, incoming_headers).await {
                Ok(connection) => break Ok(connection),
                Err(error) if is_auth_error(&error) && !refreshed => {
                    let status = websocket_error_status(&error);
                    refreshed = true;
                    if let Err(refresh_error) = proxy::refresh_selected(state, &mut selected).await
                    {
                        let message =
                            format!("WebSocket authentication refresh failed: {refresh_error}");
                        db::mark_auth_failed(&state.pool, account_id, &message)
                            .await
                            .ok();
                        break Err((AppError::Upstream(message), status));
                    }
                }
                Err(error) => {
                    let status = websocket_error_status(&error);
                    break Err((AppError::Upstream(websocket_error_message(&error)), status));
                }
            }
        };

        match connection {
            Ok((socket, response)) => {
                reservation.release().await;
                if let Some(affinity) = affinity
                    && let Err(error) =
                        db::bind_affinity(&state.pool, &affinity.hash, &affinity.kind, account_id)
                            .await
                {
                    tracing::warn!(%account_id, %error, "failed to bind WebSocket affinity");
                }
                return Ok(PreparedWebSocket {
                    selected,
                    response_headers: response.headers().clone(),
                    socket,
                });
            }
            Err((error, status)) => {
                if status.is_some_and(|status| {
                    status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN
                }) {
                    db::mark_auth_failed(
                        &state.pool,
                        account_id,
                        "upstream WebSocket authentication failed after refresh",
                    )
                    .await
                    .ok();
                } else if status == Some(StatusCode::TOO_MANY_REQUESTS) {
                    db::cooldown_account(
                        &state.pool,
                        account_id,
                        settings.rate_limit_cooldown_seconds,
                        "upstream WebSocket rate limited",
                    )
                    .await
                    .ok();
                } else {
                    db::cooldown_account(
                        &state.pool,
                        account_id,
                        10,
                        "upstream WebSocket unavailable",
                    )
                    .await
                    .ok();
                }
                reservation.release().await;
                excluded.insert(account_id);
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        AppError::Upstream("no account could open a Responses WebSocket".to_string())
    }))
}

async fn connect_upstream(
    state: &AppState,
    selected: &SelectedAccount,
    incoming_headers: &HeaderMap,
) -> Result<(UpstreamSocket, Response<Option<Vec<u8>>>), WebSocketError> {
    let url = state
        .config
        .upstream_codex_responses_websocket_url()
        .map_err(|error| {
            WebSocketError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                error.to_string(),
            ))
        })?;
    let mut request = url.into_client_request()?;
    for (name, value) in incoming_headers {
        if should_forward_handshake_header(name.as_str()) {
            request.headers_mut().insert(name, value.clone());
        }
    }
    request.headers_mut().insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {}", selected.tokens.access_token))
            .map_err(|error| WebSocketError::HttpFormat(error.into()))?,
    );
    if let Some(account_id) = selected.account.chatgpt_account_id.as_deref() {
        request.headers_mut().insert(
            HeaderName::from_static("chatgpt-account-id"),
            HeaderValue::from_str(account_id)
                .map_err(|error| WebSocketError::HttpFormat(error.into()))?,
        );
    }
    if !request.headers().contains_key("openai-beta") {
        request.headers_mut().insert(
            HeaderName::from_static("openai-beta"),
            HeaderValue::from_static(OPENAI_BETA_WEBSOCKET_V2),
        );
    }

    match tokio::time::timeout(
        std::time::Duration::from_secs(CONNECT_TIMEOUT_SECONDS),
        connect_async(request),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(WebSocketError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "upstream WebSocket connection timed out",
        ))),
    }
}

fn should_forward_handshake_header(name: &str) -> bool {
    matches!(
        name,
        "user-agent"
            | "originator"
            | "openai-beta"
            | "accept-language"
            | "session-id"
            | "thread-id"
            | "x-session-id"
            | "x-client-request-id"
            | "traceparent"
            | "tracestate"
    ) || name.starts_with("x-codex-")
        || name.starts_with("x-openai-")
        || name.starts_with("x-stainless-")
}

fn copy_upstream_handshake_headers(source: &HeaderMap, target: &mut HeaderMap) {
    for name in [
        "x-reasoning-included",
        "x-models-etag",
        "openai-model",
        "x-codex-turn-state",
    ] {
        if let Some(value) = source.get(name) {
            target.insert(HeaderName::from_static(name), value.clone());
        }
    }
}

async fn bridge(
    state: AppState,
    mut client: WebSocket,
    mut upstream_socket: UpstreamSocket,
    account_id: Uuid,
    affinity: Option<AffinityKey>,
) {
    let mut active_turn: Option<ActiveTurn> = None;
    let idle_timeout = state.config.request_timeout;
    let write_timeout = idle_timeout.min(std::time::Duration::from_secs(30));

    let disconnect_message = loop {
        let turn_deadline = active_turn
            .as_ref()
            .map(|turn| tokio::time::Instant::from_std(turn.started + idle_timeout));
        let hard_turn_timeout = async move {
            if let Some(deadline) = turn_deadline {
                tokio::time::sleep_until(deadline).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(hard_turn_timeout);
        tokio::select! {
            _ = &mut hard_turn_timeout => {
                break "WebSocket request exceeded the configured request budget";
            }
            client_message = tokio::time::timeout(idle_timeout, client.recv()) => {
                let client_message = match client_message {
                    Ok(Some(message)) => message,
                    Ok(None) => break "Codex closed the WebSocket before a terminal response",
                    Err(_) => break "Codex WebSocket idle timeout before a terminal response",
                };
                let client_message = match client_message {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::debug!(%account_id, %error, "Codex WebSocket read failed");
                        break "Codex WebSocket read failed before a terminal response";
                    }
                };

                if let ClientMessage::Text(text) = &client_message
                    && let Some(request) = parse_response_create(text.as_str())
                {
                    if let Some(previous) = active_turn.take() {
                        finish_disconnected_turn(
                            &state,
                            account_id,
                            previous,
                            "a new WebSocket request started before the previous request completed",
                        ).await;
                    }
                    match start_turn(&state, account_id, request).await {
                        Ok(turn) => {
                            if let Some(affinity) = affinity.as_ref()
                                && let Err(error) = db::bind_affinity(
                                    &state.pool,
                                    &affinity.hash,
                                    &affinity.kind,
                                    account_id,
                                )
                                .await
                            {
                                tracing::warn!(%account_id, %error, "failed to refresh WebSocket session route");
                            }
                            active_turn = Some(turn);
                        }
                        Err(error) => {
                            tracing::warn!(%account_id, %error, "failed to account WebSocket request");
                            break "local request accounting failed";
                        }
                    }
                }

                let closes = matches!(client_message, ClientMessage::Close(_));
                match tokio::time::timeout(
                    write_timeout,
                    upstream_socket.send(to_upstream_message(client_message)),
                ).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::debug!(%account_id, %error, "upstream WebSocket write failed");
                        break "upstream WebSocket write failed before a terminal response";
                    }
                    Err(_) => break "upstream WebSocket write timeout before a terminal response",
                }
                if closes {
                    break "Codex closed the WebSocket before a terminal response";
                }
            }
            upstream_message = tokio::time::timeout(idle_timeout, upstream_socket.next()) => {
                let upstream_message = match upstream_message {
                    Ok(Some(message)) => message,
                    Ok(None) => break "upstream WebSocket closed before a terminal response",
                    Err(_) => break "upstream WebSocket idle timeout before a terminal response",
                };
                let upstream_message = match upstream_message {
                    Ok(message) => message,
                    Err(error) => {
                        tracing::debug!(%account_id, %error, "upstream WebSocket read failed");
                        break "upstream WebSocket read failed before a terminal response";
                    }
                };

                let mut reconnect_for_retry = false;
                if let UpstreamMessage::Text(text) = &upstream_message
                    && let Ok(event) = serde_json::from_str::<Value>(text.as_str())
                    && terminal_event(&event).is_some()
                {
                    reconnect_for_retry = should_retry_on_new_connection(&event);
                    if let Some(turn) = active_turn.take() {
                        finish_turn(&state, account_id, turn, &event).await;
                    }
                }
                if reconnect_for_retry {
                    break "upstream requested a retry on a new WebSocket connection";
                }

                let closes = matches!(upstream_message, UpstreamMessage::Close(_));
                if let Some(message) = to_client_message(upstream_message) {
                    match tokio::time::timeout(write_timeout, client.send(message)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => {
                            tracing::debug!(%account_id, %error, "Codex WebSocket write failed");
                            break "Codex WebSocket write failed before a terminal response";
                        }
                        Err(_) => break "Codex WebSocket write timeout before a terminal response",
                    }
                }
                if closes {
                    break "upstream WebSocket closed before a terminal response";
                }
            }
        }
    };

    if let Some(turn) = active_turn {
        finish_disconnected_turn(&state, account_id, turn, disconnect_message).await;
    }
    let _ = tokio::time::timeout(write_timeout, upstream_socket.close(None)).await;
    let _ = tokio::time::timeout(write_timeout, client.close()).await;
}

struct ParsedRequest {
    request_id: String,
    model: Option<String>,
}

fn parse_response_create(text: &str) -> Option<ParsedRequest> {
    let value = serde_json::from_str::<Value>(text).ok()?;
    if value.get("type").and_then(Value::as_str) != Some("response.create") {
        return None;
    }
    let request_id = [
        "/client_metadata/turn_id",
        "/client_metadata/request_id",
        "/client_metadata/x-codex-ws-stream-request-start-ms",
    ]
    .into_iter()
    .find_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
    .filter(|value| !value.trim().is_empty())
    .map(str::to_string)
    .unwrap_or_else(|| Uuid::new_v4().to_string());
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .map(str::to_string);
    Some(ParsedRequest { request_id, model })
}

struct ActiveTurn {
    request_id: String,
    model: Option<String>,
    started: Instant,
    lease: AccountLease,
}

async fn start_turn(
    state: &AppState,
    account_id: Uuid,
    request: ParsedRequest,
) -> AppResult<ActiveTurn> {
    if !db::acquire_account_if_available(&state.pool, account_id).await? {
        return Err(AppError::Unavailable(
            "selected WebSocket account is no longer available; reconnect required".to_string(),
        ));
    }
    Ok(ActiveTurn {
        request_id: request.request_id,
        model: request.model,
        started: Instant::now(),
        lease: AccountLease::new(state.pool.clone(), account_id),
    })
}

#[derive(Clone, Copy)]
enum TerminalEvent {
    Completed,
    Incomplete,
    Failed,
    Error,
}

fn terminal_event(value: &Value) -> Option<TerminalEvent> {
    match value.get("type").and_then(Value::as_str)? {
        "response.completed" => Some(TerminalEvent::Completed),
        "response.incomplete" => Some(TerminalEvent::Incomplete),
        "response.failed" => Some(TerminalEvent::Failed),
        "error" => Some(TerminalEvent::Error),
        _ => None,
    }
}

fn should_retry_on_new_connection(value: &Value) -> bool {
    let status = event_status(value);
    matches!(status, Some(401 | 403 | 429 | 500..=599))
        || value.pointer("/error/code").and_then(Value::as_str)
            == Some("websocket_connection_limit_reached")
}

fn event_status(value: &Value) -> Option<u64> {
    value
        .get("status")
        .or_else(|| value.get("status_code"))
        .and_then(|status| {
            status
                .as_u64()
                .or_else(|| status.as_str().and_then(|status| status.parse().ok()))
        })
}

async fn finish_turn(state: &AppState, account_id: Uuid, turn: ActiveTurn, event: &Value) {
    let terminal = terminal_event(event).unwrap_or(TerminalEvent::Error);
    let (status, fallback_code) = match terminal {
        TerminalEvent::Completed => ("success", None),
        TerminalEvent::Incomplete => ("error", Some("response_incomplete")),
        TerminalEvent::Failed => ("error", Some("response_failed")),
        TerminalEvent::Error => ("error", Some("upstream_error")),
    };
    let error_code = event
        .pointer("/error/code")
        .or_else(|| event.pointer("/error/type"))
        .and_then(Value::as_str)
        .or(fallback_code);
    let error_message = event
        .pointer("/error/message")
        .or_else(|| event.get("message"))
        .and_then(Value::as_str);
    let usage = upstream::extract_usage_from_json(event);

    if let Some(response_id) = event
        .pointer("/response/id")
        .or_else(|| event.get("id"))
        .and_then(Value::as_str)
    {
        bind_response_affinity(state, account_id, response_id).await;
    }
    if status == "error" {
        apply_terminal_error_state(state, account_id, event, error_message).await;
    }
    persist_turn(
        state,
        account_id,
        &turn,
        status,
        error_code,
        error_message,
        usage,
    )
    .await;
    turn.lease.release().await;
}

async fn finish_disconnected_turn(
    state: &AppState,
    account_id: Uuid,
    turn: ActiveTurn,
    message: &str,
) {
    persist_turn(
        state,
        account_id,
        &turn,
        "error",
        Some("websocket_disconnected"),
        Some(message),
        UsageData::default(),
    )
    .await;
    turn.lease.release().await;
}

#[allow(clippy::too_many_arguments)]
async fn persist_turn(
    state: &AppState,
    account_id: Uuid,
    turn: &ActiveTurn,
    status: &str,
    error_code: Option<&str>,
    error_message: Option<&str>,
    usage: UsageData,
) {
    if let Err(error) = db::insert_request_log(
        &state.pool,
        NewRequestLog {
            request_id: &turn.request_id,
            account_id: Some(account_id),
            model: turn.model.as_deref(),
            status,
            error_code,
            error_message,
            usage,
            latency_ms: Some(turn.started.elapsed().as_millis().min(i32::MAX as u128) as i32),
        },
    )
    .await
    {
        tracing::warn!(%account_id, %error, "failed to persist WebSocket request log");
    }
}

async fn apply_terminal_error_state(
    state: &AppState,
    account_id: Uuid,
    event: &Value,
    error_message: Option<&str>,
) {
    let status = event_status(event);
    if status == Some(StatusCode::TOO_MANY_REQUESTS.as_u16().into()) {
        let settings = db::runtime_settings(&state.pool).await.unwrap_or_default();
        db::cooldown_account(
            &state.pool,
            account_id,
            settings.rate_limit_cooldown_seconds,
            "upstream WebSocket rate limited",
        )
        .await
        .ok();
    } else if matches!(status, Some(401 | 403)) {
        let message = error_message.unwrap_or("upstream WebSocket authentication failed");
        db::mark_auth_failed(&state.pool, account_id, message)
            .await
            .ok();
    } else if status.is_some_and(|status| (500..=599).contains(&status)) {
        db::cooldown_account(
            &state.pool,
            account_id,
            10,
            "upstream WebSocket server error",
        )
        .await
        .ok();
    }
}

fn to_upstream_message(message: ClientMessage) -> UpstreamMessage {
    match message {
        ClientMessage::Text(text) => UpstreamMessage::Text(text.to_string().into()),
        ClientMessage::Binary(bytes) => UpstreamMessage::Binary(bytes),
        ClientMessage::Ping(bytes) => UpstreamMessage::Ping(bytes),
        ClientMessage::Pong(bytes) => UpstreamMessage::Pong(bytes),
        ClientMessage::Close(frame) => {
            UpstreamMessage::Close(frame.map(|frame| UpstreamCloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            }))
        }
    }
}

fn to_client_message(message: UpstreamMessage) -> Option<ClientMessage> {
    match message {
        UpstreamMessage::Text(text) => Some(ClientMessage::Text(text.to_string().into())),
        UpstreamMessage::Binary(bytes) => Some(ClientMessage::Binary(bytes)),
        UpstreamMessage::Ping(bytes) => Some(ClientMessage::Ping(bytes)),
        UpstreamMessage::Pong(bytes) => Some(ClientMessage::Pong(bytes)),
        UpstreamMessage::Close(frame) => {
            Some(ClientMessage::Close(frame.map(|frame| ClientCloseFrame {
                code: frame.code.into(),
                reason: frame.reason.to_string().into(),
            })))
        }
        UpstreamMessage::Frame(_) => None,
    }
}

#[derive(Debug)]
struct AffinityKey {
    hash: String,
    kind: String,
}

fn websocket_affinity(headers: &HeaderMap) -> Option<AffinityKey> {
    const KEYS: [(&str, &str); 5] = [
        ("session-id", "session_id"),
        ("x-session-id", "session_id"),
        ("x-codex-session-id", "session_id"),
        ("thread-id", "thread_id"),
        ("x-client-request-id", "thread_id"),
    ];
    for (header_name, kind) in KEYS {
        if let Some(value) = headers
            .get(header_name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty() && value.len() <= 64 * 1024)
        {
            return Some(AffinityKey::new(kind, value));
        }
    }
    None
}

impl AffinityKey {
    fn new(kind: &str, value: &str) -> Self {
        Self {
            hash: db::affinity_hash(kind, value),
            kind: kind.to_string(),
        }
    }
}

async fn bind_response_affinity(state: &AppState, account_id: Uuid, response_id: &str) {
    let affinity = AffinityKey::new("response_id", response_id);
    if let Err(error) =
        db::bind_affinity(&state.pool, &affinity.hash, &affinity.kind, account_id).await
    {
        tracing::warn!(%account_id, %error, "failed to bind WebSocket response affinity");
    }
}

fn is_auth_error(error: &WebSocketError) -> bool {
    websocket_error_status(error)
        .is_some_and(|status| status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN)
}

fn websocket_error_status(error: &WebSocketError) -> Option<StatusCode> {
    match error {
        WebSocketError::Http(response) => Some(response.status()),
        _ => None,
    }
}

fn websocket_error_message(error: &WebSocketError) -> String {
    match error {
        WebSocketError::Http(response) => {
            let body = response
                .body()
                .as_ref()
                .map(|bytes| String::from_utf8_lossy(bytes))
                .unwrap_or_default();
            format!(
                "upstream WebSocket handshake failed ({}): {}",
                response.status(),
                body.chars().take(2_000).collect::<String>()
            )
        }
        _ => format!("upstream WebSocket connection failed: {error}"),
    }
}

struct AccountReservation {
    pool: SqlitePool,
    account_id: Uuid,
    armed: bool,
}

impl AccountReservation {
    fn new(pool: SqlitePool, account_id: Uuid) -> Self {
        Self {
            pool,
            account_id,
            armed: true,
        }
    }

    async fn release(mut self) {
        match db::release_account_reservation(&self.pool, self.account_id).await {
            Ok(()) => self.armed = false,
            Err(error) => {
                tracing::warn!(account_id = %self.account_id, %error, "failed to release WebSocket account reservation");
            }
        }
    }
}

impl Drop for AccountReservation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let pool = self.pool.clone();
        let account_id = self.account_id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = db::release_account_reservation(&pool, account_id).await {
                    tracing::warn!(%account_id, %error, "failed to release dropped WebSocket reservation");
                }
            });
        }
    }
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

    async fn release(mut self) {
        match db::release_account(&self.pool, self.account_id).await {
            Ok(()) => self.armed = false,
            Err(error) => {
                tracing::warn!(account_id = %self.account_id, %error, "failed to release WebSocket account lease");
            }
        }
    }
}

impl Drop for AccountLease {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let pool = self.pool.clone();
        let account_id = self.account_id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = db::release_account(&pool, account_id).await {
                    tracing::warn!(%account_id, %error, "failed to release dropped WebSocket account lease");
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::extract::ws::{CloseFrame, Message};
    use axum::http::{HeaderMap, HeaderValue};
    use serde_json::json;

    use super::{
        event_status, parse_response_create, should_retry_on_new_connection, to_client_message,
        to_upstream_message, websocket_affinity,
    };

    #[test]
    fn parses_websocket_response_create_metadata() {
        let request = parse_response_create(
            r#"{"type":"response.create","model":"gpt-test","client_metadata":{"turn_id":"turn-1"}}"#,
        )
        .expect("response.create");

        assert_eq!(request.request_id, "turn-1");
        assert_eq!(request.model.as_deref(), Some("gpt-test"));
    }

    #[test]
    fn hashes_websocket_session_affinity() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "session-id",
            HeaderValue::from_static("private-session-value"),
        );

        let first = websocket_affinity(&headers).expect("affinity");
        let second = websocket_affinity(&headers).expect("affinity");
        assert_eq!(first.hash, second.hash);
        assert_eq!(first.kind, "session_id");
        assert!(!first.hash.contains("private-session-value"));
    }

    #[test]
    fn retryable_terminal_errors_move_to_a_new_connection() {
        assert!(should_retry_on_new_connection(
            &json!({"type":"error", "status":429})
        ));
        assert!(should_retry_on_new_connection(&json!({
            "type":"error",
            "status":400,
            "error":{"code":"websocket_connection_limit_reached"}
        })));
        assert!(!should_retry_on_new_connection(
            &json!({"type":"error", "status":400})
        ));
        assert_eq!(event_status(&json!({"status_code": 429})), Some(429));
        assert_eq!(event_status(&json!({"status_code": "503"})), Some(503));
    }

    #[test]
    fn preserves_close_code_and_reason_across_bridge() {
        let upstream = to_upstream_message(Message::Close(Some(CloseFrame {
            code: 1008,
            reason: "policy rejected".into(),
        })));
        let restored = to_client_message(upstream).expect("close frame");
        let Message::Close(Some(frame)) = restored else {
            panic!("expected close frame");
        };
        assert_eq!(frame.code, 1008);
        assert_eq!(frame.reason, "policy rejected");
    }
}
