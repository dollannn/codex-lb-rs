use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{Request, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::Response,
    routing::{get, patch, post},
};
use serde_json::Value;
use uuid::Uuid;

use crate::{
    auth_file::parse_auth_json,
    db,
    error::{AppError, AppResult},
    models::{AccountUpdateRequest, LogsQuery, ResolveSessionRoutesRequest, SessionRoutesQuery},
    state::AppState,
    upstream,
};

pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/accounts", get(list_accounts).post(import_account))
        .route(
            "/accounts/{id}",
            patch(update_account).delete(delete_account),
        )
        .route("/accounts/{id}/refresh-token", post(refresh_account_token))
        .route("/accounts/{id}/refresh-usage", post(refresh_account_usage))
        .route("/usage/summary", get(usage_summary))
        .route("/usage/accounts/{id}", get(account_usage))
        .route("/usage/refresh", post(refresh_all_usage))
        .route("/request-logs", get(request_logs))
        .route(
            "/session-routes",
            get(session_routes).post(resolve_session_routes),
        )
        .route("/settings", get(settings).put(update_settings))
        .route_layer(middleware::from_fn_with_state(state, admin_auth))
}

async fn admin_auth(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response, AppError> {
    let Some(expected) = state.config.admin_token.as_deref() else {
        return Ok(next.run(req).await);
    };
    let provided = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if provided == Some(expected) {
        Ok(next.run(req).await)
    } else {
        Err(AppError::Unauthorized("invalid admin token".to_string()))
    }
}

async fn list_accounts(State(state): State<AppState>) -> AppResult<Json<Value>> {
    let accounts = db::list_accounts(&state.pool).await?;
    Ok(Json(serde_json::json!({ "accounts": accounts })))
}

async fn import_account(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> AppResult<Json<Value>> {
    let label = payload
        .get("label")
        .and_then(Value::as_str)
        .map(str::to_string);
    let auth_payload = payload.get("auth").cloned().unwrap_or(payload);
    let (auth, claims) =
        parse_auth_json(auth_payload).map_err(|err| AppError::BadRequest(err.to_string()))?;
    let account = db::upsert_account(&state.pool, &state.crypto, auth, claims, label).await?;

    let mut warnings = Vec::new();
    let mut auth_ready = true;
    let account = if account
        .access_token_expires_at
        .is_some_and(|expiry| expiry <= chrono::Utc::now() + chrono::Duration::minutes(5))
    {
        match upstream::refresh_account_tokens(
            &state.pool,
            &state.crypto,
            &state.http,
            &state.config,
            account.id,
        )
        .await
        {
            Ok(account) => account,
            Err(error) => {
                let message = format!("initial token refresh failed: {error}");
                db::mark_auth_failed(&state.pool, account.id, &message)
                    .await
                    .ok();
                warnings.push(message);
                auth_ready = false;
                account
            }
        }
    } else {
        account
    };
    if auth_ready
        && let Err(error) = upstream::refresh_account_usage(
            &state.pool,
            &state.crypto,
            &state.http,
            &state.config,
            account.id,
        )
        .await
    {
        let message = format!("initial usage refresh failed: {error}");
        db::mark_usage_error(&state.pool, account.id, &message)
            .await
            .ok();
        warnings.push(message);
    }
    let account = db::get_account(&state.pool, account.id)
        .await?
        .ok_or_else(|| AppError::NotFound("imported account disappeared".to_string()))?;
    Ok(Json(
        serde_json::json!({ "account": account, "warnings": warnings }),
    ))
}

async fn update_account(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<AccountUpdateRequest>,
) -> AppResult<Json<Value>> {
    let account = db::update_account(
        &state.pool,
        id,
        payload.status,
        payload.label,
        payload.email,
        payload.plan_type,
    )
    .await?;
    Ok(Json(serde_json::json!({ "account": account })))
}

async fn delete_account(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<StatusCode> {
    db::delete_account(&state.pool, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn refresh_account_token(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<Value>> {
    let account = upstream::refresh_account_tokens(
        &state.pool,
        &state.crypto,
        &state.http,
        &state.config,
        id,
    )
    .await?;
    Ok(Json(serde_json::json!({ "account": account })))
}

async fn refresh_account_usage(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<Value>> {
    let snapshot =
        upstream::refresh_account_usage(&state.pool, &state.crypto, &state.http, &state.config, id)
            .await?;
    Ok(Json(serde_json::json!({ "usage": snapshot })))
}

async fn usage_summary(State(state): State<AppState>) -> AppResult<Json<Value>> {
    let summary = db::usage_summary(&state.pool).await?;
    Ok(Json(serde_json::json!({ "summary": summary })))
}

async fn account_usage(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> AppResult<Json<Value>> {
    let usage = db::list_usage_for_account(&state.pool, id, 100).await?;
    Ok(Json(serde_json::json!({ "usage": usage })))
}

async fn refresh_all_usage(State(state): State<AppState>) -> AppResult<Json<Value>> {
    let usage =
        upstream::refresh_all_usage(&state.pool, &state.crypto, &state.http, &state.config).await?;
    Ok(Json(serde_json::json!({ "usage": usage })))
}

async fn request_logs(
    State(state): State<AppState>,
    Query(query): Query<LogsQuery>,
) -> AppResult<Json<Value>> {
    let logs = db::list_request_logs(&state.pool, query).await?;
    Ok(Json(serde_json::json!({ "requestLogs": logs })))
}

async fn session_routes(
    State(state): State<AppState>,
    Query(query): Query<SessionRoutesQuery>,
) -> AppResult<Json<Value>> {
    let settings = db::runtime_settings(&state.pool).await?;
    let routes = db::list_session_routes(
        &state.pool,
        query.limit.unwrap_or(100),
        settings.sticky_session_ttl_seconds,
    )
    .await?;
    Ok(Json(serde_json::json!({
        "sessionRoutes": routes,
        "stickyTtlSeconds": settings.sticky_session_ttl_seconds,
        "semantics": "last_routed"
    })))
}

async fn resolve_session_routes(
    State(state): State<AppState>,
    Json(payload): Json<ResolveSessionRoutesRequest>,
) -> AppResult<Json<Value>> {
    if payload.key_hashes.len() > 500 {
        return Err(AppError::BadRequest(
            "at most 500 session hashes can be resolved at once".to_string(),
        ));
    }
    if payload
        .key_hashes
        .iter()
        .any(|hash| hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err(AppError::BadRequest(
            "session hashes must be 64 hexadecimal characters".to_string(),
        ));
    }
    let settings = db::runtime_settings(&state.pool).await?;
    let routes = db::resolve_session_routes(
        &state.pool,
        &payload.key_hashes,
        settings.sticky_session_ttl_seconds,
    )
    .await?;
    Ok(Json(serde_json::json!({
        "sessionRoutes": routes,
        "stickyTtlSeconds": settings.sticky_session_ttl_seconds,
        "semantics": "last_routed"
    })))
}

async fn settings(State(state): State<AppState>) -> AppResult<Json<Value>> {
    let settings = db::list_settings(&state.pool).await?;
    Ok(Json(serde_json::json!({ "settings": settings })))
}

async fn update_settings(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> AppResult<Json<Value>> {
    let object = payload.as_object().cloned().ok_or_else(|| {
        AppError::BadRequest("settings payload must be a JSON object".to_string())
    })?;
    let settings = db::upsert_settings(&state.pool, object).await?;
    Ok(Json(serde_json::json!({ "settings": settings })))
}
