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
    models::{AccountUpdateRequest, LogsQuery},
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
    let (auth, claims) =
        parse_auth_json(payload).map_err(|err| AppError::BadRequest(err.to_string()))?;
    let account = db::upsert_account(&state.pool, &state.crypto, auth, claims).await?;
    Ok(Json(serde_json::json!({ "account": account })))
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
