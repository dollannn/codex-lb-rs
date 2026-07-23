pub mod admin;
pub mod auth_file;
pub mod cli;
pub mod config;
pub mod crypto;
pub mod db;
pub mod error;
pub mod models;
pub mod pricing;
pub mod proxy;
pub mod scheduler;
pub mod session_registry;
pub mod state;
pub mod status;
pub mod upstream;
pub mod usage;
pub mod websocket;

use axum::{Json, Router, extract::State, routing::get};
use serde_json::Value;
use tower_http::trace::TraceLayer;

use crate::state::AppState;

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .nest("/admin", admin::router(state.clone()))
        .merge(status::router())
        .merge(proxy::router())
        .with_state(state)
        .layer(TraceLayer::new_for_http())
}

pub async fn health(State(state): State<AppState>) -> Json<Value> {
    let db_ok = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await
        .ok()
        == Some(1);
    Json(serde_json::json!({ "status": if db_ok { "ok" } else { "degraded" }, "database": db_ok }))
}
