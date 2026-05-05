use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use codex_lb_rs::{
    auth_file::{AuthFile, AuthTokens, claims_from_auth},
    build_app,
    config::Config,
    crypto::TokenCrypto,
    db,
    models::LogsQuery,
    state::AppState,
};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use uuid::Uuid;

const ADMIN_TOKEN: &str = "test-admin-token";
const PROXY_TOKEN: &str = "test-proxy-token";

#[tokio::test]
async fn postgres_admin_and_proxy_failover_smoke() -> Result<()> {
    let Some(database_url) = safe_test_database_url()? else {
        eprintln!("skipping Postgres integration smoke; set CODEX_LB_TEST_DATABASE_URL");
        return Ok(());
    };

    let pool = db::connect(&database_url).await?;
    db::run_migrations(&pool).await?;
    reset_database(&pool).await?;

    let fake_upstream = FakeUpstream::start().await?;
    let key_path = unique_key_path("postgres-proxy-smoke");
    let crypto = TokenCrypto::load_or_create(&key_path).await?;
    let config = test_config(&database_url, &fake_upstream.base_url, key_path);

    let rate_limited_account = insert_account(
        &pool,
        &crypto,
        "rate-limited-access",
        "acct-rate-limited",
        "rate@example.com",
    )
    .await?;
    let good_account = insert_account(
        &pool,
        &crypto,
        "good-access",
        "acct-good",
        "good@example.com",
    )
    .await?;
    db::upsert_settings(
        &pool,
        serde_json::Map::from_iter([
            ("proxy_max_attempts".to_string(), json!(2)),
            ("rate_limit_cooldown_seconds".to_string(), json!(120)),
        ]),
    )
    .await?;

    let paused = db::update_account(
        &pool,
        rate_limited_account,
        Some("paused".to_string()),
        None,
        None,
    )
    .await?;
    assert_eq!(paused.status, "paused");
    let reactivated = db::update_account(
        &pool,
        rate_limited_account,
        Some("active".to_string()),
        None,
        None,
    )
    .await?;
    assert_eq!(reactivated.status, "active");

    let app = build_app(AppState::new(config, pool.clone(), crypto.clone()));
    let app_server = TestServer::start(app).await?;
    let client = reqwest::Client::new();

    let health: Value = client
        .get(format!("{}/health", app_server.base_url))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(health["status"], "ok");

    let unauthorized_admin = client
        .get(format!("{}/admin/accounts", app_server.base_url))
        .send()
        .await?;
    assert_eq!(
        unauthorized_admin.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    let accounts: Value = client
        .get(format!("{}/admin/accounts", app_server.base_url))
        .bearer_auth(ADMIN_TOKEN)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(accounts["accounts"].as_array().unwrap().len(), 2);

    let unauthorized_proxy = client
        .get(format!("{}/v1/models", app_server.base_url))
        .bearer_auth("wrong-token")
        .send()
        .await?;
    assert_eq!(
        unauthorized_proxy.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    let response_text = client
        .post(format!(
            "{}/backend-api/codex/responses",
            app_server.base_url
        ))
        .bearer_auth(PROXY_TOKEN)
        .header("content-type", "application/json")
        .header("x-request-id", "integration-request-1")
        .body(json!({"model":"gpt-5.1-codex-mini","input":"hello"}).to_string())
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    assert!(response_text.contains("[DONE]"));

    let upstream_auth = fake_upstream.authorizations().await;
    assert_eq!(
        upstream_auth,
        vec![
            "Bearer rate-limited-access".to_string(),
            "Bearer good-access".to_string()
        ]
    );

    let logs = db::list_request_logs(
        &pool,
        LogsQuery {
            limit: Some(10),
            offset: None,
        },
    )
    .await?;
    assert_eq!(logs.len(), 2);
    assert!(logs.iter().any(|log| {
        log.account_id == Some(rate_limited_account)
            && log.status == "error"
            && log.error_code.as_deref() == Some("rate_limited")
    }));
    assert!(logs.iter().any(|log| {
        log.account_id == Some(good_account)
            && log.status == "success"
            && log.input_tokens == Some(7)
            && log.output_tokens == Some(11)
            && log.cached_input_tokens == Some(2)
            && log.reasoning_tokens == Some(3)
    }));

    let summary = db::usage_summary(&pool).await?;
    assert_eq!(summary.account_count, 2);
    assert_eq!(summary.active_account_count, 2);
    assert_eq!(summary.request_count, 2);
    assert_eq!(summary.successful_request_count, 1);
    assert_eq!(summary.failed_request_count, 1);
    assert_eq!(summary.input_tokens, 7);
    assert_eq!(summary.output_tokens, 11);

    reset_database(&pool).await?;
    Ok(())
}

fn safe_test_database_url() -> Result<Option<String>> {
    let Some(url) = std::env::var("CODEX_LB_TEST_DATABASE_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };

    let allow_destructive = std::env::var("CODEX_LB_ALLOW_DESTRUCTIVE_TEST_DB")
        .ok()
        .as_deref()
        == Some("1");
    if !allow_destructive && !url.to_ascii_lowercase().contains("test") {
        bail!(
            "CODEX_LB_TEST_DATABASE_URL must contain 'test' because integration tests truncate tables; set CODEX_LB_ALLOW_DESTRUCTIVE_TEST_DB=1 to override"
        );
    }
    Ok(Some(url))
}

async fn reset_database(pool: &PgPool) -> Result<()> {
    sqlx::query(
        "TRUNCATE request_logs, usage_snapshots, account_runtime_state, accounts RESTART IDENTITY CASCADE",
    )
    .execute(pool)
    .await?;
    sqlx::query("DELETE FROM settings").execute(pool).await?;
    sqlx::query(
        r#"
        INSERT INTO settings (key, value) VALUES
            ('routing_strategy', '"round_robin"'::jsonb),
            ('proxy_max_attempts', '2'::jsonb),
            ('rate_limit_cooldown_seconds', '60'::jsonb)
        ON CONFLICT (key) DO NOTHING
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_account(
    pool: &PgPool,
    crypto: &TokenCrypto,
    access_token: &str,
    account_id: &str,
    email: &str,
) -> Result<Uuid> {
    let auth = AuthFile {
        openai_api_key: None,
        tokens: AuthTokens {
            access_token: access_token.to_string(),
            refresh_token: format!("refresh-{account_id}"),
            id_token: jwt(json!({
                "email": email,
                "https://api.openai.com/auth": {
                    "chatgpt_account_id": account_id,
                    "chatgpt_plan_type": "plus"
                }
            })),
            account_id: Some(account_id.to_string()),
        },
        last_refresh_at: Some(Utc::now()),
    };
    let claims = claims_from_auth(&auth);
    let account = db::upsert_account(pool, crypto, auth, claims).await?;
    Ok(account.id)
}

fn jwt(payload: Value) -> String {
    format!(
        "{}.{}.sig",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
        URL_SAFE_NO_PAD.encode(payload.to_string())
    )
}

fn test_config(database_url: &str, upstream_origin: &str, encryption_key_file: PathBuf) -> Config {
    Config {
        database_url: database_url.to_string(),
        host: "127.0.0.1".to_string(),
        port: 0,
        upstream_base_url: format!("{}/backend-api", upstream_origin.trim_end_matches('/')),
        auth_base_url: upstream_origin.to_string(),
        oauth_client_id: "test-client".to_string(),
        oauth_scope: "openid profile email".to_string(),
        encryption_key_file,
        admin_token: Some(ADMIN_TOKEN.to_string()),
        proxy_api_token: Some(PROXY_TOKEN.to_string()),
        request_timeout: Duration::from_secs(5),
        token_refresh_interval_days: 3650,
    }
}

fn unique_key_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("codex-lb-rs-{name}-{}.key", Uuid::new_v4()))
}

struct TestServer {
    base_url: String,
    handle: JoinHandle<()>,
}

impl TestServer {
    async fn start(app: Router) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let handle = spawn_server(listener, app);
        Ok(Self {
            base_url: format!("http://{addr}"),
            handle,
        })
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn spawn_server(listener: TcpListener, app: Router) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            eprintln!("test server failed: {err}");
        }
    })
}

#[derive(Default)]
struct FakeUpstreamState {
    authorizations: Mutex<Vec<String>>,
}

struct FakeUpstream {
    base_url: String,
    state: Arc<FakeUpstreamState>,
    handle: JoinHandle<()>,
}

impl FakeUpstream {
    async fn start() -> Result<Self> {
        let state = Arc::new(FakeUpstreamState::default());
        let app = Router::new()
            .route("/backend-api/codex/responses", post(fake_responses))
            .route("/backend-api/wham/usage", get(fake_usage))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr: SocketAddr = listener.local_addr()?;
        let handle = spawn_server(listener, app);
        Ok(Self {
            base_url: format!("http://{addr}"),
            state,
            handle,
        })
    }

    async fn authorizations(&self) -> Vec<String> {
        self.state.authorizations.lock().await.clone()
    }
}

impl Drop for FakeUpstream {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn fake_responses(
    State(state): State<Arc<FakeUpstreamState>>,
    headers: HeaderMap,
    _body: Bytes,
) -> Response {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    state
        .authorizations
        .lock()
        .await
        .push(authorization.clone());

    match authorization.as_str() {
        "Bearer rate-limited-access" => (StatusCode::TOO_MANY_REQUESTS, "rate limited")
            .into_response(),
        "Bearer good-access" => (
            [(header::CONTENT_TYPE, "text/event-stream")],
            "data: {\"response\":{\"usage\":{\"input_tokens\":7,\"output_tokens\":11,\"input_tokens_details\":{\"cached_tokens\":2},\"output_tokens_details\":{\"reasoning_tokens\":3}}}}\n\ndata: [DONE]\n\n",
        )
            .into_response(),
        _ => (StatusCode::UNAUTHORIZED, "unexpected token").into_response(),
    }
}

async fn fake_usage() -> impl IntoResponse {
    axum::Json(json!({
        "plan": "plus",
        "usage": {
            "used_percent": 12.5,
            "reset_at": 1_700_000_000_i64
        }
    }))
}
