use std::{collections::HashSet, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{
        State,
        ws::{Message as AxumWebSocketMessage, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{Duration as ChronoDuration, Utc};
use codex_lb_rs::{
    auth_file::{AuthFile, AuthTokens, claims_from_auth},
    build_app,
    config::Config,
    crypto::TokenCrypto,
    db,
    models::LogsQuery,
    state::AppState,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use sqlx::{SqlitePool, migrate::Migrate};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Error as ClientWebSocketError, Message as ClientWebSocketMessage, client::IntoClientRequest,
    },
};
use uuid::Uuid;

const ADMIN_TOKEN: &str = "test-admin-token";
const PROXY_TOKEN: &str = "test-proxy-token";

#[tokio::test]
async fn sqlite_admin_and_proxy_failover_smoke() -> Result<()> {
    let storage = TestStorage::new();
    let database_url = storage.database_url();
    let pool = db::connect(&database_url).await?;
    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await?;
    let foreign_keys: i64 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&pool)
        .await?;
    let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&pool)
        .await?;
    assert_eq!(journal_mode, "wal");
    assert_eq!(foreign_keys, 1);
    assert!(busy_timeout >= 5_000);

    db::run_migrations(&pool).await?;

    let fake_upstream = FakeUpstream::start().await?;
    let key_path = storage.key_path.clone();
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
        None,
    )
    .await?;
    assert_eq!(reactivated.status, "active");

    let settings = db::runtime_settings(&pool).await?;
    let first_selection = db::select_account_for_request(
        &pool,
        &crypto,
        None,
        &HashSet::new(),
        &settings,
        config.usage_refresh_interval,
    )
    .await?;
    db::release_account(&pool, first_selection.account.id).await?;
    let second_selection = db::select_account_for_request(
        &pool,
        &crypto,
        None,
        &HashSet::new(),
        &settings,
        config.usage_refresh_interval,
    )
    .await?;
    assert_ne!(first_selection.account.id, second_selection.account.id);
    assert_eq!(
        HashSet::from([first_selection.account.id, second_selection.account.id]),
        HashSet::from([rate_limited_account, good_account])
    );
    db::release_account(&pool, second_selection.account.id).await?;

    db::cooldown_account(&pool, good_account, 10, "transient upstream error").await?;
    let cooling = db::list_accounts(&pool)
        .await?
        .into_iter()
        .find(|account| account.id == good_account)
        .expect("cooling account");
    assert_eq!(cooling.status_reason, None);
    assert_eq!(
        cooling.cooldown_reason.as_deref(),
        Some("transient upstream error")
    );
    sqlx::query(
        "UPDATE account_runtime_state SET cooldown_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-1 second') WHERE account_id = $1",
    )
    .bind(good_account)
    .execute(&pool)
    .await?;
    assert!(db::acquire_account_if_available(&pool, good_account).await?);
    db::release_account(&pool, good_account).await?;
    let recovered = db::list_accounts(&pool)
        .await?
        .into_iter()
        .find(|account| account.id == good_account)
        .expect("recovered account");
    assert_eq!(recovered.cooldown_until, None);
    assert_eq!(recovered.cooldown_reason, None);

    let state = AppState::new(config, pool.clone(), crypto.clone());
    let app = build_app(state.clone());
    let app_server = TestServer::start(app).await?;
    let client = reqwest::Client::new();

    db::update_account(
        &pool,
        good_account,
        Some("paused".to_string()),
        None,
        None,
        None,
    )
    .await?;
    sqlx::query(
        "UPDATE account_runtime_state SET cooldown_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+60 seconds') WHERE account_id = $1",
    )
    .bind(rate_limited_account)
    .execute(&pool)
    .await?;
    let temporarily_unavailable = client
        .post(format!(
            "{}/backend-api/codex/responses",
            app_server.base_url
        ))
        .bearer_auth(PROXY_TOKEN)
        .json(&json!({"model":"gpt-test","input":"retry me"}))
        .send()
        .await?;
    assert_eq!(
        temporarily_unavailable.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    let unavailable_body: Value = temporarily_unavailable.json().await?;
    assert_eq!(unavailable_body["error"]["code"], "unavailable");
    db::update_account(
        &pool,
        good_account,
        Some("active".to_string()),
        None,
        None,
        None,
    )
    .await?;
    sqlx::query("UPDATE account_runtime_state SET cooldown_until = NULL WHERE account_id = $1")
        .bind(rate_limited_account)
        .execute(&pool)
        .await?;

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

    let browser_origin_proxy = client
        .get(format!("{}/v1/models", app_server.base_url))
        .bearer_auth(PROXY_TOKEN)
        .header(header::ORIGIN, "https://malicious.example")
        .send()
        .await?;
    assert_eq!(
        browser_origin_proxy.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    let response = client
        .post(format!(
            "{}/backend-api/codex/responses",
            app_server.base_url
        ))
        .bearer_auth(PROXY_TOKEN)
        .header("content-type", "application/json")
        .header("x-request-id", "integration-request-1")
        .body(json!({"model":"gpt-5.1-codex-mini","input":"hello"}).to_string())
        .send()
        .await?;
    let response_status = response.status();
    let response_text = response.text().await?;
    assert!(
        response_status.is_success(),
        "proxy returned {response_status}: {response_text}"
    );
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
            && log.input_tokens == Some(1_200)
            && log.output_tokens == Some(11)
            && log.cached_input_tokens == Some(200)
            && log.cache_write_input_tokens == Some(100)
            && log.reasoning_tokens == Some(3)
            && log.effective_model.as_deref() == Some("gpt-5.6-sol")
            && log.effective_service_tier.as_deref() == Some("default")
            && log.api_cost_status.as_deref() == Some("complete")
            && log.api_cost_lower_nano_usd == Some(5_555_000)
            && log.api_cost_upper_nano_usd == Some(5_555_000)
    }));

    let accounts = db::list_accounts(&pool).await?;
    let good_summary = accounts
        .iter()
        .find(|account| account.id == good_account)
        .expect("good account summary");
    assert_eq!(good_summary.cached_input_tokens, 200);
    assert_eq!(good_summary.observed_cache_write_input_tokens, 100);
    assert_eq!(good_summary.api_cost_lower_nano_usd, 5_555_000);
    assert_eq!(good_summary.api_cost_upper_nano_usd, 5_555_000);
    assert_eq!(good_summary.api_cost_complete_request_count, 1);
    assert_eq!(good_summary.api_cost_partial_request_count, 0);
    assert_eq!(good_summary.api_cost_unpriced_request_count, 0);
    let rate_limited_summary = accounts
        .iter()
        .find(|account| account.id == rate_limited_account)
        .expect("rate-limited account summary");
    assert_eq!(rate_limited_summary.api_cost_unpriced_request_count, 1);

    let summary = db::usage_summary(&pool).await?;
    assert_eq!(summary.account_count, 2);
    assert_eq!(summary.active_account_count, 2);
    assert_eq!(summary.request_count, 2);
    assert_eq!(summary.successful_request_count, 1);
    assert_eq!(summary.failed_request_count, 1);
    assert_eq!(summary.input_tokens, 1_200);
    assert_eq!(summary.output_tokens, 11);

    let compact: Value = client
        .post(format!("{}/v1/responses/compact", app_server.base_url))
        .bearer_auth(PROXY_TOKEN)
        .json(&json!({"model":"gpt-5.1-codex-mini","input":"compact me"}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(compact["kind"], "compact");

    let models: Value = client
        .get(format!(
            "{}/backend-api/codex/models?client_version=test-version",
            app_server.base_url
        ))
        .bearer_auth(PROXY_TOKEN)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    assert_eq!(models["models"][0]["slug"], "test-model");

    // Make the first WebSocket select the account that emits a retryable 429.
    // The bridge must cool it down and close without forwarding the terminal
    // event so Codex can reconnect and replay on the other account.
    sqlx::query("UPDATE account_runtime_state SET cooldown_until = NULL WHERE account_id = $1")
        .bind(rate_limited_account)
        .execute(&pool)
        .await?;
    let websocket_payload = json!({
        "type": "response.create",
        "model": "gpt-ws-test",
        "stream": true,
        "input": [],
        "client_metadata": {"turn_id": "integration-ws-turn"}
    })
    .to_string();

    let mut browser_websocket_request = proxy_websocket_request(&app_server.base_url)?;
    browser_websocket_request.headers_mut().insert(
        header::ORIGIN,
        header::HeaderValue::from_static("https://malicious.example"),
    );
    let browser_error = connect_async(browser_websocket_request)
        .await
        .expect_err("browser-origin WebSocket should be rejected");
    let ClientWebSocketError::Http(browser_response) = browser_error else {
        panic!("expected HTTP rejection for browser-origin WebSocket");
    };
    assert_eq!(browser_response.status(), StatusCode::UNAUTHORIZED);

    let (mut first_websocket, first_response) =
        connect_async(proxy_websocket_request(&app_server.base_url)?).await?;
    assert_eq!(first_response.status(), StatusCode::SWITCHING_PROTOCOLS);
    first_websocket
        .send(ClientWebSocketMessage::Text(
            websocket_payload.clone().into(),
        ))
        .await?;
    let leaked_terminal_error = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(message) = first_websocket.next().await {
            match message {
                Ok(ClientWebSocketMessage::Text(text)) => {
                    let event: Value = serde_json::from_str(text.as_str())?;
                    if event.get("type").and_then(Value::as_str) == Some("error") {
                        return Ok::<_, anyhow::Error>(true);
                    }
                }
                Ok(ClientWebSocketMessage::Close(_)) | Err(_) => break,
                _ => {}
            }
        }
        Ok(false)
    })
    .await??;
    assert!(
        !leaked_terminal_error,
        "retryable upstream error should become a reconnect, not a terminal client event"
    );

    // A connection is pinned for cache continuity, but each turn must re-check
    // account state. Pausing after the handshake should close before forwarding
    // the next response.create so Codex can reconnect to an eligible account.
    let (mut paused_websocket, paused_response) =
        connect_async(proxy_websocket_request(&app_server.base_url)?).await?;
    assert_eq!(paused_response.status(), StatusCode::SWITCHING_PROTOCOLS);
    db::update_account(
        &pool,
        good_account,
        Some("paused".to_string()),
        None,
        None,
        None,
    )
    .await?;
    paused_websocket
        .send(ClientWebSocketMessage::Text(
            websocket_payload.clone().into(),
        ))
        .await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(message) = paused_websocket.next().await {
            match message {
                Ok(ClientWebSocketMessage::Close(_)) | Err(_) => return Ok::<_, anyhow::Error>(()),
                _ => {}
            }
        }
        Ok(())
    })
    .await??;
    db::update_account(
        &pool,
        good_account,
        Some("active".to_string()),
        None,
        None,
        None,
    )
    .await?;

    let (mut websocket, websocket_response) =
        connect_async(proxy_websocket_request(&app_server.base_url)?).await?;
    assert_eq!(websocket_response.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(
        websocket_response
            .headers()
            .get("x-reasoning-included")
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );
    websocket
        .send(ClientWebSocketMessage::Text(websocket_payload.into()))
        .await?;
    let mut completed = false;
    while let Some(message) = websocket.next().await {
        let ClientWebSocketMessage::Text(text) = message? else {
            continue;
        };
        let event: Value = serde_json::from_str(text.as_str())?;
        if event.get("type").and_then(Value::as_str) == Some("response.completed") {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "local WebSocket did not forward response.completed"
    );
    websocket.close(None).await?;

    let websocket_log = db::list_request_logs(
        &pool,
        LogsQuery {
            limit: Some(1),
            offset: None,
        },
    )
    .await?
    .pop()
    .expect("WebSocket request log");
    assert_eq!(websocket_log.request_id, "integration-ws-turn");
    assert_eq!(websocket_log.account_id, Some(good_account));
    assert_eq!(websocket_log.model.as_deref(), Some("gpt-ws-test"));
    assert_eq!(websocket_log.status, "success");
    assert_eq!(websocket_log.input_tokens, Some(1_300));
    assert_eq!(websocket_log.output_tokens, Some(17));
    assert_eq!(websocket_log.cached_input_tokens, Some(500));
    assert_eq!(websocket_log.cache_write_input_tokens, Some(200));
    assert_eq!(websocket_log.reasoning_tokens, Some(7));
    assert_eq!(
        websocket_log.effective_model.as_deref(),
        Some("gpt-5.6-sol")
    );
    assert_eq!(
        websocket_log.effective_service_tier.as_deref(),
        Some("default")
    );
    assert_eq!(websocket_log.api_cost_status.as_deref(), Some("complete"));
    assert_eq!(websocket_log.api_cost_lower_nano_usd, Some(5_010_000));
    assert_eq!(websocket_log.api_cost_upper_nano_usd, Some(5_010_000));

    let session_hash = db::affinity_hash("session_id", "integration-session");
    let session_routes: Value = client
        .post(format!("{}/admin/session-routes", app_server.base_url))
        .bearer_auth(ADMIN_TOKEN)
        .json(&json!({"keyHashes": [session_hash]}))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let session_route = session_routes["sessionRoutes"]
        .as_array()
        .and_then(|routes| routes.first())
        .expect("resolved WebSocket session route");
    assert_eq!(session_route["keyHash"], session_hash);
    assert_eq!(session_route["accountId"], good_account.to_string());
    assert_eq!(session_routes["semantics"], "last_routed");
    assert!(!session_routes.to_string().contains("integration-session"));
    let raw_session_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM affinity WHERE key_hash = 'integration-session' OR key_hash LIKE '%integration-session%'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(raw_session_rows, 0);

    let invalid_session_hash = client
        .post(format!("{}/admin/session-routes", app_server.base_url))
        .bearer_auth(ADMIN_TOKEN)
        .json(&json!({"keyHashes": ["not-a-hash"]}))
        .send()
        .await?;
    assert_eq!(
        invalid_session_hash.status(),
        reqwest::StatusCode::BAD_REQUEST
    );

    let authorizations = fake_upstream.authorizations().await;
    assert_eq!(
        &authorizations[authorizations.len() - 3..],
        [
            "Bearer rate-limited-access",
            "Bearer good-access",
            "Bearer good-access"
        ]
    );

    let (mut restart_websocket, restart_response) =
        connect_async(proxy_websocket_request(&app_server.base_url)?).await?;
    assert_eq!(restart_response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let shutdown_started = std::time::Instant::now();
    state.signal_shutdown();
    let restart_close = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(message) = restart_websocket.next().await {
            if let ClientWebSocketMessage::Close(frame) = message? {
                return Ok::<_, anyhow::Error>(frame);
            }
        }
        anyhow::bail!("WebSocket ended without a service-restart close frame")
    })
    .await??
    .expect("service-restart close frame");
    assert_eq!(u16::from(restart_close.code), 1012);
    assert_eq!(restart_close.reason, "service restart");
    assert!(shutdown_started.elapsed() < Duration::from_secs(2));

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let inflight: i64 = sqlx::query_scalar(
                "SELECT COALESCE(SUM(inflight_count), 0) FROM account_runtime_state",
            )
            .fetch_one(&pool)
            .await
            .expect("inflight query");
            if inflight == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;

    drop(app_server);
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn migration_backfills_historical_cost_as_cache_write_range() -> Result<()> {
    let storage = TestStorage::new();
    let database_url = storage.database_url();
    let pool = db::connect(&database_url).await?;
    migrate_before_api_cost(&pool).await?;
    let crypto = TokenCrypto::load_or_create(&storage.key_path).await?;
    let account_id = insert_account(
        &pool,
        &crypto,
        "historical-access",
        "acct-historical",
        "historical@example.com",
    )
    .await?;

    sqlx::query(
        r#"
        INSERT INTO request_logs (
            request_id, account_id, model, status, input_tokens, output_tokens,
            cached_input_tokens, reasoning_tokens
        ) VALUES ('historical-request', $1, 'gpt-5.6-sol', 'success', 1200, 11, 200, 3)
        "#,
    )
    .bind(account_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"
        UPDATE account_runtime_state
        SET request_count = 1, successful_request_count = 1,
            input_tokens = 1200, output_tokens = 11
        WHERE account_id = $1
        "#,
    )
    .bind(account_id)
    .execute(&pool)
    .await?;

    db::run_migrations(&pool).await?;
    let pending_account = db::list_accounts(&pool)
        .await?
        .into_iter()
        .find(|account| account.id == account_id)
        .expect("pending historical account summary");
    assert_eq!(pending_account.api_cost_complete_request_count, 0);
    assert_eq!(pending_account.api_cost_partial_request_count, 0);
    assert_eq!(pending_account.api_cost_unpriced_request_count, 1);

    let first_batch = db::backfill_api_costs_batch(&pool, 1).await?;
    assert_eq!(first_batch.selected, 1);
    assert_eq!(first_batch.updated, 1);
    let finished_batch = db::backfill_api_costs_batch(&pool, 1).await?;
    assert_eq!(finished_batch.selected, 0);
    assert_eq!(finished_batch.updated, 0);
    assert_eq!(db::backfill_api_costs(&pool).await?, 0);

    let log = db::list_request_logs(
        &pool,
        LogsQuery {
            limit: Some(1),
            offset: None,
        },
    )
    .await?
    .pop()
    .expect("backfilled request log");
    assert_eq!(log.api_cost_status.as_deref(), Some("missing_cache_write"));
    assert_eq!(log.api_cost_lower_nano_usd, Some(5_430_000));
    assert_eq!(log.api_cost_upper_nano_usd, Some(6_680_000));

    let account = db::list_accounts(&pool)
        .await?
        .into_iter()
        .find(|account| account.id == account_id)
        .expect("historical account summary");
    assert_eq!(account.cached_input_tokens, 200);
    assert_eq!(account.observed_cache_write_input_tokens, 0);
    assert_eq!(account.api_cost_lower_nano_usd, 5_430_000);
    assert_eq!(account.api_cost_upper_nano_usd, 6_680_000);
    assert_eq!(account.api_cost_complete_request_count, 0);
    assert_eq!(account.api_cost_partial_request_count, 1);
    assert_eq!(account.api_cost_unpriced_request_count, 0);

    Ok(())
}

#[tokio::test]
async fn usage_weighted_balances_quota_pressure_and_inflight_load() -> Result<()> {
    let storage = TestStorage::new();
    let pool = db::connect(&storage.database_url()).await?;
    db::run_migrations(&pool).await?;
    let crypto = TokenCrypto::load_or_create(&storage.key_path).await?;
    let work = insert_account(
        &pool,
        &crypto,
        "routing-work-access",
        "acct-routing-work",
        "routing-work@example.invalid",
    )
    .await?;
    let personal = insert_account(
        &pool,
        &crypto,
        "routing-personal-access",
        "acct-routing-personal",
        "routing-personal@example.invalid",
    )
    .await?;
    let now = Utc::now();
    insert_core_window(
        &pool,
        work,
        "primary",
        59.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(500)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    insert_core_window(
        &pool,
        personal,
        "primary",
        8.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(500)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    set_inflight(&pool, work, 2).await?;
    set_inflight(&pool, personal, 8).await?;

    let settings = db::runtime_settings(&pool).await?;
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, personal,
        "a 51-point quota advantage should beat six additional in-flight requests"
    );

    set_inflight(&pool, personal, 30).await?;
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, work,
        "a sufficiently large concurrency gap should still protect an overloaded account"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn usage_weighted_uses_reset_timing_and_the_tightest_core_window() -> Result<()> {
    let storage = TestStorage::new();
    let pool = db::connect(&storage.database_url()).await?;
    db::run_migrations(&pool).await?;
    let crypto = TokenCrypto::load_or_create(&storage.key_path).await?;
    let early_reset = insert_account(
        &pool,
        &crypto,
        "early-reset-access",
        "acct-early-reset",
        "early-reset@example.invalid",
    )
    .await?;
    let late_reset = insert_account(
        &pool,
        &crypto,
        "late-reset-access",
        "acct-late-reset",
        "late-reset@example.invalid",
    )
    .await?;
    let now = Utc::now();
    insert_core_window(
        &pool,
        early_reset,
        "primary",
        60.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(100)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    insert_core_window(
        &pool,
        late_reset,
        "primary",
        20.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(800)),
        now - ChronoDuration::seconds(60),
    )
    .await?;

    let settings = db::runtime_settings(&pool).await?;
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, early_reset,
        "the account closer to reset has more sustainable capacity despite higher raw usage"
    );

    sqlx::query("DELETE FROM usage_windows")
        .execute(&pool)
        .await?;
    let now = Utc::now();
    for (account_id, slot, used_percent) in [
        (early_reset, "primary", 30.0),
        (early_reset, "secondary", 65.0),
        (late_reset, "primary", 60.0),
    ] {
        insert_core_window(
            &pool,
            account_id,
            slot,
            used_percent,
            Some(1_000),
            Some(now + ChronoDuration::seconds(500)),
            now - ChronoDuration::seconds(60),
        )
        .await?;
    }
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, late_reset,
        "the most pressured core window must control each account's score"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn usage_weighted_treats_unknown_stale_and_reset_telemetry_conservatively() -> Result<()> {
    let storage = TestStorage::new();
    let pool = db::connect(&storage.database_url()).await?;
    db::run_migrations(&pool).await?;
    let crypto = TokenCrypto::load_or_create(&storage.key_path).await?;
    let uncertain = insert_account(
        &pool,
        &crypto,
        "uncertain-access",
        "acct-uncertain",
        "uncertain@example.invalid",
    )
    .await?;
    let known = insert_account(
        &pool,
        &crypto,
        "known-access",
        "acct-known",
        "known@example.invalid",
    )
    .await?;
    let settings = db::runtime_settings(&pool).await?;
    let now = Utc::now();
    insert_core_window(
        &pool,
        known,
        "primary",
        95.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(500)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, known,
        "missing telemetry must not look like zero usage"
    );

    insert_core_window(
        &pool,
        uncertain,
        "primary",
        0.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(500)),
        now - ChronoDuration::seconds(400),
    )
    .await?;
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, known,
        "stale telemetry must receive an unknown penalty"
    );

    sqlx::query("DELETE FROM usage_windows")
        .execute(&pool)
        .await?;
    let now = Utc::now();
    insert_core_window(
        &pool,
        uncertain,
        "primary",
        20.0,
        None,
        None,
        now - ChronoDuration::seconds(60),
    )
    .await?;
    insert_core_window(
        &pool,
        known,
        "primary",
        75.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(500)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, uncertain,
        "fresh usage without timing should fall back to raw percentage"
    );

    sqlx::query("DELETE FROM usage_windows")
        .execute(&pool)
        .await?;
    let now = Utc::now();
    insert_core_window(
        &pool,
        uncertain,
        "primary",
        100.0,
        Some(1_000),
        Some(now - ChronoDuration::seconds(60)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    insert_core_window(
        &pool,
        known,
        "primary",
        100.0,
        Some(1_000),
        Some(now - ChronoDuration::seconds(400)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, uncertain,
        "a just-reset quota should be usable while an old expired sample stays unknown"
    );

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn routing_preserves_stickiness_eligibility_failover_and_round_robin() -> Result<()> {
    let storage = TestStorage::new();
    let pool = db::connect(&storage.database_url()).await?;
    db::run_migrations(&pool).await?;
    let crypto = TokenCrypto::load_or_create(&storage.key_path).await?;
    let pinned = insert_account(
        &pool,
        &crypto,
        "pinned-access",
        "acct-pinned",
        "pinned@example.invalid",
    )
    .await?;
    let healthy = insert_account(
        &pool,
        &crypto,
        "healthy-access",
        "acct-healthy",
        "healthy@example.invalid",
    )
    .await?;
    let settings = db::runtime_settings(&pool).await?;
    let now = Utc::now();
    insert_core_window(
        &pool,
        pinned,
        "primary",
        90.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(500)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    insert_core_window(
        &pool,
        healthy,
        "primary",
        10.0,
        Some(1_000),
        Some(now + ChronoDuration::seconds(500)),
        now - ChronoDuration::seconds(60),
    )
    .await?;
    let affinity_hash = db::affinity_hash("session_id", "routing-test-session");
    db::bind_affinity(&pool, &affinity_hash, "session_id", pinned).await?;
    let affinity = Some((affinity_hash.as_str(), "session_id"));
    let selected = select_for_test(&pool, &crypto, &settings, affinity, &HashSet::new()).await?;
    assert_eq!(
        selected, pinned,
        "eligible stateful sessions must remain sticky"
    );

    db::cooldown_account(&pool, pinned, 60, "routing test cooldown").await?;
    let selected = select_for_test(&pool, &crypto, &settings, affinity, &HashSet::new()).await?;
    assert_eq!(selected, healthy, "cooldown must override affinity");
    let cooling = db::list_accounts(&pool)
        .await?
        .into_iter()
        .find(|account| account.id == pinned)
        .expect("pinned account summary");
    assert!(
        cooling.cooldown_until.is_some(),
        "sticky lookup must not clear cooldown"
    );

    sqlx::query(
        "UPDATE account_runtime_state SET cooldown_until = NULL, cooldown_reason = NULL WHERE account_id = $1",
    )
    .bind(pinned)
    .execute(&pool)
    .await?;
    sqlx::query("UPDATE usage_windows SET used_percent = 100, reset_at = 'not-a-date' WHERE account_id = $1")
        .bind(pinned)
        .execute(&pool)
        .await?;
    let selected = select_for_test(&pool, &crypto, &settings, None, &HashSet::new()).await?;
    assert_eq!(
        selected, healthy,
        "100% usage with an unknown reset must stay ineligible"
    );

    sqlx::query("UPDATE usage_windows SET used_percent = 90, reset_at = $2 WHERE account_id = $1")
        .bind(pinned)
        .bind(Utc::now() + ChronoDuration::seconds(500))
        .execute(&pool)
        .await?;
    let mut excluded = HashSet::new();
    excluded.insert(healthy);
    let selected = select_for_test(&pool, &crypto, &settings, None, &excluded).await?;
    assert_eq!(selected, pinned, "the retry exclusion set must be honored");

    set_inflight(&pool, pinned, 50).await?;
    set_inflight(&pool, healthy, 0).await?;
    sqlx::query(
        "UPDATE account_runtime_state SET last_selected_at = CASE WHEN account_id = $1 THEN '2020-01-01T00:00:00Z' ELSE '2021-01-01T00:00:00Z' END",
    )
    .bind(pinned)
    .execute(&pool)
    .await?;
    let mut round_robin = settings.clone();
    round_robin.routing_strategy = "round_robin".to_string();
    let selected = select_for_test(&pool, &crypto, &round_robin, None, &HashSet::new()).await?;
    assert_eq!(
        selected, pinned,
        "round-robin must continue to ignore quota and in-flight load"
    );

    pool.close().await;
    Ok(())
}

async fn migrate_before_api_cost(pool: &SqlitePool) -> Result<()> {
    let migration_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations");
    let migrator = sqlx::migrate::Migrator::new(migration_dir.as_path()).await?;
    let mut connection = pool.acquire().await?;
    connection.ensure_migrations_table().await?;
    for migration in migrator
        .iter()
        .filter(|migration| migration.version < 202607220002)
    {
        connection.apply(migration).await?;
    }
    Ok(())
}

async fn insert_account(
    pool: &SqlitePool,
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
    let account = db::upsert_account(pool, crypto, auth, claims, None).await?;
    Ok(account.id)
}

async fn insert_core_window(
    pool: &SqlitePool,
    account_id: Uuid,
    source_slot: &str,
    used_percent: f64,
    window_seconds: Option<i64>,
    reset_at: Option<chrono::DateTime<Utc>>,
    fetched_at: chrono::DateTime<Utc>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO usage_windows (
            account_id, quota_key, quota_name, source_slot, window_kind,
            used_percent, window_seconds, reset_at, fetched_at
        ) VALUES ($1, 'codex', 'Codex', $2, $2, $3, $4, $5, $6)
        ON CONFLICT (account_id, quota_key, source_slot) DO UPDATE SET
            used_percent = EXCLUDED.used_percent,
            window_seconds = EXCLUDED.window_seconds,
            reset_at = EXCLUDED.reset_at,
            fetched_at = EXCLUDED.fetched_at
        "#,
    )
    .bind(account_id)
    .bind(source_slot)
    .bind(used_percent)
    .bind(window_seconds)
    .bind(reset_at)
    .bind(fetched_at)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_inflight(pool: &SqlitePool, account_id: Uuid, inflight: i64) -> Result<()> {
    sqlx::query("UPDATE account_runtime_state SET inflight_count = $2 WHERE account_id = $1")
        .bind(account_id)
        .bind(inflight)
        .execute(pool)
        .await?;
    Ok(())
}

async fn select_for_test(
    pool: &SqlitePool,
    crypto: &TokenCrypto,
    settings: &codex_lb_rs::models::RuntimeSettings,
    affinity: Option<(&str, &str)>,
    excluded: &HashSet<Uuid>,
) -> Result<Uuid> {
    let selected = db::select_account_for_request(
        pool,
        crypto,
        affinity,
        excluded,
        settings,
        Duration::from_secs(120),
    )
    .await?;
    let account_id = selected.account.id;
    db::release_account(pool, account_id).await?;
    Ok(account_id)
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
        usage_refresh_interval: Duration::from_secs(120),
    }
}

struct TestStorage {
    database_path: PathBuf,
    key_path: PathBuf,
}

impl TestStorage {
    fn new() -> Self {
        let id = Uuid::new_v4();
        let temp = std::env::temp_dir();
        Self {
            database_path: temp.join(format!("codex-lb-rs-{id}.sqlite")),
            key_path: temp.join(format!("codex-lb-rs-{id}.key")),
        }
    }

    fn database_url(&self) -> String {
        format!("sqlite://{}", self.database_path.display())
    }
}

impl Drop for TestStorage {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.database_path);
        let _ = std::fs::remove_file(self.database_path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(self.database_path.with_extension("sqlite-shm"));
        let _ = std::fs::remove_file(&self.key_path);
    }
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
            .route(
                "/backend-api/codex/responses",
                get(fake_responses_websocket).post(fake_responses),
            )
            .route("/backend-api/codex/responses/compact", post(fake_compact))
            .route("/backend-api/codex/models", get(fake_models))
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
            "data: {\"response\":{\"model\":\"gpt-5.6-sol\",\"service_tier\":\"default\",\"usage\":{\"input_tokens\":1200,\"output_tokens\":11,\"input_tokens_details\":{\"cached_tokens\":200,\"cache_write_tokens\":100},\"output_tokens_details\":{\"reasoning_tokens\":3}}}}\n\ndata: [DONE]\n\n",
        )
            .into_response(),
        _ => (StatusCode::UNAUTHORIZED, "unexpected token").into_response(),
    }
}

async fn fake_responses_websocket(
    State(state): State<Arc<FakeUpstreamState>>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Response {
    let authorization = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let rate_limited = authorization == "Bearer rate-limited-access";
    state.authorizations.lock().await.push(authorization);

    let mut response = upgrade.on_upgrade(move |mut socket| async move {
        while let Some(message) = socket.recv().await {
            let Ok(AxumWebSocketMessage::Text(text)) = message else {
                break;
            };
            let Ok(request) = serde_json::from_str::<Value>(text.as_str()) else {
                break;
            };
            if request.get("type").and_then(Value::as_str) != Some("response.create") {
                continue;
            }
            if rate_limited {
                let event = json!({
                    "type": "error",
                    "status": 429,
                    "error": {
                        "type": "rate_limit_error",
                        "code": "rate_limit_exceeded",
                        "message": "fake WebSocket rate limit"
                    }
                });
                let _ = socket
                    .send(AxumWebSocketMessage::Text(event.to_string().into()))
                    .await;
                break;
            }
            let created = json!({
                "type": "response.created",
                "response": {"id": "resp-ws-test"}
            });
            let completed = json!({
                "type": "response.completed",
                "response": {
                    "id": "resp-ws-test",
                    "model": "gpt-5.6-sol",
                    "service_tier": "default",
                    "usage": {
                        "input_tokens": 1300,
                        "output_tokens": 17,
                        "input_tokens_details": {"cached_tokens": 500, "cache_write_tokens": 200},
                        "output_tokens_details": {"reasoning_tokens": 7}
                    }
                }
            });
            if socket
                .send(AxumWebSocketMessage::Text(created.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
            if socket
                .send(AxumWebSocketMessage::Text(completed.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    response.headers_mut().insert(
        "x-reasoning-included",
        header::HeaderValue::from_static("true"),
    );
    response
}

fn proxy_websocket_request(
    base_url: &str,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>> {
    let mut request = format!(
        "{}/backend-api/codex/responses",
        base_url.replacen("http://", "ws://", 1)
    )
    .into_client_request()?;
    request.headers_mut().insert(
        header::AUTHORIZATION,
        header::HeaderValue::from_static("Bearer test-proxy-token"),
    );
    request.headers_mut().insert(
        "session-id",
        header::HeaderValue::from_static("integration-session"),
    );
    request.headers_mut().insert(
        "openai-beta",
        header::HeaderValue::from_static("responses_websockets=2026-02-06"),
    );
    Ok(request)
}

async fn fake_compact(headers: HeaderMap) -> Response {
    let authorized = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some("Bearer good-access");
    let accepts_json = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        == Some("application/json");
    if !authorized || !accepts_json {
        return (StatusCode::BAD_REQUEST, "bad compact request").into_response();
    }
    axum::Json(json!({"kind":"compact"})).into_response()
}

async fn fake_models(axum::extract::RawQuery(query): axum::extract::RawQuery) -> Response {
    if query.as_deref() != Some("client_version=test-version") {
        return (StatusCode::BAD_REQUEST, "missing query").into_response();
    }
    axum::Json(json!({
        "models": [{"slug":"test-model", "display_name":"Test Model"}]
    }))
    .into_response()
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
