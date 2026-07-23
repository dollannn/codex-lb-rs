use std::{
    collections::{HashMap, HashSet},
    env,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{
    QueryBuilder, Sqlite, SqlitePool, Transaction,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use uuid::Uuid;

use crate::{
    auth_file::{AuthClaims, AuthFile},
    crypto::TokenCrypto,
    error::{AppError, AppResult},
    models::{
        Account, AccountSummary, AccountTokens, LogsQuery, NewRequestLog, RequestLog,
        ResolvedSessionRouteTarget, RuntimeSettings, SelectedAccount, SelectionReason,
        SessionRoute, SessionRouteActionKind, SessionRouteActionRequest,
        SessionRouteActionResponse, SessionRouteActionStatus, SessionRouteContext,
        SessionRouteEpoch, SessionRouteKey, SettingRow, SmartAccountPreview, UsageData,
        UsageSample, UsageSnapshot, UsageSummary, UsageWindow,
    },
    pricing::{self, ApiCostStatus, PRICING_VERSION},
    usage::ParsedUsageWindow,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
pub const API_COST_BACKFILL_BATCH_SIZE: i64 = 500;
const MIN_ROUTING_USAGE_FRESHNESS_SECONDS: u64 = 300;
const ROUTING_USAGE_FRESHNESS_INTERVALS: u64 = 3;
const ROUTING_CLOCK_SKEW_SECONDS: f64 = 300.0;
const ROUTING_UNKNOWN_PRESSURE: f64 = 100.0;
const ROUTING_INFLIGHT_PENALTY: f64 = 2.0;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApiCostBackfillBatch {
    pub selected: u64,
    pub updated: u64,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct SessionRouteKeyRow {
    key_hash: String,
    root_key_hash: String,
    kind: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct AffinityRouteActionRow {
    key_hash: String,
    kind: String,
    account_id: Uuid,
    account_label: String,
    route_generation: i64,
}

pub async fn connect(database_url: &str) -> AppResult<SqlitePool> {
    let database_url = expand_home_in_sqlite_url(database_url)?;
    let mut options = SqliteConnectOptions::from_str(&database_url)?
        .create_if_missing(true)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5))
        .synchronous(SqliteSynchronous::Normal);

    let filename = options.get_filename().to_path_buf();
    if filename != Path::new(":memory:") {
        if let Some(parent) = filename
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
        {
            let parent_existed = tokio::fs::try_exists(parent).await.map_err(|err| {
                AppError::Internal(format!(
                    "failed to inspect SQLite directory {}: {err}",
                    parent.display()
                ))
            })?;
            tokio::fs::create_dir_all(parent).await.map_err(|err| {
                AppError::Internal(format!(
                    "failed to create SQLite directory {}: {err}",
                    parent.display()
                ))
            })?;
            #[cfg(unix)]
            if !parent_existed {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
                    .await
                    .map_err(|err| {
                        AppError::Internal(format!(
                            "failed to secure SQLite directory {}: {err}",
                            parent.display()
                        ))
                    })?;
            }
        }
        options = options.journal_mode(SqliteJournalMode::Wal);
    }

    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options)
        .await
        .map_err(AppError::Database)?;
    #[cfg(unix)]
    if filename != Path::new(":memory:") {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(&filename, std::fs::Permissions::from_mode(0o600))
            .await
            .map_err(|err| {
                AppError::Internal(format!(
                    "failed to secure SQLite database {}: {err}",
                    filename.display()
                ))
            })?;
    }
    Ok(pool)
}

pub async fn run_migrations(pool: &SqlitePool) -> AppResult<()> {
    MIGRATOR
        .run(pool)
        .await
        .map_err(|err| AppError::Internal(format!("migration failed: {err}")))
}

#[derive(sqlx::FromRow)]
struct PendingApiCostRow {
    id: i64,
    account_id: Option<Uuid>,
    model: Option<String>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    cached_input_tokens: Option<i64>,
    cache_write_input_tokens: Option<i64>,
    reasoning_tokens: Option<i64>,
    effective_model: Option<String>,
    effective_service_tier: Option<String>,
}

#[derive(Default)]
struct ApiCostAggregateDelta {
    cached_input_tokens: i64,
    observed_cache_write_input_tokens: i64,
    lower_nano_usd: i64,
    upper_nano_usd: i64,
    complete_requests: i64,
    partial_requests: i64,
}

impl ApiCostAggregateDelta {
    fn add(
        &mut self,
        row: &PendingApiCostRow,
        estimate: pricing::ApiCostEstimate,
    ) -> AppResult<()> {
        checked_accumulate(
            &mut self.cached_input_tokens,
            row.cached_input_tokens.unwrap_or(0).max(0),
        )?;
        checked_accumulate(
            &mut self.observed_cache_write_input_tokens,
            row.cache_write_input_tokens.unwrap_or(0).max(0),
        )?;
        checked_accumulate(
            &mut self.lower_nano_usd,
            estimate.lower_nano_usd.unwrap_or(0),
        )?;
        checked_accumulate(
            &mut self.upper_nano_usd,
            estimate.upper_nano_usd.unwrap_or(0),
        )?;
        checked_accumulate(
            &mut self.complete_requests,
            i64::from(estimate.status == ApiCostStatus::Complete),
        )?;
        checked_accumulate(
            &mut self.partial_requests,
            i64::from(estimate.status == ApiCostStatus::MissingCacheWrite),
        )
    }
}

fn checked_accumulate(total: &mut i64, value: i64) -> AppResult<()> {
    *total = total
        .checked_add(value)
        .ok_or_else(|| AppError::Internal("API cost backfill aggregate overflowed".to_string()))?;
    Ok(())
}

/// Fill every pending historical estimate in bounded, restart-safe transactions.
///
/// The server uses `backfill_api_costs_batch` from a background task so it can bind
/// immediately. The explicit migration command uses this helper to finish all work.
pub async fn backfill_api_costs(pool: &SqlitePool) -> AppResult<u64> {
    let mut total = 0_u64;
    loop {
        let batch = backfill_api_costs_batch(pool, API_COST_BACKFILL_BATCH_SIZE).await?;
        if batch.selected == 0 {
            return Ok(total);
        }
        total = total
            .checked_add(batch.updated)
            .ok_or_else(|| AppError::Internal("API cost backfill count overflowed".to_string()))?;
        tokio::task::yield_now().await;
    }
}

pub async fn backfill_api_costs_batch(
    pool: &SqlitePool,
    batch_size: i64,
) -> AppResult<ApiCostBackfillBatch> {
    let rows = sqlx::query_as::<_, PendingApiCostRow>(
        r#"
        SELECT
            id, account_id, model, input_tokens, output_tokens, cached_input_tokens,
            cache_write_input_tokens, reasoning_tokens, effective_model,
            effective_service_tier
        FROM request_logs
        WHERE api_cost_status IS NULL
        ORDER BY id
        LIMIT $1
        "#,
    )
    .bind(batch_size.clamp(1, API_COST_BACKFILL_BATCH_SIZE))
    .fetch_all(pool)
    .await?;
    if rows.is_empty() {
        return Ok(ApiCostBackfillBatch::default());
    }
    // Return the selected count rather than the affected count. A concurrent pruner or
    // second migrator may win every guarded update in this batch; the caller must still
    // continue until a fresh selection is empty.
    let selected = rows.len() as u64;

    let mut tx = pool.begin().await?;
    let mut aggregate_deltas = HashMap::<Uuid, ApiCostAggregateDelta>::new();
    let mut updated = 0_u64;
    for row in &rows {
        let usage = UsageData {
            input_tokens: row.input_tokens,
            output_tokens: row.output_tokens,
            cached_input_tokens: row.cached_input_tokens,
            cache_write_input_tokens: row.cache_write_input_tokens,
            reasoning_tokens: row.reasoning_tokens,
            effective_model: row.effective_model.clone(),
            effective_service_tier: row.effective_service_tier.clone(),
        };
        let model = usage.effective_model.as_deref().or(row.model.as_deref());
        let estimate = pricing::estimate_standard_api_cost(model, &usage);
        let result = sqlx::query(
            r#"
            UPDATE request_logs
            SET api_pricing_version = $2,
                api_cost_status = $3,
                api_cost_lower_nano_usd = $4,
                api_cost_upper_nano_usd = $5
            WHERE id = $1 AND api_cost_status IS NULL
            "#,
        )
        .bind(row.id)
        .bind(PRICING_VERSION)
        .bind(estimate.status.as_str())
        .bind(estimate.lower_nano_usd)
        .bind(estimate.upper_nano_usd)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            continue;
        }
        updated += 1;
        if let Some(account_id) = row.account_id {
            aggregate_deltas
                .entry(account_id)
                .or_default()
                .add(row, estimate)?;
        }
    }

    for (account_id, delta) in aggregate_deltas {
        sqlx::query(
            r#"
            UPDATE account_runtime_state
            SET cached_input_tokens = cached_input_tokens + $2,
                observed_cache_write_input_tokens = observed_cache_write_input_tokens + $3,
                api_cost_lower_nano_usd = api_cost_lower_nano_usd + $4,
                api_cost_upper_nano_usd = api_cost_upper_nano_usd + $5,
                api_cost_complete_request_count = api_cost_complete_request_count + $6,
                api_cost_partial_request_count = api_cost_partial_request_count + $7,
                api_cost_unpriced_request_count = MAX(
                    0,
                    request_count
                        - (api_cost_complete_request_count + $6)
                        - (api_cost_partial_request_count + $7)
                )
            WHERE account_id = $1
            "#,
        )
        .bind(account_id)
        .bind(delta.cached_input_tokens)
        .bind(delta.observed_cache_write_input_tokens)
        .bind(delta.lower_nano_usd)
        .bind(delta.upper_nano_usd)
        .bind(delta.complete_requests)
        .bind(delta.partial_requests)
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(ApiCostBackfillBatch { selected, updated })
}

pub async fn upsert_account(
    pool: &SqlitePool,
    crypto: &TokenCrypto,
    auth: AuthFile,
    claims: AuthClaims,
    label: Option<String>,
) -> AppResult<Account> {
    let label = label.map(|value| normalize_label(&value)).transpose()?;
    let encrypted_access_token = crypto.encrypt(&auth.tokens.access_token)?;
    let encrypted_refresh_token = crypto.encrypt(&auth.tokens.refresh_token)?;
    let encrypted_id_token = crypto.encrypt(&auth.tokens.id_token)?;
    let account = sqlx::query_as::<_, Account>(
        r#"
        INSERT INTO accounts (
            id, chatgpt_account_id, label, email, plan_type,
            encrypted_access_token, encrypted_refresh_token, encrypted_id_token,
            last_refresh_at, access_token_expires_at, status, status_reason
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, COALESCE($9, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), $10, 'active', NULL)
        ON CONFLICT (chatgpt_account_id) WHERE chatgpt_account_id IS NOT NULL DO UPDATE SET
            label = CASE WHEN EXCLUDED.label = '' THEN accounts.label ELSE EXCLUDED.label END,
            email = EXCLUDED.email,
            plan_type = EXCLUDED.plan_type,
            encrypted_access_token = EXCLUDED.encrypted_access_token,
            encrypted_refresh_token = EXCLUDED.encrypted_refresh_token,
            encrypted_id_token = EXCLUDED.encrypted_id_token,
            last_refresh_at = EXCLUDED.last_refresh_at,
            access_token_expires_at = EXCLUDED.access_token_expires_at,
            status = 'active',
            status_reason = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        RETURNING *
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(claims.chatgpt_account_id)
    .bind(label.unwrap_or_default())
    .bind(claims.email)
    .bind(claims.plan_type)
    .bind(encrypted_access_token)
    .bind(encrypted_refresh_token)
    .bind(encrypted_id_token)
    .bind(auth.last_refresh_at)
    .bind(claims.access_token_expires_at)
    .fetch_one(pool)
    .await?;

    ensure_runtime_row(pool, account.id).await?;
    Ok(account)
}

pub async fn list_accounts(pool: &SqlitePool) -> AppResult<Vec<AccountSummary>> {
    sqlx::query_as::<_, AccountSummary>(
        r#"
        SELECT
            a.id,
            a.chatgpt_account_id,
            a.label,
            a.email,
            a.plan_type,
            a.status,
            a.status_reason,
            a.last_refresh_at,
            a.access_token_expires_at,
            a.last_usage_refresh_at,
            a.last_usage_error,
            a.created_at,
            (SELECT MAX(used_percent) FROM usage_windows WHERE account_id = a.id AND quota_key = 'codex') AS latest_used_percent,
            (SELECT MIN(reset_at) FROM usage_windows WHERE account_id = a.id AND quota_key = 'codex') AS latest_reset_at,
            COALESCE(r.request_count, 0) AS request_count,
            COALESCE(r.input_tokens, 0) AS input_tokens,
            COALESCE(r.cached_input_tokens, 0) AS cached_input_tokens,
            COALESCE(r.observed_cache_write_input_tokens, 0) AS observed_cache_write_input_tokens,
            COALESCE(r.output_tokens, 0) AS output_tokens,
            COALESCE(r.api_cost_lower_nano_usd, 0) AS api_cost_lower_nano_usd,
            COALESCE(r.api_cost_upper_nano_usd, 0) AS api_cost_upper_nano_usd,
            COALESCE(r.api_cost_complete_request_count, 0) AS api_cost_complete_request_count,
            COALESCE(r.api_cost_partial_request_count, 0) AS api_cost_partial_request_count,
            COALESCE(r.api_cost_unpriced_request_count, 0) AS api_cost_unpriced_request_count,
            r.last_selected_at,
            r.last_request_at,
            r.cooldown_until,
            r.cooldown_reason,
            COALESCE(r.inflight_count, 0) AS inflight_count
        FROM accounts a
        LEFT JOIN account_runtime_state r ON r.account_id = a.id
        ORDER BY a.created_at ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn get_account(pool: &SqlitePool, id: Uuid) -> AppResult<Option<Account>> {
    sqlx::query_as::<_, Account>("SELECT * FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(AppError::Database)
}

async fn account_display_label(pool: &SqlitePool, account_id: Uuid) -> AppResult<String> {
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT CASE
            WHEN label = '' THEN 'unlabeled'
            ELSE label
        END
        FROM accounts
        WHERE id = $1
        "#,
    )
    .bind(account_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound("account target no longer exists".to_string()))
}

/// Resolve an exact UUID or normalized account label without considering the
/// account's current routing eligibility.
pub async fn resolve_session_route_target(
    pool: &SqlitePool,
    target: &str,
) -> AppResult<ResolvedSessionRouteTarget> {
    let target = target.trim();
    if target.is_empty() {
        return Err(AppError::BadRequest(
            "reroute target must not be empty".to_string(),
        ));
    }
    if let Ok(account_id) = Uuid::parse_str(target) {
        let account_label = account_display_label(pool, account_id)
            .await
            .map_err(|error| {
                if matches!(error, AppError::NotFound(_)) {
                    AppError::NotFound("account target not found".to_string())
                } else {
                    error
                }
            })?;
        return Ok(ResolvedSessionRouteTarget {
            account_id,
            account_label,
        });
    }

    let normalized = normalize_label(target)?;
    let matches = sqlx::query_as::<_, (Uuid, String)>(
        r#"
        SELECT
            id,
            CASE
                WHEN label = '' THEN 'unlabeled'
                ELSE label
            END AS account_label
        FROM accounts
        WHERE lower(trim(label)) = $1
        ORDER BY created_at, id
        LIMIT 2
        "#,
    )
    .bind(normalized)
    .fetch_all(pool)
    .await?;
    match matches.as_slice() {
        [] => Err(AppError::NotFound("account target not found".to_string())),
        [(account_id, account_label)] => Ok(ResolvedSessionRouteTarget {
            account_id: *account_id,
            account_label: account_label.clone(),
        }),
        _ => Err(AppError::BadRequest(
            "account target label is ambiguous".to_string(),
        )),
    }
}

pub async fn update_account(
    pool: &SqlitePool,
    id: Uuid,
    status: Option<String>,
    label: Option<String>,
    email: Option<String>,
    plan_type: Option<String>,
) -> AppResult<Account> {
    validate_status_opt(status.as_deref())?;
    let label = label.map(|value| normalize_label(&value)).transpose()?;
    sqlx::query_as::<_, Account>(
        r#"
        UPDATE accounts SET
            status = COALESCE($2, status),
            label = COALESCE($3, label),
            email = COALESCE($4, email),
            plan_type = COALESCE($5, plan_type),
            status_reason = CASE WHEN $2 = 'active' THEN NULL ELSE status_reason END,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(status)
    .bind(label)
    .bind(email)
    .bind(plan_type)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("account {id} not found")))
}

pub async fn delete_account(pool: &SqlitePool, id: Uuid) -> AppResult<()> {
    let result = sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::NotFound(format!("account {id} not found")));
    }
    Ok(())
}

pub async fn update_account_tokens(
    pool: &SqlitePool,
    crypto: &TokenCrypto,
    id: Uuid,
    auth: &AuthFile,
    claims: AuthClaims,
) -> AppResult<Account> {
    sqlx::query_as::<_, Account>(
        r#"
        UPDATE accounts SET
            encrypted_access_token = $2,
            encrypted_refresh_token = $3,
            encrypted_id_token = $4,
            chatgpt_account_id = COALESCE($5, chatgpt_account_id),
            email = COALESCE($6, email),
            plan_type = COALESCE($7, plan_type),
            access_token_expires_at = COALESCE($8, access_token_expires_at),
            last_refresh_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            status = 'active',
            status_reason = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(crypto.encrypt(&auth.tokens.access_token)?)
    .bind(crypto.encrypt(&auth.tokens.refresh_token)?)
    .bind(crypto.encrypt(&auth.tokens.id_token)?)
    .bind(claims.chatgpt_account_id)
    .bind(Some(claims.email))
    .bind(Some(claims.plan_type))
    .bind(claims.access_token_expires_at)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("account {id} not found")))
}

pub async fn select_account_for_request(
    pool: &SqlitePool,
    crypto: &TokenCrypto,
    affinity: Option<(&str, &str)>,
    excluded: &HashSet<Uuid>,
    settings: &RuntimeSettings,
    usage_refresh_interval: Duration,
) -> AppResult<SelectedAccount> {
    let selected_at = Utc::now().format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string();
    if let Some((key_hash, _)) = affinity {
        let pinned = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT af.account_id
            FROM affinity af
            INNER JOIN accounts a ON a.id = af.account_id
            INNER JOIN account_runtime_state r ON r.account_id = a.id
            WHERE af.key_hash = $1
              AND a.status = 'active'
              AND julianday(af.last_used_at) >= julianday($3, printf('-%d seconds', $2))
              AND (r.cooldown_until IS NULL OR julianday(r.cooldown_until) <= julianday($3))
              AND NOT EXISTS (
                  SELECT 1 FROM usage_windows exhausted
                  WHERE exhausted.account_id = a.id
                    AND exhausted.quota_key = 'codex'
                    AND exhausted.used_percent >= 100
                    AND (
                        exhausted.reset_at IS NULL
                        OR julianday(exhausted.reset_at) IS NULL
                        OR julianday(exhausted.reset_at) > julianday($3)
                    )
              )
            "#,
        )
        .bind(key_hash)
        .bind(settings.sticky_session_ttl_seconds)
        .bind(&selected_at)
        .fetch_optional(pool)
        .await?;
        if let Some(account_id) = pinned.filter(|id| !excluded.contains(id)) {
            // Re-check and acquire in one UPDATE so a cooldown or pause applied
            // after the affinity lookup cannot be cleared by the sticky path.
            if acquire_account_if_available(pool, account_id).await? {
                let reason = if excluded.is_empty() {
                    SelectionReason::Sticky
                } else {
                    SelectionReason::Failover
                };
                return match selected_account(pool, crypto, account_id, reason).await {
                    Ok(selected) => Ok(selected),
                    Err(error) => {
                        release_account(pool, account_id).await.ok();
                        Err(error)
                    }
                };
            }
        }
    }

    let mut tx = pool.begin().await?;
    let usage_weighted = settings.routing_strategy != "round_robin";
    let mut query = QueryBuilder::<Sqlite>::new("");
    push_smart_routing_ctes(
        &mut query,
        &selected_at,
        usage_refresh_interval,
        usage_weighted,
    );
    query.push(
        r#"
        UPDATE account_runtime_state
        SET last_selected_at = "#,
    );
    query.push_bind(&selected_at);
    query.push(
        r#",
            cooldown_until = NULL,
            cooldown_reason = NULL,
            inflight_count = inflight_count + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE account_id = (
        "#,
    );
    push_smart_account_candidate(&mut query, usage_weighted, excluded);
    query.push(") RETURNING account_id");
    let account_id = query
        .build_query_scalar::<Uuid>()
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            AppError::Unavailable(
                "no eligible accounts are available; retry after the current cooldown".to_string(),
            )
        })?;
    tx.commit().await?;

    let reason = if !excluded.is_empty() {
        SelectionReason::Failover
    } else if usage_weighted {
        SelectionReason::UsageWeighted
    } else {
        SelectionReason::RoundRobin
    };
    match selected_account(pool, crypto, account_id, reason).await {
        Ok(selected) => Ok(selected),
        Err(error) => {
            release_account(pool, account_id).await.ok();
            Err(error)
        }
    }
}

fn push_smart_routing_ctes(
    query: &mut QueryBuilder<'_, Sqlite>,
    selected_at: &str,
    usage_refresh_interval: Duration,
    usage_weighted: bool,
) {
    let freshness_seconds = usage_refresh_interval
        .as_secs()
        .saturating_mul(ROUTING_USAGE_FRESHNESS_INTERVALS)
        .max(MIN_ROUTING_USAGE_FRESHNESS_SECONDS)
        .min(i64::MAX as u64) as i64;
    query.push("WITH clock(now_jd, freshness_days, skew_days) AS (SELECT julianday(");
    query.push_bind(selected_at.to_string());
    query.push("), CAST(");
    query.push_bind(freshness_seconds);
    query.push(" AS REAL) / 86400.0, ");
    query.push_bind(ROUTING_CLOCK_SKEW_SECONDS);
    query.push(" / 86400.0)");
    if usage_weighted {
        query.push(
            r#",
            raw_windows AS (
                SELECT
                    window.account_id,
                    MIN(100.0, MAX(0.0, window.used_percent)) AS used_percent,
                    window.window_seconds,
                    window.reset_at,
                    julianday(window.reset_at) AS reset_jd,
                    julianday(window.fetched_at) AS fetched_jd,
                    clock.now_jd,
                    clock.freshness_days,
                    clock.skew_days
                FROM usage_windows window
                CROSS JOIN clock
                WHERE window.quota_key = 'codex'
            ),
            classified_windows AS (
                SELECT
                    raw.*,
                    CASE
                        WHEN raw.used_percent IS NULL
                          OR (raw.reset_at IS NOT NULL AND raw.reset_jd IS NULL)
                          OR (raw.window_seconds IS NOT NULL AND raw.window_seconds <= 0)
                          OR (
                              raw.reset_jd IS NOT NULL
                              AND raw.window_seconds IS NOT NULL
                              AND raw.reset_jd > raw.now_jd
                                  + CAST(raw.window_seconds AS REAL) / 86400.0
                                  + raw.skew_days
                          ) THEN 'unknown'
                        WHEN raw.reset_jd IS NOT NULL
                          AND raw.reset_jd <= raw.now_jd
                          AND raw.reset_jd >= raw.now_jd - raw.freshness_days
                            THEN 'reset'
                        WHEN raw.reset_jd IS NOT NULL
                          AND raw.reset_jd < raw.now_jd - raw.freshness_days
                            THEN 'unknown'
                        WHEN raw.fetched_jd IS NULL
                          OR raw.fetched_jd < raw.now_jd - raw.freshness_days
                          OR raw.fetched_jd > raw.now_jd + raw.skew_days
                            THEN 'unknown'
                        WHEN raw.reset_at IS NULL OR raw.window_seconds IS NULL
                            THEN 'raw'
                        ELSE 'timed'
                    END AS score_kind
                FROM raw_windows raw
            ),
            window_scores AS (
                SELECT
                    account_id,
                    CASE score_kind
                        WHEN 'timed' THEN used_percent - 100.0 * (
                            1.0 - MIN(1.0, MAX(
                                0.0,
                                (reset_jd - now_jd) * 86400.0
                                    / CAST(window_seconds AS REAL)
                            ))
                        )
                        WHEN 'raw' THEN used_percent
                        WHEN 'reset' THEN 0.0
                        ELSE "#,
        );
        query.push_bind(ROUTING_UNKNOWN_PRESSURE);
        query.push(
            r#"
                    END AS pressure,
                    CASE score_kind
                        WHEN 'unknown' THEN 2
                        WHEN 'reset' THEN 1
                        ELSE 0
                    END AS telemetry_quality,
                    CASE score_kind
                        WHEN 'unknown' THEN "#,
        );
        query.push_bind(ROUTING_UNKNOWN_PRESSURE);
        query.push(
            r#"
                        WHEN 'reset' THEN 0.0
                        ELSE used_percent
                    END AS effective_used
                FROM classified_windows
            ),
            account_scores AS (
                SELECT
                    account_id,
                    MAX(pressure) AS pressure,
                    MAX(telemetry_quality) AS telemetry_quality,
                    MAX(effective_used) AS worst_effective_used
                FROM window_scores
                GROUP BY account_id
            )"#,
        );
    }
}

fn push_smart_account_candidate(
    query: &mut QueryBuilder<'_, Sqlite>,
    usage_weighted: bool,
    excluded: &HashSet<Uuid>,
) {
    query.push(
        r#"
        SELECT a.id
        FROM accounts a
        INNER JOIN account_runtime_state r ON r.account_id = a.id
        "#,
    );
    if usage_weighted {
        query.push(" LEFT JOIN account_scores scores ON scores.account_id = a.id");
    }
    query.push(
        r#"
        CROSS JOIN clock
        WHERE a.status = 'active'
          AND (r.cooldown_until IS NULL OR julianday(r.cooldown_until) <= clock.now_jd)
          AND NOT EXISTS (
              SELECT 1 FROM usage_windows exhausted
              WHERE exhausted.account_id = a.id
                AND exhausted.quota_key = 'codex'
                AND exhausted.used_percent >= 100
                AND (
                    exhausted.reset_at IS NULL
                    OR julianday(exhausted.reset_at) IS NULL
                    OR julianday(exhausted.reset_at) > clock.now_jd
                )
          )
        "#,
    );
    if !excluded.is_empty() {
        query.push(" AND a.id NOT IN (");
        let mut separated = query.separated(", ");
        for account_id in excluded {
            separated.push_bind(*account_id);
        }
        separated.push_unseparated(")");
    }
    query.push(" ORDER BY ");
    if usage_weighted {
        query.push("ROUND(COALESCE(scores.pressure, ");
        query.push_bind(ROUTING_UNKNOWN_PRESSURE);
        query.push(") + ");
        query.push_bind(ROUTING_INFLIGHT_PENALTY);
        query.push(
            r#" * MAX(r.inflight_count, 0), 6) ASC,
                COALESCE(scores.telemetry_quality, 2) ASC,
                COALESCE(scores.worst_effective_used, "#,
        );
        query.push_bind(ROUTING_UNKNOWN_PRESSURE);
        query.push(
            r#") ASC,
                MAX(r.inflight_count, 0) ASC,
            "#,
        );
    }
    query.push(
        r#"COALESCE(r.last_selected_at, '1970-01-01T00:00:00.000Z') ASC,
             a.created_at ASC,
        "#,
    );
    if usage_weighted {
        query.push("a.id ASC");
    } else {
        query.push("a.rowid ASC");
    }
    query.push(" LIMIT 1");
}

fn smart_account_preview_query(
    selected_at: &str,
    settings: &RuntimeSettings,
    usage_refresh_interval: Duration,
    excluded: &HashSet<Uuid>,
) -> QueryBuilder<'static, Sqlite> {
    let usage_weighted = settings.routing_strategy != "round_robin";
    let mut query = QueryBuilder::<Sqlite>::new("");
    push_smart_routing_ctes(
        &mut query,
        selected_at,
        usage_refresh_interval,
        usage_weighted,
    );
    push_smart_account_candidate(&mut query, usage_weighted, excluded);
    query
}

/// Preview the account selected by the normal global routing order without
/// changing cooldowns, timestamps, or in-flight counters.
pub async fn preview_smart_account(
    pool: &SqlitePool,
    settings: &RuntimeSettings,
    usage_refresh_interval: Duration,
    excluded: &HashSet<Uuid>,
) -> AppResult<Option<SmartAccountPreview>> {
    let selected_at = Utc::now().format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string();
    let mut query =
        smart_account_preview_query(&selected_at, settings, usage_refresh_interval, excluded);
    let account_id = query
        .build_query_scalar::<Uuid>()
        .fetch_optional(pool)
        .await?;
    match account_id {
        Some(account_id) => Ok(Some(SmartAccountPreview {
            account_id,
            account_label: account_display_label(pool, account_id).await?,
        })),
        None => Ok(None),
    }
}

async fn preview_smart_account_id_in_transaction(
    tx: &mut Transaction<'_, Sqlite>,
    selected_at: &str,
    settings: &RuntimeSettings,
    usage_refresh_interval: Duration,
    excluded: &HashSet<Uuid>,
) -> AppResult<Option<Uuid>> {
    let mut query =
        smart_account_preview_query(selected_at, settings, usage_refresh_interval, excluded);
    query
        .build_query_scalar::<Uuid>()
        .fetch_optional(&mut **tx)
        .await
        .map_err(AppError::Database)
}

async fn account_is_eligible_in_transaction(
    tx: &mut Transaction<'_, Sqlite>,
    account_id: Uuid,
    selected_at: &str,
) -> AppResult<bool> {
    let eligible = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM accounts account
        INNER JOIN account_runtime_state runtime ON runtime.account_id = account.id
        WHERE account.id = $1
          AND account.status = 'active'
          AND (
              runtime.cooldown_until IS NULL
              OR julianday(runtime.cooldown_until) <= julianday($2)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM usage_windows exhausted
              WHERE exhausted.account_id = account.id
                AND exhausted.quota_key = 'codex'
                AND exhausted.used_percent >= 100
                AND (
                    exhausted.reset_at IS NULL
                    OR julianday(exhausted.reset_at) IS NULL
                    OR julianday(exhausted.reset_at) > julianday($2)
                )
          )
        "#,
    )
    .bind(account_id)
    .bind(selected_at)
    .fetch_one(&mut **tx)
    .await?;
    Ok(eligible == 1)
}

async fn account_display_label_in_transaction(
    tx: &mut Transaction<'_, Sqlite>,
    account_id: Uuid,
) -> AppResult<Option<String>> {
    sqlx::query_scalar::<_, String>(
        "SELECT CASE WHEN label = '' THEN 'unlabeled' ELSE label END FROM accounts WHERE id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(AppError::Database)
}

async fn selected_account(
    pool: &SqlitePool,
    crypto: &TokenCrypto,
    account_id: Uuid,
    selection_reason: SelectionReason,
) -> AppResult<SelectedAccount> {
    let account = get_account(pool, account_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("account {account_id} not found")))?;
    let tokens = AccountTokens {
        access_token: crypto.decrypt(&account.encrypted_access_token)?,
        refresh_token: crypto.decrypt(&account.encrypted_refresh_token)?,
        id_token: crypto.decrypt(&account.encrypted_id_token)?,
    };
    Ok(SelectedAccount {
        account,
        tokens,
        selection_reason,
    })
}

/// Account one request on an already-pinned transport, such as a reused
/// Responses WebSocket connection, only while the account remains eligible.
pub async fn acquire_account_if_available(pool: &SqlitePool, account_id: Uuid) -> AppResult<bool> {
    let acquired = sqlx::query_scalar::<_, Uuid>(
        r#"
        UPDATE account_runtime_state
        SET last_selected_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            cooldown_until = CASE
                WHEN cooldown_until IS NOT NULL
                 AND julianday(cooldown_until) <= julianday('now') THEN NULL
                ELSE cooldown_until
            END,
            cooldown_reason = CASE
                WHEN cooldown_until IS NOT NULL
                 AND julianday(cooldown_until) <= julianday('now') THEN NULL
                ELSE cooldown_reason
            END,
            inflight_count = inflight_count + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE account_id = $1
          AND EXISTS (
              SELECT 1 FROM accounts account
              WHERE account.id = $1 AND account.status = 'active'
          )
          AND (cooldown_until IS NULL OR julianday(cooldown_until) <= julianday('now'))
          AND NOT EXISTS (
              SELECT 1 FROM usage_windows exhausted
              WHERE exhausted.account_id = $1
                AND exhausted.quota_key = 'codex'
                AND exhausted.used_percent >= 100
                AND (
                    exhausted.reset_at IS NULL
                    OR julianday(exhausted.reset_at) IS NULL
                    OR julianday(exhausted.reset_at) > julianday('now')
                )
          )
        RETURNING account_id
        "#,
    )
    .bind(account_id)
    .fetch_optional(pool)
    .await?;
    Ok(acquired.is_some())
}

pub async fn release_account(pool: &SqlitePool, account_id: Uuid) -> AppResult<()> {
    release_account_inner(pool, account_id, true).await
}

/// Release an account chosen for transport setup before an actual model request
/// starts. Unlike `release_account`, this does not advance `last_request_at`.
pub async fn release_account_reservation(pool: &SqlitePool, account_id: Uuid) -> AppResult<()> {
    release_account_inner(pool, account_id, false).await
}

async fn release_account_inner(
    pool: &SqlitePool,
    account_id: Uuid,
    update_last_request: bool,
) -> AppResult<()> {
    let query = if update_last_request {
        r#"
        UPDATE account_runtime_state
        SET inflight_count = MAX(0, inflight_count - 1),
            last_request_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE account_id = $1
        "#
    } else {
        r#"
        UPDATE account_runtime_state
        SET inflight_count = MAX(0, inflight_count - 1),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE account_id = $1
        "#
    };
    let mut last_error = None;
    for attempt in 0..3_u64 {
        match sqlx::query(query).bind(account_id).execute(pool).await {
            Ok(_) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                if attempt < 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(25 * (attempt + 1))).await;
                }
            }
        }
    }
    Err(AppError::Database(
        last_error.expect("release loop always attempts at least once"),
    ))
}

pub async fn reset_inflight(pool: &SqlitePool) -> AppResult<()> {
    sqlx::query("UPDATE account_runtime_state SET inflight_count = 0")
        .execute(pool)
        .await?;
    Ok(())
}

/// Apply one privacy-preserving session route command. Alias/root merging,
/// smart fallback selection, command persistence, and affinity rewrites all
/// happen in one SQLite transaction. A dry run executes the same reads and
/// decision logic, then rolls the transaction back.
pub async fn apply_session_route_action(
    pool: &SqlitePool,
    settings: &RuntimeSettings,
    usage_refresh_interval: Duration,
    request: SessionRouteActionRequest,
    live_account_ids: &HashSet<Uuid>,
) -> AppResult<SessionRouteActionResponse> {
    if !valid_affinity_hash(&request.root_key_hash) {
        return Err(AppError::BadRequest(
            "session root hashes must be 64 lowercase hexadecimal characters".to_string(),
        ));
    }
    let input_keys = validate_session_route_keys(&request.keys)?;
    if input_keys.is_empty() {
        return Err(AppError::BadRequest(
            "at least one session route key is required".to_string(),
        ));
    }
    if !input_keys
        .iter()
        .any(|key| key.key_hash == request.root_key_hash)
    {
        return Err(AppError::BadRequest(
            "the session root hash must also be present in keys".to_string(),
        ));
    }
    let requested_target = match request.action {
        SessionRouteActionKind::Rebalance => {
            if request.target.is_some() {
                return Err(AppError::BadRequest(
                    "rebalance actions do not accept a target".to_string(),
                ));
            }
            None
        }
        SessionRouteActionKind::Reroute => {
            let target = request.target.as_deref().ok_or_else(|| {
                AppError::BadRequest("reroute actions require a target".to_string())
            })?;
            Some(resolve_session_route_target(pool, target).await?)
        }
    };

    let selected_at = Utc::now().format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string();
    let mut tx = pool.begin().await?;
    let canonical_existed = if request.dry_run {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM session_roots WHERE root_key_hash = $1")
            .bind(&request.root_key_hash)
            .fetch_one(&mut *tx)
            .await?
            == 1
    } else {
        // This is intentionally the first statement in the transaction: it
        // obtains SQLite's single-writer lock before any merge snapshot.
        sqlx::query_scalar::<_, String>(
            r#"
            INSERT INTO session_roots (root_key_hash)
            VALUES ($1)
            ON CONFLICT (root_key_hash) DO NOTHING
            RETURNING root_key_hash
            "#,
        )
        .bind(&request.root_key_hash)
        .fetch_optional(&mut *tx)
        .await?
        .is_none()
    };

    if let Some(target) = requested_target.as_ref()
        && account_display_label_in_transaction(&mut tx, target.account_id)
            .await?
            .is_none()
    {
        tx.rollback().await?;
        return Err(AppError::NotFound("account target not found".to_string()));
    }

    let mut prior_roots_query = QueryBuilder::<Sqlite>::new(
        "SELECT DISTINCT root_key_hash FROM session_route_keys WHERE key_hash IN (",
    );
    let mut separated = prior_roots_query.separated(", ");
    for key in &input_keys {
        separated.push_bind(key.key_hash.clone());
    }
    separated.push_unseparated(")");
    let prior_mapped_roots = prior_roots_query
        .build_query_scalar::<String>()
        .fetch_all(&mut *tx)
        .await?;
    let mut root_hashes = prior_mapped_roots.iter().cloned().collect::<HashSet<_>>();
    root_hashes.insert(request.root_key_hash.clone());

    let mut existing_keys_query = QueryBuilder::<Sqlite>::new(
        "SELECT key_hash, root_key_hash, kind FROM session_route_keys WHERE root_key_hash IN (",
    );
    let mut separated = existing_keys_query.separated(", ");
    for root in &root_hashes {
        separated.push_bind(root.clone());
    }
    separated.push_unseparated(") ORDER BY key_hash");
    let existing_keys = existing_keys_query
        .build_query_as::<SessionRouteKeyRow>()
        .fetch_all(&mut *tx)
        .await?;
    let existing_by_hash = existing_keys
        .iter()
        .map(|key| (key.key_hash.clone(), key.clone()))
        .collect::<HashMap<_, _>>();
    let mut all_keys_by_hash = existing_keys
        .iter()
        .map(|key| (key.key_hash.clone(), key.kind.clone()))
        .collect::<HashMap<_, _>>();
    for key in &input_keys {
        all_keys_by_hash.insert(key.key_hash.clone(), key.kind.clone());
    }
    let mut all_keys = all_keys_by_hash
        .into_iter()
        .map(|(key_hash, kind)| SessionRouteKey { key_hash, kind })
        .collect::<Vec<_>>();
    all_keys.sort_by(|left, right| left.key_hash.cmp(&right.key_hash));

    let merged_root_count = prior_mapped_roots
        .iter()
        .filter(|root| root.as_str() != request.root_key_hash)
        .count();
    let mapping_changed = !canonical_existed
        || existing_keys.len() != all_keys.len()
        || all_keys.iter().any(|key| {
            existing_by_hash.get(&key.key_hash).is_none_or(|existing| {
                existing.root_key_hash != request.root_key_hash || existing.kind != key.kind
            })
        });

    let mut root_generation_query = QueryBuilder::<Sqlite>::new(
        "SELECT COALESCE(MAX(route_generation), 0) FROM session_roots WHERE root_key_hash IN (",
    );
    let mut separated = root_generation_query.separated(", ");
    for root in &root_hashes {
        separated.push_bind(root.clone());
    }
    separated.push_unseparated(")");
    let root_generation = root_generation_query
        .build_query_scalar::<i64>()
        .fetch_one(&mut *tx)
        .await?;

    let mut affinity_query = QueryBuilder::<Sqlite>::new(
        r#"
        SELECT affinity.key_hash, affinity.kind, affinity.account_id,
               CASE WHEN account.label = '' THEN 'unlabeled' ELSE account.label END AS account_label,
               affinity.route_generation
        FROM affinity
        INNER JOIN accounts account ON account.id = affinity.account_id
        WHERE affinity.key_hash IN (
        "#,
    );
    let mut separated = affinity_query.separated(", ");
    for key in &all_keys {
        separated.push_bind(key.key_hash.clone());
    }
    separated.push_unseparated(") ORDER BY affinity.key_hash");
    let affinity_routes = affinity_query
        .build_query_as::<AffinityRouteActionRow>()
        .fetch_all(&mut *tx)
        .await?;
    if !canonical_existed
        && prior_mapped_roots.is_empty()
        && affinity_routes.is_empty()
        && live_account_ids.is_empty()
    {
        tx.rollback().await?;
        return Err(AppError::NotFound(
            "no routed session matches the supplied fingerprints".to_string(),
        ));
    }
    let affinity_generation = affinity_routes
        .iter()
        .map(|route| route.route_generation)
        .max()
        .unwrap_or(0);
    let current_generation = root_generation.max(affinity_generation);

    let mut previous_accounts = affinity_routes
        .iter()
        .map(|route| route.account_label.clone())
        .collect::<HashSet<_>>();
    for account_id in live_account_ids {
        if let Some(label) = account_display_label_in_transaction(&mut tx, *account_id).await? {
            previous_accounts.insert(label);
        }
    }
    let mut previous_accounts = previous_accounts.into_iter().collect::<Vec<_>>();
    previous_accounts.sort();

    let requested_account = if let Some(target) = requested_target.as_ref() {
        account_display_label_in_transaction(&mut tx, target.account_id).await?
    } else {
        None
    };
    let mut used_fallback = false;
    let effective_account_id = match requested_target.as_ref() {
        Some(target)
            if account_is_eligible_in_transaction(&mut tx, target.account_id, &selected_at)
                .await? =>
        {
            Some(target.account_id)
        }
        Some(_) => {
            used_fallback = true;
            preview_smart_account_id_in_transaction(
                &mut tx,
                &selected_at,
                settings,
                usage_refresh_interval,
                &HashSet::new(),
            )
            .await?
        }
        None => {
            preview_smart_account_id_in_transaction(
                &mut tx,
                &selected_at,
                settings,
                usage_refresh_interval,
                &HashSet::new(),
            )
            .await?
        }
    };
    let effective_account = match effective_account_id {
        Some(account_id) => account_display_label_in_transaction(&mut tx, account_id).await?,
        None => None,
    };

    let affinity_by_hash = affinity_routes
        .iter()
        .map(|route| (route.key_hash.as_str(), route))
        .collect::<HashMap<_, _>>();
    let changed_route_count = effective_account_id.map_or(0, |account_id| {
        all_keys
            .iter()
            .filter(|key| {
                affinity_by_hash
                    .get(key.key_hash.as_str())
                    .is_none_or(|route| route.account_id != account_id || route.kind != key.kind)
            })
            .count()
    });
    let live_account_changed = effective_account_id
        .is_some_and(|account_id| live_account_ids.iter().any(|live| *live != account_id));
    let generation_mismatch = affinity_routes
        .iter()
        .any(|route| route.route_generation != current_generation);
    let requires_move = effective_account_id.is_some()
        && (changed_route_count != 0
            || live_account_changed
            || mapping_changed
            || generation_mismatch);
    let route_generation = if requires_move {
        current_generation
            .checked_add(1)
            .ok_or_else(|| AppError::Internal("session route generation overflowed".to_string()))?
    } else {
        current_generation
    };
    let status = if effective_account_id.is_none() {
        SessionRouteActionStatus::Pending
    } else if used_fallback {
        SessionRouteActionStatus::Fallback
    } else if requires_move {
        SessionRouteActionStatus::Applied
    } else {
        SessionRouteActionStatus::NoOp
    };

    if request.dry_run {
        tx.rollback().await?;
    } else {
        sqlx::query(
            r#"
            UPDATE session_roots
            SET route_generation = MAX(route_generation, $2),
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE root_key_hash = $1
            "#,
        )
        .bind(&request.root_key_hash)
        .bind(current_generation)
        .execute(&mut *tx)
        .await?;
        for key in &all_keys {
            sqlx::query(
                r#"
                INSERT INTO session_route_keys (key_hash, root_key_hash, kind)
                VALUES ($1, $2, $3)
                ON CONFLICT (key_hash) DO UPDATE SET
                    root_key_hash = EXCLUDED.root_key_hash,
                    kind = EXCLUDED.kind,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                "#,
            )
            .bind(&key.key_hash)
            .bind(&request.root_key_hash)
            .bind(&key.kind)
            .execute(&mut *tx)
            .await?;
        }
        for old_root in root_hashes
            .iter()
            .filter(|root| root.as_str() != request.root_key_hash)
        {
            sqlx::query("DELETE FROM session_roots WHERE root_key_hash = $1")
                .bind(old_root)
                .execute(&mut *tx)
                .await?;
        }
        if requires_move {
            let account_id = effective_account_id.expect("move requires an effective account");
            for key in &all_keys {
                sqlx::query(
                    r#"
                    INSERT INTO affinity (
                        key_hash, kind, account_id, route_generation, last_used_at
                    ) VALUES (
                        $1, $2, $3, $4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    )
                    ON CONFLICT (key_hash) DO UPDATE SET
                        kind = EXCLUDED.kind,
                        account_id = EXCLUDED.account_id,
                        route_generation = EXCLUDED.route_generation,
                        last_used_at = EXCLUDED.last_used_at
                    "#,
                )
                .bind(&key.key_hash)
                .bind(&key.kind)
                .bind(account_id)
                .bind(route_generation)
                .execute(&mut *tx)
                .await?;
            }
        }
        sqlx::query(
            r#"
            UPDATE session_roots
            SET route_generation = $2,
                last_action = $3,
                requested_account_id = $4,
                effective_account_id = $5,
                action_status = $6,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE root_key_hash = $1
            "#,
        )
        .bind(&request.root_key_hash)
        .bind(route_generation)
        .bind(request.action.as_str())
        .bind(requested_target.as_ref().map(|target| target.account_id))
        .bind(effective_account_id)
        .bind(status.as_str())
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
    }

    Ok(SessionRouteActionResponse {
        root_key_hash: request.root_key_hash.clone(),
        session_fingerprint: request.root_key_hash[..12].to_string(),
        action: request.action,
        status,
        requested_account,
        effective_account,
        previous_accounts,
        route_generation,
        linked_route_count: all_keys.len(),
        changed_route_count,
        merged_root_count,
        dry_run: request.dry_run,
    })
}

/// Reconstruct durable pending commands for the scheduler. Requests are
/// intentionally non-serializable because they contain complete hashes and an
/// internal UUID selector.
pub async fn list_pending_session_route_actions(
    pool: &SqlitePool,
    limit: i64,
) -> AppResult<Vec<SessionRouteActionRequest>> {
    let roots = sqlx::query_as::<_, (String, String, Option<Uuid>)>(
        r#"
        SELECT root_key_hash, last_action, requested_account_id
        FROM session_roots
        WHERE action_status = 'pending'
          AND last_action IN ('rebalance', 'reroute')
        ORDER BY updated_at ASC, root_key_hash ASC
        LIMIT $1
        "#,
    )
    .bind(limit.clamp(1, 100))
    .fetch_all(pool)
    .await?;
    let mut pending = Vec::with_capacity(roots.len());
    for (root_key_hash, action, requested_account_id) in roots {
        let mut action = parse_session_route_action(&action)?;
        if action == SessionRouteActionKind::Reroute && requested_account_id.is_none() {
            action = SessionRouteActionKind::Rebalance;
        }
        let keys = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT key_hash, kind
            FROM session_route_keys
            WHERE root_key_hash = $1
            ORDER BY key_hash
            "#,
        )
        .bind(&root_key_hash)
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|(key_hash, kind)| SessionRouteKey { key_hash, kind })
        .collect();
        pending.push(SessionRouteActionRequest {
            root_key_hash,
            keys,
            action,
            target: requested_account_id.map(|account_id| account_id.to_string()),
            dry_run: false,
        });
    }
    Ok(pending)
}

pub async fn bind_affinity(
    pool: &SqlitePool,
    key_hash: &str,
    kind: &str,
    account_id: Uuid,
) -> AppResult<()> {
    let key = SessionRouteKey {
        key_hash: key_hash.to_string(),
        kind: kind.to_string(),
    };
    bind_affinities_at_epoch(pool, std::slice::from_ref(&key), account_id, None).await?;
    Ok(())
}

/// Resolve the newest durable routing epoch attached to any supplied hashed
/// alias. Normally all aliases map to one root; ordering by the latest root
/// command also gives deterministic behavior while a historical conflict is
/// waiting to be merged by a route action.
pub async fn resolve_session_route_epoch(
    pool: &SqlitePool,
    keys: &[SessionRouteKey],
) -> AppResult<Option<SessionRouteEpoch>> {
    let keys = validate_session_route_keys(keys)?;
    if keys.is_empty() {
        return Ok(None);
    }
    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
        SELECT roots.root_key_hash, roots.route_generation, route_key.key_hash
        FROM session_route_keys route_key
        INNER JOIN session_roots roots ON roots.root_key_hash = route_key.root_key_hash
        WHERE route_key.key_hash IN (
        "#,
    );
    let mut separated = query.separated(", ");
    for key in &keys {
        separated.push_bind(key.key_hash.clone());
    }
    separated.push_unseparated(
        r#")
        ORDER BY julianday(roots.updated_at) DESC,
                 roots.route_generation DESC,
                 roots.root_key_hash ASC,
                 route_key.key_hash ASC
        LIMIT 1"#,
    );
    query
        .build_query_as::<(String, i64, String)>()
        .fetch_optional(pool)
        .await
        .map(|row| {
            row.map(
                |(root_key_hash, route_generation, selection_key_hash)| SessionRouteEpoch {
                    root_key_hash,
                    route_generation,
                    selection_key_hash,
                },
            )
        })
        .map_err(AppError::Database)
}

pub async fn resolve_session_route_context(
    pool: &SqlitePool,
    keys: &[SessionRouteKey],
) -> AppResult<Option<SessionRouteContext>> {
    let Some(epoch) = resolve_session_route_epoch(pool, keys).await? else {
        return Ok(None);
    };
    let row = sqlx::query_as::<
        _,
        (
            i64,
            Option<String>,
            Option<String>,
            Option<Uuid>,
            Option<Uuid>,
        ),
    >(
        r#"
        SELECT route_generation, last_action, action_status,
               requested_account_id, effective_account_id
        FROM session_roots
        WHERE root_key_hash = $1
        "#,
    )
    .bind(&epoch.root_key_hash)
    .fetch_optional(pool)
    .await?;
    let Some((route_generation, action, action_status, requested_account_id, effective_account_id)) =
        row
    else {
        return Ok(None);
    };
    Ok(Some(SessionRouteContext {
        root_key_hash: epoch.root_key_hash,
        route_generation,
        action: action
            .as_deref()
            .map(parse_session_route_action)
            .transpose()?,
        action_status: action_status
            .as_deref()
            .map(parse_session_route_status)
            .transpose()?,
        requested_account_id,
        effective_account_id,
    }))
}

pub async fn session_route_epoch_is_current(
    pool: &SqlitePool,
    epoch: &SessionRouteEpoch,
) -> AppResult<bool> {
    let current = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)
        FROM session_roots
        WHERE root_key_hash = $1 AND route_generation = $2
        "#,
    )
    .bind(&epoch.root_key_hash)
    .bind(epoch.route_generation)
    .fetch_one(pool)
    .await?;
    Ok(current == 1)
}

/// Bind one or more affinity aliases only if their captured root epoch is still
/// current. `None` retains legacy behavior only while no alias has been claimed
/// by a durable session root. A false result means the caller must reconnect.
pub async fn bind_affinities_at_epoch(
    pool: &SqlitePool,
    keys: &[SessionRouteKey],
    account_id: Uuid,
    epoch: Option<&SessionRouteEpoch>,
) -> AppResult<bool> {
    let keys = validate_session_route_keys(keys)?;
    if keys.is_empty() {
        return Ok(true);
    }
    let mut tx = pool.begin().await?;
    if let Some(epoch) = epoch {
        let locked = sqlx::query_as::<_, (i64, Option<String>, Option<Uuid>, Option<Uuid>)>(
            r#"
            UPDATE session_roots
            SET route_generation = route_generation
            WHERE root_key_hash = $1 AND route_generation = $2
            RETURNING route_generation, last_action,
                      requested_account_id, effective_account_id
            "#,
        )
        .bind(&epoch.root_key_hash)
        .bind(epoch.route_generation)
        .fetch_optional(&mut *tx)
        .await?;
        let Some((_, last_action, requested_account_id, effective_account_id)) = locked else {
            tx.rollback().await?;
            return Ok(false);
        };
        let mut conflicts = QueryBuilder::<Sqlite>::new(
            "SELECT COUNT(*) FROM session_route_keys WHERE root_key_hash <> ",
        );
        conflicts.push_bind(epoch.root_key_hash.clone());
        conflicts.push(" AND key_hash IN (");
        let mut separated = conflicts.separated(", ");
        for key in &keys {
            separated.push_bind(key.key_hash.clone());
        }
        separated.push_unseparated(")");
        let conflict_count = conflicts
            .build_query_scalar::<i64>()
            .fetch_one(&mut *tx)
            .await?;
        if conflict_count != 0 {
            tx.rollback().await?;
            return Ok(false);
        }
        for key in &keys {
            sqlx::query(
                r#"
                INSERT INTO session_route_keys (key_hash, root_key_hash, kind)
                VALUES ($1, $2, $3)
                ON CONFLICT (key_hash) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE session_route_keys.root_key_hash = EXCLUDED.root_key_hash
                "#,
            )
            .bind(&key.key_hash)
            .bind(&epoch.root_key_hash)
            .bind(&key.kind)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO affinity (
                    key_hash, kind, account_id, route_generation, last_used_at
                ) VALUES (
                    $1, $2, $3, $4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                )
                ON CONFLICT (key_hash) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    account_id = EXCLUDED.account_id,
                    route_generation = EXCLUDED.route_generation,
                    last_used_at = EXCLUDED.last_used_at
                WHERE affinity.route_generation <= EXCLUDED.route_generation
                "#,
            )
            .bind(&key.key_hash)
            .bind(&key.kind)
            .bind(account_id)
            .bind(epoch.route_generation)
            .execute(&mut *tx)
            .await?;
        }
        if last_action.as_deref() == Some("reroute") && effective_account_id != Some(account_id) {
            sqlx::query(
                r#"
                UPDATE affinity
                SET account_id = $2,
                    route_generation = $3,
                    last_used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE key_hash IN (
                    SELECT key_hash
                    FROM session_route_keys
                    WHERE root_key_hash = $1
                )
                "#,
            )
            .bind(&epoch.root_key_hash)
            .bind(account_id)
            .bind(epoch.route_generation)
            .execute(&mut *tx)
            .await?;
            let fallback = requested_account_id != Some(account_id);
            sqlx::query(
                r#"
                UPDATE session_roots
                SET effective_account_id = $2,
                    action_status = CASE WHEN $3 THEN 'fallback' ELSE 'applied' END,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE root_key_hash = $1 AND route_generation = $4
                "#,
            )
            .bind(&epoch.root_key_hash)
            .bind(account_id)
            .bind(fallback)
            .bind(epoch.route_generation)
            .execute(&mut *tx)
            .await?;
        }
    } else {
        let preferred_root = keys
            .iter()
            .find(|key| key.kind == "session_id")
            .unwrap_or(&keys[0])
            .key_hash
            .clone();
        let inserted_root = sqlx::query_scalar::<_, String>(
            r#"
            INSERT INTO session_roots (root_key_hash)
            VALUES ($1)
            ON CONFLICT (root_key_hash) DO NOTHING
            RETURNING root_key_hash
            "#,
        )
        .bind(&preferred_root)
        .fetch_optional(&mut *tx)
        .await?;
        let mut mapped = QueryBuilder::<Sqlite>::new(
            r#"
            SELECT DISTINCT roots.root_key_hash, roots.route_generation, roots.last_action
            FROM session_route_keys route_key
            INNER JOIN session_roots roots ON roots.root_key_hash = route_key.root_key_hash
            WHERE route_key.key_hash IN (
            "#,
        );
        let mut separated = mapped.separated(", ");
        for key in &keys {
            separated.push_bind(key.key_hash.clone());
        }
        separated.push_unseparated(")");
        let mapped_roots = mapped
            .build_query_as::<(String, i64, Option<String>)>()
            .fetch_all(&mut *tx)
            .await?;
        if mapped_roots.len() > 1
            || mapped_roots
                .first()
                .is_some_and(|(_, generation, action)| *generation != 0 || action.is_some())
        {
            tx.rollback().await?;
            return Ok(false);
        }
        let root_key_hash = mapped_roots
            .first()
            .map(|(root, _, _)| root.clone())
            .unwrap_or_else(|| preferred_root.clone());
        if inserted_root.is_some() && root_key_hash != preferred_root {
            sqlx::query(
                "DELETE FROM session_roots WHERE root_key_hash = $1 AND last_action IS NULL AND NOT EXISTS (SELECT 1 FROM session_route_keys WHERE root_key_hash = $1)",
            )
            .bind(&preferred_root)
            .execute(&mut *tx)
            .await?;
        }
        for key in &keys {
            sqlx::query(
                r#"
                INSERT INTO session_route_keys (key_hash, root_key_hash, kind)
                VALUES ($1, $2, $3)
                ON CONFLICT (key_hash) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE session_route_keys.root_key_hash = EXCLUDED.root_key_hash
                "#,
            )
            .bind(&key.key_hash)
            .bind(&root_key_hash)
            .bind(&key.kind)
            .execute(&mut *tx)
            .await?;
            sqlx::query(
                r#"
                INSERT INTO affinity (key_hash, kind, account_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (key_hash) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    account_id = EXCLUDED.account_id,
                    last_used_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                "#,
            )
            .bind(&key.key_hash)
            .bind(&key.kind)
            .bind(account_id)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;
    Ok(true)
}

pub fn affinity_hash(kind: &str, value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(kind.as_bytes());
    hasher.update([0]);
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn validate_session_route_keys(keys: &[SessionRouteKey]) -> AppResult<Vec<SessionRouteKey>> {
    if keys.len() > 500 {
        return Err(AppError::BadRequest(
            "at most 500 session route keys can be used at once".to_string(),
        ));
    }
    let mut unique = HashMap::<String, String>::with_capacity(keys.len());
    let mut validated = Vec::with_capacity(keys.len());
    for key in keys {
        if !valid_affinity_hash(&key.key_hash) {
            return Err(AppError::BadRequest(
                "session route hashes must be 64 lowercase hexadecimal characters".to_string(),
            ));
        }
        let kind = key.kind.trim();
        if kind.is_empty()
            || kind.len() > 64
            || !kind
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(AppError::BadRequest(
                "session route kinds must be 1-64 ASCII letters, numbers, '_' or '-'".to_string(),
            ));
        }
        match unique.get(&key.key_hash) {
            Some(previous) if previous != kind => {
                return Err(AppError::BadRequest(
                    "one session route hash cannot have multiple kinds".to_string(),
                ));
            }
            Some(_) => {}
            None => {
                unique.insert(key.key_hash.clone(), kind.to_string());
                validated.push(SessionRouteKey {
                    key_hash: key.key_hash.clone(),
                    kind: kind.to_string(),
                });
            }
        }
    }
    Ok(validated)
}

fn valid_affinity_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn parse_session_route_action(value: &str) -> AppResult<SessionRouteActionKind> {
    match value {
        "rebalance" => Ok(SessionRouteActionKind::Rebalance),
        "reroute" => Ok(SessionRouteActionKind::Reroute),
        _ => Err(AppError::Internal(
            "database contains an invalid session route action".to_string(),
        )),
    }
}

fn parse_session_route_status(value: &str) -> AppResult<SessionRouteActionStatus> {
    match value {
        "applied" => Ok(SessionRouteActionStatus::Applied),
        "fallback" => Ok(SessionRouteActionStatus::Fallback),
        "pending" => Ok(SessionRouteActionStatus::Pending),
        "no_op" => Ok(SessionRouteActionStatus::NoOp),
        _ => Err(AppError::Internal(
            "database contains an invalid session route status".to_string(),
        )),
    }
}

pub async fn list_session_routes(
    pool: &SqlitePool,
    limit: i64,
    sticky_ttl_seconds: i64,
) -> AppResult<Vec<SessionRoute>> {
    sqlx::query_as::<_, SessionRoute>(
        r#"
        SELECT
            af.key_hash,
            af.kind,
            af.account_id,
            CASE WHEN a.label = '' THEN 'account-' || substr(hex(a.id), 1, 8) ELSE a.label END AS account_label,
            af.created_at,
            af.last_used_at
        FROM affinity af
        INNER JOIN accounts a ON a.id = af.account_id
        WHERE af.kind IN ('session_id', 'thread_id', 'conversation_id', 'prompt_cache_key')
          AND julianday(af.last_used_at) >= julianday('now', printf('-%d seconds', $1))
        ORDER BY julianday(af.last_used_at) DESC, af.key_hash ASC
        LIMIT $2
        "#,
    )
    .bind(sticky_ttl_seconds.max(1))
    .bind(limit.clamp(1, 500))
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn resolve_session_routes(
    pool: &SqlitePool,
    key_hashes: &[String],
    sticky_ttl_seconds: i64,
) -> AppResult<Vec<SessionRoute>> {
    if key_hashes.is_empty() {
        return Ok(Vec::new());
    }
    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
        SELECT
            af.key_hash,
            af.kind,
            af.account_id,
            CASE WHEN a.label = '' THEN 'account-' || substr(hex(a.id), 1, 8) ELSE a.label END AS account_label,
            af.created_at,
            af.last_used_at
        FROM affinity af
        INNER JOIN accounts a ON a.id = af.account_id
        WHERE julianday(af.last_used_at) >= julianday('now', printf('-%d seconds', "#,
    );
    query.push_bind(sticky_ttl_seconds.max(1));
    query.push(")) AND af.key_hash IN (");
    let mut separated = query.separated(", ");
    for key_hash in key_hashes.iter().take(500) {
        separated.push_bind(key_hash);
    }
    separated.push_unseparated(") ORDER BY julianday(af.last_used_at) DESC, af.key_hash ASC");
    query
        .build_query_as::<SessionRoute>()
        .fetch_all(pool)
        .await
        .map_err(AppError::Database)
}

pub async fn account_count(pool: &SqlitePool) -> AppResult<i64> {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM accounts WHERE status = 'active'")
        .fetch_one(pool)
        .await
        .map_err(AppError::Database)
}

pub async fn cooldown_account(
    pool: &SqlitePool,
    id: Uuid,
    seconds: i64,
    reason: &str,
) -> AppResult<()> {
    ensure_runtime_row(pool, id).await?;
    sqlx::query(
        r#"
        UPDATE account_runtime_state
        SET cooldown_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', printf('+%d seconds', $2)),
            cooldown_reason = $3,
            failure_count = failure_count + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE account_id = $1
        "#,
    )
    .bind(id)
    .bind(seconds)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_auth_failed(pool: &SqlitePool, id: Uuid, reason: &str) -> AppResult<()> {
    sqlx::query(
        "UPDATE accounts SET status = 'auth_failed', status_reason = $2, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = $1",
    )
    .bind(id)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_request_log(pool: &SqlitePool, entry: NewRequestLog<'_>) -> AppResult<()> {
    let mut tx = pool.begin().await?;
    let input_tokens = entry.usage.input_tokens;
    let output_tokens = entry.usage.output_tokens;
    let aggregate_cached_input_tokens = entry.usage.cached_input_tokens.map(|value| value.max(0));
    let aggregate_cache_write_input_tokens = entry
        .usage
        .cache_write_input_tokens
        .map(|value| value.max(0));
    let cost_model = entry.usage.effective_model.as_deref().or(entry.model);
    let cost = pricing::estimate_standard_api_cost(cost_model, &entry.usage);
    let complete_cost = i64::from(cost.status == ApiCostStatus::Complete);
    let partial_cost = i64::from(cost.status == ApiCostStatus::MissingCacheWrite);
    let unpriced_cost = i64::from(!cost.status.is_priced());
    sqlx::query(
        r#"
        INSERT INTO request_logs (
            request_id, account_id, model, status, selection_reason, error_code, error_message,
            input_tokens, output_tokens, cached_input_tokens, cache_write_input_tokens,
            reasoning_tokens, effective_model, effective_service_tier,
            api_pricing_version, api_cost_status, api_cost_lower_nano_usd,
            api_cost_upper_nano_usd, latency_ms
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
        "#,
    )
    .bind(entry.request_id)
    .bind(entry.account_id)
    .bind(entry.model)
    .bind(entry.status)
    .bind(entry.selection_reason.map(SelectionReason::as_str))
    .bind(entry.error_code)
    .bind(entry.error_message)
    .bind(input_tokens)
    .bind(output_tokens)
    .bind(entry.usage.cached_input_tokens)
    .bind(entry.usage.cache_write_input_tokens)
    .bind(entry.usage.reasoning_tokens)
    .bind(entry.usage.effective_model.as_deref())
    .bind(entry.usage.effective_service_tier.as_deref())
    .bind(PRICING_VERSION)
    .bind(cost.status.as_str())
    .bind(cost.lower_nano_usd)
    .bind(cost.upper_nano_usd)
    .bind(entry.latency_ms)
    .execute(&mut *tx)
    .await?;
    if let Some(account_id) = entry.account_id {
        sqlx::query(
            r#"
            UPDATE account_runtime_state
            SET request_count = request_count + 1,
                successful_request_count = successful_request_count + CASE WHEN $2 = 'success' THEN 1 ELSE 0 END,
                failed_request_count = failed_request_count + CASE WHEN $2 <> 'success' THEN 1 ELSE 0 END,
                input_tokens = input_tokens + COALESCE($3, 0),
                output_tokens = output_tokens + COALESCE($4, 0),
                cached_input_tokens = cached_input_tokens + COALESCE($5, 0),
                observed_cache_write_input_tokens = observed_cache_write_input_tokens + COALESCE($6, 0),
                api_cost_lower_nano_usd = api_cost_lower_nano_usd + COALESCE($7, 0),
                api_cost_upper_nano_usd = api_cost_upper_nano_usd + COALESCE($8, 0),
                api_cost_complete_request_count = api_cost_complete_request_count + $9,
                api_cost_partial_request_count = api_cost_partial_request_count + $10,
                api_cost_unpriced_request_count = api_cost_unpriced_request_count + $11,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE account_id = $1
            "#,
        )
        .bind(account_id)
        .bind(entry.status)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(aggregate_cached_input_tokens)
        .bind(aggregate_cache_write_input_tokens)
        .bind(cost.lower_nano_usd)
        .bind(cost.upper_nano_usd)
        .bind(complete_cost)
        .bind(partial_cost)
        .bind(unpriced_cost)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn list_request_logs(pool: &SqlitePool, query: LogsQuery) -> AppResult<Vec<RequestLog>> {
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let offset = query.offset.unwrap_or(0).max(0);
    sqlx::query_as::<_, RequestLog>(
        r#"
        SELECT * FROM request_logs
        ORDER BY created_at DESC, id DESC
        LIMIT $1 OFFSET $2
        "#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn usage_summary(pool: &SqlitePool) -> AppResult<UsageSummary> {
    sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*) FROM accounts) AS account_count,
            (SELECT COUNT(*) FROM accounts WHERE status = 'active') AS active_account_count,
            COALESCE((SELECT SUM(request_count) FROM account_runtime_state), 0) AS request_count,
            COALESCE((SELECT SUM(successful_request_count) FROM account_runtime_state), 0) AS successful_request_count,
            COALESCE((SELECT SUM(failed_request_count) FROM account_runtime_state), 0) AS failed_request_count,
            COALESCE((SELECT SUM(input_tokens) FROM account_runtime_state), 0) AS input_tokens,
            COALESCE((SELECT SUM(output_tokens) FROM account_runtime_state), 0) AS output_tokens
        "#,
    )
    .fetch_one(pool)
    .await
    .map(|row| UsageSummary {
        account_count: row.0,
        active_account_count: row.1,
        request_count: row.2,
        successful_request_count: row.3,
        failed_request_count: row.4,
        input_tokens: row.5,
        output_tokens: row.6,
    })
    .map_err(AppError::Database)
}

pub async fn insert_usage_snapshot(
    pool: &SqlitePool,
    account_id: Uuid,
    used_percent: Option<f64>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
    reset_at: Option<DateTime<Utc>>,
    raw_json: Value,
) -> AppResult<UsageSnapshot> {
    sqlx::query_as::<_, UsageSnapshot>(
        r#"
        INSERT INTO usage_snapshots (account_id, used_percent, input_tokens, output_tokens, reset_at, raw_json)
        VALUES ($1,$2,$3,$4,$5,$6)
        RETURNING *
        "#,
    )
    .bind(account_id)
    .bind(used_percent)
    .bind(input_tokens)
    .bind(output_tokens)
    .bind(reset_at)
    .bind(raw_json)
    .fetch_one(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn list_usage_for_account(
    pool: &SqlitePool,
    account_id: Uuid,
    limit: i64,
) -> AppResult<Vec<UsageSnapshot>> {
    sqlx::query_as::<_, UsageSnapshot>(
        r#"
        SELECT * FROM usage_snapshots
        WHERE account_id = $1
        ORDER BY recorded_at DESC, id DESC
        LIMIT $2
        "#,
    )
    .bind(account_id)
    .bind(limit.clamp(1, 500))
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn replace_usage_windows(
    pool: &SqlitePool,
    account_id: Uuid,
    plan_type: Option<&str>,
    fetched_at: DateTime<Utc>,
    windows: &[ParsedUsageWindow],
) -> AppResult<()> {
    if !windows.iter().any(|window| window.quota_key == "codex") {
        return Err(AppError::Upstream(
            "usage response did not contain a Codex quota window".to_string(),
        ));
    }
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM usage_windows WHERE account_id = $1")
        .bind(account_id)
        .execute(&mut *tx)
        .await?;

    for window in windows {
        sqlx::query(
            r#"
            INSERT INTO usage_windows (
                account_id, quota_key, quota_name, source_slot, window_kind, used_percent,
                window_seconds, reset_at, fetched_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            "#,
        )
        .bind(account_id)
        .bind(&window.quota_key)
        .bind(&window.quota_name)
        .bind(&window.source_slot)
        .bind(&window.window_kind)
        .bind(window.used_percent)
        .bind(window.window_seconds)
        .bind(window.reset_at)
        .bind(fetched_at)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO usage_samples (
                account_id, quota_key, source_slot, window_kind, used_percent, reset_at, recorded_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            "#,
        )
        .bind(account_id)
        .bind(&window.quota_key)
        .bind(&window.source_slot)
        .bind(&window.window_kind)
        .bind(window.used_percent)
        .bind(window.reset_at)
        .bind(fetched_at)
        .execute(&mut *tx)
        .await?;
    }

    sqlx::query(
        r#"
        UPDATE accounts
        SET plan_type = COALESCE($2, plan_type),
            last_usage_refresh_at = $3,
            last_usage_error = NULL,
            status = CASE
                WHEN status IN ('rate_limited', 'quota_exceeded') THEN 'active'
                ELSE status
            END,
            status_reason = CASE
                WHEN status IN ('rate_limited', 'quota_exceeded') THEN NULL
                ELSE status_reason
            END,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE id = $1
        "#,
    )
    .bind(account_id)
    .bind(plan_type)
    .bind(fetched_at)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn mark_usage_error(pool: &SqlitePool, account_id: Uuid, message: &str) -> AppResult<()> {
    sqlx::query(
        r#"
        UPDATE accounts
        SET last_usage_error = $2,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE id = $1
        "#,
    )
    .bind(account_id)
    .bind(message)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_usage_windows(pool: &SqlitePool) -> AppResult<Vec<UsageWindow>> {
    sqlx::query_as::<_, UsageWindow>(
        r#"
        SELECT * FROM usage_windows
        ORDER BY account_id, quota_key, window_seconds ASC, window_kind
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn previous_usage_sample(
    pool: &SqlitePool,
    window: &UsageWindow,
) -> AppResult<Option<UsageSample>> {
    sqlx::query_as::<_, UsageSample>(
        r#"
        SELECT used_percent, reset_at, recorded_at
        FROM usage_samples
        WHERE account_id = $1
          AND quota_key = $2
          AND source_slot = $3
          AND julianday(recorded_at) <= julianday($4, '-15 minutes')
        ORDER BY recorded_at DESC, id DESC
        LIMIT 1
        "#,
    )
    .bind(window.account_id)
    .bind(&window.quota_key)
    .bind(&window.source_slot)
    .bind(window.fetched_at)
    .fetch_optional(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn active_accounts(pool: &SqlitePool) -> AppResult<Vec<Account>> {
    sqlx::query_as::<_, Account>(
        "SELECT * FROM accounts WHERE status = 'active' ORDER BY created_at ASC",
    )
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn prune_history(pool: &SqlitePool, retention_days: i64) -> AppResult<()> {
    let modifier = format!("-{} days", retention_days.clamp(1, 365));
    sqlx::query(
        "DELETE FROM usage_samples WHERE recorded_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', $1)",
    )
    .bind(&modifier)
    .execute(pool)
    .await?;
    sqlx::query(
        "DELETE FROM usage_snapshots WHERE recorded_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', $1)",
    )
    .bind(&modifier)
    .execute(pool)
    .await?;
    sqlx::query("DELETE FROM affinity WHERE julianday(last_used_at) < julianday('now', $1)")
        .bind(&modifier)
        .execute(pool)
        .await?;
    sqlx::query(
        "DELETE FROM request_logs WHERE created_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', $1)",
    )
    .bind(&modifier)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_settings(pool: &SqlitePool) -> AppResult<Vec<SettingRow>> {
    sqlx::query_as::<_, SettingRow>("SELECT * FROM settings ORDER BY key")
        .fetch_all(pool)
        .await
        .map_err(AppError::Database)
}

pub async fn runtime_settings(pool: &SqlitePool) -> AppResult<RuntimeSettings> {
    let rows = sqlx::query_as::<_, SettingRow>(
        r#"
        SELECT * FROM settings
        WHERE key IN (
            'routing_strategy', 'proxy_max_attempts', 'rate_limit_cooldown_seconds',
            'sticky_session_ttl_seconds', 'usage_sample_retention_days'
        )
        ORDER BY key
        "#,
    )
    .fetch_all(pool)
    .await?;

    let mut settings = RuntimeSettings::default();
    for row in rows {
        settings.apply(&row.key, &row.value);
    }
    Ok(settings)
}

pub async fn upsert_settings(
    pool: &SqlitePool,
    settings: serde_json::Map<String, Value>,
) -> AppResult<Vec<SettingRow>> {
    for (key, value) in &settings {
        validate_setting(key, value)?;
    }
    let mut tx = pool.begin().await?;
    for (key, value) in settings {
        sqlx::query(
            r#"
            INSERT INTO settings (key, value, updated_at)
            VALUES ($1, $2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            ON CONFLICT (key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            "#,
        )
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await?;
    }
    tx.commit().await?;
    list_settings(pool).await
}

fn validate_setting(key: &str, value: &Value) -> AppResult<()> {
    match key {
        "routing_strategy" => match value.as_str() {
            Some("round_robin" | "usage_weighted") => Ok(()),
            _ => Err(AppError::BadRequest(
                "routing_strategy must be round_robin or usage_weighted".to_string(),
            )),
        },
        "proxy_max_attempts" => validate_integer_setting(key, value, 1, 10),
        "rate_limit_cooldown_seconds" => validate_integer_setting(key, value, 1, 3_600),
        "sticky_session_ttl_seconds" => {
            validate_integer_setting(key, value, 300, 30 * 24 * 60 * 60)
        }
        "usage_sample_retention_days" => validate_integer_setting(key, value, 1, 365),
        _ => Err(AppError::BadRequest(format!(
            "unsupported runtime setting '{key}'"
        ))),
    }
}

fn validate_integer_setting(key: &str, value: &Value, min: i64, max: i64) -> AppResult<()> {
    let parsed = value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()));
    match parsed {
        Some(value) if (min..=max).contains(&value) => Ok(()),
        _ => Err(AppError::BadRequest(format!(
            "{key} must be an integer from {min} through {max}"
        ))),
    }
}

async fn ensure_runtime_row(pool: &SqlitePool, account_id: Uuid) -> AppResult<()> {
    sqlx::query(
        r#"
        INSERT INTO account_runtime_state (account_id)
        VALUES ($1)
        ON CONFLICT (account_id) DO NOTHING
        "#,
    )
    .bind(account_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn expand_home_in_sqlite_url(database_url: &str) -> AppResult<String> {
    let relative = database_url
        .strip_prefix("sqlite://~/")
        .or_else(|| database_url.strip_prefix("sqlite:~/"));
    let Some(relative) = relative else {
        return Ok(database_url.to_string());
    };

    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| AppError::Internal("HOME is not set; cannot expand SQLite path".into()))?;
    Ok(format!("sqlite://{}", home.join(relative).display()))
}

fn validate_status_opt(status: Option<&str>) -> AppResult<()> {
    if let Some(status) = status {
        match status {
            "active" | "rate_limited" | "quota_exceeded" | "paused" | "auth_failed" => Ok(()),
            other => Err(AppError::BadRequest(format!(
                "invalid account status '{other}'"
            ))),
        }
    } else {
        Ok(())
    }
}

pub fn normalize_label(label: &str) -> AppResult<String> {
    let value = label.trim().to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 32
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(AppError::BadRequest(
            "label must be 1-32 ASCII letters, numbers, '-' or '_'".to_string(),
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod session_route_action_tests {
    use std::path::PathBuf;

    use super::*;

    async fn route_test_pool() -> (SqlitePool, PathBuf) {
        let path =
            std::env::temp_dir().join(format!("codex-lb-session-route-{}.sqlite", Uuid::new_v4()));
        let pool = connect(&format!("sqlite://{}", path.display()))
            .await
            .expect("connect route test database");
        run_migrations(&pool).await.expect("run route migrations");
        (pool, path)
    }

    async fn insert_route_test_account(pool: &SqlitePool, label: &str, created_at: &str) -> Uuid {
        let account_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO accounts (
                id, label, email, plan_type,
                encrypted_access_token, encrypted_refresh_token, encrypted_id_token,
                created_at, updated_at
            ) VALUES ($1, $2, $3, 'plus', 'access', 'refresh', 'id', $4, $4)
            "#,
        )
        .bind(account_id)
        .bind(label)
        .bind(format!("{label}@example.invalid"))
        .bind(created_at)
        .execute(pool)
        .await
        .expect("insert route test account");
        sqlx::query("INSERT INTO account_runtime_state (account_id) VALUES ($1)")
            .bind(account_id)
            .execute(pool)
            .await
            .expect("insert route runtime row");
        account_id
    }

    fn route_key(kind: &str, value: &str) -> SessionRouteKey {
        SessionRouteKey {
            key_hash: affinity_hash(kind, value),
            kind: kind.to_string(),
        }
    }

    fn reroute_request(
        root: &SessionRouteKey,
        keys: Vec<SessionRouteKey>,
        target: &str,
        dry_run: bool,
    ) -> SessionRouteActionRequest {
        SessionRouteActionRequest {
            root_key_hash: root.key_hash.clone(),
            keys,
            action: SessionRouteActionKind::Reroute,
            target: Some(target.to_string()),
            dry_run,
        }
    }

    async fn close_route_test_pool(pool: SqlitePool, path: PathBuf) {
        pool.close().await;
        tokio::fs::remove_file(path)
            .await
            .expect("remove route test database");
    }

    #[tokio::test]
    async fn route_actions_are_atomic_durable_and_generation_guarded() {
        let (pool, path) = route_test_pool().await;
        let personal =
            insert_route_test_account(&pool, "personal", "2020-01-01T00:00:00.000Z").await;
        let work = insert_route_test_account(&pool, "work", "2021-01-01T00:00:00.000Z").await;
        let session = route_key("session_id", "route-action-session");
        bind_affinity(&pool, &session.key_hash, &session.kind, personal)
            .await
            .expect("seed legacy route");
        let old_epoch = resolve_session_route_epoch(&pool, std::slice::from_ref(&session))
            .await
            .expect("resolve route epoch")
            .expect("seeded route root");
        assert_eq!(old_epoch.route_generation, 0);

        let settings = RuntimeSettings::default();
        let applied = apply_session_route_action(
            &pool,
            &settings,
            Duration::from_secs(120),
            reroute_request(&session, vec![session.clone()], "work", false),
            &HashSet::new(),
        )
        .await
        .expect("apply reroute");
        assert_eq!(applied.status, SessionRouteActionStatus::Applied);
        assert_eq!(applied.route_generation, 1);
        assert_eq!(applied.previous_accounts, ["personal"]);
        assert_eq!(applied.effective_account.as_deref(), Some("work"));

        let persisted: (Uuid, i64) =
            sqlx::query_as("SELECT account_id, route_generation FROM affinity WHERE key_hash = $1")
                .bind(&session.key_hash)
                .fetch_one(&pool)
                .await
                .expect("read applied route");
        assert_eq!(persisted, (work, 1));
        assert!(
            !bind_affinities_at_epoch(
                &pool,
                std::slice::from_ref(&session),
                personal,
                Some(&old_epoch),
            )
            .await
            .expect("reject stale route bind")
        );

        let no_op = apply_session_route_action(
            &pool,
            &settings,
            Duration::from_secs(120),
            reroute_request(&session, vec![session.clone()], "work", false),
            &HashSet::new(),
        )
        .await
        .expect("repeat reroute");
        assert_eq!(no_op.status, SessionRouteActionStatus::NoOp);
        assert_eq!(no_op.route_generation, 1);

        let dry_run = apply_session_route_action(
            &pool,
            &settings,
            Duration::from_secs(120),
            SessionRouteActionRequest {
                root_key_hash: session.key_hash.clone(),
                keys: vec![session.clone()],
                action: SessionRouteActionKind::Rebalance,
                target: None,
                dry_run: true,
            },
            &HashSet::new(),
        )
        .await
        .expect("preview rebalance");
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.status, SessionRouteActionStatus::Applied);
        assert_eq!(dry_run.effective_account.as_deref(), Some("personal"));
        let after_dry_run: (Uuid, i64) =
            sqlx::query_as("SELECT account_id, route_generation FROM affinity WHERE key_hash = $1")
                .bind(&session.key_hash)
                .fetch_one(&pool)
                .await
                .expect("read route after dry run");
        assert_eq!(after_dry_run, (work, 1));

        sqlx::query("UPDATE accounts SET status = 'paused' WHERE id = $1")
            .bind(work)
            .execute(&pool)
            .await
            .expect("pause requested target");
        let fallback = apply_session_route_action(
            &pool,
            &settings,
            Duration::from_secs(120),
            reroute_request(&session, vec![session.clone()], "work", false),
            &HashSet::new(),
        )
        .await
        .expect("fallback reroute");
        assert_eq!(fallback.status, SessionRouteActionStatus::Fallback);
        assert_eq!(fallback.effective_account.as_deref(), Some("personal"));
        assert_eq!(fallback.route_generation, 2);

        sqlx::query("UPDATE accounts SET status = 'paused' WHERE id = $1")
            .bind(personal)
            .execute(&pool)
            .await
            .expect("pause fallback target");
        let pending = apply_session_route_action(
            &pool,
            &settings,
            Duration::from_secs(120),
            reroute_request(&session, vec![session.clone()], "work", false),
            &HashSet::new(),
        )
        .await
        .expect("persist pending reroute");
        assert_eq!(pending.status, SessionRouteActionStatus::Pending);
        assert_eq!(pending.route_generation, 2);
        let after_pending: (Uuid, i64) =
            sqlx::query_as("SELECT account_id, route_generation FROM affinity WHERE key_hash = $1")
                .bind(&session.key_hash)
                .fetch_one(&pool)
                .await
                .expect("read route after pending command");
        assert_eq!(after_pending, (personal, 2));
        let retry = list_pending_session_route_actions(&pool, 100)
            .await
            .expect("list pending reroutes");
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].target.as_deref(), Some(work.to_string().as_str()));

        close_route_test_pool(pool, path).await;
    }

    #[tokio::test]
    async fn route_actions_merge_roots_and_reject_orphans() {
        let (pool, path) = route_test_pool().await;
        let personal =
            insert_route_test_account(&pool, "personal", "2020-01-01T00:00:00.000Z").await;
        let work = insert_route_test_account(&pool, "work", "2021-01-01T00:00:00.000Z").await;
        let root = route_key("session_id", "merge-root");
        let alias = route_key("prompt_cache_key", "merge-alias");
        bind_affinity(&pool, &root.key_hash, &root.kind, personal)
            .await
            .expect("seed first route root");
        bind_affinity(&pool, &alias.key_hash, &alias.kind, work)
            .await
            .expect("seed second route root");

        let merged = apply_session_route_action(
            &pool,
            &RuntimeSettings::default(),
            Duration::from_secs(120),
            reroute_request(&root, vec![root.clone(), alias.clone()], "work", false),
            &HashSet::new(),
        )
        .await
        .expect("merge conflicting roots");
        assert_eq!(merged.status, SessionRouteActionStatus::Applied);
        assert_eq!(merged.merged_root_count, 1);
        assert_eq!(merged.linked_route_count, 2);
        let accounts = sqlx::query_scalar::<_, Uuid>(
            "SELECT account_id FROM affinity WHERE key_hash IN ($1, $2) ORDER BY key_hash",
        )
        .bind(&root.key_hash)
        .bind(&alias.key_hash)
        .fetch_all(&pool)
        .await
        .expect("read merged routes");
        assert_eq!(accounts, vec![work, work]);
        let current_epoch = resolve_session_route_epoch(&pool, std::slice::from_ref(&root))
            .await
            .expect("resolve merged epoch")
            .expect("merged root epoch");
        assert!(
            bind_affinities_at_epoch(
                &pool,
                std::slice::from_ref(&root),
                personal,
                Some(&current_epoch),
            )
            .await
            .expect("bind successful transport fallback")
        );
        let fallback_accounts = sqlx::query_scalar::<_, Uuid>(
            "SELECT account_id FROM affinity WHERE key_hash IN ($1, $2) ORDER BY key_hash",
        )
        .bind(&root.key_hash)
        .bind(&alias.key_hash)
        .fetch_all(&pool)
        .await
        .expect("read propagated fallback routes");
        assert_eq!(fallback_accounts, vec![personal, personal]);
        let fallback_state: (Option<Uuid>, String) = sqlx::query_as(
            "SELECT effective_account_id, action_status FROM session_roots WHERE root_key_hash = $1",
        )
        .bind(&root.key_hash)
        .fetch_one(&pool)
        .await
        .expect("read propagated fallback state");
        assert_eq!(fallback_state, (Some(personal), "fallback".to_string()));
        let roots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM session_roots")
            .fetch_one(&pool)
            .await
            .expect("count merged roots");
        assert_eq!(roots, 1);

        let orphan = route_key("session_id", "never-routed-session");
        let error = apply_session_route_action(
            &pool,
            &RuntimeSettings::default(),
            Duration::from_secs(120),
            reroute_request(&orphan, vec![orphan.clone()], "work", false),
            &HashSet::new(),
        )
        .await
        .expect_err("reject an unknown session fingerprint");
        assert!(matches!(error, AppError::NotFound(_)));
        let orphan_roots: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM session_roots WHERE root_key_hash = $1")
                .bind(&orphan.key_hash)
                .fetch_one(&pool)
                .await
                .expect("count rolled-back orphan roots");
        assert_eq!(orphan_roots, 0);

        close_route_test_pool(pool, path).await;
    }
}
