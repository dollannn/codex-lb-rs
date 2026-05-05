use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

use crate::{
    auth_file::{AuthClaims, AuthFile},
    crypto::TokenCrypto,
    error::{AppError, AppResult},
    models::{
        Account, AccountSummary, AccountTokens, LogsQuery, RequestLog, RuntimeSettings,
        SelectedAccount, SettingRow, UsageData, UsageSnapshot, UsageSummary,
    },
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn connect(database_url: &str) -> AppResult<PgPool> {
    PgPoolOptions::new()
        .max_connections(10)
        .connect(database_url)
        .await
        .map_err(AppError::Database)
}

pub async fn run_migrations(pool: &PgPool) -> AppResult<()> {
    MIGRATOR
        .run(pool)
        .await
        .map_err(|err| AppError::Internal(format!("migration failed: {err}")))
}

pub async fn upsert_account(
    pool: &PgPool,
    crypto: &TokenCrypto,
    auth: AuthFile,
    claims: AuthClaims,
) -> AppResult<Account> {
    let encrypted_access_token = crypto.encrypt(&auth.tokens.access_token)?;
    let encrypted_refresh_token = crypto.encrypt(&auth.tokens.refresh_token)?;
    let encrypted_id_token = crypto.encrypt(&auth.tokens.id_token)?;
    let account = sqlx::query_as::<_, Account>(
        r#"
        INSERT INTO accounts (
            id, chatgpt_account_id, email, plan_type,
            encrypted_access_token, encrypted_refresh_token, encrypted_id_token,
            last_refresh_at, status, status_reason
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8, now()), 'active', NULL)
        ON CONFLICT (chatgpt_account_id) WHERE chatgpt_account_id IS NOT NULL DO UPDATE SET
            email = EXCLUDED.email,
            plan_type = EXCLUDED.plan_type,
            encrypted_access_token = EXCLUDED.encrypted_access_token,
            encrypted_refresh_token = EXCLUDED.encrypted_refresh_token,
            encrypted_id_token = EXCLUDED.encrypted_id_token,
            last_refresh_at = EXCLUDED.last_refresh_at,
            status = 'active',
            status_reason = NULL,
            updated_at = now()
        RETURNING *
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(claims.chatgpt_account_id)
    .bind(claims.email)
    .bind(claims.plan_type)
    .bind(encrypted_access_token)
    .bind(encrypted_refresh_token)
    .bind(encrypted_id_token)
    .bind(auth.last_refresh_at)
    .fetch_one(pool)
    .await?;

    ensure_runtime_row(pool, account.id).await?;
    Ok(account)
}

pub async fn list_accounts(pool: &PgPool) -> AppResult<Vec<AccountSummary>> {
    sqlx::query_as::<_, AccountSummary>(
        r#"
        SELECT
            a.id,
            a.chatgpt_account_id,
            a.email,
            a.plan_type,
            a.status,
            a.status_reason,
            a.last_refresh_at,
            a.created_at,
            us.used_percent AS latest_used_percent,
            us.reset_at AS latest_reset_at,
            COALESCE(rl.request_count, 0)::bigint AS request_count,
            COALESCE(rl.input_tokens, 0)::bigint AS input_tokens,
            COALESCE(rl.output_tokens, 0)::bigint AS output_tokens
        FROM accounts a
        LEFT JOIN LATERAL (
            SELECT used_percent, reset_at
            FROM usage_snapshots
            WHERE account_id = a.id
            ORDER BY recorded_at DESC, id DESC
            LIMIT 1
        ) us ON true
        LEFT JOIN LATERAL (
            SELECT
                COUNT(*)::bigint AS request_count,
                COALESCE(SUM(input_tokens), 0)::bigint AS input_tokens,
                COALESCE(SUM(output_tokens), 0)::bigint AS output_tokens
            FROM request_logs
            WHERE account_id = a.id
        ) rl ON true
        ORDER BY a.created_at ASC
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(AppError::Database)
}

pub async fn get_account(pool: &PgPool, id: Uuid) -> AppResult<Option<Account>> {
    sqlx::query_as::<_, Account>("SELECT * FROM accounts WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(AppError::Database)
}

pub async fn update_account(
    pool: &PgPool,
    id: Uuid,
    status: Option<String>,
    email: Option<String>,
    plan_type: Option<String>,
) -> AppResult<Account> {
    validate_status_opt(status.as_deref())?;
    sqlx::query_as::<_, Account>(
        r#"
        UPDATE accounts SET
            status = COALESCE($2, status),
            email = COALESCE($3, email),
            plan_type = COALESCE($4, plan_type),
            status_reason = CASE WHEN $2 = 'active' THEN NULL ELSE status_reason END,
            updated_at = now()
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(status)
    .bind(email)
    .bind(plan_type)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("account {id} not found")))
}

pub async fn delete_account(pool: &PgPool, id: Uuid) -> AppResult<()> {
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
    pool: &PgPool,
    crypto: &TokenCrypto,
    id: Uuid,
    access_token: &str,
    refresh_token: &str,
    id_token: &str,
    chatgpt_account_id: Option<String>,
    email: Option<String>,
    plan_type: Option<String>,
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
            last_refresh_at = now(),
            status = 'active',
            status_reason = NULL,
            updated_at = now()
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .bind(crypto.encrypt(access_token)?)
    .bind(crypto.encrypt(refresh_token)?)
    .bind(crypto.encrypt(id_token)?)
    .bind(chatgpt_account_id)
    .bind(email)
    .bind(plan_type)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| AppError::NotFound(format!("account {id} not found")))
}

pub async fn select_account(pool: &PgPool, crypto: &TokenCrypto) -> AppResult<SelectedAccount> {
    ensure_runtime_rows(pool).await?;

    let mut tx = pool.begin().await?;
    let account = sqlx::query_as::<_, Account>(
        r#"
        WITH candidate AS (
            SELECT a.id
            FROM accounts a
            INNER JOIN account_runtime_state r ON r.account_id = a.id
            WHERE a.status = 'active'
              AND (r.cooldown_until IS NULL OR r.cooldown_until <= now())
            ORDER BY COALESCE(r.last_selected_at, 'epoch'::timestamptz) ASC, a.created_at ASC
            FOR UPDATE OF r SKIP LOCKED
            LIMIT 1
        ), updated AS (
            UPDATE account_runtime_state r
            SET last_selected_at = now(),
                cooldown_until = NULL,
                updated_at = now()
            FROM candidate
            WHERE r.account_id = candidate.id
            RETURNING r.account_id
        )
        SELECT a.*
        FROM accounts a
        INNER JOIN updated u ON u.account_id = a.id
        "#,
    )
    .fetch_optional(&mut *tx)
    .await?
    .ok_or_else(|| AppError::BadRequest("no active accounts available".to_string()))?;
    tx.commit().await?;

    let tokens = AccountTokens {
        access_token: crypto.decrypt(&account.encrypted_access_token)?,
        refresh_token: crypto.decrypt(&account.encrypted_refresh_token)?,
        id_token: crypto.decrypt(&account.encrypted_id_token)?,
    };
    Ok(SelectedAccount { account, tokens })
}

pub async fn account_count(pool: &PgPool) -> AppResult<i64> {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::bigint FROM accounts WHERE status = 'active'")
        .fetch_one(pool)
        .await
        .map_err(AppError::Database)
}

pub async fn cooldown_account(
    pool: &PgPool,
    id: Uuid,
    seconds: i64,
    reason: &str,
) -> AppResult<()> {
    ensure_runtime_row(pool, id).await?;
    sqlx::query(
        r#"
        UPDATE account_runtime_state
        SET cooldown_until = now() + ($2::text || ' seconds')::interval,
            failure_count = failure_count + 1,
            updated_at = now()
        WHERE account_id = $1
        "#,
    )
    .bind(id)
    .bind(seconds)
    .execute(pool)
    .await?;
    sqlx::query("UPDATE accounts SET status_reason = $2, updated_at = now() WHERE id = $1")
        .bind(id)
        .bind(reason)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn mark_auth_failed(pool: &PgPool, id: Uuid, reason: &str) -> AppResult<()> {
    sqlx::query(
        "UPDATE accounts SET status = 'auth_failed', status_reason = $2, updated_at = now() WHERE id = $1",
    )
    .bind(id)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn insert_request_log(
    pool: &PgPool,
    request_id: &str,
    account_id: Option<Uuid>,
    model: Option<&str>,
    status: &str,
    error_code: Option<&str>,
    error_message: Option<&str>,
    usage: UsageData,
    latency_ms: Option<i32>,
) -> AppResult<()> {
    sqlx::query(
        r#"
        INSERT INTO request_logs (
            request_id, account_id, model, status, error_code, error_message,
            input_tokens, output_tokens, cached_input_tokens, reasoning_tokens, latency_ms
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        "#,
    )
    .bind(request_id)
    .bind(account_id)
    .bind(model)
    .bind(status)
    .bind(error_code)
    .bind(error_message)
    .bind(usage.input_tokens)
    .bind(usage.output_tokens)
    .bind(usage.cached_input_tokens)
    .bind(usage.reasoning_tokens)
    .bind(latency_ms)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn list_request_logs(pool: &PgPool, query: LogsQuery) -> AppResult<Vec<RequestLog>> {
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

pub async fn usage_summary(pool: &PgPool) -> AppResult<UsageSummary> {
    sqlx::query_as::<_, (i64, i64, i64, i64, i64, i64, i64)>(
        r#"
        SELECT
            (SELECT COUNT(*)::bigint FROM accounts) AS account_count,
            (SELECT COUNT(*)::bigint FROM accounts WHERE status = 'active') AS active_account_count,
            COUNT(*)::bigint AS request_count,
            COUNT(*) FILTER (WHERE status = 'success')::bigint AS successful_request_count,
            COUNT(*) FILTER (WHERE status <> 'success')::bigint AS failed_request_count,
            COALESCE(SUM(input_tokens), 0)::bigint AS input_tokens,
            COALESCE(SUM(output_tokens), 0)::bigint AS output_tokens
        FROM request_logs
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
    pool: &PgPool,
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
    pool: &PgPool,
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

pub async fn list_settings(pool: &PgPool) -> AppResult<Vec<SettingRow>> {
    sqlx::query_as::<_, SettingRow>("SELECT * FROM settings ORDER BY key")
        .fetch_all(pool)
        .await
        .map_err(AppError::Database)
}

pub async fn runtime_settings(pool: &PgPool) -> AppResult<RuntimeSettings> {
    let rows = sqlx::query_as::<_, SettingRow>(
        r#"
        SELECT * FROM settings
        WHERE key IN ('routing_strategy', 'proxy_max_attempts', 'rate_limit_cooldown_seconds')
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
    pool: &PgPool,
    settings: serde_json::Map<String, Value>,
) -> AppResult<Vec<SettingRow>> {
    for (key, value) in settings {
        sqlx::query(
            r#"
            INSERT INTO settings (key, value, updated_at) VALUES ($1, $2, now())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
            "#,
        )
        .bind(key)
        .bind(value)
        .execute(pool)
        .await?;
    }
    list_settings(pool).await
}

async fn ensure_runtime_row(pool: &PgPool, account_id: Uuid) -> AppResult<()> {
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

async fn ensure_runtime_rows(pool: &PgPool) -> AppResult<()> {
    sqlx::query(
        r#"
        INSERT INTO account_runtime_state (account_id)
        SELECT id FROM accounts
        ON CONFLICT (account_id) DO NOTHING
        "#,
    )
    .execute(pool)
    .await?;
    Ok(())
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
