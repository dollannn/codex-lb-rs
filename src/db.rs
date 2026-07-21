use std::{
    collections::HashSet,
    env,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{
    QueryBuilder, Sqlite, SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
};
use uuid::Uuid;

use crate::{
    auth_file::{AuthClaims, AuthFile},
    crypto::TokenCrypto,
    error::{AppError, AppResult},
    models::{
        Account, AccountSummary, AccountTokens, LogsQuery, NewRequestLog, RequestLog,
        RuntimeSettings, SelectedAccount, SettingRow, UsageSample, UsageSnapshot, UsageSummary,
        UsageWindow,
    },
    usage::ParsedUsageWindow,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

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
            COALESCE(r.output_tokens, 0) AS output_tokens,
            r.last_selected_at,
            r.last_request_at,
            r.cooldown_until,
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
) -> AppResult<SelectedAccount> {
    if let Some((key_hash, _)) = affinity {
        let pinned = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT af.account_id
            FROM affinity af
            INNER JOIN accounts a ON a.id = af.account_id
            INNER JOIN account_runtime_state r ON r.account_id = a.id
            WHERE af.key_hash = $1
              AND a.status = 'active'
              AND julianday(af.last_used_at) >= julianday('now', printf('-%d seconds', $2))
              AND (r.cooldown_until IS NULL OR julianday(r.cooldown_until) <= julianday('now'))
              AND NOT EXISTS (
                  SELECT 1 FROM usage_windows exhausted
                  WHERE exhausted.account_id = a.id
                    AND exhausted.quota_key = 'codex'
                    AND exhausted.used_percent >= 100
                    AND (exhausted.reset_at IS NULL OR julianday(exhausted.reset_at) > julianday('now'))
              )
            "#,
        )
        .bind(key_hash)
        .bind(settings.sticky_session_ttl_seconds)
        .fetch_optional(pool)
        .await?;
        if let Some(account_id) = pinned.filter(|id| !excluded.contains(id)) {
            mark_selected(pool, account_id).await?;
            return match selected_account(pool, crypto, account_id).await {
                Ok(selected) => Ok(selected),
                Err(error) => {
                    release_account(pool, account_id).await.ok();
                    Err(error)
                }
            };
        }
    }

    let mut tx = pool.begin().await?;
    let selected_at = Utc::now().format("%Y-%m-%dT%H:%M:%S%.9fZ").to_string();
    let mut query = QueryBuilder::<Sqlite>::new(
        r#"
        UPDATE account_runtime_state
        SET last_selected_at = "#,
    );
    query.push_bind(selected_at);
    query.push(
        r#",
            cooldown_until = NULL,
            inflight_count = inflight_count + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE account_id = (
            SELECT a.id
            FROM accounts a
            INNER JOIN account_runtime_state r ON r.account_id = a.id
            WHERE a.status = 'active'
              AND (r.cooldown_until IS NULL OR julianday(r.cooldown_until) <= julianday('now'))
              AND NOT EXISTS (
                  SELECT 1 FROM usage_windows exhausted
                  WHERE exhausted.account_id = a.id
                    AND exhausted.quota_key = 'codex'
                    AND exhausted.used_percent >= 100
                    AND (exhausted.reset_at IS NULL OR julianday(exhausted.reset_at) > julianday('now'))
              )
        "#,
    );
    if !excluded.is_empty() {
        query.push(" AND a.id NOT IN (");
        let mut separated = query.separated(", ");
        for account_id in excluded {
            separated.push_bind(account_id);
        }
        separated.push_unseparated(")");
    }
    query.push(" ORDER BY ");
    if settings.routing_strategy != "round_robin" {
        query.push(
            r#"r.inflight_count ASC,
                COALESCE((
                    SELECT window.used_percent
                    FROM usage_windows window
                    WHERE window.account_id = a.id AND window.quota_key = 'codex'
                    ORDER BY COALESCE(window.window_seconds, 0) DESC
                    LIMIT 1
                ), 0) ASC,
                COALESCE((
                    SELECT window.used_percent
                    FROM usage_windows window
                    WHERE window.account_id = a.id AND window.quota_key = 'codex'
                    ORDER BY COALESCE(window.window_seconds, 0) ASC
                    LIMIT 1
                ), 0) ASC,
            "#,
        );
    }
    query.push(
        r#"COALESCE(r.last_selected_at, '1970-01-01T00:00:00.000Z') ASC,
                 a.created_at ASC,
                 a.rowid ASC
            LIMIT 1
        )
        RETURNING account_id
        "#,
    );
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

    match selected_account(pool, crypto, account_id).await {
        Ok(selected) => Ok(selected),
        Err(error) => {
            release_account(pool, account_id).await.ok();
            Err(error)
        }
    }
}

async fn selected_account(
    pool: &SqlitePool,
    crypto: &TokenCrypto,
    account_id: Uuid,
) -> AppResult<SelectedAccount> {
    let account = get_account(pool, account_id)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("account {account_id} not found")))?;
    let tokens = AccountTokens {
        access_token: crypto.decrypt(&account.encrypted_access_token)?,
        refresh_token: crypto.decrypt(&account.encrypted_refresh_token)?,
        id_token: crypto.decrypt(&account.encrypted_id_token)?,
    };
    Ok(SelectedAccount { account, tokens })
}

async fn mark_selected(pool: &SqlitePool, account_id: Uuid) -> AppResult<()> {
    sqlx::query(
        r#"
        UPDATE account_runtime_state
        SET last_selected_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            inflight_count = inflight_count + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE account_id = $1
        "#,
    )
    .bind(account_id)
    .execute(pool)
    .await?;
    Ok(())
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
                AND (exhausted.reset_at IS NULL OR julianday(exhausted.reset_at) > julianday('now'))
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

pub async fn bind_affinity(
    pool: &SqlitePool,
    key_hash: &str,
    kind: &str,
    account_id: Uuid,
) -> AppResult<()> {
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
    .bind(key_hash)
    .bind(kind)
    .bind(account_id)
    .execute(pool)
    .await?;
    Ok(())
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
            failure_count = failure_count + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE account_id = $1
        "#,
    )
    .bind(id)
    .bind(seconds)
    .execute(pool)
    .await?;
    sqlx::query(
        "UPDATE accounts SET status_reason = $2, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = $1",
    )
    .bind(id)
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
    sqlx::query(
        r#"
        INSERT INTO request_logs (
            request_id, account_id, model, status, error_code, error_message,
            input_tokens, output_tokens, cached_input_tokens, reasoning_tokens, latency_ms
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        "#,
    )
    .bind(entry.request_id)
    .bind(entry.account_id)
    .bind(entry.model)
    .bind(entry.status)
    .bind(entry.error_code)
    .bind(entry.error_message)
    .bind(input_tokens)
    .bind(output_tokens)
    .bind(entry.usage.cached_input_tokens)
    .bind(entry.usage.reasoning_tokens)
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
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE account_id = $1
            "#,
        )
        .bind(account_id)
        .bind(entry.status)
        .bind(input_tokens)
        .bind(output_tokens)
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
