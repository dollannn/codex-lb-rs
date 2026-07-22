use std::collections::HashMap;

use axum::{Json, Router, extract::State, routing::get};
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    db,
    error::AppResult,
    models::{AccountSummary, UsageWindow},
    state::AppState,
    usage::{PaceMetrics, ParsedUsageWindow, pace_metrics},
};

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolStatus {
    pub service: &'static str,
    pub version: &'static str,
    pub healthy: bool,
    pub degraded: bool,
    pub routing_strategy: String,
    pub account_count: usize,
    pub active_account_count: usize,
    pub available_account_count: usize,
    pub inflight_requests: i64,
    pub selected_account: Option<String>,
    pub accounts: Vec<AccountStatus>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountStatus {
    pub id: Uuid,
    pub label: String,
    pub plan: String,
    pub status: String,
    pub available: bool,
    pub selected: bool,
    pub status_reason: Option<String>,
    pub auth_expires_at: Option<DateTime<Utc>>,
    pub last_usage_refresh_at: Option<DateTime<Utc>>,
    pub last_usage_error: Option<String>,
    pub last_selected_at: Option<DateTime<Utc>>,
    pub last_request_at: Option<DateTime<Utc>>,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub inflight_requests: i64,
    pub request_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub quotas: Vec<WindowStatus>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WindowStatus {
    pub quota_key: String,
    pub quota_name: String,
    pub window: String,
    pub used_percent: f64,
    pub remaining_percent: f64,
    pub window_seconds: Option<i64>,
    pub reset_at: Option<DateTime<Utc>>,
    pub fetched_at: DateTime<Utc>,
    pub pace: PaceMetrics,
}

#[derive(Debug, Serialize)]
pub struct WaybarStatus {
    pub text: String,
    pub tooltip: String,
    pub class: Vec<String>,
    pub percentage: i32,
    pub alt: String,
}

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/v1/status", get(status_json))
        .route("/api/v1/status/waybar", get(waybar_json))
}

async fn status_json(State(state): State<AppState>) -> AppResult<Json<PoolStatus>> {
    Ok(Json(build_status(&state).await?))
}

async fn waybar_json(State(state): State<AppState>) -> AppResult<Json<WaybarStatus>> {
    Ok(Json(format_waybar(&build_status(&state).await?)))
}

pub async fn build_status(state: &AppState) -> AppResult<PoolStatus> {
    let now = Utc::now();
    let settings = db::runtime_settings(&state.pool).await?;
    let accounts = db::list_accounts(&state.pool).await?;
    let windows = db::list_usage_windows(&state.pool).await?;
    let mut by_account: HashMap<Uuid, Vec<UsageWindow>> = HashMap::new();
    for window in windows {
        by_account
            .entry(window.account_id)
            .or_default()
            .push(window);
    }

    let selected_id = accounts
        .iter()
        .filter_map(|account| account.last_selected_at.map(|time| (time, account.id)))
        .max_by_key(|(time, _)| *time)
        .map(|(_, id)| id);
    let mut account_statuses = Vec::with_capacity(accounts.len());
    for (index, account) in accounts.iter().enumerate() {
        let mut quotas = Vec::new();
        for window in by_account.remove(&account.id).unwrap_or_default() {
            quotas.push(window_status(state, window, now).await?);
        }
        quotas.sort_by(|left, right| {
            left.quota_key
                .cmp(&right.quota_key)
                .then_with(|| left.window_seconds.cmp(&right.window_seconds))
        });
        account_statuses.push(to_account_status(
            account,
            index,
            quotas,
            now,
            selected_id == Some(account.id),
        ));
    }

    let stale_after = chrono::Duration::from_std(state.config.usage_refresh_interval * 4)
        .unwrap_or_else(|_| chrono::Duration::minutes(10));
    let available_account_count = account_statuses
        .iter()
        .filter(|account| account.available)
        .count();
    let healthy = available_account_count > 0;
    let degraded = account_statuses.is_empty()
        || available_account_count < account_statuses.len()
        || account_statuses.iter().any(|account| {
            account.last_usage_error.is_some()
                || account
                    .last_usage_refresh_at
                    .is_none_or(|refreshed| now - refreshed > stale_after)
        });
    let selected_account = selected_id.and_then(|id| {
        account_statuses
            .iter()
            .find(|account| account.id == id)
            .map(|account| account.label.clone())
    });

    Ok(PoolStatus {
        service: "codex-lb-rs",
        version: env!("CARGO_PKG_VERSION"),
        healthy,
        degraded,
        routing_strategy: settings.routing_strategy,
        account_count: account_statuses.len(),
        active_account_count: account_statuses
            .iter()
            .filter(|account| account.status == "active")
            .count(),
        available_account_count,
        inflight_requests: account_statuses
            .iter()
            .map(|account| account.inflight_requests)
            .sum(),
        selected_account,
        accounts: account_statuses,
        generated_at: now,
    })
}

async fn window_status(
    state: &AppState,
    window: UsageWindow,
    now: DateTime<Utc>,
) -> AppResult<WindowStatus> {
    let previous = db::previous_usage_sample(&state.pool, &window)
        .await?
        .map(|sample| (sample.used_percent, sample.recorded_at, sample.reset_at));
    let parsed = ParsedUsageWindow {
        quota_key: window.quota_key.clone(),
        quota_name: window.quota_name.clone(),
        source_slot: window.source_slot.clone(),
        window_kind: window.window_kind.clone(),
        used_percent: window.used_percent,
        window_seconds: window.window_seconds,
        reset_at: window.reset_at,
    };
    Ok(WindowStatus {
        quota_key: window.quota_key,
        quota_name: window.quota_name,
        window: window.window_kind,
        used_percent: window.used_percent,
        remaining_percent: (100.0 - window.used_percent).max(0.0),
        window_seconds: window.window_seconds,
        reset_at: window.reset_at,
        fetched_at: window.fetched_at,
        pace: pace_metrics(&parsed, window.fetched_at, previous, now),
    })
}

fn to_account_status(
    account: &AccountSummary,
    index: usize,
    quotas: Vec<WindowStatus>,
    now: DateTime<Utc>,
    selected: bool,
) -> AccountStatus {
    let exhausted = quotas.iter().any(|window| {
        window.quota_key == "codex"
            && window.used_percent >= 100.0
            && window.reset_at.is_none_or(|reset_at| reset_at > now)
    });
    let available = account.status == "active"
        && account
            .cooldown_until
            .is_none_or(|cooldown_until| cooldown_until <= now)
        && !exhausted;
    let status_reason = if account.status != "active" {
        account.status_reason.clone()
    } else if account
        .cooldown_until
        .is_some_and(|cooldown_until| cooldown_until > now)
    {
        account.cooldown_reason.clone()
    } else {
        None
    };
    AccountStatus {
        id: account.id,
        label: if account.label.is_empty() {
            format!("account-{}", index + 1)
        } else {
            account.label.clone()
        },
        plan: account.plan_type.clone(),
        status: account.status.clone(),
        available,
        selected,
        status_reason,
        auth_expires_at: account.access_token_expires_at,
        last_usage_refresh_at: account.last_usage_refresh_at,
        last_usage_error: account.last_usage_error.clone(),
        last_selected_at: account.last_selected_at,
        last_request_at: account.last_request_at,
        cooldown_until: account.cooldown_until,
        inflight_requests: account.inflight_count,
        request_count: account.request_count,
        input_tokens: account.input_tokens,
        output_tokens: account.output_tokens,
        quotas,
    }
}

pub fn format_waybar(status: &PoolStatus) -> WaybarStatus {
    if status.accounts.is_empty() {
        return WaybarStatus {
            text: "󰬫 no accounts".to_string(),
            tooltip: "Codex pool is running, but no accounts are configured.".to_string(),
            class: vec!["codex-pool".to_string(), "degraded".to_string()],
            percentage: 0,
            alt: "empty".to_string(),
        };
    }

    let mut hottest = 0.0_f64;
    let mut pace_hot = false;
    let text_accounts = status
        .accounts
        .iter()
        .map(|account| {
            for window in account
                .quotas
                .iter()
                .filter(|window| window.quota_key == "codex")
            {
                hottest = hottest.max(window.used_percent);
                pace_hot |= matches!(window.pace.risk, "critical" | "danger");
            }
            let selected = if account.selected { "●" } else { "" };
            let label = short_label(&account.label);
            format!(
                "{selected}{label}:{}",
                compact_account_state(account, status.generated_at)
            )
        })
        .collect::<Vec<_>>()
        .join(" · ");

    let mut tooltip = format!(
        "CODEX POOL  •  {}/{} READY\n{}  •  cached locally",
        status.available_account_count,
        status.account_count,
        status.routing_strategy.replace('_', " ")
    );
    for account in &status.accounts {
        let selected = if account.selected { "●" } else { "○" };
        let state = account_display_state(account, status.generated_at);
        tooltip.push_str(&format!(
            "\n\n┌ {selected} {}  •  {}  •  {state}",
            account.label.to_ascii_uppercase(),
            account.plan.to_ascii_uppercase(),
        ));
        if account.status == "auth_failed" {
            tooltip.push_str("\n│ Device login required");
        } else if let Some(reason) = &account.status_reason {
            tooltip.push_str(&format!("\n│ {}", one_line(reason, 88)));
        }
        if account.quotas.is_empty() {
            tooltip.push_str("\n│ No fresh quota data");
        }
        for window in account
            .quotas
            .iter()
            .filter(|window| window.quota_key == "codex")
        {
            tooltip.push_str(&format!(
                "\n├ {}  •  {}\n│ {}  {:>3.0}% used  •  {:>3.0}% free\n│ at pace  {}  •  {}\n│ reset {}",
                window.quota_name,
                window.window,
                usage_bar(window.used_percent),
                window.used_percent,
                window.remaining_percent,
                exhaustion_forecast(window, status.generated_at),
                window.pace.risk.to_ascii_uppercase(),
                reset_eta(window.reset_at, status.generated_at),
            ));
        }
        if account.status != "auth_failed"
            && let Some(error) = &account.last_usage_error
        {
            tooltip.push_str(&format!("\n│ Usage: {}", one_line(error, 88)));
        }
        tooltip.push_str(&format!(
            "\n└ Activity  {} requests  •  {} input / {} output tokens",
            compact_count(account.request_count),
            compact_count(account.input_tokens),
            compact_count(account.output_tokens),
        ));
    }

    let mut class = vec!["codex-pool".to_string()];
    class.push(if !status.healthy || status.degraded {
        "degraded".to_string()
    } else {
        "healthy".to_string()
    });
    if pace_hot {
        class.push("pace-hot".to_string());
    }
    if hottest >= 90.0 {
        class.push("pool-low".to_string());
    }
    WaybarStatus {
        text: format!("󰬫 {text_accounts}"),
        tooltip,
        class,
        percentage: hottest.round() as i32,
        alt: if !status.healthy {
            "unavailable"
        } else if status.degraded {
            "degraded"
        } else {
            "healthy"
        }
        .to_string(),
    }
}

fn display_window(account: &AccountStatus) -> Option<&WindowStatus> {
    account
        .quotas
        .iter()
        .filter(|window| window.quota_key == "codex")
        .max_by(|left, right| {
            left.used_percent
                .total_cmp(&right.used_percent)
                .then_with(|| {
                    left.pace
                        .pace_ratio
                        .unwrap_or(0.0)
                        .total_cmp(&right.pace.pace_ratio.unwrap_or(0.0))
                })
        })
}

fn compact_account_state(account: &AccountStatus, now: DateTime<Utc>) -> String {
    if account.status == "auth_failed" {
        return "login".to_string();
    }
    if account.status == "paused" {
        return "pause".to_string();
    }
    if account.cooldown_until.is_some_and(|until| until > now) {
        return "wait".to_string();
    }

    match display_window(account) {
        Some(window) if window.used_percent >= 100.0 => "100%".to_string(),
        Some(window) if account.available => {
            format!("{:.0}%{}", window.used_percent, pace_arrow(window))
        }
        Some(_) => "!".to_string(),
        None if account.available => "—".to_string(),
        None => "!".to_string(),
    }
}

fn short_label(label: &str) -> String {
    label
        .chars()
        .find(|character| character.is_ascii_alphanumeric())
        .map(|character| character.to_ascii_uppercase().to_string())
        .unwrap_or_else(|| "?".to_string())
}

fn pace_arrow(window: &WindowStatus) -> &'static str {
    match window.pace.risk {
        "critical" | "danger" => "↑",
        "warning" => "↗",
        "safe" => "→",
        _ => "?",
    }
}

fn usage_bar(used_percent: f64) -> String {
    let filled = (used_percent.clamp(0.0, 100.0) / 10.0).round() as usize;
    format!("{}{}", "█".repeat(filled), "░".repeat(10 - filled))
}

fn account_display_state(account: &AccountStatus, now: DateTime<Utc>) -> &'static str {
    if account.status == "auth_failed" {
        "SIGN IN"
    } else if account.status == "paused" {
        "PAUSED"
    } else if account.cooldown_until.is_some_and(|until| until > now) {
        "COOLDOWN"
    } else if !account.available {
        "UNAVAILABLE"
    } else if account.quotas.is_empty() {
        "NO DATA"
    } else if account.last_usage_error.is_some() {
        "STALE"
    } else {
        "READY"
    }
}

fn exhaustion_forecast(window: &WindowStatus, now: DateTime<Utc>) -> String {
    if window.reset_at.is_some_and(|reset_at| reset_at <= now) {
        return "ETA stale".to_string();
    }
    if window.used_percent >= 100.0 {
        return "empty now".to_string();
    }
    let Some(projected) = window.pace.projected_exhaustion_at else {
        return "empty ETA —".to_string();
    };
    if window
        .reset_at
        .is_some_and(|reset_at| projected >= reset_at)
    {
        return "survives to reset".to_string();
    }
    let seconds = (projected - now).num_seconds();
    if seconds <= 0 {
        return "empty ETA due".to_string();
    }
    format!("empty in ~{}", human_duration(seconds))
}

fn reset_eta(reset_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> String {
    let Some(seconds) = reset_at.map(|reset| (reset - now).num_seconds()) else {
        return "unknown".to_string();
    };
    if seconds <= 0 {
        return "due; refresh pending".to_string();
    }
    format!("in {}", human_duration(seconds))
}

fn human_duration(seconds: i64) -> String {
    if seconds < 60 {
        return "<1m".to_string();
    }
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    if days > 0 {
        format!("{days}d {hours}h")
    } else if hours > 0 {
        format!("{hours}h {minutes}m")
    } else {
        format!("{minutes}m")
    }
}

fn compact_count(value: i64) -> String {
    const UNITS: [&str; 7] = ["", "k", "M", "B", "T", "P", "E"];
    let sign = if value < 0 { "-" } else { "" };
    let magnitude = value.unsigned_abs() as f64;
    let mut unit = 0;
    let mut scaled = magnitude;
    while scaled >= 1_000.0 && unit < UNITS.len() - 1 {
        scaled /= 1_000.0;
        unit += 1;
    }
    if unit == 0 {
        return value.to_string();
    }

    let precision = usize::from(scaled < 100.0);
    let factor = if precision == 0 { 1.0 } else { 10.0 };
    let mut rounded = (scaled * factor).round() / factor;
    if rounded >= 1_000.0 && unit < UNITS.len() - 1 {
        rounded /= 1_000.0;
        unit += 1;
    }
    let number = if rounded.fract() == 0.0 {
        format!("{rounded:.0}")
    } else {
        format!("{rounded:.1}")
    };
    format!("{sign}{number}{}", UNITS[unit])
}

fn one_line(value: &str, limit: usize) -> String {
    let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if value.chars().count() <= limit {
        value
    } else {
        format!("{}…", value.chars().take(limit).collect::<String>())
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use crate::{models::AccountSummary, usage::PaceMetrics};

    use super::{
        AccountStatus, PoolStatus, WindowStatus, compact_count, exhaustion_forecast, format_waybar,
        human_duration, reset_eta, to_account_status,
    };

    #[test]
    fn empty_pool_has_actionable_waybar_state() {
        let output = format_waybar(&PoolStatus {
            service: "codex-lb-rs",
            version: "test",
            healthy: false,
            degraded: true,
            routing_strategy: "usage_weighted".to_string(),
            account_count: 0,
            active_account_count: 0,
            available_account_count: 0,
            inflight_requests: 0,
            selected_account: None,
            accounts: vec![],
            generated_at: Utc::now(),
        });
        assert_eq!(output.alt, "empty");
        assert!(output.text.contains("no accounts"));
    }

    #[test]
    fn waybar_only_renders_core_codex_quota() {
        let now = Utc::now();
        let output = format_waybar(&PoolStatus {
            service: "codex-lb-rs",
            version: "test",
            healthy: true,
            degraded: false,
            routing_strategy: "usage_weighted".to_string(),
            account_count: 1,
            active_account_count: 1,
            available_account_count: 1,
            inflight_requests: 0,
            selected_account: Some("account-a".to_string()),
            accounts: vec![AccountStatus {
                id: Uuid::nil(),
                label: "account-a".to_string(),
                plan: "pro".to_string(),
                status: "active".to_string(),
                available: true,
                selected: true,
                status_reason: None,
                auth_expires_at: None,
                last_usage_refresh_at: Some(now),
                last_usage_error: None,
                last_selected_at: Some(now),
                last_request_at: Some(now),
                cooldown_until: None,
                inflight_requests: 0,
                // Intentionally synthetic values; do not copy live account telemetry here.
                request_count: 12_345,
                input_tokens: 1_234_567_890,
                output_tokens: 5_678_901,
                quotas: vec![
                    quota("codex", "Codex", 42.0, "safe", Some(0.5)),
                    quota(
                        "codex_bengalfox",
                        "GPT-5.3-Codex-Spark",
                        99.0,
                        "danger",
                        Some(10.0),
                    ),
                ],
            }],
            generated_at: now,
        });

        assert!(output.tooltip.contains("Codex  •  7d"));
        assert!(!output.tooltip.contains("GPT-5.3-Codex-Spark"));
        assert!(
            output
                .tooltip
                .contains("Activity  12.3k requests  •  1.2B input / 5.7M output tokens")
        );
        assert_eq!(output.percentage, 42);
        assert!(!output.class.iter().any(|class| class == "pace-hot"));
        assert!(!output.class.iter().any(|class| class == "pool-low"));
    }

    #[test]
    fn compact_counts_use_decimal_units_and_promote_after_rounding() {
        let cases = [
            (0, "0"),
            (999, "999"),
            (1_000, "1k"),
            (1_250, "1.3k"),
            (12_345, "12.3k"),
            (999_499, "999k"),
            (999_500, "1M"),
            (5_678_901, "5.7M"),
            (1_234_567_890, "1.2B"),
            (i64::MAX, "9.2E"),
        ];
        for (value, expected) in cases {
            assert_eq!(compact_count(value), expected, "value={value}");
        }
    }

    #[test]
    fn exhaustion_forecast_explains_survival_instead_of_pace_ratio() {
        let now = Utc::now();
        let mut window = quota("codex", "Codex", 50.0, "danger", Some(113.0));
        window.reset_at = Some(now + Duration::days(2));
        window.pace.projected_exhaustion_at = Some(now + Duration::hours(15));
        assert_eq!(exhaustion_forecast(&window, now), "empty in ~15h 0m");

        window.pace.projected_exhaustion_at = window.reset_at;
        assert_eq!(exhaustion_forecast(&window, now), "survives to reset");

        window.pace.projected_exhaustion_at = Some(now - Duration::minutes(1));
        assert_eq!(exhaustion_forecast(&window, now), "empty ETA due");

        window.used_percent = 100.0;
        assert_eq!(exhaustion_forecast(&window, now), "empty now");

        window.reset_at = Some(now);
        assert_eq!(exhaustion_forecast(&window, now), "ETA stale");
    }

    #[test]
    fn relative_times_are_clear_at_boundaries() {
        let now = Utc::now();
        assert_eq!(human_duration(59), "<1m");
        assert_eq!(human_duration(60), "1m");
        assert_eq!(human_duration(3_600), "1h 0m");
        assert_eq!(human_duration(90_000), "1d 1h");
        assert_eq!(reset_eta(None, now), "unknown");
        assert_eq!(reset_eta(Some(now), now), "due; refresh pending");
        assert_eq!(reset_eta(Some(now + Duration::minutes(5)), now), "in 5m");
    }

    #[test]
    fn temporary_reason_is_visible_only_during_cooldown() {
        let now = Utc::now();
        let mut account = account_summary(now);
        account.status_reason = Some("legacy stale reason".to_string());
        account.cooldown_reason = Some("transient upstream error".to_string());
        account.cooldown_until = Some(now + Duration::seconds(10));

        let cooling = to_account_status(&account, 0, vec![], now, false);
        assert_eq!(
            cooling.status_reason.as_deref(),
            Some("transient upstream error")
        );

        account.cooldown_until = Some(now - Duration::seconds(1));
        let recovered = to_account_status(&account, 0, vec![], now, false);
        assert_eq!(recovered.status_reason, None);

        account.status = "auth_failed".to_string();
        account.status_reason = Some("device login required".to_string());
        let persistent = to_account_status(&account, 0, vec![], now, false);
        assert_eq!(
            persistent.status_reason.as_deref(),
            Some("device login required")
        );
    }

    fn quota(
        quota_key: &str,
        quota_name: &str,
        used_percent: f64,
        risk: &'static str,
        pace_ratio: Option<f64>,
    ) -> WindowStatus {
        WindowStatus {
            quota_key: quota_key.to_string(),
            quota_name: quota_name.to_string(),
            window: "7d".to_string(),
            used_percent,
            remaining_percent: 100.0 - used_percent,
            window_seconds: Some(604_800),
            reset_at: None,
            fetched_at: Utc::now(),
            pace: PaceMetrics {
                observed_percent_per_hour: None,
                sustainable_percent_per_hour: None,
                pace_ratio,
                headroom_percent: None,
                projected_exhaustion_at: None,
                risk,
            },
        }
    }

    fn account_summary(now: chrono::DateTime<Utc>) -> AccountSummary {
        AccountSummary {
            id: Uuid::nil(),
            chatgpt_account_id: None,
            label: "account-a".to_string(),
            email: "account-a@example.com".to_string(),
            plan_type: "pro".to_string(),
            status: "active".to_string(),
            status_reason: None,
            last_refresh_at: now,
            access_token_expires_at: None,
            last_usage_refresh_at: Some(now),
            last_usage_error: None,
            created_at: now,
            latest_used_percent: None,
            latest_reset_at: None,
            request_count: 0,
            input_tokens: 0,
            output_tokens: 0,
            last_selected_at: None,
            last_request_at: None,
            cooldown_until: None,
            cooldown_reason: None,
            inflight_count: 0,
        }
    }
}
