use std::collections::HashSet;

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedUsageWindow {
    pub quota_key: String,
    pub quota_name: String,
    pub source_slot: String,
    pub window_kind: String,
    pub used_percent: f64,
    pub window_seconds: Option<i64>,
    pub reset_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaceMetrics {
    pub observed_percent_per_hour: Option<f64>,
    pub sustainable_percent_per_hour: Option<f64>,
    pub pace_ratio: Option<f64>,
    pub headroom_percent: Option<f64>,
    pub projected_exhaustion_at: Option<DateTime<Utc>>,
    pub risk: &'static str,
}

pub fn parse_usage_windows(raw: &Value, now: DateTime<Utc>) -> Vec<ParsedUsageWindow> {
    let mut windows = Vec::new();
    append_rate_limit(&mut windows, "codex", "Codex", raw.get("rate_limit"), now);
    append_rate_limit(
        &mut windows,
        "code_review",
        "Code review",
        raw.get("code_review_rate_limit"),
        now,
    );

    for (index, additional) in raw
        .get("additional_rate_limits")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .enumerate()
    {
        let quota_name = additional
            .get("limit_name")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("Additional quota");
        let quota_key = additional
            .get("metered_feature")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(normalize_key)
            .unwrap_or_else(|| format!("additional_{index}"));
        append_rate_limit(
            &mut windows,
            &quota_key,
            quota_name,
            additional.get("rate_limit"),
            now,
        );
    }
    let mut seen = HashSet::with_capacity(windows.len());
    windows.retain(|window| seen.insert((window.quota_key.clone(), window.source_slot.clone())));
    windows
}

fn append_rate_limit(
    target: &mut Vec<ParsedUsageWindow>,
    quota_key: &str,
    quota_name: &str,
    rate_limit: Option<&Value>,
    now: DateTime<Utc>,
) {
    let Some(rate_limit) = rate_limit.filter(|value| value.is_object()) else {
        return;
    };
    for kind in ["primary", "secondary"] {
        let Some(window) = rate_limit.get(format!("{kind}_window")) else {
            continue;
        };
        let Some(used_percent) = number(window.get("used_percent")) else {
            continue;
        };
        let window_seconds = integer(window.get("limit_window_seconds"));
        let reset_at = integer(window.get("reset_at"))
            .and_then(|epoch| DateTime::<Utc>::from_timestamp(epoch, 0))
            .or_else(|| {
                integer(window.get("reset_after_seconds"))
                    .map(|seconds| now + Duration::seconds(seconds.max(0)))
            });
        target.push(ParsedUsageWindow {
            quota_key: quota_key.to_string(),
            quota_name: quota_name.to_string(),
            source_slot: kind.to_string(),
            window_kind: window_kind(kind, window_seconds),
            used_percent: used_percent.clamp(0.0, 100.0),
            window_seconds,
            reset_at,
        });
    }
}

fn window_kind(fallback: &str, seconds: Option<i64>) -> String {
    match seconds {
        Some(18_000) => "5h".to_string(),
        Some(604_800) => "7d".to_string(),
        Some(value) if value > 0 && value % 86_400 == 0 => format!("{}d", value / 86_400),
        Some(value) if value > 0 && value % 3_600 == 0 => format!("{}h", value / 3_600),
        _ => fallback.to_string(),
    }
}

fn normalize_key(value: &str) -> String {
    value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn integer(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(value) => value.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

pub fn pace_metrics(
    current: &ParsedUsageWindow,
    fetched_at: DateTime<Utc>,
    previous: Option<(f64, DateTime<Utc>, Option<DateTime<Utc>>)>,
    now: DateTime<Utc>,
) -> PaceMetrics {
    let remaining = (100.0 - current.used_percent).max(0.0);
    let remaining_hours = current
        .reset_at
        .map(|reset| (reset - now).num_seconds().max(0) as f64 / 3_600.0)
        .filter(|hours| *hours > 0.0);
    let sustainable = remaining_hours.map(|hours| remaining / hours);

    let observed = previous.and_then(|(used, recorded_at, reset_at)| {
        if !same_reset_window(reset_at, current.reset_at) || current.used_percent < used {
            return None;
        }
        let elapsed_hours = (fetched_at - recorded_at).num_milliseconds() as f64 / 3_600_000.0;
        (elapsed_hours >= 0.25)
            .then(|| (current.used_percent - used) / elapsed_hours)
            .filter(|rate| rate.is_finite() && *rate >= 0.0)
    });

    let fallback_ratio = elapsed_window_ratio(current, now);
    let ratio = match (observed, sustainable) {
        (Some(rate), Some(safe)) if safe > 0.0 => Some(rate / safe),
        _ => fallback_ratio,
    };
    let projected_exhaustion_at = observed
        .filter(|rate| *rate > 0.0 && remaining > 0.0)
        .map(|rate| now + Duration::milliseconds((remaining / rate * 3_600_000.0) as i64));
    let headroom =
        elapsed_window_percent(current, now).map(|elapsed| elapsed - current.used_percent);
    let risk = if current.used_percent >= 100.0 {
        "critical"
    } else if current.used_percent >= 90.0 {
        "danger"
    } else if current.used_percent >= 75.0 {
        "warning"
    } else {
        match ratio {
            Some(value) if value >= 2.0 => "danger",
            Some(value) if value > 1.0 => "warning",
            Some(_) => "safe",
            None => "unknown",
        }
    };

    PaceMetrics {
        observed_percent_per_hour: observed,
        sustainable_percent_per_hour: sustainable,
        pace_ratio: ratio,
        headroom_percent: headroom,
        projected_exhaustion_at,
        risk,
    }
}

fn same_reset_window(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => (left - right).num_seconds().abs() <= 300,
        (None, None) => true,
        _ => false,
    }
}

fn elapsed_window_ratio(window: &ParsedUsageWindow, now: DateTime<Utc>) -> Option<f64> {
    let elapsed = elapsed_window_percent(window, now)?;
    if elapsed <= 0.01 {
        return None;
    }
    Some(window.used_percent / elapsed)
}

fn elapsed_window_percent(window: &ParsedUsageWindow, now: DateTime<Utc>) -> Option<f64> {
    let total = window.window_seconds?;
    let reset_at = window.reset_at?;
    if total <= 0 {
        return None;
    }
    let remaining = (reset_at - now).num_seconds().clamp(0, total);
    Some(((total - remaining) as f64 / total as f64 * 100.0).clamp(0.0, 100.0))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::json;

    use super::{pace_metrics, parse_usage_windows};

    #[test]
    fn parses_dynamic_core_and_additional_windows() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let windows = parse_usage_windows(
            &json!({
                "rate_limit": {
                    "primary_window": {
                        "used_percent": 12,
                        "limit_window_seconds": 604800,
                        "reset_after_seconds": 3600
                    },
                    "secondary_window": null
                },
                "additional_rate_limits": [{
                    "limit_name": "GPT Spark",
                    "metered_feature": "codex-spark",
                    "rate_limit": {
                        "primary_window": {
                            "used_percent": 4.5,
                            "limit_window_seconds": 18000,
                            "reset_at": 1700007200
                        }
                    }
                }]
            }),
            now,
        );

        assert_eq!(windows.len(), 2);
        assert_eq!(windows[0].quota_key, "codex");
        assert_eq!(windows[0].window_kind, "7d");
        assert_eq!(windows[1].quota_key, "codex_spark");
        assert_eq!(windows[1].window_kind, "5h");
    }

    #[test]
    fn computes_observed_and_sustainable_pace() {
        let now = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let mut window = parse_usage_windows(
            &json!({
                "rate_limit": {"primary_window": {
                    "used_percent": 50,
                    "limit_window_seconds": 604800,
                    "reset_after_seconds": 360000
                }}
            }),
            now,
        )
        .remove(0);
        window.reset_at = Some(now + Duration::hours(100));

        let pace = pace_metrics(
            &window,
            now,
            Some((40.0, now - Duration::hours(10), window.reset_at)),
            now,
        );

        assert_eq!(pace.observed_percent_per_hour, Some(1.0));
        assert_eq!(pace.sustainable_percent_per_hour, Some(0.5));
        assert_eq!(pace.pace_ratio, Some(2.0));
        assert_eq!(pace.risk, "danger");
    }
}
