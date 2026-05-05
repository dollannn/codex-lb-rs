use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Account {
    pub id: Uuid,
    pub chatgpt_account_id: Option<String>,
    pub email: String,
    pub plan_type: String,
    #[serde(skip_serializing)]
    pub encrypted_access_token: String,
    #[serde(skip_serializing)]
    pub encrypted_refresh_token: String,
    #[serde(skip_serializing)]
    pub encrypted_id_token: String,
    pub last_refresh_at: DateTime<Utc>,
    pub status: String,
    pub status_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct AccountSummary {
    pub id: Uuid,
    pub chatgpt_account_id: Option<String>,
    pub email: String,
    pub plan_type: String,
    pub status: String,
    pub status_reason: Option<String>,
    pub last_refresh_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub latest_used_percent: Option<f64>,
    pub latest_reset_at: Option<DateTime<Utc>>,
    pub request_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct UsageSnapshot {
    pub id: i64,
    pub account_id: Uuid,
    pub recorded_at: DateTime<Utc>,
    pub used_percent: Option<f64>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub reset_at: Option<DateTime<Utc>>,
    pub raw_json: Value,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct RequestLog {
    pub id: i64,
    pub request_id: String,
    pub account_id: Option<Uuid>,
    pub model: Option<String>,
    pub status: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
    pub latency_ms: Option<i32>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct SettingRow {
    pub key: String,
    pub value: Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeSettings {
    pub routing_strategy: String,
    pub proxy_max_attempts: usize,
    pub rate_limit_cooldown_seconds: i64,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            routing_strategy: "round_robin".to_string(),
            proxy_max_attempts: 2,
            rate_limit_cooldown_seconds: 60,
        }
    }
}

impl RuntimeSettings {
    const MAX_PROXY_ATTEMPTS: usize = 10;
    const MAX_RATE_LIMIT_COOLDOWN_SECONDS: i64 = 60 * 60;

    pub fn apply(&mut self, key: &str, value: &Value) {
        match key {
            "routing_strategy" => {
                if let Some(strategy) = value.as_str().filter(|value| !value.trim().is_empty()) {
                    self.routing_strategy = strategy.trim().to_string();
                }
            }
            "proxy_max_attempts" => {
                if let Some(value) = json_i64(value) {
                    self.proxy_max_attempts = value
                        .clamp(1, Self::MAX_PROXY_ATTEMPTS as i64)
                        .try_into()
                        .unwrap_or(Self::MAX_PROXY_ATTEMPTS);
                }
            }
            "rate_limit_cooldown_seconds" => {
                if let Some(value) = json_i64(value) {
                    self.rate_limit_cooldown_seconds =
                        value.clamp(1, Self::MAX_RATE_LIMIT_COOLDOWN_SECONDS);
                }
            }
            _ => {}
        }
    }
}

fn json_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(value) => value.trim().parse::<i64>().ok(),
        _ => None,
    }
}

#[derive(Debug, Clone)]
pub struct AccountTokens {
    pub access_token: String,
    pub refresh_token: String,
    pub id_token: String,
}

#[derive(Debug, Clone)]
pub struct SelectedAccount {
    pub account: Account,
    pub tokens: AccountTokens,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageData {
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_input_tokens: Option<i64>,
    pub reasoning_tokens: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct AccountUpdateRequest {
    pub status: Option<String>,
    pub email: Option<String>,
    pub plan_type: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UsageSummary {
    pub account_count: i64,
    pub active_account_count: i64,
    pub request_count: i64,
    pub successful_request_count: i64,
    pub failed_request_count: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
}

#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::RuntimeSettings;

    #[test]
    fn runtime_settings_apply_json_and_string_values() {
        let mut settings = RuntimeSettings::default();

        settings.apply("routing_strategy", &json!("round_robin"));
        settings.apply("proxy_max_attempts", &json!(3));
        settings.apply("rate_limit_cooldown_seconds", &json!("120"));

        assert_eq!(settings.routing_strategy, "round_robin");
        assert_eq!(settings.proxy_max_attempts, 3);
        assert_eq!(settings.rate_limit_cooldown_seconds, 120);
    }

    #[test]
    fn runtime_settings_clamp_unsafe_values() {
        let mut settings = RuntimeSettings::default();

        settings.apply("proxy_max_attempts", &json!(0));
        settings.apply("rate_limit_cooldown_seconds", &json!(99_999));

        assert_eq!(settings.proxy_max_attempts, 1);
        assert_eq!(settings.rate_limit_cooldown_seconds, 3_600);
    }

    #[test]
    fn runtime_settings_ignore_invalid_values() {
        let mut settings = RuntimeSettings::default();

        settings.apply("proxy_max_attempts", &json!({"bad": true}));
        settings.apply("rate_limit_cooldown_seconds", &json!(false));

        assert_eq!(settings, RuntimeSettings::default());
    }
}
