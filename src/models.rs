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
