use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
pub struct AuthFile {
    #[serde(default, alias = "OPENAI_API_KEY")]
    #[allow(dead_code)]
    pub openai_api_key: Option<String>,
    pub tokens: AuthTokens,
    #[serde(default, rename = "lastRefreshAt", alias = "last_refresh_at")]
    pub last_refresh_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct AuthTokens {
    #[serde(rename = "idToken", alias = "id_token")]
    pub id_token: String,
    #[serde(rename = "accessToken", alias = "access_token")]
    pub access_token: String,
    #[serde(rename = "refreshToken", alias = "refresh_token")]
    pub refresh_token: String,
    #[serde(default, rename = "accountId", alias = "account_id")]
    pub account_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AuthClaims {
    pub chatgpt_account_id: Option<String>,
    pub email: String,
    pub plan_type: String,
}

pub fn parse_auth_json(value: Value) -> Result<(AuthFile, AuthClaims)> {
    let auth: AuthFile = serde_json::from_value(value).context("invalid auth.json payload")?;
    let claims = claims_from_auth(&auth);
    if auth.tokens.access_token.trim().is_empty()
        || auth.tokens.refresh_token.trim().is_empty()
        || auth.tokens.id_token.trim().is_empty()
    {
        return Err(anyhow!("auth.json is missing one or more tokens"));
    }
    Ok((auth, claims))
}

pub fn claims_from_auth(auth: &AuthFile) -> AuthClaims {
    let id_claims = decode_jwt_payload(&auth.tokens.id_token).unwrap_or(Value::Null);
    let auth_claims = id_claims
        .get("https://api.openai.com/auth")
        .and_then(Value::as_object);

    let account_from_claims = auth_claims
        .and_then(|obj| obj.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .or_else(|| id_claims.get("chatgpt_account_id").and_then(Value::as_str))
        .map(str::to_string);
    let plan_type = auth_claims
        .and_then(|obj| obj.get("chatgpt_plan_type"))
        .and_then(Value::as_str)
        .or_else(|| id_claims.get("chatgpt_plan_type").and_then(Value::as_str))
        .unwrap_or("unknown")
        .to_string();
    let email = id_claims
        .get("email")
        .and_then(Value::as_str)
        .unwrap_or("unknown@example.com")
        .to_string();

    AuthClaims {
        chatgpt_account_id: auth.tokens.account_id.clone().or(account_from_claims),
        email,
        plan_type,
    }
}

pub fn decode_jwt_payload(token: &str) -> Option<Value> {
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice(&decoded).ok()
}
