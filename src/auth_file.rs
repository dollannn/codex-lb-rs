use std::fmt;

use anyhow::{Context, Result, anyhow};
use base64::{
    Engine as _,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{Map, Value};

const AUTH_CLAIMS_NAMESPACE: &str = "https://api.openai.com/auth";
const PROFILE_CLAIMS_NAMESPACE: &str = "https://api.openai.com/profile";

#[derive(Deserialize)]
pub struct AuthFile {
    #[serde(default, alias = "OPENAI_API_KEY")]
    #[allow(dead_code)]
    pub openai_api_key: Option<String>,
    pub tokens: AuthTokens,
    #[serde(
        default,
        rename = "lastRefreshAt",
        alias = "last_refresh_at",
        alias = "last_refresh"
    )]
    pub last_refresh_at: Option<DateTime<Utc>>,
}

impl fmt::Debug for AuthFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthFile")
            .field(
                "openai_api_key",
                &self.openai_api_key.as_ref().map(|_| "[REDACTED]"),
            )
            .field("tokens", &self.tokens)
            .field("last_refresh_at", &self.last_refresh_at)
            .finish()
    }
}

#[derive(Deserialize)]
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

impl fmt::Debug for AuthTokens {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthTokens")
            .field("id_token", &redacted(&self.id_token))
            .field("access_token", &redacted(&self.access_token))
            .field("refresh_token", &redacted(&self.refresh_token))
            .field("account_id", &self.account_id)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct AuthClaims {
    pub chatgpt_account_id: Option<String>,
    pub email: String,
    pub plan_type: String,
    pub access_token_expires_at: Option<DateTime<Utc>>,
    pub id_token_expires_at: Option<DateTime<Utc>>,
    pub workspace_id: Option<String>,
    pub workspace_name: Option<String>,
    pub workspace_role: Option<String>,
    pub workspace_is_default: Option<bool>,
}

impl AuthClaims {
    /// The expiry that controls whether this credential can make upstream requests.
    pub fn token_expires_at(&self) -> Option<DateTime<Utc>> {
        self.access_token_expires_at.or(self.id_token_expires_at)
    }
}

#[derive(Deserialize)]
struct OpenCodeAuthRecord {
    #[serde(default, rename = "type")]
    _auth_type: Option<String>,
    #[serde(default)]
    access: String,
    #[serde(default)]
    refresh: String,
    #[serde(default, rename = "accountId", alias = "account_id")]
    account_id: Option<String>,
    #[serde(default)]
    expires: Option<Value>,
    #[serde(default, rename = "idToken", alias = "id_token", alias = "id")]
    id_token: Option<String>,
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

/// Parse either an OpenCode auth root or one OAuth provider record.
///
/// OpenCode commonly stores a root object keyed by provider name. A direct provider
/// record is also accepted so callers that have already selected a provider do not
/// need to wrap it again. For a root object, `openai` is preferred over `codex`, then
/// the first OAuth-shaped provider is used.
pub fn parse_opencode_auth_json(value: Value) -> Result<(AuthFile, AuthClaims)> {
    let provider = find_opencode_provider(&value)
        .ok_or_else(|| anyhow!("OpenCode auth payload does not contain an OAuth provider"))?;
    let record: OpenCodeAuthRecord = serde_json::from_value(provider.clone())
        .context("invalid OpenCode OAuth provider payload")?;

    if record.access.trim().is_empty() || record.refresh.trim().is_empty() {
        return Err(anyhow!(
            "OpenCode OAuth provider is missing an access or refresh token"
        ));
    }

    let id_token = record
        .id_token
        .filter(|token| !token.trim().is_empty())
        .unwrap_or_else(|| record.access.clone());
    let account_id = non_empty_owned(record.account_id);
    let explicit_expiry = record.expires.as_ref().and_then(parse_timestamp);
    let auth = AuthFile {
        openai_api_key: None,
        tokens: AuthTokens {
            id_token,
            access_token: record.access,
            refresh_token: record.refresh,
            account_id,
        },
        last_refresh_at: None,
    };
    let mut claims = claims_from_auth(&auth);
    if explicit_expiry.is_some() {
        claims.access_token_expires_at = explicit_expiry;
    }

    Ok((auth, claims))
}

pub fn claims_from_auth(auth: &AuthFile) -> AuthClaims {
    let id_claims = decode_jwt_payload(&auth.tokens.id_token).unwrap_or(Value::Null);
    let access_claims = decode_jwt_payload(&auth.tokens.access_token).unwrap_or(Value::Null);

    let account_from_claims = claim_string(&id_claims, "chatgpt_account_id")
        .or_else(|| claim_string(&access_claims, "chatgpt_account_id"));
    let plan_type = claim_string(&id_claims, "chatgpt_plan_type")
        .or_else(|| claim_string(&access_claims, "chatgpt_plan_type"))
        .unwrap_or_else(|| "unknown".to_string());
    let email = email_claim(&id_claims)
        .or_else(|| email_claim(&access_claims))
        .unwrap_or_else(|| "unknown@example.com".to_string());

    let id_workspace = workspace_from_claims(&id_claims);
    let access_workspace = workspace_from_claims(&access_claims);
    let workspace = id_workspace.merge(access_workspace);
    let chatgpt_account_id =
        non_empty_owned(auth.tokens.account_id.clone()).or(account_from_claims);

    AuthClaims {
        workspace_id: workspace.id.or_else(|| chatgpt_account_id.clone()),
        workspace_name: workspace.name,
        workspace_role: workspace.role,
        workspace_is_default: workspace.is_default,
        chatgpt_account_id,
        email,
        plan_type,
        access_token_expires_at: token_expiry(&access_claims),
        id_token_expires_at: token_expiry(&id_claims),
    }
}

pub fn decode_jwt_payload(token: &str) -> Option<Value> {
    let payload = token.trim().split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| URL_SAFE.decode(payload))
        .ok()?;
    serde_json::from_slice(&decoded).ok()
}

fn find_opencode_provider(value: &Value) -> Option<&Value> {
    if is_opencode_provider(value) {
        return Some(value);
    }

    let root = value.as_object()?;
    for provider_name in ["openai", "codex"] {
        if let Some(provider) = root
            .get(provider_name)
            .filter(|item| is_opencode_provider(item))
        {
            return Some(provider);
        }
    }

    root.values()
        .find(|provider| is_opencode_provider(provider))
}

fn is_opencode_provider(value: &Value) -> bool {
    value.as_object().is_some_and(|provider| {
        provider
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|auth_type| auth_type.eq_ignore_ascii_case("oauth"))
            || provider.contains_key("access")
            || provider.contains_key("refresh")
    })
}

fn auth_claims(claims: &Value) -> Option<&Map<String, Value>> {
    claims.get(AUTH_CLAIMS_NAMESPACE).and_then(Value::as_object)
}

fn claim_string(claims: &Value, key: &str) -> Option<String> {
    auth_claims(claims)
        .and_then(|auth| auth.get(key))
        .and_then(non_empty_str)
        .or_else(|| claims.get(key).and_then(non_empty_str))
        .map(str::to_string)
}

fn email_claim(claims: &Value) -> Option<String> {
    claims
        .get("email")
        .and_then(non_empty_str)
        .or_else(|| {
            claims
                .get(PROFILE_CLAIMS_NAMESPACE)
                .and_then(Value::as_object)
                .and_then(|profile| profile.get("email"))
                .and_then(non_empty_str)
        })
        .map(str::to_string)
}

fn token_expiry(claims: &Value) -> Option<DateTime<Utc>> {
    claims.get("exp").and_then(parse_timestamp)
}

fn parse_timestamp(value: &Value) -> Option<DateTime<Utc>> {
    if let Some(raw) = value.as_i64() {
        return epoch_timestamp(raw);
    }
    if let Some(raw) = value.as_u64().and_then(|raw| i64::try_from(raw).ok()) {
        return epoch_timestamp(raw);
    }
    if let Some(raw) = value.as_f64() {
        if raw.is_finite() && raw >= i64::MIN as f64 && raw <= i64::MAX as f64 {
            return epoch_timestamp(raw.trunc() as i64);
        }
        return None;
    }

    let raw = value.as_str()?.trim();
    if let Ok(epoch) = raw.parse::<i64>() {
        return epoch_timestamp(epoch);
    }
    DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

fn epoch_timestamp(raw: i64) -> Option<DateTime<Utc>> {
    // OpenCode writes JavaScript timestamps (milliseconds); JWT `exp` uses seconds.
    if raw.unsigned_abs() >= 100_000_000_000 {
        DateTime::from_timestamp_millis(raw)
    } else {
        DateTime::from_timestamp(raw, 0)
    }
}

#[derive(Default)]
struct WorkspaceClaims {
    id: Option<String>,
    name: Option<String>,
    role: Option<String>,
    is_default: Option<bool>,
}

impl WorkspaceClaims {
    fn merge(self, fallback: Self) -> Self {
        Self {
            id: self.id.or(fallback.id),
            name: self.name.or(fallback.name),
            role: self.role.or(fallback.role),
            is_default: self.is_default.or(fallback.is_default),
        }
    }
}

fn workspace_from_claims(claims: &Value) -> WorkspaceClaims {
    let Some(auth) = auth_claims(claims) else {
        return WorkspaceClaims::default();
    };

    if let Some(workspace) = auth.get("workspace").and_then(Value::as_object) {
        return workspace_from_object(workspace);
    }

    let Some(organizations) = auth.get("organizations").and_then(Value::as_array) else {
        return WorkspaceClaims {
            id: object_string(auth, &["workspace_id", "chatgpt_workspace_id"]),
            name: object_string(
                auth,
                &[
                    "workspace_name",
                    "chatgpt_workspace_name",
                    "workspace_title",
                ],
            ),
            role: object_string(auth, &["workspace_role"]),
            is_default: auth.get("workspace_is_default").and_then(Value::as_bool),
        };
    };

    organizations
        .iter()
        .filter_map(Value::as_object)
        .find(|organization| organization.get("is_default").and_then(Value::as_bool) == Some(true))
        .or_else(|| organizations.iter().find_map(Value::as_object))
        .map(workspace_from_object)
        .unwrap_or_default()
}

fn workspace_from_object(workspace: &Map<String, Value>) -> WorkspaceClaims {
    WorkspaceClaims {
        id: object_string(workspace, &["id", "workspace_id", "account_id"]),
        name: object_string(workspace, &["name", "title", "workspace_name"]),
        role: object_string(workspace, &["role", "workspace_role"]),
        is_default: workspace
            .get("is_default")
            .or_else(|| workspace.get("isDefault"))
            .and_then(Value::as_bool),
    }
}

fn object_string(object: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(non_empty_str))
        .map(str::to_string)
}

fn non_empty_str(value: &Value) -> Option<&str> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn non_empty_owned(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn redacted(value: &str) -> &'static str {
    if value.is_empty() {
        "[EMPTY]"
    } else {
        "[REDACTED]"
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine as _;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    use super::{
        AuthFile, AuthTokens, URL_SAFE_NO_PAD, claims_from_auth, parse_auth_json,
        parse_opencode_auth_json,
    };

    #[test]
    fn claims_from_auth_decodes_id_token_and_prefers_token_account_id() {
        let auth = AuthFile {
            openai_api_key: None,
            tokens: AuthTokens {
                id_token: jwt(json!({
                    "email": "person@example.com",
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": "claim-account",
                        "chatgpt_plan_type": "plus"
                    }
                })),
                access_token: "access".to_string(),
                refresh_token: "refresh".to_string(),
                account_id: Some("token-account".to_string()),
            },
            last_refresh_at: None,
        };

        let claims = claims_from_auth(&auth);

        assert_eq!(claims.email, "person@example.com");
        assert_eq!(claims.plan_type, "plus");
        assert_eq!(claims.chatgpt_account_id.as_deref(), Some("token-account"));
        assert_eq!(claims.workspace_id.as_deref(), Some("token-account"));
    }

    #[test]
    fn claims_fall_back_to_access_token_and_extract_workspace_and_expiry() {
        let auth = AuthFile {
            openai_api_key: None,
            tokens: AuthTokens {
                id_token: jwt(json!({
                    "email": "person@example.com",
                    "exp": 1_800_000_000
                })),
                access_token: jwt(json!({
                    "exp": "1900000000",
                    "https://api.openai.com/auth": {
                        "chatgpt_account_id": "access-account",
                        "chatgpt_plan_type": "pro",
                        "organizations": [
                            {"id": "workspace-old", "title": "Old", "is_default": false},
                            {
                                "id": "workspace-current",
                                "title": "Work",
                                "role": "owner",
                                "is_default": true
                            }
                        ]
                    }
                })),
                refresh_token: "refresh".to_string(),
                account_id: None,
            },
            last_refresh_at: None,
        };

        let claims = claims_from_auth(&auth);

        assert_eq!(claims.email, "person@example.com");
        assert_eq!(claims.plan_type, "pro");
        assert_eq!(claims.chatgpt_account_id.as_deref(), Some("access-account"));
        assert_eq!(claims.workspace_id.as_deref(), Some("workspace-current"));
        assert_eq!(claims.workspace_name.as_deref(), Some("Work"));
        assert_eq!(claims.workspace_role.as_deref(), Some("owner"));
        assert_eq!(claims.workspace_is_default, Some(true));
        assert_eq!(
            claims.access_token_expires_at,
            Utc.timestamp_opt(1_900_000_000, 0).single()
        );
        assert_eq!(
            claims.id_token_expires_at,
            Utc.timestamp_opt(1_800_000_000, 0).single()
        );
        assert_eq!(claims.token_expires_at(), claims.access_token_expires_at);
    }

    #[test]
    fn parse_auth_json_accepts_current_codex_last_refresh_name() {
        let (auth, _) = parse_auth_json(json!({
            "tokens": {
                "id_token": "id",
                "access_token": "access",
                "refresh_token": "refresh"
            },
            "last_refresh": "2026-07-21T12:34:56Z"
        }))
        .expect("current Codex auth shape should parse");

        assert_eq!(
            auth.last_refresh_at,
            Some(Utc.with_ymd_and_hms(2026, 7, 21, 12, 34, 56).unwrap())
        );
    }

    #[test]
    fn parse_opencode_provider_preserves_metadata_and_uses_access_as_id_fallback() {
        let access_token = jwt(json!({
            "email": "work@example.com",
            "exp": 1_800_000_000,
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "claim-account",
                "chatgpt_plan_type": "pro"
            }
        }));
        let (auth, claims) = parse_opencode_auth_json(json!({
            "type": "oauth",
            "access": access_token,
            "refresh": "refresh-secret",
            "accountId": "stable-account",
            "expires": 1_900_000_000_000_i64
        }))
        .expect("OpenCode provider should parse");

        assert_eq!(auth.tokens.id_token, auth.tokens.access_token);
        assert_eq!(claims.email, "work@example.com");
        assert_eq!(claims.plan_type, "pro");
        assert_eq!(claims.chatgpt_account_id.as_deref(), Some("stable-account"));
        assert_eq!(
            claims.access_token_expires_at,
            Utc.timestamp_opt(1_900_000_000, 0).single()
        );
    }

    #[test]
    fn parse_opencode_root_selects_openai_provider() {
        let (auth, claims) = parse_opencode_auth_json(json!({
            "anthropic": {"type": "api", "key": "not-selected"},
            "codex": {
                "type": "oauth",
                "access": jwt(json!({
                    "https://api.openai.com/auth": {"chatgpt_plan_type": "plus"}
                })),
                "refresh": "codex-refresh",
                "accountId": "codex-account",
                "expires": 1_800_000_000_000_i64
            },
            "openai": {
                "type": "oauth",
                "access": jwt(json!({
                    "https://api.openai.com/auth": {"chatgpt_plan_type": "pro"}
                })),
                "refresh": "openai-refresh",
                "accountId": "openai-account",
                "expires": 1_900_000_000_000_i64
            }
        }))
        .expect("OpenCode auth root should parse");

        assert_eq!(auth.tokens.account_id.as_deref(), Some("openai-account"));
        assert_eq!(claims.plan_type, "pro");
    }

    #[test]
    fn parse_opencode_provider_rejects_blank_access_or_refresh_tokens() {
        for provider in [
            json!({"type": "oauth", "access": " ", "refresh": "refresh"}),
            json!({"type": "oauth", "access": "access", "refresh": "\t"}),
        ] {
            let error = parse_opencode_auth_json(provider)
                .expect_err("blank OpenCode credentials should be rejected");
            assert!(
                error
                    .to_string()
                    .contains("missing an access or refresh token")
            );
        }
    }

    #[test]
    fn auth_debug_output_redacts_credentials() {
        let auth = AuthFile {
            openai_api_key: Some("api-key-secret".to_string()),
            tokens: AuthTokens {
                id_token: "id-token-secret".to_string(),
                access_token: "access-token-secret".to_string(),
                refresh_token: "refresh-token-secret".to_string(),
                account_id: Some("account-id".to_string()),
            },
            last_refresh_at: None,
        };

        let debug = format!("{auth:?}");
        assert!(debug.contains("[REDACTED]"));
        for secret in [
            "api-key-secret",
            "id-token-secret",
            "access-token-secret",
            "refresh-token-secret",
        ] {
            assert!(!debug.contains(secret));
        }
    }

    #[test]
    fn parse_auth_json_rejects_missing_tokens() {
        let error = parse_auth_json(json!({
            "tokens": {
                "idToken": "id",
                "accessToken": "",
                "refreshToken": "refresh"
            }
        }))
        .expect_err("empty access token should be rejected");

        assert!(error.to_string().contains("missing one or more tokens"));
    }

    fn jwt(payload: serde_json::Value) -> String {
        format!(
            "{}.{}.sig",
            URL_SAFE_NO_PAD.encode(r#"{"alg":"none"}"#),
            URL_SAFE_NO_PAD.encode(payload.to_string())
        )
    }
}
