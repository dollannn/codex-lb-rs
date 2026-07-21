use std::{env, net::SocketAddr, path::PathBuf, time::Duration};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub host: String,
    pub port: u16,
    pub upstream_base_url: String,
    pub auth_base_url: String,
    pub oauth_client_id: String,
    pub oauth_scope: String,
    pub encryption_key_file: PathBuf,
    pub admin_token: Option<String>,
    pub proxy_api_token: Option<String>,
    pub request_timeout: Duration,
    pub token_refresh_interval_days: i64,
    pub usage_refresh_interval: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let _ = dotenvy::dotenv();

        let database_url = env_var("CODEX_LB_DATABASE_URL").unwrap_or_else(default_database_url);
        let host = env_var("HOST").unwrap_or_else(|| "127.0.0.1".to_string());
        let port = env_var("PORT")
            .or_else(|| env_var("CODEX_LB_PORT"))
            .unwrap_or_else(|| "2455".to_string())
            .parse::<u16>()
            .context("PORT/CODEX_LB_PORT must be a valid TCP port")?;
        let upstream_base_url = env_var("CODEX_LB_UPSTREAM_BASE_URL")
            .unwrap_or_else(|| "https://chatgpt.com/backend-api".to_string());
        let auth_base_url = env_var("CODEX_LB_AUTH_BASE_URL")
            .unwrap_or_else(|| "https://auth.openai.com".to_string());
        let oauth_client_id = env_var("CODEX_LB_OAUTH_CLIENT_ID")
            .unwrap_or_else(|| "app_EMoamEEZ73f0CkXaXp7hrann".to_string());
        let oauth_scope =
            env_var("CODEX_LB_OAUTH_SCOPE").unwrap_or_else(|| "openid profile email".to_string());
        let encryption_key_file = env_var("CODEX_LB_ENCRYPTION_KEY_FILE")
            .map(PathBuf::from)
            .unwrap_or_else(default_encryption_key_file);
        let request_timeout = Duration::from_secs(
            env_var("CODEX_LB_PROXY_REQUEST_BUDGET_SECONDS")
                .unwrap_or_else(|| "600".to_string())
                .parse::<u64>()
                .context("CODEX_LB_PROXY_REQUEST_BUDGET_SECONDS must be an integer")?,
        );
        let token_refresh_interval_days = env_var("CODEX_LB_TOKEN_REFRESH_INTERVAL_DAYS")
            .unwrap_or_else(|| "8".to_string())
            .parse::<i64>()
            .context("CODEX_LB_TOKEN_REFRESH_INTERVAL_DAYS must be an integer")?;
        let usage_refresh_interval = Duration::from_secs(
            env_var("CODEX_LB_USAGE_REFRESH_INTERVAL_SECONDS")
                .unwrap_or_else(|| "120".to_string())
                .parse::<u64>()
                .context("CODEX_LB_USAGE_REFRESH_INTERVAL_SECONDS must be an integer")?
                .max(30),
        );

        Ok(Self {
            database_url,
            host,
            port,
            upstream_base_url,
            auth_base_url,
            oauth_client_id,
            oauth_scope,
            encryption_key_file,
            admin_token: env_var("CODEX_LB_ADMIN_TOKEN"),
            proxy_api_token: env_var("CODEX_LB_PROXY_API_TOKEN"),
            request_timeout,
            token_refresh_interval_days,
            usage_refresh_interval,
        })
    }

    pub fn socket_addr(&self) -> Result<SocketAddr> {
        format!("{}:{}", self.host, self.port)
            .parse()
            .with_context(|| format!("invalid listen address {}:{}", self.host, self.port))
    }

    pub fn upstream_codex_responses_url(&self) -> String {
        format!(
            "{}/codex/responses",
            self.upstream_base_url.trim_end_matches('/')
        )
    }

    pub fn upstream_codex_responses_websocket_url(&self) -> Result<String> {
        let url = self.upstream_codex_responses_url();
        if let Some(rest) = url.strip_prefix("https://") {
            Ok(format!("wss://{rest}"))
        } else if let Some(rest) = url.strip_prefix("http://") {
            Ok(format!("ws://{rest}"))
        } else if url.starts_with("ws://") || url.starts_with("wss://") {
            Ok(url)
        } else {
            anyhow::bail!("CODEX_LB_UPSTREAM_BASE_URL must use http(s) or ws(s)")
        }
    }

    pub fn upstream_codex_compact_url(&self) -> String {
        format!(
            "{}/codex/responses/compact",
            self.upstream_base_url.trim_end_matches('/')
        )
    }

    pub fn upstream_codex_models_url(&self) -> String {
        format!(
            "{}/codex/models",
            self.upstream_base_url.trim_end_matches('/')
        )
    }

    pub fn upstream_usage_url(&self) -> String {
        let base = self.upstream_base_url.trim_end_matches('/');
        if base.ends_with("/backend-api") || base.contains("/backend-api/") {
            format!("{base}/wham/usage")
        } else {
            format!("{base}/backend-api/wham/usage")
        }
    }

    pub fn token_refresh_url(&self) -> String {
        format!("{}/oauth/token", self.auth_base_url.trim_end_matches('/'))
    }
}

fn env_var(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn default_encryption_key_file() -> PathBuf {
    default_data_dir().join("encryption.key")
}

fn default_database_url() -> String {
    format!(
        "sqlite://{}",
        default_data_dir().join("codex-lb.sqlite").display()
    )
}

fn default_data_dir() -> PathBuf {
    env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            env::var_os("HOME").map(|home| PathBuf::from(home).join(".local").join("share"))
        })
        .unwrap_or_else(|| PathBuf::from("."))
        .join("codex-lb-rs")
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use super::Config;

    #[test]
    fn upstream_responses_url_appends_codex_responses() {
        let config = config_with_upstream("https://chatgpt.com/backend-api/");

        assert_eq!(
            config.upstream_codex_responses_url(),
            "https://chatgpt.com/backend-api/codex/responses"
        );
    }

    #[test]
    fn upstream_websocket_url_rewrites_https_scheme() {
        let config = config_with_upstream("https://chatgpt.com/backend-api/");

        assert_eq!(
            config.upstream_codex_responses_websocket_url().unwrap(),
            "wss://chatgpt.com/backend-api/codex/responses"
        );
    }

    #[test]
    fn usage_url_handles_backend_api_base() {
        let config = config_with_upstream("https://chatgpt.com/backend-api");

        assert_eq!(
            config.upstream_usage_url(),
            "https://chatgpt.com/backend-api/wham/usage"
        );
    }

    #[test]
    fn usage_url_adds_backend_api_when_base_is_origin() {
        let config = config_with_upstream("https://chatgpt.com");

        assert_eq!(
            config.upstream_usage_url(),
            "https://chatgpt.com/backend-api/wham/usage"
        );
    }

    fn config_with_upstream(upstream_base_url: &str) -> Config {
        Config {
            database_url: "sqlite::memory:".to_string(),
            host: "127.0.0.1".to_string(),
            port: 2455,
            upstream_base_url: upstream_base_url.to_string(),
            auth_base_url: "https://auth.openai.com".to_string(),
            oauth_client_id: "client".to_string(),
            oauth_scope: "openid profile email".to_string(),
            encryption_key_file: PathBuf::from("key"),
            admin_token: None,
            proxy_api_token: None,
            request_timeout: Duration::from_secs(1),
            token_refresh_interval_days: 8,
            usage_refresh_interval: Duration::from_secs(120),
        }
    }
}
