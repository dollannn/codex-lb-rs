use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::{Path, PathBuf},
    process::Command as ProcessCommand,
    time::SystemTime,
};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use reqwest::Method;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "codex-lb-rs",
    version,
    about = "Lean local Codex multi-account load balancer"
)]
pub struct Cli {
    #[arg(
        long,
        env = "CODEX_LB_BASE_URL",
        default_value = "http://127.0.0.1:2455"
    )]
    pub base_url: String,
    #[arg(long, env = "CODEX_LB_ADMIN_TOKEN")]
    pub admin_token: Option<String>,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve(ServeArgs),
    Migrate(MigrateArgs),
    Accounts {
        #[command(subcommand)]
        command: AccountsCommand,
    },
    Usage {
        #[command(subcommand)]
        command: UsageCommand,
    },
    Logs {
        #[command(subcommand)]
        command: LogsCommand,
    },
    /// Inspect privacy-preserving sticky routes from local Codex sessions to accounts.
    Sessions {
        #[command(subcommand)]
        command: SessionsCommand,
    },
    Settings {
        #[command(subcommand)]
        command: SettingsCommand,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Show the daemon's cached local status without contacting OpenAI.
    Status {
        /// Emit Waybar's JSON custom-module format.
        #[arg(long)]
        waybar: bool,
    },
}

#[derive(Debug, Args)]
pub struct ServeArgs {
    #[arg(long, env = "HOST")]
    pub host: Option<String>,
    #[arg(long, env = "PORT")]
    pub port: Option<u16>,
}

#[derive(Debug, Args)]
pub struct MigrateArgs {
    #[command(subcommand)]
    pub command: MigrateCommand,
}

#[derive(Debug, Subcommand)]
pub enum MigrateCommand {
    Up,
}

#[derive(Debug, Subcommand)]
pub enum AccountsCommand {
    List,
    Import {
        path: PathBuf,
        /// Stable display name such as "account-a" or "account-b".
        #[arg(long)]
        label: Option<String>,
    },
    /// Import an existing OpenCode OAuth slot once, then let the daemon refresh it.
    ImportOpencode {
        path: PathBuf,
        #[arg(long, default_value = "openai")]
        provider: String,
        #[arg(long)]
        label: String,
    },
    /// Run Codex's device login in an isolated home and import the result.
    Login {
        label: String,
    },
    Pause {
        id: Uuid,
    },
    Reactivate {
        id: Uuid,
    },
    Remove {
        id: Uuid,
    },
    RefreshToken {
        id: Uuid,
    },
    RefreshUsage {
        id: Uuid,
    },
}

#[derive(Debug, Subcommand)]
pub enum UsageCommand {
    Summary,
    Account { id: Uuid },
    Refresh,
}

#[derive(Debug, Subcommand)]
pub enum LogsCommand {
    List {
        #[arg(long, default_value_t = 100)]
        limit: i64,
        #[arg(long, default_value_t = 0)]
        offset: i64,
    },
}

#[derive(Debug, Subcommand)]
pub enum SessionsCommand {
    /// Match recent local Codex rollout IDs to their last routed pool account.
    List {
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Resolve one Codex session UUID to its last routed pool account.
    Show { session_id: Uuid },
    /// Show current hashed routes without reading local Codex session files.
    Routes {
        #[arg(long, default_value_t = 100)]
        limit: i64,
    },
}

#[derive(Debug, Subcommand)]
pub enum SettingsCommand {
    Get,
    Set { key: String, value: String },
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    Check,
}

pub async fn run_api_command(cli: &Cli) -> Result<()> {
    match &cli.command {
        Command::Accounts { command } => run_accounts(cli, command).await,
        Command::Usage { command } => run_usage(cli, command).await,
        Command::Logs { command } => run_logs(cli, command).await,
        Command::Sessions { command } => run_sessions(cli, command).await,
        Command::Settings { command } => run_settings(cli, command).await,
        Command::Config {
            command: ConfigCommand::Check,
        } => {
            let config = crate::config::Config::from_env()?;
            print_json(&serde_json::json!({
                "databaseUrlConfigured": !config.database_url.is_empty(),
                "listen": format!("{}:{}", config.host, config.port),
                "upstreamBaseUrl": config.upstream_base_url,
                "adminTokenConfigured": config.admin_token.is_some(),
                "proxyTokenConfigured": config.proxy_api_token.is_some(),
            }))
        }
        Command::Status { waybar } => run_status(cli, *waybar).await,
        Command::Serve(_) | Command::Migrate(_) => Ok(()),
    }
}

async fn run_accounts(cli: &Cli, command: &AccountsCommand) -> Result<()> {
    match command {
        AccountsCommand::List => print_response(cli, Method::GET, "/admin/accounts", None).await,
        AccountsCommand::Import { path, label } => {
            let raw = tokio::fs::read(path)
                .await
                .with_context(|| format!("reading {}", path.display()))?;
            let payload: Value = serde_json::from_slice(&raw).context("auth file must be JSON")?;
            import_payload(cli, payload, label.as_deref()).await
        }
        AccountsCommand::ImportOpencode {
            path,
            provider,
            label,
        } => {
            let raw = tokio::fs::read(path)
                .await
                .with_context(|| format!("reading {}", path.display()))?;
            let root: Value =
                serde_json::from_slice(&raw).context("OpenCode auth file must be JSON")?;
            let slot = root
                .get(provider)
                .and_then(Value::as_object)
                .with_context(|| format!("OpenCode provider '{provider}' was not found"))?;
            let access = required_string(slot.get("access"), "access")?;
            let refresh = required_string(slot.get("refresh"), "refresh")?;
            let account_id = required_string(slot.get("accountId"), "accountId")?;
            let payload = serde_json::json!({
                "tokens": {
                    "idToken": access,
                    "accessToken": access,
                    "refreshToken": refresh,
                    "accountId": account_id,
                }
            });
            import_payload(cli, payload, Some(label)).await
        }
        AccountsCommand::Login { label } => {
            let auth_path = run_isolated_codex_login(label)?;
            let raw = tokio::fs::read(&auth_path)
                .await
                .with_context(|| format!("reading {}", auth_path.display()))?;
            let payload: Value =
                serde_json::from_slice(&raw).context("Codex auth file must be JSON")?;
            import_payload(cli, payload, Some(label)).await
        }
        AccountsCommand::Pause { id } => {
            print_response(
                cli,
                Method::PATCH,
                &format!("/admin/accounts/{id}"),
                Some(serde_json::json!({"status":"paused"})),
            )
            .await
        }
        AccountsCommand::Reactivate { id } => {
            print_response(
                cli,
                Method::PATCH,
                &format!("/admin/accounts/{id}"),
                Some(serde_json::json!({"status":"active"})),
            )
            .await
        }
        AccountsCommand::Remove { id } => {
            request(cli, Method::DELETE, &format!("/admin/accounts/{id}"), None).await?;
            println!("deleted {id}");
            Ok(())
        }
        AccountsCommand::RefreshToken { id } => {
            print_response(
                cli,
                Method::POST,
                &format!("/admin/accounts/{id}/refresh-token"),
                None,
            )
            .await
        }
        AccountsCommand::RefreshUsage { id } => {
            print_response(
                cli,
                Method::POST,
                &format!("/admin/accounts/{id}/refresh-usage"),
                None,
            )
            .await
        }
    }
}

async fn import_payload(cli: &Cli, auth: Value, label: Option<&str>) -> Result<()> {
    let payload = serde_json::json!({
        "auth": auth,
        "label": label,
    });
    print_response(cli, Method::POST, "/admin/accounts", Some(payload)).await
}

fn required_string<'a>(value: Option<&'a Value>, name: &str) -> Result<&'a str> {
    value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("OpenCode OAuth slot is missing '{name}'"))
}

fn run_isolated_codex_login(label: &str) -> Result<PathBuf> {
    let safe_label = sanitize_label(label)?;
    let data_root = env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .context("HOME or XDG_DATA_HOME is required")?;
    let codex_home = data_root
        .join("codex-lb-rs")
        .join("login-homes")
        .join(safe_label);
    std::fs::create_dir_all(&codex_home)
        .with_context(|| format!("creating {}", codex_home.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&codex_home, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("securing {}", codex_home.display()))?;
    }

    let status = ProcessCommand::new(env::var_os("CODEX_BINARY").unwrap_or_else(|| "codex".into()))
        .env("CODEX_HOME", &codex_home)
        .arg("-c")
        .arg("cli_auth_credentials_store=\"file\"")
        .arg("login")
        .arg("--device-auth")
        .status()
        .context("starting Codex device login")?;
    if !status.success() {
        bail!("Codex login exited with {status}");
    }
    Ok(codex_home.join("auth.json"))
}

fn sanitize_label(label: &str) -> Result<String> {
    let value = label.trim().to_ascii_lowercase();
    if value.is_empty()
        || value.len() > 32
        || !value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        bail!("label must be 1-32 ASCII letters, numbers, '-' or '_'");
    }
    Ok(value)
}

async fn run_status(cli: &Cli, waybar: bool) -> Result<()> {
    let path = if waybar {
        "/api/v1/status/waybar"
    } else {
        "/api/v1/status"
    };
    match request(cli, Method::GET, path, None).await {
        Ok(value) if waybar => {
            println!("{}", serde_json::to_string(&value)?);
            Ok(())
        }
        Ok(value) => print_json(&value),
        Err(error) if waybar => {
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "text": "󰬫 offline",
                    "tooltip": format!("codex-lb-rs is unavailable: {error}"),
                    "class": ["codex-pool", "offline"],
                    "percentage": 0,
                    "alt": "offline"
                }))?
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn run_usage(cli: &Cli, command: &UsageCommand) -> Result<()> {
    match command {
        UsageCommand::Summary => {
            print_response(cli, Method::GET, "/admin/usage/summary", None).await
        }
        UsageCommand::Account { id } => {
            print_response(
                cli,
                Method::GET,
                &format!("/admin/usage/accounts/{id}"),
                None,
            )
            .await
        }
        UsageCommand::Refresh => {
            print_response(cli, Method::POST, "/admin/usage/refresh", None).await
        }
    }
}

async fn run_logs(cli: &Cli, command: &LogsCommand) -> Result<()> {
    match command {
        LogsCommand::List { limit, offset } => {
            print_response(
                cli,
                Method::GET,
                &format!("/admin/request-logs?limit={limit}&offset={offset}"),
                None,
            )
            .await
        }
    }
}

async fn run_sessions(cli: &Cli, command: &SessionsCommand) -> Result<()> {
    match command {
        SessionsCommand::Routes { limit } => {
            print_response(
                cli,
                Method::GET,
                &format!("/admin/session-routes?limit={}", (*limit).clamp(1, 500)),
                None,
            )
            .await
        }
        SessionsCommand::Show { session_id } => {
            let key_hash = crate::db::affinity_hash("session_id", &session_id.to_string());
            let response = resolve_session_hashes(cli, std::slice::from_ref(&key_hash)).await?;
            let route = response
                .get("sessionRoutes")
                .and_then(Value::as_array)
                .and_then(|routes| routes.first())
                .cloned();
            print_json(&serde_json::json!({
                "sessionId": session_id,
                "fingerprint": &key_hash[..12],
                "route": route,
                "semantics": "last_routed",
                "note": "A sticky route is not proof that the session process is currently connected."
            }))
        }
        SessionsCommand::List { limit } => {
            let limit = (*limit).clamp(1, 500);
            let session_ids = discover_codex_session_ids(500)?;
            let by_hash = session_ids
                .iter()
                .map(|session_id| {
                    (
                        crate::db::affinity_hash("session_id", &session_id.to_string()),
                        *session_id,
                    )
                })
                .collect::<HashMap<_, _>>();
            let key_hashes = by_hash.keys().cloned().collect::<Vec<_>>();
            let response = resolve_session_hashes(cli, &key_hashes).await?;
            let sessions = response
                .get("sessionRoutes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|route| {
                    let key_hash = route.get("keyHash")?.as_str()?;
                    let session_id = by_hash.get(key_hash)?;
                    Some(serde_json::json!({
                        "sessionId": session_id,
                        "fingerprint": &key_hash[..12],
                        "account": route.get("accountLabel"),
                        "lastRoutedAt": route.get("lastUsedAt"),
                        "firstRoutedAt": route.get("createdAt")
                    }))
                })
                .take(limit)
                .collect::<Vec<_>>();
            print_json(&serde_json::json!({
                "sessions": sessions,
                "semantics": "last_routed",
                "note": "Raw session IDs are matched locally; the daemon stores and receives only SHA-256 hashes. A sticky route is not proof that the session process is currently connected."
            }))
        }
    }
}

async fn resolve_session_hashes(cli: &Cli, key_hashes: &[String]) -> Result<Value> {
    request(
        cli,
        Method::POST,
        "/admin/session-routes",
        Some(serde_json::json!({ "keyHashes": key_hashes })),
    )
    .await
}

fn discover_codex_session_ids(limit: usize) -> Result<Vec<Uuid>> {
    let codex_home = env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
        .context("neither CODEX_HOME nor HOME is set")?;
    let root = codex_home.join("sessions");
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut directories = vec![root];
    let mut discovered = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory)
            .with_context(|| format!("reading Codex session directory {}", directory.display()))?
        {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                directories.push(entry.path());
            } else if file_type.is_file()
                && let Some(session_id) = session_id_from_rollout_path(&entry.path())
            {
                let modified = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                discovered.push((modified, session_id));
            }
        }
    }
    discovered.sort_by_key(|item| std::cmp::Reverse(item.0));
    let mut seen = HashSet::new();
    Ok(discovered
        .into_iter()
        .filter_map(|(_, session_id)| seen.insert(session_id).then_some(session_id))
        .take(limit)
        .collect())
}

fn session_id_from_rollout_path(path: &Path) -> Option<Uuid> {
    if path.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let session_id = stem.get(stem.len().checked_sub(36)?..)?;
    Uuid::parse_str(session_id).ok()
}

async fn run_settings(cli: &Cli, command: &SettingsCommand) -> Result<()> {
    match command {
        SettingsCommand::Get => print_response(cli, Method::GET, "/admin/settings", None).await,
        SettingsCommand::Set { key, value } => {
            let parsed = serde_json::from_str::<Value>(value)
                .unwrap_or_else(|_| Value::String(value.clone()));
            print_response(
                cli,
                Method::PUT,
                "/admin/settings",
                Some(serde_json::json!({ key: parsed })),
            )
            .await
        }
    }
}

async fn print_response(cli: &Cli, method: Method, path: &str, body: Option<Value>) -> Result<()> {
    let value = request(cli, method, path, body).await?;
    print_json(&value)
}

async fn request(cli: &Cli, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
    let client = reqwest::Client::new();
    let url = format!("{}{}", cli.base_url.trim_end_matches('/'), path);
    let mut req = client.request(method, url);
    if let Some(token) = cli.admin_token.as_deref() {
        req = req.bearer_auth(token);
    }
    if let Some(body) = body {
        req = req.json(&body);
    }
    let response = req.send().await.context("admin API request failed")?;
    let status = response.status();
    if status == reqwest::StatusCode::NO_CONTENT {
        return Ok(serde_json::json!({"status":"ok"}));
    }
    let text = response
        .text()
        .await
        .context("reading admin API response")?;
    let value = serde_json::from_str::<Value>(&text).unwrap_or(Value::String(text));
    if !status.is_success() {
        anyhow::bail!("admin API returned {status}: {}", value);
    }
    Ok(value)
}

fn print_json(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::session_id_from_rollout_path;

    #[test]
    fn reads_session_id_from_rollout_filename() {
        let session_id = session_id_from_rollout_path(Path::new(
            "rollout-2026-07-22T12-00-00-019f8a00-1234-7abc-8def-0123456789ab.jsonl",
        ));
        assert_eq!(
            session_id.map(|value| value.to_string()).as_deref(),
            Some("019f8a00-1234-7abc-8def-0123456789ab")
        );
        assert_eq!(session_id_from_rollout_path(Path::new("notes.jsonl")), None);
        assert_eq!(
            session_id_from_rollout_path(Path::new(
                "rollout-2026-07-22T12-00-00-019f8a00-1234-7abc-8def-0123456789ab.txt",
            )),
            None
        );
    }
}
