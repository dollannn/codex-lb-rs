use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use reqwest::Method;
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(
    name = "codex-lb-rs",
    version,
    about = "Postgres-backed Codex account load balancer"
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
    Settings {
        #[command(subcommand)]
        command: SettingsCommand,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
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
    Import { path: String },
    Pause { id: Uuid },
    Reactivate { id: Uuid },
    Remove { id: Uuid },
    RefreshToken { id: Uuid },
    RefreshUsage { id: Uuid },
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
        Command::Serve(_) | Command::Migrate(_) => Ok(()),
    }
}

async fn run_accounts(cli: &Cli, command: &AccountsCommand) -> Result<()> {
    match command {
        AccountsCommand::List => print_response(cli, Method::GET, "/admin/accounts", None).await,
        AccountsCommand::Import { path } => {
            let raw = tokio::fs::read(path)
                .await
                .with_context(|| format!("reading {path}"))?;
            let payload: Value = serde_json::from_slice(&raw).context("auth file must be JSON")?;
            print_response(cli, Method::POST, "/admin/accounts/import", Some(payload)).await
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
    let value = serde_json::from_str::<Value>(&text).unwrap_or_else(|_| Value::String(text));
    if !status.is_success() {
        anyhow::bail!("admin API returned {status}: {}", value);
    }
    Ok(value)
}

fn print_json(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
