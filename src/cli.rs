use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::{BufRead, BufReader},
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
    /// Release a session tree's sticky routes so the normal router chooses again.
    Rebalance {
        session_id: Uuid,
        #[arg(long)]
        dry_run: bool,
    },
    /// Route a session tree to one account label or UUID.
    Reroute {
        session_id: Uuid,
        #[arg(long)]
        to: String,
        #[arg(long)]
        dry_run: bool,
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
            let catalog = discover_codex_session_catalog()?;
            let tree = catalog.tree_for(*session_id)?;
            let root_key_hash = root_session_key_hash(tree.root_id);
            let aliases = session_tree_aliases(&tree)?;
            let key_hashes = aliases
                .iter()
                .map(|alias| alias.key_hash.clone())
                .collect::<Vec<_>>();
            let response = resolve_session_hashes(cli, &key_hashes).await?;
            let routes = response
                .get("sessionRoutes")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let route = routes
                .iter()
                .find(|route| route.get("keyHash").and_then(Value::as_str) == Some(&root_key_hash))
                .or_else(|| routes.first())
                .cloned();
            print_json(&serde_json::json!({
                "sessionId": session_id,
                "rootSessionId": tree.root_id,
                "fingerprint": &root_key_hash[..12],
                "route": route,
                "routes": routes,
                "semantics": "last_routed",
                "note": "A sticky route is not proof that the session process is currently connected."
            }))
        }
        SessionsCommand::List { limit } => {
            let limit = (*limit).clamp(1, 500);
            let catalog = discover_codex_session_catalog()?;
            // Group the complete rollout graph before imposing the API's 500-key cap.
            let trees = catalog.session_trees().into_iter().take(500);
            let by_hash = trees
                .map(|tree| (root_session_key_hash(tree.root_id), tree.root_id))
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
        SessionsCommand::Rebalance {
            session_id,
            dry_run,
        } => {
            let catalog = discover_codex_session_catalog()?;
            let tree = catalog.mutation_tree_for(*session_id)?;
            let payload = session_action_payload(&tree, "rebalance", None, *dry_run)?;
            print_response(
                cli,
                Method::POST,
                "/admin/session-routes/actions",
                Some(payload),
            )
            .await
        }
        SessionsCommand::Reroute {
            session_id,
            to,
            dry_run,
        } => {
            let target = to.trim();
            if target.is_empty() {
                bail!("--to must be a non-empty account label or UUID");
            }
            let catalog = discover_codex_session_catalog()?;
            let tree = catalog.mutation_tree_for(*session_id)?;
            let payload = session_action_payload(&tree, "reroute", Some(target), *dry_run)?;
            print_response(
                cli,
                Method::POST,
                "/admin/session-routes/actions",
                Some(payload),
            )
            .await
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

const SESSION_META_SCAN_LINE_LIMIT: usize = 64;
const SESSION_META_SCAN_BYTE_LIMIT: usize = 1024 * 1024;
const MAX_SESSION_ACTION_KEYS: usize = 500;
const SESSION_ALIAS_KINDS: [&str; 4] = [
    "session_id",
    "thread_id",
    "conversation_id",
    "prompt_cache_key",
];

#[derive(Debug, Clone)]
struct RolloutNode {
    id: Uuid,
    session_id: Option<Uuid>,
    parent_thread_id: Option<Uuid>,
    forked_from_id: Option<Uuid>,
    modified: SystemTime,
}

impl RolloutNode {
    fn has_same_links(&self, other: &Self) -> bool {
        self.session_id == other.session_id
            && self.parent_thread_id == other.parent_thread_id
            && self.forked_from_id == other.forked_from_id
    }

    fn parent_id(&self) -> Option<Uuid> {
        self.parent_thread_id.or(self.forked_from_id)
    }
}

#[derive(Debug, Clone)]
struct SessionTree {
    root_id: Uuid,
    member_ids: Vec<Uuid>,
    modified: SystemTime,
}

#[derive(Debug, Default)]
struct SessionCatalog {
    nodes: HashMap<Uuid, RolloutNode>,
    issues: HashMap<Uuid, String>,
}

impl SessionCatalog {
    fn discover(root: &Path) -> Result<Self> {
        if !root.exists() {
            return Ok(Self::default());
        }

        let mut catalog = Self::default();
        let mut directories = vec![root.to_path_buf()];
        while let Some(directory) = directories.pop() {
            for entry in fs::read_dir(&directory)
                .with_context(|| format!("reading Codex directory {}", directory.display()))?
            {
                let entry = entry?;
                let file_type = entry.file_type()?;
                if file_type.is_dir() {
                    directories.push(entry.path());
                    continue;
                }
                if !file_type.is_file()
                    || entry
                        .path()
                        .extension()
                        .and_then(|extension| extension.to_str())
                        != Some("jsonl")
                {
                    continue;
                }

                let path = entry.path();
                let filename_id = session_id_from_rollout_path(&path);
                let modified = entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                match first_session_meta(&path)? {
                    Some(meta) => match rollout_node_from_meta(&meta, modified) {
                        Ok(node) => {
                            if let Some(filename_id) = filename_id
                                && filename_id != node.id
                            {
                                let message = format!(
                                    "session metadata ID does not match rollout filename {}",
                                    path.display()
                                );
                                catalog.issues.insert(filename_id, message.clone());
                                catalog.issues.insert(node.id, message);
                            }
                            if let (Some(parent), Some(forked)) =
                                (node.parent_thread_id, node.forked_from_id)
                                && parent != forked
                            {
                                catalog.issues.insert(
                                    node.id,
                                    "session metadata has conflicting parent and fork IDs"
                                        .to_string(),
                                );
                            }
                            catalog.insert_node(node);
                        }
                        Err(error) => {
                            if let Some(filename_id) = filename_id {
                                catalog.issues.insert(
                                    filename_id,
                                    format!(
                                        "invalid session metadata in {}: {error}",
                                        path.display()
                                    ),
                                );
                            }
                        }
                    },
                    None => {
                        if let Some(id) = filename_id {
                            catalog.insert_node(RolloutNode {
                                id,
                                session_id: None,
                                parent_thread_id: None,
                                forked_from_id: None,
                                modified,
                            });
                        }
                    }
                }
            }
        }
        Ok(catalog)
    }

    fn insert_node(&mut self, node: RolloutNode) {
        if let Some(existing) = self.nodes.get_mut(&node.id) {
            if !existing.has_same_links(&node) {
                self.issues.insert(
                    node.id,
                    "conflicting session metadata exists for one rollout ID".to_string(),
                );
            }
            if node.modified > existing.modified {
                existing.modified = node.modified;
            }
        } else {
            self.nodes.insert(node.id, node);
        }
    }

    fn canonical_root(&self, id: Uuid) -> Result<Uuid> {
        self.resolve_root(id, &mut HashSet::new())
    }

    fn resolve_root(&self, id: Uuid, visiting: &mut HashSet<Uuid>) -> Result<Uuid> {
        if let Some(issue) = self.issues.get(&id) {
            bail!("cannot resolve this session tree: {issue}");
        }
        let Some(node) = self.nodes.get(&id) else {
            return Ok(id);
        };
        if !visiting.insert(id) {
            bail!("cannot resolve this session tree because its parent graph contains a cycle");
        }

        let root = if let Some(session_id) = node.session_id {
            let authoritative_root = if session_id == id {
                id
            } else if self.nodes.contains_key(&session_id) {
                let resolved = self.resolve_root(session_id, visiting)?;
                if resolved != session_id {
                    bail!(
                        "cannot resolve this session tree because its authoritative root is not a root"
                    );
                }
                session_id
            } else if let Some(issue) = self.issues.get(&session_id) {
                bail!("cannot resolve this session tree: {issue}");
            } else {
                session_id
            };
            if let Some(parent_id) = node.parent_id() {
                let parent_root = self.resolve_root(parent_id, visiting)?;
                if parent_root != authoritative_root {
                    bail!(
                        "cannot resolve this session tree because its session_id conflicts with its parent root"
                    );
                }
            }
            authoritative_root
        } else if let Some(parent_id) = node.parent_id() {
            self.resolve_root(parent_id, visiting)?
        } else {
            id
        };
        visiting.remove(&id);
        Ok(root)
    }

    fn session_trees(&self) -> Vec<SessionTree> {
        let mut grouped = HashMap::<Uuid, (HashSet<Uuid>, SystemTime)>::new();
        for node in self.nodes.values() {
            let Ok(root_id) = self.canonical_root(node.id) else {
                continue;
            };
            let group = grouped
                .entry(root_id)
                .or_insert_with(|| (HashSet::from([root_id]), SystemTime::UNIX_EPOCH));
            group.0.insert(node.id);
            for alias in [node.session_id, node.parent_thread_id, node.forked_from_id]
                .into_iter()
                .flatten()
            {
                if self.canonical_root(alias).ok() == Some(root_id) {
                    group.0.insert(alias);
                }
            }
            if node.modified > group.1 {
                group.1 = node.modified;
            }
        }

        let mut trees = grouped
            .into_iter()
            .map(|(root_id, (members, modified))| {
                let mut member_ids = members.into_iter().collect::<Vec<_>>();
                member_ids.sort_unstable();
                SessionTree {
                    root_id,
                    member_ids,
                    modified,
                }
            })
            .collect::<Vec<_>>();
        trees.sort_by(|left, right| {
            right
                .modified
                .cmp(&left.modified)
                .then_with(|| left.root_id.cmp(&right.root_id))
        });
        trees
    }

    fn tree_for(&self, id: Uuid) -> Result<SessionTree> {
        let root_id = self.canonical_root(id)?;
        Ok(self
            .session_trees()
            .into_iter()
            .find(|tree| tree.root_id == root_id)
            .unwrap_or_else(|| SessionTree {
                root_id,
                member_ids: vec![root_id],
                modified: SystemTime::UNIX_EPOCH,
            }))
    }

    fn mutation_tree_for(&self, id: Uuid) -> Result<SessionTree> {
        let connected_ids = self.connected_ids(id);
        for connected_id in &connected_ids {
            if let Some(issue) = self.issues.get(connected_id) {
                bail!(
                    "cannot mutate session routes because the requested rollout tree has inconsistent metadata: {issue}"
                );
            }
        }
        // Validate the complete connected component so a malformed descendant cannot produce a
        // partial action-key set, while unrelated damaged rollout history remains harmless.
        for connected_id in connected_ids {
            if self.nodes.contains_key(&connected_id) {
                self.canonical_root(connected_id)?;
            }
        }
        self.tree_for(id)
    }

    fn connected_ids(&self, id: Uuid) -> HashSet<Uuid> {
        let mut connected = HashSet::from([id]);
        loop {
            let mut changed = false;
            for node in self.nodes.values() {
                let related = [
                    Some(node.id),
                    node.session_id,
                    node.parent_thread_id,
                    node.forked_from_id,
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
                if related
                    .iter()
                    .any(|related_id| connected.contains(related_id))
                {
                    for related_id in related {
                        changed |= connected.insert(related_id);
                    }
                }
            }
            if !changed {
                return connected;
            }
        }
    }
}

#[derive(Debug)]
struct SessionAlias {
    kind: &'static str,
    key_hash: String,
}

fn discover_codex_session_catalog() -> Result<SessionCatalog> {
    let codex_home = env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".codex")))
        .context("neither CODEX_HOME nor HOME is set")?;
    SessionCatalog::discover(&codex_home)
}

fn first_session_meta(path: &Path) -> Result<Option<Value>> {
    let file = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut total_bytes = 0;
    for _ in 0..SESSION_META_SCAN_LINE_LIMIT {
        line.clear();
        let read = reader
            .read_line(&mut line)
            .with_context(|| format!("reading {}", path.display()))?;
        if read == 0 {
            break;
        }
        total_bytes += read;
        if total_bytes > SESSION_META_SCAN_BYTE_LIMIT {
            break;
        }
        let Ok(value) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        if value.get("type").and_then(Value::as_str) == Some("session_meta") {
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn rollout_node_from_meta(meta: &Value, modified: SystemTime) -> Result<RolloutNode> {
    let payload = meta
        .get("payload")
        .and_then(Value::as_object)
        .context("session_meta payload must be an object")?;
    let id = required_uuid_field(payload.get("id"), "id")?;
    Ok(RolloutNode {
        id,
        session_id: optional_uuid_field(payload.get("session_id"), "session_id")?,
        parent_thread_id: optional_uuid_field(payload.get("parent_thread_id"), "parent_thread_id")?,
        forked_from_id: optional_uuid_field(payload.get("forked_from_id"), "forked_from_id")?,
        modified,
    })
}

fn required_uuid_field(value: Option<&Value>, name: &str) -> Result<Uuid> {
    let value = value
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .with_context(|| format!("session_meta payload.{name} must be a UUID string"))?;
    Uuid::parse_str(value).with_context(|| format!("session_meta payload.{name} must be a UUID"))
}

fn optional_uuid_field(value: Option<&Value>, name: &str) -> Result<Option<Uuid>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if value.trim().is_empty() => Ok(None),
        Some(Value::String(value)) => Uuid::parse_str(value)
            .map(Some)
            .with_context(|| format!("session_meta payload.{name} must be a UUID")),
        Some(_) => bail!("session_meta payload.{name} must be a UUID string"),
    }
}

fn root_session_key_hash(root_id: Uuid) -> String {
    crate::db::affinity_hash("session_id", &root_id.to_string())
}

fn session_tree_aliases(tree: &SessionTree) -> Result<Vec<SessionAlias>> {
    let mut ids = tree.member_ids.clone();
    ids.sort_unstable();
    ids.retain(|id| *id != tree.root_id);
    ids.insert(0, tree.root_id);

    let mut aliases = Vec::new();
    let mut seen = HashSet::new();
    for id in ids {
        for kind in SESSION_ALIAS_KINDS {
            let key_hash = crate::db::affinity_hash(kind, &id.to_string());
            if seen.insert(key_hash.clone()) {
                if aliases.len() == MAX_SESSION_ACTION_KEYS {
                    bail!(
                        "session tree requires more than {MAX_SESSION_ACTION_KEYS} route keys; refusing to operate on only part of the tree"
                    );
                }
                aliases.push(SessionAlias { kind, key_hash });
            }
        }
    }
    Ok(aliases)
}

fn session_action_payload(
    tree: &SessionTree,
    action: &str,
    target: Option<&str>,
    dry_run: bool,
) -> Result<Value> {
    let keys = session_tree_aliases(tree)?
        .into_iter()
        .map(|alias| {
            serde_json::json!({
                "kind": alias.kind,
                "keyHash": alias.key_hash,
            })
        })
        .collect::<Vec<_>>();
    Ok(serde_json::json!({
        "action": action,
        "rootKeyHash": root_session_key_hash(tree.root_id),
        "keys": keys,
        "target": target,
        "dryRun": dry_run,
    }))
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
    use std::{
        collections::HashSet,
        fs,
        path::{Path, PathBuf},
        time::SystemTime,
    };

    use clap::Parser;
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{
        Cli, Command, MAX_SESSION_ACTION_KEYS, RolloutNode, SESSION_ALIAS_KINDS, SessionCatalog,
        SessionTree, SessionsCommand, root_session_key_hash, session_action_payload,
        session_id_from_rollout_path, session_tree_aliases,
    };

    struct TestCodexHome {
        root: PathBuf,
    }

    impl TestCodexHome {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!("codex-lb-cli-{}", Uuid::new_v4()));
            fs::create_dir_all(&root).expect("create test Codex home");
            Self { root }
        }

        fn write_rollout(&self, directory: &str, filename_id: Uuid, records: &[Value]) -> PathBuf {
            let directory = self.root.join(directory);
            fs::create_dir_all(&directory).expect("create rollout directory");
            let path = directory.join(format!("rollout-test-{filename_id}.jsonl"));
            let contents = records
                .iter()
                .map(Value::to_string)
                .collect::<Vec<_>>()
                .join("\n");
            fs::write(&path, format!("{contents}\n")).expect("write rollout");
            path
        }
    }

    impl Drop for TestCodexHome {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn test_uuid(value: u128) -> Uuid {
        Uuid::from_u128(value)
    }

    fn session_meta(
        id: Uuid,
        session_id: Option<Uuid>,
        parent_thread_id: Option<Uuid>,
        forked_from_id: Option<Uuid>,
    ) -> Value {
        json!({
            "type": "session_meta",
            "payload": {
                "id": id,
                "session_id": session_id,
                "parent_thread_id": parent_thread_id,
                "forked_from_id": forked_from_id,
            }
        })
    }

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

    #[test]
    fn modern_metadata_groups_a_whole_root_tree_before_limits() {
        let home = TestCodexHome::new();
        let root = test_uuid(1);
        let child = test_uuid(2);
        let grandchild = test_uuid(3);
        let other_root = test_uuid(4);
        home.write_rollout(
            "sessions/2026/07/23",
            root,
            &[session_meta(root, Some(root), None, None)],
        );
        home.write_rollout(
            "archived_sessions",
            child,
            &[
                json!({"type":"ignored_before_meta"}),
                session_meta(child, Some(root), Some(root), Some(root)),
            ],
        );
        home.write_rollout(
            "nested/agent/tree",
            grandchild,
            &[session_meta(
                grandchild,
                Some(root),
                Some(child),
                Some(child),
            )],
        );
        home.write_rollout(
            "sessions/2026/07/22",
            other_root,
            &[session_meta(other_root, Some(other_root), None, None)],
        );

        let catalog = SessionCatalog::discover(&home.root).expect("discover catalog");
        let trees = catalog.session_trees();
        assert_eq!(trees.len(), 2, "children must not consume list slots");
        let tree = catalog.tree_for(grandchild).expect("resolve descendant");
        assert_eq!(tree.root_id, root);
        assert_eq!(
            tree.member_ids.into_iter().collect::<HashSet<_>>(),
            HashSet::from([root, child, grandchild])
        );
    }

    #[test]
    fn legacy_metadata_follows_parent_graph_and_filename_fallback() {
        let home = TestCodexHome::new();
        let root = test_uuid(10);
        let child = test_uuid(11);
        let filename_only = test_uuid(12);
        home.write_rollout(
            "sessions/legacy",
            root,
            &[session_meta(root, None, None, None)],
        );
        home.write_rollout(
            "sessions/legacy",
            child,
            &[session_meta(child, None, Some(root), Some(root))],
        );
        home.write_rollout(
            "other-history",
            filename_only,
            &[json!({"type":"event_msg","payload":{}})],
        );

        let catalog = SessionCatalog::discover(&home.root).expect("discover catalog");
        assert_eq!(catalog.tree_for(child).unwrap().root_id, root);
        assert_eq!(
            catalog.tree_for(filename_only).unwrap().root_id,
            filename_only
        );
    }

    #[test]
    fn mutation_rejects_metadata_filename_mismatch() {
        let home = TestCodexHome::new();
        let filename_id = test_uuid(20);
        let metadata_id = test_uuid(21);
        home.write_rollout(
            "sessions/mismatch",
            filename_id,
            &[session_meta(metadata_id, Some(metadata_id), None, None)],
        );

        let catalog = SessionCatalog::discover(&home.root).expect("discover catalog");
        let error = catalog
            .mutation_tree_for(metadata_id)
            .expect_err("mismatch must block mutation");
        assert!(error.to_string().contains("inconsistent"));
    }

    #[test]
    fn mutation_rejects_cycles_and_duplicate_conflicts() {
        let cycle_home = TestCodexHome::new();
        let first = test_uuid(30);
        let second = test_uuid(31);
        cycle_home.write_rollout(
            "sessions/cycle",
            first,
            &[session_meta(first, None, Some(second), Some(second))],
        );
        cycle_home.write_rollout(
            "sessions/cycle",
            second,
            &[session_meta(second, None, Some(first), Some(first))],
        );
        let catalog = SessionCatalog::discover(&cycle_home.root).expect("discover cycle");
        assert!(
            catalog
                .mutation_tree_for(first)
                .expect_err("cycle must fail")
                .to_string()
                .contains("cycle")
        );

        let conflict_home = TestCodexHome::new();
        let duplicate = test_uuid(32);
        let root_a = test_uuid(33);
        let root_b = test_uuid(34);
        conflict_home.write_rollout(
            "sessions/a",
            duplicate,
            &[session_meta(duplicate, Some(root_a), None, None)],
        );
        conflict_home.write_rollout(
            "sessions/b",
            duplicate,
            &[session_meta(duplicate, Some(root_b), None, None)],
        );
        let catalog = SessionCatalog::discover(&conflict_home.root).expect("discover conflict");
        assert!(
            catalog
                .mutation_tree_for(duplicate)
                .expect_err("conflict must fail")
                .to_string()
                .contains("inconsistent")
        );
    }

    #[test]
    fn mutation_validates_only_the_requested_connected_tree() {
        let home = TestCodexHome::new();
        let valid_root = test_uuid(40);
        let unrelated_filename = test_uuid(41);
        let unrelated_metadata = test_uuid(42);
        home.write_rollout(
            "sessions/valid",
            valid_root,
            &[session_meta(valid_root, Some(valid_root), None, None)],
        );
        home.write_rollout(
            "sessions/unrelated-mismatch",
            unrelated_filename,
            &[session_meta(
                unrelated_metadata,
                Some(unrelated_metadata),
                None,
                None,
            )],
        );

        let catalog = SessionCatalog::discover(&home.root).expect("discover catalog");
        assert!(catalog.mutation_tree_for(valid_root).is_ok());
        assert!(catalog.mutation_tree_for(unrelated_metadata).is_err());
    }

    #[test]
    fn modern_session_root_must_match_the_parent_graph() {
        let home = TestCodexHome::new();
        let claimed_root = test_uuid(50);
        let parent_root = test_uuid(51);
        let child = test_uuid(52);
        home.write_rollout(
            "sessions/conflicting-modern-root",
            claimed_root,
            &[session_meta(claimed_root, Some(claimed_root), None, None)],
        );
        home.write_rollout(
            "sessions/conflicting-modern-root",
            parent_root,
            &[session_meta(parent_root, Some(parent_root), None, None)],
        );
        home.write_rollout(
            "sessions/conflicting-modern-root",
            child,
            &[session_meta(
                child,
                Some(claimed_root),
                Some(parent_root),
                Some(parent_root),
            )],
        );

        let catalog = SessionCatalog::discover(&home.root).expect("discover catalog");
        let error = catalog
            .mutation_tree_for(child)
            .expect_err("conflicting modern and parent roots must fail");
        assert!(error.to_string().contains("conflicts with its parent root"));
    }

    #[test]
    fn action_aliases_are_typed_unique_bounded_and_privacy_safe() {
        let root = test_uuid(100);
        let member_ids = (100..225).map(test_uuid).collect::<Vec<_>>();
        let tree = SessionTree {
            root_id: root,
            member_ids: member_ids.clone(),
            modified: SystemTime::UNIX_EPOCH,
        };

        let aliases = session_tree_aliases(&tree).expect("500 aliases fit exactly");
        assert_eq!(aliases.len(), MAX_SESSION_ACTION_KEYS);
        assert_eq!(aliases[0].key_hash, root_session_key_hash(root));
        assert_eq!(
            aliases
                .iter()
                .take(SESSION_ALIAS_KINDS.len())
                .map(|alias| alias.kind)
                .collect::<Vec<_>>(),
            SESSION_ALIAS_KINDS
        );
        assert_eq!(
            aliases
                .iter()
                .map(|alias| alias.key_hash.as_str())
                .collect::<HashSet<_>>()
                .len(),
            aliases.len()
        );

        let payload = session_action_payload(&tree, "reroute", Some("account-a"), true)
            .expect("build bounded payload");
        assert_eq!(payload["action"], "reroute");
        assert_eq!(payload["target"], "account-a");
        assert_eq!(payload["dryRun"], true);
        assert_eq!(
            payload["keys"].as_array().unwrap().len(),
            MAX_SESSION_ACTION_KEYS
        );
        let serialized = payload.to_string();
        for id in member_ids {
            assert!(!serialized.contains(&id.to_string()));
        }
    }

    #[test]
    fn action_aliases_reject_a_partial_tree() {
        let root = test_uuid(400);
        let tree = SessionTree {
            root_id: root,
            member_ids: (400..526).map(test_uuid).collect(),
            modified: SystemTime::UNIX_EPOCH,
        };

        let error = session_tree_aliases(&tree).expect_err("more than 500 keys must fail");
        assert!(
            error
                .to_string()
                .contains("refusing to operate on only part")
        );
        assert!(session_action_payload(&tree, "rebalance", None, false).is_err());
    }

    #[test]
    fn parses_rebalance_and_reroute_commands() {
        let session_id = test_uuid(200);
        let cli = Cli::try_parse_from([
            "codex-lb-rs",
            "sessions",
            "rebalance",
            &session_id.to_string(),
            "--dry-run",
        ])
        .expect("parse rebalance");
        assert!(matches!(
            cli.command,
            Command::Sessions {
                command: SessionsCommand::Rebalance {
                    session_id: parsed,
                    dry_run: true,
                }
            } if parsed == session_id
        ));

        let cli = Cli::try_parse_from([
            "codex-lb-rs",
            "sessions",
            "reroute",
            &session_id.to_string(),
            "--to",
            "account-b",
        ])
        .expect("parse reroute");
        assert!(matches!(
            cli.command,
            Command::Sessions {
                command: SessionsCommand::Reroute {
                    session_id: parsed,
                    to,
                    dry_run: false,
                }
            } if parsed == session_id && to == "account-b"
        ));
    }

    #[test]
    fn duplicate_identical_metadata_merges_latest_rollout_without_conflict() {
        let id = test_uuid(300);
        let mut catalog = SessionCatalog::default();
        catalog.insert_node(RolloutNode {
            id,
            session_id: Some(id),
            parent_thread_id: None,
            forked_from_id: None,
            modified: SystemTime::UNIX_EPOCH,
        });
        catalog.insert_node(RolloutNode {
            id,
            session_id: Some(id),
            parent_thread_id: None,
            forked_from_id: None,
            modified: SystemTime::now(),
        });
        assert!(catalog.mutation_tree_for(id).is_ok());
    }
}
