# codex-lb-rs

A small, local-first Rust daemon that load-balances Codex requests across multiple ChatGPT accounts. It keeps encrypted OAuth tokens and usage history in SQLite, refreshes account usage in the background, and exposes cached pool status for Waybar and other local clients.

The current implementation intentionally stays narrow:

- native Codex Responses API proxying over streaming HTTP and WebSockets, including remote compaction;
- sticky, usage-aware account selection with cooldown and failover;
- account import, isolated device login, token refresh, and pause/reactivate controls;
- dynamically discovered quota windows and pace calculations (no hard-coded 5-hour/7-day assumptions);
- local status, request logs, and a Waybar JSON endpoint.

There is no dashboard, PostgreSQL, or Docker dependency. Codex CLI is the primary client and can reuse the local Responses WebSocket bridge; OpenCode can use the OpenAI-compatible `/v1` HTTP routes.

## Install as a user service

The installer builds an optimized release binary, installs it at `~/.local/bin/codex-lb-rs`, installs the systemd user unit, and enables and starts it:

```bash
./scripts/install-user-service.sh
```

The service is restarted on failure and starts with the user's systemd session. Its private state is kept under `${XDG_STATE_HOME:-$HOME/.local/state}/codex-lb-rs` by systemd (normally `~/.local/state/codex-lb-rs`):

- `codex-lb.sqlite` — accounts, current quotas, usage samples, affinity, and request logs;
- `encryption.key` — AES-GCM key used to encrypt OAuth tokens in the database.

Useful service commands:

```bash
systemctl --user status codex-lb-rs.service
journalctl --user -u codex-lb-rs.service -f
systemctl --user restart codex-lb-rs.service
```

If the daemon must keep running while the user is logged out, enable user lingering once:

```bash
loginctl enable-linger "$USER"
```

The unit binds only to `127.0.0.1` by default and applies a restrictive systemd sandbox. See [Local development](#local-development) if systemd is unavailable.

## Add accounts

Account labels are stable, non-sensitive names shown in status output and Waybar. Neutral names such as `account-a` and `account-b` work well.

### Log in once per account

This runs Codex's device login in a separate `CODEX_HOME` for each label, then imports the resulting credentials into the daemon:

```bash
codex-lb-rs accounts login account-a
codex-lb-rs accounts login account-b
```

The isolated login homes live under `${XDG_DATA_HOME:-$HOME/.local/share}/codex-lb-rs/login-homes/`. They prevent one login from replacing another account's Codex auth file.

### Import existing credentials

An existing Codex auth file can be imported without another login:

```bash
codex-lb-rs accounts import ~/.codex/auth.json --label account-a
```

An existing OpenCode OAuth slot can also be imported once:

```bash
codex-lb-rs accounts import-opencode \
  ~/.local/share/opencode/auth.json \
  --provider openai \
  --label account-b
```

Importing immediately attempts an upstream token/usage refresh. A stale access token is fine when its refresh token is still valid; warnings in the import response indicate when a fresh device login is required.

Verify the pool without contacting OpenAI again:

```bash
codex-lb-rs accounts list
codex-lb-rs status
```

Treat auth files, the SQLite database, and `encryption.key` as secrets. Back up the database and key together while the service is stopped; a replacement key cannot decrypt previously imported tokens.

## Point Codex CLI at the pool

Add the provider to the user-level `~/.codex/config.toml`. Keep your existing `model` and reasoning settings:

```toml
model_provider = "codex-lb"

[model_providers.codex-lb]
name = "Local Codex pool"
base_url = "http://127.0.0.1:2455/backend-api/codex"
wire_api = "responses"
supports_websockets = true
```

Provider redirection belongs in the user config; Codex deliberately ignores `model_provider` and `model_providers` in project-local `.codex/config.toml` files. The custom provider does not need the account bearer token: the daemon selects an account and supplies its encrypted-at-rest credential upstream. See Codex's [advanced configuration guide](https://developers.openai.com/codex/config-advanced) for the provider and config-layer behavior.

If `CODEX_LB_PROXY_API_TOKEN` is enabled on the daemon, give Codex the same value through an environment variable:

```toml
[model_providers.codex-lb]
name = "Local Codex pool"
base_url = "http://127.0.0.1:2455/backend-api/codex"
wire_api = "responses"
supports_websockets = true
env_key = "CODEX_LB_PROXY_API_TOKEN"
```

Then start Codex with that variable available. The daemon rejects browser-originated proxy requests even on loopback, because browser WebSockets are not protected by CORS. Set both API tokens before exposing the listener beyond `127.0.0.1`.

Codex stores model/provider settings with existing threads. A Codex process that was already running when this config changed keeps its in-memory provider, and a normal resume can restore the provider saved with the old thread. Move an old direct-OpenAI thread to the pool with an explicit one-time override:

```bash
codex resume -c 'model_provider="codex-lb"' <session-id>
```

Do not copy an isolated account's `auth.json` back into `~/.codex`: two independently refreshing clients can rotate the same refresh token and invalidate each other. New threads use `codex-lb` directly from the user config.

Quick checks:

```bash
curl --fail http://127.0.0.1:2455/health
codex-lb-rs status
codex exec "Reply with OK only."
```

## Waybar

`codex-lb-rs status --waybar` emits Waybar's custom-module JSON format. It reads cached SQLite data and does not perform an upstream request on each poll:

```jsonc
"custom/codex-pool": {
  "exec": "$HOME/.local/bin/codex-lb-rs status --waybar",
  "return-type": "json",
  "interval": 15,
  "tooltip": true
}
```

The bar stays compact: each account is reduced to its initial, selected marker, core Codex quota percentage, and pace/state symbol (for example `●W:11%↑ · P:!`). Hovering opens a grouped mini-dashboard with full aliases, readiness, the core Codex quota bar, remaining capacity, reset time, pace, and locally recorded activity. Additional feature meters are retained in the status API but omitted from Waybar. CSS classes include `codex-pool` plus health/pace classes from the returned payload.

For another local status client, use:

```bash
curl http://127.0.0.1:2455/api/v1/status
curl http://127.0.0.1:2455/api/v1/status/waybar
```

## Optional OpenCode client

Use OpenCode's built-in `openai` provider with a `baseURL` override so it stays on OpenCode's Responses API path. Merge this into `~/.config/opencode/opencode.json` rather than defining an `@ai-sdk/openai-compatible` provider:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "provider": {
    "openai": {
      "options": {
        "baseURL": "http://127.0.0.1:2455/v1",
        "apiKey": "{env:CODEX_LB_PROXY_API_TOKEN}"
      }
    }
  }
}
```

Keep your existing `openai/<model>` selection. When proxy authentication is disabled on the loopback-only daemon, OpenCode may still require a non-empty client-side API-key value; a dummy local value is sufficient and is not a security boundary. When proxy authentication is enabled, the environment variable must exactly match the daemon's value.

## Configuration

The daemon reads environment variables and an optional `.env` in its working directory.

| Variable | Default | Purpose |
| --- | --- | --- |
| `CODEX_LB_DATABASE_URL` | `sqlite://${XDG_DATA_HOME:-$HOME/.local/share}/codex-lb-rs/codex-lb.sqlite` | SQLite database URL |
| `CODEX_LB_ENCRYPTION_KEY_FILE` | `${XDG_DATA_HOME:-$HOME/.local/share}/codex-lb-rs/encryption.key` | Stable AES-GCM key file |
| `HOST` | `127.0.0.1` | Listen host |
| `PORT` / `CODEX_LB_PORT` | `2455` | Listen port |
| `CODEX_LB_ADMIN_TOKEN` | unset | Optional bearer token for `/admin/*` |
| `CODEX_LB_PROXY_API_TOKEN` | unset | Optional bearer token for Responses/model routes |
| `CODEX_LB_BASE_URL` | `http://127.0.0.1:2455` | Base URL used by CLI client commands |
| `CODEX_LB_UPSTREAM_BASE_URL` | `https://chatgpt.com/backend-api` | ChatGPT backend API base |
| `CODEX_LB_AUTH_BASE_URL` | `https://auth.openai.com` | OAuth base URL |
| `CODEX_LB_OAUTH_CLIENT_ID` | Codex public client ID | OAuth client used for refresh |
| `CODEX_LB_OAUTH_SCOPE` | `openid profile email` | OAuth refresh scope |
| `CODEX_LB_PROXY_REQUEST_BUDGET_SECONDS` | `600` | Upstream request timeout |
| `CODEX_LB_TOKEN_REFRESH_INTERVAL_DAYS` | `8` | Proactive token refresh age |
| `CODEX_LB_USAGE_REFRESH_INTERVAL_SECONDS` | `120` (minimum `30`) | Background quota refresh period |

`cargo run -- config check` prints the effective high-level configuration without displaying secrets.

The user unit has no proxy or admin token by default because it is loopback-only. If the listen address is exposed beyond the local machine, configure both tokens in a systemd user override before changing `HOST`:

```bash
systemctl --user edit codex-lb-rs.service
```

For example:

```ini
[Service]
Environment=CODEX_LB_ADMIN_TOKEN=replace-with-a-long-random-value
Environment=CODEX_LB_PROXY_API_TOKEN=replace-with-a-different-long-random-value
```

Restart the unit after changing its override.

## Operations

Normal CLI operations are thin local HTTP clients, so the daemon must be running. Add `--admin-token ...` or set `CODEX_LB_ADMIN_TOKEN` when admin authentication is enabled.

```bash
codex-lb-rs accounts list
codex-lb-rs accounts pause <account-id>
codex-lb-rs accounts reactivate <account-id>
codex-lb-rs accounts refresh-token <account-id>
codex-lb-rs accounts refresh-usage <account-id>
codex-lb-rs usage summary
codex-lb-rs usage refresh
codex-lb-rs logs list --limit 50
```

Runtime settings are stored in SQLite and apply without a daemon restart:

```bash
codex-lb-rs settings get
codex-lb-rs settings set routing_strategy usage_weighted
codex-lb-rs settings set proxy_max_attempts 3
codex-lb-rs settings set rate_limit_cooldown_seconds 120
codex-lb-rs settings set sticky_session_ttl_seconds 604800
codex-lb-rs settings set usage_sample_retention_days 30
```

`usage_weighted` is the default. It prefers available accounts with fewer in-flight requests and more remaining core capacity, while sticky session keys keep a conversation on one account when possible. `round_robin` is also available.

## Local development

No external database is needed:

```bash
cp .env.example .env
mkdir -p .local
cargo run -- migrate up
cargo run -- serve
```

Migrations also run automatically on `serve`. The SQLite connection uses WAL mode, foreign keys, a short busy timeout, and a small connection pool to keep idle memory and contention low.

Development checks:

```bash
cargo fmt --check
cargo check --all-targets
cargo test
```

The integration smoke test uses a temporary SQLite database and an in-process fake upstream. It covers HTTP and WebSocket routing, retryable failover, per-turn account revalidation, browser-origin rejection, request accounting, and temporary-pool-exhaustion responses. It needs no Docker service or destructive-test opt-in.
