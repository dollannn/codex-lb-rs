# AGENTS.md

## Repo shape

- Single Rust 2024 crate, not a workspace; use direct `cargo` commands.
- Local-first daemon: no frontend, PostgreSQL, or Docker dependency; Codex uses streaming HTTP or the local Responses WebSocket bridge.
- `src/main.rs` handles `serve`, `migrate up`, and CLI API commands. `src/lib.rs::build_app` wires health, status, admin, and proxy routes.
- SQLite migrations live in `migrations/` and are embedded with `sqlx::migrate!`. Both `cargo run -- migrate up` and `cargo run -- serve` apply them.
- `packaging/systemd/codex-lb-rs.service` is a hardened systemd user unit. `scripts/install-user-service.sh` builds release mode, installs the binary/unit atomically, and enables/restarts the service.

## Local commands

- Optional dev env: `cp .env.example .env && mkdir -p .local`.
- Run: `cargo run -- migrate up` then `cargo run -- serve`; the explicit migration step is optional because `serve` also migrates.
- Install/restart the user service: `./scripts/install-user-service.sh`.
- Service logs: `journalctl --user -u codex-lb-rs.service -f`.
- Checks: `cargo fmt --check`, `cargo check --all-targets`, `cargo test`.
- Resolved high-level config: `cargo run -- config check`.

## CLI and routes

- Account/status/admin commands are thin HTTP clients; start the daemon first. Their base URL defaults to `http://127.0.0.1:2455` and can be changed with `--base-url` or `CODEX_LB_BASE_URL`.
- Account bootstrap supports `accounts login <label>`, `accounts import <path> --label <label>`, and `accounts import-opencode <path> --provider openai --label <label>`.
- `accounts login` deliberately uses a label-specific `CODEX_HOME` below the app data directory so multiple device logins do not overwrite each other.
- `POST /backend-api/codex/responses` and `POST /v1/responses` proxy streaming HTTP; `GET` on those paths upgrades the Responses WebSocket bridge. Compact/model routes remain HTTP-only.
- Cached status routes are `/api/v1/status` and `/api/v1/status/waybar`; `status --waybar` emits a valid offline fallback even when the daemon cannot be reached.
- Admin routes use `CODEX_LB_ADMIN_TOKEN` when configured. Proxy/model routes use `CODEX_LB_PROXY_API_TOKEN` when configured. Keep the default service loopback-only if either token is unset.

## Tests

- `cargo test` always runs the SQLite integration smoke against a temporary directory; no external database or destructive-test environment variable is needed.
- The smoke starts an in-process fake upstream and covers SQLite pragmas/migrations, admin/proxy auth, account selection/failover, SSE and WebSocket usage logging, per-turn WebSocket eligibility, browser-origin rejection, and usage aggregation. Keep it self-contained.
- Prefer focused unit tests for auth parsing, quota normalization, pace calculations, and setting bounds, plus the SQLite smoke for route/database behavior.

## Data and behavior gotchas

- Default state is `${XDG_DATA_HOME:-$HOME/.local/share}/codex-lb-rs/{codex-lb.sqlite,encryption.key}` outside the systemd unit; the unit uses systemd's private `%S/codex-lb-rs` state directory (normally `~/.local/state/codex-lb-rs`).
- SQLite is configured for WAL, foreign keys, a busy timeout, and a deliberately small pool. Preserve that lean local profile unless measurements justify a change.
- Tokens are AES-GCM encrypted but the database, key, imported auth JSON, and isolated login homes are still sensitive. Never log tokens or commit those files. Changing the key makes existing token rows unreadable.
- Fresh databases default to `usage_weighted` routing. Runtime settings also include `proxy_max_attempts`, `rate_limit_cooldown_seconds`, `sticky_session_ttl_seconds`, and `usage_sample_retention_days`.
- Quota windows are dynamic upstream data. Do not hard-code primary as 5 hours, secondary as 7 days, or assume every account/model has both windows.
- Sticky routing stores a hash of the affinity key, not the raw session/conversation identifier. Ensure every selected-account path decrements its in-flight lease, including errors and disconnected streams.
- The Waybar path must remain a cheap cached SQLite read; upstream usage refresh belongs in the background scheduler (default 120 seconds, minimum 30).
- Provider configuration for Codex must be documented as user-level `~/.codex/config.toml`; project config cannot redirect auth/providers. Set `supports_websockets = true` only after the installed service includes the tested bridge.
- Proxy routes reject every request carrying an `Origin` header. Preserve that defense unless proxy authentication becomes mandatory, because arbitrary webpages can otherwise reach loopback WebSockets without CORS protection.
