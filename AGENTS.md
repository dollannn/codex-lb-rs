# AGENTS.md

## Repo shape
- Single Rust 2024 crate, not a workspace; use direct `cargo` commands (no Makefile/justfile/CI config in repo).
- Backend-only MVP: no frontend, no WebSockets, and only responses proxying is implemented.
- `src/main.rs` routes between `serve`, `migrate up`, and CLI API commands; `src/lib.rs::build_app` wires Axum `/health`, `/admin/*`, and proxy routes.
- Postgres schema lives in `migrations/` and is embedded through `sqlx::migrate!("./migrations")`; both `cargo run -- migrate up` and `cargo run -- serve` apply migrations.

## Local commands
- Start DB: `docker compose up -d postgres` (override host port with `CODEX_LB_POSTGRES_PORT=55432`).
- Local env/key setup: `cp .env.example .env && mkdir -p .local`; keep `CODEX_LB_ENCRYPTION_KEY_FILE` stable because changing it makes imported tokens unreadable.
- Run app: `cargo run -- migrate up` then `cargo run -- serve`.
- Normal checks: `cargo fmt --check`, `cargo check`, `cargo test`.
- Check resolved config: `cargo run -- config check`.

## CLI/API quirks
- Admin CLI commands for `accounts`, `usage`, `logs`, and `settings` are thin HTTP clients; start the server first and pass `--admin-token` or set `CODEX_LB_ADMIN_TOKEN` when admin auth is enabled.
- CLI base URL defaults to `http://127.0.0.1:2455`; override with `--base-url` or `CODEX_LB_BASE_URL`.
- If `CODEX_LB_ADMIN_TOKEN` or `CODEX_LB_PROXY_API_TOKEN` is unset, the corresponding API is intentionally unauthenticated with only a startup warning.
- Proxy routes are `/backend-api/codex/responses` and `/v1/responses`; model list stubs are `/backend-api/codex/models` and `/v1/models`.

## Tests and database safety
- `cargo test` skips the destructive Postgres integration smoke unless `CODEX_LB_TEST_DATABASE_URL` is set.
- Integration smoke applies migrations, truncates app tables, starts an in-process fake upstream, and covers admin/proxy auth, 429 failover, SSE usage logging, and usage summary aggregation.
- Run it with a dedicated DB whose URL contains `test`:
  `CODEX_LB_TEST_DATABASE_URL=postgres://codex_lb:codex_lb@127.0.0.1:5432/codex_lb_test cargo test --test postgres_proxy postgres_admin_and_proxy_failover_smoke -- --nocapture`
- The integration test refuses URLs without `test` unless `CODEX_LB_ALLOW_DESTRUCTIVE_TEST_DB=1`; do not point it at a real/dev DB.

## Data/config gotchas
- Runtime settings are rows in Postgres (`routing_strategy`, `proxy_max_attempts`, `rate_limit_cooldown_seconds`), updated via `cargo run -- --admin-token <token> settings set <key> <value>`; only `round_robin` routing is implemented.
- Account import expects Codex/ChatGPT auth JSON with `tokens.idToken`, `tokens.accessToken`, and `tokens.refreshToken` (snake_case aliases also work); do not commit `.env`, `.local/encryption.key`, or imported `auth.json` files.
