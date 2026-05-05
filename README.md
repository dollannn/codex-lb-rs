# codex-lb-rs

Postgres-backed Rust MVP for load-balancing Codex/ChatGPT account tokens across proxy requests.

## Status

This is an early backend-only MVP. It currently focuses on:

- importing Codex/ChatGPT auth JSON files,
- storing tokens encrypted locally,
- selecting active accounts for `/backend-api/codex/responses` and `/v1/responses`,
- refreshing account tokens and usage snapshots,
- exposing admin operations through both HTTP and CLI.

The frontend, full Python compatibility, WebSockets, and non-responses endpoints are intentionally deferred.

## Quickstart

1. Start Postgres:

   ```bash
   docker compose up -d postgres
   ```

   If local port `5432` is already in use, start Postgres on another host port
   and update `CODEX_LB_DATABASE_URL` accordingly:

   ```bash
   CODEX_LB_POSTGRES_PORT=55432 docker compose up -d postgres
   ```

2. Create local config:

   ```bash
   cp .env.example .env
   mkdir -p .local
   ```

3. Apply migrations:

   ```bash
   cargo run -- migrate up
   ```

4. Start the server:

   ```bash
   cargo run -- serve
   ```

5. Import an auth JSON file from another terminal:

   ```bash
   cargo run -- --admin-token change-me-admin-token accounts import ./auth.json
   cargo run -- --admin-token change-me-admin-token accounts list
   ```

6. Send a proxied responses request:

   ```bash
   curl -N http://127.0.0.1:2455/backend-api/codex/responses \
     -H 'Authorization: Bearer change-me-proxy-token' \
     -H 'Content-Type: application/json' \
     -d '{"model":"gpt-5.1-codex-mini","input":"Say hello"}'
   ```

## Configuration

Environment variables are loaded from `.env` when present.

| Variable | Default | Purpose |
| --- | --- | --- |
| `CODEX_LB_DATABASE_URL` | `postgres://codex_lb:codex_lb@127.0.0.1:5432/codex_lb` | Postgres connection string |
| `HOST` | `127.0.0.1` | Server bind host |
| `PORT` / `CODEX_LB_PORT` | `2455` | Server bind port |
| `CODEX_LB_ADMIN_TOKEN` | unset | Optional bearer token for `/admin/*` |
| `CODEX_LB_PROXY_API_TOKEN` | unset | Optional bearer token for proxy endpoints |
| `CODEX_LB_UPSTREAM_BASE_URL` | `https://chatgpt.com/backend-api` | ChatGPT backend API base |
| `CODEX_LB_AUTH_BASE_URL` | `https://auth.openai.com` | OAuth base URL |
| `CODEX_LB_ENCRYPTION_KEY_FILE` | `~/.codex-lb-rs/encryption.key` | AES-GCM token encryption key |
| `CODEX_LB_PROXY_REQUEST_BUDGET_SECONDS` | `600` | Per upstream request timeout |
| `CODEX_LB_TOKEN_REFRESH_INTERVAL_DAYS` | `8` | Token refresh age threshold |

Use `cargo run -- config check` to print the effective high-level config.

## Admin CLI

The CLI is intentionally a thin API client for normal operations. Pass `--admin-token` or set `CODEX_LB_ADMIN_TOKEN` when admin auth is enabled.

```bash
cargo run -- --admin-token change-me-admin-token accounts list
cargo run -- --admin-token change-me-admin-token accounts pause <account-id>
cargo run -- --admin-token change-me-admin-token accounts reactivate <account-id>
cargo run -- --admin-token change-me-admin-token accounts refresh-token <account-id>
cargo run -- --admin-token change-me-admin-token accounts refresh-usage <account-id>
cargo run -- --admin-token change-me-admin-token usage summary
cargo run -- --admin-token change-me-admin-token usage refresh
cargo run -- --admin-token change-me-admin-token logs list --limit 50
```

## Runtime settings

Runtime settings live in Postgres and can be updated without restarting the server:

```bash
cargo run -- --admin-token change-me-admin-token settings set proxy_max_attempts 3
cargo run -- --admin-token change-me-admin-token settings set rate_limit_cooldown_seconds 120
cargo run -- --admin-token change-me-admin-token settings get
```

Current settings:

- `routing_strategy`: currently only `round_robin` is implemented.
- `proxy_max_attempts`: maximum accounts tried per proxy request.
- `rate_limit_cooldown_seconds`: cooldown applied after upstream `429` responses.

## Development checks

```bash
cargo fmt --check
cargo check
cargo test
```

The default test suite skips destructive Postgres integration coverage unless a
test database URL is explicitly provided. The integration smoke test applies
migrations, truncates app tables, starts an in-process fake upstream, and checks
admin auth, proxy auth, 429 failover, SSE usage logging, and usage summary
aggregation.

```bash
createdb codex_lb_test
CODEX_LB_TEST_DATABASE_URL=postgres://codex_lb:codex_lb@127.0.0.1:5432/codex_lb_test \
  cargo test --test postgres_proxy -- --nocapture
```

When using the Compose database on a non-default host port, create the test
database inside the container and point the test at that port:

```bash
CODEX_LB_POSTGRES_PORT=55432 docker compose up -d postgres
until docker compose exec -T postgres pg_isready -U codex_lb -d codex_lb; do sleep 1; done
docker compose exec -T postgres createdb -U codex_lb codex_lb_test
CODEX_LB_TEST_DATABASE_URL=postgres://codex_lb:codex_lb@127.0.0.1:55432/codex_lb_test \
  cargo test --test postgres_proxy -- --nocapture
```

For safety, `CODEX_LB_TEST_DATABASE_URL` must contain `test` unless you also set
`CODEX_LB_ALLOW_DESTRUCTIVE_TEST_DB=1`.
