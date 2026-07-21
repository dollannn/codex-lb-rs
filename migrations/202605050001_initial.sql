CREATE TABLE accounts (
    id BLOB PRIMARY KEY NOT NULL CHECK (length(id) = 16),
    chatgpt_account_id TEXT,
    email TEXT NOT NULL,
    plan_type TEXT NOT NULL DEFAULT 'unknown',
    encrypted_access_token TEXT NOT NULL,
    encrypted_refresh_token TEXT NOT NULL,
    encrypted_id_token TEXT NOT NULL,
    last_refresh_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'rate_limited', 'quota_exceeded', 'paused', 'auth_failed')),
    status_reason TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE UNIQUE INDEX idx_accounts_chatgpt_account_id
    ON accounts (chatgpt_account_id)
    WHERE chatgpt_account_id IS NOT NULL;

CREATE INDEX idx_accounts_status ON accounts (status);

CREATE TABLE account_runtime_state (
    account_id BLOB PRIMARY KEY NOT NULL CHECK (length(account_id) = 16)
        REFERENCES accounts(id) ON DELETE CASCADE,
    last_selected_at TEXT,
    cooldown_until TEXT,
    failure_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_account_runtime_selection
    ON account_runtime_state (cooldown_until, last_selected_at);

CREATE TABLE usage_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id BLOB NOT NULL CHECK (length(account_id) = 16)
        REFERENCES accounts(id) ON DELETE CASCADE,
    recorded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    used_percent REAL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    reset_at TEXT,
    raw_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_usage_snapshots_account_time
    ON usage_snapshots (account_id, recorded_at DESC);

CREATE TABLE request_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL,
    account_id BLOB CHECK (account_id IS NULL OR length(account_id) = 16)
        REFERENCES accounts(id) ON DELETE SET NULL,
    model TEXT,
    status TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cached_input_tokens INTEGER,
    reasoning_tokens INTEGER,
    latency_ms INTEGER,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_request_logs_created_at ON request_logs (created_at DESC, id DESC);
CREATE INDEX idx_request_logs_account_time ON request_logs (account_id, created_at DESC);
CREATE INDEX idx_request_logs_request_id ON request_logs (request_id);

CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

INSERT INTO settings (key, value) VALUES
    ('routing_strategy', '"round_robin"'),
    ('proxy_max_attempts', '2'),
    ('rate_limit_cooldown_seconds', '60')
ON CONFLICT (key) DO NOTHING;
