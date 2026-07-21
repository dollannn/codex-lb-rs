ALTER TABLE accounts ADD COLUMN label TEXT NOT NULL DEFAULT '';
ALTER TABLE accounts ADD COLUMN access_token_expires_at TEXT;
ALTER TABLE accounts ADD COLUMN last_usage_refresh_at TEXT;
ALTER TABLE accounts ADD COLUMN last_usage_error TEXT;

ALTER TABLE account_runtime_state ADD COLUMN last_request_at TEXT;
ALTER TABLE account_runtime_state ADD COLUMN inflight_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN request_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN successful_request_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN failed_request_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN input_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN output_tokens INTEGER NOT NULL DEFAULT 0;

CREATE TABLE usage_windows (
    account_id BLOB NOT NULL CHECK (length(account_id) = 16)
        REFERENCES accounts(id) ON DELETE CASCADE,
    quota_key TEXT NOT NULL,
    quota_name TEXT NOT NULL,
    source_slot TEXT NOT NULL,
    window_kind TEXT NOT NULL,
    used_percent REAL NOT NULL,
    window_seconds INTEGER,
    reset_at TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (account_id, quota_key, source_slot)
);

CREATE INDEX idx_usage_windows_routing
    ON usage_windows (quota_key, used_percent, reset_at);

CREATE TABLE usage_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id BLOB NOT NULL CHECK (length(account_id) = 16)
        REFERENCES accounts(id) ON DELETE CASCADE,
    quota_key TEXT NOT NULL,
    source_slot TEXT NOT NULL,
    window_kind TEXT NOT NULL,
    used_percent REAL NOT NULL,
    reset_at TEXT,
    recorded_at TEXT NOT NULL
);

CREATE INDEX idx_usage_samples_window_time
    ON usage_samples (account_id, quota_key, source_slot, recorded_at DESC, id DESC);

CREATE TABLE affinity (
    key_hash TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    account_id BLOB NOT NULL CHECK (length(account_id) = 16)
        REFERENCES accounts(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_affinity_account ON affinity (account_id, last_used_at DESC);
CREATE INDEX idx_affinity_last_used ON affinity (last_used_at);

UPDATE settings SET value = '"usage_weighted"', updated_at = CURRENT_TIMESTAMP
WHERE key = 'routing_strategy' AND value = '"round_robin"';

INSERT INTO settings (key, value) VALUES
    ('sticky_session_ttl_seconds', '604800'),
    ('usage_sample_retention_days', '30')
ON CONFLICT (key) DO NOTHING;
