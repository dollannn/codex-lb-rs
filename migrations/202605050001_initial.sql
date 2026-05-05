CREATE TABLE accounts (
    id UUID PRIMARY KEY,
    chatgpt_account_id TEXT,
    email TEXT NOT NULL,
    plan_type TEXT NOT NULL DEFAULT 'unknown',
    encrypted_access_token TEXT NOT NULL,
    encrypted_refresh_token TEXT NOT NULL,
    encrypted_id_token TEXT NOT NULL,
    last_refresh_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'rate_limited', 'quota_exceeded', 'paused', 'auth_failed')),
    status_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_accounts_chatgpt_account_id
    ON accounts (chatgpt_account_id)
    WHERE chatgpt_account_id IS NOT NULL;

CREATE INDEX idx_accounts_status ON accounts (status);

CREATE TABLE account_runtime_state (
    account_id UUID PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    last_selected_at TIMESTAMPTZ,
    cooldown_until TIMESTAMPTZ,
    failure_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_account_runtime_selection
    ON account_runtime_state (cooldown_until, last_selected_at);

CREATE TABLE usage_snapshots (
    id BIGSERIAL PRIMARY KEY,
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    used_percent DOUBLE PRECISION,
    input_tokens BIGINT,
    output_tokens BIGINT,
    reset_at TIMESTAMPTZ,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_usage_snapshots_account_time
    ON usage_snapshots (account_id, recorded_at DESC);

CREATE TABLE request_logs (
    id BIGSERIAL PRIMARY KEY,
    request_id TEXT NOT NULL,
    account_id UUID REFERENCES accounts(id) ON DELETE SET NULL,
    model TEXT,
    status TEXT NOT NULL,
    error_code TEXT,
    error_message TEXT,
    input_tokens BIGINT,
    output_tokens BIGINT,
    cached_input_tokens BIGINT,
    reasoning_tokens BIGINT,
    latency_ms INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_request_logs_created_at ON request_logs (created_at DESC, id DESC);
CREATE INDEX idx_request_logs_account_time ON request_logs (account_id, created_at DESC);
CREATE INDEX idx_request_logs_request_id ON request_logs (request_id);

CREATE TABLE settings (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO settings (key, value) VALUES
    ('routing_strategy', '"round_robin"'::jsonb),
    ('proxy_max_attempts', '2'::jsonb),
    ('rate_limit_cooldown_seconds', '60'::jsonb)
ON CONFLICT (key) DO NOTHING;
