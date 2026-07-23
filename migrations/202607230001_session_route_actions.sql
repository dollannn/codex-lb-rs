ALTER TABLE affinity ADD COLUMN route_generation INTEGER NOT NULL DEFAULT 0
    CHECK (route_generation >= 0);

CREATE TABLE session_roots (
    root_key_hash TEXT PRIMARY KEY
        CHECK (
            length(root_key_hash) = 64
            AND root_key_hash NOT GLOB '*[^0-9a-f]*'
        ),
    route_generation INTEGER NOT NULL DEFAULT 0
        CHECK (route_generation >= 0),
    last_action TEXT
        CHECK (last_action IS NULL OR last_action IN ('rebalance', 'reroute')),
    requested_account_id BLOB
        CHECK (requested_account_id IS NULL OR length(requested_account_id) = 16)
        REFERENCES accounts(id) ON DELETE SET NULL,
    effective_account_id BLOB
        CHECK (effective_account_id IS NULL OR length(effective_account_id) = 16)
        REFERENCES accounts(id) ON DELETE SET NULL,
    action_status TEXT
        CHECK (
            action_status IS NULL
            OR action_status IN ('applied', 'fallback', 'pending', 'no_op')
        ),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_session_roots_requested_account
    ON session_roots (requested_account_id, updated_at DESC);

CREATE INDEX idx_session_roots_effective_account
    ON session_roots (effective_account_id, updated_at DESC);

CREATE INDEX idx_session_roots_pending
    ON session_roots (updated_at, root_key_hash)
    WHERE action_status = 'pending';

CREATE TABLE session_route_keys (
    key_hash TEXT PRIMARY KEY
        CHECK (
            length(key_hash) = 64
            AND key_hash NOT GLOB '*[^0-9a-f]*'
        ),
    root_key_hash TEXT NOT NULL
        REFERENCES session_roots(root_key_hash) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (length(trim(kind)) > 0 AND length(kind) <= 64),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_session_route_keys_root
    ON session_route_keys (root_key_hash, key_hash);
