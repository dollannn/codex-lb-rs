ALTER TABLE request_logs ADD COLUMN selection_reason TEXT
    CHECK (
        selection_reason IS NULL
        OR selection_reason IN (
            'sticky', 'usage_weighted', 'round_robin', 'failover', 'websocket_reuse'
        )
    );
