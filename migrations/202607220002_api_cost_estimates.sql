ALTER TABLE request_logs ADD COLUMN cache_write_input_tokens INTEGER;
ALTER TABLE request_logs ADD COLUMN effective_model TEXT;
ALTER TABLE request_logs ADD COLUMN effective_service_tier TEXT;
ALTER TABLE request_logs ADD COLUMN api_pricing_version TEXT;
ALTER TABLE request_logs ADD COLUMN api_cost_status TEXT;
ALTER TABLE request_logs ADD COLUMN api_cost_lower_nano_usd INTEGER;
ALTER TABLE request_logs ADD COLUMN api_cost_upper_nano_usd INTEGER;

CREATE INDEX idx_request_logs_pending_api_cost
    ON request_logs (id)
    WHERE api_cost_status IS NULL;

ALTER TABLE account_runtime_state ADD COLUMN cached_input_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN observed_cache_write_input_tokens INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN api_cost_lower_nano_usd INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN api_cost_upper_nano_usd INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN api_cost_complete_request_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN api_cost_partial_request_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE account_runtime_state ADD COLUMN api_cost_unpriced_request_count INTEGER NOT NULL DEFAULT 0;

-- Every pre-existing request starts uncovered. The resumable backfill moves retained,
-- priceable rows into the complete/partial counters; already-pruned history stays here.
UPDATE account_runtime_state
SET api_cost_unpriced_request_count = request_count;
