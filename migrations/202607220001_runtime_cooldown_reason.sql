ALTER TABLE account_runtime_state ADD COLUMN cooldown_reason TEXT;

-- Older builds stored temporary cooldown messages on the account itself. Move
-- any still-live message to runtime state, then clear stale messages from
-- active accounts. Persistent states such as auth_failed keep their reason.
UPDATE account_runtime_state
SET cooldown_reason = (
    SELECT accounts.status_reason
    FROM accounts
    WHERE accounts.id = account_runtime_state.account_id
)
WHERE cooldown_until IS NOT NULL
  AND julianday(cooldown_until) > julianday('now')
  AND EXISTS (
      SELECT 1
      FROM accounts
      WHERE accounts.id = account_runtime_state.account_id
        AND accounts.status = 'active'
        AND accounts.status_reason IS NOT NULL
  );

UPDATE accounts
SET status_reason = NULL,
    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
WHERE status = 'active' AND status_reason IS NOT NULL;
