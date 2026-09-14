-- no-transaction
CREATE INDEX CONCURRENTLY browser_sessions_user_provider_idx
    ON browser_sessions (user_id, auth_provider);
