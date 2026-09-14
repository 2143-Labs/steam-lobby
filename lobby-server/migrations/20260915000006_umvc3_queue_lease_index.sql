-- no-transaction
CREATE INDEX CONCURRENTLY matchmaking_queue_native_lease_idx
    ON matchmaking_queue (game_mode, lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;
