-- no-transaction
CREATE INDEX CONCURRENTLY player_state_active_match_token_idx
    ON player_state (active_match_token)
    WHERE active_match_token IS NOT NULL;
