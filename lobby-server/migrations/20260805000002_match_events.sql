CREATE TABLE match_events (
    id BIGSERIAL PRIMARY KEY,
    match_token TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('paired', 'accepted', 'declined')),
    steam_id BIGINT REFERENCES users(steam_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX match_events_token_idx ON match_events (match_token);
