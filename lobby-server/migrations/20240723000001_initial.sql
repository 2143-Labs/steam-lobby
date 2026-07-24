CREATE TABLE users (
    steam_id BIGINT PRIMARY KEY,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE player_state (
    steam_id BIGINT PRIMARY KEY REFERENCES users(steam_id),
    state TEXT NOT NULL DEFAULT 'InMenus',
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE ratings (
    steam_id BIGINT NOT NULL REFERENCES users(steam_id),
    game_mode TEXT NOT NULL,
    mu DOUBLE PRECISION NOT NULL DEFAULT 25.0,
    sigma DOUBLE PRECISION NOT NULL DEFAULT 8.333,
    last_updated TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (steam_id, game_mode)
);

CREATE TABLE matchmaking_queue (
    steam_id BIGINT NOT NULL,
    game_mode TEXT NOT NULL,
    match_difficulty TEXT NOT NULL DEFAULT 'normal',
    mu DOUBLE PRECISION NOT NULL,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (steam_id, game_mode)
);

CREATE TABLE matches (
    match_token TEXT PRIMARY KEY,
    player_a BIGINT NOT NULL,
    player_a_difficulty TEXT NOT NULL,
    player_b BIGINT NOT NULL,
    player_b_difficulty TEXT NOT NULL,
    game_mode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PendingAccept',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    accepted_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ
);

CREATE TABLE match_reports (
    id BIGSERIAL PRIMARY KEY,
    match_token TEXT NOT NULL REFERENCES matches(match_token),
    reporting_player BIGINT NOT NULL,
    winner BIGINT,
    demo_hash TEXT,
    reported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(match_token, reporting_player)
);

CREATE TABLE match_results (
    match_token TEXT PRIMARY KEY REFERENCES matches(match_token),
    outcome TEXT NOT NULL,
    mu_change_a DOUBLE PRECISION,
    mu_change_b DOUBLE PRECISION,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
