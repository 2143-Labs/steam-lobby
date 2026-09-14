-- Additive UMVC3 ranked schema. Legacy physical objects and wire columns remain
-- in place for rolling compatibility with older pods.

ALTER TABLE user_identities ADD COLUMN linked_at TIMESTAMPTZ;
UPDATE user_identities SET linked_at = last_login_at WHERE linked_at IS NULL;
ALTER TABLE user_identities
    ALTER COLUMN linked_at SET DEFAULT NOW(),
    ALTER COLUMN linked_at SET NOT NULL;

CREATE VIEW accounts AS
SELECT provider, provider_uid, user_id, last_login_at, linked_at
FROM user_identities;

CREATE TABLE browser_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    auth_provider TEXT NOT NULL,
    csrf_hash BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ NULL
);

CREATE TABLE native_sessions (
    session_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ NULL
);

CREATE TABLE provider_sessions (
    nonce_hash BYTEA PRIMARY KEY,
    provider TEXT NOT NULL CHECK (provider = 'discord'),
    provider_uid TEXT NOT NULL,
    display_name TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    consumed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE link_intents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('native', 'browser')),
    provider_session_hash BYTEA NOT NULL REFERENCES provider_sessions(nonce_hash) ON DELETE RESTRICT,
    browser_session_id UUID NULL REFERENCES browser_sessions(session_id) ON DELETE CASCADE,
    native_session_id UUID NULL REFERENCES native_sessions(session_id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'committed', 'expired')),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '5 minutes',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (
        (kind = 'browser' AND browser_session_id IS NOT NULL AND native_session_id IS NULL)
        OR
        (kind = 'native' AND native_session_id IS NOT NULL AND browser_session_id IS NULL)
    )
);

CREATE TABLE oauth_handoffs (
    state_hash BYTEA PRIMARY KEY,
    provider TEXT NOT NULL CHECK (provider = 'discord'),
    native_session_id UUID NOT NULL REFERENCES native_sessions(session_id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '5 minutes',
    consumed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE oauth_login_states (
    state_hash BYTEA PRIMARY KEY,
    browser_nonce_hash BYTEA NOT NULL,
    provider TEXT NOT NULL CHECK (provider IN ('steam', 'discord', 'au2143')),
    return_to TEXT NOT NULL,
    code_verifier TEXT NULL,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '10 minutes',
    consumed_at TIMESTAMPTZ NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE player_state
    ADD COLUMN active_match_token TEXT NULL REFERENCES matches(match_token) ON DELETE SET NULL;

ALTER TABLE match_reports
    ADD COLUMN outcome TEXT NULL CHECK (outcome IN ('win', 'loss', 'draw')),
    ADD COLUMN score TEXT NULL,
    ADD COLUMN checksum TEXT NULL,
    ADD COLUMN end_frame BIGINT NULL CHECK (end_frame >= 0);

CREATE TABLE umvc3_matches (
    match_token TEXT PRIMARY KEY REFERENCES matches(match_token) ON DELETE CASCADE,
    phase TEXT NOT NULL CHECK (phase IN (
        'AwaitingAccepted', 'AwaitingTrying', 'AwaitingConnect', 'AwaitingReady',
        'Playing', 'AwaitingReport', 'Terminal'
    )),
    terminal_reason TEXT NULL CHECK (terminal_reason IN (
        'declined', 'accept_timeout', 'trying_timeout', 'connect_timeout',
        'ready_timeout', 'playing_timeout', 'resolved', 'disputed'
    )),
    phase_version BIGINT NOT NULL DEFAULT 0,
    next_command_sequence BIGINT NOT NULL DEFAULT 1,
    phase_deadline TIMESTAMPTZ NULL,
    attempt_id UUID NOT NULL DEFAULT gen_random_uuid(),
    lobby_id TEXT NULL,
    lobby_reported_at TIMESTAMPTZ NULL,
    trying_a BOOLEAN NOT NULL DEFAULT FALSE,
    trying_b BOOLEAN NOT NULL DEFAULT FALSE,
    connect_local_a BOOLEAN NOT NULL DEFAULT FALSE,
    connect_peer_a BOOLEAN NOT NULL DEFAULT FALSE,
    connect_local_b BOOLEAN NOT NULL DEFAULT FALSE,
    connect_peer_b BOOLEAN NOT NULL DEFAULT FALSE,
    ready_a BOOLEAN NOT NULL DEFAULT FALSE,
    ready_b BOOLEAN NOT NULL DEFAULT FALSE,
    abandon_vote_a UUID NULL REFERENCES users(id),
    abandon_vote_a_at TIMESTAMPTZ NULL,
    abandon_vote_b UUID NULL REFERENCES users(id),
    abandon_vote_b_at TIMESTAMPTZ NULL,
    original_queued_at_a TIMESTAMPTZ NOT NULL,
    original_queued_at_b TIMESTAMPTZ NOT NULL,
    workflow_started_at TIMESTAMPTZ NULL
);

CREATE TABLE command_inbox (
    session_kind TEXT NOT NULL CHECK (session_kind IN ('native', 'websocket')),
    session_id UUID NOT NULL,
    command_id UUID NOT NULL,
    receipt UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    user_sequence BIGINT NULL,
    match_token TEXT NULL REFERENCES matches(match_token) ON DELETE RESTRICT,
    match_sequence BIGINT NULL,
    expected_phase_version BIGINT NULL,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    payload_hash BYTEA NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'applied', 'rejected')),
    error_code TEXT NULL,
    processed_event_sequence BIGINT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    PRIMARY KEY (session_kind, session_id, command_id),
    UNIQUE (user_id, user_sequence),
    UNIQUE (match_token, match_sequence)
);

CREATE TABLE command_user_counters (
    user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    next_sequence BIGINT NOT NULL DEFAULT 1
);

ALTER TABLE match_events DROP CONSTRAINT match_events_event_type_check;
ALTER TABLE match_events
    ADD COLUMN actor_user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    ADD COLUMN recipient_user_id UUID NULL REFERENCES users(id) ON DELETE CASCADE,
    ADD COLUMN recipient_sequence BIGINT NULL,
    ADD COLUMN payload JSONB NULL;
UPDATE match_events SET actor_user_id = user_id WHERE user_id IS NOT NULL;
ALTER TABLE match_events ADD CONSTRAINT match_events_event_type_check CHECK (event_type IN (
    'paired', 'accepted', 'declined', 'lobby_published', 'phase_changed',
    'command_result', 'resolved', 'disputed', 'phase_expired'
));
ALTER TABLE match_events ADD CONSTRAINT match_events_recipient_sequence_pair_check CHECK (
    (recipient_user_id IS NULL AND recipient_sequence IS NULL)
    OR
    (recipient_user_id IS NOT NULL AND recipient_sequence IS NOT NULL AND recipient_sequence > 0)
);

CREATE FUNCTION mirror_match_event_actor_columns() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.actor_user_id IS NOT NULL AND NEW.user_id IS NOT NULL
       AND NEW.actor_user_id <> NEW.user_id THEN
        RAISE EXCEPTION 'match_events actor_user_id and user_id must match';
    END IF;
    IF NEW.actor_user_id IS NULL THEN
        NEW.actor_user_id := NEW.user_id;
    ELSIF NEW.user_id IS NULL THEN
        NEW.user_id := NEW.actor_user_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER match_events_mirror_actor_columns
BEFORE INSERT OR UPDATE OF actor_user_id, user_id ON match_events
FOR EACH ROW EXECUTE FUNCTION mirror_match_event_actor_columns();

CREATE TABLE event_recipient_counters (
    recipient_user_id UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    next_sequence BIGINT NOT NULL DEFAULT 1
);

ALTER TABLE matchmaking_queue ADD COLUMN lease_expires_at TIMESTAMPTZ NULL;

-- Refuse to collapse two independently-valued rows onto one primary key.
DO $$
DECLARE
    conflicting_rating_users TEXT;
    conflicting_queue_users TEXT;
BEGIN
    SELECT string_agg(user_id::TEXT, ', ' ORDER BY user_id::TEXT)
      INTO conflicting_rating_users
      FROM (
          SELECT user_id
          FROM ratings
          WHERE game_mode IN ('ranked_1v1', 'pong_1v1')
          GROUP BY user_id
          HAVING COUNT(DISTINCT game_mode) = 2
      ) conflicts;
    IF conflicting_rating_users IS NOT NULL THEN
        RAISE EXCEPTION 'cannot rename ranked_1v1 to pong_1v1: conflicting ratings for users %', conflicting_rating_users;
    END IF;

    SELECT string_agg(user_id::TEXT, ', ' ORDER BY user_id::TEXT)
      INTO conflicting_queue_users
      FROM (
          SELECT user_id
          FROM matchmaking_queue
          WHERE game_mode IN ('ranked_1v1', 'pong_1v1')
          GROUP BY user_id
          HAVING COUNT(DISTINCT game_mode) = 2
      ) conflicts;
    IF conflicting_queue_users IS NOT NULL THEN
        RAISE EXCEPTION 'cannot rename ranked_1v1 to pong_1v1: conflicting queue rows for users %', conflicting_queue_users;
    END IF;

    UPDATE ratings SET game_mode = 'pong_1v1' WHERE game_mode = 'ranked_1v1';
    UPDATE matchmaking_queue SET game_mode = 'pong_1v1' WHERE game_mode = 'ranked_1v1';
    UPDATE matches SET game_mode = 'pong_1v1' WHERE game_mode = 'ranked_1v1';
END;
$$;
