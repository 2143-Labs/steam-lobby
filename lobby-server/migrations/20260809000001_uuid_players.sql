-- users: id becomes the PK; steam_id stays as the optional Steam identity.
-- Detach the FKs referencing users(steam_id) first: Postgres refuses to drop a
-- PK that foreign keys depend on, and every dependent table is re-keyed below
-- anyway (player_state, ratings, match_reports, match_events).
ALTER TABLE player_state DROP CONSTRAINT player_state_steam_id_fkey;
ALTER TABLE ratings DROP CONSTRAINT ratings_steam_id_fkey;
ALTER TABLE match_reports DROP CONSTRAINT match_reports_winner_fk;
ALTER TABLE match_events DROP CONSTRAINT match_events_steam_id_fkey;

ALTER TABLE users DROP CONSTRAINT users_pkey;
ALTER TABLE users ALTER COLUMN steam_id DROP NOT NULL;
ALTER TABLE users ADD CONSTRAINT users_steam_id_key UNIQUE (steam_id);
ALTER TABLE users ADD PRIMARY KEY (id);
ALTER TABLE users ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT FALSE;  -- au.2143.me "pvp_admin" group at login (Step 8); no consumer yet

-- player_state: steam_id -> user_id (FK users.id)
ALTER TABLE player_state ADD COLUMN user_id UUID;
UPDATE player_state ps SET user_id = u.id FROM users u WHERE u.steam_id = ps.steam_id;
ALTER TABLE player_state ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE player_state DROP CONSTRAINT player_state_pkey;
ALTER TABLE player_state DROP COLUMN steam_id;
ALTER TABLE player_state ADD PRIMARY KEY (user_id);
ALTER TABLE player_state ADD FOREIGN KEY (user_id) REFERENCES users(id);

-- ratings: PK (steam_id, game_mode) -> (user_id, game_mode)
ALTER TABLE ratings ADD COLUMN user_id UUID;
UPDATE ratings r SET user_id = u.id FROM users u WHERE u.steam_id = r.steam_id;
ALTER TABLE ratings ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE ratings DROP CONSTRAINT ratings_pkey;
ALTER TABLE ratings DROP COLUMN steam_id;
ALTER TABLE ratings ADD PRIMARY KEY (user_id, game_mode);
ALTER TABLE ratings ADD FOREIGN KEY (user_id) REFERENCES users(id);

-- matchmaking_queue: PK (steam_id, game_mode) -> (user_id, game_mode)
ALTER TABLE matchmaking_queue ADD COLUMN user_id UUID;
UPDATE matchmaking_queue q SET user_id = u.id FROM users u WHERE u.steam_id = q.steam_id;
ALTER TABLE matchmaking_queue ALTER COLUMN user_id SET NOT NULL;
ALTER TABLE matchmaking_queue DROP CONSTRAINT matchmaking_queue_pkey;
ALTER TABLE matchmaking_queue DROP COLUMN steam_id;
ALTER TABLE matchmaking_queue ADD PRIMARY KEY (user_id, game_mode);
ALTER TABLE matchmaking_queue ADD FOREIGN KEY (user_id) REFERENCES users(id);

-- matches: player_a/player_b BIGINT -> UUID. Add temp columns, backfill via
-- the steam_id join, drop the old BIGINT columns, then rename the temps.
ALTER TABLE matches ADD COLUMN player_a_uuid UUID;
UPDATE matches m SET player_a_uuid = u.id FROM users u WHERE u.steam_id = m.player_a;
ALTER TABLE matches ADD COLUMN player_b_uuid UUID;
UPDATE matches m SET player_b_uuid = u.id FROM users u WHERE u.steam_id = m.player_b;
ALTER TABLE matches DROP COLUMN player_a, DROP COLUMN player_b;
ALTER TABLE matches RENAME COLUMN player_a_uuid TO player_a;
ALTER TABLE matches RENAME COLUMN player_b_uuid TO player_b;
ALTER TABLE matches ALTER COLUMN player_a SET NOT NULL;
ALTER TABLE matches ALTER COLUMN player_b SET NOT NULL;
ALTER TABLE matches ADD FOREIGN KEY (player_a) REFERENCES users(id);
ALTER TABLE matches ADD FOREIGN KEY (player_b) REFERENCES users(id);

-- match_reports: reporting_player/winner BIGINT -> UUID (winner nullable)
ALTER TABLE match_reports ADD COLUMN reporting_player_uuid UUID, ADD COLUMN winner_uuid UUID;
UPDATE match_reports r SET reporting_player_uuid = u.id FROM users u WHERE u.steam_id = r.reporting_player;
UPDATE match_reports r SET winner_uuid = u.id FROM users u WHERE u.steam_id = r.winner;
ALTER TABLE match_reports ALTER COLUMN reporting_player_uuid SET NOT NULL;
ALTER TABLE match_reports DROP COLUMN reporting_player, DROP COLUMN winner;
ALTER TABLE match_reports RENAME COLUMN reporting_player_uuid TO reporting_player;
ALTER TABLE match_reports RENAME COLUMN winner_uuid TO winner;
ALTER TABLE match_reports ADD FOREIGN KEY (reporting_player) REFERENCES users(id);
ALTER TABLE match_reports ADD FOREIGN KEY (winner) REFERENCES users(id);

-- match_events: steam_id -> user_id (stays nullable; the FK moves)
ALTER TABLE match_events ADD COLUMN user_id UUID;
UPDATE match_events e SET user_id = u.id FROM users u WHERE u.steam_id = e.steam_id;
ALTER TABLE match_events DROP COLUMN steam_id;
ALTER TABLE match_events ADD FOREIGN KEY (user_id) REFERENCES users(id);
