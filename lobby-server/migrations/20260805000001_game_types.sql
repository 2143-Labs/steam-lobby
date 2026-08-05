ALTER TABLE matches ADD COLUMN game_type TEXT NOT NULL DEFAULT 'p2p';
ALTER TABLE matches ADD COLUMN server_address TEXT;
ALTER TABLE matches ADD COLUMN join_token TEXT;
ALTER TABLE matches ADD COLUMN result_secret TEXT;
