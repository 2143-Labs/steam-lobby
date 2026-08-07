-- Abstract account id: every users row gets a stable UUID. Gameplay tables and
-- the wire protocol stay keyed by steam_id (still the PK); this id is the
-- provider-agnostic account key used as the JWT `sub`.
ALTER TABLE users ADD COLUMN id UUID UNIQUE NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN primary_provider TEXT NOT NULL DEFAULT 'steam';

-- One row per (provider, provider_uid); provider_uid is the subject id inside
-- that provider (SteamID64 decimal string for 'steam').
CREATE TABLE user_identities (
    provider     TEXT NOT NULL,
    provider_uid TEXT NOT NULL,
    user_id      UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider, provider_uid),
    UNIQUE (user_id, provider)
);
CREATE INDEX user_identities_user_idx ON user_identities (user_id);
