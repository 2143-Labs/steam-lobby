//! `PlayerStore` impl for `PostgresStore`: users, player_state, ratings, and
//! token-version rows.
use super::*;

#[async_trait]
impl PlayerStore for PostgresStore {
    async fn upsert_player(&self, user_id: uuid::Uuid, display_name: &str) -> Result<()> {
        // The users row already exists (created by find_or_create_user at
        // login). Keep an existing name when the arg is empty — enter_menus
        // passes "" on every WS connect and must not wipe the stored name
        // (a guest's entire identity IS the stored name).
        sqlx::query(
            "UPDATE users SET display_name = COALESCE(NULLIF($1, ''), display_name) \
             WHERE id = $2",
        )
        .bind(display_name)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;

        sqlx::query(
            "INSERT INTO player_state (user_id, state, last_heartbeat) \
             VALUES ($1, 'InMenus', NOW()) \
             ON CONFLICT (user_id) DO NOTHING",
        )
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(())
    }

    async fn get_player_state(&self, user_id: uuid::Uuid) -> Result<Option<PlayerInfo>> {
        let row = sqlx::query_as::<_, (uuid::Uuid, String, String, DateTime<Utc>)>(
            "SELECT u.id, u.display_name, ps.state, ps.last_heartbeat \
             FROM users u JOIN player_state ps ON u.id = ps.user_id \
             WHERE u.id = $1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(row.map(|(uid, name, state_str, hb)| PlayerInfo {
            user_id: uid,
            display_name: name,
            state: parse_player_state(&state_str),
            last_heartbeat: hb,
        }))
    }

    async fn get_rating(&self, user_id: uuid::Uuid, mode: &str) -> Result<OpenSkillRating> {
        let row = sqlx::query_as::<_, (f64, f64, DateTime<Utc>)>(
            "SELECT mu, sigma, last_updated FROM ratings WHERE user_id = $1 AND game_mode = $2",
        )
        .bind(user_id)
        .bind(mode)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        match row {
            Some((mu, sigma, last_updated)) => Ok(OpenSkillRating {
                mu,
                sigma,
                last_updated,
            }),
            None => {
                let rating = OpenSkillRating {
                    mu: 25.0,
                    sigma: 25.0 / 3.0,
                    last_updated: Utc::now(),
                };
                sqlx::query(
                    "INSERT INTO ratings (user_id, game_mode, mu, sigma, last_updated) \
                     VALUES ($1, $2, $3, $4, NOW()) \
                     ON CONFLICT (user_id, game_mode) DO NOTHING",
                )
                .bind(user_id)
                .bind(mode)
                .bind(rating.mu)
                .bind(rating.sigma)
                .execute(&self.pool)
                .await
                .map_err(map_db_error)?;
                Ok(rating)
            }
        }
    }

    async fn update_rating(
        &self,
        user_id: uuid::Uuid,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE ratings SET mu = $1, sigma = $2, last_updated = NOW() \
             WHERE user_id = $3 AND game_mode = $4",
        )
        .bind(rating.mu)
        .bind(rating.sigma)
        .bind(user_id)
        .bind(mode)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn set_player_state(&self, user_id: uuid::Uuid, state: PlayerState) -> Result<()> {
        let state_str = match state {
            PlayerState::InMenus => "InMenus",
            PlayerState::Queueing => "Queueing",
            PlayerState::MatchAccepted => "MatchAccepted",
            PlayerState::InMatch => "InMatch",
            PlayerState::Reporting => "Reporting",
        };
        sqlx::query("UPDATE player_state SET state = $1 WHERE user_id = $2")
            .bind(state_str)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn update_heartbeat(&self, user_id: uuid::Uuid) -> Result<()> {
        sqlx::query("UPDATE player_state SET last_heartbeat = NOW() WHERE user_id = $1")
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn get_token_version(&self, user_id: uuid::Uuid) -> Result<u32> {
        let row = sqlx::query_as::<_, (i32,)>(
            "SELECT COALESCE((SELECT token_version FROM users WHERE id = $1), 0)",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(row.0 as u32)
    }

    async fn bump_token_version(&self, user_id: uuid::Uuid) -> Result<()> {
        sqlx::query(
            "INSERT INTO users (id, display_name, token_version) \
             VALUES ($1, '', 1) \
             ON CONFLICT (id) DO UPDATE SET token_version = users.token_version + 1",
        )
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }
}

impl PostgresStore {
    /// Find or create the user row for a provider identity, returning the
    /// player key (users.id). `provider` is the provider id ("steam",
    /// "discord", "au2143", ...); `provider_uid` is the subject id inside that
    /// provider (SteamID64 decimal string for 'steam'). `verified` = the id
    /// came from genuine provider verification: ensure the identity row
    /// exists. Test-token minting passes false and never creates identity rows.
    pub async fn find_or_create_user(
        &self,
        provider: &str,
        provider_uid: &str,
        display_name: &str,
        verified: bool,
    ) -> Result<uuid::Uuid> {
        // Identity already linked -> update the display name and return.
        let existing = sqlx::query_as::<_, (uuid::Uuid,)>(
            "SELECT user_id FROM user_identities WHERE provider = $1 AND provider_uid = $2",
        )
        .bind(provider)
        .bind(provider_uid)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;
        if let Some((user_id,)) = existing {
            sqlx::query(
                "UPDATE users SET display_name = $1, last_login_at = NOW() WHERE id = $2",
            )
            .bind(display_name)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
            return Ok(user_id);
        }

        let user_id: uuid::Uuid = if provider == "steam" {
            // The uid is a decimal SteamID64 — the users.steam_id column holds
            // the Steam identity, so upsert by it.
            sqlx::query_as::<_, (uuid::Uuid,)>(
                "INSERT INTO users (steam_id, display_name, primary_provider) \
                 VALUES ($1, $2, 'steam') \
                 ON CONFLICT (steam_id) DO UPDATE SET display_name = EXCLUDED.display_name, last_login_at = NOW() \
                 RETURNING id",
            )
            .bind(provider_uid.parse::<i64>().unwrap_or(0))
            .bind(display_name)
            .fetch_one(&self.pool)
            .await
            .map_err(map_db_error)?
            .0
        } else {
            // Discord/au2143: steam_id stays NULL; primary_provider = provider.
            sqlx::query_as::<_, (uuid::Uuid,)>(
                "INSERT INTO users (display_name, primary_provider) \
                 VALUES ($1, $2) \
                 RETURNING id",
            )
            .bind(display_name)
            .bind(provider)
            .fetch_one(&self.pool)
            .await
            .map_err(map_db_error)?
            .0
        };

        if verified {
            sqlx::query(
                "INSERT INTO user_identities (provider, provider_uid, user_id, last_login_at) \
                 VALUES ($1, $2, $3, NOW()) \
                 ON CONFLICT (provider, provider_uid) DO UPDATE SET last_login_at = NOW()",
            )
            .bind(provider)
            .bind(provider_uid)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        }
        Ok(user_id)
    }

    /// The player's display name (stored at login from the provider userinfo).
    pub async fn get_display_name(&self, user_id: uuid::Uuid) -> Result<Option<String>> {
        let row = sqlx::query_as::<_, (String,)>("SELECT display_name FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(row.map(|r| r.0))
    }

    /// Admin flag for au.2143.me logins (from the Pocket ID `groups` claim).
    /// Storage only — nothing consumes the flag yet. Best-effort at the
    /// au2143 callback; no other caller touches the column.
    pub async fn set_admin_flag(&self, user_id: uuid::Uuid, is_admin: bool) -> Result<()> {
        sqlx::query("UPDATE users SET is_admin = $1 WHERE id = $2")
            .bind(is_admin)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    /// Read-only lookup of the abstract account id (users.id) for a Steam ID.
    /// None when the user row is missing (shouldn't happen — users is the
    /// parent of player_state/ratings — but callers fall back to a placeholder).
    /// Callers convert in Step 7; deleted once ticker + pair_matches notify move
    /// to get_display_name.
    pub async fn get_user_id(&self, steam_id: SteamId) -> Result<Option<uuid::Uuid>> {
        let row = sqlx::query_as::<_, (uuid::Uuid,)>("SELECT id FROM users WHERE steam_id = $1")
            .bind(steam_id as i64)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(row.map(|r| r.0))
    }
}
