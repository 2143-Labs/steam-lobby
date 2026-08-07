//! `PlayerStore` impl for `PostgresStore`: users, player_state, ratings, and
//! token-version rows.
use super::*;

#[async_trait]
impl PlayerStore for PostgresStore {
    async fn upsert_player(&self, steam_id: SteamId, display_name: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO users (steam_id, display_name, last_login_at) \
             VALUES ($1, $2, NOW()) \
             ON CONFLICT (steam_id) DO UPDATE SET display_name = EXCLUDED.display_name, last_login_at = NOW()",
        )
        .bind(steam_id as i64)
        .bind(display_name)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;

        sqlx::query(
            "INSERT INTO player_state (steam_id, state, last_heartbeat) \
             VALUES ($1, 'InMenus', NOW()) \
             ON CONFLICT (steam_id) DO NOTHING",
        )
        .bind(steam_id as i64)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(())
    }

    async fn get_player_state(&self, steam_id: SteamId) -> Result<Option<PlayerInfo>> {
        let row = sqlx::query_as::<_, (i64, String, String, DateTime<Utc>)>(
            "SELECT u.steam_id, u.display_name, ps.state, ps.last_heartbeat \
             FROM users u JOIN player_state ps ON u.steam_id = ps.steam_id \
             WHERE u.steam_id = $1",
        )
        .bind(steam_id as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(row.map(|(sid, name, state_str, hb)| PlayerInfo {
            steam_id: sid as u64,
            display_name: name,
            state: parse_player_state(&state_str),
            last_heartbeat: hb,
        }))
    }

    async fn get_rating(&self, steam_id: SteamId, mode: &str) -> Result<OpenSkillRating> {
        let row = sqlx::query_as::<_, (f64, f64, DateTime<Utc>)>(
            "SELECT mu, sigma, last_updated FROM ratings WHERE steam_id = $1 AND game_mode = $2",
        )
        .bind(steam_id as i64)
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
                    "INSERT INTO ratings (steam_id, game_mode, mu, sigma, last_updated) \
                     VALUES ($1, $2, $3, $4, NOW()) \
                     ON CONFLICT (steam_id, game_mode) DO NOTHING",
                )
                .bind(steam_id as i64)
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
        steam_id: SteamId,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE ratings SET mu = $1, sigma = $2, last_updated = NOW() \
             WHERE steam_id = $3 AND game_mode = $4",
        )
        .bind(rating.mu)
        .bind(rating.sigma)
        .bind(steam_id as i64)
        .bind(mode)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn set_player_state(&self, steam_id: SteamId, state: PlayerState) -> Result<()> {
        let state_str = match state {
            PlayerState::InMenus => "InMenus",
            PlayerState::Queueing => "Queueing",
            PlayerState::MatchAccepted => "MatchAccepted",
            PlayerState::InMatch => "InMatch",
            PlayerState::Reporting => "Reporting",
        };
        sqlx::query("UPDATE player_state SET state = $1 WHERE steam_id = $2")
            .bind(state_str)
            .bind(steam_id as i64)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn update_heartbeat(&self, steam_id: SteamId) -> Result<()> {
        sqlx::query("UPDATE player_state SET last_heartbeat = NOW() WHERE steam_id = $1")
            .bind(steam_id as i64)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn get_token_version(&self, steam_id: SteamId) -> Result<u32> {
        let row = sqlx::query_as::<_, (i32,)>(
            "SELECT COALESCE((SELECT token_version FROM users WHERE steam_id = $1), 0)",
        )
        .bind(steam_id as i64)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(row.0 as u32)
    }

    async fn bump_token_version(&self, steam_id: SteamId) -> Result<()> {
        sqlx::query(
            "INSERT INTO users (steam_id, display_name, token_version) \
             VALUES ($1, '', 1) \
             ON CONFLICT (steam_id) DO UPDATE SET token_version = users.token_version + 1",
        )
        .bind(steam_id as i64)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }
}

impl PostgresStore {
    /// Find or create the user row for a Steam ID, returning the abstract
    /// account id (users.id). `verified` = the Steam ID came from genuine
    /// Steam verification (OpenID callback or ticket): ensure the
    /// ('steam', steam_id) identity row exists. Test-token minting passes
    /// false and never creates identity rows.
    pub async fn find_or_create_user(
        &self,
        steam_id: SteamId,
        display_name: &str,
        verified: bool,
    ) -> Result<uuid::Uuid> {
        let row = sqlx::query_as::<_, (uuid::Uuid,)>(
            "INSERT INTO users (steam_id, display_name, last_login_at) \
             VALUES ($1, $2, NOW()) \
             ON CONFLICT (steam_id) DO UPDATE SET display_name = EXCLUDED.display_name, last_login_at = NOW() \
             RETURNING id",
        )
        .bind(steam_id as i64)
        .bind(display_name)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)?;
        if verified {
            sqlx::query(
                "INSERT INTO user_identities (provider, provider_uid, user_id, last_login_at) \
                 VALUES ('steam', $1, $2, NOW()) \
                 ON CONFLICT (provider, provider_uid) DO UPDATE SET last_login_at = NOW()",
            )
            .bind(steam_id.to_string())
            .bind(row.0)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        }
        Ok(row.0)
    }
}
