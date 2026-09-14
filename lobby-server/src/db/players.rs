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
        if self.steam_backed_accounts_only && (!verified || provider != "steam") {
            return Err(LobbyError::SteamAuthFailed(
                "account creation is restricted to verified Steam identities".into(),
            ));
        }
        let steam_id = if provider == "steam" {
            Some(provider_uid.parse::<i64>().map_err(|_| {
                LobbyError::SteamAuthFailed("invalid Steam identity".into())
            })?)
        } else {
            None
        };

        // The accounts row is the ownership authority. Lock it before touching
        // either the user or login timestamps so concurrent logins cannot
        // create or reassign the same provider subject.
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;
        if let Some((user_id,)) = sqlx::query_as::<_, (uuid::Uuid,)>(
            "SELECT user_id FROM accounts WHERE provider=$1 AND provider_uid=$2 FOR UPDATE",
        )
        .bind(provider)
        .bind(provider_uid)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_db_error)?
        {
            sqlx::query(
                "UPDATE users SET display_name=CASE WHEN $1='' THEN display_name ELSE $1 END, last_login_at=NOW() WHERE id=$2",
            )
            .bind(display_name)
            .bind(user_id)
            .execute(&mut *tx)
            .await
            .map_err(map_db_error)?;
            sqlx::query(
                "UPDATE accounts SET last_login_at=NOW() WHERE provider=$1 AND provider_uid=$2",
            )
            .bind(provider)
            .bind(provider_uid)
            .execute(&mut *tx)
            .await
            .map_err(map_db_error)?;
            tx.commit().await.map_err(map_db_error)?;
            return Ok(user_id);
        }

        let user_id = if let Some(steam_id) = steam_id {
            sqlx::query_as::<_, (uuid::Uuid,)>(
                "INSERT INTO users (steam_id,display_name,primary_provider) VALUES ($1,$2,'steam') ON CONFLICT (steam_id) DO UPDATE SET display_name=CASE WHEN EXCLUDED.display_name='' THEN users.display_name ELSE EXCLUDED.display_name END,last_login_at=NOW() RETURNING id",
            )
            .bind(steam_id)
            .bind(display_name)
            .fetch_one(&mut *tx)
            .await
            .map_err(map_db_error)?
            .0
        } else {
            sqlx::query_as::<_, (uuid::Uuid,)>(
                "INSERT INTO users (display_name,primary_provider) VALUES ($1,$2) RETURNING id",
            )
            .bind(display_name)
            .bind(provider)
            .fetch_one(&mut *tx)
            .await
            .map_err(map_db_error)?
            .0
        };

        if verified {
            let inserted = sqlx::query(
                "INSERT INTO accounts (provider,provider_uid,user_id,last_login_at,linked_at) VALUES ($1,$2,$3,NOW(),NOW()) ON CONFLICT (provider,provider_uid) DO NOTHING",
            )
            .bind(provider)
            .bind(provider_uid)
            .bind(user_id)
            .execute(&mut *tx)
            .await
            .map_err(map_db_error)?;
            if inserted.rows_affected() == 0 {
                // A concurrent transaction won the subject. Roll back our
                // provisional user, then return the canonical owner.
                tx.rollback().await.map_err(map_db_error)?;
                let (owner,) = sqlx::query_as::<_, (uuid::Uuid,)>(
                    "SELECT user_id FROM accounts WHERE provider=$1 AND provider_uid=$2",
                )
                .bind(provider)
                .bind(provider_uid)
                .fetch_one(&self.pool)
                .await
                .map_err(map_db_error)?;
                return Ok(owner);
            }
        }
        tx.commit().await.map_err(map_db_error)?;
        Ok(user_id)
    }

    /// Brand-new identity-less account: steam_id NULL, primary_provider
    /// 'guest', no accounts row. Plain INSERT — every call is a fresh account;
    pub async fn login_linked_account(
        &self,
        provider: &str,
        provider_uid: &str,
        display_name: &str,
    ) -> Result<Option<uuid::Uuid>> {
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;
        let row = sqlx::query_as::<_, (uuid::Uuid,)>(
            "SELECT a.user_id FROM accounts a WHERE a.provider=$1 AND a.provider_uid=$2 AND EXISTS(SELECT 1 FROM accounts steam WHERE steam.user_id=a.user_id AND steam.provider='steam') FOR UPDATE",
        )
        .bind(provider)
        .bind(provider_uid)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_db_error)?;
        let Some((user_id,)) = row else {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(None);
        };
        sqlx::query(
            "UPDATE users SET display_name=CASE WHEN $1='' THEN display_name ELSE $1 END,last_login_at=NOW() WHERE id=$2",
        )
        .bind(display_name)
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE accounts SET last_login_at=NOW() WHERE provider=$1 AND provider_uid=$2 AND user_id=$3",
        )
        .bind(provider)
        .bind(provider_uid)
        .bind(user_id)
        .execute(&mut *tx)
        .await
        .map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)?;
        Ok(Some(user_id))
    }

    pub async fn account_owner(&self, provider: &str, provider_uid: &str) -> Result<Option<uuid::Uuid>> {
        sqlx::query_as::<_, (uuid::Uuid,)>(
            "SELECT user_id FROM accounts WHERE provider=$1 AND provider_uid=$2",
        )
        .bind(provider)
        .bind(provider_uid)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)
        .map(|row| row.map(|r| r.0))
    }

    pub async fn has_account(&self, user_id: uuid::Uuid, provider: &str) -> Result<bool> {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM accounts WHERE user_id=$1 AND provider=$2)",
        )
        .bind(user_id)
        .bind(provider)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)
    }

    /// losing the session JWT loses the account (by design).
    pub async fn create_guest_user(&self, display_name: &str) -> Result<uuid::Uuid> {
        if self.steam_backed_accounts_only {
            return Err(LobbyError::SteamAuthFailed(
                "guest accounts are disabled".into(),
            ));
        }
        let row = sqlx::query_as::<_, (uuid::Uuid,)>(
            "INSERT INTO users (display_name, primary_provider) VALUES ($1, 'guest') RETURNING id",
        )
        .bind(display_name)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(row.0)
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

    /// Public profile fields for the player page: display name, primary
    /// provider, and account creation time. None when the user row is missing.
    pub async fn player_profile(
        &self,
        user_id: uuid::Uuid,
    ) -> Result<Option<(String, String, DateTime<Utc>)>> {
        let row = sqlx::query_as::<_, (String, String, DateTime<Utc>)>(
            "SELECT display_name, primary_provider, created_at FROM users WHERE id = $1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(row)
    }

    /// The player's linked identities (provider + last login). The
    /// provider_uid is deliberately NOT returned — it never leaves the server.
    pub async fn accounts_for_user(
        &self,
        user_id: uuid::Uuid,
    ) -> Result<Vec<(String, DateTime<Utc>)>> {
        let rows = sqlx::query_as::<_, (String, DateTime<Utc>)>(
            "SELECT provider, last_login_at FROM accounts \
             WHERE user_id = $1 ORDER BY last_login_at DESC",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(rows)
    }
}
