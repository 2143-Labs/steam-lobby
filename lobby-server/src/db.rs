use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use lobby_core::error::{LobbyError, Result};
use lobby_core::traits::{MatchStore, PlayerStore, QueueStore, RatingStore};
use lobby_core::types::{
    MatchDifficulty, MatchInfo, MatchReport, MatchStatus, OpenSkillRating, PlayerInfo, PlayerState,
    QueueEntry, SteamId,
};
use sqlx::PgPool;

pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn map_db_error(e: sqlx::Error) -> LobbyError {
    LobbyError::Database(e.to_string())
}

fn parse_difficulty(s: &str) -> MatchDifficulty {
    match s {
        "easy" => MatchDifficulty::Easy,
        "hard" => MatchDifficulty::Hard,
        _ => MatchDifficulty::Normal,
    }
}

fn parse_match_status(s: &str) -> MatchStatus {
    match s {
        "PendingAccept" => MatchStatus::PendingAccept,
        "InProgress" => MatchStatus::InProgress,
        "Reporting" => MatchStatus::Reporting,
        "Disputed" => MatchStatus::Disputed,
        "Resolved" => MatchStatus::Resolved,
        _ => MatchStatus::PendingAccept,
    }
}

fn parse_player_state(s: &str) -> PlayerState {
    match s {
        "InMenus" => PlayerState::InMenus,
        "Queueing" => PlayerState::Queueing,
        "MatchAccepted" => PlayerState::MatchAccepted,
        "InMatch" => PlayerState::InMatch,
        "Reporting" => PlayerState::Reporting,
        _ => PlayerState::InMenus,
    }
}

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

#[async_trait]
impl MatchStore for PostgresStore {
    async fn create_match(&self, match_info: &MatchInfo) -> Result<()> {
        sqlx::query(
            "INSERT INTO matches (match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, status, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, 'PendingAccept', NOW())",
        )
        .bind(&match_info.match_token)
        .bind(match_info.player_a as i64)
        .bind(format!("{:?}", match_info.player_a_difficulty).to_lowercase())
        .bind(match_info.player_b as i64)
        .bind(format!("{:?}", match_info.player_b_difficulty).to_lowercase())
        .bind(&match_info.game_mode)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn get_match(&self, token: &str) -> Result<Option<MatchInfo>> {
        let row = sqlx::query_as::<_, (String, i64, String, i64, String, String, String, DateTime<Utc>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, bool, bool, bool, bool)>(
            "SELECT match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, status, created_at, accepted_at, started_at, ended_at, accepted_a, accepted_b, connected_a, connected_b \
             FROM matches WHERE match_token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(row.map(
            |(tok, pa, pad, pb, pbd, gm, st, ca, aa, sa, ea, acc_a, acc_b, con_a, con_b)| MatchInfo {
                match_token: tok,
                player_a: pa as u64,
                player_a_difficulty: parse_difficulty(&pad),
                player_b: pb as u64,
                player_b_difficulty: parse_difficulty(&pbd),
                game_mode: gm,
                status: parse_match_status(&st),
                created_at: ca,
                accepted_at: aa,
                started_at: sa,
                ended_at: ea,
                accepted_a: acc_a,
                accepted_b: acc_b,
                connected_a: con_a,
                connected_b: con_b,
            },
        ))
    }

    async fn update_match_status(&self, token: &str, status: MatchStatus) -> Result<()> {
        let status_str = format!("{:?}", status);
        sqlx::query("UPDATE matches SET status = $1 WHERE match_token = $2")
            .bind(&status_str)
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn submit_report(&self, report: &MatchReport) -> Result<()> {
        let winner_val: Option<i64> = report.winner.map(|w| w as i64);
        sqlx::query(
            "INSERT INTO match_reports (match_token, reporting_player, winner, demo_hash) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (match_token, reporting_player) DO NOTHING",
        )
        .bind(&report.match_token)
        .bind(report.reporting_player as i64)
        .bind(winner_val)
        .bind(&report.demo_hash)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn get_reports(&self, token: &str) -> Result<Vec<MatchReport>> {
        let rows = sqlx::query_as::<_, (String, i64, Option<i64>, Option<String>)>(
            "SELECT match_token, reporting_player, winner, demo_hash \
             FROM match_reports WHERE match_token = $1",
        )
        .bind(token)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(rows
            .into_iter()
            .map(|(mt, rp, w, dh)| MatchReport {
                match_token: mt,
                reporting_player: rp as u64,
                winner: w.map(|v| v as u64),
                demo_hash: dh,
            })
            .collect())
    }

    async fn get_matches_by_status(&self, status: MatchStatus) -> Result<Vec<MatchInfo>> {
        let status_str = format!("{:?}", status);
        let rows = sqlx::query_as::<_, (String, i64, String, i64, String, String, String, DateTime<Utc>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, Option<DateTime<Utc>>, bool, bool, bool, bool)>(
            "SELECT match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, status, created_at, accepted_at, started_at, ended_at, accepted_a, accepted_b, connected_a, connected_b \
             FROM matches WHERE status = $1",
        )
        .bind(&status_str)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(rows
            .into_iter()
            .map(
                |(tok, pa, pad, pb, pbd, gm, st, ca, aa, sa, ea, acc_a, acc_b, con_a, con_b)| MatchInfo {
                    match_token: tok,
                    player_a: pa as u64,
                    player_a_difficulty: parse_difficulty(&pad),
                    player_b: pb as u64,
                    player_b_difficulty: parse_difficulty(&pbd),
                    game_mode: gm,
                    status: parse_match_status(&st),
                    created_at: ca,
                    accepted_at: aa,
                    started_at: sa,
                    ended_at: ea,
                    accepted_a: acc_a,
                    accepted_b: acc_b,
                    connected_a: con_a,
                    connected_b: con_b,
                },
            )
            .collect())
    }

    async fn update_match(
        &self,
        token: &str,
        status: MatchStatus,
        ended_at: DateTime<Utc>,
    ) -> Result<()> {
        let status_str = format!("{:?}", status);
        sqlx::query("UPDATE matches SET status = $1, ended_at = $2 WHERE match_token = $3")
            .bind(&status_str)
            .bind(ended_at)
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn mark_accepted(&self, token: &str, steam_id: SteamId) -> Result<bool> {
        sqlx::query(
            "UPDATE matches SET accepted_a = TRUE, accepted_at = NOW() \
             WHERE match_token = $1 AND player_a = $2",
        )
        .bind(token)
        .bind(steam_id as i64)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE matches SET accepted_b = TRUE, accepted_at = NOW() \
             WHERE match_token = $1 AND player_b = $2",
        )
        .bind(token)
        .bind(steam_id as i64)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        let row = sqlx::query_as::<_, (bool,)>(
            "SELECT accepted_a AND accepted_b FROM matches WHERE match_token = $1",
        )
        .bind(token)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(row.0)
    }

    async fn mark_started(&self, token: &str, steam_id: SteamId) -> Result<bool> {
        sqlx::query(
            "UPDATE matches SET connected_a = TRUE, started_at = NOW() \
             WHERE match_token = $1 AND player_a = $2",
        )
        .bind(token)
        .bind(steam_id as i64)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE matches SET connected_b = TRUE, started_at = NOW() \
             WHERE match_token = $1 AND player_b = $2",
        )
        .bind(token)
        .bind(steam_id as i64)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        let row = sqlx::query_as::<_, (bool,)>(
            "SELECT connected_a AND connected_b FROM matches WHERE match_token = $1",
        )
        .bind(token)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(row.0)
    }


    async fn write_match_result(
        &self,
        token: &str,
        outcome: &str,
        mu_change_a: Option<f64>,
        mu_change_b: Option<f64>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO match_results (match_token, outcome, mu_change_a, mu_change_b) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(token)
        .bind(outcome)
        .bind(mu_change_a)
        .bind(mu_change_b)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn recent_match_between(
        &self,
        a: SteamId,
        b: SteamId,
        since: DateTime<Utc>,
    ) -> Result<bool> {
        let row = sqlx::query_as::<_, (bool,)>(
            "SELECT EXISTS(SELECT 1 FROM matches \
             WHERE ((player_a = $1 AND player_b = $2) OR (player_a = $2 AND player_b = $1)) \
               AND status = 'Resolved' AND ended_at >= $3)",
        )
        .bind(a as i64)
        .bind(b as i64)
        .bind(since)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(row.0)
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_match(
        &self,
        token: &str,
        game_mode: &str,
        player_a: SteamId,
        player_b: SteamId,
        outcome: &str,
        mu_change_a: Option<f64>,
        mu_change_b: Option<f64>,
        rating_a: &OpenSkillRating,
        rating_b: &OpenSkillRating,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;
        sqlx::query(
            "UPDATE ratings SET mu = $1, sigma = $2, last_updated = NOW() \
             WHERE steam_id = $3 AND game_mode = $4",
        )
        .bind(rating_a.mu)
        .bind(rating_a.sigma)
        .bind(player_a as i64)
        .bind(game_mode)
        .execute(&mut *tx)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE ratings SET mu = $1, sigma = $2, last_updated = NOW() \
             WHERE steam_id = $3 AND game_mode = $4",
        )
        .bind(rating_b.mu)
        .bind(rating_b.sigma)
        .bind(player_b as i64)
        .bind(game_mode)
        .execute(&mut *tx)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "INSERT INTO match_results (match_token, outcome, mu_change_a, mu_change_b) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(token)
        .bind(outcome)
        .bind(mu_change_a)
        .bind(mu_change_b)
        .execute(&mut *tx)
        .await
        .map_err(map_db_error)?;
        sqlx::query("UPDATE matches SET status = 'Resolved', ended_at = NOW() WHERE match_token = $1")
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)
    }
}
#[async_trait]
impl QueueStore for PostgresStore {
    async fn enqueue(&self, entry: &QueueEntry) -> Result<()> {
        sqlx::query(
            "INSERT INTO matchmaking_queue (steam_id, game_mode, match_difficulty, mu, queued_at) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (steam_id, game_mode) DO UPDATE SET match_difficulty = $3, mu = $4, queued_at = NOW()",
        )
        .bind(entry.steam_id as i64)
        .bind(&entry.game_mode)
        .bind(format!("{:?}", entry.difficulty).to_lowercase())
        .bind(entry.mu)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn dequeue(&self, steam_id: SteamId, mode: &str) -> Result<()> {
        sqlx::query("DELETE FROM matchmaking_queue WHERE steam_id = $1 AND game_mode = $2")
            .bind(steam_id as i64)
            .bind(mode)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn get_queue(&self, mode: &str) -> Result<Vec<QueueEntry>> {
        let rows = sqlx::query_as::<_, (i64, String, String, f64, DateTime<Utc>)>(
            "SELECT steam_id, game_mode, match_difficulty, mu, queued_at \
             FROM matchmaking_queue WHERE game_mode = $1",
        )
        .bind(mode)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(rows
            .into_iter()
            .map(|(sid, gm, md, mu, qa)| QueueEntry {
                steam_id: sid as u64,
                game_mode: gm,
                difficulty: parse_difficulty(&md),
                mu,
                queued_at: qa,
            })
            .collect())
    }

    async fn remove_stale_queue_entries(&self, timeout: Duration) -> Result<()> {
        let cutoff = Utc::now() - timeout;
        sqlx::query("DELETE FROM matchmaking_queue WHERE queued_at < $1")
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }
}

#[async_trait]
impl RatingStore for PostgresStore {
    async fn get_rating(&self, steam_id: SteamId, mode: &str) -> Result<OpenSkillRating> {
        <Self as PlayerStore>::get_rating(self, steam_id, mode).await
    }

    async fn list_ratings(&self) -> Result<Vec<(SteamId, OpenSkillRating)>> {
        let rows = sqlx::query_as::<_, (i64, f64, f64, DateTime<Utc>)>(
            "SELECT steam_id, mu, sigma, last_updated FROM ratings \
             ORDER BY (mu - 3 * sigma) DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(rows
            .into_iter()
            .map(|(id, mu, sigma, last_updated)| {
                (id as u64, OpenSkillRating { mu, sigma, last_updated })
            })
            .collect())
    }

    async fn update_rating(
        &self,
        steam_id: SteamId,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO ratings (steam_id, game_mode, mu, sigma, last_updated) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (steam_id, game_mode) DO UPDATE SET \
               mu = EXCLUDED.mu, sigma = EXCLUDED.sigma, last_updated = EXCLUDED.last_updated",
        )
        .bind(steam_id as i64)
        .bind(mode)
        .bind(rating.mu)
        .bind(rating.sigma)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }
}
