//! `MatchStore` impl for `PostgresStore`: matches, reports, results, and
//! match_events rows.
use super::*;

#[async_trait]
impl MatchStore for PostgresStore {
    async fn create_match(&self, match_info: &MatchInfo) -> Result<()> {
        sqlx::query(
            "INSERT INTO matches (match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, game_type, result_secret, status, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PendingAccept', NOW())",
        )
        .bind(&match_info.match_token)
        .bind(match_info.player_a as i64)
        .bind(format!("{:?}", match_info.player_a_difficulty).to_lowercase())
        .bind(match_info.player_b as i64)
        .bind(format!("{:?}", match_info.player_b_difficulty).to_lowercase())
        .bind(&match_info.game_mode)
        .bind(format!("{:?}", match_info.game_type).to_lowercase())
        .bind(&match_info.result_secret)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn get_match(&self, token: &str) -> Result<Option<MatchInfo>> {
        let row = sqlx::query_as::<_, MatchRow>(
            "SELECT match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, game_type, status, created_at, accepted_at, started_at, ended_at, server_address, join_token, result_secret, accepted_a, accepted_b, connected_a, connected_b \
             FROM matches WHERE match_token = $1",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(row.map(MatchInfo::from))
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
        let rows = sqlx::query_as::<_, MatchRow>(
            "SELECT match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, game_type, status, created_at, accepted_at, started_at, ended_at, server_address, join_token, result_secret, accepted_a, accepted_b, connected_a, connected_b \
             FROM matches WHERE status = $1",
        )
        .bind(&status_str)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(rows.into_iter().map(MatchInfo::from).collect())
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

    async fn mark_server_ready(&self, token: &str, address: &str, join_token: Option<&str>) -> Result<()> {
        sqlx::query(
            "UPDATE matches SET status = 'Playing', server_address = $2, join_token = $3, started_at = NOW() \
             WHERE match_token = $1",
        )
        .bind(token)
        .bind(address)
        .bind(join_token)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn record_match_event(
        &self,
        match_token: &str,
        event: MatchEvent,
        steam_id: Option<SteamId>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO match_events (match_token, event_type, steam_id) VALUES ($1, $2, $3)",
        )
        .bind(match_token)
        .bind(format!("{:?}", event).to_lowercase())
        .bind(steam_id.map(|s| s as i64))
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
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
