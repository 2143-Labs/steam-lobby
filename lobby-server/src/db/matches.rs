//! `MatchStore` impl for `PostgresStore`: matches, reports, results, and
//! match_events rows.
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Umvc3Verdict {
    Win(uuid::Uuid),
    Draw,
    Disputed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RequeueDecision {
    pub player_a: bool,
    pub player_b: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredResolution {
    pub outcome: String,
    pub mu_change_a: Option<f64>,
    pub mu_change_b: Option<f64>,
    pub newly_finalized: bool,
}

#[async_trait]
impl MatchStore for PostgresStore {
    async fn create_match(&self, match_info: &MatchInfo) -> Result<()> {
        sqlx::query(
            "INSERT INTO matches (match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, game_type, result_secret, status, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PendingAccept', NOW())",
        )
        .bind(&match_info.match_token)
        .bind(match_info.player_a)
        .bind(format!("{:?}", match_info.player_a_difficulty).to_lowercase())
        .bind(match_info.player_b)
        .bind(format!("{:?}", match_info.player_b_difficulty).to_lowercase())
        .bind(&match_info.game_mode)
        .bind(format!("{:?}", match_info.connection).to_lowercase())
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
        sqlx::query(
            "INSERT INTO match_reports (match_token, reporting_player, winner, demo_hash) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (match_token, reporting_player) DO NOTHING",
        )
        .bind(&report.match_token)
        .bind(report.reporting_player)
        .bind(report.winner)
        .bind(&report.demo_hash)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn get_reports(&self, token: &str) -> Result<Vec<MatchReport>> {
        let rows = sqlx::query_as::<_, (String, uuid::Uuid, Option<uuid::Uuid>, Option<String>)>(
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
                reporting_player: rp,
                winner: w,
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

    async fn mark_accepted(&self, token: &str, user_id: uuid::Uuid) -> Result<bool> {
        sqlx::query(
            "UPDATE matches SET accepted_a = TRUE, accepted_at = NOW() \
             WHERE match_token = $1 AND player_a = $2",
        )
        .bind(token)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE matches SET accepted_b = TRUE, accepted_at = NOW() \
             WHERE match_token = $1 AND player_b = $2",
        )
        .bind(token)
        .bind(user_id)
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

    async fn mark_started(&self, token: &str, user_id: uuid::Uuid) -> Result<bool> {
        sqlx::query(
            "UPDATE matches SET connected_a = TRUE, started_at = NOW() \
             WHERE match_token = $1 AND player_a = $2",
        )
        .bind(token)
        .bind(user_id)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE matches SET connected_b = TRUE, started_at = NOW() \
             WHERE match_token = $1 AND player_b = $2",
        )
        .bind(token)
        .bind(user_id)
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

    async fn mark_server_ready(
        &self,
        token: &str,
        address: &str,
        join_token: Option<&str>,
    ) -> Result<()> {
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
        actor: Option<uuid::Uuid>,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO match_events (match_token, event_type, actor_user_id) VALUES ($1, $2, $3)",
        )
        .bind(match_token)
        .bind(format!("{:?}", event).to_lowercase())
        .bind(actor)
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
        a: uuid::Uuid,
        b: uuid::Uuid,
        since: DateTime<Utc>,
    ) -> Result<bool> {
        let row = sqlx::query_as::<_, (bool,)>(
            "SELECT EXISTS(SELECT 1 FROM matches \
             WHERE ((player_a = $1 AND player_b = $2) OR (player_a = $2 AND player_b = $1)) \
               AND status = 'Resolved' AND ended_at >= $3)",
        )
        .bind(a)
        .bind(b)
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
        player_a: uuid::Uuid,
        player_b: uuid::Uuid,
        outcome: &str,
        mu_change_a: Option<f64>,
        mu_change_b: Option<f64>,
        rating_a: &OpenSkillRating,
        rating_b: &OpenSkillRating,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;
        sqlx::query(
            "UPDATE ratings SET mu = $1, sigma = $2, last_updated = NOW() \
             WHERE user_id = $3 AND game_mode = $4",
        )
        .bind(rating_a.mu)
        .bind(rating_a.sigma)
        .bind(player_a)
        .bind(game_mode)
        .execute(&mut *tx)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE ratings SET mu = $1, sigma = $2, last_updated = NOW() \
             WHERE user_id = $3 AND game_mode = $4",
        )
        .bind(rating_b.mu)
        .bind(rating_b.sigma)
        .bind(player_b)
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
        sqlx::query(
            "UPDATE matches SET status = 'Resolved', ended_at = NOW() WHERE match_token = $1",
        )
        .bind(token)
        .execute(&mut *tx)
        .await
        .map_err(map_db_error)?;
        tx.commit().await.map_err(map_db_error)
    }
}

impl PostgresStore {
    pub async fn finalize_umvc3(
        &self,
        match_token: &str,
        verdict: Umvc3Verdict,
        requeue: RequeueDecision,
    ) -> Result<StoredResolution> {
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;
        let result = self
            .finalize_umvc3_in_tx(&mut tx, match_token, verdict, requeue)
            .await?;
        tx.commit().await.map_err(map_db_error)?;
        Ok(result)
    }

    /// Nonterminal ranked matches whose durable workflow must exist. Used by
    /// startup and the periodic reconciler; PostgreSQL remains canonical.
    pub async fn nonterminal_umvc3_tokens(&self) -> Result<Vec<String>> {
        sqlx::query_scalar(
            "SELECT match_token FROM umvc3_matches WHERE phase <> 'Terminal' ORDER BY match_token",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)
    }

    /// Audit the first successful workflow ensure without overwriting its
    /// original timestamp on retries or reconciliation.
    pub async fn mark_umvc3_workflow_started(&self, match_token: &str) -> Result<()> {
        sqlx::query(
            "UPDATE umvc3_matches SET workflow_started_at = COALESCE(workflow_started_at, NOW()) \
             WHERE match_token = $1 AND phase <> 'Terminal'",
        )
        .bind(match_token)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    pub(crate) async fn finalize_umvc3_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        match_token: &str,
        verdict: Umvc3Verdict,
        requeue: RequeueDecision,
    ) -> Result<StoredResolution> {
        type FinalizeMatchRow = (
            uuid::Uuid,
            String,
            uuid::Uuid,
            String,
            String,
            String,
        );
        let Some((player_a, difficulty_a, player_b, difficulty_b, game_mode, status)) =
            sqlx::query_as::<_, FinalizeMatchRow>(
                "SELECT player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, status \
                 FROM matches WHERE match_token = $1 FOR UPDATE",
            )
            .bind(match_token)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_db_error)?
        else {
            return Err(LobbyError::MatchNotFound(match_token.to_string()));
        };
        if game_mode != "umvc3_1v1" {
            return Err(LobbyError::MatchStateMismatch(match_token.to_string()));
        }
        let Some((phase, original_queued_at_a, original_queued_at_b)) =
            sqlx::query_as::<_, (String, DateTime<Utc>, DateTime<Utc>)>(
                "SELECT phase, original_queued_at_a, original_queued_at_b \
                 FROM umvc3_matches WHERE match_token = $1 FOR UPDATE",
            )
            .bind(match_token)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_db_error)?
        else {
            return Err(LobbyError::MatchStateMismatch(match_token.to_string()));
        };

        // The match row serializes finalization. Once it is locked, an existing
        // result is authoritative and retries perform no state, rating, queue,
        // counter, or event writes.
        if let Some((outcome, mu_change_a, mu_change_b)) =
            sqlx::query_as::<_, (String, Option<f64>, Option<f64>)>(
                "SELECT outcome, mu_change_a, mu_change_b FROM match_results WHERE match_token = $1",
            )
            .bind(match_token)
            .fetch_optional(&mut **tx)
            .await
            .map_err(map_db_error)?
        {
            return Ok(StoredResolution {
                outcome,
                mu_change_a,
                mu_change_b,
                newly_finalized: false,
            });
        }
        if phase == "Terminal" {
            return Err(LobbyError::MatchStateMismatch(match_token.to_string()));
        }
        if status == "Resolved" {
            return Err(LobbyError::MatchStateMismatch(match_token.to_string()));
        }

        if let Umvc3Verdict::Win(winner) = verdict
            && winner != player_a
            && winner != player_b
        {
            return Err(LobbyError::InvalidReport(match_token.to_string()));
        }

        let mut ids = [player_a, player_b];
        ids.sort_unstable();
        let locked_states = sqlx::query_as::<_, (uuid::Uuid, Option<String>)>(
            "SELECT user_id, active_match_token FROM player_state \
             WHERE user_id = ANY($1) ORDER BY user_id FOR UPDATE",
        )
        .bind(&ids[..])
        .fetch_all(&mut **tx)
        .await
        .map_err(map_db_error)?;
        if locked_states.len() != 2
            || locked_states
                .iter()
                .any(|(_, active)| active.as_deref() != Some(match_token))
        {
            return Err(LobbyError::MatchStateMismatch(match_token.to_string()));
        }
        let (outcome, rating_outcome) = match verdict {
            Umvc3Verdict::Win(winner) if winner == player_a => {
                ("Win", Some(lobby_core::mmr::RatingOutcome::Win))
            }
            Umvc3Verdict::Win(_) => ("Loss", Some(lobby_core::mmr::RatingOutcome::Loss)),
            Umvc3Verdict::Draw => ("Draw", Some(lobby_core::mmr::RatingOutcome::Draw)),
            Umvc3Verdict::Disputed => ("Disputed", None),
        };

        let (mu_change_a, mu_change_b) = if let Some(rating_outcome) = rating_outcome {
            // Materialize defaults before locking, then lock both ratings in UUID
            // order. Match ownership prevents either participant from being rated
            // by another UMVC3 match concurrently.
            for user_id in ids {
                sqlx::query(
                    "INSERT INTO ratings (user_id, game_mode, mu, sigma, last_updated) \
                     VALUES ($1, $2, 25.0, $3, NOW()) ON CONFLICT DO NOTHING",
                )
                .bind(user_id)
                .bind(&game_mode)
                .bind(25.0 / 3.0)
                .execute(&mut **tx)
                .await
                .map_err(map_db_error)?;
            }
            let ratings = sqlx::query_as::<_, (uuid::Uuid, f64, f64, DateTime<Utc>)>(
                "SELECT user_id, mu, sigma, last_updated FROM ratings \
                 WHERE user_id = ANY($1) AND game_mode = $2 ORDER BY user_id FOR UPDATE",
            )
            .bind(&ids[..])
            .bind(&game_mode)
            .fetch_all(&mut **tx)
            .await
            .map_err(map_db_error)?;
            if ratings.len() != 2 {
                return Err(LobbyError::Database(
                    "failed to materialize participant ratings".into(),
                ));
            }
            let old_a = ratings
                .iter()
                .find(|row| row.0 == player_a)
                .map(|row| OpenSkillRating {
                    mu: row.1,
                    sigma: row.2,
                    last_updated: row.3,
                })
                .ok_or_else(|| LobbyError::Database("missing player A rating".into()))?;
            let old_b = ratings
                .iter()
                .find(|row| row.0 == player_b)
                .map(|row| OpenSkillRating {
                    mu: row.1,
                    sigma: row.2,
                    last_updated: row.3,
                })
                .ok_or_else(|| LobbyError::Database("missing player B rating".into()))?;
            let (new_a, new_b) = lobby_core::mmr::update_ratings_for_outcome(
                &old_a,
                &old_b,
                rating_outcome,
            );
            sqlx::query(
                "UPDATE ratings SET mu = $1, sigma = $2, last_updated = NOW() \
                 WHERE user_id = $3 AND game_mode = $4",
            )
            .bind(new_a.mu)
            .bind(new_a.sigma)
            .bind(player_a)
            .bind(&game_mode)
            .execute(&mut **tx)
            .await
            .map_err(map_db_error)?;
            sqlx::query(
                "UPDATE ratings SET mu = $1, sigma = $2, last_updated = NOW() \
                 WHERE user_id = $3 AND game_mode = $4",
            )
            .bind(new_b.mu)
            .bind(new_b.sigma)
            .bind(player_b)
            .bind(&game_mode)
            .execute(&mut **tx)
            .await
            .map_err(map_db_error)?;
            (Some(new_a.mu - old_a.mu), Some(new_b.mu - old_b.mu))
        } else {
            (None, None)
        };
        if rating_outcome.is_none() && (requeue.player_a || requeue.player_b) {
            for user_id in ids {
                sqlx::query(
                    "INSERT INTO ratings (user_id, game_mode, mu, sigma, last_updated) \
                     VALUES ($1, $2, 25.0, $3, NOW()) ON CONFLICT DO NOTHING",
                )
                .bind(user_id)
                .bind(&game_mode)
                .bind(25.0 / 3.0)
                .execute(&mut **tx)
                .await
                .map_err(map_db_error)?;
            }
            let locked_rating_ids = sqlx::query_scalar::<_, uuid::Uuid>(
                "SELECT user_id FROM ratings WHERE user_id = ANY($1) AND game_mode = $2 \
                 ORDER BY user_id FOR UPDATE",
            )
            .bind(&ids[..])
            .bind(&game_mode)
            .fetch_all(&mut **tx)
            .await
            .map_err(map_db_error)?;
            if locked_rating_ids.len() != 2 {
                return Err(LobbyError::Database(
                    "failed to lock participant ratings for requeue".into(),
                ));
            }
        }

        sqlx::query(
            "INSERT INTO match_results (match_token, outcome, mu_change_a, mu_change_b) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(match_token)
        .bind(outcome)
        .bind(mu_change_a)
        .bind(mu_change_b)
        .execute(&mut **tx)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE matches SET status = 'Resolved', ended_at = NOW() WHERE match_token = $1",
        )
        .bind(match_token)
        .execute(&mut **tx)
        .await
        .map_err(map_db_error)?;
        sqlx::query(
            "UPDATE umvc3_matches SET phase = 'Terminal', \
             terminal_reason = $2, phase_version = phase_version + 1, phase_deadline = NULL \
             WHERE match_token = $1",
        )
        .bind(match_token)
        .bind(if matches!(verdict, Umvc3Verdict::Disputed) {
            "disputed"
        } else {
            "resolved"
        })
        .execute(&mut **tx)
        .await
        .map_err(map_db_error)?;

        let _locked_queue_ids = sqlx::query_scalar::<_, uuid::Uuid>(
            "SELECT user_id FROM matchmaking_queue WHERE user_id = ANY($1) \
             ORDER BY user_id FOR UPDATE",
        )
        .bind(&ids[..])
        .fetch_all(&mut **tx)
        .await
        .map_err(map_db_error)?;

        for (user_id, should_requeue, difficulty, original_queued_at) in [
            (
                player_a,
                requeue.player_a,
                difficulty_a.as_str(),
                original_queued_at_a,
            ),
            (
                player_b,
                requeue.player_b,
                difficulty_b.as_str(),
                original_queued_at_b,
            ),
        ] {
            sqlx::query(
                "UPDATE player_state SET state = $1, active_match_token = NULL \
                 WHERE user_id = $2 AND active_match_token = $3",
            )
            .bind(if should_requeue { "Queueing" } else { "InMenus" })
            .bind(user_id)
            .bind(match_token)
            .execute(&mut **tx)
            .await
            .map_err(map_db_error)?;
            if should_requeue {
                // Ranked owns at most one queue row per user. A stale legacy
                // mode row must not survive an innocent ranked requeue.
                sqlx::query("DELETE FROM matchmaking_queue WHERE user_id = $1 AND game_mode <> $2")
                    .bind(user_id)
                    .bind(&game_mode)
                    .execute(&mut **tx)
                    .await
                    .map_err(map_db_error)?;
                sqlx::query(
                    "INSERT INTO matchmaking_queue \
                     (user_id, game_mode, match_difficulty, mu, queued_at, lease_expires_at) \
                     SELECT $1, $2, $3, r.mu, $4, NOW() + INTERVAL '45 seconds' \
                     FROM ratings r WHERE r.user_id = $1 AND r.game_mode = $2 \
                     ON CONFLICT (user_id, game_mode) DO UPDATE SET \
                       match_difficulty = EXCLUDED.match_difficulty, \
                       mu = EXCLUDED.mu, \
                       queued_at = LEAST(matchmaking_queue.queued_at, EXCLUDED.queued_at), \
                       lease_expires_at = EXCLUDED.lease_expires_at",
                )
                .bind(user_id)
                .bind(&game_mode)
                .bind(difficulty)
                .bind(original_queued_at)
                .execute(&mut **tx)
                .await
                .map_err(map_db_error)?;
            }
        }

        let event_type = if matches!(verdict, Umvc3Verdict::Disputed) {
            "disputed"
        } else {
            "resolved"
        };
        let mut sequences = std::collections::HashMap::with_capacity(2);
        for recipient in ids {
            let sequence = sqlx::query_scalar::<_, i64>(
                "INSERT INTO event_recipient_counters (recipient_user_id, next_sequence) \
                 VALUES ($1, 2) \
                 ON CONFLICT (recipient_user_id) DO UPDATE SET \
                   next_sequence = event_recipient_counters.next_sequence + 1 \
                 RETURNING next_sequence - 1",
            )
            .bind(recipient)
            .fetch_one(&mut **tx)
            .await
            .map_err(map_db_error)?;
            sequences.insert(recipient, sequence);
        }
        for (recipient, player_outcome, mmr_delta) in [
            (player_a, outcome.to_lowercase(), mu_change_a),
            (
                player_b,
                match outcome {
                    "Win" => "loss".to_string(),
                    "Loss" => "win".to_string(),
                    other => other.to_lowercase(),
                },
                mu_change_b,
            ),
        ] {
            sqlx::query(
                "INSERT INTO match_events \
                 (match_token, event_type, recipient_user_id, recipient_sequence, payload) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(match_token)
            .bind(event_type)
            .bind(recipient)
            .bind(sequences[&recipient])
            .bind(serde_json::json!({
                "result": { "outcome": player_outcome, "mmr_delta": mmr_delta },
            }))
            .execute(&mut **tx)
            .await
            .map_err(map_db_error)?;
        }

        Ok(StoredResolution {
            outcome: outcome.to_string(),
            mu_change_a,
            mu_change_b,
            newly_finalized: true,
        })
    }
}

/// One row for the player page's recent-matches table. `outcome` is the raw
/// stored value (player_a perspective, null when the match has no result
/// yet); the route handler flips it to the viewer's perspective.
#[derive(sqlx::FromRow)]
pub struct RecentMatchRow {
    pub match_token: String,
    pub game_mode: String,
    pub status: String,
    pub player_a: uuid::Uuid,
    pub player_b: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub outcome: Option<String>,
    pub mu_change_a: Option<f64>,
    pub mu_change_b: Option<f64>,
    pub opponent_id: uuid::Uuid,
    pub opponent_name: String,
}

impl PostgresStore {
    /// The player's most recent matches (newest first), each joined with its
    /// result row (LEFT JOIN — unresolved matches have null outcome) and the
    /// opponent's identity for display.
    pub async fn recent_matches_for_user(
        &self,
        user_id: uuid::Uuid,
        limit: i64,
    ) -> Result<Vec<RecentMatchRow>> {
        let rows = sqlx::query_as::<_, RecentMatchRow>(
            "SELECT m.match_token, m.game_mode, m.status, m.player_a, m.player_b, \
                    m.created_at, m.started_at, m.ended_at, \
                    r.outcome, r.mu_change_a, r.mu_change_b, \
                    CASE WHEN m.player_a = $1 THEN m.player_b ELSE m.player_a END AS opponent_id, \
                    COALESCE(u2.display_name, '') AS opponent_name \
             FROM matches m \
             LEFT JOIN match_results r ON r.match_token = m.match_token \
             LEFT JOIN users u2 ON u2.id = CASE WHEN m.player_a = $1 THEN m.player_b ELSE m.player_a END \
             WHERE m.player_a = $1 OR m.player_b = $1 \
             ORDER BY m.created_at DESC \
             LIMIT $2",
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(rows)
    }
}
