//! `QueueStore` impl for `PostgresStore`: the `matchmaking_queue` table.
use super::*;

#[async_trait]
impl QueueStore for PostgresStore {
    async fn enqueue(&self, entry: &QueueEntry) -> Result<()> {
        sqlx::query(
            "INSERT INTO matchmaking_queue (user_id, game_mode, match_difficulty, mu, queued_at) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (user_id, game_mode) DO UPDATE SET match_difficulty = $3, mu = $4, queued_at = NOW()",
        )
        .bind(entry.user_id)
        .bind(&entry.game_mode)
        .bind(format!("{:?}", entry.difficulty).to_lowercase())
        .bind(entry.mu)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }

    async fn dequeue(&self, user_id: uuid::Uuid, mode: &str) -> Result<()> {
        sqlx::query("DELETE FROM matchmaking_queue WHERE user_id = $1 AND game_mode = $2")
            .bind(user_id)
            .bind(mode)
            .execute(&self.pool)
            .await
            .map_err(map_db_error)?;
        Ok(())
    }

    async fn get_queue(&self, mode: &str) -> Result<Vec<QueueEntry>> {
        let rows = sqlx::query_as::<_, (uuid::Uuid, String, String, f64, DateTime<Utc>)>(
            "SELECT user_id, game_mode, match_difficulty, mu, queued_at \
             FROM matchmaking_queue WHERE game_mode = $1",
        )
        .bind(mode)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;

        Ok(rows
            .into_iter()
            .map(|(uid, gm, md, mu, qa)| QueueEntry {
                user_id: uid,
                game_mode: gm,
                difficulty: parse_difficulty(&md),
                mu,
                queued_at: qa,
            })
            .collect())
    }

    async fn remove_stale_queue_entries(&self, timeout: Duration) -> Result<Vec<uuid::Uuid>> {
        // Legacy modes retain heartbeat liveness. Ranked NativeReport rows use
        // their explicit lease, and no cleanup path may evict an active match.
        let cutoff = Utc::now() - timeout;
        let rows = sqlx::query_scalar::<_, uuid::Uuid>(
            "DELETE FROM matchmaking_queue q \
             USING player_state ps \
             WHERE q.user_id = ps.user_id \
               AND ps.active_match_token IS NULL \
               AND ((q.lease_expires_at IS NULL AND ps.last_heartbeat < $1) \
                    OR (q.lease_expires_at IS NOT NULL AND q.lease_expires_at <= NOW())) \
             RETURNING q.user_id",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        if !rows.is_empty() {
            tracing::info!("removed {} stale or lease-expired queue entries", rows.len());
        }
        Ok(rows)
    }

    async fn is_queued(&self, user_id: uuid::Uuid) -> Result<bool> {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM matchmaking_queue WHERE user_id = $1)",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .map_err(map_db_error)
    }
    async fn get_queued_entry(&self, user_id: uuid::Uuid) -> Result<Option<QueueEntry>> {
        let row = sqlx::query_as::<_, (String, String, f64, DateTime<Utc>)>(
            "SELECT game_mode, match_difficulty, mu, queued_at \
             FROM matchmaking_queue WHERE user_id = $1 ORDER BY queued_at DESC LIMIT 1",
        )
        .bind(user_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(row.map(|(gm, md, mu, qa)| QueueEntry {
            user_id,
            game_mode: gm,
            difficulty: parse_difficulty(&md),
            mu,
            queued_at: qa,
        }))
    }
}
impl PostgresStore {
    /// Pick a compatible pair without holding broad queue locks, then atomically
    /// claim the two exact rows in the global state-before-queue lock order.
    pub async fn pair_next_match(
        &self,
        mode: &str,
        spec: &'static ModeSpec,
        pair_cooldown_secs: i64,
    ) -> Result<Option<MatchInfo>> {
        self.pair_next_match_with_accept_timeout(mode, spec, pair_cooldown_secs, 15)
            .await
    }

    pub async fn pair_next_match_with_accept_timeout(
        &self,
        mode: &str,
        spec: &'static ModeSpec,
        pair_cooldown_secs: i64,
        match_accept_timeout_secs: u64,
    ) -> Result<Option<MatchInfo>> {
        type CandidateRow = (
            uuid::Uuid,
            String,
            String,
            f64,
            DateTime<Utc>,
            Option<DateTime<Utc>>,
        );

        // Deliberately unlocked. A pair is only a hint until both state rows and
        // both exact queue rows have been locked and revalidated below.
        let candidates = sqlx::query_as::<_, CandidateRow>(
            "SELECT user_id, game_mode, match_difficulty, mu, queued_at, lease_expires_at \
             FROM matchmaking_queue WHERE game_mode = $1 ORDER BY queued_at, user_id",
        )
        .bind(mode)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        if candidates.len() < 2 {
            return Ok(None);
        }

        let preselected_at = Utc::now();
        let native = spec.authority == ResultAuthority::NativeReport;
        for i in 0..candidates.len() {
            let a = &candidates[i];
            if native && a.5.is_none_or(|lease| lease <= preselected_at) {
                continue;
            }
            let wait_s = (preselected_at - a.4).num_seconds().max(0) as f64;
            let difficulty_a = parse_difficulty(&a.2);
            let (lo, hi) = lobby_core::queue::search_band(
                wait_s,
                a.3,
                difficulty_a.mmr_offset(),
            );

            for b in &candidates {
                if b.0 == a.0 {
                    continue;
                }
                if native && b.5.is_none_or(|lease| lease <= preselected_at) {
                    continue;
                }
                if b.3 < lo || b.3 > hi {
                    continue;
                }

                let mut ids = [a.0, b.0];
                ids.sort_unstable();
                let mut tx = self.pool.begin().await.map_err(map_db_error)?;

                // This ordering is shared with command draining and finalization.
                let states = sqlx::query_as::<_, (uuid::Uuid, String, Option<String>)>(
                    "SELECT user_id, state, active_match_token FROM player_state \
                     WHERE user_id = ANY($1) ORDER BY user_id FOR UPDATE",
                )
                .bind(&ids[..])
                .fetch_all(&mut *tx)
                .await
                .map_err(map_db_error)?;
                if states.len() != 2
                    || states.iter().any(|(_, state, active)| {
                        parse_player_state(state) != PlayerState::Queueing || active.is_some()
                    })
                {
                    tx.rollback().await.map_err(map_db_error)?;
                    continue;
                }

                let locked = sqlx::query_as::<_, CandidateRow>(
                    "SELECT user_id, game_mode, match_difficulty, mu, queued_at, lease_expires_at \
                     FROM matchmaking_queue \
                     WHERE user_id = ANY($1) AND game_mode = $2 \
                     ORDER BY user_id FOR UPDATE",
                )
                .bind(&ids[..])
                .bind(mode)
                .fetch_all(&mut *tx)
                .await
                .map_err(map_db_error)?;
                if locked.len() != 2 {
                    tx.rollback().await.map_err(map_db_error)?;
                    continue;
                }

                let db_now = sqlx::query_scalar::<_, DateTime<Utc>>("SELECT NOW()")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(map_db_error)?;
                if native
                    && locked
                        .iter()
                        .any(|row| row.5.is_none_or(|lease| lease <= db_now))
                {
                    tx.rollback().await.map_err(map_db_error)?;
                    continue;
                }

                let Some(locked_a) = locked.iter().find(|row| row.0 == a.0) else {
                    tx.rollback().await.map_err(map_db_error)?;
                    continue;
                };
                let Some(locked_b) = locked.iter().find(|row| row.0 == b.0) else {
                    tx.rollback().await.map_err(map_db_error)?;
                    continue;
                };
                let difficulty_a = parse_difficulty(&locked_a.2);
                let difficulty_b = parse_difficulty(&locked_b.2);
                let wait_s = (db_now - locked_a.4).num_seconds().max(0) as f64;
                let (lo, hi) = lobby_core::queue::search_band(
                    wait_s,
                    locked_a.3,
                    difficulty_a.mmr_offset(),
                );
                if locked_b.3 < lo || locked_b.3 > hi {
                    tx.rollback().await.map_err(map_db_error)?;
                    continue;
                }

                let on_cooldown = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM matches \
                     WHERE ((player_a = $1 AND player_b = $2) OR (player_a = $2 AND player_b = $1)) \
                       AND status = 'Resolved' AND ended_at >= $3)",
                )
                .bind(a.0)
                .bind(b.0)
                .bind(db_now - chrono::Duration::seconds(pair_cooldown_secs))
                .fetch_one(&mut *tx)
                .await
                .map_err(map_db_error)?;
                if on_cooldown {
                    tx.rollback().await.map_err(map_db_error)?;
                    continue;
                }

                let match_info = MatchInfo {
                    match_token: uuid::Uuid::new_v4().to_string(),
                    player_a: a.0,
                    player_a_difficulty: difficulty_a,
                    player_b: b.0,
                    player_b_difficulty: difficulty_b,
                    game_mode: mode.to_string(),
                    connection: spec.connection,
                    status: MatchStatus::PendingAccept,
                    created_at: db_now,
                    accepted_at: None,
                    started_at: None,
                    ended_at: None,
                    server_address: None,
                    join_token: None,
                    result_secret: (spec.authority == ResultAuthority::Gameserver)
                        .then(|| uuid::Uuid::new_v4().to_string()),
                    accepted_a: false,
                    accepted_b: false,
                    connected_a: false,
                    connected_b: false,
                };
                sqlx::query(
                    "INSERT INTO matches (match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, game_type, result_secret, status, created_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'PendingAccept', $9)",
                )
                .bind(&match_info.match_token)
                .bind(match_info.player_a)
                .bind(format!("{:?}", match_info.player_a_difficulty).to_lowercase())
                .bind(match_info.player_b)
                .bind(format!("{:?}", match_info.player_b_difficulty).to_lowercase())
                .bind(&match_info.game_mode)
                .bind(format!("{:?}", match_info.connection).to_lowercase())
                .bind(&match_info.result_secret)
                .bind(db_now)
                .execute(&mut *tx)
                .await
                .map_err(map_db_error)?;

                if native {
                    let attempt_id = uuid::Uuid::new_v4();
                    let deadline = db_now
                        + chrono::Duration::seconds(match_accept_timeout_secs as i64);
                    sqlx::query(
                        "INSERT INTO umvc3_matches \
                         (match_token, phase, phase_deadline, attempt_id, original_queued_at_a, original_queued_at_b) \
                         VALUES ($1, 'AwaitingAccepted', $2, $3, $4, $5)",
                    )
                    .bind(&match_info.match_token)
                    .bind(deadline)
                    .bind(attempt_id)
                    .bind(locked_a.4)
                    .bind(locked_b.4)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_db_error)?;
                    sqlx::query(
                        "UPDATE player_state SET state = 'MatchAccepted', active_match_token = $1 \
                         WHERE user_id = ANY($2)",
                    )
                    .bind(&match_info.match_token)
                    .bind(&ids[..])
                    .execute(&mut *tx)
                    .await
                    .map_err(map_db_error)?;

                    // Counter rows are claimed in UUID order as well. The returned
                    // value is the sequence allocated by this increment.
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
                        .fetch_one(&mut *tx)
                        .await
                        .map_err(map_db_error)?;
                        sequences.insert(recipient, sequence);
                    }
                    for (recipient, opponent, role) in [
                        (a.0, b.0, "create"),
                        (b.0, a.0, "join"),
                    ] {
                        sqlx::query(
                            "INSERT INTO match_events \
                             (match_token, event_type, recipient_user_id, recipient_sequence, payload) \
                             VALUES ($1, 'paired', $2, $3, $4)",
                        )
                        .bind(&match_info.match_token)
                        .bind(recipient)
                        .bind(sequences[&recipient])
                        .bind(serde_json::json!({
                            "opponent": opponent,
                            "role": role,
                            "deadline": deadline,
                            "attempt": attempt_id,
                        }))
                        .execute(&mut *tx)
                        .await
                        .map_err(map_db_error)?;
                    }
                } else {
                    sqlx::query(
                        "INSERT INTO match_events (match_token, event_type, actor_user_id) \
                         VALUES ($1, 'paired', NULL)",
                    )
                    .bind(&match_info.match_token)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_db_error)?;
                }

                sqlx::query(
                    "DELETE FROM matchmaking_queue WHERE user_id = ANY($1) AND game_mode = $2",
                )
                .bind(&ids[..])
                .bind(mode)
                .execute(&mut *tx)
                .await
                .map_err(map_db_error)?;
                tx.commit().await.map_err(map_db_error)?;
                return Ok(Some(match_info));
            }
        }

        Ok(None)
    }
}
