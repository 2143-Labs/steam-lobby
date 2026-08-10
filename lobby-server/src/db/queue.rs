//! `QueueStore` impl for `PostgresStore`: the `matchmaking_queue` table.
use std::collections::HashMap;

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
        // Liveness is the player's heartbeat, not the moment they queued:
        // a client that keeps heartbeating may stay queued indefinitely.
        let cutoff = Utc::now() - timeout;
        let rows = sqlx::query_scalar::<_, uuid::Uuid>(
            "DELETE FROM matchmaking_queue q \
             USING player_state ps \
             WHERE q.user_id = ps.user_id \
               AND ps.last_heartbeat < $1 \
             RETURNING q.user_id",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        if !rows.is_empty() {
            tracing::info!("removed {} stale queue entries (no heartbeat)", rows.len());
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
    /// Atomically pick a compatible pair for `mode`: SELECT the queue FOR UPDATE,
    /// scan for the first compatible pair, DELETE both rows + INSERT the match +
    /// INSERT the Paired event, COMMIT. No pair -> rollback, Ok(None). Concurrent
    /// pairers block on FOR UPDATE and find nothing after the winner commits.
    pub async fn pair_next_match(
        &self,
        mode: &str,
        game_type: GameType,
        pair_cooldown_secs: i64,
    ) -> Result<Option<MatchInfo>> {
        let mut tx = self.pool.begin().await.map_err(map_db_error)?;

        let queue: Vec<QueueEntry> = sqlx::query_as::<_, (uuid::Uuid, String, String, f64, DateTime<Utc>)>(
            "SELECT user_id, game_mode, match_difficulty, mu, queued_at \
             FROM matchmaking_queue WHERE game_mode = $1 ORDER BY queued_at ASC FOR UPDATE",
        )
        .bind(mode)
        .fetch_all(&mut *tx)
        .await
        .map_err(map_db_error)?
        .into_iter()
        .map(|(uid, gm, md, mu, qa)| QueueEntry {
            user_id: uid,
            game_mode: gm,
            difficulty: parse_difficulty(&md),
            mu,
            queued_at: qa,
        })
        .collect();

        // Fewer than two queued players can never pair — roll back (nothing to
        // write) and release the FOR UPDATE lock.
        if queue.len() < 2 {
            tx.rollback().await.map_err(map_db_error)?;
            return Ok(None);
        }

        let now = Utc::now();

        // Pre-fetch every queued player's state once (O(n) reads inside the tx
        // instead of O(n²)); a missing row means the player is not Queueing.
        let ids: Vec<uuid::Uuid> = queue.iter().map(|e| e.user_id).collect();
        let states: HashMap<uuid::Uuid, Option<PlayerState>> = sqlx::query_as::<_, (uuid::Uuid, String)>(
            "SELECT user_id, state FROM player_state WHERE user_id = ANY($1)",
        )
        .bind(&ids)
        .fetch_all(&mut *tx)
        .await
        .map_err(map_db_error)?
        .into_iter()
        .map(|(uid, state)| (uid, Some(parse_player_state(&state))))
        .collect();

        for i in 0..queue.len() {
            let player_a = queue[i].clone();

            // Skip desynced players — and, since we hold the FOR UPDATE lock,
            // drop their stale entry atomically so the next pairer never sees it.
            if states.get(&player_a.user_id).and_then(|s| *s) != Some(PlayerState::Queueing) {
                sqlx::query("DELETE FROM matchmaking_queue WHERE user_id = $1 AND game_mode = $2")
                    .bind(player_a.user_id)
                    .bind(mode)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_db_error)?;
                continue;
            }

            let wait_s = (now - player_a.queued_at).num_seconds().max(0) as f64;
            let (lo, hi) =
                lobby_core::queue::search_band(wait_s, player_a.mu, player_a.difficulty.mmr_offset());
            // Find first compatible opponent
            let mut opponent: Option<QueueEntry> = None;
            for player_b in queue.iter().cloned() {
                if player_b.user_id == player_a.user_id {
                    continue;
                }

                // Skip if opponent is also desynced (same atomic delete as above).
                if states.get(&player_b.user_id).and_then(|s| *s) != Some(PlayerState::Queueing) {
                    sqlx::query("DELETE FROM matchmaking_queue WHERE user_id = $1 AND game_mode = $2")
                        .bind(player_b.user_id)
                        .bind(mode)
                        .execute(&mut *tx)
                        .await
                        .map_err(map_db_error)?;
                    continue;
                }

                // Same-pair cooldown: don't immediately re-pair two accounts that
                // just resolved a match against each other.
                let cooldown = sqlx::query_scalar::<_, bool>(
                    "SELECT EXISTS(SELECT 1 FROM matches \
                     WHERE ((player_a = $1 AND player_b = $2) OR (player_a = $2 AND player_b = $1)) \
                       AND status = 'Resolved' AND ended_at >= $3)",
                )
                .bind(player_a.user_id)
                .bind(player_b.user_id)
                .bind(now - chrono::Duration::seconds(pair_cooldown_secs))
                .fetch_one(&mut *tx)
                .await
                .map_err(map_db_error)?;
                if cooldown {
                    continue;
                }

                if player_b.mu >= lo && player_b.mu <= hi {
                    opponent = Some(player_b);
                    break;
                }
            }

            if let Some(player_b) = opponent {
                // Remove both from the queue (within the same transaction as the
                // match insert — the fix for the old dequeue-then-create window).
                sqlx::query("DELETE FROM matchmaking_queue WHERE user_id = $1 AND game_mode = $2")
                    .bind(player_a.user_id)
                    .bind(mode)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_db_error)?;
                sqlx::query("DELETE FROM matchmaking_queue WHERE user_id = $1 AND game_mode = $2")
                    .bind(player_b.user_id)
                    .bind(mode)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_db_error)?;

                let match_info = MatchInfo {
                    match_token: uuid::Uuid::new_v4().to_string(),
                    player_a: player_a.user_id,
                    player_a_difficulty: player_a.difficulty,
                    player_b: player_b.user_id,
                    player_b_difficulty: player_b.difficulty,
                    game_mode: mode.to_string(),
                    game_type,
                    status: MatchStatus::PendingAccept,
                    created_at: now,
                    accepted_at: None,
                    started_at: None,
                    ended_at: None,
                    server_address: None,
                    join_token: None,
                    result_secret: (game_type == GameType::Server)
                        .then(|| uuid::Uuid::new_v4().to_string()),
                    accepted_a: false,
                    accepted_b: false,
                    connected_a: false,
                    connected_b: false,
                };

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
                .bind(format!("{:?}", match_info.game_type).to_lowercase())
                .bind(&match_info.result_secret)
                .execute(&mut *tx)
                .await
                .map_err(map_db_error)?;
                sqlx::query(
                    "INSERT INTO match_events (match_token, event_type, user_id) VALUES ($1, $2, NULL)",
                )
                .bind(&match_info.match_token)
                .bind(format!("{:?}", MatchEvent::Paired).to_lowercase())
                .execute(&mut *tx)
                .await
                .map_err(map_db_error)?;

                tx.commit().await.map_err(map_db_error)?;
                return Ok(Some(match_info));
            }
        }

        tx.rollback().await.map_err(map_db_error)?;
        Ok(None)
    }
}
