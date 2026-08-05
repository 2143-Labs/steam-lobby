//! `QueueStore` impl for `PostgresStore`: the `matchmaking_queue` table.
use super::*;

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

    async fn remove_stale_queue_entries(&self, timeout: Duration) -> Result<Vec<SteamId>> {
        // Liveness is the player's heartbeat, not the moment they queued:
        // a client that keeps heartbeating may stay queued indefinitely.
        let cutoff = Utc::now() - timeout;
        let rows = sqlx::query_scalar::<_, i64>(
            "DELETE FROM matchmaking_queue q \
             USING player_state ps \
             WHERE q.steam_id = ps.steam_id \
               AND ps.last_heartbeat < $1 \
             RETURNING q.steam_id",
        )
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        if !rows.is_empty() {
            tracing::info!("removed {} stale queue entries (no heartbeat)", rows.len());
        }
        Ok(rows.into_iter().map(|id| id as SteamId).collect())
    }
}
