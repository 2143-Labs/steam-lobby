use std::collections::{HashMap, HashSet};

use chrono::Utc;
use uuid::Uuid;

use crate::error::Result;
use crate::traits::{GameCallbacks, MatchStore, PlayerStore, QueueStore, RatingStore};
use crate::types::{MatchInfo, MatchStatus};

/// Do not re-pair the same two accounts within this window after a resolved match.
const SAME_PAIR_COOLDOWN_SECS: i64 = 300;

/// Expanding search band: 50 at t=0, +25 every 10s of wait, capped at 400.
/// Returns `(lo, hi)` in MMR terms, both shifted by the difficulty offset.
pub fn search_band(wait_secs: f64, mu: f64, offset: f64) -> (f64, f64) {
    let band = (50.0 + (wait_secs / 10.0).floor() * 25.0).min(400.0);
    (mu - band + offset, mu + band + offset)
}

pub struct MatchmakingQueue<CB: GameCallbacks> {
    callbacks: CB,
}

impl<CB: GameCallbacks> MatchmakingQueue<CB> {
    pub fn new(callbacks: CB) -> Self {
        Self { callbacks }
    }

    /// tick scans the queue for a mode, pairs compatible players using expanding MMR bands.
    /// Returns Some(MatchInfo) when a match is formed, None otherwise.
    pub async fn tick(
        &self,
        mode: &str,
        queue_store: &dyn QueueStore,
        match_store: &dyn MatchStore,
        player_store: &dyn PlayerStore,
        _rating_store: &dyn RatingStore,
    ) -> Result<Option<MatchInfo>> {
        let mut queue = queue_store.get_queue(mode).await?;
        // Sort by queued_at ASC (longest waiting first)
        queue.sort_by_key(|e| e.queued_at);

        if queue.len() < 2 {
            return Ok(None);
        }

        let now = Utc::now();

        // Pre-fetch every queued player's state once (O(n) DB reads instead of O(n²)).
        let ids: HashSet<u64> = queue.iter().map(|e| e.steam_id).collect();
        let mut states: HashMap<u64, Option<crate::types::PlayerInfo>> = HashMap::new();
        for id in ids {
            states.insert(id, player_store.get_player_state(id).await?);
        }

        for i in 0..queue.len() {
            let player_a = &queue[i];

            // Skip desynced players
            if let Some(state) = states.get(&player_a.steam_id).cloned().flatten() {
                if state.state != crate::types::PlayerState::Queueing {
                    queue_store.dequeue(player_a.steam_id, mode).await?;
                    continue;
                }
            }

            let wait_s = (now - player_a.queued_at).num_seconds().max(0) as f64;
            let (lo, hi) = search_band(wait_s, player_a.mu, player_a.difficulty.mmr_offset());
            // Find first compatible opponent
            let mut opponent_idx = None;
            for (j, player_b) in queue.iter().enumerate() {
                if i == j {
                    continue;
                }

                // Skip if opponent is also desynced
                if let Some(state) = states.get(&player_b.steam_id).cloned().flatten() {
                    if state.state != crate::types::PlayerState::Queueing {
                        queue_store.dequeue(player_b.steam_id, mode).await?;
                        continue;
                    }
                }

                // Same-pair cooldown: don't immediately re-pair two accounts that
                // just resolved a match against each other.
                if match_store
                    .recent_match_between(
                        player_a.steam_id,
                        player_b.steam_id,
                        now - chrono::Duration::seconds(SAME_PAIR_COOLDOWN_SECS),
                    )
                    .await?
                {
                    continue;
                }

                if player_b.mu >= lo && player_b.mu <= hi {
                    opponent_idx = Some(j);
                    break;
                }
            }

            if let Some(j) = opponent_idx {
                let player_b = &queue[j];

                // Remove both from queue
                queue_store.dequeue(player_a.steam_id, mode).await?;
                queue_store.dequeue(player_b.steam_id, mode).await?;

                let match_info = MatchInfo {
                    match_token: Uuid::new_v4().to_string(),
                    player_a: player_a.steam_id,
                    player_a_difficulty: player_a.difficulty,
                    player_b: player_b.steam_id,
                    player_b_difficulty: player_b.difficulty,
                    game_mode: mode.to_string(),
                    status: MatchStatus::PendingAccept,
                    created_at: now,
                    accepted_at: None,
                    started_at: None,
                    ended_at: None,
                    accepted_a: false,
                    accepted_b: false,
                    connected_a: false,
                    connected_b: false,
                };

                // Check if game-specific logic rejects this match
                if !self.callbacks.on_match_found(&match_info).await? {
                    // Re-enqueue both players
                    queue_store.enqueue(player_a).await?;
                    queue_store.enqueue(player_b).await?;
                    continue;
                }

                match_store.create_match(&match_info).await?;
                return Ok(Some(match_info));
            }
        }

        Ok(None)
    }

    /// Remove queue entries for players with no heartbeat in the last 30 seconds.
    pub async fn cleanup_stale(&self, queue_store: &dyn QueueStore) -> Result<()> {
        queue_store
            .remove_stale_queue_entries(chrono::Duration::seconds(30))
            .await
    }
}
