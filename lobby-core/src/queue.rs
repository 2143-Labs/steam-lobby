//! Matchmaking with an expanding MMR `search_band` (50 at t=0, +25 every 10s,
//! capped at 400): `MatchmakingQueue::tick` pairs compatible players per mode
//! under a re-pair cooldown, and `cleanup_stale` drops heartbeat-dead entries.
use std::collections::{HashMap, HashSet};

use chrono::Utc;
use uuid::Uuid;

use crate::error::Result;
use crate::traits::{GameCallbacks, MatchStore, PlayerStore, QueueStore, RatingStore};
use crate::types::{GameType, MatchEvent, MatchInfo, MatchStatus, SteamId};

/// Expanding search band: 50 at t=0, +25 every 10s of wait, capped at 400.
/// Returns `(lo, hi)` in MMR terms, both shifted by the difficulty offset.
pub fn search_band(wait_secs: f64, mu: f64, offset: f64) -> (f64, f64) {
    let band = (50.0 + (wait_secs / 10.0).floor() * 25.0).min(400.0);
    (mu - band + offset, mu + band + offset)
}

/// Do not re-pair the same two accounts within this window after a resolved
/// match (anti rating-farm). Configurable so a demo can rematch instantly.
pub struct MatchmakingQueue<CB: GameCallbacks> {
    callbacks: CB,
    pair_cooldown_secs: i64,
}

impl<CB: GameCallbacks> MatchmakingQueue<CB> {
    pub fn new(callbacks: CB) -> Self {
        Self::with_pair_cooldown(callbacks, 300)
    }

    pub fn with_pair_cooldown(callbacks: CB, pair_cooldown_secs: i64) -> Self {
        Self {
            callbacks,
            pair_cooldown_secs,
        }
    }

    /// tick scans the queue for a mode, pairs compatible players using expanding MMR bands.
    /// Returns Some(MatchInfo) when a match is formed, None otherwise.
    pub async fn tick(
        &self,
        mode: &str,
        game_type: GameType,
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
                        now - chrono::Duration::seconds(self.pair_cooldown_secs),
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
                    game_type,
                    status: MatchStatus::PendingAccept,
                    created_at: now,
                    accepted_at: None,
                    started_at: None,
                    ended_at: None,
                    server_address: None,
                    join_token: None,
                    result_secret: (game_type == GameType::Server)
                        .then(|| Uuid::new_v4().to_string()),
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
                match_store
                    .record_match_event(&match_info.match_token, MatchEvent::Paired, None)
                    .await?;
                return Ok(Some(match_info));
            }
        }

        Ok(None)
    }

    /// Remove queue entries for players with no heartbeat in the last 30 seconds.
    /// Returns the steam_ids that were removed.
    pub async fn cleanup_stale(&self, queue_store: &dyn QueueStore) -> Result<Vec<SteamId>> {
        queue_store
            .remove_stale_queue_entries(chrono::Duration::seconds(30))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::search_band;

    #[test]
    fn band_starts_50_wide() {
        assert_eq!(search_band(0.0, 1000.0, 0.0), (950.0, 1050.0));
    }

    #[test]
    fn band_widens_in_ten_second_steps() {
        // Sub-step waits keep the base 50-wide band.
        assert_eq!(search_band(9.9, 1000.0, 0.0), (950.0, 1050.0));
        // Each full 10s of waiting widens the band by 25 per side.
        assert_eq!(search_band(10.0, 1000.0, 0.0), (925.0, 1075.0));
        assert_eq!(search_band(30.0, 1000.0, 0.0), (875.0, 1125.0));
    }

    #[test]
    fn band_caps_at_400() {
        assert_eq!(search_band(200.0, 1000.0, 0.0), (600.0, 1400.0));
        // Far longer waits never widen past the cap.
        assert_eq!(search_band(10_000.0, 1000.0, 0.0), (600.0, 1400.0));
    }

    #[test]
    fn difficulty_offset_shifts_the_whole_band() {
        // Easy (-150) targets weaker opponents; Hard (+150) targets stronger.
        assert_eq!(search_band(0.0, 1000.0, -150.0), (800.0, 900.0));
        assert_eq!(search_band(0.0, 1000.0, 150.0), (1100.0, 1200.0));
    }

    #[test]
    fn low_mmr_easy_offset_can_go_negative() {
        // The lower bound may dip below zero for a new player on Easy; that is
        // fine because pairing compares `opponent.mu >= lo` (everyone passes).
        assert_eq!(search_band(0.0, 25.0, -150.0), (-175.0, -75.0));
    }
}
