use std::sync::Arc;
use std::time::Duration;

use lobby_core::traits::{QueueStore, RatingStore};
use lobby_core::types::LeaderboardEntry;

use crate::state::AppState;
use crate::ws::{OpponentInfo, ServerMessage};

pub async fn tick_loop(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        for mode in &["ranked_1v1"] {
            match state
                .matchmaking_queue
                .tick(mode, &state.store, &state.store, &state.store, &state.store)
                .await
            {
                Ok(Some(match_info)) => {
                    tracing::info!(
                        "match {} formed: {} vs {} ({})",
                        match_info.match_token,
                        match_info.player_a,
                        match_info.player_b,
                        match_info.game_mode
                    );
                    // Notify both players — spawn each notification to avoid
                    // blocking the 2s tick cycle on Steam API HTTP calls.
                    let state_a = state.clone();
                    let info_a = match_info.clone();
                    tokio::spawn(async move {
                        // Fetch the display name BEFORE taking the connections lock so
                        // the global mutex is never held across a Steam API call.
                        let opponent_name = state_a
                            .steam_auth
                            .get_player_summary(info_a.player_b)
                            .await
                            .unwrap_or_else(|_| "Unknown".into());
                        let connections = state_a.connections.lock().await;
                        if let Some(tx_a) = connections.get(&info_a.player_a).map(|e| &e.tx) {
                            let _ = tx_a.send(ServerMessage::MatchFound {
                                match_token: info_a.match_token.clone(),
                                opponent: OpponentInfo {
                                    steam_id: info_a.player_b,
                                    display_name: opponent_name,
                                },
                                timeout_ms: 30_000,
                            });
                        }
                    });

                    let state_b = state.clone();
                    let info_b = match_info;
                    tokio::spawn(async move {
                        let opponent_name = state_b
                            .steam_auth
                            .get_player_summary(info_b.player_a)
                            .await
                            .unwrap_or_else(|_| "Unknown".into());
                        let connections = state_b.connections.lock().await;
                        if let Some(tx_b) = connections.get(&info_b.player_b).map(|e| &e.tx) {
                            let _ = tx_b.send(ServerMessage::MatchFound {
                                match_token: info_b.match_token.clone(),
                                opponent: OpponentInfo {
                                    steam_id: info_b.player_a,
                                    display_name: opponent_name,
                                },
                                timeout_ms: 30_000,
                            });
                        }
                    });
                }
                Ok(None) => {}
                Err(e) => tracing::error!("queue tick failed: {e}"),
            }
        }

        // Live queue stats for the demo: best-effort, never blocks the tick on errors.
        if let Ok(queue) = state.store.get_queue("ranked_1v1").await {
            let leaderboard: Vec<LeaderboardEntry> = state
                .store
                .list_ratings()
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|(id, r)| LeaderboardEntry {
                    steam_id: id,
                    mu: r.mu,
                    sigma: r.sigma,
                    rating: r.mu - 3.0 * r.sigma,
                })
                .collect();
            let now = chrono::Utc::now();
            let mut sends: Vec<(u64, ServerMessage)> = Vec::new();
            for entry in &queue {
                let wait_s = (now - entry.queued_at).num_seconds().max(0) as f64;
                let (lo, hi) =
                    lobby_core::queue::search_band(wait_s, entry.mu, entry.difficulty.mmr_offset());
                let candidates = queue
                    .iter()
                    .filter(|o| o.steam_id != entry.steam_id && o.mu >= lo && o.mu <= hi)
                    .count() as u32;
                let rating = state
                    .store
                    .get_rating(entry.steam_id, "ranked_1v1")
                    .await
                    .unwrap_or(lobby_core::types::OpenSkillRating {
                        mu: entry.mu,
                        sigma: 25.0 / 3.0,
                        last_updated: now,
                    });
                sends.push((
                    entry.steam_id,
                    ServerMessage::QueueStatus {
                        elapsed_ms: (wait_s * 1000.0) as u64,
                        band_lo: lo,
                        band_hi: hi,
                        candidates,
                        queue_size: queue.len() as u32,
                        my_mu: rating.mu,
                        my_sigma: rating.sigma,
                        my_rating: rating.mu - 3.0 * rating.sigma,
                        leaderboard: leaderboard.clone(),
                    },
                ));
            }
            let connections = state.connections.lock().await; // lock ONLY after all awaits
            for (sid, msg) in sends {
                if let Some(entry) = connections.get(&sid) {
                    let _ = entry.tx.send(msg);
                }
            }
        }
        let _ = state.matchmaking_queue.cleanup_stale(&state.store).await;
        let _ = state
            .match_manager
            .expire_pending_accepts(&state.store)
            .await;
        let _ = state
            .match_manager
            .expire_pending_reports(&state.store, &state.store)
            .await;
    }
}
