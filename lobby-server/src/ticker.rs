use std::sync::Arc;
use std::time::Duration;

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
